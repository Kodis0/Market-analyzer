from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import websockets

from utils.collections import chunked

log = logging.getLogger("bybit_ws")


class BybitWS:
    """
    Bybit v5 Public WS (Spot) orderbook subscriber with dynamic topic updates.

    Key points:
      - single reader task (no concurrent ws.recv in multiple coroutines)
      - subscribe/unsubscribe with req_id + ack futures
      - set_symbols() triggers incremental resubscribe without restarting the process
      - reconnect on network errors / subscribe failures
    """

    def __init__(
        self,
        ws_url: str,
        symbols: list[str],
        depth: int,
        ping_interval_sec: int,
        on_orderbook_message: Callable[[dict], Awaitable[None]],
        subscribe_batch: int = 10,
        subscribe_ack_timeout: float = 6.0,
    ) -> None:
        self.ws_url = ws_url
        self.depth = int(depth)
        self.ping_interval_sec = int(ping_interval_sec)
        self.on_orderbook_message = on_orderbook_message

        self.subscribe_batch = max(1, int(subscribe_batch))
        self.subscribe_ack_timeout = float(subscribe_ack_timeout)

        self._stop = asyncio.Event()
        self._reconnect = asyncio.Event()

        self._ws: Any | None = None
        self._send_lock = asyncio.Lock()

        self._state_lock = asyncio.Lock()
        self._desired_topics: set[str] = set()
        self._subscribed_topics: set[str] = set()

        self._desired_changed = asyncio.Event()

        self._pending_acks: dict[str, asyncio.Future[dict]] = {}

        self._last_reconnect_ts = 0.0
        self._reconnect_cooldown_sec = 3.0

        self.set_symbols(symbols, notify=False)

    def _topics_for_symbols(self, symbols: list[str]) -> set[str]:
        return {f"orderbook.{self.depth}.{s}" for s in symbols if s}

    def set_symbols(self, symbols: list[str], notify: bool = True) -> None:
        """
        Update desired symbols. If connected, control loop will apply deltas.
        Non-async by design: safe to call from anywhere.
        """
        topics = self._topics_for_symbols(symbols)

        # store desired topics
        async def _set():
            async with self._state_lock:
                self._desired_topics = topics
            self._desired_changed.set()

        if notify:
            asyncio.get_running_loop().call_soon_threadsafe(lambda: asyncio.create_task(_set()))
        else:
            # used in __init__ before loop runs: best-effort synchronous set
            self._desired_topics = topics

    async def run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                await self._connect_and_run()
                backoff = 1.0
            except Exception as e:
                log.exception("ws crashed: %s", e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)

    async def stop(self) -> None:
        self._stop.set()
        self._reconnect.set()
        try:
            if self._ws is not None:
                await self._ws.close()
        except Exception:
            pass

    def request_reconnect(self) -> None:
        now = time.time()
        if (now - self._last_reconnect_ts) < self._reconnect_cooldown_sec:
            return
        self._last_reconnect_ts = now
        self._reconnect.set()

    async def _connect_and_run(self) -> None:
        log.info("connecting %s", self.ws_url)
        self._reconnect.clear()

        async with websockets.connect(
            self.ws_url,
            ping_interval=None,
            close_timeout=2,
            max_queue=2048,
        ) as ws:
            self._ws = ws

            async with self._state_lock:
                self._subscribed_topics = set()

            reader_task = asyncio.create_task(self._reader_loop(ws), name="bybit_reader")
            ping_task = asyncio.create_task(self._ping_loop(ws), name="bybit_ping")
            control_task = asyncio.create_task(self._control_loop(), name="bybit_control")
            reconnect_task = asyncio.create_task(self._reconnect_watcher(ws), name="bybit_reconnect")

            try:
                # initial apply
                await self._apply_topic_delta()

                # wait until something ends
                done, pending = await asyncio.wait(
                    [reader_task, ping_task, control_task, reconnect_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )

                for d in done:
                    exc = d.exception()
                    if exc:
                        raise exc

            finally:
                for t in [reader_task, ping_task, control_task, reconnect_task]:
                    t.cancel()
                self._ws = None

                # resolve pending acks to avoid leaks
                for req_id, fut in list(self._pending_acks.items()):
                    if not fut.done():
                        fut.set_exception(RuntimeError("ws closed"))
                self._pending_acks.clear()

    async def _control_loop(self) -> None:
        """
        Applies desired topic changes (subscribe/unsubscribe) when set_symbols() is called.
        Coalesces bursts via event.
        """
        while True:
            await self._desired_changed.wait()
            self._desired_changed.clear()
            await self._apply_topic_delta()

    async def _apply_topic_delta(self) -> None:
        ws = self._ws
        if ws is None:
            return

        async with self._state_lock:
            desired = set(self._desired_topics)
            subscribed = set(self._subscribed_topics)

        to_sub = sorted(list(desired - subscribed))
        to_unsub = sorted(list(subscribed - desired))

        if not to_sub and not to_unsub:
            return

        # unsubscribe first (reduce load)
        if to_unsub:
            for part in chunked(to_unsub, self.subscribe_batch):
                await self._send_with_ack(ws, op="unsubscribe", topics=part)

            async with self._state_lock:
                self._subscribed_topics -= set(to_unsub)

        if to_sub:
            for part in chunked(to_sub, self.subscribe_batch):
                await self._send_with_ack(ws, op="subscribe", topics=part)

            async with self._state_lock:
                self._subscribed_topics |= set(to_sub)

        log.info(
            "topics updated: subscribed=%d desired=%d (+%d -%d)",
            len(self._subscribed_topics),
            len(desired),
            len(to_sub),
            len(to_unsub),
        )

    async def _send_with_ack(self, ws: Any, op: str, topics: list[str]) -> None:
        if not topics:
            return

        attempts = 3
        for attempt in range(1, attempts + 1):
            req_id = uuid.uuid4().hex
            fut: asyncio.Future[dict] = asyncio.get_running_loop().create_future()
            self._pending_acks[req_id] = fut

            payload = {"op": op, "args": topics, "req_id": req_id}

            try:
                async with self._send_lock:
                    await ws.send(json.dumps(payload))

                msg = await asyncio.wait_for(fut, timeout=self.subscribe_ack_timeout)

                # cleanup
                self._pending_acks.pop(req_id, None)

                if (
                    isinstance(msg, dict)
                    and msg.get("op") in {"subscribe", "unsubscribe"}
                    and msg.get("success") is True
                ):
                    return

                # if negative ack
                raise RuntimeError(f"{op} failed: {msg}")

            except asyncio.TimeoutError:
                self._pending_acks.pop(req_id, None)
                if attempt < attempts:
                    log.warning("ack timeout op=%s topics=%d attempt=%d/%d", op, len(topics), attempt, attempts)
                    await asyncio.sleep(0.3 * attempt)
                    continue
                # final failure -> reconnect
                self.request_reconnect()
                raise

            except Exception:
                self._pending_acks.pop(req_id, None)
                if attempt < attempts:
                    log.warning(
                        "ack error op=%s topics=%d attempt=%d/%d", op, len(topics), attempt, attempts, exc_info=True
                    )
                    await asyncio.sleep(0.3 * attempt)
                    continue
                self.request_reconnect()
                raise

    async def _reader_loop(self, ws: Any) -> None:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except Exception:
                continue

            if not isinstance(msg, dict):
                continue

            # ack routing
            req_id = msg.get("req_id")
            if req_id and req_id in self._pending_acks:
                fut = self._pending_acks.get(req_id)
                if fut and not fut.done():
                    fut.set_result(msg)
                continue

            # some acks might not include req_id (rare); treat success=false as reconnect trigger
            if msg.get("op") in {"subscribe", "unsubscribe"} and msg.get("success") is False:
                log.error("subscribe/unsubscribe failed: %s", msg)
                self.request_reconnect()
                continue

            # ignore ping/pong
            if msg.get("op") in {"pong", "ping"}:
                continue

            topic = str(msg.get("topic", "") or "")
            if topic.startswith("orderbook."):
                await self.on_orderbook_message(msg)

    async def _reconnect_watcher(self, ws: Any) -> None:
        await self._reconnect.wait()
        try:
            await ws.close()
        except Exception:
            pass

    async def _ping_loop(self, ws: Any) -> None:
        while True:
            await asyncio.sleep(self.ping_interval_sec)
            try:
                async with self._send_lock:
                    await ws.send(json.dumps({"op": "ping"}))
            except Exception:
                return
