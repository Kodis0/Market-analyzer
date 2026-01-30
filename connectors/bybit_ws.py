from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Callable, Awaitable, Any, Iterable, List

import websockets

log = logging.getLogger("bybit_ws")


def _chunks(seq: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


class BybitWS:
    def __init__(
        self,
        ws_url: str,
        symbols: list[str],
        depth: int,
        ping_interval_sec: int,
        on_orderbook_message: Callable[[dict], Awaitable[None]],
        subscribe_batch: int = 10,          
        subscribe_ack_timeout: float = 5.0, 
    ) -> None:
        self.ws_url = ws_url
        self.symbols = symbols
        self.depth = depth
        self.ping_interval_sec = ping_interval_sec
        self.on_orderbook_message = on_orderbook_message

        self.subscribe_batch = max(1, int(subscribe_batch))
        self.subscribe_ack_timeout = float(subscribe_ack_timeout)

        self._stop = asyncio.Event()
        self._reconnect = asyncio.Event()
        self._last_reconnect_ts = 0.0
        self._reconnect_cooldown_sec = 5.0

    async def run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                await self._connect_and_listen()
                backoff = 1.0
            except Exception as e:
                log.exception("ws crashed: %s", e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)

    async def stop(self) -> None:
        self._stop.set()
        self._reconnect.set()

    def request_reconnect(self) -> None:
        now = time.time()
        if (now - self._last_reconnect_ts) < self._reconnect_cooldown_sec:
            return
        self._last_reconnect_ts = now
        self._reconnect.set()

    async def _connect_and_listen(self) -> None:
        log.info("connecting %s", self.ws_url)
        self._reconnect.clear()

        async with websockets.connect(
            self.ws_url,
            ping_interval=None,  
            close_timeout=2,
            max_queue=1024,
        ) as ws:
            topics = [f"orderbook.{self.depth}.{s}" for s in self.symbols]

            # --- subscribe батчами + ждём ack ---
            for part in _chunks(topics, self.subscribe_batch):
                await ws.send(json.dumps({"op": "subscribe", "args": part}))
                await self._wait_subscribe_ack(ws, part)

            log.info("subscribed ok: %d topics", len(topics))

            ping_task = asyncio.create_task(self._ping_loop(ws), name="bybit_ping")
            reconnect_task = asyncio.create_task(self._reconnect_watcher(ws), name="bybit_reconnect")

            try:
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    if not isinstance(msg, dict):
                        continue

                    if msg.get("op") in {"pong", "ping", "subscribe"} or "success" in msg:
                        # если вдруг подписка неуспешна - форсим реконнект
                        if msg.get("op") == "subscribe" and msg.get("success") is False:
                            log.error("subscribe failed: %s", msg)
                            self.request_reconnect()
                        continue

                    topic = str(msg.get("topic", "") or "")
                    if topic.startswith("orderbook."):
                        await self.on_orderbook_message(msg)

            finally:
                ping_task.cancel()
                reconnect_task.cancel()

    async def _wait_subscribe_ack(self, ws: Any, topics: List[str]) -> None:
        """
        Ждём подтверждение подписки. Если Bybit отвечает error/success=false — падаем и уходим в reconnect.
        """
        deadline = time.time() + self.subscribe_ack_timeout
        while time.time() < deadline:
            raw = await asyncio.wait_for(ws.recv(), timeout=max(0.1, deadline - time.time()))
            msg = json.loads(raw)

            if not isinstance(msg, dict):
                continue

            # ack на subscribe (v5)
            if msg.get("op") == "subscribe":
                if msg.get("success") is True:
                    return
                raise RuntimeError(f"Bybit subscribe failed: {msg}")

            # иногда приходит {"success":true, "ret_msg":"subscribe", ...}
            if "success" in msg and msg.get("success") is False:
                raise RuntimeError(f"Bybit subscribe failed: {msg}")

            # параллельно может прилетать pong/прочее — игнорим
            if msg.get("op") in {"pong", "ping"}:
                continue

            # если вдруг это уже данные ордербука — не выкидываем
            topic = str(msg.get("topic", "") or "")
            if topic.startswith("orderbook."):
                await self.on_orderbook_message(msg)
                # и продолжаем ждать ack

        raise TimeoutError(f"Subscribe ACK timeout for {len(topics)} topics")

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
                await ws.send(json.dumps({"op": "ping"}))
            except Exception:
                return
