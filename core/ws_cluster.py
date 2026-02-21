"""
Bybit WebSocket cluster: sharded clients with dynamic symbol updates.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

from connectors.bybit_ws import BybitWS
from utils.collections import chunked


class BybitWSCluster:
    """
    Manages N BybitWS clients with sharding, supports dynamic resubscribe.
    """

    def __init__(
        self,
        ws_url: str,
        depth: int,
        ping_interval_sec: int,
        on_orderbook_message: Callable[[dict], Awaitable[None]],
        max_symbols_per_ws: int = 100,
    ) -> None:
        self.ws_url = ws_url
        self.depth = depth
        self.ping_interval_sec = ping_interval_sec
        self.on_orderbook_message = on_orderbook_message
        self.max_symbols_per_ws = max(1, int(max_symbols_per_ws))

        self._clients: list[BybitWS] = []
        self._tasks: list[asyncio.Task] = []
        self._lock = asyncio.Lock()

    @property
    def clients(self) -> list[BybitWS]:
        return list(self._clients)

    async def start(self, symbols: list[str]) -> None:
        await self.update_symbols(symbols)

    async def stop(self) -> None:
        async with self._lock:
            for c in self._clients:
                await c.stop()
            for t in self._tasks:
                t.cancel()
            self._clients.clear()
            self._tasks.clear()

    async def update_symbols(self, symbols: list[str]) -> None:
        shards = list(chunked(list(symbols), self.max_symbols_per_ws))

        async with self._lock:
            while len(self._clients) < len(shards):
                c = BybitWS(
                    ws_url=self.ws_url,
                    symbols=[],
                    depth=self.depth,
                    ping_interval_sec=self.ping_interval_sec,
                    on_orderbook_message=self.on_orderbook_message,
                    subscribe_batch=5,
                    subscribe_ack_timeout=12.0,
                )

                self._clients.append(c)
                self._tasks.append(asyncio.create_task(c.run(), name=f"bybit_ws_{len(self._clients)}"))

            for idx, shard in enumerate(shards):
                self._clients[idx].set_symbols(shard)

            for idx in range(len(shards), len(self._clients)):
                self._clients[idx].set_symbols([])

        log = logging.getLogger("app")
        log.info("WS cluster updated: clients=%d symbols=%d", len(self._clients), len(symbols))
