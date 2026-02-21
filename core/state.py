from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field

from connectors.jupiter import JupQuote
from core.orderbook import OrderBook


@dataclass
class QuotePair:
    """
    Quotes for one token. Protected by per-token lock so we can:
      - update buy/sell independently
      - take an atomic-ish snapshot inside the engine tick
    """

    lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    buy_quote: JupQuote | None = None
    buy_updated_ms: int = 0

    sell_quote: JupQuote | None = None
    sell_updated_ms: int = 0
    sell_amount_raw: int = 0


@dataclass
class MarketState:
    orderbooks: dict[str, OrderBook] = field(default_factory=dict)
    quotes: dict[str, QuotePair] = field(default_factory=dict)

    # Backward compatible: lock for orderbooks
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    # New: separate lock for quotes map
    quotes_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def upsert_orderbook(self, symbol: str) -> OrderBook:
        async with self.lock:
            ob = self.orderbooks.get(symbol)
            if ob is None:
                ob = OrderBook(symbol=symbol)
                self.orderbooks[symbol] = ob
            return ob

    async def get_orderbook(self, symbol: str) -> OrderBook | None:
        async with self.lock:
            return self.orderbooks.get(symbol)

    async def get_quote_pair(self, token_key: str) -> QuotePair:
        async with self.quotes_lock:
            qp = self.quotes.get(token_key)
            if qp is None:
                qp = QuotePair()
                self.quotes[token_key] = qp
            return qp

    @staticmethod
    def now_ms() -> int:
        return int(time.time() * 1000)
