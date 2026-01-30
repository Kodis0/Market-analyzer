from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, List, Tuple, Optional
import time


@dataclass
class OrderBook:
    symbol: str
    bids: Dict[Decimal, Decimal] = field(default_factory=dict)  # price -> qty
    asks: Dict[Decimal, Decimal] = field(default_factory=dict)  # price -> qty
    last_update_ms: int = 0
    last_cts_ms: int = 0
    last_snapshot_ms: int = 0

    def apply_snapshot(self, bids: List[List[str]], asks: List[List[str]], ts_ms: int, cts_ms: int) -> None:
        self.bids = {Decimal(p): Decimal(q) for p, q in bids if Decimal(q) > 0}
        self.asks = {Decimal(p): Decimal(q) for p, q in asks if Decimal(q) > 0}
        self.last_update_ms = ts_ms
        self.last_cts_ms = cts_ms
        self.last_snapshot_ms = cts_ms or ts_ms

    def apply_delta(self, bids: List[List[str]], asks: List[List[str]], ts_ms: int, cts_ms: int) -> None:
        for p, q in bids:
            price, qty = Decimal(p), Decimal(q)
            if qty == 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = qty

        for p, q in asks:
            price, qty = Decimal(p), Decimal(q)
            if qty == 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = qty

        self.last_update_ms = ts_ms
        self.last_cts_ms = cts_ms

    def bids_sorted(self) -> List[Tuple[Decimal, Decimal]]:
        return sorted(self.bids.items(), key=lambda x: x[0], reverse=True)

    def asks_sorted(self) -> List[Tuple[Decimal, Decimal]]:
        return sorted(self.asks.items(), key=lambda x: x[0])

    def age_ms(self) -> int:
        now = int(time.time() * 1000)
        # если у тебя хранится last_ts_ms / last_cts_ms — используй то, что реально обновляется
        last = int(self.last_cts_ms or self.last_update_ms or 0)
        if last <= 0:
            return 10_000_000
        return max(0, now - last)
