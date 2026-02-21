from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Tuple, Optional
import logging
import time

log = logging.getLogger(__name__)


def _safe_decimal(s: str) -> Optional[Decimal]:
    """Parse string to Decimal; return None on invalid input."""
    try:
        return Decimal(s)
    except (InvalidOperation, TypeError, ValueError):
        return None


@dataclass
class OrderBook:
    symbol: str
    bids: Dict[Decimal, Decimal] = field(default_factory=dict)  # price -> qty
    asks: Dict[Decimal, Decimal] = field(default_factory=dict)  # price -> qty
    last_update_ms: int = 0
    last_cts_ms: int = 0
    last_snapshot_ms: int = 0

    def apply_snapshot(self, bids: List[List[str]], asks: List[List[str]], ts_ms: int, cts_ms: int) -> None:
        def _parse_side(rows: List[List[str]]) -> Dict[Decimal, Decimal]:
            result: Dict[Decimal, Decimal] = {}
            for row in rows:
                if len(row) < 2:
                    log.warning("orderbook %s: skip malformed snapshot row (len<2) row=%r", self.symbol, row)
                    continue
                p, q = row[0], row[1]
                price, qty = _safe_decimal(p), _safe_decimal(q)
                if price is not None and qty is not None and qty > 0:
                    result[price] = qty
                elif price is None or qty is None:
                    log.warning("orderbook %s: skip malformed snapshot row p=%r q=%r", self.symbol, p, q)
            return result

        self.bids = _parse_side(bids or [])
        self.asks = _parse_side(asks or [])
        self.last_update_ms = ts_ms
        self.last_cts_ms = cts_ms
        self.last_snapshot_ms = cts_ms or ts_ms

    def apply_delta(self, bids: List[List[str]], asks: List[List[str]], ts_ms: int, cts_ms: int) -> None:
        for row in bids or []:
            if len(row) < 2:
                log.warning("orderbook %s: skip malformed delta bid row (len<2) row=%r", self.symbol, row)
                continue
            p, q = row[0], row[1]
            price, qty = _safe_decimal(p), _safe_decimal(q)
            if price is None or qty is None:
                log.warning("orderbook %s: skip malformed delta bid p=%r q=%r", self.symbol, p, q)
                continue
            if qty == 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = qty

        for row in asks or []:
            if len(row) < 2:
                log.warning("orderbook %s: skip malformed delta ask row (len<2) row=%r", self.symbol, row)
                continue
            p, q = row[0], row[1]
            price, qty = _safe_decimal(p), _safe_decimal(q)
            if price is None or qty is None:
                log.warning("orderbook %s: skip malformed delta ask p=%r q=%r", self.symbol, p, q)
                continue
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
