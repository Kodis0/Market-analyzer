from __future__ import annotations

import time
from decimal import Decimal

DEDUP_PRUNE_INTERVAL_SEC = 60.0


class Dedup:
    """
    Prevent spamming the same signal too often unless profit improved enough.
    """

    def __init__(self, cooldown_sec: int, min_delta_profit: Decimal) -> None:
        self.cooldown_sec = int(cooldown_sec)
        self.min_delta_profit = Decimal(min_delta_profit)
        self._last_sent: dict[str, tuple[float, Decimal]] = {}
        self._last_prune_ts: float = 0.0

    def _prune_stale(self) -> None:
        now = time.time()
        if (now - self._last_prune_ts) < DEDUP_PRUNE_INTERVAL_SEC:
            return
        self._last_prune_ts = now
        cutoff = now - (self.cooldown_sec * 2)
        stale = [k for k, (ts, _) in self._last_sent.items() if ts < cutoff]
        for k in stale:
            del self._last_sent[k]

    def can_send(self, key: str, profit: Decimal) -> bool:
        self._prune_stale()
        now = time.time()
        prev = self._last_sent.get(key)
        if not prev:
            return True
        last_ts, last_profit = prev
        if (now - last_ts) < self.cooldown_sec and (profit - last_profit) < self.min_delta_profit:
            return False
        return True

    def mark_sent(self, key: str, profit: Decimal) -> None:
        self._last_sent[key] = (time.time(), profit)
