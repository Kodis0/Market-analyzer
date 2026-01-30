
from __future__ import annotations

import time
from decimal import Decimal
from typing import Dict, Tuple


class Dedup:
    """
    Prevent spamming the same signal too often unless profit improved enough.
    """
    def __init__(self, cooldown_sec: int, min_delta_profit: Decimal) -> None:
        self.cooldown_sec = int(cooldown_sec)
        self.min_delta_profit = Decimal(min_delta_profit)
        self._last_sent: Dict[str, Tuple[float, Decimal]] = {}

    def can_send(self, key: str, profit: Decimal) -> bool:
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
