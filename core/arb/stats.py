
from __future__ import annotations

import time
from typing import Dict, Optional


class SkipStats:
    """
    Sliding-window counters for "why we skipped" instrumentation.
    """
    def __init__(self, window_sec: int = 30) -> None:
        self.window_sec = int(window_sec)
        self._counts: Dict[str, int] = {}
        self._last_flush = time.time()

    def inc(self, key: str, n: int = 1) -> None:
        self._counts[key] = self._counts.get(key, 0) + n

    def flush_if_due(self) -> Optional[Dict[str, int]]:
        now = time.time()
        if (now - self._last_flush) < self.window_sec:
            return None
        self._last_flush = now
        data = self._counts
        self._counts = {}
        return data
