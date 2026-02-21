"""
MetricsCollector: collects signal and skip events over a sliding window.

Used by auto-tuner to evaluate whether parameters need adjustment.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Any

log = logging.getLogger("auto_tune.metrics")


@dataclass
class SignalEvent:
    """Record of a signal sent."""

    timestamp: float


@dataclass
class SkipEvent:
    """Record of skip stats from engine drain."""

    timestamp: float
    stats: dict[str, int]


class MetricsCollector:
    """
    Collects signal and skip events in a sliding time window.

    - record_signal(): call from on_signal callback
    - record_skips(stats_dict): call when engine.drain_debug_stats() returns data
    - get_window_stats(): returns aggregated stats for the window
    """

    # Keys we aggregate from engine skip stats (A + B branches)
    SKIP_PROFIT_KEYS = ("A_skip_profit_le0", "B_skip_profit_le0")
    SKIP_PERSISTENCE_KEYS = ("A_skip_persistence", "B_skip_persistence")
    SKIP_DEDUP_KEYS = ("A_skip_dedup", "B_skip_dedup")
    SKIP_DEPTH_KEYS = ("A_skip_depth", "B_skip_depth")
    SKIP_CEX_SLIP_KEYS = ("A_skip_cex_slip", "B_skip_cex_slip")

    def __init__(self, window_sec: float = 30 * 60) -> None:
        """
        Args:
            window_sec: Sliding window duration in seconds (default 30 min).
        """
        self._window_sec = float(window_sec)
        self._signals: deque[SignalEvent] = deque()
        self._skips: deque[SkipEvent] = deque()
        self._lock = threading.Lock()

    def record_signal(self) -> None:
        """Record that a signal was sent. Call from on_signal callback."""
        with self._lock:
            now = time.time()
            self._signals.append(SignalEvent(timestamp=now))
            self._prune(now)

    def record_skips(self, stats_dict: dict[str, int]) -> None:
        """
        Record skip stats from engine.drain_debug_stats().

        Args:
            stats_dict: Dict returned by engine.drain_debug_stats(), e.g.
                {"A_skip_profit_le0": 10, "B_skip_profit_le0": 5, ...}
        """
        if not stats_dict:
            return
        with self._lock:
            now = time.time()
            self._skips.append(SkipEvent(timestamp=now, stats=dict(stats_dict)))
            self._prune(now)

    def _prune(self, now: float) -> None:
        """Remove events older than window."""
        cutoff = now - self._window_sec
        while self._signals and self._signals[0].timestamp < cutoff:
            self._signals.popleft()
        while self._skips and self._skips[0].timestamp < cutoff:
            self._skips.popleft()

    def get_window_stats(self) -> dict[str, Any]:
        """
        Return aggregated stats for the current window.

        Returns:
            Dict with keys:
            - signals_sent: int
            - skip_profit_le0: int (A + B)
            - skip_persistence: int (A + B)
            - skip_dedup: int (A + B)
            - skip_spread: int
            - skip_depth: int (A + B)
            - skip_cex_slip: int (A + B)
            - window_sec: float
            - events_count: int (number of skip batches)
        """
        with self._lock:
            now = time.time()
            self._prune(now)

            signals_sent = len(self._signals)

            skip_profit_le0 = 0
            skip_persistence = 0
            skip_dedup = 0
            skip_spread = 0
            skip_depth = 0
            skip_cex_slip = 0

            for ev in self._skips:
                for k in self.SKIP_PROFIT_KEYS:
                    skip_profit_le0 += ev.stats.get(k, 0)
                for k in self.SKIP_PERSISTENCE_KEYS:
                    skip_persistence += ev.stats.get(k, 0)
                for k in self.SKIP_DEDUP_KEYS:
                    skip_dedup += ev.stats.get(k, 0)
                skip_spread += ev.stats.get("skip_spread", 0) + ev.stats.get("poll_skip_spread", 0)
                for k in self.SKIP_DEPTH_KEYS:
                    skip_depth += ev.stats.get(k, 0)
                for k in self.SKIP_CEX_SLIP_KEYS:
                    skip_cex_slip += ev.stats.get(k, 0)

            return {
                "signals_sent": signals_sent,
                "skip_profit_le0": skip_profit_le0,
                "skip_persistence": skip_persistence,
                "skip_dedup": skip_dedup,
                "skip_spread": skip_spread,
                "skip_depth": skip_depth,
                "skip_cex_slip": skip_cex_slip,
                "window_sec": self._window_sec,
                "events_count": len(self._skips),
            }
