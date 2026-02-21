"""
Quarantine manager: load, apply, sync, add symbols to quarantine.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

from core.config import AppConfig
from core.quarantine import QuarantineEntry, load_quarantine, now_ts, prune_expired, save_quarantine

log = logging.getLogger("app")


class QuarantineManager:
    """
    Manages quarantine state: applies to config, syncs with file, adds symbols.
    """

    def __init__(
        self,
        quarantine_path: Path | str,
        cfg: AppConfig,
        full_symbols: list[str],
        full_tokens: dict[str, Any],
        base_denylist: list[str],
        token_cfgs: dict[str, dict[str, Any]],
    ) -> None:
        self.quarantine_path = Path(quarantine_path)
        self.cfg = cfg
        self.full_symbols = full_symbols
        self.full_tokens = full_tokens
        self.base_denylist = base_denylist
        self.token_cfgs = token_cfgs

        self.quarantined_set: set[str] = set()
        self._lock = asyncio.Lock()
        self._file_lock = asyncio.Lock()  # protects file I/O from add vs sync_loop race
        self._last_write: dict[str, int] = {}

    def _rebuild_denylist_inplace(self) -> None:
        merged = sorted(set(self.base_denylist) | self.quarantined_set)
        if self.cfg.filters.denylist_symbols is None:
            self.cfg.filters.denylist_symbols = merged
        else:
            self.cfg.filters.denylist_symbols.clear()
            self.cfg.filters.denylist_symbols.extend(merged)

    def _apply_quarantine_to_cfg(self) -> None:
        self.cfg.bybit.symbols = [s for s in self.full_symbols if s not in self.quarantined_set]
        self.cfg.trading.tokens = {
            k: v for k, v in self.full_tokens.items() if v.bybit_symbol not in self.quarantined_set
        }
        self._rebuild_denylist_inplace()

    def _rebuild_token_cfgs_inplace(self) -> None:
        self.token_cfgs.clear()
        for token_key, t in self.cfg.trading.tokens.items():
            self.token_cfgs[token_key] = {"bybit_symbol": t.bybit_symbol, "mint": t.mint, "decimals": t.decimals}

    def load_initial(self) -> None:
        """Load quarantine, validate BAD_TOKEN_CFG, apply to config."""
        q0 = prune_expired(load_quarantine(str(self.quarantine_path)))

        bad_added = False
        for token_key, t in list(self.full_tokens.items()):
            ok = (
                bool(getattr(t, "mint", None))
                and getattr(t, "decimals", None) is not None
                and getattr(t, "bybit_symbol", None)
            )
            if not ok:
                sym = getattr(t, "bybit_symbol", "") or ""
                if sym and sym not in q0:
                    q0[sym] = QuarantineEntry(reason="BAD_TOKEN_CFG", until_ts=now_ts() + 24 * 3600)
                    bad_added = True
                    log.warning(
                        "BAD_TOKEN_CFG: token_key=%s sym=%s mint=%s decimals=%s",
                        token_key,
                        sym,
                        getattr(t, "mint", None),
                        getattr(t, "decimals", None),
                    )

        if bad_added:
            save_quarantine(str(self.quarantine_path), q0)

        self.quarantined_set = set(q0.keys())
        self._apply_quarantine_to_cfg()
        self._rebuild_token_cfgs_inplace()

        if self.quarantined_set:
            log.warning(
                "Quarantine enabled: %d symbols disabled. File=%s", len(self.quarantined_set), self.quarantine_path
            )
        else:
            log.info("Quarantine empty/disabled. File=%s", self.quarantine_path)

    async def add(self, symbol: str, reason: str, ttl_sec: int) -> None:
        """Add symbol to quarantine."""
        if not symbol:
            return

        now = now_ts()
        async with self._file_lock:
            last = self._last_write.get(symbol, 0)
            if (now - last) < 15:
                return
            self._last_write[symbol] = now

            until = now + int(ttl_sec)

            q = prune_expired(load_quarantine(str(self.quarantine_path)))
            prev = q.get(symbol)

            if prev is not None and prev.until_ts > (now + 1800):
                async with self._lock:
                    self.quarantined_set.add(symbol)
                    self._apply_quarantine_to_cfg()
                    self._rebuild_token_cfgs_inplace()
                return

            q[symbol] = QuarantineEntry(reason=reason, until_ts=until)
            save_quarantine(str(self.quarantine_path), q)

        async with self._lock:
            self.quarantined_set.add(symbol)
            self._apply_quarantine_to_cfg()
            self._rebuild_token_cfgs_inplace()

        log.warning("AUTO-QUARANTINE: %s reason=%s ttl=%ds file=%s", symbol, reason, ttl_sec, self.quarantine_path)

    async def contains(self, symbol: str) -> bool:
        """Check if symbol is quarantined. Thread-safe."""
        async with self._lock:
            return symbol in self.quarantined_set

    async def sync_loop(
        self,
        poll_sec: float = 10.0,
        on_symbols_changed: Callable[[], Any] | None = None,
    ) -> None:
        """Watch quarantine file and apply changes."""
        last_mtime = 0.0
        while True:
            try:
                mtime = os.path.getmtime(self.quarantine_path)
            except FileNotFoundError:
                mtime = 0.0

            changed = mtime > last_mtime
            last_mtime = max(last_mtime, mtime)

            if changed:
                try:
                    async with self._file_lock:
                        q = load_quarantine(str(self.quarantine_path))
                        q2 = prune_expired(q)
                        if q2.keys() != q.keys():
                            save_quarantine(str(self.quarantine_path), q2)
                        new_set = set(q2.keys())
                except Exception:
                    log.exception("Failed to sync quarantine file=%s", self.quarantine_path)
                    new_set = set()

                async with self._lock:
                    before = set(self.quarantined_set)
                    added = new_set - before
                    removed = before - new_set

                    if added or removed:
                        self.quarantined_set.clear()
                        self.quarantined_set.update(new_set)
                        self._apply_quarantine_to_cfg()
                        self._rebuild_token_cfgs_inplace()

                        if on_symbols_changed:
                            result = on_symbols_changed()
                            if asyncio.iscoroutine(result):
                                await result

                        log.warning(
                            "Quarantine sync: added=%d removed=%d active=%d quarantined=%d",
                            len(added),
                            len(removed),
                            len(self.cfg.bybit.symbols),
                            len(self.quarantined_set),
                        )

            await asyncio.sleep(poll_sec)
