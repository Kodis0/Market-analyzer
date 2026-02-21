from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal

from connectors.jupiter import JupiterClient
from core.calc import calc_mid_spread
from core.state import MarketState

from .denylist import Denylist
from .stats import SkipStats
from .utils import snapshot_book, to_raw

log = logging.getLogger("engine")


class QuotePoller:
    """
    Periodically fetch ONLY BUY quotes (stable -> token) from Jupiter.
    Sell quotes are fetched on-demand in the engine (B-branch).
    """

    def __init__(
        self,
        *,
        state: MarketState,
        jup: JupiterClient,
        token_cfgs: dict[str, dict],
        stable_mint: str,
        stable_decimals: int,
        notional: Decimal,
        denylist: Denylist,
        max_spread_bps: Decimal,
        max_ob_age_ms: int,
        poll_interval_sec: float,
        max_quote_age_ms: int,
        poll_concurrency: int = 24,
        poll_batch_mult: int = 4,
        poll_jitter_ratio: float = 0.15,
        backoff_on_none_sec: float = 5.0,
        backoff_on_err_sec: float = 10.0,
        skip_stats: SkipStats | None = None,
        stop_event: asyncio.Event | None = None,
        exchange_enabled_event: asyncio.Event | None = None,
    ) -> None:
        self.state = state
        self.jup = jup
        self.token_cfgs = token_cfgs

        self.stable_mint = str(stable_mint)
        self.stable_decimals = int(stable_decimals)
        self.notional = Decimal(str(notional))

        self.denylist = denylist
        self.max_spread_bps = Decimal(str(max_spread_bps))
        self.max_ob_age_ms = int(max_ob_age_ms)
        self.poll_interval = float(poll_interval_sec)
        self.max_quote_age_ms = int(max_quote_age_ms)

        self.poll_concurrency = int(poll_concurrency)
        self._poll_batch_mult = int(poll_batch_mult)
        self.poll_jitter_ratio = float(poll_jitter_ratio)

        self._poll_backoff_until: dict[str, float] = {}
        self.backoff_on_none_sec = float(backoff_on_none_sec)
        self.backoff_on_err_sec = float(backoff_on_err_sec)

        self._skip_stats = skip_stats
        self._stop = stop_event or asyncio.Event()
        self._exchange_enabled = exchange_enabled_event  # None = always enabled

    def stop(self) -> None:
        self._stop.set()

    def _dbg(self, k: str) -> None:
        if self._skip_stats:
            self._skip_stats.inc(k)

    def _poll_allowed(self, token_key: str) -> bool:
        return time.time() >= self._poll_backoff_until.get(token_key, 0.0)

    def _poll_backoff(self, token_key: str, sec: float) -> None:
        self._poll_backoff_until[token_key] = time.time() + float(sec)

    def _prune_poll_backoff(self) -> None:
        valid = set(self.token_cfgs)
        stale = [k for k in self._poll_backoff_until if k not in valid]
        for k in stale:
            del self._poll_backoff_until[k]

    @staticmethod
    def _is_pump_mint(mint: str) -> bool:
        try:
            return str(mint).lower().endswith("pump")
        except Exception:
            return False

    async def _poll_one_token(self, token_key: str, cfg: dict) -> None:
        try:
            mint = str(cfg.get("mint", ""))
            bybit_symbol = str(cfg.get("bybit_symbol", ""))
            decimals = int(cfg.get("decimals", 0) or 0)

            if self._is_pump_mint(mint):
                self._dbg("poll_skip_pump_mint")
                return
            if self.denylist.is_denied(token_key, bybit_symbol):
                self._dbg("poll_skip_denied")
                return
            if decimals <= 0 or decimals > 18:
                self._dbg("poll_skip_bad_decimals")
                return

            ob = await self.state.get_orderbook(bybit_symbol)
            if ob is None or not ob.asks or not ob.bids:
                self._dbg("poll_skip_no_ob")
                return
            if ob.age_ms() > self.max_ob_age_ms:
                self._dbg("poll_skip_ob_stale")
                return

            bids, asks = snapshot_book(ob)
            _mid, spread_bps = calc_mid_spread(bids, asks)
            if spread_bps is None:
                self._dbg("poll_skip_no_spread")
                return
            if spread_bps > self.max_spread_bps:
                self._dbg("poll_skip_spread")
                return

            qp = await self.state.get_quote_pair(token_key)

            stable_raw = to_raw(self.notional, self.stable_decimals)
            buy_q = await self.jup.quote_exact_in(self.stable_mint, mint, stable_raw)

            if buy_q is not None:
                async with qp.lock:
                    qp.buy_quote = buy_q
                    qp.buy_updated_ms = self.state.now_ms()
                self._poll_backoff_until.pop(token_key, None)
            else:
                self._dbg("poll_buy_quote_none")
                self._poll_backoff(token_key, self.backoff_on_none_sec)

        except Exception as e:
            self._poll_backoff(token_key, self.backoff_on_err_sec)
            self._dbg("poll_error")
            log.exception("quote_poller error token=%s: %s", token_key, e)

    async def run(self) -> None:
        sem = asyncio.Semaphore(max(1, int(self.poll_concurrency)))

        async def runner(token_key: str, cfg: dict) -> None:
            if not self._poll_allowed(token_key):
                self._dbg("poll_skip_backoff")
                return
            async with sem:
                await self._poll_one_token(token_key, cfg)

        while not self._stop.is_set():
            if self._exchange_enabled is not None and not self._exchange_enabled.is_set():
                await asyncio.sleep(1)
                continue
            self._prune_poll_backoff()
            started = time.time()
            items = list(self.token_cfgs.items())
            batch_size = max(1, int(self.poll_concurrency) * int(self._poll_batch_mult))

            for i in range(0, len(items), batch_size):
                chunk = items[i : i + batch_size]
                tasks = [asyncio.create_task(runner(token_key, cfg)) for token_key, cfg in chunk]
                await asyncio.gather(*tasks, return_exceptions=True)
                await asyncio.sleep(0)

            elapsed = time.time() - started
            jitter = self.poll_interval * float(self.poll_jitter_ratio)
            sleep_for = max(0.0, self.poll_interval - elapsed)
            sleep_for += jitter * (time.time() % 1.0)
            await asyncio.sleep(sleep_for)
