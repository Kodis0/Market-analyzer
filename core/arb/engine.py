from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal, getcontext
from typing import Any, Callable, Dict, Optional

from core.calc import calc_mid_spread, coverage_pct, net_profit, price_ratio_ok, gross_cap_ok
from core.state import MarketState
from core.vwap import simulate_buy_with_notional, simulate_sell_base
from core.fees import Thresholds
from connectors.jupiter import JupiterClient

from .types import Signal, Buttons
from .dedup import Dedup
from .persistence import Persistence
from .stats import SkipStats
from .denylist import Denylist
from .poller import QuotePoller
from .utils import bybit_spot_url, jup_swap_url_by_symbol, to_raw, from_raw, snapshot_book

log = logging.getLogger("engine")

# Type-only import for reload_settings
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from core.runtime_settings import RuntimeSettings
getcontext().prec = 28


class ArbEngine:
    """
    Orchestrator:
      - QuotePoller keeps BUY quotes warm (stable -> token).
      - Engine tick evaluates both directions and emits Signal via callback.

    Public API is kept compatible with your current app.py:
      - quote_poller() -> async loop
      - run(on_signal) -> async loop
      - stop(), drain_debug_stats()
    """
    def __init__(
        self,
        state: MarketState,
        jup: JupiterClient,
        thresholds: Thresholds,
        notional_usd: Decimal,
        stable_mint: str,
        stable_decimals: int,
        token_cfgs: Dict[str, dict],
        max_cex_slippage_bps: Decimal,
        max_dex_price_impact_pct: Decimal,
        persistence_hits: int,
        cooldown_sec: int,
        min_delta_profit_usd_to_resend: Decimal,
        engine_tick_hz: int,
        jupiter_poll_interval_sec: float,
        price_ratio_max: Decimal | float | str | None = None,
        gross_profit_cap_pct: Decimal | float | str | None = None,
        max_spread_bps: Decimal | float | str | None = None,
        min_depth_coverage_pct: Decimal | float | str | None = None,
        denylist_symbols: list[str] | None = None,
        denylist_regex: list[str] | None = None,
        max_ob_age_ms: int = 2000,
        max_quote_age_ms: int | None = None,
        exchange_enabled_event: asyncio.Event | None = None,
    ) -> None:
        self.state = state
        self.jup = jup
        self.thresholds = thresholds
        self.token_cfgs = token_cfgs

        self.notional = Decimal(str(notional_usd))
        self.stable_mint = str(stable_mint)
        self.stable_decimals = int(stable_decimals)

        self.max_cex_slippage_bps = Decimal(str(max_cex_slippage_bps))
        self.max_dex_price_impact_pct = Decimal(str(max_dex_price_impact_pct))

        self.persistence = Persistence(persistence_hits)
        self.dedup = Dedup(cooldown_sec, Decimal(str(min_delta_profit_usd_to_resend)))

        self.tick_sleep = 1 / max(1, int(engine_tick_hz))

        self.max_price_ratio = Decimal(str(price_ratio_max if price_ratio_max is not None else "3"))
        self.max_gross_profit_pct = Decimal(str(gross_profit_cap_pct if gross_profit_cap_pct is not None else "10"))
        self.max_spread_bps = Decimal(str(max_spread_bps if max_spread_bps is not None else "50"))
        self.min_depth_coverage_pct = Decimal(str(min_depth_coverage_pct if min_depth_coverage_pct is not None else "98"))
        self.max_ob_age_ms = int(max_ob_age_ms)

        # Quotes freshness (default: max(3x poll interval, 5000ms))
        if max_quote_age_ms is None:
            self.max_quote_age_ms = int(max(5000, float(jupiter_poll_interval_sec) * 3 * 1000))
        else:
            self.max_quote_age_ms = int(max_quote_age_ms)

        # Debug instrumentation
        self._skip_stats = SkipStats(window_sec=30)
        self._stop = asyncio.Event()
        self._exchange_enabled = exchange_enabled_event  # None = always enabled

        # Denylist
        self.denylist = Denylist.build(symbols=denylist_symbols, regex=denylist_regex)

        # B-branch sell re-quote throttling
        self._last_b_requote: Dict[str, float] = {}
        self.b_requote_cooldown_sec = 2.0

        # Engine bounded concurrency (safe default)
        self.engine_concurrency = 64
        self._engine_sem = asyncio.Semaphore(self.engine_concurrency)
        self._engine_batch_mult = 4

        # Quote poller component
        self._poller = QuotePoller(
            state=self.state,
            jup=self.jup,
            token_cfgs=self.token_cfgs,
            stable_mint=self.stable_mint,
            stable_decimals=self.stable_decimals,
            notional=self.notional,
            denylist=self.denylist,
            max_spread_bps=self.max_spread_bps,
            max_ob_age_ms=self.max_ob_age_ms,
            poll_interval_sec=float(jupiter_poll_interval_sec),
            max_quote_age_ms=self.max_quote_age_ms,
            skip_stats=self._skip_stats,
            stop_event=self._stop,
            exchange_enabled_event=exchange_enabled_event,
        )

    async def stop(self) -> None:
        self._stop.set()

    def reload_settings(self, settings: "RuntimeSettings") -> None:
        """Apply runtime settings. Called when user updates via /settings."""
        self.thresholds.bybit_taker_fee_bps = Decimal(str(settings.bybit_taker_fee_bps))
        self.thresholds.solana_tx_fee_usd = Decimal(str(settings.solana_tx_fee_usd))
        self.thresholds.latency_buffer_bps = Decimal(str(settings.latency_buffer_bps))
        self.thresholds.usdt_usdc_buffer_bps = Decimal(str(settings.usdt_usdc_buffer_bps))
        self.thresholds.min_profit_usd = Decimal(str(settings.min_profit_usd))

        self.notional = Decimal(str(settings.notional_usd))
        self.max_cex_slippage_bps = Decimal(str(settings.max_cex_slippage_bps))
        self.max_dex_price_impact_pct = Decimal(str(settings.max_dex_price_impact_pct))
        self.max_price_ratio = Decimal(str(settings.price_ratio_max))
        self.max_gross_profit_pct = Decimal(str(settings.gross_profit_cap_pct))
        self.max_spread_bps = Decimal(str(settings.max_spread_bps))
        self.min_depth_coverage_pct = Decimal(str(settings.min_depth_coverage_pct))
        self.max_ob_age_ms = int(settings.max_ob_age_ms)
        self.tick_sleep = 1 / max(1, int(settings.engine_tick_hz))

        self.persistence.hits = max(1, int(settings.persistence_hits))
        self.dedup.cooldown_sec = int(settings.cooldown_sec)
        self.dedup.min_delta_profit = Decimal(str(settings.min_delta_profit_usd_to_resend))

        self._poller.notional = Decimal(str(settings.notional_usd))
        self._poller.max_spread_bps = Decimal(str(settings.max_spread_bps))
        self._poller.max_ob_age_ms = int(settings.max_ob_age_ms)
        self._poller.poll_interval = float(settings.jupiter_poll_interval_sec)
        self._poller.max_quote_age_ms = int(max(5000, settings.jupiter_poll_interval_sec * 3 * 1000))

        log.info("Settings reloaded: min_profit=%.2f notional=%.0f", settings.min_profit_usd, settings.notional_usd)

    def drain_debug_stats(self) -> Optional[Dict[str, int]]:
        return self._skip_stats.flush_if_due()

    def _dbg_inc(self, k: str, n: int = 1) -> None:
        self._skip_stats.inc(k, n)

    @staticmethod
    def _is_pump_mint(mint: str) -> bool:
        try:
            return str(mint).lower().endswith("pump")
        except Exception:
            return False

    async def quote_poller(self) -> None:
        await self._poller.run()

    def _reset_persistence(self, token_key: str) -> None:
        self.persistence.hit(f"{token_key}:A", False)
        self.persistence.hit(f"{token_key}:B", False)

    async def _run_one_token(self, token_key: str, cfg: dict, on_signal: Callable[[Signal], Any]) -> None:
        try:
            mint = str(cfg.get("mint", ""))
            bybit_symbol = str(cfg.get("bybit_symbol", ""))
            decimals = int(cfg.get("decimals", 0) or 0)

            if self._is_pump_mint(mint):
                self._dbg_inc("skip_pump_mint")
                self._reset_persistence(token_key)
                return
            if self.denylist.is_denied(token_key, bybit_symbol):
                self._dbg_inc("skip_denied")
                self._reset_persistence(token_key)
                return
            if decimals <= 0 or decimals > 18:
                self._dbg_inc("skip_bad_decimals")
                self._reset_persistence(token_key)
                return

            ob = await self.state.get_orderbook(bybit_symbol)
            if ob is None or not ob.bids or not ob.asks:
                self._dbg_inc("skip_no_ob")
                self._reset_persistence(token_key)
                return
            if ob.age_ms() > self.max_ob_age_ms:
                self._dbg_inc("skip_ob_stale")
                self._reset_persistence(token_key)
                return

            bids, asks = snapshot_book(ob)
            mid, spread_bps = calc_mid_spread(bids, asks)
            if mid is None or spread_bps is None:
                self._dbg_inc("skip_no_mid")
                self._reset_persistence(token_key)
                return
            if spread_bps > self.max_spread_bps:
                self._dbg_inc("skip_spread")
                self._reset_persistence(token_key)
                return

            qp = await self.state.get_quote_pair(token_key)
            now_ms = self.state.now_ms()

            # atomic-ish snapshot of quotes
            async with qp.lock:
                if qp.buy_quote is not None and (now_ms - int(qp.buy_updated_ms or 0)) > self.max_quote_age_ms:
                    self._dbg_inc("skip_stale_buy_quote")
                    qp.buy_quote = None
                    qp.buy_updated_ms = 0

                if qp.sell_quote is not None and (now_ms - int(qp.sell_updated_ms or 0)) > self.max_quote_age_ms:
                    self._dbg_inc("skip_stale_sell_quote")
                    qp.sell_quote = None
                    qp.sell_updated_ms = 0
                    qp.sell_amount_raw = 0

                j_buy = qp.buy_quote
                j_sell = qp.sell_quote
                sell_amount_raw = int(qp.sell_amount_raw or 0)
                sell_updated_ms = int(qp.sell_updated_ms or 0)

            required = self.thresholds.required_profit_usd(self.notional)

            bybit_url = bybit_spot_url(bybit_symbol)
            jup_buy_url = jup_swap_url_by_symbol(bybit_symbol, buy=True)
            jup_sell_url = jup_swap_url_by_symbol(bybit_symbol, buy=False)

            # ---------- A) Jupiter -> Bybit ----------
            a_key = f"{token_key}:A"
            a_valid = False

            if not j_buy:
                self._dbg_inc("A_no_jup_buy_quote")
            elif j_buy.output_mint != mint or j_buy.input_mint != self.stable_mint:
                self._dbg_inc("A_skip_mint_mismatch")
            elif Decimal(str(j_buy.price_impact_pct)) > self.max_dex_price_impact_pct:
                self._dbg_inc("A_skip_dex_impact")
            else:
                token_out = from_raw(int(j_buy.out_amount_raw), decimals)
                if token_out <= 0:
                    self._dbg_inc("A_skip_token_out_le0")
                else:
                    sim_sell = simulate_sell_base(bids, token_out)
                    if sim_sell is None:
                        self._dbg_inc("A_skip_sim_sell_none")
                    else:
                        depth_cov = coverage_pct(Decimal(str(sim_sell.base_out)), token_out)
                        if depth_cov < self.min_depth_coverage_pct:
                            self._dbg_inc("A_skip_depth")
                        elif Decimal(str(sim_sell.slippage_bps)) > self.max_cex_slippage_bps:
                            self._dbg_inc("A_skip_cex_slip")
                        else:
                            stable_out = Decimal(str(sim_sell.quote_out))
                            if stable_out <= 0:
                                self._dbg_inc("A_skip_stable_out_le0")
                            else:
                                jup_implied = self.notional / token_out
                                if not price_ratio_ok(jup_implied, mid, self.max_price_ratio):
                                    self._dbg_inc("A_skip_price_ratio")
                                elif not gross_cap_ok(stable_out, self.notional, self.max_gross_profit_pct):
                                    self._dbg_inc("A_skip_gross_cap")
                                else:
                                    profit = net_profit(stable_out, self.notional, required)
                                    if profit <= 0:
                                        self._dbg_inc("A_skip_profit_le0")
                                    else:
                                        a_valid = True
                                        ready = self.persistence.hit(a_key, True)
                                        if not ready:
                                            self._dbg_inc("A_skip_persistence")
                                        else:
                                            key = f"{token_key}:JUP->BYBIT:{int(self.notional)}"
                                            if not self.dedup.can_send(key, profit):
                                                self._dbg_inc("A_skip_dedup")
                                            else:
                                                net_pct = (profit / self.notional) * Decimal("100")
                                                price_jup = self.notional / token_out
                                                price_bybit = sim_sell.avg_price
                                                text = (
                                                    f"🚨 <b>АРБИТРАЖ</b> • <b>{token_key}</b>\n"
                                                    f"Маршрут: <b>Jupiter → Bybit</b>\n"
                                                    f"Объём: <code>{self.notional:.0f} USDC</code>\n"
                                                    f"Ожидаемый выход: <code>{stable_out:.2f} USDT</code>\n"
                                                    f"Чистая прибыль: <b>{profit:.2f}$</b> (<b>{net_pct:.2f}%</b>)\n"
                                                    f"Комиссии/запас: <code>{required:.2f}$</code>\n"
                                                    f"Цена на Jupiter: <code>{price_jup:.6f}$</code>\n"
                                                    f"Цена на Bybit: <code>{price_bybit:.6f}$</code>"
                                                )
                                                buttons: Buttons = [[
                                                    ("🟢 Купить на Jupiter", jup_buy_url),
                                                    ("🟠 Продать на Bybit", bybit_url),
                                                ]]
                                                sig = Signal(key, token_key, "JUP->BYBIT", profit, self.notional, text, buttons)
                                                self.dedup.mark_sent(key, profit)
                                                res = on_signal(sig)
                                                if asyncio.iscoroutine(res):
                                                    await res

            if not a_valid:
                self.persistence.hit(a_key, False)

            # ---------- B) Bybit -> Jupiter ----------
            b_key = f"{token_key}:B"
            b_valid = False

            sim_buy2 = simulate_buy_with_notional(asks, self.notional)
            if sim_buy2 is None:
                self._dbg_inc("B_skip_sim_buy_none")
            else:
                depth_cov2 = coverage_pct(Decimal(str(sim_buy2.quote_out)), self.notional)
                if depth_cov2 < self.min_depth_coverage_pct:
                    self._dbg_inc("B_skip_depth")
                elif Decimal(str(sim_buy2.slippage_bps)) > self.max_cex_slippage_bps:
                    self._dbg_inc("B_skip_cex_slip")
                else:
                    token_out2 = Decimal(str(sim_buy2.base_out))
                    if token_out2 <= 0:
                        self._dbg_inc("B_skip_token_out_le0")
                    else:
                        expected_raw = to_raw(token_out2, decimals)

                        need_requote = False
                        if not j_sell:
                            self._dbg_inc("B_sell_missing_requote")
                            need_requote = True
                        elif j_sell.input_mint != mint or j_sell.output_mint != self.stable_mint:
                            self._dbg_inc("B_skip_mint_mismatch")
                            need_requote = True
                        elif Decimal(str(j_sell.price_impact_pct)) > self.max_dex_price_impact_pct:
                            self._dbg_inc("B_skip_dex_impact")
                            need_requote = True
                        else:
                            if not sell_updated_ms or (now_ms - sell_updated_ms) > self.max_quote_age_ms:
                                self._dbg_inc("B_sell_stale_requote")
                                need_requote = True
                            elif sell_amount_raw <= 0:
                                self._dbg_inc("B_sell_amount_raw_missing_requote")
                                need_requote = True
                            else:
                                ratio = Decimal(expected_raw) / Decimal(sell_amount_raw)
                                if not (Decimal("0.997") <= ratio <= Decimal("1.003")):
                                    self._dbg_inc("B_amount_mismatch_requote")
                                    need_requote = True

                        if need_requote:
                            now = time.time()
                            last = self._last_b_requote.get(token_key, 0.0)
                            if (now - last) < self.b_requote_cooldown_sec:
                                self._dbg_inc("B_skip_requote_cooldown")
                            else:
                                self._last_b_requote[token_key] = now
                                fresh_sell = await self.jup.quote_exact_in(mint, self.stable_mint, int(expected_raw))
                                if fresh_sell is None:
                                    self._dbg_inc("B_requote_none")
                                elif Decimal(str(fresh_sell.price_impact_pct)) > self.max_dex_price_impact_pct:
                                    self._dbg_inc("B_requote_skip_dex_impact")
                                else:
                                    j_sell = fresh_sell
                                    sell_amount_raw = int(expected_raw)
                                    sell_updated_ms = self.state.now_ms()
                                    async with qp.lock:
                                        qp.sell_quote = fresh_sell
                                        qp.sell_updated_ms = sell_updated_ms
                                        qp.sell_amount_raw = sell_amount_raw

                        if not j_sell:
                            self._dbg_inc("B_skip_no_sell_quote_after_requote")
                        else:
                            stable_out2 = from_raw(int(j_sell.out_amount_raw), self.stable_decimals)
                            if stable_out2 <= 0:
                                self._dbg_inc("B_skip_stable_out_le0")
                            else:
                                jup_implied2 = stable_out2 / token_out2
                                if not price_ratio_ok(jup_implied2, mid, self.max_price_ratio):
                                    self._dbg_inc("B_skip_price_ratio")
                                elif not gross_cap_ok(stable_out2, self.notional, self.max_gross_profit_pct):
                                    self._dbg_inc("B_skip_gross_cap")
                                else:
                                    profit2 = net_profit(stable_out2, self.notional, required)
                                    if profit2 <= 0:
                                        self._dbg_inc("B_skip_profit_le0")
                                    else:
                                        b_valid = True
                                        ready2 = self.persistence.hit(b_key, True)
                                        if not ready2:
                                            self._dbg_inc("B_skip_persistence")
                                        else:
                                            key2 = f"{token_key}:BYBIT->JUP:{int(self.notional)}"
                                            if not self.dedup.can_send(key2, profit2):
                                                self._dbg_inc("B_skip_dedup")
                                            else:
                                                net_pct2 = (profit2 / self.notional) * Decimal("100")
                                                price_bybit2 = sim_buy2.avg_price
                                                price_jup2 = stable_out2 / token_out2
                                                text2 = (
                                                    f"🚨 <b>АРБИТРАЖ</b> • <b>{token_key}</b>\n"
                                                    f"Маршрут: <b>Bybit → Jupiter</b>\n"
                                                    f"Объём: <code>{self.notional:.0f} USDC</code>\n"
                                                    f"Ожидаемый выход: <code>{stable_out2:.2f} USDT</code>\n"
                                                    f"Чистая прибыль: <b>{profit2:.2f}$</b> (<b>{net_pct2:.2f}%</b>)\n"
                                                    f"Комиссии/запас: <code>{required:.2f}$</code>\n"
                                                    f"Цена на Bybit: <code>{price_bybit2:.6f}$</code>\n"
                                                    f"Цена на Jupiter: <code>{price_jup2:.6f}$</code>"
                                                )
                                                buttons2: Buttons = [[
                                                    ("🟠 Купить на Bybit", bybit_url),
                                                    ("🟢 Продать на Jupiter", jup_sell_url),
                                                ]]
                                                sig2 = Signal(key2, token_key, "BYBIT->JUP", profit2, self.notional, text2, buttons2)
                                                self.dedup.mark_sent(key2, profit2)
                                                res2 = on_signal(sig2)
                                                if asyncio.iscoroutine(res2):
                                                    await res2

            if not b_valid:
                self.persistence.hit(b_key, False)

        except Exception as e:
            self._dbg_inc("engine_error")
            log.exception("engine error token=%s: %s", token_key, e)

    async def _run_one_bounded(self, token_key: str, cfg: dict, on_signal: Callable[[Signal], Any]) -> None:
        async with self._engine_sem:
            await self._run_one_token(token_key, cfg, on_signal)

    async def run(self, on_signal: Callable[[Signal], Any]) -> None:
        while not self._stop.is_set():
            if self._exchange_enabled is not None and not self._exchange_enabled.is_set():
                await asyncio.sleep(1)
                continue
            started = time.time()
            items = list(self.token_cfgs.items())
            batch_size = max(1, int(self.engine_concurrency) * int(self._engine_batch_mult))

            for i in range(0, len(items), batch_size):
                chunk = items[i : i + batch_size]
                tasks = [
                    asyncio.create_task(self._run_one_bounded(token_key, cfg, on_signal))
                    for token_key, cfg in chunk
                ]
                await asyncio.gather(*tasks, return_exceptions=True)
                await asyncio.sleep(0)

            elapsed = time.time() - started
            await asyncio.sleep(max(0.0, self.tick_sleep - elapsed))
