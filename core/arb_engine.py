from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass
from decimal import Decimal, getcontext
from typing import Dict, Tuple, Callable, Optional, List, Any

from core.calc import calc_mid_spread, coverage_pct, net_profit, price_ratio_ok, gross_cap_ok
from core.fees import Thresholds
from core.state import MarketState
from core.vwap import simulate_buy_with_notional, simulate_sell_base
from connectors.jupiter import JupiterClient

log = logging.getLogger("engine")
getcontext().prec = 28

BYBIT_UI_BASE = "https://www.bybit.com/en/trade/spot"  # /BASE/QUOTE
JUP_UI_BASE = "https://jup.ag/swap"  # ?inputMint=...&outputMint=...

ButtonRow = List[Tuple[str, str]]
Buttons = List[ButtonRow]

DEFAULT_DENYLIST_SYMBOLS = {
    "XAUT",
    "PAXG",
    "AAPLX",
    "GOOGLX",
    "TSLAX",
    "NVDAX",
    "CRCLX",
    "HOODX",
}

DEFAULT_DENYLIST_REGEX = [
    r"^(1000|10000|100000)[A-Z0-9]+$",  # multiplier symbols like 1000BONK
]


@dataclass
class Signal:
    key: str
    token: str
    direction: str  # "JUP->BYBIT" | "BYBIT->JUP"
    profit_usd: Decimal
    notional_usd: Decimal
    text: str
    buttons: Buttons | None = None

    def to_reply_markup(self) -> dict | None:
        if not self.buttons:
            return None
        return {
            "inline_keyboard": [
                [{"text": title, "url": url} for (title, url) in row]
                for row in self.buttons
            ]
        }


class Dedup:
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


class Persistence:
    def __init__(self, hits: int) -> None:
        self.hits = max(1, int(hits))
        self._cnt: Dict[str, int] = {}

    def hit(self, key: str, ok: bool) -> bool:
        if not ok:
            self._cnt[key] = 0
            return False
        self._cnt[key] = self._cnt.get(key, 0) + 1
        return self._cnt[key] >= self.hits


class SkipStats:
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


class ArbEngine:
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
        denylist_symbols: List[str] | None = None,
        denylist_regex: List[str] | None = None,
        max_ob_age_ms: int = 2000,
        max_quote_age_ms: int | None = None,
    ) -> None:
        self.state = state
        self.jup = jup
        self.thresholds = thresholds

        self.notional = Decimal(str(notional_usd))
        self.stable_mint = str(stable_mint)
        self.stable_decimals = int(stable_decimals)
        self.token_cfgs = token_cfgs

        self.max_cex_slippage_bps = Decimal(str(max_cex_slippage_bps))
        self.max_dex_price_impact_pct = Decimal(str(max_dex_price_impact_pct))

        self.persistence = Persistence(persistence_hits)
        self.dedup = Dedup(cooldown_sec, Decimal(str(min_delta_profit_usd_to_resend)))

        self.tick_sleep = 1 / max(1, int(engine_tick_hz))
        self.jup_poll_interval = float(jupiter_poll_interval_sec)

        # B-branch re-quote throttling (prevents Jupiter spam when sell is missing/mismatched frequently)
        self._last_b_requote: Dict[str, float] = {}
        self.b_requote_cooldown_sec = 2.0

        # Poller backoff (avoid hammering Jupiter for failing tokens)
        self._poll_backoff_until: Dict[str, float] = {}
        self.poll_backoff_on_none_sec = 5.0
        self.poll_backoff_on_err_sec = 10.0

        # Poll batching
        self.poll_concurrency = 24
        self._poll_batch_mult = 4  # batch = concurrency * mult
        self.poll_jitter_ratio = 0.15  # 15% jitter

        # Quotes can go stale when token list grows; enforce freshness in the engine.
        # Default: max(3x poll interval, 5000ms)
        if max_quote_age_ms is None:
            self.max_quote_age_ms = int(max(5000, self.jup_poll_interval * 3 * 1000))
        else:
            self.max_quote_age_ms = int(max_quote_age_ms)

        self.max_price_ratio = Decimal(str(price_ratio_max if price_ratio_max is not None else "3"))
        self.max_gross_profit_pct = Decimal(str(gross_profit_cap_pct if gross_profit_cap_pct is not None else "10"))
        self.max_spread_bps = Decimal(str(max_spread_bps if max_spread_bps is not None else "50"))
        self.min_depth_coverage_pct = Decimal(str(min_depth_coverage_pct if min_depth_coverage_pct is not None else "98"))
        self.max_ob_age_ms = int(max_ob_age_ms)

        deny_syms = {s.upper() for s in (denylist_symbols or [])}
        deny_syms |= DEFAULT_DENYLIST_SYMBOLS
        self._denylist_symbols = deny_syms

        self._denylist_regex: List[re.Pattern[str]] = []
        for rx in (denylist_regex or []):
            try:
                self._denylist_regex.append(re.compile(rx, re.IGNORECASE))
            except re.error:
                log.warning("bad denylist regex: %s", rx)
        for rx in DEFAULT_DENYLIST_REGEX:
            self._denylist_regex.append(re.compile(rx, re.IGNORECASE))

        self._skip_stats = SkipStats(window_sec=30)
        self._stop = asyncio.Event()

        # Engine bounded concurrency (for large token lists)
        self.engine_concurrency = 64  # safe default; can be tuned later
        self._engine_sem = asyncio.Semaphore(self.engine_concurrency)
        self._engine_batch_mult = 4  # batch = concurrency * mult

    async def stop(self) -> None:
        self._stop.set()

    def drain_debug_stats(self) -> Optional[Dict[str, int]]:
        return self._skip_stats.flush_if_due()

    # ---------- utils ----------
    def _dbg_inc(self, k: str, n: int = 1) -> None:
        self._skip_stats.inc(k, n)

    def _poll_allowed(self, token_key: str) -> bool:
        return time.time() >= self._poll_backoff_until.get(token_key, 0.0)

    def _poll_backoff(self, token_key: str, sec: float) -> None:
        self._poll_backoff_until[token_key] = time.time() + float(sec)

    def _to_raw(self, amount: Decimal, decimals: int) -> int:
        scale = Decimal(10) ** Decimal(decimals)
        return int((Decimal(amount) * scale).to_integral_value(rounding="ROUND_DOWN"))

    def _from_raw(self, raw: int, decimals: int) -> Decimal:
        scale = Decimal(10) ** Decimal(decimals)
        return Decimal(raw) / scale

    def _is_pump_mint(self, mint: str) -> bool:
        try:
            return str(mint).lower().endswith("pump")
        except Exception:
            return False

    def _normalize_bybit_base(self, bybit_symbol: str) -> str:
        s = (bybit_symbol or "").upper().strip()
        for q in ("USDT", "USDC", "USD"):
            if s.endswith(q):
                return s[: -len(q)]
        return s

    def _is_denied_token(self, token_key: str, bybit_symbol: str) -> bool:
        base = self._normalize_bybit_base(bybit_symbol)
        candidates = [token_key, base, bybit_symbol]
        for c in candidates:
            if not c:
                continue
            u = str(c).upper()
            if u in self._denylist_symbols:
                return True
            for rx in self._denylist_regex:
                if rx.search(u):
                    return True
        return False

    def _snapshot_book(self, ob) -> tuple[list[tuple[Decimal, Decimal]], list[tuple[Decimal, Decimal]]]:
        """
        Take an atomic-ish snapshot of the current orderbook levels for this tick.
        We copy dicts first, then sort locally once.
        """
        try:
            bids_map = ob.bids.copy()
            asks_map = ob.asks.copy()
        except Exception:
            return [], []
        bids = sorted(bids_map.items(), key=lambda x: x[0], reverse=True)
        asks = sorted(asks_map.items(), key=lambda x: x[0])
        return bids, asks

    def _reset_persistence(self, token_key: str) -> None:
        self.persistence.hit(f"{token_key}:A", False)
        self.persistence.hit(f"{token_key}:B", False)

    def _bybit_spot_url(self, bybit_symbol: str) -> str:
        s = (bybit_symbol or "").upper().strip()
        quote = None
        base = None
        for q in ("USDT", "USDC", "USD"):
            if s.endswith(q):
                quote = q
                base = s[: -len(q)]
                break
        if not quote or not base:
            return f"{BYBIT_UI_BASE}/{s}"
        return f"{BYBIT_UI_BASE}/{base}/{quote}"

    def _jup_swap_url(self, input_mint: str, output_mint: str) -> str:
        return f"{JUP_UI_BASE}?inputMint={input_mint}&outputMint={output_mint}"

    # ---------- poller ----------
    async def _poll_one_token(self, token_key: str, cfg: dict) -> None:
        try:
            mint = str(cfg.get("mint", ""))
            bybit_symbol = str(cfg.get("bybit_symbol", ""))
            decimals = int(cfg.get("decimals", 0) or 0)

            if self._is_pump_mint(mint):
                self._dbg_inc("poll_skip_pump_mint")
                return
            if self._is_denied_token(token_key, bybit_symbol):
                self._dbg_inc("poll_skip_denied")
                return
            if decimals <= 0 or decimals > 18:
                self._dbg_inc("poll_skip_bad_decimals")
                return

            ob = await self.state.get_orderbook(bybit_symbol)
            if ob is None or not ob.asks or not ob.bids:
                self._dbg_inc("poll_skip_no_ob")
                return
            if ob.age_ms() > self.max_ob_age_ms:
                self._dbg_inc("poll_skip_ob_stale")
                return

            bids, asks = self._snapshot_book(ob)
            _mid, spread_bps = calc_mid_spread(bids, asks)
            if spread_bps is None:
                self._dbg_inc("poll_skip_no_spread")
                return
            if spread_bps > self.max_spread_bps:
                self._dbg_inc("poll_skip_spread")
                return

            qp = await self.state.get_quote_pair(token_key)

            stable_raw = self._to_raw(self.notional, self.stable_decimals)
            buy_q = await self.jup.quote_exact_in(self.stable_mint, mint, stable_raw)

            if buy_q is not None:
                async with qp.lock:
                    qp.buy_quote = buy_q
                    qp.buy_updated_ms = self.state.now_ms()
                self._poll_backoff_until.pop(token_key, None)
            else:
                self._dbg_inc("poll_buy_quote_none")
                self._poll_backoff(token_key, self.poll_backoff_on_none_sec)

            # NOTE: we do NOT poll sell quotes anymore.
            # Sell quote is fetched on-demand inside B) branch when needed.

        except Exception as e:
            self._poll_backoff(token_key, self.poll_backoff_on_err_sec)
            self._dbg_inc("poll_error")
            log.exception("quote_poller error token=%s: %s", token_key, e)

    async def quote_poller(self) -> None:
        sem = asyncio.Semaphore(int(self.poll_concurrency))

        async def runner(token_key: str, cfg: dict) -> None:
            if not self._poll_allowed(token_key):
                self._dbg_inc("poll_skip_backoff")
                return
            async with sem:
                await self._poll_one_token(token_key, cfg)

        while not self._stop.is_set():
            started = time.time()

            items = list(self.token_cfgs.items())
            batch_size = max(1, int(self.poll_concurrency) * int(self._poll_batch_mult))

            for i in range(0, len(items), batch_size):
                chunk = items[i : i + batch_size]
                tasks = [asyncio.create_task(runner(token_key, cfg)) for token_key, cfg in chunk]
                await asyncio.gather(*tasks, return_exceptions=True)
                await asyncio.sleep(0)

            elapsed = time.time() - started

            jitter = self.jup_poll_interval * float(self.poll_jitter_ratio)
            sleep_for = max(0.0, self.jup_poll_interval - elapsed)
            sleep_for += (jitter * (time.time() % 1.0))
            await asyncio.sleep(sleep_for)

    # ---------- engine ----------
    async def _run_one_token(self, token_key: str, cfg: dict, on_signal: Callable[[Signal], Any]) -> None:
        try:
            mint = str(cfg.get("mint", ""))
            bybit_symbol = str(cfg.get("bybit_symbol", ""))
            decimals = int(cfg.get("decimals", 0) or 0)

            if self._is_pump_mint(mint):
                self._dbg_inc("skip_pump_mint")
                self._reset_persistence(token_key)
                return
            if self._is_denied_token(token_key, bybit_symbol):
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

            bids, asks = self._snapshot_book(ob)
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

            # ===== snapshot quotes under lock (single source of truth) =====
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

            # ===== constants for this tick =====
            required = self.thresholds.required_profit_usd(self.notional)
            bybit_url = self._bybit_spot_url(bybit_symbol)
            jup_buy_url = self._jup_swap_url(self.stable_mint, mint)
            jup_sell_url = self._jup_swap_url(mint, self.stable_mint)

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
                token_out = self._from_raw(int(j_buy.out_amount_raw), decimals)
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
                                            sig_key = f"{token_key}:JUP->BYBIT:{int(self.notional)}"
                                            if not self.dedup.can_send(sig_key, profit):
                                                self._dbg_inc("A_skip_dedup")
                                            else:
                                                net_pct = (profit / self.notional) * Decimal("100")
                                                text = (
                                                    f"🚨 <b>АРБИТРАЖ</b> • <b>{token_key}</b>\n"
                                                    f"Маршрут: <b>Jupiter → Bybit</b>\n"
                                                    f"Объём: <code>{self.notional:.0f} USDC</code>\n"
                                                    f"Ожидаемый выход: <code>{stable_out:.2f} USDT</code>\n"
                                                    f"Чистая прибыль: <b>{profit:.2f}$</b> (<b>{net_pct:.2f}%</b>)\n"
                                                    f"Комиссии/запас: <code>{required:.2f}$</code>\n"
                                                    f"DEX impact: <code>{Decimal(str(j_buy.price_impact_pct)):.4f}%</code> • "
                                                    f"CEX slip: <code>{Decimal(str(sim_sell.slippage_bps)):.2f} bps</code>\n"
                                                    f"Spread: <code>{spread_bps:.1f} bps</code> • Depth: <code>{depth_cov:.1f}%</code>\n"
                                                    f"Возраст стакана: <code>{ob.age_ms()} ms</code>"
                                                )

                                                buttons: Buttons = [[
                                                    ("🟢 Купить на Jupiter", jup_buy_url),
                                                    ("🟠 Продать на Bybit", bybit_url),
                                                ]]

                                                sig = Signal(sig_key, token_key, "JUP->BYBIT", profit, self.notional, text, buttons)
                                                res = on_signal(sig)
                                                if asyncio.iscoroutine(res):
                                                    await res
                                                self.dedup.mark_sent(sig_key, profit)

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
                        expected_raw = self._to_raw(token_out2, decimals)

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
                                        qp.sell_amount_raw = sell_amount_raw
                                        qp.sell_updated_ms = sell_updated_ms

                        if not j_sell:
                            self._dbg_inc("B_skip_no_sell_quote_after_requote")
                        else:
                            stable_out2 = self._from_raw(int(j_sell.out_amount_raw), self.stable_decimals)
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
                                            sig_key2 = f"{token_key}:BYBIT->JUP:{int(self.notional)}"
                                            if not self.dedup.can_send(sig_key2, profit2):
                                                self._dbg_inc("B_skip_dedup")
                                            else:
                                                net_pct2 = (profit2 / self.notional) * Decimal("100")
                                                text2 = (
                                                    f"🚨 <b>АРБИТРАЖ</b> • <b>{token_key}</b>\n"
                                                    f"Маршрут: <b>Bybit → Jupiter</b>\n"
                                                    f"Объём: <code>{self.notional:.0f} USDC</code>\n"
                                                    f"Ожидаемый выход: <code>{stable_out2:.2f} USDT</code>\n"
                                                    f"Чистая прибыль: <b>{profit2:.2f}$</b> (<b>{net_pct2:.2f}%</b>)\n"
                                                    f"Комиссии/запас: <code>{required:.2f}$</code>\n"
                                                    f"DEX impact: <code>{Decimal(str(j_sell.price_impact_pct)):.4f}%</code> • "
                                                    f"CEX slip: <code>{Decimal(str(sim_buy2.slippage_bps)):.2f} bps</code>\n"
                                                    f"Spread: <code>{spread_bps:.1f} bps</code> • Depth: <code>{depth_cov2:.1f}%</code>\n"
                                                    f"Возраст стакана: <code>{ob.age_ms()} ms</code>"
                                                )

                                                buttons2: Buttons = [[
                                                    ("🟠 Купить на Bybit", bybit_url),
                                                    ("🟢 Продать на Jupiter", jup_sell_url),
                                                ]]

                                                sig2 = Signal(sig_key2, token_key, "BYBIT->JUP", profit2, self.notional, text2, buttons2)
                                                res2 = on_signal(sig2)
                                                if asyncio.iscoroutine(res2):
                                                    await res2
                                                self.dedup.mark_sent(sig_key2, profit2)

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
