from __future__ import annotations

import argparse
import asyncio
import logging
import os
import time
from decimal import Decimal
from typing import Any

import aiohttp
import yaml
from dotenv import load_dotenv

from connectors.bybit_ws import BybitWS
from connectors.jupiter import JupiterClient
from core.arb_engine import ArbEngine
from core.config import AppConfig
from core.fees import Thresholds
from core.quarantine import QuarantineEntry, load_quarantine, now_ts, prune_expired, save_quarantine
from core.state import MarketState
from core.runtime_settings import RuntimeSettings, load_runtime_settings, save_runtime_settings
from notifier.commands import run_settings_command_handler
from notifier.telegram import TelegramNotifier
from utils.log import setup_logging

log = logging.getLogger("app")


def chunked(lst, n: int):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


class BybitWSCluster:
    """
    Manages N BybitWS clients with sharding, supports dynamic resubscribe.
    """

    def __init__(
        self,
        ws_url: str,
        depth: int,
        ping_interval_sec: int,
        on_orderbook_message,
        max_symbols_per_ws: int = 100,
    ) -> None:
        self.ws_url = ws_url
        self.depth = depth
        self.ping_interval_sec = ping_interval_sec
        self.on_orderbook_message = on_orderbook_message
        self.max_symbols_per_ws = max(1, int(max_symbols_per_ws))

        self._clients: list[BybitWS] = []
        self._tasks: list[asyncio.Task] = []
        self._lock = asyncio.Lock()

    @property
    def clients(self) -> list[BybitWS]:
        return list(self._clients)

    async def start(self, symbols: list[str]) -> None:
        await self.update_symbols(symbols)

    async def stop(self) -> None:
        async with self._lock:
            for c in self._clients:
                await c.stop()
            for t in self._tasks:
                t.cancel()
            self._clients.clear()
            self._tasks.clear()

    async def update_symbols(self, symbols: list[str]) -> None:
        shards = list(chunked(list(symbols), self.max_symbols_per_ws))

        async with self._lock:
            while len(self._clients) < len(shards):
                c = BybitWS(
                    ws_url=self.ws_url,
                    symbols=[],
                    depth=self.depth,
                    ping_interval_sec=self.ping_interval_sec,
                    on_orderbook_message=self.on_orderbook_message,
                    subscribe_batch=5,
                    subscribe_ack_timeout=12.0,
                )

                self._clients.append(c)
                self._tasks.append(asyncio.create_task(c.run(), name=f"bybit_ws_{len(self._clients)}"))

            for idx, shard in enumerate(shards):
                self._clients[idx].set_symbols(shard)

            for idx in range(len(shards), len(self._clients)):
                self._clients[idx].set_symbols([])

        log.info("WS cluster updated: clients=%d symbols=%d", len(self._clients), len(symbols))


async def main(cfg_path: str) -> None:
    load_dotenv()

    with open(cfg_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    cfg = AppConfig.model_validate(raw)
    setup_logging(cfg.logging.level)

    # ---- paths ----
    cfg_dir = os.path.dirname(os.path.abspath(cfg_path))
    settings_path = os.path.join(cfg_dir, "settings.json")
    api_db_path = os.path.join(cfg_dir, "request_stats.db")

    # ---- API (stats for Mini App) ----
    from api import db as api_db

    api_db.log = log
    api_db.init(api_db_path)

    quarantine_path = raw.get("quarantine_path") if isinstance(raw, dict) else None
    if not quarantine_path:
        quarantine_path = "quarantine.yaml"
    if isinstance(quarantine_path, str) and quarantine_path and not os.path.isabs(quarantine_path):
        quarantine_path = os.path.join(cfg_dir, quarantine_path)

    # ---- source-of-truth copies (to restore on unquarantine) ----
    full_symbols: list[str] = list(cfg.bybit.symbols)
    full_tokens = dict(cfg.trading.tokens)  # token_key -> token_cfg model
    base_denylist = list(cfg.filters.denylist_symbols or [])

    # mint -> bybit_symbol (для JUP авто-санитизации)
    mint_to_symbol = {t.mint: t.bybit_symbol for t in full_tokens.values() if getattr(t, "mint", None)}
    stable_mint = cfg.trading.stable.mint

    # ---- runtime quarantine state ----
    q_lock = asyncio.Lock()
    quarantined_set: set[str] = set()
    last_quarantine_write: dict[str, int] = {}  # anti-spam per symbol (sec)

    def rebuild_denylist_inplace() -> None:
        merged = sorted(set(base_denylist) | set(quarantined_set))
        if cfg.filters.denylist_symbols is None:
            cfg.filters.denylist_symbols = merged
        else:
            cfg.filters.denylist_symbols.clear()
            cfg.filters.denylist_symbols.extend(merged)

    def apply_quarantine_to_cfg() -> None:
        cfg.bybit.symbols = [s for s in full_symbols if s not in quarantined_set]
        cfg.trading.tokens = {k: v for k, v in full_tokens.items() if v.bybit_symbol not in quarantined_set}
        rebuild_denylist_inplace()

    # ---- initial quarantine load/prune ----
    q0 = prune_expired(load_quarantine(quarantine_path))

    # ---- BAD_TOKEN_CFG validator (after q0 exists) ----
    bad_added = False
    for token_key, t in list(full_tokens.items()):
        ok = True
        if not getattr(t, "mint", None):
            ok = False
        if getattr(t, "decimals", None) is None:
            ok = False
        if not getattr(t, "bybit_symbol", None):
            ok = False

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
        save_quarantine(quarantine_path, q0)

    quarantined_set = set(q0.keys())
    apply_quarantine_to_cfg()

    if quarantined_set:
        log.warning("Quarantine enabled: %d symbols disabled. File=%s", len(quarantined_set), quarantine_path)
    else:
        log.info("Quarantine empty/disabled. File=%s", quarantine_path)

    # ---- env ----
    tg_token = os.environ.get("TG_BOT_TOKEN")
    jup_key = os.environ.get("JUP_API_KEY")
    if not tg_token:
        raise RuntimeError("Нет TG_BOT_TOKEN в .env")
    if not jup_key:
        raise RuntimeError("Нет JUP_API_KEY в .env")

    state = MarketState()

    # Runtime settings (config defaults + settings.json overrides)
    settings_defaults = RuntimeSettings(
        bybit_taker_fee_bps=float(cfg.thresholds.bybit_taker_fee_bps),
        solana_tx_fee_usd=float(cfg.thresholds.solana_tx_fee_usd),
        latency_buffer_bps=float(cfg.thresholds.latency_buffer_bps),
        usdt_usdc_buffer_bps=float(cfg.thresholds.usdt_usdc_buffer_bps),
        min_profit_usd=float(cfg.thresholds.min_profit_usd),
        notional_usd=float(cfg.trading.notional_usd),
        max_cex_slippage_bps=float(cfg.filters.max_cex_slippage_bps),
        max_dex_price_impact_pct=float(cfg.filters.max_dex_price_impact_pct),
        persistence_hits=int(cfg.filters.persistence_hits),
        cooldown_sec=int(cfg.filters.cooldown_sec),
        min_delta_profit_usd_to_resend=float(cfg.filters.min_delta_profit_usd_to_resend),
        price_ratio_max=float(cfg.filters.price_ratio_max),
        gross_profit_cap_pct=float(cfg.filters.gross_profit_cap_pct),
        max_spread_bps=float(cfg.filters.max_spread_bps),
        min_depth_coverage_pct=float(cfg.filters.min_depth_coverage_pct),
        engine_tick_hz=int(cfg.runtime.engine_tick_hz),
        jupiter_poll_interval_sec=float(cfg.jupiter.poll_interval_sec),
        stale_ttl_sec=int(cfg.notifier.stale_ttl_sec),
        delete_stale=bool(cfg.notifier.delete_stale),
    )
    settings = load_runtime_settings(settings_path, settings_defaults)

    async with aiohttp.ClientSession() as session:
        tg = TelegramNotifier(
            session,
            tg_token,
            cfg.telegram.chat_id,
            cfg.telegram.thread_id,
            edit_min_interval_sec=cfg.notifier.edit_min_interval_sec,
            edit_mode=cfg.notifier.edit_mode,
            stale_ttl_sec=float(settings.stale_ttl_sec),
            delete_stale=settings.delete_stale,
        )

        thresholds = Thresholds(
            bybit_taker_fee_bps=Decimal(str(settings.bybit_taker_fee_bps)),
            solana_tx_fee_usd=Decimal(str(settings.solana_tx_fee_usd)),
            latency_buffer_bps=Decimal(str(settings.latency_buffer_bps)),
            usdt_usdc_buffer_bps=Decimal(str(settings.usdt_usdc_buffer_bps)),
            min_profit_usd=Decimal(str(settings.min_profit_usd)),
        )

        # token_cfgs passed into engine (keep dict identity stable)
        token_cfgs: dict[str, dict[str, Any]] = {}
        for token_key, t in cfg.trading.tokens.items():
            token_cfgs[token_key] = {"bybit_symbol": t.bybit_symbol, "mint": t.mint, "decimals": t.decimals}

        def rebuild_token_cfgs_inplace() -> None:
            token_cfgs.clear()
            for token_key, t in cfg.trading.tokens.items():
                token_cfgs[token_key] = {"bybit_symbol": t.bybit_symbol, "mint": t.mint, "decimals": t.decimals}

        async def quarantine_add(symbol: str, reason: str, ttl_sec: int) -> None:
            if not symbol:
                return

            now = now_ts()

            # per-symbol spam guard: 1 quarantine write / 60 sec
            last = last_quarantine_write.get(symbol, 0)
            if (now - last) < 60:
                return
            last_quarantine_write[symbol] = now

            until = now + int(ttl_sec)

            async with q_lock:
                q = prune_expired(load_quarantine(quarantine_path))
                prev = q.get(symbol)

                # if already quarantined far enough — don't rewrite file
                if prev is not None and prev.until_ts > (now + 1800):
                    quarantined_set.add(symbol)
                    apply_quarantine_to_cfg()
                    rebuild_token_cfgs_inplace()
                    return

                q[symbol] = QuarantineEntry(reason=reason, until_ts=until)
                save_quarantine(quarantine_path, q)

                quarantined_set.add(symbol)
                apply_quarantine_to_cfg()
                rebuild_token_cfgs_inplace()

            log.warning("AUTO-QUARANTINE: %s reason=%s ttl=%ds file=%s", symbol, reason, ttl_sec, quarantine_path)

        # -----------------------------
        # Jupiter auto-sanitization
        # -----------------------------
        jup_bad_counts: dict[str, int] = {}
        jup_bad_last_ts: dict[str, float] = {}

        JUP_BAD_WINDOW_SEC = 20 * 60          # 20 минут окно
        JUP_NOT_TRADABLE_TTL_SEC = 24 * 3600  # 24 часа карантин
        JUP_NO_ROUTE_TTL_SEC = 2 * 3600       # 2 часа карантин

        JUP_NOT_TRADABLE_HITS = 1             # сразу выкидываем
        JUP_NO_ROUTE_HITS = 30                # только если стабильно нет маршрута

        # rate-limit quarantines per minute (global for jupiter skips)
        jup_qrate = {"ts": 0.0, "cnt": 0}
        JUP_MAX_QUARANTINES_PER_MIN = 10

        def allow_jup_quarantine() -> bool:
            now2 = time.time()
            if (now2 - jup_qrate["ts"]) > 60:
                jup_qrate["ts"] = now2
                jup_qrate["cnt"] = 0
            if jup_qrate["cnt"] >= JUP_MAX_QUARANTINES_PER_MIN:
                return False
            jup_qrate["cnt"] += 1
            return True

        async def on_jup_skip(code: str, input_mint: str, output_mint: str, bad_mint: str, msg: str) -> None:
            # determine problematic mint
            m = bad_mint or ""
            if not m:
                if output_mint and output_mint != stable_mint:
                    m = output_mint
                elif input_mint and input_mint != stable_mint:
                    m = input_mint

            if not m or m == stable_mint:
                return

            symbol = mint_to_symbol.get(m)
            if not symbol:
                return

            now = time.time()
            last = jup_bad_last_ts.get(m, 0.0)
            if (now - last) > JUP_BAD_WINDOW_SEC:
                jup_bad_counts[m] = 0
            jup_bad_last_ts[m] = now
            jup_bad_counts[m] = jup_bad_counts.get(m, 0) + 1

            if code == "TOKEN_NOT_TRADABLE" and jup_bad_counts[m] >= JUP_NOT_TRADABLE_HITS:
                if not allow_jup_quarantine():
                    return
                await quarantine_add(symbol, reason="JUP_TOKEN_NOT_TRADABLE", ttl_sec=JUP_NOT_TRADABLE_TTL_SEC)
                return

            if code == "COULD_NOT_FIND_ANY_ROUTE" and jup_bad_counts[m] >= JUP_NO_ROUTE_HITS:
                if not allow_jup_quarantine():
                    return
                await quarantine_add(symbol, reason="JUP_NO_ROUTE", ttl_sec=JUP_NO_ROUTE_TTL_SEC)
                return

        def _record_jupiter(source: str, count: int = 1) -> None:
            try:
                from api.db import record

                record(source, count)
            except Exception:
                pass

        jup = JupiterClient(
            session=session,
            base_url=cfg.jupiter.base_url,
            api_key=jup_key,
            timeout_sec=cfg.jupiter.timeout_sec,
            slippage_bps=cfg.jupiter.slippage_bps,
            restrict_intermediate_tokens=cfg.jupiter.restrict_intermediate_tokens,
            max_accounts=cfg.jupiter.max_accounts,
            rps=cfg.rate_limits.jupiter_rps,
            concurrency=cfg.rate_limits.jupiter_concurrency,
            max_retries=cfg.rate_limits.jupiter_max_retries,
            on_skip=on_jup_skip,
            on_request=_record_jupiter,
        )

        engine = ArbEngine(
            state=state,
            jup=jup,
            thresholds=thresholds,
            notional_usd=Decimal(str(settings.notional_usd)),
            stable_mint=cfg.trading.stable.mint,
            stable_decimals=cfg.trading.stable.decimals,
            token_cfgs=token_cfgs,
            max_cex_slippage_bps=Decimal(str(settings.max_cex_slippage_bps)),
            max_dex_price_impact_pct=Decimal(str(settings.max_dex_price_impact_pct)),
            persistence_hits=int(settings.persistence_hits),
            cooldown_sec=int(settings.cooldown_sec),
            min_delta_profit_usd_to_resend=Decimal(str(settings.min_delta_profit_usd_to_resend)),
            engine_tick_hz=int(settings.engine_tick_hz),
            jupiter_poll_interval_sec=float(settings.jupiter_poll_interval_sec),
            price_ratio_max=Decimal(str(settings.price_ratio_max)),
            gross_profit_cap_pct=Decimal(str(settings.gross_profit_cap_pct)),
            max_spread_bps=Decimal(str(settings.max_spread_bps)),
            min_depth_coverage_pct=Decimal(str(settings.min_depth_coverage_pct)),
            denylist_symbols=cfg.filters.denylist_symbols,
            denylist_regex=cfg.filters.denylist_regex,
        )

        async def on_signal(sig):
            reply_markup = sig.to_reply_markup() if hasattr(sig, "to_reply_markup") else None
            await tg.upsert(sig.key, sig.text, reply_markup=reply_markup)

        stats_bybit_sample = max(1, int(getattr(cfg.runtime, "stats_bybit_sample", 1)))
        _bybit_record_counter = [0]  # mutable for closure

        def _record_bybit() -> None:
            try:
                from api.db import record

                _bybit_record_counter[0] += 1
                if _bybit_record_counter[0] >= stats_bybit_sample:
                    record("bybit", _bybit_record_counter[0])
                    _bybit_record_counter[0] = 0
            except Exception:
                pass

        async def on_ob(msg: dict) -> None:
            _record_bybit()
            topic = str(msg.get("topic", "") or "")
            typ = str(msg.get("type", "") or "")
            data = msg.get("data")

            if data is None:
                return

            if isinstance(data, list):
                parts = [x for x in data if isinstance(x, dict)]
            elif isinstance(data, dict):
                parts = [data]
            else:
                return

            now_ms = state.now_ms()

            for it in parts:
                symbol = it.get("s") or it.get("symbol")
                if not symbol and topic:
                    symbol = topic.split(".")[-1]
                if not symbol:
                    continue

                async with q_lock:
                    if symbol in quarantined_set:
                        continue

                bids = it.get("b") or it.get("bids") or []
                asks = it.get("a") or it.get("asks") or []

                if (not bids and not asks) and isinstance(it.get("data"), dict):
                    inner = it["data"]
                    bids = inner.get("b") or inner.get("bids") or []
                    asks = inner.get("a") or inner.get("asks") or []

                ob = await state.upsert_orderbook(symbol)
                if typ == "snapshot":
                    ob.apply_snapshot(bids, asks, now_ms, now_ms)
                elif typ == "delta":
                    ob.apply_delta(bids, asks, now_ms, now_ms)
                else:
                    if bids or asks:
                        ob.apply_delta(bids, asks, now_ms, now_ms)

        # ---- WS cluster ----
        ws_cluster = BybitWSCluster(
            ws_url=cfg.bybit.ws_url,
            depth=cfg.bybit.depth,
            ping_interval_sec=cfg.bybit.ping_interval_sec,
            on_orderbook_message=on_ob,
            max_symbols_per_ws=100,
        )

        await ws_cluster.start(cfg.bybit.symbols)
        log.info("Bybit WS clients=%d, symbols=%d", len(ws_cluster.clients), len(cfg.bybit.symbols))

        # ---- quarantine sync loop ----
        async def quarantine_sync_loop(poll_sec: float = 10.0) -> None:
            last_mtime = 0.0
            while True:
                try:
                    mtime = os.path.getmtime(quarantine_path)
                except FileNotFoundError:
                    mtime = 0.0

                changed = mtime > last_mtime
                last_mtime = max(last_mtime, mtime)

                if changed:
                    try:
                        q = load_quarantine(quarantine_path)
                        q2 = prune_expired(q)
                        if q2.keys() != q.keys():
                            save_quarantine(quarantine_path, q2)
                        new_set = set(q2.keys())
                    except Exception:
                        log.exception("Failed to sync quarantine file=%s", quarantine_path)
                        new_set = set()

                    async with q_lock:
                        before = set(quarantined_set)
                        added = new_set - before
                        removed = before - new_set

                        if added or removed:
                            quarantined_set.clear()
                            quarantined_set.update(new_set)

                            apply_quarantine_to_cfg()
                            rebuild_token_cfgs_inplace()

                            await ws_cluster.update_symbols(cfg.bybit.symbols)

                            log.warning(
                                "Quarantine sync: added=%d removed=%d active=%d quarantined=%d",
                                len(added),
                                len(removed),
                                len(cfg.bybit.symbols),
                                len(quarantined_set),
                            )

                await asyncio.sleep(poll_sec)

        # ---- status loop (sampling для снижения нагрузки при 300+ символах) ----
        _n = len(cfg.bybit.symbols)
        status_sample_step = max(1, min(10, _n // 50)) if _n > 100 else 1

        async def status_loop():
            while True:
                symbols = list(cfg.bybit.symbols)
                total = len(symbols)

                fresh_cnt = 0
                non_empty_cnt = 0
                sampled_n = 0
                FRESH_MS = 2000

                sample_syms = symbols[:5]
                sample_parts: list[str] = []

                for i in range(0, total, status_sample_step):
                    sym = symbols[i]
                    ob = await state.get_orderbook(sym)
                    sampled_n += 1
                    if ob is not None and ob.bids and ob.asks:
                        non_empty_cnt += 1
                        if ob.age_ms() <= FRESH_MS:
                            fresh_cnt += 1

                if sampled_n > 0 and status_sample_step > 1:
                    scale = total / sampled_n
                    non_empty_cnt = min(total, int(non_empty_cnt * scale))
                    fresh_cnt = min(total, int(fresh_cnt * scale))

                for sym in sample_syms:
                    ob = await state.get_orderbook(sym)
                    if ob is None or not ob.bids or not ob.asks:
                        sample_parts.append(f"{sym} OB empty")
                    else:
                        best_bid = max(ob.bids.keys())
                        best_ask = min(ob.asks.keys())
                        sample_parts.append(f"{sym} bid={best_bid} ask={best_ask} age={ob.age_ms()}ms")

                stats = engine.drain_debug_stats()
                if stats is not None:
                    if stats:
                        top = sorted(stats.items(), key=lambda kv: kv[1], reverse=True)[:5]
                        skip_text = ", ".join([f"{k}={v}" for k, v in top])
                    else:
                        skip_text = "none"
                else:
                    skip_text = "n/a"

                status_interval = float(getattr(cfg.runtime, "status_interval_sec", 15))
                log.info(
                    "[STATUS] active=%d | quarantined=%d | OB non-empty %d/%d | OB fresh %d/%d (<=%dms) | skips(30s): %s | sample: %s",
                    total,
                    len(quarantined_set),
                    non_empty_cnt,
                    total,
                    fresh_cnt,
                    total,
                    FRESH_MS,
                    skip_text,
                    " | ".join(sample_parts),
                )
                await asyncio.sleep(status_interval)

        async def stale_loop():
            while True:
                await tg.expire_stale()
                await asyncio.sleep(5)

        async def ws_health_loop():
            timeout_sec = float(cfg.runtime.ws_snapshot_timeout_sec)
            if timeout_sec <= 0:
                return

            start_ts = time.time()
            AUTO_Q_TTL_SEC = 6 * 3600  # 6h

            while True:
                await asyncio.sleep(max(5.0, timeout_sec / 2))
                if (time.time() - start_ts) < timeout_sec:
                    continue

                now_ms = state.now_ms()
                symbols = list(cfg.bybit.symbols)

                stale_syms: list[str] = []
                for sym in symbols:
                    ob = await state.get_orderbook(sym)
                    last_msg_ms = 0
                    if ob is not None:
                        last_msg_ms = int(ob.last_cts_ms or ob.last_update_ms or ob.last_snapshot_ms or 0)
                    if last_msg_ms <= 0 or (now_ms - last_msg_ms) > timeout_sec * 1000:
                        stale_syms.append(sym)

                if stale_syms:
                    for sym in stale_syms[:50]:
                        await quarantine_add(sym, reason="WS_STALE", ttl_sec=AUTO_Q_TTL_SEC)

                    log.warning(
                        "[HEALTH] stale snapshots %d/%d (>%.0fs) sample=%s",
                        len(stale_syms),
                        len(symbols),
                        timeout_sec,
                        ", ".join(stale_syms[:5]),
                    )

        commands_stop = asyncio.Event()

        def apply_settings_reload(s: RuntimeSettings) -> None:
            engine.reload_settings(s)
            tg.update_stale_settings(s.stale_ttl_sec, s.delete_stale)

        api_port = int(os.environ.get("PORT") or os.environ.get("API_PORT") or "8080")

        tasks: list[asyncio.Task] = [
            asyncio.create_task(
                __import__("api.server", fromlist=["run_server"]).run_server(host="0.0.0.0", port=api_port),
                name="api_server",
            ),
            asyncio.create_task(quarantine_sync_loop(), name="quarantine_sync"),
            asyncio.create_task(engine.quote_poller(), name="jup_poller"),
            asyncio.create_task(engine.run(on_signal), name="arb_engine"),
            asyncio.create_task(status_loop(), name="status"),
            asyncio.create_task(stale_loop(), name="tg_stale"),
            asyncio.create_task(
                run_settings_command_handler(
                    session=session,
                    bot_token=tg_token,
                    chat_id=cfg.telegram.chat_id,
                    thread_id=cfg.telegram.thread_id,
                    settings=settings,
                    settings_path=settings_path,
                    on_reload=apply_settings_reload,
                    stop_event=commands_stop,
                    web_app_url=getattr(cfg.telegram, "web_app_url", None),
                    pinned_message_text=getattr(cfg.telegram, "pinned_message_text", None),
                ),
                name="settings_cmd",
            ),
        ]

        if cfg.runtime.ws_snapshot_timeout_sec > 0:
            tasks.append(asyncio.create_task(ws_health_loop(), name="ws_health"))

        log.info("Bot started. /settings available. Press Ctrl+C to stop.")

        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            log.info("Stopping...")
        finally:
            commands_stop.set()
            for t in tasks:
                t.cancel()
            await ws_cluster.stop()
            await engine.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("-c", "--config", default="config.yaml", help="Path to config.yaml")
    args = p.parse_args()
    asyncio.run(main(args.config))
