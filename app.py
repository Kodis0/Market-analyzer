from __future__ import annotations

import argparse
import asyncio
import logging
import os
import time
from decimal import Decimal
from typing import Any

import aiohttp
from dotenv import load_dotenv

from connectors.jupiter import JupiterClient
from core.arb_engine import ArbEngine
from core.bootstrap import get_paths, load_config, require_env
from core.config import AppConfig
from core.fees import Thresholds
from core.jupiter_sanitizer import make_on_jup_skip
from core.quarantine_manager import QuarantineManager
from core.state import MarketState
from core.runtime_settings import RuntimeSettings, load_runtime_settings, save_runtime_settings
from core.ws_cluster import BybitWSCluster
from notifier.commands import run_settings_command_handler
from notifier.telegram import TelegramNotifier
from utils.log import setup_logging

log = logging.getLogger("app")


async def main(cfg_path: str) -> None:
    cfg, raw, cfg_dir = load_config(cfg_path)
    setup_logging(cfg.logging.level)

    from api.log_buffer import init_log_buffer

    init_log_buffer(
        enabled=cfg.api.logs_enabled,
        buffer_size=cfg.api.logs_buffer_size,
        max_line_len=cfg.api.logs_max_line_len,
    )

    settings_path, api_db_path, quarantine_path = get_paths(cfg_dir, raw)

    from api import db as api_db

    api_db.init(api_db_path)

    full_symbols: list[str] = list(cfg.bybit.symbols)
    full_tokens = dict(cfg.trading.tokens)
    base_denylist = list(cfg.filters.denylist_symbols or [])

    token_cfgs: dict[str, dict[str, Any]] = {}
    for token_key, t in cfg.trading.tokens.items():
        token_cfgs[token_key] = {"bybit_symbol": t.bybit_symbol, "mint": t.mint, "decimals": t.decimals}

    q_manager = QuarantineManager(
        quarantine_path=quarantine_path,
        cfg=cfg,
        full_symbols=full_symbols,
        full_tokens=full_tokens,
        base_denylist=base_denylist,
        token_cfgs=token_cfgs,
    )
    q_manager.load_initial()

    env = require_env("TG_BOT_TOKEN", "JUP_API_KEY")
    tg_token = env["TG_BOT_TOKEN"]
    jup_key = env["JUP_API_KEY"]

    state = MarketState()

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
    settings = load_runtime_settings(str(settings_path), settings_defaults)

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

        mint_to_symbol = {t.mint: t.bybit_symbol for t in full_tokens.values() if getattr(t, "mint", None)}
        stable_mint = cfg.trading.stable.mint

        on_jup_skip = make_on_jup_skip(stable_mint, mint_to_symbol, q_manager.add)

        def _record_jupiter(source: str, count: int = 1) -> None:
            try:
                from api.db import record_async

                asyncio.get_running_loop().create_task(record_async(source, count))
            except Exception as e:
                log.warning("Failed to record Jupiter stats: %s", e)

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

        exchange_enabled_event = asyncio.Event()
        if settings.exchange_enabled:
            exchange_enabled_event.set()
        else:
            exchange_enabled_event.clear()
            log.info("Exchange logic disabled at startup (settings.exchange_enabled=false)")

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
            exchange_enabled_event=exchange_enabled_event,
        )

        async def on_signal(sig):
            reply_markup = sig.to_reply_markup() if hasattr(sig, "to_reply_markup") else None
            await tg.upsert(sig.key, sig.text, reply_markup=reply_markup)
            try:
                from api.db import record_signal_async

                await record_signal_async(
                    sig.token,
                    sig.direction,
                    float(sig.profit_usd),
                    float(sig.notional_usd),
                )
            except Exception as e:
                log.warning("Failed to record signal: %s", e)

        stats_bybit_sample = max(1, int(getattr(cfg.runtime, "stats_bybit_sample", 1)))
        _bybit_record_counter = [0]

        def _record_bybit() -> None:
            try:
                from api.db import record_async

                _bybit_record_counter[0] += 1
                if _bybit_record_counter[0] >= stats_bybit_sample:
                    asyncio.get_running_loop().create_task(
                        record_async("bybit", _bybit_record_counter[0])
                    )
                    _bybit_record_counter[0] = 0
            except Exception as e:
                log.warning("Failed to record Bybit stats: %s", e)

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

                if await q_manager.contains(symbol):
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

        ws_cluster = BybitWSCluster(
            ws_url=cfg.bybit.ws_url,
            depth=cfg.bybit.depth,
            ping_interval_sec=cfg.bybit.ping_interval_sec,
            on_orderbook_message=on_ob,
            max_symbols_per_ws=100,
        )

        if settings.exchange_enabled:
            await ws_cluster.start(cfg.bybit.symbols)
            log.info("Bybit WS clients=%d, symbols=%d", len(ws_cluster.clients), len(cfg.bybit.symbols))
        else:
            log.info("Bybit WS not started (exchange_enabled=false)")

        async def on_exchange_toggle(enabled: bool) -> None:
            settings.exchange_enabled = enabled
            save_runtime_settings(str(settings_path), settings)
            if enabled:
                exchange_enabled_event.set()
                await ws_cluster.start(cfg.bybit.symbols)
                log.info("Exchange logic ENABLED (Jupiter, Bybit WS, arb engine)")
            else:
                exchange_enabled_event.clear()
                await ws_cluster.stop()
                log.info("Exchange logic DISABLED (ws stopped, poller+engine paused)")

        async def on_symbols_changed():
            await ws_cluster.update_symbols(cfg.bybit.symbols)

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
                    skip_text = ", ".join([f"{k}={v}" for k, v in sorted(stats.items(), key=lambda kv: kv[1], reverse=True)[:5]]) if stats else "none"
                else:
                    skip_text = "n/a"

                status_interval = float(getattr(cfg.runtime, "status_interval_sec", 15))
                log.info(
                    "[STATUS] active=%d | quarantined=%d | OB non-empty %d/%d | OB fresh %d/%d (<=%dms) | skips(30s): %s | sample: %s",
                    total,
                    len(q_manager.quarantined_set),
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

        async def stats_heartbeat_loop():
            while True:
                await asyncio.sleep(60)
                if not settings.exchange_enabled:
                    continue
                try:
                    from api.db import record_async

                    await asyncio.gather(
                        record_async("jupiter", 1),
                        record_async("bybit", 1),
                    )
                except Exception as e:
                    log.warning("Stats heartbeat failed: %s", e)

        async def ws_health_loop():
            timeout_sec = float(cfg.runtime.ws_snapshot_timeout_sec)
            if timeout_sec <= 0:
                return
            start_ts = time.time()
            AUTO_Q_TTL_SEC = 6 * 3600

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
                        await q_manager.add(sym, "WS_STALE", AUTO_Q_TTL_SEC)
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

        def get_status() -> dict:
            return {"exchange_enabled": settings.exchange_enabled}

        def get_settings() -> dict:
            return {"settings": settings.to_dict(), "labels": settings.LABELS}

        async def on_settings_update(updates: dict) -> dict:
            updated: dict = {}
            for k, v in updates.items():
                if settings.update(k, v):
                    updated[k] = getattr(settings, k)
            if updated:
                save_runtime_settings(str(settings_path), settings)
                apply_settings_reload(settings)
            return {"ok": True, "updated": updated, "settings": settings.to_dict()}

        api_cfg = getattr(cfg, "api", None)
        auth_config = None
        if api_cfg and getattr(api_cfg, "auth_required", True):
            auth_config = {
                "bot_token": tg_token,
                "api_cfg": {
                    "auth_required": api_cfg.auth_required,
                    "auth_ttl_sec": api_cfg.auth_ttl_sec,
                    "allowed_user_ids": list(api_cfg.allowed_user_ids or []),
                    "rate_limit_per_min": api_cfg.rate_limit_per_min,
                    "cors_origins": list(api_cfg.cors_origins or []),
                    "logs_enabled": getattr(api_cfg, "logs_enabled", True),
                    "logs_rate_limit_per_min": getattr(api_cfg, "logs_rate_limit_per_min", 10),
                },
            }
        elif api_cfg and not getattr(api_cfg, "auth_required", True):
            auth_config = {
                "bot_token": None,
                "api_cfg": {
                    "auth_required": False,
                    "cors_origins": list(getattr(api_cfg, "cors_origins", None) or []),
                    "logs_enabled": getattr(api_cfg, "logs_enabled", True),
                    "logs_rate_limit_per_min": getattr(api_cfg, "logs_rate_limit_per_min", 10),
                },
            }

        api_server_mod = __import__("api.server", fromlist=["run_server"])
        tasks: list[asyncio.Task] = [
            asyncio.create_task(
                api_server_mod.run_server(
                    host="0.0.0.0",
                    port=api_port,
                    on_exchange_toggle=on_exchange_toggle,
                    get_status=get_status,
                    get_settings=get_settings,
                    on_settings_update=on_settings_update,
                    auth_config=auth_config,
                ),
                name="api_server",
            ),
            asyncio.create_task(q_manager.sync_loop(poll_sec=10.0, on_symbols_changed=on_symbols_changed), name="quarantine_sync"),
            asyncio.create_task(engine.quote_poller(), name="jup_poller"),
            asyncio.create_task(engine.run(on_signal), name="arb_engine"),
            asyncio.create_task(status_loop(), name="status"),
            asyncio.create_task(stale_loop(), name="tg_stale"),
            asyncio.create_task(stats_heartbeat_loop(), name="stats_heartbeat"),
            asyncio.create_task(
                run_settings_command_handler(
                    session=session,
                    bot_token=tg_token,
                    chat_id=cfg.telegram.chat_id,
                    thread_id=cfg.telegram.thread_id,
                    settings=settings,
                    settings_path=str(settings_path),
                    on_reload=apply_settings_reload,
                    stop_event=commands_stop,
                    web_app_url=getattr(cfg.telegram, "web_app_url", None),
                    pinned_message_text=getattr(cfg.telegram, "pinned_message_text", None),
                    on_exchange_toggle=on_exchange_toggle,
                ),
                name="settings_cmd",
            ),
        ]

        if cfg.runtime.ws_snapshot_timeout_sec > 0:
            tasks.append(asyncio.create_task(ws_health_loop(), name="ws_health"))

        log.info("Bot started. /settings available. Press Ctrl+C to stop.")

        try:
            await asyncio.gather(*tasks)
        except (KeyboardInterrupt, SystemExit):
            log.info("Shutting down gracefully...")
        finally:
            commands_stop.set()
            await asyncio.sleep(1)
            try:
                await api_db.flush_async()
                log.info("DB buffer flushed")
            except Exception as e:
                log.warning("DB flush on shutdown failed: %s", e)
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await ws_cluster.stop()
            await engine.stop()
            log.info("Shutdown complete")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("-c", "--config", default="config.yaml", help="Path to config.yaml")
    args = p.parse_args()
    asyncio.run(main(args.config))
