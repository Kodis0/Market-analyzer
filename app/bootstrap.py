"""Bootstrap: load config, create services, build AppContext."""

from __future__ import annotations

import asyncio
import logging
import os
from decimal import Decimal
from pathlib import Path
from typing import Any

import aiohttp

from connectors.jupiter import JupiterClient
from core.arb_engine import ArbEngine
from core.auto_tune.metrics import MetricsCollector
from core.bootstrap import get_paths, load_config, require_env
from core.fees import Thresholds
from core.jupiter_sanitizer import make_on_jup_skip
from core.quarantine_manager import QuarantineManager
from core.runtime_settings import RuntimeSettings, load_runtime_settings
from core.state import MarketState
from core.ws_cluster import BybitWSCluster
from utils.log import log_task_exception

from app.context import AppContext
from app.handlers import make_on_ob

log = logging.getLogger("app.bootstrap")


def _record_jupiter(source: str, count: int = 1) -> None:
    try:
        from api.db import record_async

        t = asyncio.create_task(record_async(source, count))
        t.add_done_callback(log_task_exception)
    except Exception as e:
        log.warning("Failed to record Jupiter stats: %s", e)


async def build_context(cfg_path: str, session: aiohttp.ClientSession) -> AppContext:
    """Load config, create all services, return AppContext."""
    cfg, raw, cfg_dir = load_config(cfg_path)

    from utils.log import setup_logging

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
        auto_tune_enabled=bool(getattr(cfg.auto_tune, "enabled", False)),
    )
    settings = load_runtime_settings(str(settings_path), settings_defaults)

    ctx = AppContext()
    ctx.cfg = cfg
    ctx.raw = raw
    ctx.cfg_dir = cfg_dir
    ctx.settings_path = settings_path
    ctx.settings = settings
    ctx.state = state
    ctx.q_manager = q_manager

    auto_tune_cfg = getattr(cfg, "auto_tune", None)
    window_sec = float(getattr(auto_tune_cfg, "window_sec", 30 * 60) if auto_tune_cfg else 30 * 60)
    ctx.metrics_collector = MetricsCollector(window_sec=window_sec)
    ctx.auto_tune_interval_sec = float(getattr(auto_tune_cfg, "interval_sec", 15 * 60) if auto_tune_cfg else 15 * 60)
    ctx.stats_bybit_sample = max(1, int(getattr(cfg.runtime, "stats_bybit_sample", 1)))

    exchange_enabled_event = asyncio.Event()
    if settings.exchange_enabled:
        exchange_enabled_event.set()
    else:
        exchange_enabled_event.clear()
        log.info("Exchange logic disabled at startup (settings.exchange_enabled=false)")
    ctx.exchange_enabled_event = exchange_enabled_event

    mint_to_symbol = {t.mint: t.bybit_symbol for t in full_tokens.values() if getattr(t, "mint", None)}
    on_jup_skip = make_on_jup_skip(cfg.trading.stable.mint, mint_to_symbol, q_manager.add)

    from notifier.telegram import TelegramNotifier

    ctx.tg = tg = TelegramNotifier(
        session,
        tg_token,
        cfg.telegram.chat_id,
        cfg.telegram.thread_id,
        edit_min_interval_sec=cfg.notifier.edit_min_interval_sec,
        edit_mode=cfg.notifier.edit_mode,
        stale_ttl_sec=float(settings.stale_ttl_sec),
        delete_stale=settings.delete_stale,
    )

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
    ctx.jup = jup

    thresholds = Thresholds(
        bybit_taker_fee_bps=Decimal(str(settings.bybit_taker_fee_bps)),
        solana_tx_fee_usd=Decimal(str(settings.solana_tx_fee_usd)),
        latency_buffer_bps=Decimal(str(settings.latency_buffer_bps)),
        usdt_usdc_buffer_bps=Decimal(str(settings.usdt_usdc_buffer_bps)),
        min_profit_usd=Decimal(str(settings.min_profit_usd)),
    )

    ctx.engine = engine = ArbEngine(
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

    on_ob = make_on_ob(ctx)
    ctx.ws_cluster = ws_cluster = BybitWSCluster(
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

    ctx._tg_token = tg_token
    ctx._api_db = api_db
    return ctx
