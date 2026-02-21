"""Handler factories: on_signal, on_ob, on_exchange_toggle, API callbacks."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from core.runtime_settings import save_runtime_settings

from utils.log import log_task_exception

if TYPE_CHECKING:
    from app.context import AppContext

log = logging.getLogger("app.handlers")

AUTO_TUNE_HISTORY_MAX = 50


def make_on_signal(ctx: AppContext):
    async def on_signal(sig):
        ctx.metrics_collector.record_signal()
        reply_markup = sig.to_reply_markup() if hasattr(sig, "to_reply_markup") else None
        await ctx.tg.upsert(sig.key, sig.text, reply_markup=reply_markup)
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

    return on_signal


def make_on_ob(ctx: AppContext):
    async def on_ob(msg: dict) -> None:
        # Record Bybit stats (sampled)
        try:
            from api.db import record_async

            ctx.bybit_record_counter[0] += 1
            if ctx.bybit_record_counter[0] >= ctx.stats_bybit_sample:
                t = asyncio.create_task(record_async("bybit", ctx.bybit_record_counter[0]))
                t.add_done_callback(log_task_exception)
                ctx.bybit_record_counter[0] = 0
        except Exception as e:
            log.warning("Failed to record Bybit stats: %s", e)

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

        now_ms = ctx.state.now_ms()

        for it in parts:
            symbol = it.get("s") or it.get("symbol")
            if not symbol and topic:
                symbol = topic.split(".")[-1]
            if not symbol:
                continue

            if await ctx.q_manager.contains(symbol):
                continue

            bids = it.get("b") or it.get("bids") or []
            asks = it.get("a") or it.get("asks") or []

            if (not bids and not asks) and isinstance(it.get("data"), dict):
                inner = it["data"]
                bids = inner.get("b") or inner.get("bids") or []
                asks = inner.get("a") or inner.get("asks") or []

            ob = await ctx.state.upsert_orderbook(symbol)
            if typ == "snapshot":
                ob.apply_snapshot(bids, asks, now_ms, now_ms)
            elif typ == "delta":
                ob.apply_delta(bids, asks, now_ms, now_ms)
            else:
                if bids or asks:
                    ob.apply_delta(bids, asks, now_ms, now_ms)

    return on_ob


def make_on_exchange_toggle(ctx: AppContext):
    async def on_exchange_toggle(enabled: bool) -> None:
        ctx.settings.exchange_enabled = enabled
        save_runtime_settings(str(ctx.settings_path), ctx.settings)
        if enabled:
            ctx.exchange_enabled_event.set()
            await ctx.ws_cluster.start(ctx.cfg.bybit.symbols)
            log.info("Exchange logic ENABLED (Jupiter, Bybit WS, arb engine)")
        else:
            ctx.exchange_enabled_event.clear()
            await ctx.ws_cluster.stop()
            log.info("Exchange logic DISABLED (ws stopped, poller+engine paused)")

    return on_exchange_toggle


def make_on_symbols_changed(ctx: AppContext):
    async def on_symbols_changed():
        await ctx.ws_cluster.update_symbols(ctx.cfg.bybit.symbols)

    return on_symbols_changed


def make_apply_settings_reload(ctx: AppContext):
    def apply_settings_reload(s):
        ctx.engine.reload_settings(s)
        ctx.tg.update_stale_settings(s.stale_ttl_sec, s.delete_stale)

    return apply_settings_reload


def make_get_status(ctx: AppContext):
    def get_status() -> dict:
        return {
            "exchange_enabled": ctx.settings.exchange_enabled,
            "auto_tune_enabled": ctx.settings.auto_tune_enabled,
        }

    return get_status


def make_get_settings(ctx: AppContext):
    def get_settings() -> dict:
        s = ctx.settings.to_dict()
        labels = dict(ctx.settings.LABELS)
        for k in ("auto_tune_enabled", "auto_tune_bounds"):
            s.pop(k, None)
            labels.pop(k, None)
        return {"settings": s, "labels": labels}

    return get_settings


def make_on_settings_update(ctx: AppContext):
    apply_settings_reload = make_apply_settings_reload(ctx)

    async def on_settings_update(updates: dict) -> dict:
        updated: dict = {}
        for k, v in updates.items():
            if ctx.settings.update(k, v):
                updated[k] = getattr(ctx.settings, k)
        if updated:
            save_runtime_settings(str(ctx.settings_path), ctx.settings)
            apply_settings_reload(ctx.settings)
        s = ctx.settings.to_dict()
        for k in ("auto_tune_enabled", "auto_tune_bounds"):
            s.pop(k, None)
        return {"ok": True, "updated": updated, "settings": s}

    return on_settings_update


def make_get_auto_tune(ctx: AppContext):
    def get_auto_tune() -> dict:
        return {
            "enabled": ctx.settings.auto_tune_enabled,
            "metrics": ctx.metrics_collector.get_window_stats(),
            "history": list(ctx.auto_tune_history),
            "bounds": ctx.settings.auto_tune_bounds or {},
        }

    return get_auto_tune


def make_on_auto_tune_update(ctx: AppContext):
    import time

    apply_settings_reload = make_apply_settings_reload(ctx)
    get_auto_tune = make_get_auto_tune(ctx)

    async def on_auto_tune_update(updates: dict) -> dict:
        updated: dict = {}
        if updates.get("action") == "reset_to_defaults":
            base = {
                "min_profit_usd": float(ctx.cfg.thresholds.min_profit_usd),
                "persistence_hits": int(ctx.cfg.filters.persistence_hits),
                "cooldown_sec": int(ctx.cfg.filters.cooldown_sec),
                "max_spread_bps": float(ctx.cfg.filters.max_spread_bps),
            }
            for k, v in base.items():
                if ctx.settings.update(k, v):
                    updated[k] = v
            if updated:
                save_runtime_settings(str(ctx.settings_path), ctx.settings)
                apply_settings_reload(ctx.settings)
                entry = {
                    "ts": time.time(),
                    "source": "manual",
                    "action": "reset_to_defaults",
                    "params": base,
                }
                ctx.auto_tune_history.append(entry)
                if len(ctx.auto_tune_history) > AUTO_TUNE_HISTORY_MAX:
                    ctx.auto_tune_history.pop(0)
            return {"ok": True, "updated": updated, "auto_tune": get_auto_tune()}
        if "enabled" in updates:
            v = str(updates["enabled"]).lower() in ("true", "1", "yes", "on")
            if ctx.settings.auto_tune_enabled != v:
                ctx.settings.auto_tune_enabled = v
                updated["enabled"] = v
                save_runtime_settings(str(ctx.settings_path), ctx.settings)
        if "bounds" in updates and isinstance(updates["bounds"], dict):
            ctx.settings.auto_tune_bounds = updates["bounds"]
            updated["bounds"] = ctx.settings.auto_tune_bounds
            save_runtime_settings(str(ctx.settings_path), ctx.settings)
        return {"ok": True, "updated": updated, "auto_tune": get_auto_tune()}

    return on_auto_tune_update
