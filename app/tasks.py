"""Background task loops: status, stale, auto_tune, stats_heartbeat, ws_health."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING

from core.auto_tune.tuner import AutoTuner, TunerBounds, TunerConfig
from core.runtime_settings import save_runtime_settings

from app.handlers import AUTO_TUNE_HISTORY_MAX, make_apply_settings_reload

if TYPE_CHECKING:
    from app.context import AppContext

log = logging.getLogger("app.tasks")


def make_status_loop(ctx: AppContext):
    async def status_loop():
        _n = len(ctx.cfg.bybit.symbols)
        status_sample_step = max(1, min(10, _n // 50)) if _n > 100 else 1
        FRESH_MS = 2000

        while True:
            symbols = list(ctx.cfg.bybit.symbols)
            total = len(symbols)
            fresh_cnt = 0
            non_empty_cnt = 0
            sampled_n = 0
            sample_syms = symbols[:5]
            sample_parts: list[str] = []

            for i in range(0, total, status_sample_step):
                sym = symbols[i]
                ob = await ctx.state.get_orderbook(sym)
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
                ob = await ctx.state.get_orderbook(sym)
                if ob is None or not ob.bids or not ob.asks:
                    sample_parts.append(f"{sym} OB empty")
                else:
                    best_bid = max(ob.bids.keys())
                    best_ask = min(ob.asks.keys())
                    sample_parts.append(f"{sym} bid={best_bid} ask={best_ask} age={ob.age_ms()}ms")

            stats = ctx.engine.drain_debug_stats()
            if stats is not None:
                ctx.metrics_collector.record_skips(stats)
                skip_text = (
                    ", ".join(
                        f"{k}={v}"
                        for k, v in sorted(stats.items(), key=lambda kv: kv[1], reverse=True)[:5]
                    )
                    if stats
                    else "none"
                )
            else:
                skip_text = "n/a"

            status_interval = float(getattr(ctx.cfg.runtime, "status_interval_sec", 15))
            log.info(
                "[STATUS] active=%d | quarantined=%d | OB non-empty %d/%d | OB fresh %d/%d (<=%dms) | skips(30s): %s | sample: %s",
                total,
                len(ctx.q_manager.quarantined_set),
                non_empty_cnt,
                total,
                fresh_cnt,
                total,
                FRESH_MS,
                skip_text,
                " | ".join(sample_parts),
            )
            await asyncio.sleep(status_interval)

    return status_loop


def make_stale_loop(ctx: AppContext):
    async def stale_loop():
        while True:
            await ctx.tg.expire_stale()
            await asyncio.sleep(5)

    return stale_loop


def make_auto_tune_loop(ctx: AppContext):
    tuner = AutoTuner(config=TunerConfig())
    apply_settings_reload = make_apply_settings_reload(ctx)

    async def auto_tune_loop():
        last_eval_ts = 0.0
        while True:
            await asyncio.sleep(60)
            if not ctx.settings.auto_tune_enabled or not ctx.settings.exchange_enabled:
                continue
            now = time.time()
            if (now - last_eval_ts) < ctx.auto_tune_interval_sec:
                continue
            last_eval_ts = now
            try:
                metrics = ctx.metrics_collector.get_window_stats()
                bounds = TunerBounds.from_dict(ctx.settings.auto_tune_bounds)
                changes = tuner.evaluate(metrics, ctx.settings, bounds)
                if not changes:
                    continue
                for c in changes:
                    if ctx.settings.update(c.param, c.new_value):
                        apply_settings_reload(ctx.settings)
                        save_runtime_settings(str(ctx.settings_path), ctx.settings)
                        entry = {
                            "ts": time.time(),
                            "source": "auto",
                            "param": c.param,
                            "old_value": c.old_value,
                            "new_value": c.new_value,
                            "reason": c.reason,
                        }
                        ctx.auto_tune_history.append(entry)
                        if len(ctx.auto_tune_history) > AUTO_TUNE_HISTORY_MAX:
                            ctx.auto_tune_history.pop(0)
                        log.info(
                            "[AUTO_TUNE] %s: %s -> %s (%s)",
                            c.param,
                            c.old_value,
                            c.new_value,
                            c.reason,
                        )
            except Exception as e:
                log.warning("Auto-tune loop error: %s", e, exc_info=True)

    return auto_tune_loop


def make_stats_heartbeat_loop(ctx: AppContext):
    async def stats_heartbeat_loop():
        while True:
            await asyncio.sleep(60)
            if not ctx.settings.exchange_enabled:
                continue
            try:
                from api.db import record_async

                await asyncio.gather(
                    record_async("jupiter", 1),
                    record_async("bybit", 1),
                )
            except Exception as e:
                log.warning("Stats heartbeat failed: %s", e)

    return stats_heartbeat_loop


def make_ws_health_loop(ctx: AppContext):
    AUTO_Q_TTL_SEC = 6 * 3600

    async def ws_health_loop():
        timeout_sec = float(ctx.cfg.runtime.ws_snapshot_timeout_sec)
        if timeout_sec <= 0:
            return
        start_ts = time.time()

        while True:
            await asyncio.sleep(max(5.0, timeout_sec / 2))
            if (time.time() - start_ts) < timeout_sec:
                continue

            now_ms = ctx.state.now_ms()
            symbols = list(ctx.cfg.bybit.symbols)
            stale_syms: list[str] = []
            for sym in symbols:
                ob = await ctx.state.get_orderbook(sym)
                last_msg_ms = 0
                if ob is not None:
                    last_msg_ms = int(ob.last_cts_ms or ob.last_update_ms or ob.last_snapshot_ms or 0)
                if last_msg_ms <= 0 or (now_ms - last_msg_ms) > timeout_sec * 1000:
                    stale_syms.append(sym)

            if stale_syms:
                for sym in stale_syms[:50]:
                    await ctx.q_manager.add(sym, "WS_STALE", AUTO_Q_TTL_SEC)
                log.warning(
                    "[HEALTH] stale snapshots %d/%d (>%.0fs) sample=%s",
                    len(stale_syms),
                    len(symbols),
                    timeout_sec,
                    ", ".join(stale_syms[:5]),
                )

    return ws_health_loop
