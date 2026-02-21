"""Entry point. Orchestrates bootstrap, handlers, tasks."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
from decimal import Decimal

import aiohttp

from core.quarantine_verifier import verify_loop
from notifier.commands import run_settings_command_handler
from utils.log import setup_logging

from app.bootstrap import build_context
from app.handlers import (
    make_apply_settings_reload,
    make_get_auto_tune,
    make_get_settings,
    make_get_status,
    make_on_auto_tune_update,
    make_on_exchange_toggle,
    make_on_settings_update,
    make_on_signal,
    make_on_symbols_changed,
)
from app.tasks import (
    make_auto_tune_loop,
    make_stats_heartbeat_loop,
    make_status_loop,
    make_stale_loop,
    make_ws_health_loop,
)

log = logging.getLogger("app")


async def main(cfg_path: str) -> None:
    async with aiohttp.ClientSession() as session:
        ctx = await build_context(cfg_path, session)
        cfg = ctx.cfg
        settings = ctx.settings
        settings_path = ctx.settings_path

        on_signal = make_on_signal(ctx)
        on_exchange_toggle = make_on_exchange_toggle(ctx)
        on_symbols_changed = make_on_symbols_changed(ctx)
        apply_settings_reload = make_apply_settings_reload(ctx)
        get_status = make_get_status(ctx)
        get_settings = make_get_settings(ctx)
        on_settings_update = make_on_settings_update(ctx)
        get_auto_tune = make_get_auto_tune(ctx)
        on_auto_tune_update = make_on_auto_tune_update(ctx)

        status_loop = make_status_loop(ctx)
        stale_loop = make_stale_loop(ctx)
        auto_tune_loop = make_auto_tune_loop(ctx)
        stats_heartbeat_loop = make_stats_heartbeat_loop(ctx)
        ws_health_loop = make_ws_health_loop(ctx)

        commands_stop = asyncio.Event()
        api_port = int(os.environ.get("PORT") or os.environ.get("API_PORT") or "8080")

        api_cfg = getattr(cfg, "api", None)
        auth_config = None
        if api_cfg:
            api_cfg_base = {
                "rate_limit_per_min": api_cfg.rate_limit_per_min,
                "cors_origins": list(getattr(api_cfg, "cors_origins", None) or []),
                "logs_enabled": getattr(api_cfg, "logs_enabled", True),
                "logs_rate_limit_per_min": getattr(api_cfg, "logs_rate_limit_per_min", 10),
            }
            if getattr(api_cfg, "auth_required", True):
                auth_config = {
                    "bot_token": ctx._tg_token,
                    "api_cfg": {
                        **api_cfg_base,
                        "auth_required": True,
                        "auth_ttl_sec": api_cfg.auth_ttl_sec,
                        "allowed_user_ids": list(api_cfg.allowed_user_ids or []),
                    },
                }
            else:
                auth_config = {
                    "bot_token": None,
                    "api_cfg": {**api_cfg_base, "auth_required": False},
                }

        api_server_mod = __import__("api.server", fromlist=["run_server"])
        full_tokens = dict(cfg.trading.tokens)

        tasks: list[asyncio.Task] = [
            asyncio.create_task(
                api_server_mod.run_server(
                    host="0.0.0.0",
                    port=api_port,
                    on_exchange_toggle=on_exchange_toggle,
                    get_status=get_status,
                    get_settings=get_settings,
                    on_settings_update=on_settings_update,
                    get_auto_tune=get_auto_tune,
                    on_auto_tune_update=on_auto_tune_update,
                    auth_config=auth_config,
                ),
                name="api_server",
            ),
            asyncio.create_task(
                ctx.q_manager.sync_loop(poll_sec=10.0, on_symbols_changed=on_symbols_changed),
                name="quarantine_sync",
            ),
            asyncio.create_task(ctx.engine.quote_poller(), name="jup_poller"),
            asyncio.create_task(ctx.engine.run(on_signal), name="arb_engine"),
            asyncio.create_task(status_loop(), name="status"),
            asyncio.create_task(stale_loop(), name="tg_stale"),
            asyncio.create_task(auto_tune_loop(), name="auto_tune"),
            asyncio.create_task(stats_heartbeat_loop(), name="stats_heartbeat"),
            asyncio.create_task(
                run_settings_command_handler(
                    session=session,
                    bot_token=ctx._tg_token,
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

        verify_interval = float(getattr(cfg.runtime, "quarantine_verify_interval_sec", 30 * 60))
        tasks.append(
            asyncio.create_task(
                verify_loop(
                    q_manager=ctx.q_manager,
                    jup=ctx.jup,
                    full_tokens=full_tokens,
                    stable_mint=cfg.trading.stable.mint,
                    stable_decimals=cfg.trading.stable.decimals,
                    notional_usd=Decimal(str(settings.notional_usd)),
                    on_symbols_changed=on_symbols_changed,
                    interval_sec=verify_interval,
                    exchange_enabled_getter=lambda: settings.exchange_enabled,
                ),
                name="quarantine_verify",
            )
        )

        log.info("Bot started. /settings available. Press Ctrl+C to stop.")

        try:
            await asyncio.gather(*tasks)
        except (KeyboardInterrupt, SystemExit):
            log.info("Shutting down gracefully...")
        finally:
            commands_stop.set()
            await asyncio.sleep(1)
            try:
                await ctx._api_db.flush_async()
                log.info("DB buffer flushed")
            except Exception as e:
                log.warning("DB flush on shutdown failed: %s", e)
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await ctx.ws_cluster.stop()
            await ctx.engine.stop()
            log.info("Shutdown complete")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("-c", "--config", default="config.yaml", help="Path to config.yaml")
    args = p.parse_args()
    asyncio.run(main(args.config))
