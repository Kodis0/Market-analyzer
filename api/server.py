"""
HTTP API для Mini App дашборда.
GET /api/stats?period=1h|1d|1w|all
GET /api/signal-history?period=1h|1d|1w|all
GET /api/status — статус бота (exchange_enabled)
GET /api/settings — всё настройки (read-only)
POST /api/exchange — вкл/выкл биржевую логику
"""
from __future__ import annotations

import argparse
import asyncio
import logging
from typing import Awaitable, Callable, Optional

from aiohttp import web

from api.db import get_signal_history, get_stats, init as db_init

log = logging.getLogger("api")

CORS_HEADERS = {"Access-Control-Allow-Origin": "*", "Cache-Control": "no-store"}


def create_app(
    on_exchange_toggle: Optional[Callable[[bool], Awaitable[None]]] = None,
    get_status: Optional[Callable[[], dict]] = None,
    get_settings: Optional[Callable[[], dict]] = None,
) -> web.Application:
    app = web.Application()

    async def handle_stats(req: web.Request) -> web.Response:
        period = req.query.get("period", "1h")
        if period not in ("1h", "1d", "1w", "all"):
            period = "1h"
        data = get_stats(period)
        return web.json_response(data, headers=CORS_HEADERS)

    async def handle_signal_history(req: web.Request) -> web.Response:
        period = req.query.get("period", "1d")
        if period not in ("1h", "1d", "1w", "all"):
            period = "1d"
        limit = min(500, max(1, int(req.query.get("limit", 200))))
        data = get_signal_history(period, limit=limit)
        return web.json_response(data, headers=CORS_HEADERS)

    app.router.add_get("/api/stats", handle_stats)
    app.router.add_get("/api/signal-history", handle_signal_history)

    if get_status is not None:

        async def handle_status(req: web.Request) -> web.Response:
            data = get_status()
            return web.json_response(data, headers=CORS_HEADERS)

        app.router.add_get("/api/status", handle_status)

    if get_settings is not None:

        async def handle_settings(req: web.Request) -> web.Response:
            data = get_settings()
            return web.json_response(data, headers=CORS_HEADERS)

        app.router.add_get("/api/settings", handle_settings)

    if on_exchange_toggle is not None:

        async def handle_exchange(req: web.Request) -> web.Response:
            if req.method == "POST":
                try:
                    data = await req.json() if req.content_length else {}
                except Exception:
                    data = {}
                enabled = data.get("enabled")
                if enabled is None:
                    enabled = req.query.get("enabled")
                if enabled is None:
                    return web.json_response(
                        {"error": "Missing enabled (true|false)"},
                        status=400,
                        headers=CORS_HEADERS,
                    )
                val = str(enabled).lower() in ("true", "1", "yes", "on")
                await on_exchange_toggle(val)
                return web.json_response({"ok": True, "exchange_enabled": val}, headers=CORS_HEADERS)
            return web.json_response({"error": "Method not allowed"}, status=405, headers=CORS_HEADERS)

        app.router.add_route("POST", "/api/exchange", handle_exchange)

    return app


async def run_server(
    host: str = "0.0.0.0",
    port: int = 8080,
    db_path: Optional[str] = None,
    on_exchange_toggle: Optional[Callable[[bool], Awaitable[None]]] = None,
    get_status: Optional[Callable[[], dict]] = None,
    get_settings: Optional[Callable[[], dict]] = None,
) -> None:
    if db_path:
        from pathlib import Path

        db_init(Path(db_path))
    app = create_app(
        on_exchange_toggle=on_exchange_toggle,
        get_status=get_status,
        get_settings=get_settings,
    )
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    log.info("API server listening on %s:%d", host, port)
    await asyncio.Event().wait()  # run forever


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--db", default="request_stats.db")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO)
    db_init(__import__("pathlib").Path(args.db))

    async def _main():
        await run_server(args.host, args.port)

    asyncio.run(_main())
