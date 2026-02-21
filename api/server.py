"""
HTTP API для Mini App дашборда.

GET /api/stats?period=1h|1d|1w|all
GET /api/signal-history?period=1h|1d|1w|all&limit=200
GET /api/status — статус бота (exchange_enabled)
GET /api/settings — всё настройки (read-only)
POST /api/exchange — вкл/выкл биржевую логику
POST /api/settings — обновить настройки { key: value, ... }

Защита: Telegram initData, auth_date TTL, allowlist user_id, rate limiting.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import time
from collections import defaultdict
from typing import Awaitable, Callable, Optional, Set

from aiohttp import web

from api.auth import validate_telegram_init_data
from api.db import delete_signal, get_signal_history, get_stats, init as db_init, update_signal_status

log = logging.getLogger("api")

CORS_BASE = {
    "Access-Control-Allow-Methods": "GET, POST, PATCH, DELETE, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, X-Telegram-Init-Data",
    "Access-Control-Max-Age": "86400",
    "Cache-Control": "no-store",
}


def _cors_headers(request: web.Request) -> dict:
    """Build CORS headers. origins empty = *; else echo Origin if allowed."""
    h = dict(CORS_BASE)
    auth = request.app.get("auth_config", {}) or {}
    origins = auth.get("api_cfg", {}).get("cors_origins", [])
    if not origins:
        h["Access-Control-Allow-Origin"] = "*"
    else:
        origin = request.headers.get("Origin", "")
        h["Access-Control-Allow-Origin"] = origin if origin in origins else (origins[0] if origins else "*")
    return h

# Rate limit: IP -> list of request timestamps (sliding window)
_rate_timestamps: dict[str, list[float]] = defaultdict(list)
_rate_lock = asyncio.Lock()


def _get_client_ip(req: web.Request) -> str:
    peername = req.transport.get_extra_info("peername") if req.transport else None
    if peername:
        return str(peername[0])
    forwarded = req.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return req.remote or "unknown"


async def _check_rate_limit(ip: str, limit_per_min: int) -> bool:
    """Return True if allowed, False if rate limited."""
    if limit_per_min <= 0:
        return True
    now = time.time()
    cutoff = now - 60
    async with _rate_lock:
        times = _rate_timestamps[ip]
        times[:] = [t for t in times if t > cutoff]
        if len(times) >= limit_per_min:
            return False
        times.append(now)
    return True


@web.middleware
async def auth_middleware(request: web.Request, handler):
    """Protect /api/* with Telegram initData validation and rate limiting."""
    if not request.path.startswith("/api/"):
        return await handler(request)

    # CORS preflight: browser sends OPTIONS before GET/POST with custom headers
    if request.method == "OPTIONS":
        return web.Response(status=200, headers=_cors_headers(request))

    auth_config = request.app.get("auth_config")
    if not auth_config:
        return await handler(request)

    bot_token = auth_config.get("bot_token")
    api_cfg = auth_config.get("api_cfg")
    if not api_cfg:
        return await handler(request)

    ip = _get_client_ip(request)

    if api_cfg.get("rate_limit_per_min", 0) > 0:
        allowed = await _check_rate_limit(ip, api_cfg["rate_limit_per_min"])
        if not allowed:
            log.warning("api: rate limit exceeded ip=%s", ip)
            return web.json_response(
                {"error": "Too many requests"},
                status=429,
                headers={**_cors_headers(request), "Retry-After": "60"},
            )

    if not api_cfg.get("auth_required", True):
        return await handler(request)

    if not bot_token:
        log.warning("api: auth required but no bot_token")
        return web.json_response(
            {"error": "Unauthorized", "detail": "Server misconfiguration"},
            status=401,
            headers=_cors_headers(request),
        )

    init_data = request.headers.get("X-Telegram-Init-Data")
    if not init_data:
        log.warning("api: missing X-Telegram-Init-Data")
        return web.json_response(
            {"error": "Unauthorized", "detail": "Open dashboard via Telegram (Navigation button)"},
            status=401,
            headers=_cors_headers(request),
        )

    allowed_ids: Optional[Set[int]] = None
    if api_cfg.get("allowed_user_ids"):
        allowed_ids = set(api_cfg["allowed_user_ids"])

    validated = validate_telegram_init_data(
        init_data,
        bot_token,
        auth_ttl_sec=api_cfg.get("auth_ttl_sec", 3600),
        allowed_user_ids=allowed_ids,
    )
    if not validated:
        return web.json_response(
            {"error": "Unauthorized", "detail": "Invalid or expired auth. Open via Telegram."},
            status=401,
            headers=_cors_headers(request),
        )

    return await handler(request)


def create_app(
    on_exchange_toggle: Optional[Callable[[bool], Awaitable[None]]] = None,
    get_status: Optional[Callable[[], dict]] = None,
    get_settings: Optional[Callable[[], dict]] = None,
    on_settings_update: Optional[Callable[[dict], Awaitable[dict]]] = None,
    auth_config: Optional[dict] = None,
) -> web.Application:
    app = web.Application(middlewares=[auth_middleware])
    if auth_config:
        app["auth_config"] = auth_config

    async def handle_stats(req: web.Request) -> web.Response:
        period = req.query.get("period", "1h")
        if period not in ("1h", "1d", "1w", "all"):
            period = "1h"
        data = get_stats(period)
        return web.json_response(data, headers=_cors_headers(req))

    async def handle_signal_history(req: web.Request) -> web.Response:
        period = req.query.get("period", "1d")
        if period not in ("1h", "1d", "1w", "all"):
            period = "1d"
        try:
            limit = min(500, max(1, int(req.query.get("limit", 200))))
        except (TypeError, ValueError):
            limit = 200
        data = get_signal_history(period, limit=limit)
        return web.json_response(data, headers=_cors_headers(req))

    async def handle_root(req: web.Request) -> web.Response:
        return web.json_response({"ok": True, "api": "market-analyzer"}, headers=_cors_headers(req))

    async def handle_signal_history_patch(req: web.Request) -> web.Response:
        try:
            data = await req.json() if req.content_length else {}
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400, headers=_cors_headers(req))
        sid = data.get("id")
        status = data.get("status")
        if sid is None or status not in ("active", "stale"):
            return web.json_response(
                {"error": "Expected { id: number, status: 'active'|'stale' }"},
                status=400,
                headers=_cors_headers(req),
            )
        ok = update_signal_status(int(sid), status)
        return web.json_response({"ok": ok}, headers=_cors_headers(req))

    async def handle_signal_history_delete(req: web.Request) -> web.Response:
        sid = req.query.get("id")
        if sid is None:
            return web.json_response(
                {"error": "Missing id (query param)"},
                status=400,
                headers=_cors_headers(req),
            )
        try:
            sid = int(sid)
        except (TypeError, ValueError):
            return web.json_response({"error": "id must be number"}, status=400, headers=_cors_headers(req))
        ok = delete_signal(sid)
        return web.json_response({"ok": ok}, headers=_cors_headers(req))

    app.router.add_get("/", handle_root)
    app.router.add_get("/api/stats", handle_stats)
    app.router.add_get("/api/signal-history", handle_signal_history)
    app.router.add_route("PATCH", "/api/signal-history", handle_signal_history_patch)
    app.router.add_route("DELETE", "/api/signal-history", handle_signal_history_delete)

    if get_status is not None:

        async def handle_status(req: web.Request) -> web.Response:
            data = get_status()
            return web.json_response(data, headers=_cors_headers(req))

        app.router.add_get("/api/status", handle_status)

    if get_settings is not None:

        async def handle_settings(req: web.Request) -> web.Response:
            data = get_settings()
            return web.json_response(data, headers=_cors_headers(req))

        app.router.add_get("/api/settings", handle_settings)

    if on_settings_update is not None:

        async def handle_settings_post(req: web.Request) -> web.Response:
            if req.method != "POST":
                return web.json_response({"error": "Method not allowed"}, status=405, headers=_cors_headers(req))
            try:
                data = await req.json() if req.content_length else {}
            except Exception:
                return web.json_response({"error": "Invalid JSON"}, status=400, headers=_cors_headers(req))
            if not isinstance(data, dict) or not data:
                return web.json_response({"error": "Expected non-empty object { key: value }"}, status=400, headers=_cors_headers(req))
            result = await on_settings_update(data)
            return web.json_response(result, headers=_cors_headers(req))

        app.router.add_route("POST", "/api/settings", handle_settings_post)

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
                        headers=_cors_headers(req),
                    )
                val = str(enabled).lower() in ("true", "1", "yes", "on")
                await on_exchange_toggle(val)
                return web.json_response({"ok": True, "exchange_enabled": val}, headers=_cors_headers(req))
            return web.json_response({"error": "Method not allowed"}, status=405, headers=_cors_headers(req))

        app.router.add_route("POST", "/api/exchange", handle_exchange)

    return app


async def run_server(
    host: str = "0.0.0.0",
    port: int = 8080,
    db_path: Optional[str] = None,
    on_exchange_toggle: Optional[Callable[[bool], Awaitable[None]]] = None,
    get_status: Optional[Callable[[], dict]] = None,
    get_settings: Optional[Callable[[], dict]] = None,
    on_settings_update: Optional[Callable[[dict], Awaitable[dict]]] = None,
    auth_config: Optional[dict] = None,
) -> None:
    if db_path:
        from pathlib import Path

        db_init(Path(db_path))
    app = create_app(
        on_exchange_toggle=on_exchange_toggle,
        get_status=get_status,
        get_settings=get_settings,
        on_settings_update=on_settings_update,
        auth_config=auth_config,
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
