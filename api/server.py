"""
HTTP API для Mini App дашборда.
GET /api/stats?period=1h|1d|1w|all
"""
from __future__ import annotations

import argparse
import asyncio
import logging
from typing import Optional

from aiohttp import web

from api.db import get_stats, init as db_init

log = logging.getLogger("api")

CORS_HEADERS = {"Access-Control-Allow-Origin": "*", "Cache-Control": "no-store"}


def create_app() -> web.Application:
    app = web.Application()

    async def handle_stats(req: web.Request) -> web.Response:
        period = req.query.get("period", "1h")
        if period not in ("1h", "1d", "1w", "all"):
            period = "1h"
        data = get_stats(period)
        return web.json_response(data, headers=CORS_HEADERS)

    app.router.add_get("/api/stats", handle_stats)

    return app


async def run_server(host: str = "0.0.0.0", port: int = 8080, db_path: Optional[str] = None) -> None:
    if db_path:
        from pathlib import Path

        db_init(Path(db_path))
    app = create_app()
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
