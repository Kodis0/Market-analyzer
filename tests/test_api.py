"""Basic API route tests."""

from __future__ import annotations

from pathlib import Path

import pytest
from aiohttp.test_utils import TestClient, TestServer

from api.db import close as db_close, init as db_init
from api.server import create_app


def _make_app_no_auth(tmp_path: Path):
    db_init(tmp_path / "test.db")
    return create_app(
        get_status=lambda: {"exchange_enabled": True, "auto_tune_enabled": False},
        auth_config=None,
    )


def _make_app_with_auth(tmp_path: Path):
    db_init(tmp_path / "test_auth.db")
    return create_app(
        get_status=lambda: {"exchange_enabled": True},
        auth_config={
            "bot_token": "test_token",
            "api_cfg": {
                "auth_required": True,
                "auth_ttl_sec": 3600,
                "allowed_user_ids": [],
                "rate_limit_per_min": 120,
                "cors_origins": [],
                "logs_enabled": False,
                "logs_rate_limit_per_min": 10,
            },
        },
    )


@pytest.mark.asyncio
async def test_root_returns_200(tmp_path: Path):
    app = _make_app_no_auth(tmp_path)
    try:
        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/")
            assert resp.status == 200
            data = await resp.json()
            assert data.get("ok") is True
            assert data.get("api") == "market-analyzer"
            assert "db_ok" in data
            assert data.get("exchange_enabled") is True
    finally:
        db_close()


@pytest.mark.asyncio
async def test_api_status_returns_200(tmp_path: Path):
    app = _make_app_no_auth(tmp_path)
    try:
        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/api/status")
            assert resp.status == 200
            data = await resp.json()
            assert "exchange_enabled" in data
    finally:
        db_close()


@pytest.mark.asyncio
async def test_api_stats_returns_array(tmp_path: Path):
    app = _make_app_no_auth(tmp_path)
    try:
        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/api/stats?period=1h")
            assert resp.status == 200
            data = await resp.json()
            assert isinstance(data, list)
    finally:
        db_close()


@pytest.mark.asyncio
async def test_api_status_401_without_init_data_when_auth_required(tmp_path: Path):
    app = _make_app_with_auth(tmp_path)
    try:
        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/api/status")
            assert resp.status == 401
            data = await resp.json()
            assert "error" in data
    finally:
        db_close()
