"""Tests for api.auth."""
from __future__ import annotations

import hashlib
import hmac
import json
import time
from urllib.parse import quote

import pytest

from api.auth import WEBAPP_DATA_CONST, validate_telegram_init_data


def _make_valid_init_data(bot_token: str, auth_date: int, user_id: int = 12345) -> str:
    """Build valid initData for testing."""
    user_str = json.dumps({"id": user_id, "first_name": "Test"})
    parsed = {"auth_date": str(auth_date), "user": user_str}
    data_check_parts = [f"{k}={parsed[k]}" for k in sorted(parsed.keys())]
    data_check_string = "\n".join(data_check_parts)
    secret_key = hmac.new(
        WEBAPP_DATA_CONST.encode(),
        bot_token.encode(),
        hashlib.sha256,
    ).digest()
    computed_hash = hmac.new(
        secret_key,
        data_check_string.encode(),
        hashlib.sha256,
    ).hexdigest()
    parts = [f"auth_date={quote(str(auth_date))}", f"user={quote(user_str)}", f"hash={computed_hash}"]
    return "&".join(parts)


def test_validate_empty_init_data():
    assert validate_telegram_init_data("", "token") is None
    assert validate_telegram_init_data("hash=abc", "") is None


def test_validate_missing_hash():
    assert validate_telegram_init_data("auth_date=123", "token") is None


def test_validate_invalid_hash():
    assert validate_telegram_init_data("auth_date=123&hash=badhash", "token") is None


def test_validate_expired_auth_date():
    bot_token = "test_bot_token"
    old_ts = int(time.time()) - 7200  # 2 hours ago
    init_data = _make_valid_init_data(bot_token, old_ts)
    result = validate_telegram_init_data(init_data, bot_token, auth_ttl_sec=3600)
    assert result is None


def test_validate_valid_init_data():
    bot_token = "test_bot_token"
    auth_date = int(time.time())
    init_data = _make_valid_init_data(bot_token, auth_date, user_id=999)
    result = validate_telegram_init_data(init_data, bot_token, auth_ttl_sec=3600)
    assert result is not None
    assert result.get("auth_date") == str(auth_date)
    assert "user" in result


def test_validate_allowed_user_ids_accept():
    bot_token = "test_bot_token"
    auth_date = int(time.time())
    init_data = _make_valid_init_data(bot_token, auth_date, user_id=42)
    result = validate_telegram_init_data(
        init_data, bot_token, auth_ttl_sec=3600, allowed_user_ids={42, 100}
    )
    assert result is not None


def test_validate_allowed_user_ids_reject():
    bot_token = "test_bot_token"
    auth_date = int(time.time())
    init_data = _make_valid_init_data(bot_token, auth_date, user_id=999)
    result = validate_telegram_init_data(
        init_data, bot_token, auth_ttl_sec=3600, allowed_user_ids={42, 100}
    )
    assert result is None
