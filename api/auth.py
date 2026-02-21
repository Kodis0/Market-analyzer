"""
Telegram Mini App initData validation.
Validates X-Telegram-Init-Data header using HMAC-SHA256.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
from typing import Optional, Set
from urllib.parse import unquote

log = logging.getLogger(__name__)

WEBAPP_DATA_CONST = "WebAppData"


def validate_telegram_init_data(
    init_data: str,
    bot_token: str,
    auth_ttl_sec: int = 3600,
    allowed_user_ids: Optional[Set[int]] = None,
) -> Optional[dict]:
    """
    Validate Telegram Web App initData.
    Returns parsed data dict if valid, None otherwise.
    """
    if not init_data or not bot_token:
        return None

    try:
        parsed: dict[str, str] = {}
        received_hash = ""

        for part in init_data.split("&"):
            if "=" not in part:
                continue
            key, value = part.split("=", 1)
            if key == "hash":
                received_hash = value
            else:
                parsed[key] = unquote(value)

        if not received_hash:
            log.warning("api auth: initData missing hash")
            return None

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

        if not hmac.compare_digest(computed_hash, received_hash):
            log.warning("api auth: initData hash mismatch")
            return None

        auth_date = parsed.get("auth_date")
        if auth_date:
            try:
                ts = int(auth_date)
                if time.time() - ts > auth_ttl_sec:
                    log.warning("api auth: initData expired (auth_date too old)")
                    return None
            except (TypeError, ValueError):
                log.warning("api auth: invalid auth_date")
                return None

        if allowed_user_ids is not None and len(allowed_user_ids) > 0:
            user_str = parsed.get("user")
            if not user_str:
                log.warning("api auth: allowed_user_ids set but no user in initData")
                return None
            try:
                user_data = json.loads(user_str)
                user_id = user_data.get("id")
                if user_id is None:
                    log.warning("api auth: user object missing id")
                    return None
                if int(user_id) not in allowed_user_ids:
                    log.warning("api auth: user_id %s not in allowlist", user_id)
                    return None
            except (json.JSONDecodeError, TypeError, ValueError) as e:
                log.warning("api auth: failed to parse user: %s", e)
                return None

        return parsed
    except Exception as e:
        log.exception("api auth: validation error: %s", e)
        return None
