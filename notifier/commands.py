"""
Telegram command handler for /settings.
Polls getUpdates and processes commands from the configured chat.
"""
from __future__ import annotations

import asyncio
import logging
import re
from typing import Any, Callable, Optional

import aiohttp

from core.runtime_settings import RuntimeSettings, save_runtime_settings

log = logging.getLogger("commands")

TG_GET_UPDATES = "https://api.telegram.org/bot{token}/getUpdates"
TG_SEND_MESSAGE = "https://api.telegram.org/bot{token}/sendMessage"
TG_SET_MY_COMMANDS = "https://api.telegram.org/bot{token}/setMyCommands"


def _parse_settings_args(text: str) -> Optional[tuple[str, Any]]:
    """
    Parse /settings key value or /settings key=value.
    Returns (key, value) or None if invalid.
    """
    text = (text or "").strip()
    if not text:
        return None

    # key=value
    if "=" in text:
        parts = text.split("=", 1)
        if len(parts) == 2:
            key = parts[0].strip().lower()
            val_str = parts[1].strip()
            return key, _parse_value(key, val_str)

    # key value
    parts = text.split(maxsplit=1)
    if len(parts) == 2:
        key = parts[0].strip().lower()
        val_str = parts[1].strip()
        return key, _parse_value(key, val_str)

    return None


def _parse_value(key: str, s: str) -> Any:
    """Parse string value to appropriate type for the setting."""
    if key in ("persistence_hits", "cooldown_sec", "engine_tick_hz", "max_ob_age_ms"):
        return int(float(s))
    if key in (
        "bybit_taker_fee_bps", "solana_tx_fee_usd", "latency_buffer_bps",
        "usdt_usdc_buffer_bps", "min_profit_usd", "notional_usd",
        "max_cex_slippage_bps", "max_dex_price_impact_pct",
        "min_delta_profit_usd_to_resend", "price_ratio_max", "gross_profit_cap_pct",
        "max_spread_bps", "min_depth_coverage_pct",
        "jupiter_poll_interval_sec",
    ):
        return float(s)
    return s


async def _register_bot_commands(session: aiohttp.ClientSession, bot_token: str) -> None:
    """Register /settings and /help in Telegram menu (shown when user types /)."""
    url = TG_SET_MY_COMMANDS.format(token=bot_token)
    commands = [
        {"command": "settings", "description": "Настройки: /settings min_profit_usd 20"},
        {"command": "help", "description": "Справка по параметрам"},
    ]
    try:
        async with session.post(url, json={"commands": commands}, timeout=aiohttp.ClientTimeout(total=10)) as r:
            r.raise_for_status()
        log.info("Bot commands registered")
    except Exception as e:
        log.warning("Failed to register bot commands: %s", e)


async def run_settings_command_handler(
    session: aiohttp.ClientSession,
    bot_token: str,
    chat_id: int,
    thread_id: Optional[int],
    settings: RuntimeSettings,
    settings_path: str,
    on_reload: Callable[[RuntimeSettings], None],
    stop_event: asyncio.Event,
    poll_interval_sec: float = 2.0,
) -> None:
    """
    Poll for Telegram updates and handle /settings, /help commands.
    Only processes messages from the configured chat_id.
    """
    await _register_bot_commands(session, bot_token)

    url_updates = TG_GET_UPDATES.format(token=bot_token)
    url_send = TG_SEND_MESSAGE.format(token=bot_token)
    offset = 0

    async def send(text: str) -> None:
        payload: dict = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if thread_id is not None:
            payload["message_thread_id"] = thread_id
        async with session.post(url_send, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as _:
            pass

    while not stop_event.is_set():
        try:
            params = {"offset": offset, "timeout": 30}
            async with session.get(url_updates, params=params, timeout=aiohttp.ClientTimeout(total=35)) as r:
                data = await r.json()

            if not data.get("ok"):
                log.warning("getUpdates error: %s", data)
                await asyncio.sleep(poll_interval_sec)
                continue

            for upd in data.get("result", []) or []:
                offset = upd.get("update_id", 0) + 1

                msg = upd.get("message") or upd.get("edited_message")
                if not msg:
                    continue

                from_chat = msg.get("chat", {}) or {}
                if int(from_chat.get("id", 0)) != chat_id:
                    continue

                text = (msg.get("text") or "").strip()
                if not text.startswith("/settings"):
                    continue

                # /settings or /settings@botname
                rest = text[8:].strip()
                rest = re.sub(r"@\S+\s*", "", rest).strip()  # Remove @botname if present

                if not rest:
                    # Show current settings
                    await send(settings.format_for_telegram())
                    continue

                parsed = _parse_settings_args(rest)
                if not parsed:
                    await send("❌ Формат: /settings ключ значение\nПример: /settings min_profit_usd 15")
                    continue

                key, value = parsed
                if not settings.update(key, value):
                    await send(f"❌ Неизвестный параметр: {key}")
                    continue

                save_runtime_settings(settings_path, settings)
                on_reload(settings)
                await send(f"✅ Обновлено: {settings.LABELS.get(key, key)} = {value}")

        except asyncio.CancelledError:
            break
        except Exception as e:
            log.exception("settings handler error: %s", e)
            await asyncio.sleep(poll_interval_sec)
