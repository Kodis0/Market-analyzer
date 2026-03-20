"""
Telegram command handler for /settings.
Polls getUpdates and processes commands from the configured chat.
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable
from typing import Any

import aiohttp

from core.runtime_settings import RuntimeSettings, save_runtime_settings

log = logging.getLogger("commands")

TG_GET_UPDATES = "https://api.telegram.org/bot{token}/getUpdates"
TG_SEND_MESSAGE = "https://api.telegram.org/bot{token}/sendMessage"
TG_DELETE_MESSAGE = "https://api.telegram.org/bot{token}/deleteMessage"
TG_SET_MY_COMMANDS = "https://api.telegram.org/bot{token}/setMyCommands"
LIGHTNING_MESSAGE_EFFECT_ID = "5123236135417415011"


def _parse_settings_args(text: str) -> tuple[str, Any] | None:
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
    if key in ("delete_stale", "exchange_enabled"):
        return str(s).lower() in ("true", "1", "yes", "да", "on")
    if key in ("persistence_hits", "cooldown_sec", "engine_tick_hz", "max_ob_age_ms", "stale_ttl_sec"):
        return int(float(s))
    if key in (
        "bybit_taker_fee_bps",
        "solana_tx_fee_usd",
        "latency_buffer_bps",
        "usdt_usdc_buffer_bps",
        "min_profit_usd",
        "notional_usd",
        "max_cex_slippage_bps",
        "max_dex_price_impact_pct",
        "min_delta_profit_usd_to_resend",
        "price_ratio_max",
        "gross_profit_cap_pct",
        "max_spread_bps",
        "min_depth_coverage_pct",
        "jupiter_poll_interval_sec",
    ):
        return float(s)
    return s


async def _register_bot_commands(session: aiohttp.ClientSession, bot_token: str, chat_id: int | None = None) -> None:
    """Register /settings and /help in Telegram menu (shown when user types /)."""
    url = TG_SET_MY_COMMANDS.format(token=bot_token)
    commands = [
        {"command": "settings", "description": "Настройки: /settings min_profit_usd 20"},
        {"command": "exchange", "description": "Вкл/выкл биржевую логику: /exchange on|off"},
        {"command": "cleanup", "description": "Проверить висящие сигналы и удалить не-прибыльные: /cleanup"},
        {"command": "test_signal", "description": "Тестовый сигнал (авто-удаление через 1 мин)"},
        {"command": "help", "description": "Справка по параметрам"},
        {"command": "pin_setup", "description": "Отправить сообщение с кнопкой Навигация (закрепи вручную)"},
    ]
    payload: dict = {"commands": commands}
    if chat_id:
        payload["scope"] = {"type": "chat", "chat_id": chat_id}
    try:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as r:
            r.raise_for_status()
        log.info("Bot commands registered")
    except Exception as e:
        log.warning("Failed to register bot commands: %s", e)


DEFAULT_PINNED_TEXT = (
    "Навигация по единой торговой системе.\n"
    "Здесь собраны все инструменты для мониторинга арбитражных возможностей между Jupiter и Bybit.\n"
    "Нажмите кнопку ниже — откроется дашборд в Telegram (Mini App)."
)


def _make_navigation_button_payload(
    chat_id: int,
    thread_id: int | None,
    web_app_url: str | None,
    pinned_text: str | None = None,
    app_link: str | None = None,
) -> dict:
    text = (pinned_text or DEFAULT_PINNED_TEXT).strip() or DEFAULT_PINNED_TEXT
    payload: dict = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if thread_id is not None:
        payload["message_thread_id"] = thread_id

    # Ссылка t.me/бот/приложение открывается как Mini App (в т.ч. из группы). Прямой https:// — в браузере.
    button_url = (app_link or "").strip() if app_link else (web_app_url or "").strip()
    if not button_url:
        button_url = "https://t.me/AutoArbitrage0Bot/market"
    elif button_url.startswith("https://t.me/"):
        pass  # уже t.me — откроется Mini App
    elif button_url.startswith("https://"):
        pass  # прямой URL — откроется в браузере
    if button_url:
        payload["reply_markup"] = {
            "inline_keyboard": [[{"text": "НАВИГАЦИЯ", "url": button_url}]],
        }

    return payload


async def run_settings_command_handler(
    session: aiohttp.ClientSession,
    bot_token: str,
    chat_id: int,
    thread_id: int | None,
    settings: RuntimeSettings,
    settings_path: str,
    on_reload: Callable[[RuntimeSettings], None],
    stop_event: asyncio.Event,
    web_app_url: str | None = None,
    pinned_message_text: str | None = None,
    poll_interval_sec: float = 2.0,
    on_exchange_toggle: Callable[[bool], Awaitable[None]] | None = None,
    app_link: str | None = None,
    on_cleanup: Callable[[], Awaitable[str]] | None = None,
) -> None:
    """
    Poll for Telegram updates and handle /settings, /help commands.
    Only processes messages from the configured chat_id.
    """
    await _register_bot_commands(session, bot_token, chat_id)

    url_updates = TG_GET_UPDATES.format(token=bot_token)
    url_send = TG_SEND_MESSAGE.format(token=bot_token)
    offset = 0

    async def send_to_chat(
        target_chat_id: int,
        text: str,
        target_thread_id: int | None = None,
        message_effect_id: str | None = None,
    ) -> int | None:
        payload: dict = {
            "chat_id": target_chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if target_thread_id is not None:
            payload["message_thread_id"] = target_thread_id
        if message_effect_id is not None:
            payload["message_effect_id"] = message_effect_id
        try:
            async with session.post(url_send, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as r:
                j = await r.json(content_type=None)
            if j.get("ok"):
                return int((j.get("result") or {}).get("message_id") or 0)
        except Exception:
            pass
        return None

    async def send(text: str) -> int | None:
        return await send_to_chat(chat_id, text, target_thread_id=thread_id)

    async def delete_message(message_id: int) -> None:
        if not message_id:
            return
        url_delete = TG_DELETE_MESSAGE.format(token=bot_token)
        payload = {"chat_id": chat_id, "message_id": int(message_id)}
        try:
            async with session.post(url_delete, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as _:
                pass
        except Exception:
            pass

    async def send_test_signal_and_autodelete() -> None:
        payload: dict = {
            "chat_id": chat_id,
            "text": (
                "⚡️ 🚨 <b>АРБИТРАЖ</b> • <b>TEST</b>\n"
                "Маршрут: <b>Bybit → Jupiter</b>\n"
                "Объём: <code>1000 USDC</code>\n"
                "Ожидаемый выход: <code>1013.42 USDT</code>\n"
                "Чистая прибыль: <b>5.73$</b> (<b>0.57%</b>)\n"
                "Комиссии/запас: <code>7.69$</code>\n"
                "Цена на Bybit: <code>1.234567$</code>\n"
                "Цена на Jupiter: <code>1.241800$</code>\n"
                "\n"
                "<b>Последнее изменение (Telegram):</b> <code>2026-03-20 22:22:22 MSK</code>\n"
                "<b>Последняя проверка бирж:</b>\n"
                "<code>Jupiter: 2026-03-20 22:22:21 (412ms)</code>\n"
                "<code>Bybit: 2026-03-20 22:22:22 (97ms)</code>\n"
                "\n"
                "🧪 <i>Тестовый сигнал для проверки UI. Сообщение удалится через 1 минуту.</i>"
            ),
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
            "reply_markup": {
                "inline_keyboard": [
                    [
                        {"text": "🟢 Купить на Bybit", "url": "https://www.bybit.com/en/trade/spot/BTC/USDT"},
                        {"text": "🔴 Продать на Jupiter", "url": "https://jup.ag/swap/BTC-USDC"},
                    ]
                ]
            },
        }
        if thread_id is not None:
            payload["message_thread_id"] = thread_id

        url_delete = TG_DELETE_MESSAGE.format(token=bot_token)
        sent_message_id: int | None = None
        try:
            async with session.post(url_send, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as r:
                j = await r.json(content_type=None)
            if j.get("ok"):
                sent_message_id = int((j.get("result") or {}).get("message_id") or 0)
            else:
                await send(f"❌ Не удалось отправить тестовый сигнал: {j}")
                return
        except Exception as e:
            await send(f"❌ Не удалось отправить тестовый сигнал: {e}")
            return

        if not sent_message_id:
            return

        async def _del_later(mid: int) -> None:
            await asyncio.sleep(60)
            delete_payload: dict = {"chat_id": chat_id, "message_id": mid}
            try:
                async with session.post(
                    url_delete,
                    json=delete_payload,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as _:
                    pass
            except Exception:
                # silently ignore: test message is non-critical
                pass

        asyncio.create_task(_del_later(sent_message_id))

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

                text = (msg.get("text") or "").strip()
                if not text.startswith("/"):
                    continue

                cmd = text.split()[0].lower() if text.split() else ""
                cmd = re.sub(r"@\S+$", "", cmd)  # Remove @botname

                from_chat = msg.get("chat", {}) or {}
                from_chat_id = int(from_chat.get("id", 0))
                from_chat_type = str(from_chat.get("type") or "")

                # /start in private chat: greeting with animated effect (if allowed)
                if cmd == "/start" and from_chat_type == "private":
                    welcome_text = (
                        "✨ <b>Привет!</b>\n"
                        "Я бот арбитражных сигналов.\n\n"
                        "⚡️ Здесь ты можешь протестировать визуал сообщений и команды.\n"
                        "🧪 Для теста UI: <code>/test_signal</code>\n"
                        "🧹 Ручная проверка: <code>/cleanup</code>\n"
                        "⚙️ Настройки: <code>/settings</code>"
                    )
                    # Try with animated message effect first, fallback to plain send.
                    sent_mid = await send_to_chat(
                        from_chat_id,
                        welcome_text,
                        target_thread_id=None,
                        message_effect_id=LIGHTNING_MESSAGE_EFFECT_ID,
                    )
                    if not sent_mid:
                        await send_to_chat(from_chat_id, welcome_text, target_thread_id=None, message_effect_id=None)
                    continue

                if from_chat_id != chat_id:
                    continue

                # /help
                if cmd == "/help":
                    await send(RuntimeSettings.format_help())
                    continue

                # /cleanup
                if cmd == "/cleanup":
                    if on_cleanup is None:
                        await send("❌ Команда /cleanup отключена на сервере")
                        continue
                    progress_mid = await send("⏳ Запуск ручной проверки сигналов...")
                    try:
                        # Защита от зависания ручной проверки: всегда даём финальный ответ.
                        result_text = await asyncio.wait_for(on_cleanup(), timeout=180.0)
                    except asyncio.TimeoutError:
                        result_text = "⚠️ /cleanup: проверка заняла слишком много времени (таймаут 180с). Попробуй ещё раз."
                    except Exception as e:
                        result_text = f"❌ /cleanup: ошибка: {e}"
                    finally:
                        if progress_mid:
                            await delete_message(progress_mid)

                    await send(result_text)
                    continue

                # /test_signal
                if cmd == "/test_signal":
                    await send_test_signal_and_autodelete()
                    continue

                # /exchange on|off
                if cmd == "/exchange" and on_exchange_toggle is not None:
                    rest = text[len(cmd) :].strip().lower()
                    if rest in ("on", "1", "yes", "вкл", "включить"):
                        settings.exchange_enabled = True
                        save_runtime_settings(settings_path, settings)
                        await on_exchange_toggle(True)
                        await send("✅ Биржевая логика <b>включена</b> (Jupiter, Bybit, арбитраж)")
                        continue
                    if rest in ("off", "0", "no", "выкл", "выключить"):
                        settings.exchange_enabled = False
                        save_runtime_settings(settings_path, settings)
                        await on_exchange_toggle(False)
                        await send("⏸ Биржевая логика <b>выключена</b> (запросы к биржам остановлены)")
                        continue
                    status = "включена" if settings.exchange_enabled else "выключена"
                    await send(f"Биржевая логика: <b>{status}</b>\nИспользуй: /exchange on | /exchange off")
                    continue

                # /pin_setup
                if cmd == "/pin_setup":
                    url_send_full = TG_SEND_MESSAGE.format(token=bot_token)
                    pl = _make_navigation_button_payload(
                        chat_id, thread_id, web_app_url, pinned_message_text, app_link=app_link
                    )
                    try:
                        async with session.post(url_send_full, json=pl, timeout=aiohttp.ClientTimeout(total=10)) as r:
                            j = await r.json()
                        if not j.get("ok"):
                            await send(f"Ошибка: {j.get('description', 'unknown')}")
                    except Exception as e:
                        await send(f"Ошибка: {e}")
                    continue

                # /settings
                if not cmd.startswith("/settings"):
                    continue

                rest = text[len(cmd) :].strip()
                rest = re.sub(r"@\S+\s*", "", rest).strip()  # Remove @botname if present

                if not rest:
                    # Show current settings + list of parameters
                    await send(settings.format_for_telegram())
                    continue

                parsed = _parse_settings_args(rest)
                if not parsed:
                    await send(
                        "❌ Формат: /settings ключ значение\n"
                        "Пример: /settings min_profit_usd 20\n"
                        "Список параметров: /settings"
                    )
                    continue

                key, value = parsed
                if not settings.update(key, value):
                    await send(f"❌ Неизвестный параметр: {key}\nСписок: /settings")
                    continue

                save_runtime_settings(settings_path, settings)
                on_reload(settings)
                if key == "exchange_enabled" and on_exchange_toggle is not None:
                    await on_exchange_toggle(bool(settings.exchange_enabled))
                await send(f"✅ Обновлено: {settings.LABELS.get(key, key)} = {value}")

        except asyncio.CancelledError:
            break
        except Exception as e:
            log.exception("settings handler error: %s", e)
            await asyncio.sleep(poll_interval_sec)
