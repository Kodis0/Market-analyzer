"""
Telegram command handler for /settings.
Polls getUpdates and processes commands from the configured chat.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from collections.abc import Awaitable, Callable
from typing import Any

import aiohttp

from core.runtime_settings import RuntimeSettings, save_runtime_settings

log = logging.getLogger("commands")

TG_GET_UPDATES = "https://api.telegram.org/bot{token}/getUpdates"
TG_SEND_MESSAGE = "https://api.telegram.org/bot{token}/sendMessage"
TG_DELETE_MESSAGE = "https://api.telegram.org/bot{token}/deleteMessage"
TG_EDIT_MESSAGE_TEXT = "https://api.telegram.org/bot{token}/editMessageText"
TG_SET_MY_COMMANDS = "https://api.telegram.org/bot{token}/setMyCommands"

LIGHTNING_MESSAGE_EFFECT_ID = "5123236135417415011"

CUSTOM_ALERT_EMOJI_HTML = '<tg-emoji emoji-id="5420323339723881652">🚨</tg-emoji>'
CUSTOM_GREEN_EMOJI_HTML = '<tg-emoji emoji-id="5416081784641168838">🟢</tg-emoji>'
CUSTOM_RED_EMOJI_HTML = '<tg-emoji emoji-id="5411225014148014586">🔴</tg-emoji>'

CUSTOM_ERROR_EMOJI_HTML = '<tg-emoji emoji-id="5210952531676504517">❌</tg-emoji>'
CUSTOM_SUCCESS_EMOJI_HTML = '<tg-emoji emoji-id="5206607081334906820">✅</tg-emoji>'
CUSTOM_WARNING_EMOJI_HTML = '<tg-emoji emoji-id="5447644880824181073">⚠️</tg-emoji>'
CUSTOM_REFRESH_EMOJI_HTML = '<tg-emoji emoji-id="5386367538735104399">🔄</tg-emoji>'
CUSTOM_HOURGLASS_EMOJI_HTML = '<tg-emoji emoji-id="5231012545799666522">⏳</tg-emoji>'
CUSTOM_SPARKLES_EMOJI_HTML = '<tg-emoji emoji-id="5325547803936572038">✨</tg-emoji>'
CUSTOM_TEST_EMOJI_HTML = '<tg-emoji emoji-id="5452069934089641166">🧪</tg-emoji>'

# InlineKeyboardButton: style + icon_custom_emoji_id (Bot API; нужен клиент с поддержкой и
# Premium/Fragment для icon_custom_emoji_id — см. документацию Telegram).
DEFAULT_MINI_APP_URL = "https://t.me/AutoArbitrage0Bot/market"


def _inline_kb_button(
    text: str,
    *,
    url: str | None = None,
    callback_data: str | None = None,
    style: str | None = None,
    icon_custom_emoji_id: str | None = None,
) -> dict[str, Any]:
    """Build one InlineKeyboardButton dict for Telegram Bot API JSON."""
    btn: dict[str, Any] = {"text": text}
    if url is not None:
        btn["url"] = url
    if callback_data is not None:
        btn["callback_data"] = callback_data
    if style:
        btn["style"] = style
    if icon_custom_emoji_id:
        btn["icon_custom_emoji_id"] = icon_custom_emoji_id
    return btn


def _resolve_dashboard_url(web_app_url: str | None, app_link: str | None) -> str:
    button_url = (app_link or "").strip() if app_link else (web_app_url or "").strip()
    return button_url or DEFAULT_MINI_APP_URL


def _parse_settings_args(text: str) -> tuple[str, Any] | None:
    """
    Parse /settings key value or /settings key=value.
    Returns (key, value) or None if invalid.
    """
    text = (text or "").strip()
    if not text:
        return None

    if "=" in text:
        key, val_str = text.split("=", 1)
        key = key.strip().lower()
        val_str = val_str.strip()
        if key and val_str:
            return key, _parse_value(key, val_str)

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

    if key in (
        "persistence_hits",
        "cooldown_sec",
        "engine_tick_hz",
        "max_ob_age_ms",
        "stale_ttl_sec",
    ):
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


async def _register_bot_commands(
    session: aiohttp.ClientSession,
    bot_token: str,
    chat_id: int | None = None,
) -> None:
    """Register bot commands in Telegram menu."""
    url = TG_SET_MY_COMMANDS.format(token=bot_token)
    commands = [
        {"command": "settings", "description": "Настройки: /settings min_profit_usd 20"},
        {"command": "exchange", "description": "Вкл/выкл биржевую логику: /exchange on|off"},
        {
            "command": "cleanup",
            "description": "Проверка по TTL — /cleanup; все слоты — /cleanup all",
        },
        {"command": "test_signal", "description": "Тестовый сигнал (авто-удаление через 1 мин)"},
        {"command": "help", "description": "Справка по параметрам"},
        {"command": "pin_setup", "description": "Отправить сообщение с кнопкой Навигация"},
    ]

    payload: dict[str, Any] = {"commands": commands}
    if chat_id:
        payload["scope"] = {"type": "chat", "chat_id": chat_id}

    try:
        async with session.post(
            url,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as response:
            data = await response.json(content_type=None)
            if not data.get("ok", False):
                log.warning("setMyCommands failed: %s", data)
                return
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
) -> dict[str, Any]:
    text = (pinned_text or DEFAULT_PINNED_TEXT).strip() or DEFAULT_PINNED_TEXT

    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    if thread_id is not None:
        payload["message_thread_id"] = thread_id

    button_url = _resolve_dashboard_url(web_app_url, app_link)

    payload["reply_markup"] = {
        "inline_keyboard": [
            [
                _inline_kb_button(
                    "НАВИГАЦИЯ",
                    url=button_url,
                    style="primary",
                    icon_custom_emoji_id="5325547803936572038",
                ),
            ],
        ],
    }
    return payload


def _build_welcome_markup(web_app_url: str | None, app_link: str | None) -> dict[str, Any]:
    """Colored inline keyboard for private /start (Bot API style + custom emoji icons)."""
    dash = _resolve_dashboard_url(web_app_url, app_link)
    bot_profile = "https://t.me/AutoArbitrage0Bot"
    return {
        "inline_keyboard": [
            [
                _inline_kb_button(
                    "Info",
                    url=bot_profile,
                    style="primary",
                    icon_custom_emoji_id="6028435952299413210",
                ),
                _inline_kb_button(
                    "Дашборд",
                    url=dash,
                    style="success",
                    icon_custom_emoji_id="5416081784641168838",
                ),
            ],
            [
                _inline_kb_button(
                    "Bybit",
                    url="https://www.bybit.com/en/trade/spot/BTC/USDT",
                    style="danger",
                    icon_custom_emoji_id="5411225014148014586",
                ),
                _inline_kb_button(
                    "Jupiter",
                    url="https://jup.ag/",
                    style="primary",
                    icon_custom_emoji_id="5372878077250519677",
                ),
            ],
        ],
    }


def _build_test_signal_markup() -> dict[str, Any]:
    """Colored inline keyboard for test signal (plain Bot API JSON, no aiogram)."""
    return {
        "inline_keyboard": [
            [
                _inline_kb_button(
                    "Купить на Bybit",
                    url="https://www.bybit.com/en/trade/spot/BTC/USDT",
                    style="success",
                    icon_custom_emoji_id="5416081784641168838",
                ),
                _inline_kb_button(
                    "Продать на Jupiter",
                    url="https://jup.ag/swap/BTC-USDC",
                    style="danger",
                    icon_custom_emoji_id="5411225014148014586",
                ),
            ],
        ],
    }


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
    on_cleanup: Callable[..., Awaitable[str]] | None = None,
) -> None:
    """
    Poll for Telegram updates and handle bot commands.
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
        reply_markup: dict[str, Any] | None = None,
    ) -> int | None:
        payload: dict[str, Any] = {
            "chat_id": target_chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }

        if target_thread_id is not None:
            payload["message_thread_id"] = target_thread_id
        if message_effect_id is not None:
            payload["message_effect_id"] = message_effect_id
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup

        try:
            async with session.post(
                url_send,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as response:
                data = await response.json(content_type=None)

            if data.get("ok"):
                return int((data.get("result") or {}).get("message_id") or 0)

            log.warning("Telegram sendMessage failed. payload=%s response=%s", payload, data)
        except Exception as e:
            log.exception("send_to_chat error: %s", e)

        return None

    async def send(text: str, reply_markup: dict[str, Any] | None = None) -> int | None:
        return await send_to_chat(
            chat_id,
            text,
            target_thread_id=thread_id,
            reply_markup=reply_markup,
        )

    async def delete_message(message_id: int) -> None:
        if not message_id:
            return

        url_delete = TG_DELETE_MESSAGE.format(token=bot_token)
        payload = {"chat_id": chat_id, "message_id": int(message_id)}

        try:
            async with session.post(
                url_delete,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as response:
                data = await response.json(content_type=None)
                if not data.get("ok", False):
                    log.warning("deleteMessage failed: %s", data)
        except Exception as e:
            log.warning("delete_message error: %s", e)

    async def edit_message(message_id: int, text: str) -> None:
        if not message_id:
            return

        url_edit = TG_EDIT_MESSAGE_TEXT.format(token=bot_token)
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "message_id": int(message_id),
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }

        try:
            async with session.post(
                url_edit,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as response:
                data = await response.json(content_type=None)
                if not data.get("ok", False):
                    log.warning("editMessageText failed: %s", data)
        except Exception as e:
            log.warning("edit_message error: %s", e)

    async def send_test_signal_and_autodelete(command_message_id: int | None = None) -> None:
        try:
            text = (
                f"{CUSTOM_ALERT_EMOJI_HTML} <b>АРБИТРАЖ</b> • <b>TEST</b>\n"
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
                f"{CUSTOM_TEST_EMOJI_HTML} <i>Тестовый сигнал для проверки UI. Сообщение удалится через 1 минуту.</i>"
            )

            sent_message_id = await send(text, reply_markup=_build_test_signal_markup())
            if not sent_message_id:
                await send(f"{CUSTOM_ERROR_EMOJI_HTML} Не удалось отправить тестовый сигнал")
                return

            async def _del_later(mid: int, user_cmd_mid: int | None) -> None:
                await asyncio.sleep(60)
                await delete_message(mid)
                if user_cmd_mid:
                    await delete_message(int(user_cmd_mid))

            asyncio.create_task(_del_later(sent_message_id, command_message_id))

        except Exception as e:
            log.exception("send_test_signal_and_autodelete error: %s", e)
            await send(f"{CUSTOM_ERROR_EMOJI_HTML} Ошибка /test_signal: {e}")

    while not stop_event.is_set():
        try:
            params = {"offset": offset, "timeout": 30}

            async with session.get(
                url_updates,
                params=params,
                timeout=aiohttp.ClientTimeout(total=35),
            ) as response:
                data = await response.json(content_type=None)

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
                cmd = re.sub(r"@\S+$", "", cmd)

                from_chat = msg.get("chat", {}) or {}
                from_chat_id = int(from_chat.get("id", 0))
                from_chat_type = str(from_chat.get("type") or "")

                if cmd == "/start" and from_chat_type == "private":
                    welcome_text = (
                        f"{CUSTOM_SPARKLES_EMOJI_HTML} <b>Привет!</b>\n"
                        "Я бот арбитражных сигналов.\n\n"
                    )
                    welcome_markup = _build_welcome_markup(web_app_url, app_link)
                    sent_mid = await send_to_chat(
                        from_chat_id,
                        welcome_text,
                        target_thread_id=None,
                        message_effect_id=LIGHTNING_MESSAGE_EFFECT_ID,
                        reply_markup=welcome_markup,
                    )
                    if not sent_mid:
                        await send_to_chat(from_chat_id, welcome_text, reply_markup=welcome_markup)
                    continue

                if from_chat_id != chat_id:
                    continue

                if cmd == "/help":
                    await send(RuntimeSettings.format_help())
                    continue

                if cmd == "/cleanup":
                    if on_cleanup is None:
                        await send(f"{CUSTOM_ERROR_EMOJI_HTML} Команда /cleanup отключена на сервере")
                        continue

                    parts = text.split(maxsplit=1)
                    only_ttl_stale = True
                    if len(parts) > 1 and parts[1].strip().lower() in ("all", "все", "full"):
                        only_ttl_stale = False

                    progress_mid = await send(
                        f"{CUSTOM_HOURGLASS_EMOJI_HTML} Запуск ручной проверки сигналов"
                        f"({'все отслеживаемые слоты' if not only_ttl_stale else 'по TTL'})..."
                    )
                    last_edit_ts = 0.0

                    async def set_progress(
                        progress_text: str, *, force: bool = False
                    ) -> None:
                        nonlocal last_edit_ts
                        if not progress_mid:
                            return

                        now = time.time()
                        # Не душим финальный ответ: быстрый /cleanup (<1.2с) иначе оставляет «Подожди» навсегда.
                        if not force and (now - last_edit_ts) < 1.2:
                            return

                        last_edit_ts = now
                        try:
                            await edit_message(progress_mid, progress_text)
                        except Exception:
                            pass

                    await set_progress(
                        f"{CUSTOM_REFRESH_EMOJI_HTML} Идёт ручная проверка сигналов...\n"
                        "Пожалуйста, подожди."
                    )

                    try:
                        result_text = await asyncio.wait_for(
                            on_cleanup(set_progress, only_ttl_stale=only_ttl_stale),
                            timeout=180.0,
                        )
                        await set_progress(result_text, force=True)
                        if not progress_mid:
                            await send(result_text)

                    except asyncio.TimeoutError:
                        result_text = (
                            f"{CUSTOM_WARNING_EMOJI_HTML} /cleanup: проверка заняла слишком много времени "
                            "(таймаут 180с). Попробуй ещё раз."
                        )
                        await set_progress(result_text, force=True)
                        if not progress_mid:
                            await send(result_text)

                    except Exception as e:
                        result_text = f"{CUSTOM_ERROR_EMOJI_HTML} /cleanup: ошибка: {e}"
                        await set_progress(result_text, force=True)
                        if not progress_mid:
                            await send(result_text)

                    continue

                if cmd == "/test_signal":
                    await send_test_signal_and_autodelete(int(msg.get("message_id") or 0))
                    continue

                if cmd == "/exchange" and on_exchange_toggle is not None:
                    rest = text[len(cmd):].strip().lower()

                    if rest in ("on", "1", "yes", "вкл", "включить"):
                        settings.exchange_enabled = True
                        save_runtime_settings(settings_path, settings)
                        await on_exchange_toggle(True)
                        await send(
                            f"{CUSTOM_SUCCESS_EMOJI_HTML} Биржевая логика <b>включена</b> "
                            "(Jupiter, Bybit, арбитраж)"
                        )
                        continue

                    if rest in ("off", "0", "no", "выкл", "выключить"):
                        settings.exchange_enabled = False
                        save_runtime_settings(settings_path, settings)
                        await on_exchange_toggle(False)
                        await send(
                            "⏸ Биржевая логика <b>выключена</b> "
                            "(запросы к биржам остановлены)"
                        )
                        continue

                    status = "включена" if settings.exchange_enabled else "выключена"
                    await send(
                        f"Биржевая логика: <b>{status}</b>\n"
                        "Используй: /exchange on | /exchange off"
                    )
                    continue

                if cmd == "/pin_setup":
                    payload = _make_navigation_button_payload(
                        chat_id=chat_id,
                        thread_id=thread_id,
                        web_app_url=web_app_url,
                        pinned_text=pinned_message_text,
                        app_link=app_link,
                    )
                    try:
                        async with session.post(
                            url_send,
                            json=payload,
                            timeout=aiohttp.ClientTimeout(total=10),
                        ) as response:
                            data = await response.json(content_type=None)

                        if not data.get("ok"):
                            await send(
                                f"{CUSTOM_ERROR_EMOJI_HTML} Ошибка: "
                                f"{data.get('description', 'unknown')}"
                            )
                    except Exception as e:
                        await send(f"{CUSTOM_ERROR_EMOJI_HTML} Ошибка: {e}")
                    continue

                if not cmd.startswith("/settings"):
                    continue

                rest = text[len(cmd):].strip()
                rest = re.sub(r"@\S+\s*", "", rest).strip()

                if not rest:
                    await send(settings.format_for_telegram())
                    continue

                parsed = _parse_settings_args(rest)
                if not parsed:
                    await send(
                        f"{CUSTOM_ERROR_EMOJI_HTML} Формат: /settings ключ значение\n"
                        "Пример: /settings min_profit_usd 20\n"
                        "Список параметров: /settings"
                    )
                    continue

                key, value = parsed
                if not settings.update(key, value):
                    await send(
                        f"{CUSTOM_ERROR_EMOJI_HTML} Неизвестный параметр: {key}\n"
                        "Список: /settings"
                    )
                    continue

                save_runtime_settings(settings_path, settings)
                on_reload(settings)

                if key == "exchange_enabled" and on_exchange_toggle is not None:
                    await on_exchange_toggle(bool(settings.exchange_enabled))

                await send(
                    f"{CUSTOM_SUCCESS_EMOJI_HTML} "
                    f"Обновлено: {settings.LABELS.get(key, key)} = {value}"
                )

        except asyncio.CancelledError:
            break
        except Exception as e:
            log.exception("settings handler error: %s", e)
            await asyncio.sleep(poll_interval_sec)