"""
Отправляет сообщение с кнопкой «НАВИГАЦИЯ» в группу.
Закрепи сообщение вручную (долгое нажатие → Закрепить).

Перед запуском:
1. Задеплой папку webapp/ на https://app.netlify.com/drop (перетащи папку)
2. Добавь полученный URL в config.yaml: telegram.web_app_url
"""

from __future__ import annotations

import argparse
import asyncio
import os

import aiohttp
import yaml
from dotenv import load_dotenv

load_dotenv()

TG_SEND = "https://api.telegram.org/bot{token}/sendMessage"


def load_config(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


async def main() -> None:
    p = argparse.ArgumentParser(description="Отправить сообщение с кнопкой Навигация")
    p.add_argument("-c", "--config", default="config.yaml")
    args = p.parse_args()

    cfg = load_config(args.config)
    tg = cfg.get("telegram") or {}
    chat_id = tg.get("chat_id")
    thread_id = tg.get("thread_id")
    web_app_url = tg.get("web_app_url") or os.environ.get("WEB_APP_URL")
    pinned_text = tg.get("pinned_message_text") or (
        "Навигация по единой торговой системе.\n"
        "Здесь собраны все инструменты для мониторинга арбитражных возможностей между Jupiter и Bybit.\n"
        "Нажмите кнопку ниже для доступа к настройкам и актуальной информации."
    )

    token = os.environ.get("TG_BOT_TOKEN")
    if not token:
        raise SystemExit("TG_BOT_TOKEN не задан в .env")
    if not chat_id:
        raise SystemExit("telegram.chat_id не задан в config.yaml")

    payload = {
        "chat_id": chat_id,
        "text": (pinned_text or "").strip() or "Навигация.",
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if thread_id is not None:
        payload["message_thread_id"] = thread_id

    if web_app_url and str(web_app_url).strip().startswith("https://"):
        payload["reply_markup"] = {
            "inline_keyboard": [
                [
                    {"text": "НАВИГАЦИЯ", "url": str(web_app_url).strip()},
                ]
            ],
        }
    else:
        payload["reply_markup"] = {
            "inline_keyboard": [
                [
                    {"text": "НАВИГАЦИЯ", "url": "https://t.me/BotFather"},
                ]
            ],
        }
        payload["text"] = "Добавь web_app_url в config.yaml и задеплой webapp/ на netlify.com/drop"

    async with aiohttp.ClientSession() as session:
        async with session.post(TG_SEND.format(token=token), json=payload) as r:
            j = await r.json()

    if not j.get("ok"):
        raise SystemExit(f"Ошибка: {j.get('description', j)}")


if __name__ == "__main__":
    asyncio.run(main())
