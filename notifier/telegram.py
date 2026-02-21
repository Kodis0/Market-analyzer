from __future__ import annotations

import logging
import time

import aiohttp

log = logging.getLogger("telegram")


class TelegramNotifier:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        bot_token: str,
        chat_id: int,
        thread_id: int | None = None,
        edit_min_interval_sec: float = 3.0,
        edit_mode: bool = True,
        stale_ttl_sec: float = 0.0,
        delete_stale: bool = False,
    ) -> None:
        self.session = session
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.thread_id = thread_id
        self.base = f"https://api.telegram.org/bot{bot_token}"
        self.edit_min_interval_sec = float(edit_min_interval_sec)
        self.edit_mode = bool(edit_mode)
        self.stale_ttl_sec = float(stale_ttl_sec)
        self.delete_stale = bool(delete_stale)

        # key -> message_id
        self._msg_ids: dict[str, int] = {}
        # key -> last_edit_ts
        self._last_edit: dict[str, float] = {}
        # key -> last_seen_ts (signal update)
        self._last_seen: dict[str, float] = {}
        # key -> last requested text/markup
        self._last_text: dict[str, str] = {}
        self._last_markup: dict[str, dict | None] = {}
        # key -> last sent text/markup
        self._last_sent_text: dict[str, str] = {}
        self._last_sent_markup: dict[str, dict | None] = {}
        # key -> stale flag
        self._stale: dict[str, bool] = {}

    def update_stale_settings(self, stale_ttl_sec: float, delete_stale: bool) -> None:
        """Обновить настройки устаревания (вызывается при /settings)."""
        self.stale_ttl_sec = float(stale_ttl_sec)
        self.delete_stale = bool(delete_stale)

    def _url(self, method: str) -> str:
        return f"{self.base}/{method}"

    async def _post(self, method: str, payload: dict) -> dict:
        async with self.session.post(self._url(method), json=payload, timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json(content_type=None)
            if not data.get("ok"):
                raise RuntimeError(f"Telegram API error {method}: {data}")
            return data

    async def send(self, text: str, reply_markup: dict | None = None) -> int:
        payload: dict = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if self.thread_id is not None:
            payload["message_thread_id"] = self.thread_id
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup

        data = await self._post("sendMessage", payload)
        msg = data.get("result") or {}
        return int(msg.get("message_id"))

    async def edit(self, message_id: int, text: str, reply_markup: dict | None = None) -> None:
        payload: dict = {
            "chat_id": self.chat_id,
            "message_id": message_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup

        await self._post("editMessageText", payload)

    async def delete(self, message_id: int) -> None:
        payload: dict = {"chat_id": self.chat_id, "message_id": message_id}
        await self._post("deleteMessage", payload)

    def _make_stale_text(self, text: str, last_seen_ts: float) -> str:
        if "Сигнал устарел" in text:
            return text
        minutes = max(0, int((time.time() - last_seen_ts) // 60))
        return f"{text}\n\n⏱ <i>Сигнал устарел ({minutes} мин назад)</i>"

    async def upsert(self, key: str, text: str, reply_markup: dict | None = None) -> None:
        now = time.time()
        self._last_seen[key] = now
        self._last_text[key] = text

        if reply_markup is None:
            reply_markup = self._last_markup.get(key)
        else:
            self._last_markup[key] = reply_markup

        self._stale[key] = False

        if not self.edit_mode:
            await self.send(text, reply_markup=reply_markup)
            return

        last = self._last_edit.get(key, 0.0)
        if (now - last) < self.edit_min_interval_sec and key in self._msg_ids:
            return

        msg_id = self._msg_ids.get(key)
        if msg_id is None:
            new_id = await self.send(text, reply_markup=reply_markup)
            self._msg_ids[key] = new_id
            self._last_edit[key] = now
            self._last_sent_text[key] = text
            self._last_sent_markup[key] = reply_markup
            return

        prev_text = self._last_sent_text.get(key)
        prev_markup = self._last_sent_markup.get(key)
        if prev_text == text and prev_markup == reply_markup:
            return

        try:
            await self.edit(msg_id, text, reply_markup=reply_markup)
            self._last_edit[key] = now
            self._last_sent_text[key] = text
            self._last_sent_markup[key] = reply_markup
        except Exception as e:
            log.warning("edit failed for key=%s msg_id=%s: %s; sending new", key, msg_id, e)
            self._msg_ids.pop(key, None)
            new_id = await self.send(text, reply_markup=reply_markup)
            self._msg_ids[key] = new_id
            self._last_edit[key] = now
            self._last_sent_text[key] = text
            self._last_sent_markup[key] = reply_markup

    async def expire_stale(self) -> None:
        if not self.edit_mode:
            return
        if self.stale_ttl_sec <= 0:
            return

        now = time.time()
        keys = list(self._last_seen.keys())
        for key in keys:
            if self._stale.get(key):
                continue
            last_seen = self._last_seen.get(key, 0.0)
            if (now - last_seen) < self.stale_ttl_sec:
                continue

            msg_id = self._msg_ids.get(key)
            if msg_id is None:
                self._stale[key] = True
                continue

            text = self._last_text.get(key, "")
            reply_markup = self._last_markup.get(key)
            stale_text = self._make_stale_text(text, last_seen)

            if self.delete_stale:
                try:
                    await self.delete(msg_id)
                    self._msg_ids.pop(key, None)
                    self._last_edit.pop(key, None)
                    self._last_sent_text.pop(key, None)
                    self._last_sent_markup.pop(key, None)
                    self._stale[key] = True
                    continue
                except Exception as e:
                    log.warning("delete failed for key=%s msg_id=%s: %s; fallback to edit", key, msg_id, e)

            try:
                await self.edit(msg_id, stale_text, reply_markup=reply_markup)
                self._last_edit[key] = now
                self._last_sent_text[key] = stale_text
                self._last_sent_markup[key] = reply_markup
                self._stale[key] = True
            except Exception as e:
                log.warning("stale edit failed for key=%s msg_id=%s: %s; sending new", key, msg_id, e)
                try:
                    new_id = await self.send(stale_text, reply_markup=reply_markup)
                    self._msg_ids[key] = new_id
                    self._last_edit[key] = now
                    self._last_sent_text[key] = stale_text
                    self._last_sent_markup[key] = reply_markup
                    self._stale[key] = True
                except Exception as e2:
                    log.warning("stale send failed for key=%s: %s", key, e2)
