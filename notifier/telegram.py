from __future__ import annotations

import logging
import time

import aiohttp

log = logging.getLogger("telegram")


def _load_tg_messages_from_db() -> list[dict]:
    """Load persisted message_ids from DB. Used at startup."""
    try:
        from api.db import get_tg_messages

        return get_tg_messages()
    except Exception as e:
        log.warning("Failed to load tg_messages from DB: %s", e)
        return []


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
        min_delta_profit_usd: float = 0.5,
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
        # Сдвиг «часов TTL» только если профит изменился ≥ этого порога (как min_delta_profit_usd_to_resend).
        self.min_delta_profit_usd = float(min_delta_profit_usd)
        # Последний профит, при котором обновляли _last_seen (для анти-дребезга по тикам).
        self._last_ttl_profit: dict[str, float] = {}

        # key -> message_id
        self._msg_ids: dict[str, int] = {}
        # key -> last_edit_ts
        self._last_edit: dict[str, float] = {}
        # key -> epoch sec последнего *видимого* обновления в TG (send/edit с новым контентом).
        # Не двигается при пропуске по rate-limit или при том же тексте — иначе TTL никогда не истекает.
        self._last_seen: dict[str, float] = {}
        # key -> last requested text/markup
        self._last_text: dict[str, str] = {}
        self._last_markup: dict[str, dict | None] = {}
        # key -> last sent text/markup
        self._last_sent_text: dict[str, str] = {}
        self._last_sent_markup: dict[str, dict | None] = {}
        # key -> stale flag
        self._stale: dict[str, bool] = {}

        # Restore from DB after restart (delete_stale survives deploy)
        for row in _load_tg_messages_from_db():
            k = row.get("key")
            mid = row.get("message_id")
            ts = row.get("ts", 0)
            if k and mid is not None:
                self._msg_ids[k] = int(mid)
                self._last_seen[k] = float(ts)
                self._stale[k] = False
        if self._msg_ids:
            log.info("Restored %d tg_messages from DB", len(self._msg_ids))
        log.info(
            "Telegram notifier: stale_ttl_sec=%.0f delete_stale=%s edit_mode=%s min_delta_ttl_profit_usd=%.4f",
            self.stale_ttl_sec,
            self.delete_stale,
            self.edit_mode,
            self.min_delta_profit_usd,
        )

    def update_stale_settings(
        self,
        stale_ttl_sec: float,
        delete_stale: bool,
        min_delta_profit_usd: float | None = None,
    ) -> None:
        """Обновить настройки устаревания (вызывается при /settings)."""
        self.stale_ttl_sec = float(stale_ttl_sec)
        self.delete_stale = bool(delete_stale)
        if min_delta_profit_usd is not None:
            self.min_delta_profit_usd = float(min_delta_profit_usd)

    def _should_bump_ttl_clock(
        self,
        key: str,
        ttl_profit_usd: float | None,
        prev_sent_text: str | None,
        new_text: str,
        is_first_send: bool,
    ) -> bool:
        """
        Сдвигать _last_seen только если первый пост, сняли баннер «устарел», или профит заметно изменился.
        Иначе котировка дергает edit каждый тик — TTL никогда не истечёт.
        """
        if is_first_send:
            if ttl_profit_usd is not None:
                self._last_ttl_profit[key] = float(ttl_profit_usd)
            return True
        if prev_sent_text and "Сигнал устарел" in prev_sent_text and "Сигнал устарел" not in new_text:
            if ttl_profit_usd is not None:
                self._last_ttl_profit[key] = float(ttl_profit_usd)
            return True
        if ttl_profit_usd is None:
            return True
        prev = self._last_ttl_profit.get(key)
        if prev is None:
            self._last_ttl_profit[key] = float(ttl_profit_usd)
            return True
        if abs(float(ttl_profit_usd) - float(prev)) >= self.min_delta_profit_usd:
            self._last_ttl_profit[key] = float(ttl_profit_usd)
            return True
        return False

    def _url(self, method: str) -> str:
        return f"{self.base}/{method}"

    async def _post(self, method: str, payload: dict) -> dict:
        async with self.session.post(self._url(method), json=payload, timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json(content_type=None)
            if not data.get("ok"):
                raise RuntimeError(f"Telegram API error {method}: {data}")
            return data

    async def _save_msg_to_db(self, key: str, message_id: int, ts: float) -> None:
        """Persist message_id to DB so it survives restart."""
        try:
            from api.db import upsert_tg_message_async

            await upsert_tg_message_async(key, message_id, int(ts))
        except Exception as e:
            log.warning("Failed to save tg_message to DB: %s", e)

    async def _remove_msg_from_db(self, key: str) -> None:
        """Remove from DB after message deleted. Keeps DB small."""
        try:
            from api.db import delete_tg_message_async

            await delete_tg_message_async(key)
        except Exception as e:
            log.warning("Failed to remove tg_message from DB: %s", e)

    async def send(
        self,
        text: str,
        reply_markup: dict | None = None,
        message_effect_id: str | None = None,
    ) -> int:
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
        if message_effect_id is not None:
            # Telegram animated message effects (premium/free) via Bot API.
            # Note: per Bot API docs this is limited to private chats.
            payload["message_effect_id"] = message_effect_id

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

    async def delete_message_for_key(self, key: str, message_id: int) -> None:
        """
        Удалить сообщение из Telegram и почистить запись из БД/кэшей по ключу.
        Используется для плановой чистки "хвостов" после сбоев/рассинхрона.
        """
        # Помечаем локально, чтобы никто не пытался редактировать
        self._stale[key] = True
        self._msg_ids.pop(key, None)
        self._last_seen.pop(key, None)
        self._last_edit.pop(key, None)
        self._last_text.pop(key, None)
        self._last_markup.pop(key, None)
        self._last_sent_text.pop(key, None)
        self._last_sent_markup.pop(key, None)
        self._last_ttl_profit.pop(key, None)

        try:
            await self.delete(int(message_id))
        finally:
            await self._remove_msg_from_db(key)

    def _make_stale_text(self, text: str, last_seen_ts: float) -> str:
        if "Сигнал устарел" in text:
            return text
        minutes = max(0, int((time.time() - last_seen_ts) // 60))
        return f"{text}\n\n⏱ <i>Сигнал устарел ({minutes} мин назад)</i>"

    async def upsert(
        self,
        key: str,
        text: str,
        reply_markup: dict | None = None,
        message_effect_id: str | None = None,
        ttl_profit_usd: float | None = None,
    ) -> None:
        now = time.time()
        self._last_text[key] = text

        if reply_markup is None:
            reply_markup = self._last_markup.get(key)
        else:
            self._last_markup[key] = reply_markup

        if not self.edit_mode:
            prev_sent = self._last_sent_text.get(key)
            first_non_edit = key not in self._msg_ids
            new_id = await self.send(text, reply_markup=reply_markup, message_effect_id=message_effect_id)
            self._msg_ids[key] = new_id
            self._last_edit[key] = now
            self._last_sent_text[key] = text
            self._last_sent_markup[key] = reply_markup
            bump = self._should_bump_ttl_clock(key, ttl_profit_usd, prev_sent, text, is_first_send=first_non_edit)
            if bump:
                self._last_seen[key] = now
                self._stale[key] = False
                await self._save_msg_to_db(key, new_id, now)
            else:
                ts = float(self._last_seen.get(key, now))
                await self._save_msg_to_db(key, new_id, ts)
            return

        last = self._last_edit.get(key, 0.0)
        if (now - last) < self.edit_min_interval_sec and key in self._msg_ids:
            return

        msg_id = self._msg_ids.get(key)
        if msg_id is None:
            new_id = await self.send(text, reply_markup=reply_markup, message_effect_id=message_effect_id)
            self._msg_ids[key] = new_id
            self._last_edit[key] = now
            self._last_sent_text[key] = text
            self._last_sent_markup[key] = reply_markup
            bump = self._should_bump_ttl_clock(key, ttl_profit_usd, None, text, is_first_send=True)
            if bump:
                self._last_seen[key] = now
                self._stale[key] = False
                await self._save_msg_to_db(key, new_id, now)
            else:
                ts = float(self._last_seen.get(key, now))
                await self._save_msg_to_db(key, new_id, ts)
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
            bump = self._should_bump_ttl_clock(key, ttl_profit_usd, prev_text, text, is_first_send=False)
            if bump:
                self._last_seen[key] = now
                self._stale[key] = False
                await self._save_msg_to_db(key, msg_id, now)
            else:
                ts = float(self._last_seen.get(key, now))
                await self._save_msg_to_db(key, msg_id, ts)
        except Exception as e:
            log.warning("edit failed for key=%s msg_id=%s: %s; invalidating, next upsert will send one new", key, msg_id, e)
            # Best-effort: если edit упал, то старое сообщение может остаться "висящим" в чате.
            # Попробуем удалить его, чтобы не копить сироты.
            try:
                await self.delete(int(msg_id))
            except Exception as del_e:
                err_str = str(del_e).lower()
                if "not found" not in err_str and "message to delete" not in err_str:
                    log.debug("edit-fail cleanup delete failed key=%s msg_id=%s: %s", key, msg_id, del_e)
            self._msg_ids.pop(key, None)
            self._last_edit.pop(key, None)
            self._last_sent_text.pop(key, None)
            self._last_sent_markup.pop(key, None)
            self._last_ttl_profit.pop(key, None)
            await self._remove_msg_from_db(key)
            # Не отправляем новое сообщение здесь — иначе при частых upsert получается спам.
            # Следующий upsert с этим key отправит одно новое сообщение (как в истории: один слот на token+direction).

    async def list_stale_cleanup_candidates_async(self) -> list[dict]:
        """
        Кандидаты на /cleanup: записи из tg_messages по TTL + слоты из памяти (_msg_ids),
        если по TTL они уже «старые». Память без строки в БД дописывается через upsert_tg_message.

        Учитывает рассинхрон: если в БД старый ts, а _last_seen (последний реальный send/edit) свежее —
        кандидатом не считается.
        """
        if self.stale_ttl_sec <= 0:
            return []
        try:
            from api.db import get_stale_tg_messages_async

            ttl = float(self.stale_ttl_sec)
            cutoff = time.time() - ttl

            db_rows = await get_stale_tg_messages_async(ttl)
            by_key: dict[str, dict] = {}

            for r in db_rows:
                k = str(r.get("key") or "")
                if not k:
                    continue
                mid = r.get("message_id")
                if mid is None:
                    continue
                ts_db = float(r.get("ts", 0))
                last_seen = float(self._last_seen.get(k, ts_db))
                if last_seen > cutoff:
                    continue
                mem_mid = self._msg_ids.get(k)
                use_mid = int(mem_mid) if mem_mid is not None else int(mid)
                by_key[k] = {"key": k, "message_id": use_mid, "ts": int(last_seen)}

            for key, m_id in list(self._msg_ids.items()):
                last_seen = float(self._last_seen.get(key, 0.0))
                if last_seen > cutoff:
                    continue
                row = {"key": key, "message_id": int(m_id), "ts": int(last_seen)}
                if key not in by_key:
                    by_key[key] = row
                    await self._save_msg_to_db(key, int(m_id), last_seen)
                elif int(m_id) != int(by_key[key]["message_id"]):
                    by_key[key] = row
                    await self._save_msg_to_db(key, int(m_id), last_seen)

            return sorted(by_key.values(), key=lambda x: int(x.get("ts", 0)))
        except Exception as e:
            log.warning("list_stale_cleanup_candidates_async failed: %s", e)
            return []

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
            stale_text = self._make_stale_text(text or "Сигнал устарел", last_seen)

            if self.delete_stale:
                try:
                    await self.delete(msg_id)
                    self._msg_ids.pop(key, None)
                    self._last_edit.pop(key, None)
                    self._last_sent_text.pop(key, None)
                    self._last_sent_markup.pop(key, None)
                    self._last_ttl_profit.pop(key, None)
                    self._stale[key] = True
                    await self._remove_msg_from_db(key)
                    continue
                except Exception as e:
                    err_str = str(e).lower()
                    if "not found" in err_str or "message to delete" in err_str:
                        self._msg_ids.pop(key, None)
                        self._last_ttl_profit.pop(key, None)
                        self._stale[key] = True
                        await self._remove_msg_from_db(key)
                        continue
                    log.warning("delete failed for key=%s msg_id=%s: %s; fallback to edit", key, msg_id, e)

            try:
                await self.edit(msg_id, stale_text, reply_markup=reply_markup)
                self._last_edit[key] = now
                self._last_sent_text[key] = stale_text
                self._last_sent_markup[key] = reply_markup
                self._stale[key] = True
                # Важно: не сбрасываем ts в БД на "время редактирования".
                # Иначе по TTL такие сообщения не будут считаться устаревшими при
                # последующей hourly/verify чистке.
                await self._save_msg_to_db(key, msg_id, float(last_seen))
            except Exception as e:
                log.warning("stale edit failed for key=%s msg_id=%s: %s; sending new", key, msg_id, e)
                try:
                    new_id = await self.send(stale_text, reply_markup=reply_markup)
                    self._msg_ids[key] = new_id
                    self._last_edit[key] = now
                    self._last_sent_text[key] = stale_text
                    self._last_sent_markup[key] = reply_markup
                    self._stale[key] = True
                    await self._save_msg_to_db(key, new_id, float(last_seen))
                except Exception as e2:
                    log.warning("stale send failed for key=%s: %s", key, e2)

    async def cleanup_stale_msgs_by_ttl(self) -> None:
        """
        Удалить сообщения в чате, которые устарели по TTL.
        Проверка "висящих" сообщений раз в час (или реже) помогает после падений
        или при других ситуациях, когда expire_stale мог только "пометить устаревшим",
        но не удалить.
        """
        if self.stale_ttl_sec <= 0:
            return

        try:
            rows = await self.list_stale_cleanup_candidates_async()
            for row in rows:
                key = row.get("key")
                msg_id = row.get("message_id")
                if not key or msg_id is None:
                    continue

                # Гасим гонку с expire_stale(): чтобы он не пытался edit/send для этого ключа.
                self._stale[key] = True
                self._msg_ids.pop(key, None)
                self._last_seen.pop(key, None)
                self._last_edit.pop(key, None)
                self._last_sent_text.pop(key, None)
                self._last_sent_markup.pop(key, None)
                self._last_ttl_profit.pop(key, None)

                try:
                    await self.delete(int(msg_id))
                except Exception as e:
                    err_str = str(e).lower()
                    if "not found" not in err_str and "message to delete" not in err_str:
                        log.warning("hourly cleanup delete failed key=%s msg_id=%s: %s", key, msg_id, e)

                # Убираем запись из БД, независимо от результата delete.
                await self._remove_msg_from_db(key)
        except Exception as e:
            log.warning("cleanup_stale_msgs_by_ttl failed: %s", e)

    async def verify_and_cleanup_stale(self) -> None:
        """
        Periodic task: for records in DB that are stale, try to delete from Telegram.
        Cleans up DB (removes record whether delete succeeds or message already gone).
        Runs rarely (e.g. every 30 min) to catch messages we missed (crash before delete).
        """
        if not self.delete_stale or self.stale_ttl_sec <= 0:
            return
        try:
            rows = await self.list_stale_cleanup_candidates_async()
            for row in rows:
                key = row.get("key")
                msg_id = row.get("message_id")
                if not key or msg_id is None:
                    continue
                try:
                    await self.delete(msg_id)
                    await self._remove_msg_from_db(key)
                    self._msg_ids.pop(key, None)
                    self._last_seen.pop(key, None)
                    self._last_ttl_profit.pop(key, None)
                    self._stale[key] = True
                    log.debug("verify_and_cleanup: deleted key=%s msg_id=%s", key, msg_id)
                except Exception as e:
                    err_str = str(e).lower()
                    if "not found" in err_str or "message to delete" in err_str:
                        await self._remove_msg_from_db(key)
                        self._msg_ids.pop(key, None)
                        self._last_seen.pop(key, None)
                        self._last_ttl_profit.pop(key, None)
                        self._stale[key] = True
                        log.debug("verify_and_cleanup: msg already gone key=%s, cleaned DB", key)
                    else:
                        log.warning("verify_and_cleanup: delete failed key=%s: %s", key, e)
        except Exception as e:
            log.warning("verify_and_cleanup failed: %s", e)
