"""
Разбор HTML-текста арбитражного сигнала из Telegram (как в core.arb.engine).
Используется в /cleanup: монеты и направление из самого сообщения → те же запросы, что у движка.
"""

from __future__ import annotations

import re


# Как в engine: Маршрут: <b>Jupiter → Bybit</b> / <b>Bybit → Jupiter</b> (стрелка Unicode или ->)
_RE_ROUTE_JUP_BYBIT = re.compile(
    r"Маршрут:\s*<b>\s*Jupiter\s*(?:→|->)\s*Bybit\s*</b>",
    re.IGNORECASE | re.DOTALL,
)
_RE_ROUTE_BYBIT_JUP = re.compile(
    r"Маршрут:\s*<b>\s*Bybit\s*(?:→|->)\s*Jupiter\s*</b>",
    re.IGNORECASE | re.DOTALL,
)
# Токен: «АРБИТРАЖ</b> • <b>KEY</b>» или варианты с пробелами
_RE_TOKEN = re.compile(
    r"АРБИТРАЖ\s*</b>\s*•\s*<b>\s*([^<]+?)\s*</b>",
    re.IGNORECASE | re.DOTALL,
)


def parse_arb_signal_from_message(message_html: str) -> tuple[str, str] | None:
    """
    Возвращает (token_key, direction), direction ∈ {JUP->BYBIT, BYBIT->JUP}, или None.

    Ожидается разметка как у ArbEngine (HTML + Маршрут).
    """
    if not message_html or not message_html.strip():
        return None

    m_tok = _RE_TOKEN.search(message_html)
    if not m_tok:
        return None
    token_key = m_tok.group(1).strip()
    if not token_key:
        return None

    if _RE_ROUTE_JUP_BYBIT.search(message_html):
        return (token_key, "JUP->BYBIT")
    if _RE_ROUTE_BYBIT_JUP.search(message_html):
        return (token_key, "BYBIT->JUP")

    return None
