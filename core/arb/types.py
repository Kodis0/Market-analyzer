from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

ButtonRow = list[tuple[str, str]]
Buttons = list[ButtonRow]

# Зелёный шарик — «Купить», красный — «Продать» (как в тестовом сигнале в commands.py).
_ICON_BUY_GREEN = "5416081784641168838"
_ICON_SELL_RED = "5411225014148014586"


def _styled_inline_button(title: str, url: str) -> dict[str, Any]:
    """InlineKeyboardButton с style + icon_custom_emoji_id (Bot API)."""
    btn: dict[str, Any] = {"text": title, "url": url}
    t = title.lower().strip()
    if "купить" in t or t.startswith("buy"):
        btn["style"] = "success"
        btn["icon_custom_emoji_id"] = _ICON_BUY_GREEN
    elif "продать" in t or t.startswith("sell"):
        btn["style"] = "danger"
        btn["icon_custom_emoji_id"] = _ICON_SELL_RED
    return btn


@dataclass
class Signal:
    key: str
    token: str
    direction: str  # "JUP->BYBIT" | "BYBIT->JUP"
    profit_usd: Decimal
    notional_usd: Decimal
    text: str
    buttons: Buttons | None = None

    def to_reply_markup(self) -> dict | None:
        if not self.buttons:
            return None
        return {
            "inline_keyboard": [
                [_styled_inline_button(title, url) for (title, url) in row] for row in self.buttons
            ],
        }
