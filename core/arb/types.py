from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

ButtonRow = list[tuple[str, str]]
Buttons = list[ButtonRow]

# Как в notifier/commands.py (тестовый сигнал): зелёный — Bybit, красный — Jupiter.
_ICON_BYBIT = "5416081784641168838"
_ICON_JUPITER = "5411225014148014586"


def _styled_inline_button(title: str, url: str) -> dict[str, Any]:
    """InlineKeyboardButton с style + icon_custom_emoji_id (Bot API)."""
    btn: dict[str, Any] = {"text": title, "url": url}
    t = title.lower()
    if "купить" in t or t.startswith("buy"):
        btn["style"] = "success"
    elif "продать" in t or t.startswith("sell"):
        btn["style"] = "danger"
    if "jupiter" in t:
        btn["icon_custom_emoji_id"] = _ICON_JUPITER
    elif "bybit" in t:
        btn["icon_custom_emoji_id"] = _ICON_BYBIT
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
