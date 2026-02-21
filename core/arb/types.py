from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

ButtonRow = list[tuple[str, str]]
Buttons = list[ButtonRow]


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
        return {"inline_keyboard": [[{"text": title, "url": url} for (title, url) in row] for row in self.buttons]}
