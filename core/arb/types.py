
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import List, Tuple, Optional

ButtonRow = List[Tuple[str, str]]
Buttons = List[ButtonRow]


@dataclass
class Signal:
    key: str
    token: str
    direction: str  # "JUP->BYBIT" | "BYBIT->JUP"
    profit_usd: Decimal
    notional_usd: Decimal
    text: str
    buttons: Optional[Buttons] = None

    def to_reply_markup(self) -> dict | None:
        if not self.buttons:
            return None
        return {
            "inline_keyboard": [
                [{"text": title, "url": url} for (title, url) in row]
                for row in self.buttons
            ]
        }
