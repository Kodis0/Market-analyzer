
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, List, Set

DEFAULT_DENYLIST_SYMBOLS = {
    "XAUT", "PAXG",
    "AAPLX", "GOOGLX", "TSLAX", "NVDAX", "CRCLX", "HOODX",
}
DEFAULT_DENYLIST_REGEX = [
    r"^(1000|10000|100000)[A-Z0-9]+$",  # multiplier symbols like 1000BONK
]


def _normalize_bybit_base(bybit_symbol: str) -> str:
    s = (bybit_symbol or "").upper().strip()
    for q in ("USDT", "USDC", "USD"):
        if s.endswith(q):
            return s[: -len(q)]
    return s


@dataclass(frozen=True)
class Denylist:
    symbols: Set[str]
    regex: List[re.Pattern[str]]

    @classmethod
    def build(cls, symbols: Iterable[str] | None = None, regex: Iterable[str] | None = None) -> "Denylist":
        deny_syms = {s.upper() for s in (symbols or [])}
        deny_syms |= DEFAULT_DENYLIST_SYMBOLS

        pats: List[re.Pattern[str]] = []
        for rx in (regex or []):
            try:
                pats.append(re.compile(rx, re.IGNORECASE))
            except re.error:
                # silently ignore bad regex; caller may log if needed
                continue
        for rx in DEFAULT_DENYLIST_REGEX:
            pats.append(re.compile(rx, re.IGNORECASE))

        return cls(symbols=deny_syms, regex=pats)

    def is_denied(self, token_key: str, bybit_symbol: str) -> bool:
        base = _normalize_bybit_base(bybit_symbol)
        candidates = [token_key, base, bybit_symbol]
        for c in candidates:
            if not c:
                continue
            u = str(c).upper()
            if u in self.symbols:
                return True
            for rx in self.regex:
                if rx.search(u):
                    return True
        return False
