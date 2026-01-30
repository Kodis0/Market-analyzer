
from __future__ import annotations

from decimal import Decimal
from typing import List, Tuple

BYBIT_UI_BASE = "https://www.bybit.com/en/trade/spot"  # /BASE/QUOTE
JUP_UI_BASE = "https://jup.ag/swap"  # ?inputMint=...&outputMint=...


def to_raw(amount: Decimal, decimals: int) -> int:
    scale = Decimal(10) ** Decimal(decimals)
    return int((Decimal(amount) * scale).to_integral_value(rounding="ROUND_DOWN"))


def from_raw(raw: int, decimals: int) -> Decimal:
    scale = Decimal(10) ** Decimal(decimals)
    return Decimal(raw) / scale


def bybit_spot_url(bybit_symbol: str) -> str:
    s = (bybit_symbol or "").upper().strip()
    quote = None
    base = None
    for q in ("USDT", "USDC", "USD"):
        if s.endswith(q):
            quote = q
            base = s[: -len(q)]
            break
    if not quote or not base:
        return f"{BYBIT_UI_BASE}/{s}"
    return f"{BYBIT_UI_BASE}/{base}/{quote}"


def jup_swap_url(input_mint: str, output_mint: str) -> str:
    return f"{JUP_UI_BASE}?inputMint={input_mint}&outputMint={output_mint}"


def snapshot_book(ob) -> tuple[list[tuple[Decimal, Decimal]], list[tuple[Decimal, Decimal]]]:
    """
    Atomic-ish snapshot: copy dicts first, then sort locally.
    """
    try:
        bids_map = ob.bids.copy()
        asks_map = ob.asks.copy()
    except Exception:
        return [], []
    bids = sorted(bids_map.items(), key=lambda x: x[0], reverse=True)
    asks = sorted(asks_map.items(), key=lambda x: x[0])
    return bids, asks
