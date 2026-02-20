
from __future__ import annotations

from decimal import Decimal
from typing import List, Tuple

BYBIT_UI_BASE = "https://www.bybit.com/en/trade/spot"  # /BASE/QUOTE
JUP_UI_BASE = "https://jup.ag/swap"


def _base_from_bybit_symbol(bybit_symbol: str) -> str:
    """Extract base coin from Bybit symbol (e.g. BTCUSDT -> BTC)."""
    s = (bybit_symbol or "").upper().strip()
    for q in ("USDT", "USDC", "USD"):
        if s.endswith(q):
            return s[: -len(q)]
    return s


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
    """Legacy: query params. Jupiter UI may ignore these."""
    return f"{JUP_UI_BASE}?inputMint={input_mint}&outputMint={output_mint}"


def jup_swap_url_by_symbol(bybit_symbol: str, buy: bool) -> str:
    """
    Jupiter's current UI uses path format: /swap/FROM-TO (e.g. /swap/BTC-USDC).
    buy=True:  USDC -> token (Купить на Jupiter)
    buy=False: token -> USDC (Продать на Jupiter)
    """
    base = _base_from_bybit_symbol(bybit_symbol)
    if buy:
        return f"{JUP_UI_BASE}/USDC-{base}"
    return f"{JUP_UI_BASE}/{base}-USDC"


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
