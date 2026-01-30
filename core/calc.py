from __future__ import annotations

from decimal import Decimal


def calc_mid_spread(
    bids: list[tuple[Decimal, Decimal]],
    asks: list[tuple[Decimal, Decimal]],
) -> tuple[Decimal | None, Decimal | None]:
    if not bids or not asks:
        return None, None
    best_bid = bids[0][0]
    best_ask = asks[0][0]
    if best_bid <= 0 or best_ask <= 0:
        return None, None
    mid = (best_bid + best_ask) / Decimal("2")
    if mid <= 0:
        return None, None
    spread_bps = (best_ask - best_bid) / mid * Decimal("10000")
    return mid, spread_bps


def coverage_pct(got: Decimal, target: Decimal) -> Decimal:
    if target <= 0:
        return Decimal("0")
    return (Decimal(got) / Decimal(target)) * Decimal("100")


def net_profit(stable_out: Decimal, notional: Decimal, required: Decimal) -> Decimal:
    return Decimal(stable_out) - Decimal(notional) - Decimal(required)


def price_ratio_ok(implied: Decimal, mid: Decimal, max_ratio: Decimal) -> bool:
    if implied <= 0 or mid <= 0:
        return False
    ratio = max(Decimal(implied), Decimal(mid)) / min(Decimal(implied), Decimal(mid))
    return ratio <= Decimal(max_ratio)


def gross_cap_ok(stable_out: Decimal, notional: Decimal, max_gross_profit_pct: Decimal) -> bool:
    if notional <= 0:
        return False
    gross_pct = (Decimal(stable_out) - Decimal(notional)) / Decimal(notional) * Decimal("100")
    return gross_pct <= Decimal(max_gross_profit_pct)
