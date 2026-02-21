from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass
class SimResult:
    base_out: Decimal
    quote_out: Decimal
    avg_price: Decimal
    slippage_bps: Decimal


def simulate_buy_with_notional(asks: list[tuple[Decimal, Decimal]], notional: Decimal) -> SimResult | None:
    if not asks or notional <= 0:
        return None

    best_ask = asks[0][0]
    remaining_quote = notional
    base_got = Decimal("0")
    quote_spent = Decimal("0")

    for price, qty in asks:
        if remaining_quote <= 0:
            break
        if price <= 0 or qty <= 0:
            continue

        max_base_at_level = remaining_quote / price
        take_base = min(qty, max_base_at_level)

        spent = take_base * price
        base_got += take_base
        quote_spent += spent
        remaining_quote -= spent

    if base_got == 0:
        return None

    avg_price = quote_spent / base_got
    slippage_bps = (avg_price / best_ask - 1) * Decimal("10000")

    return SimResult(base_out=base_got, quote_out=quote_spent, avg_price=avg_price, slippage_bps=slippage_bps)


def simulate_sell_base(bids: list[tuple[Decimal, Decimal]], base_amount: Decimal) -> SimResult | None:
    if not bids or base_amount <= 0:
        return None

    best_bid = bids[0][0]
    remaining_base = base_amount
    quote_got = Decimal("0")
    base_sold = Decimal("0")

    for price, qty in bids:
        if remaining_base <= 0:
            break
        if price <= 0 or qty <= 0:
            continue

        take_base = min(qty, remaining_base)
        got = take_base * price

        base_sold += take_base
        quote_got += got
        remaining_base -= take_base

    if base_sold == 0:
        return None

    avg_price = quote_got / base_sold
    slippage_bps = (1 - avg_price / best_bid) * Decimal("10000")

    return SimResult(base_out=base_sold, quote_out=quote_got, avg_price=avg_price, slippage_bps=slippage_bps)
