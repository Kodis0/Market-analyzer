"""
Periodic verification of quarantined symbols: remove those that can trade again.
- JUP_TOKEN_NOT_TRADABLE / JUP_NO_ROUTE: remove if Jupiter returns a valid quote
- WS_STALE: not verified (we unsubscribed from OB; rely on TTL expiry)
"""

from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from core.quarantine import load_quarantine, prune_expired

from core.arb.utils import to_raw

log = logging.getLogger("app")

if TYPE_CHECKING:
    from connectors.jupiter import JupiterClient


VERIFY_INTERVAL_SEC = 30 * 60  # 30 min
MAX_JUP_CHECKS_PER_RUN = 15  # limit Jupiter API calls per run
JUP_CHECK_DELAY_SEC = 2.0  # delay between Jupiter checks


async def verify_and_recover(
    q_manager: Any,
    jup: JupiterClient,
    full_tokens: dict[str, Any],
    stable_mint: str,
    stable_decimals: int,
    notional_usd: Decimal,
    on_symbols_changed: Any = None,
) -> int:
    """
    Check quarantined symbols; remove those that can trade again.
    Returns count of recovered symbols.
    """
    async with q_manager._file_lock:
        q = load_quarantine(str(q_manager.quarantine_path))
        q = prune_expired(q)

    if not q:
        return 0

    symbol_to_info: dict[str, tuple[str, int]] = {}
    for t in full_tokens.values():
        sym = getattr(t, "bybit_symbol", None)
        mint = getattr(t, "mint", None)
        decimals = getattr(t, "decimals", None)
        if sym and mint and decimals is not None:
            symbol_to_info[sym] = (str(mint), int(decimals))

    recovered: list[str] = []
    jup_checks = 0

    for symbol, entry in list(q.items()):
        reason = entry.reason or ""

        if reason in ("JUP_TOKEN_NOT_TRADABLE", "JUP_NO_ROUTE") and jup_checks < MAX_JUP_CHECKS_PER_RUN:
            info = symbol_to_info.get(symbol)
            if not info:
                continue
            mint, decimals = info
            if decimals <= 0 or decimals > 18:
                continue

            stable_raw = to_raw(notional_usd, stable_decimals)
            quote = await jup.quote_exact_in(stable_mint, mint, stable_raw)
            jup_checks += 1
            if jup_checks < MAX_JUP_CHECKS_PER_RUN:
                await asyncio.sleep(JUP_CHECK_DELAY_SEC)

            if quote is not None and quote.out_amount_raw > 0:
                recovered.append(symbol)
                log.info("Quarantine verify: %s recovered (%s, Jupiter OK)", symbol, reason)

        elif reason == "BAD_TOKEN_CFG":
            pass  # never auto-recover

    if not recovered:
        return 0

    await q_manager.remove_recovered(recovered, on_symbols_changed)
    log.warning(
        "Quarantine verify: recovered %d symbols: %s",
        len(recovered),
        ", ".join(sorted(recovered)[:10]) + ("..." if len(recovered) > 10 else ""),
    )
    return len(recovered)


async def verify_loop(
    q_manager: Any,
    jup: JupiterClient,
    full_tokens: dict[str, Any],
    stable_mint: str,
    stable_decimals: int,
    notional_usd: Decimal,
    on_symbols_changed: Any = None,
    interval_sec: float = VERIFY_INTERVAL_SEC,
    exchange_enabled_getter: Any = None,
) -> None:
    """Run verification periodically."""
    while True:
        await asyncio.sleep(interval_sec)
        if exchange_enabled_getter is not None and not exchange_enabled_getter():
            continue
        try:
            await verify_and_recover(
                q_manager=q_manager,
                jup=jup,
                full_tokens=full_tokens,
                stable_mint=stable_mint,
                stable_decimals=stable_decimals,
                notional_usd=notional_usd,
                on_symbols_changed=on_symbols_changed,
            )
        except Exception:
            log.exception("Quarantine verify failed")
