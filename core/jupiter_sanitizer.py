"""
Jupiter auto-sanitization: quarantine tokens that Jupiter rejects.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable

JUP_BAD_WINDOW_SEC = 20 * 60
JUP_NOT_TRADABLE_TTL_SEC = 24 * 3600
JUP_NO_ROUTE_TTL_SEC = 2 * 3600
JUP_NOT_TRADABLE_HITS = 1
JUP_NO_ROUTE_HITS = 30
JUP_MAX_QUARANTINES_PER_MIN = 10


def make_on_jup_skip(
    stable_mint: str,
    mint_to_symbol: dict[str, str],
    quarantine_add: Callable[[str, str, int], Awaitable[None]],
) -> Callable[[str, str, str, str, str], Awaitable[None]]:
    """
    Build on_jup_skip callback for JupiterClient.
    """

    jup_bad_counts: dict[str, int] = {}
    jup_bad_last_ts: dict[str, float] = {}
    jup_qrate: dict = {"ts": 0.0, "cnt": 0}

    def allow_jup_quarantine() -> bool:
        now2 = time.time()
        if (now2 - jup_qrate["ts"]) > 60:
            jup_qrate["ts"] = now2
            jup_qrate["cnt"] = 0
        if jup_qrate["cnt"] >= JUP_MAX_QUARANTINES_PER_MIN:
            return False
        jup_qrate["cnt"] += 1
        return True

    async def on_jup_skip(code: str, input_mint: str, output_mint: str, bad_mint: str, msg: str) -> None:
        m = bad_mint or ""
        if not m:
            if output_mint and output_mint != stable_mint:
                m = output_mint
            elif input_mint and input_mint != stable_mint:
                m = input_mint

        if not m or m == stable_mint:
            return

        symbol = mint_to_symbol.get(m)
        if not symbol:
            return

        now = time.time()
        last = jup_bad_last_ts.get(m, 0.0)
        if (now - last) > JUP_BAD_WINDOW_SEC:
            jup_bad_counts[m] = 0
        jup_bad_last_ts[m] = now
        jup_bad_counts[m] = jup_bad_counts.get(m, 0) + 1

        if code == "TOKEN_NOT_TRADABLE" and jup_bad_counts[m] >= JUP_NOT_TRADABLE_HITS:
            if not allow_jup_quarantine():
                return
            await quarantine_add(symbol, "JUP_TOKEN_NOT_TRADABLE", JUP_NOT_TRADABLE_TTL_SEC)
            return

        if code == "COULD_NOT_FIND_ANY_ROUTE" and jup_bad_counts[m] >= JUP_NO_ROUTE_HITS:
            if not allow_jup_quarantine():
                return
            await quarantine_add(symbol, "JUP_NO_ROUTE", JUP_NO_ROUTE_TTL_SEC)
            return

    return on_jup_skip
