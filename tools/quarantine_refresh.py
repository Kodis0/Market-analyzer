from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import aiohttp
import yaml
from dotenv import load_dotenv

from core.quarantine import QuarantineEntry, load_quarantine, prune_expired, save_quarantine

JUP_QUOTE_URL = "https://quote-api.jup.ag/v6/quote"


@dataclass
class ProbeResult:
    ok: bool
    error_code: str = ""
    error_msg: str = ""


def _ts() -> int:
    return int(time.time())


def load_cfg(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


async def jup_quote(
    session: aiohttp.ClientSession,
    api_key: str,
    input_mint: str,
    output_mint: str,
    amount_raw: int,
    slippage_bps: int,
    restrict_intermediate: bool,
    max_accounts: int,
    timeout_sec: float,
) -> ProbeResult:
    headers = {}
    if api_key:
        headers["x-api-key"] = api_key

    params = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": str(int(amount_raw)),
        "swapMode": "ExactIn",
        "slippageBps": str(int(slippage_bps)),
        "restrictIntermediateTokens": "true" if restrict_intermediate else "false",
        "maxAccounts": str(int(max_accounts)),
    }

    try:
        async with session.get(
            JUP_QUOTE_URL, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout_sec)
        ) as r:
            if r.status == 200:
                return ProbeResult(ok=True)

            body = await r.text()
            code = ""
            msg = body
            try:
                j = json.loads(body)
                if isinstance(j, dict):
                    code = str(j.get("errorCode") or "")
                    msg = str(j.get("error") or body)
            except Exception:
                pass
            return ProbeResult(ok=False, error_code=code, error_msg=msg[:200])
    except Exception as e:
        # network errors: НЕ карантиним, просто "не смог проверить"
        return ProbeResult(ok=False, error_code="NETWORK", error_msg=str(e)[:200])


def ttl_for(code: str) -> int:
    """
    TTLы можно крутить как хочешь, но базово:
    - токен не трейдится: сутки
    - нет маршрута: 6 часов
    - объём слишком большой: 2 часа
    """
    if code == "TOKEN_NOT_TRADABLE":
        return 24 * 3600
    if code == "COULD_NOT_FIND_ANY_ROUTE":
        return 6 * 3600
    if code == "ROUTE_PLAN_DOES_NOT_CONSUME_ALL_THE_AMOUNT":
        return 2 * 3600
    # прочие 400: час
    if code and code != "NETWORK":
        return 3600
    return 0


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("-c", "--config", default="config.yaml")
    p.add_argument("--out", default="quarantine.yaml")
    p.add_argument("--concurrency", type=int, default=20)
    p.add_argument("--probe-usdc", type=float, default=10.0, help="маленький объём для проверки (USDC)")
    p.add_argument("--probe-notional", action="store_true", help="добавить проверку на боевом notional из конфига")
    args = p.parse_args()

    load_dotenv()
    api_key = os.environ.get("JUP_API_KEY", "")

    cfg = load_cfg(args.config)
    trading = cfg.get("trading") or {}
    stable = trading.get("stable") or {}
    tokens = trading.get("tokens") or {}

    stable_mint = str(stable.get("mint") or "").strip()
    stable_decimals = int(stable.get("decimals") or 6)

    jup_cfg = cfg.get("jupiter") or {}
    slippage_bps = int(jup_cfg.get("slippage_bps") or 50)
    restrict_intermediate = bool(jup_cfg.get("restrict_intermediate_tokens", True))
    max_accounts = int(jup_cfg.get("max_accounts") or 64)
    timeout_sec = float(jup_cfg.get("timeout_sec") or 2.0)

    notional_usd = float(trading.get("notional_usd") or 1000.0)

    if not stable_mint:
        raise RuntimeError("config.yaml: trading.stable.mint is required")

    # existing quarantine
    q = prune_expired(load_quarantine(args.out))

    sem = asyncio.Semaphore(max(1, int(args.concurrency)))

    async with aiohttp.ClientSession() as session:

        async def probe_one(token_key: str, t: dict) -> Tuple[str, Optional[QuarantineEntry], str]:
            bybit_symbol = str(t.get("bybit_symbol") or "").strip()
            mint = str(t.get("mint") or "").strip()
            dec = int(t.get("decimals") or 0)

            if not bybit_symbol or not mint or dec <= 0:
                return bybit_symbol or token_key, QuarantineEntry("BAD_TOKEN_CFG", _ts() + 24 * 3600), "bad cfg"

            async with sem:
                # small probe
                amt_small = int(round(float(args.probe_usdc) * (10**stable_decimals)))
                r1 = await jup_quote(
                    session, api_key, stable_mint, mint, amt_small,
                    slippage_bps, restrict_intermediate, max_accounts, timeout_sec
                )
                r2 = await jup_quote(
                    session, api_key, mint, stable_mint, int(round(0.1 * (10**dec))),  # tiny token->stable
                    slippage_bps, restrict_intermediate, max_accounts, timeout_sec
                )

                worst = None
                # If either direction fails with "real" code => quarantine
                for r in (r1, r2):
                    if not r.ok and r.error_code and r.error_code != "NETWORK":
                        worst = r
                        break

                # optionally probe on real notional (stable -> token)
                if args.probe_notional and (worst is None):
                    amt_big = int(round(notional_usd * (10**stable_decimals)))
                    r3 = await jup_quote(
                        session, api_key, stable_mint, mint, amt_big,
                        slippage_bps, restrict_intermediate, max_accounts, timeout_sec
                    )
                    if not r3.ok and r3.error_code and r3.error_code != "NETWORK":
                        worst = r3

                if worst is None:
                    return bybit_symbol, None, "ok"

                ttl = ttl_for(worst.error_code)
                if ttl <= 0:
                    return bybit_symbol, None, "skip"

                ent = QuarantineEntry(reason=worst.error_code, until_ts=_ts() + ttl)
                return bybit_symbol, ent, worst.error_msg

        tasks = []
        for token_key, t in tokens.items():
            if not isinstance(t, dict):
                continue
            tasks.append(probe_one(token_key, t))

        results = await asyncio.gather(*tasks)

    added = 0
    for sym, ent, msg in results:
        if not sym:
            continue
        if ent is None:
            # if previously quarantined but now OK -> let it expire naturally / or delete now
            continue
        prev = q.get(sym)
        if (prev is None) or (ent.until_ts > prev.until_ts):
            q[sym] = ent
            added += 1

    q = prune_expired(q)
    save_quarantine(args.out, q)

    total = len(tokens)
    quarantined = len(q)
    print(f"checked={total} quarantined={quarantined} updated={added} file={args.out}")


if __name__ == "__main__":
    asyncio.run(main())
