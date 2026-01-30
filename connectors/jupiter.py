from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

import aiohttp

log = logging.getLogger("jupiter")


@dataclass
class JupQuote:
    input_mint: str
    output_mint: str
    in_amount_raw: int
    out_amount_raw: int
    price_impact_pct: Decimal
    context_slot: int
    time_taken_ms: int


class RateLimiter:
    def __init__(self, rps: float, concurrency: int) -> None:
        self._rps = max(0.0, float(rps))
        self._sem = asyncio.Semaphore(max(1, int(concurrency)))
        self._lock = asyncio.Lock()
        self._tokens = self._rps
        self._last = time.monotonic()

    async def __aenter__(self) -> "RateLimiter":
        await self._sem.acquire()
        if self._rps > 0:
            await self._wait_token()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self._sem.release()

    async def _wait_token(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self._last
                self._last = now
                self._tokens = min(self._rps, self._tokens + elapsed * self._rps)
                if self._tokens >= 1:
                    self._tokens -= 1
                    return
            await asyncio.sleep(0.01)


class JupiterClient:
    _RE_NOT_TRADABLE_MINT = re.compile(r"token\s+([A-Za-z0-9]{32,})\s+is not tradable", re.IGNORECASE)

    def __init__(
        self,
        session: aiohttp.ClientSession,
        base_url: str,
        api_key: str,
        timeout_sec: float,
        slippage_bps: int,
        restrict_intermediate_tokens: bool,
        max_accounts: int,
        rps: float = 5.0,
        concurrency: int = 4,
        max_retries: int = 4,
        # negative-cache ttl seconds
        ttl_no_route_sec: float = 300.0,          # 5 min
        ttl_not_tradable_sec: float = 6 * 3600.0, # 6h
        ttl_amount_too_big_sec: float = 90.0,     # 90 sec
        log_throttle_sec: float = 30.0,           # same message not more often than once per 30s
    ) -> None:
        self._s = session
        self._base = base_url.rstrip("/")
        self._api_key = api_key
        self._timeout = aiohttp.ClientTimeout(total=float(timeout_sec))
        self._slippage_bps = int(slippage_bps)
        self._restrict = bool(restrict_intermediate_tokens)
        self._max_accounts = int(max_accounts)

        self._rate_limiter = RateLimiter(rps, concurrency)
        self._max_retries = max(1, int(max_retries))

        self._ttl_no_route = float(ttl_no_route_sec)
        self._ttl_not_tradable = float(ttl_not_tradable_sec)
        self._ttl_amount_too_big = float(ttl_amount_too_big_sec)
        self._log_throttle = float(log_throttle_sec)

        # negative cache:
        # - token level (mint -> until_ts)
        self._blocked_token_until: dict[str, float] = {}
        # - pair level ((in, out) -> until_ts)
        self._blocked_pair_until: dict[tuple[str, str], float] = {}
        # log throttle cache (key -> last_ts)
        self._last_log_ts: dict[str, float] = {}

    def _retry_delay(self, attempt: int, retry_after: Optional[str] = None) -> float:
        if retry_after:
            try:
                return max(0.0, float(retry_after))
            except Exception:
                pass
        base = 0.25 * (2**attempt)
        jitter = random.uniform(0.0, 0.15)
        return min(5.0, base + jitter)

    def _now(self) -> float:
        return time.monotonic()

    def _is_blocked(self, input_mint: str, output_mint: str) -> bool:
        now = self._now()
        tu_in = self._blocked_token_until.get(input_mint, 0.0)
        tu_out = self._blocked_token_until.get(output_mint, 0.0)
        pu = self._blocked_pair_until.get((input_mint, output_mint), 0.0)
        return (tu_in > now) or (tu_out > now) or (pu > now)

    def _block_token(self, mint: str, ttl_sec: float) -> None:
        if not mint:
            return
        until = self._now() + max(1.0, float(ttl_sec))
        prev = self._blocked_token_until.get(mint, 0.0)
        if until > prev:
            self._blocked_token_until[mint] = until

    def _block_pair(self, input_mint: str, output_mint: str, ttl_sec: float) -> None:
        until = self._now() + max(1.0, float(ttl_sec))
        key = (input_mint, output_mint)
        prev = self._blocked_pair_until.get(key, 0.0)
        if until > prev:
            self._blocked_pair_until[key] = until

    def _should_log(self, key: str) -> bool:
        now = self._now()
        last = self._last_log_ts.get(key, 0.0)
        if (now - last) >= self._log_throttle:
            self._last_log_ts[key] = now
            return True
        return False

    async def _read_json_or_text(self, r: aiohttp.ClientResponse) -> tuple[Optional[dict], str]:
        try:
            raw = await r.text()
        except Exception:
            return None, ""
        try:
            j = json.loads(raw) if raw else None
            return (j if isinstance(j, dict) else None), raw
        except Exception:
            return None, raw

    async def quote_exact_in(self, input_mint: str, output_mint: str, amount_raw: int) -> Optional[JupQuote]:
        if not input_mint or not output_mint:
            return None
        if int(amount_raw) <= 0:
            return None

        # negative cache fast-path
        if self._is_blocked(input_mint, output_mint):
            return None

        url = f"{self._base}/quote"
        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(int(amount_raw)),
            "swapMode": "ExactIn",
            "slippageBps": str(int(self._slippage_bps)),
            "restrictIntermediateTokens": "true" if self._restrict else "false",
            "maxAccounts": str(int(self._max_accounts)),
            "instructionVersion": "V1",
        }
        headers = {"x-api-key": self._api_key} if self._api_key else {}

        for attempt in range(self._max_retries):
            try:
                async with self._rate_limiter:
                    async with self._s.get(url, params=params, headers=headers, timeout=self._timeout) as r:
                        status = int(r.status)
                        retry_after = r.headers.get("Retry-After")

                        if status == 200:
                            j = await r.json(content_type=None)
                            return JupQuote(
                                input_mint=j["inputMint"],
                                output_mint=j["outputMint"],
                                in_amount_raw=int(j["inAmount"]),
                                out_amount_raw=int(j["outAmount"]),
                                price_impact_pct=Decimal(str(j.get("priceImpactPct", "0"))),
                                context_slot=int(j.get("contextSlot", 0)),
                                time_taken_ms=int(j.get("timeTaken", 0)),
                            )

                        body_json, body_text = await self._read_json_or_text(r)

                # Retry only on 429 / 5xx
                if status == 429 or status >= 500:
                    wait_s = self._retry_delay(attempt, retry_after)
                    if self._should_log(f"retry:{status}"):
                        log.warning(
                            "quote retry status=%s attempt=%s wait=%.2fs body=%s",
                            status,
                            attempt + 1,
                            wait_s,
                            (body_text or "")[:200],
                        )
                    await asyncio.sleep(wait_s)
                    continue

                # Handle "expected" 400s without spamming warnings
                if status == 400 and body_json:
                    code = str(body_json.get("errorCode") or "")
                    msg = str(body_json.get("error") or body_text or "")

                    # TOKEN_NOT_TRADABLE: block token for long time
                    if code == "TOKEN_NOT_TRADABLE":
                        mint = ""
                        m = self._RE_NOT_TRADABLE_MINT.search(msg)
                        if m:
                            mint = m.group(1)
                        # fallback: block output mint, that's usually the offending one
                        self._block_token(mint or output_mint, self._ttl_not_tradable)
                        # also block pair a bit to reduce immediate re-tries
                        self._block_pair(input_mint, output_mint, 60.0)

                        k = f"400:{code}:{mint or output_mint}"
                        if self._should_log(k):
                            log.info("jup skip (%s): %s", code, (msg or "")[:160])
                        return None

                    # COULD_NOT_FIND_ANY_ROUTE: block this direction for a while
                    if code == "COULD_NOT_FIND_ANY_ROUTE":
                        self._block_pair(input_mint, output_mint, self._ttl_no_route)
                        k = f"400:{code}:{input_mint}->{output_mint}"
                        if self._should_log(k):
                            log.debug("jup skip (%s): %s", code, (msg or "")[:160])
                        return None

                    # ROUTE_PLAN_DOES_NOT_CONSUME_ALL_THE_AMOUNT: amount too big / not enough liquidity
                    if code == "ROUTE_PLAN_DOES_NOT_CONSUME_ALL_THE_AMOUNT":
                        self._block_pair(input_mint, output_mint, self._ttl_amount_too_big)
                        k = f"400:{code}:{input_mint}->{output_mint}"
                        if self._should_log(k):
                            log.debug("jup skip (%s): %s", code, (msg or "")[:160])
                        return None

                    # other 400s: log once per throttle window
                    k = f"400:other:{code}"
                    if self._should_log(k):
                        log.warning("quote failed status=400 code=%s body=%s", code, (body_text or "")[:300])
                    return None

                # other non-200 non-retry statuses
                k = f"fail:{status}"
                if self._should_log(k):
                    log.warning("quote failed status=%s body=%s", status, (body_text or "")[:300])
                return None

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt < self._max_retries - 1:
                    wait_s = self._retry_delay(attempt)
                    if self._should_log(f"net:{type(e).__name__}"):
                        log.warning("quote error: %s; retry in %.2fs", e, wait_s)
                    await asyncio.sleep(wait_s)
                    continue
                log.exception("quote error: %s", e)
                return None
            except Exception as e:
                log.exception("quote error: %s", e)
                return None

        return None
