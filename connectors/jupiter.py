from __future__ import annotations

import asyncio
import random
import time
import aiohttp
from dataclasses import dataclass
from typing import Optional
from decimal import Decimal
import logging

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
    ) -> None:
        self._s = session
        self._base = base_url.rstrip("/")
        self._api_key = api_key
        self._timeout = aiohttp.ClientTimeout(total=timeout_sec)
        self._slippage_bps = slippage_bps
        self._restrict = restrict_intermediate_tokens
        self._max_accounts = max_accounts
        self._rate_limiter = RateLimiter(rps, concurrency)
        self._max_retries = max(1, int(max_retries))

    def _retry_delay(self, attempt: int, retry_after: Optional[str] = None) -> float:
        if retry_after:
            try:
                return max(0.0, float(retry_after))
            except Exception:
                pass
        base = 0.25 * (2 ** attempt)
        jitter = random.uniform(0.0, 0.15)
        return min(5.0, base + jitter)

    async def quote_exact_in(self, input_mint: str, output_mint: str, amount_raw: int) -> Optional[JupQuote]:
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
                        status = r.status
                        retry_after = r.headers.get("Retry-After")
                        if status == 200:
                            j = await r.json()
                            return JupQuote(
                                input_mint=j["inputMint"],
                                output_mint=j["outputMint"],
                                in_amount_raw=int(j["inAmount"]),
                                out_amount_raw=int(j["outAmount"]),
                                price_impact_pct=Decimal(str(j.get("priceImpactPct", "0"))),
                                context_slot=int(j.get("contextSlot", 0)),
                                time_taken_ms=int(j.get("timeTaken", 0)),
                            )
                        body = await r.text()

                if status == 429 or status >= 500:
                    wait_s = self._retry_delay(attempt, retry_after)
                    log.warning("quote retry status=%s attempt=%s wait=%.2fs body=%s", status, attempt + 1, wait_s, body[:200])
                    await asyncio.sleep(wait_s)
                    continue

                log.warning("quote failed status=%s body=%s", status, body[:300])
                return None

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt < self._max_retries - 1:
                    wait_s = self._retry_delay(attempt)
                    log.warning("quote error: %s; retry in %.2fs", e, wait_s)
                    await asyncio.sleep(wait_s)
                    continue
                log.exception("quote error: %s", e)
                return None
            except Exception as e:
                log.exception("quote error: %s", e)
                return None

        return None
