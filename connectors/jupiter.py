from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Optional, Tuple, Callable, Awaitable

import aiohttp

log = logging.getLogger("jupiter")


# -----------------------------
# Models
# -----------------------------

@dataclass(frozen=True)
class JupQuote:
    input_mint: str
    output_mint: str
    in_amount_raw: int
    out_amount_raw: int
    price_impact_pct: Decimal
    context_slot: int
    time_taken_ms: int


# -----------------------------
# Helpers
# -----------------------------

class RateLimiter:
    """
    Simple combined limiter:
      - concurrency via semaphore
      - RPS via token bucket
    """

    def __init__(self, rps: float, concurrency: int) -> None:
        self._rps = max(0.0, float(rps))
        self._sem = asyncio.Semaphore(max(1, int(concurrency)))
        self._lock = asyncio.Lock()

        # token bucket
        self._tokens = self._rps if self._rps > 0 else 0.0
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
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
            await asyncio.sleep(0.01)


class LogThrottle:
    """Throttle repeated logs by key."""

    def __init__(self) -> None:
        self._last: Dict[str, float] = {}

    def allow(self, key: str, interval_sec: float) -> bool:
        now = time.monotonic()
        last = self._last.get(key, 0.0)
        if (now - last) >= interval_sec:
            self._last[key] = now
            return True
        return False

    def cleanup(self, max_items: int = 5000) -> None:
        """Best-effort bound memory."""
        if len(self._last) <= max_items:
            return
        # drop oldest ~20%
        items = sorted(self._last.items(), key=lambda kv: kv[1])
        drop = max(1, int(len(items) * 0.2))
        for k, _ in items[:drop]:
            self._last.pop(k, None)


class NegativeCache:
    """
    Negative cache to avoid hammering Jupiter for known-bad tokens/pairs.
    """

    def __init__(self) -> None:
        self._token_until: Dict[str, float] = {}
        self._pair_until: Dict[Tuple[str, str], float] = {}

    def _now(self) -> float:
        return time.monotonic()

    def is_blocked(self, input_mint: str, output_mint: str) -> bool:
        now = self._now()
        if self._token_until.get(input_mint, 0.0) > now:
            return True
        if self._token_until.get(output_mint, 0.0) > now:
            return True
        if self._pair_until.get((input_mint, output_mint), 0.0) > now:
            return True
        return False

    def block_token(self, mint: str, ttl_sec: float) -> None:
        if not mint:
            return
        until = self._now() + max(1.0, float(ttl_sec))
        prev = self._token_until.get(mint, 0.0)
        if until > prev:
            self._token_until[mint] = until

    def block_pair(self, input_mint: str, output_mint: str, ttl_sec: float) -> None:
        until = self._now() + max(1.0, float(ttl_sec))
        key = (input_mint, output_mint)
        prev = self._pair_until.get(key, 0.0)
        if until > prev:
            self._pair_until[key] = until

    def cleanup(self, max_token_items: int = 50000, max_pair_items: int = 200000) -> None:
        now = self._now()

        # remove expired
        if self._token_until:
            expired = [k for k, v in self._token_until.items() if v <= now]
            for k in expired:
                self._token_until.pop(k, None)

        if self._pair_until:
            expired = [k for k, v in self._pair_until.items() if v <= now]
            for k in expired:
                self._pair_until.pop(k, None)

        # bound sizes (best-effort)
        if len(self._token_until) > max_token_items:
            items = sorted(self._token_until.items(), key=lambda kv: kv[1])
            drop = max(1, int(len(items) * 0.2))
            for k, _ in items[:drop]:
                self._token_until.pop(k, None)

        if len(self._pair_until) > max_pair_items:
            items = sorted(self._pair_until.items(), key=lambda kv: kv[1])
            drop = max(1, int(len(items) * 0.2))
            for k, _ in items[:drop]:
                self._pair_until.pop(k, None)


# -----------------------------
# Jupiter Client
# -----------------------------

class JupiterClient:
    """
    Quote client for Jupiter quote API.

    Key goals:
      - don't spam logs for expected 400 errors (no route / not tradable / amount too big)
      - backoff & retry on 429/5xx and network issues
      - negative cache for bad tokens/pairs
      - (NEW) optional on_skip callback for autosanitization in app
    """

    _RE_NOT_TRADABLE_MINT = re.compile(
        r"token\s+([A-Za-z0-9]{32,})\s+is not tradable",
        re.IGNORECASE,
    )

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
        # negative-cache TTLs
        ttl_no_route_sec: float = 300.0,           # 5 min
        ttl_not_tradable_sec: float = 6 * 3600.0,  # 6h
        ttl_amount_too_big_sec: float = 90.0,      # 90 sec
        # logging
        expected_400_log_interval_sec: float = 60.0,
        retry_log_interval_sec: float = 10.0,
        # autosanitization hook
        on_skip: Optional[Callable[[str, str, str, str, str], Awaitable[None]]] = None,
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

        self._expected_400_log_interval = float(expected_400_log_interval_sec)
        self._retry_log_interval = float(retry_log_interval_sec)

        self._neg = NegativeCache()
        self._throttle = LogThrottle()

        self._on_skip = on_skip

    # -------- internal utils --------

    def _retry_delay(self, attempt: int, retry_after: Optional[str] = None) -> float:
        if retry_after:
            try:
                return max(0.0, float(retry_after))
            except Exception:
                pass
        # exp backoff + jitter
        base = 0.25 * (2 ** attempt)
        jitter = random.uniform(0.0, 0.20)
        return min(6.0, base + jitter)

    async def _read_text_and_json(self, r: aiohttp.ClientResponse) -> Tuple[str, Optional[Dict[str, Any]]]:
        try:
            text = await r.text()
        except Exception:
            return "", None

        j: Optional[Dict[str, Any]] = None
        if text:
            try:
                parsed = json.loads(text)
                if isinstance(parsed, dict):
                    j = parsed
            except Exception:
                j = None
        return text, j

    def _maybe_cleanup(self) -> None:
        # cheap probabilistic cleanup to avoid growth on long runs
        if random.random() < 0.02:  # ~2% calls
            self._neg.cleanup()
            self._throttle.cleanup()

    def _emit_skip(self, code: str, input_mint: str, output_mint: str, bad_mint: str, msg: str) -> None:
        cb = self._on_skip
        if not cb:
            return
        try:
            asyncio.create_task(cb(code, input_mint, output_mint, bad_mint, msg))
        except Exception:
            # never fail quotes because of observer
            pass

    # -------- public API --------

    async def quote_exact_in(self, input_mint: str, output_mint: str, amount_raw: int) -> Optional[JupQuote]:
        if not input_mint or not output_mint:
            return None
        if int(amount_raw) <= 0:
            return None

        self._maybe_cleanup()

        # negative-cache fast path
        if self._neg.is_blocked(input_mint, output_mint):
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
                                input_mint=str(j.get("inputMint", input_mint)),
                                output_mint=str(j.get("outputMint", output_mint)),
                                in_amount_raw=int(j.get("inAmount", amount_raw)),
                                out_amount_raw=int(j.get("outAmount", 0)),
                                price_impact_pct=Decimal(str(j.get("priceImpactPct", "0"))),
                                context_slot=int(j.get("contextSlot", 0)),
                                time_taken_ms=int(j.get("timeTaken", 0)),
                            )

                        body_text, body_json = await self._read_text_and_json(r)

                # Retryable: 429 / 5xx
                if status == 429 or status >= 500:
                    wait_s = self._retry_delay(attempt, retry_after)
                    if self._throttle.allow(f"retry:{status}", self._retry_log_interval):
                        log.warning(
                            "quote retry status=%s attempt=%d/%d wait=%.2fs body=%s",
                            status, attempt + 1, self._max_retries, wait_s, (body_text or "")[:200]
                        )
                    await asyncio.sleep(wait_s)
                    continue

                # Expected 400s: do not warn-spam
                if status == 400 and body_json:
                    code = str(body_json.get("errorCode") or "")
                    msg = str(body_json.get("error") or body_text or "")

                    if code == "TOKEN_NOT_TRADABLE":
                        mint = ""
                        m = self._RE_NOT_TRADABLE_MINT.search(msg)
                        if m:
                            mint = m.group(1)

                        bad = mint or output_mint

                        self._neg.block_token(bad, self._ttl_not_tradable)
                        self._neg.block_pair(input_mint, output_mint, 60.0)

                        if self._throttle.allow(f"400:{code}:{bad}", self._expected_400_log_interval):
                            log.info("jup skip %s out=%s msg=%s", code, bad, msg[:140])

                        self._emit_skip(code, input_mint, output_mint, bad, msg)
                        return None

                    if code == "COULD_NOT_FIND_ANY_ROUTE":
                        self._neg.block_pair(input_mint, output_mint, self._ttl_no_route)
                        if self._throttle.allow(f"400:{code}:{input_mint}->{output_mint}", self._expected_400_log_interval):
                            log.debug("jup skip %s %s->%s msg=%s", code, input_mint, output_mint, msg[:140])

                        self._emit_skip(code, input_mint, output_mint, "", msg)
                        return None

                    if code == "ROUTE_PLAN_DOES_NOT_CONSUME_ALL_THE_AMOUNT":
                        self._neg.block_pair(input_mint, output_mint, self._ttl_amount_too_big)
                        if self._throttle.allow(f"400:{code}:{input_mint}->{output_mint}", self._expected_400_log_interval):
                            log.debug("jup skip %s %s->%s msg=%s", code, input_mint, output_mint, msg[:140])
                        return None

                    # Other 400: warn but throttled
                    if self._throttle.allow(f"400:other:{code}", 30.0):
                        log.warning("quote failed status=400 code=%s body=%s", code, (body_text or "")[:300])
                    return None

                # Non-200, non-retry statuses
                if self._throttle.allow(f"fail:{status}", 30.0):
                    log.warning("quote failed status=%s body=%s", status, (body_text or "")[:300])
                return None

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt < self._max_retries - 1:
                    wait_s = self._retry_delay(attempt)
                    if self._throttle.allow(f"net:{type(e).__name__}", self._retry_log_interval):
                        log.warning("quote net error: %s; retry in %.2fs", e, wait_s)
                    await asyncio.sleep(wait_s)
                    continue
                log.exception("quote net error (final): %s", e)
                return None
            except Exception as e:
                log.exception("quote error: %s", e)
                return None

        return None
