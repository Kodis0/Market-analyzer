from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import logging
import os
import re
import time
import urllib.parse
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import aiohttp
import yaml
from dotenv import load_dotenv

log = logging.getLogger("watchlist")

BYBIT_BASE = "https://api.bybit.com"
BYBIT_INSTRUMENTS = f"{BYBIT_BASE}/v5/market/instruments-info"
BYBIT_TICKERS = f"{BYBIT_BASE}/v5/market/tickers"
BYBIT_COIN_INFO = f"{BYBIT_BASE}/v5/asset/coin/query-info"

JUP_TOKENS_SEARCH = "https://api.jup.ag/tokens/v2/search"

SOL_MINT = "So11111111111111111111111111111111111111112"

# Для “без мусора” — дефолтно отсекаем явные проблемы.
DEFAULT_BLACKLIST_BASE = {"USDT", "USDC", "USD1", "USDE", "USDD", "PYUSD", "TUSD", "FDUSD"}
DEFAULT_DENYLIST_BASE = {
    "XAUT", "PAXG",
    "AAPLX", "GOOGLX", "TSLAX", "NVDAX", "CRCLX", "HOODX",
}
MULTIPLIER_RE = re.compile(r"^(1000|10000|100000)[A-Z0-9]+$")

# ----------------------------- helpers -----------------------------


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def dump_yaml(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False, allow_unicode=True)


def chunks(seq: Sequence[Any], n: int) -> Iterable[Sequence[Any]]:
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def resolve_config_path(p: str) -> Path:
    """
    Чтобы не ловить FileNotFoundError когда запускаешь из tools/.
    Пробуем:
      - как есть
      - ./config.yaml
      - ../config.yaml
      - ../../config.yaml
    """
    raw = Path(p)
    if raw.exists():
        return raw

    candidates = [
        Path.cwd() / p,
        Path.cwd() / "config.yaml",
        Path.cwd().parent / "config.yaml",
        Path.cwd().parent.parent / "config.yaml",
    ]
    for c in candidates:
        if c.exists():
            return c
    return raw  # вернём как есть — ниже будет понятная ошибка


def dec(x: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(x))
    except Exception:
        return Decimal(default)


def safe_mid(bid: Decimal, ask: Decimal) -> Optional[Decimal]:
    if bid <= 0 or ask <= 0:
        return None
    m = (bid + ask) / Decimal("2")
    return m if m > 0 else None


def spread_bps(bid: Decimal, ask: Decimal) -> Optional[Decimal]:
    m = safe_mid(bid, ask)
    if not m:
        return None
    return (ask - bid) / m * Decimal("10000")


def best_level_usd(price: Decimal, size_base: Decimal) -> Decimal:
    if price <= 0 or size_base <= 0:
        return Decimal("0")
    return price * size_base


# ----------------------------- Bybit auth -----------------------------


def _bybit_sign(param_str: str, ts_ms: str, api_key: str, api_secret: str, recv_window: str) -> str:
    payload = f"{ts_ms}{api_key}{recv_window}{param_str}"
    return hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()


async def bybit_private_get(
    session: aiohttp.ClientSession,
    url: str,
    params: dict,
    api_key: str,
    api_secret: str,
    recv_window: int = 5000,
    max_retries: int = 10,
) -> dict:
    """
    Приватный GET Bybit v5 с обработкой rate limit 10006.
    """
    recv_window_s = str(recv_window)

    def make_param_str(p: dict) -> str:
        if not p:
            return ""
        items = sorted((k, str(v)) for k, v in p.items())
        return urllib.parse.urlencode(items, safe=",")

    for attempt in range(max_retries):
        ts_ms = str(int(time.time() * 1000))
        param_str = make_param_str(params)
        sign = _bybit_sign(param_str, ts_ms, api_key, api_secret, recv_window_s)

        headers = {
            "X-BAPI-API-KEY": api_key,
            "X-BAPI-SIGN": sign,
            "X-BAPI-TIMESTAMP": ts_ms,
            "X-BAPI-RECV-WINDOW": recv_window_s,
        }

        try:
            async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=12)) as r:
                j = await r.json(content_type=None)

                if isinstance(j, dict) and j.get("retCode") == 0:
                    return j

                if isinstance(j, dict) and j.get("retCode") == 10006:
                    reset = r.headers.get("X-Bapi-Limit-Reset-Timestamp")
                    now_ms = int(time.time() * 1000)
                    if reset and str(reset).isdigit():
                        sleep_s = max(0.0, (int(reset) - now_ms) / 1000.0) + 0.05
                    else:
                        sleep_s = 0.4 + attempt * 0.25
                    await asyncio.sleep(min(5.0, sleep_s))
                    continue

                raise RuntimeError(f"Bybit private GET error: {j}")

        except aiohttp.ClientError as e:
            await asyncio.sleep(min(2.0, 0.2 + attempt * 0.2))
            if attempt == max_retries - 1:
                raise RuntimeError(f"Bybit private GET network error: {e}") from e

    raise RuntimeError("Bybit private GET: exceeded retries")


# ----------------------------- fetchers -----------------------------


async def fetch_bybit_spot_instruments(session: aiohttp.ClientSession) -> List[dict]:
    out: List[dict] = []
    cursor: Optional[str] = None

    while True:
        params = {"category": "spot", "limit": "1000"}
        if cursor:
            params["cursor"] = cursor

        async with session.get(BYBIT_INSTRUMENTS, params=params, timeout=aiohttp.ClientTimeout(total=12)) as r:
            j = await r.json(content_type=None)

        if j.get("retCode") != 0:
            raise RuntimeError(f"Bybit instruments-info error: {j}")

        result = j.get("result", {}) or {}
        out.extend(result.get("list", []) or [])

        cursor = result.get("nextPageCursor")
        if not cursor:
            break

    return out


@dataclass(frozen=True)
class Ticker:
    symbol: str
    turnover24h: Decimal
    volume24h: Decimal
    bid1: Decimal
    ask1: Decimal
    bid1_size: Decimal
    ask1_size: Decimal

    @property
    def mid(self) -> Optional[Decimal]:
        return safe_mid(self.bid1, self.ask1)

    @property
    def spread_bps(self) -> Optional[Decimal]:
        return spread_bps(self.bid1, self.ask1)

    @property
    def top_bid_usd(self) -> Decimal:
        return best_level_usd(self.bid1, self.bid1_size)

    @property
    def top_ask_usd(self) -> Decimal:
        return best_level_usd(self.ask1, self.ask1_size)

    @property
    def top_min_usd(self) -> Decimal:
        return min(self.top_bid_usd, self.top_ask_usd)


async def fetch_bybit_spot_tickers(session: aiohttp.ClientSession) -> Dict[str, Ticker]:
    async with session.get(BYBIT_TICKERS, params={"category": "spot"}, timeout=aiohttp.ClientTimeout(total=12)) as r:
        j = await r.json(content_type=None)

    if j.get("retCode") != 0:
        raise RuntimeError(f"Bybit tickers error: {j}")

    items = (j.get("result", {}) or {}).get("list", []) or []
    out: Dict[str, Ticker] = {}

    for it in items:
        sym = it.get("symbol")
        if not sym:
            continue

        t = Ticker(
            symbol=str(sym),
            turnover24h=dec(it.get("turnover24h")),
            volume24h=dec(it.get("volume24h")),
            bid1=dec(it.get("bid1Price")),
            ask1=dec(it.get("ask1Price")),
            bid1_size=dec(it.get("bid1Size")),
            ask1_size=dec(it.get("ask1Size")),
        )
        out[t.symbol] = t

    return out


async def fetch_bybit_coininfo_sol_mints(
    session: aiohttp.ClientSession, api_key: str, api_secret: str
) -> Dict[str, str]:
    """
    coin -> solana mint (contractAddress на chainType Solana)
    """
    j = await bybit_private_get(session, BYBIT_COIN_INFO, {}, api_key, api_secret)
    rows = (j.get("result", {}) or {}).get("rows", []) or []

    coin_to_mint: Dict[str, str] = {}
    for row in rows:
        coin = (row.get("coin") or "").strip()
        if not coin:
            continue

        chains = row.get("chains") or []
        mint = None
        for ch in chains:
            chain_type = (ch.get("chainType") or "").lower()
            chain = (ch.get("chain") or "").lower()
            addr = (ch.get("contractAddress") or "").strip()
            if not addr:
                continue
            if "solana" in chain_type or chain in {"sol", "solana", "spl"}:
                mint = addr
                break

        if mint:
            coin_to_mint[coin] = mint

    coin_to_mint.setdefault("SOL", SOL_MINT)
    return coin_to_mint

async def fetch_jup_tokens_by_symbol(
    session: aiohttp.ClientSession,
    jup_api_key: str,
    symbol: str,
) -> List[dict]:
    headers = {"x-api-key": jup_api_key}
    params = {"query": symbol}
    async with session.get(
        JUP_TOKENS_SEARCH, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=12)
    ) as r:
        if r.status != 200:
            return []
        data = await r.json(content_type=None)
    if not isinstance(data, list):
        return []
    return [t for t in data if isinstance(t, dict)]

def pick_best_jup_token_for_symbol(base: str, items: List[dict]) -> Optional[dict]:
    base_u = base.upper()

    def score(t: dict) -> Tuple[int, int, int]:
        # больше = лучше
        sym = str(t.get("symbol") or "").upper()
        verified = 1 if t.get("isVerified") is True else 0
        exact = 1 if sym == base_u else 0
        has_dec = 1 if t.get("decimals") is not None else 0
        # сортировка: exact -> verified -> has_dec
        return (exact, verified, has_dec)

    if not items:
        return None

    # фильтр: берем только те, у кого symbol совпал или хотя бы содержит base (мягко)
    filtered = []
    for t in items:
        sym = str(t.get("symbol") or "").upper()
        if not sym:
            continue
        if sym == base_u or base_u in sym:
            filtered.append(t)

    pool = filtered if filtered else items
    pool.sort(key=score, reverse=True)

    best = pool[0]
    if best.get("id") and best.get("decimals") is not None:
        return best
    return None

async def fetch_jup_tokens_by_mints(
    session: aiohttp.ClientSession, jup_api_key: str, mints: List[str]
) -> Dict[str, dict]:
    headers = {"x-api-key": jup_api_key}
    out: Dict[str, dict] = {}

    for part in chunks(mints, 100):
        params = {"query": ",".join(part)}
        async with session.get(
            JUP_TOKENS_SEARCH, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=12)
        ) as r:
            if r.status != 200:
                body = await r.text()
                raise RuntimeError(f"Jupiter tokens search failed status={r.status} body={body[:300]}")
            data = await r.json(content_type=None)

        if not isinstance(data, list):
            raise RuntimeError(f"Unexpected Jupiter response: {type(data)} {str(data)[:200]}")

        for t in data:
            mint = t.get("id")
            if mint:
                out[str(mint)] = t

    return out


# ----------------------------- selection logic -----------------------------


@dataclass(frozen=True)
class Candidate:
    bybit_symbol: str
    base_coin: str
    quote_coin: str
    mint: str
    jup_symbol: str
    decimals: int
    turnover24h: Decimal
    spread_bps: Decimal
    top_min_usd: Decimal

    @property
    def score(self) -> Decimal:
        """
        Простая “соковыжималка”:
          - оборот вверх
          - spread вниз
          - top-of-book вверх
        Это не идеальная формула, но работает сильно лучше чем просто turnover.
        """
        # нормализация: добавим 1 чтобы не делить на 0
        s = (self.turnover24h / Decimal("1000000"))  # млн$
        tob = (self.top_min_usd / Decimal("1000"))   # тыс$
        sp = (self.spread_bps + Decimal("1"))
        return (s * Decimal("2") + tob * Decimal("1")) / sp


def unique_token_key(symbol: str, mint: str, bybit_symbol: str, used: set) -> str:
    """
    Ключ уникальный, потому что:
      - в Solana символы дублируются
      - на Bybit может быть один mint в разных quote (USDT/USDC)
    """
    base = symbol.upper()
    if base not in used:
        used.add(base)
        return base

    # Если конфликт — добавляем короткий суффикс от mint + чуть-чуть от bybit_symbol
    suffix = mint[:6]
    q = bybit_symbol[-4:]  # USDT/USDC
    k = f"{base}_{q}_{suffix}"
    used.add(k)
    return k


def build_config_snippet(picked: List[Tuple[str, Candidate]]) -> dict:
    bybit_symbols = [c.bybit_symbol for _, c in picked]
    tokens_map = {
        token_key: {
            "bybit_symbol": c.bybit_symbol,
            "mint": c.mint,
            "decimals": int(c.decimals),
        }
        for token_key, c in picked
    }
    return {"bybit": {"symbols": bybit_symbols}, "trading": {"tokens": tokens_map}}


# ----------------------------- main -----------------------------


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("-c", "--config", default="config.yaml")

    # масштаб
    p.add_argument("--count", type=int, default=300, help="сколько монет хотим в итоге")
    p.add_argument("--bybit-top", type=int, default=6000, help="сколько top-symbols по turnover рассмотреть до фильтров")

    # какие quote монеты на Bybit брать
    p.add_argument("--quotes", default="USDT", help="какие quote брать на Bybit spot, через запятую: USDT,USDC")

    # фильтры “без мусора”
    p.add_argument("--min-turnover24h-usd", type=str, default="0", help="минимум turnover24h (в $)")
    p.add_argument("--min-topbook-usd", type=str, default="0", help="минимум top-of-book (min(bid$, ask$))")
    p.add_argument("--max-spread-bps", type=str, default="999999", help="макс spread bps по bid1/ask1")

    # Jupiter
    p.add_argument("--only-verified", action="store_true", help="только Jupiter isVerified==true (обычно уменьшит список)")
    p.add_argument("--allow-unverified", action="store_true", help="явно разрешить unverified (по умолчанию да)")

    # blacklist/denylist расширяемые
    p.add_argument("--blacklist-base", default="", help="доп. blacklist baseCoin через запятую")
    p.add_argument("--denylist-base", default="", help="доп. denylist baseCoin через запятую")

    # режимы вывода
    p.add_argument("--out", default="watchlist.generated.yaml", help="куда писать snippet")
    p.add_argument("--apply", action="store_true", help="применить в config.yaml (bybit.symbols + trading.tokens)")
    p.add_argument("--log-level", default="INFO")

    args = p.parse_args()
    setup_logging(args.log_level)
    load_dotenv()

    jup_key = os.environ.get("JUP_API_KEY")
    bybit_key = os.environ.get("BYBIT_API_KEY")
    bybit_secret = os.environ.get("BYBIT_API_SECRET")

    if not jup_key:
        raise RuntimeError("Нет JUP_API_KEY в .env")
    if not bybit_key or not bybit_secret:
        raise RuntimeError("Нет BYBIT_API_KEY / BYBIT_API_SECRET в .env (нужно для coin/query-info)")

    config_path = resolve_config_path(args.config)
    if not config_path.exists():
        raise FileNotFoundError(f"config.yaml не найден: {args.config} (cwd={Path.cwd()})")

    cfg = load_yaml(config_path)

    quotes = [q.strip().upper() for q in str(args.quotes).split(",") if q.strip()]
    if not quotes:
        raise RuntimeError("--quotes пустой, укажи хотя бы USDT")

    min_turnover = dec(args.min_turnover24h_usd)
    min_topbook = dec(args.min_topbook_usd)
    max_spread = dec(args.max_spread_bps)

    blacklist = set(DEFAULT_BLACKLIST_BASE)
    denylist = set(DEFAULT_DENYLIST_BASE)

    if args.blacklist_base.strip():
        blacklist |= {x.strip().upper() for x in args.blacklist_base.split(",") if x.strip()}
    if args.denylist_base.strip():
        denylist |= {x.strip().upper() for x in args.denylist_base.split(",") if x.strip()}

    # allow-unverified по умолчанию true (если не включен only-verified)
    allow_unverified = True
    if args.only_verified:
        allow_unverified = False
    if args.allow_unverified:
        allow_unverified = True

    async with aiohttp.ClientSession() as session:
        instruments = await fetch_bybit_spot_instruments(session)
        tickers = await fetch_bybit_spot_tickers(session)
        coin_to_mint = await fetch_bybit_coininfo_sol_mints(session, bybit_key, bybit_secret)

        # 1) список всех Bybit Trading spot symbols нужных quote
        bybit_trading: List[Tuple[Decimal, str, str, str]] = []  # (turnover, symbol, base, quote)
        pre_skip = {
            "not_trading": 0,
            "quote_mismatch": 0,
            "no_ticker": 0,
            "stable_base": 0,
            "deny_base": 0,
            "multiplier": 0,
        }

        for it in instruments:
            if it.get("status") != "Trading":
                pre_skip["not_trading"] += 1
                continue

            quote = str(it.get("quoteCoin") or "").upper()
            if quote not in set(quotes):
                pre_skip["quote_mismatch"] += 1
                continue

            sym = str(it.get("symbol") or "")
            base = str(it.get("baseCoin") or "").upper()
            if not sym or not base:
                continue

            if base in blacklist:
                pre_skip["stable_base"] += 1
                continue
            if base in denylist:
                pre_skip["deny_base"] += 1
                continue
            if MULTIPLIER_RE.match(base):
                pre_skip["multiplier"] += 1
                continue

            t = tickers.get(sym)
            if not t:
                pre_skip["no_ticker"] += 1
                continue

            bybit_trading.append((t.turnover24h, sym, base, quote))

        bybit_trading.sort(key=lambda x: x[0], reverse=True)
        bybit_trading = bybit_trading[: max(1, int(args.bybit_top))]

        # 2) находим mint для каждого Bybit symbol (Solana mint из Bybit coin-info,
        #    либо fallback через Jupiter search по символу)
        mint_inputs: List[str] = []
        sym_to_mint: Dict[str, str] = {}
        sym_to_base_quote: Dict[str, Tuple[str, str]] = {}

        symbol_cache: Dict[str, Optional[dict]] = {}  # ВАЖНО: cache должен жить вне цикла

        for _, sym, base, quote in bybit_trading:
            mint = coin_to_mint.get(base)

            if not mint:
                # fallback: Jupiter search by symbol (один раз на base)
                if base not in symbol_cache:
                    items = await fetch_jup_tokens_by_symbol(session, jup_key, base)
                    symbol_cache[base] = pick_best_jup_token_for_symbol(base, items)

                jt = symbol_cache.get(base)
                if jt:
                    mint = str(jt.get("id") or "")

            if not mint:
                continue

            sym_to_mint[sym] = mint
            sym_to_base_quote[sym] = (base, quote)
            mint_inputs.append(mint)

        # Jupiter tokens search по mint (после того как собрали mint_inputs)
        jup_by_mint = await fetch_jup_tokens_by_mints(
            session,
            jup_key,
            list(dict.fromkeys(mint_inputs)),  # уникализируем, сохраняя порядок
        )



        # Jupiter tokens search по mint
        jup_by_mint = await fetch_jup_tokens_by_mints(session, jup_key, list(dict.fromkeys(mint_inputs)))

    # 3) собираем кандидатов + фильтры качества
    candidates: List[Candidate] = []
    skip = {
        "no_mint": 0,
        "no_jup": 0,
        "no_decimals": 0,
        "not_verified": 0,
        "bad_spread": 0,
        "low_turnover": 0,
        "low_topbook": 0,
        "bad_bidask": 0,
    }

    for turnover, sym, base, quote in bybit_trading:
        mint = sym_to_mint.get(sym)
        if not mint:
            skip["no_mint"] += 1
            continue

        jt = jup_by_mint.get(mint)
        if not jt:
            skip["no_jup"] += 1
            continue

        if (jt.get("isVerified") is not True) and (not allow_unverified):
            skip["not_verified"] += 1
            continue

        decs = jt.get("decimals")
        if decs is None:
            skip["no_decimals"] += 1
            continue

        t = tickers.get(sym)
        if not t:
            continue

        if t.bid1 <= 0 or t.ask1 <= 0:
            skip["bad_bidask"] += 1
            continue

        sp = t.spread_bps
        if sp is None:
            skip["bad_spread"] += 1
            continue

        if turnover < min_turnover:
            skip["low_turnover"] += 1
            continue

        tob = t.top_min_usd
        if tob < min_topbook:
            skip["low_topbook"] += 1
            continue

        if sp > max_spread:
            skip["bad_spread"] += 1
            continue

        jup_sym = str(jt.get("symbol") or "").upper() or base

        candidates.append(
            Candidate(
                bybit_symbol=sym,
                base_coin=base,
                quote_coin=quote,
                mint=mint,
                jup_symbol=jup_sym,
                decimals=int(decs),
                turnover24h=turnover,
                spread_bps=sp,
                top_min_usd=tob,
            )
        )

    candidates.sort(key=lambda c: c.score, reverse=True)

    # 4) pick top N + уникальные token_key
    picked: List[Tuple[str, Candidate]] = []
    used_keys = set()
    for c in candidates:
        token_key = unique_token_key(c.jup_symbol, c.mint, c.bybit_symbol, used_keys)
        picked.append((token_key, c))
        if len(picked) >= int(args.count):
            break

    # 5) печать отчёта
    log.info(
        "Bybit trading considered=%d | sol-mapped=%d | jup-found=%d | candidates=%d | picked=%d",
        len(bybit_trading),
        len(sym_to_mint),
        len(jup_by_mint),
        len(candidates),
        len(picked),
    )
    log.info("pre_skip=%s", pre_skip)
    log.info("skip=%s", skip)

    # 6) пишем YAML
    snippet = build_config_snippet(picked)
    out_path = Path(args.out)
    dump_yaml(out_path, snippet)
    log.info("generated: %s", out_path.resolve())

    if args.apply:
        cfg.setdefault("bybit", {})
        cfg["bybit"]["symbols"] = snippet["bybit"]["symbols"]

        cfg.setdefault("trading", {})
        cfg["trading"]["tokens"] = snippet["trading"]["tokens"]

        dump_yaml(config_path, cfg)
        log.info("applied to: %s", config_path.resolve())


if __name__ == "__main__":
    asyncio.run(main())
