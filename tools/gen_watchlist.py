from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import os
import re
import time
import urllib.parse
from decimal import Decimal
from typing import Dict, List, Tuple, Optional

import aiohttp
import yaml
from dotenv import load_dotenv


BYBIT_BASE = "https://api.bybit.com"
BYBIT_INSTRUMENTS = f"{BYBIT_BASE}/v5/market/instruments-info"
BYBIT_TICKERS = f"{BYBIT_BASE}/v5/market/tickers"
BYBIT_COIN_INFO = f"{BYBIT_BASE}/v5/asset/coin/query-info"

JUP_TOKENS_SEARCH = "https://api.jup.ag/tokens/v2/search"

# Solana native mint (на Bybit у SOL contractAddress обычно пустой, поэтому хардкодим)
SOL_MINT = "So11111111111111111111111111111111111111112"

# Чтобы не тащить стейблы в watchlist токенов
BLACKLIST_SYMBOLS = {"USDT", "USDC", "USD1", "USDE", "USDD", "PYUSD", "TUSD", "FDUSD"}
DENYLIST_SYMBOLS = {
    "XAUT",
    "PAXG",
    "AAPLX",
    "GOOGLX",
    "TSLAX",
    "NVDAX",
    "CRCLX",
    "HOODX",
}
MULTIPLIER_RE = re.compile(r"^(1000|10000|100000)[A-Z0-9]+$")


def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def dump_yaml(path: str, data: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False, allow_unicode=True)


def chunks(lst: List[str], n: int) -> List[List[str]]:
    return [lst[i : i + n] for i in range(0, len(lst), n)]


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
    max_retries: int = 8,
) -> dict:
    """
    Приватный GET Bybit v5 с обработкой rate limit 10006:
    читаем X-Bapi-Limit-Reset-Timestamp и ждём.
    """
    recv_window_s = str(recv_window)

    # Bybit подписывает query-string (отсортированный)
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

                # лимит
                if isinstance(j, dict) and j.get("retCode") == 10006:
                    reset = r.headers.get("X-Bapi-Limit-Reset-Timestamp")
                    now_ms = int(time.time() * 1000)
                    if reset and reset.isdigit():
                        sleep_s = max(0.0, (int(reset) - now_ms) / 1000.0) + 0.05
                    else:
                        sleep_s = 0.5 + attempt * 0.3
                    await asyncio.sleep(min(5.0, sleep_s))
                    continue

                raise RuntimeError(f"Bybit private GET error: {j}")

        except aiohttp.ClientError as e:
            # сетевые глюки — небольшой backoff
            await asyncio.sleep(min(2.0, 0.2 + attempt * 0.2))
            if attempt == max_retries - 1:
                raise RuntimeError(f"Bybit private GET network error: {e}") from e

    raise RuntimeError("Bybit private GET: exceeded retries due to rate limit/network issues")


async def fetch_bybit_spot_instruments(session: aiohttp.ClientSession) -> List[dict]:
    """
    Все spot инструменты (нужны baseCoin/quoteCoin/status).
    """
    out: List[dict] = []
    cursor: Optional[str] = None

    while True:
        params = {"category": "spot", "limit": "1000"}
        if cursor:
            params["cursor"] = cursor

        async with session.get(BYBIT_INSTRUMENTS, params=params, timeout=aiohttp.ClientTimeout(total=12)) as r:
            j = await r.json()

        if j.get("retCode") != 0:
            raise RuntimeError(f"Bybit instruments-info error: {j}")

        result = j.get("result", {}) or {}
        out.extend(result.get("list", []) or [])

        cursor = result.get("nextPageCursor")
        if not cursor:
            break

    return out


async def fetch_bybit_spot_tickers(session: aiohttp.ClientSession) -> Dict[str, Decimal]:
    """
    symbol -> turnover24h (для сортировки топа)
    """
    async with session.get(BYBIT_TICKERS, params={"category": "spot"}, timeout=aiohttp.ClientTimeout(total=12)) as r:
        j = await r.json()

    if j.get("retCode") != 0:
        raise RuntimeError(f"Bybit tickers error: {j}")

    result = j.get("result", {}) or {}
    items = result.get("list", []) or []

    m: Dict[str, Decimal] = {}
    for it in items:
        sym = it.get("symbol")
        t24 = it.get("turnover24h")
        if sym and t24 is not None:
            try:
                m[sym] = Decimal(str(t24))
            except Exception:
                pass
    return m


async def fetch_bybit_coininfo_sol_mints(
    session: aiohttp.ClientSession, api_key: str, api_secret: str
) -> Dict[str, str]:
    """
    coin -> solana mint (contractAddress из chainType Solana)
    ВАЖНО: делаем ОДИН запрос без параметра coin.
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

            # На Bybit обычно Solana отображается как chainType=Solana, но подстрахуемся
            if "solana" in chain_type or chain in {"sol", "solana", "spl"}:
                mint = addr
                break

        if mint:
            coin_to_mint[coin] = mint

    # SOL как исключение
    coin_to_mint.setdefault("SOL", SOL_MINT)
    return coin_to_mint


async def fetch_jup_tokens_by_mints(
    session: aiohttp.ClientSession, jup_api_key: str, mints: List[str]
) -> Dict[str, dict]:
    """
    Jupiter Tokens v2 Search:
    query=comma-separated mint addresses (до 100).
    """
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
            data = await r.json()

        if not isinstance(data, list):
            raise RuntimeError(f"Unexpected Jupiter response: {type(data)} {str(data)[:200]}")

        for t in data:
            mint = t.get("id")
            if mint:
                out[mint] = t

    return out


def unique_token_key(symbol: str, mint: str, used: set) -> str:
    """
    В Solana бывает, что символы дублируются.
    Делаем ключ уникальным, чтобы движок не путался.
    """
    base = symbol.upper()
    if base not in used:
        used.add(base)
        return base
    suffix = mint[:6]
    k = f"{base}_{suffix}"
    used.add(k)
    return k


def build_config_snippet(picked: List[dict]) -> dict:
    bybit_symbols = [p["bybit_symbol"] for p in picked]
    tokens_map = {}
    for p in picked:
        tokens_map[p["token_key"]] = {
            "bybit_symbol": p["bybit_symbol"],
            "mint": p["mint"],
            "decimals": int(p["decimals"]),
        }
    return {"bybit": {"symbols": bybit_symbols}, "trading": {"tokens": tokens_map}}


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("-c", "--config", default="config.yaml")
    p.add_argument("--count", type=int, default=50)
    p.add_argument("--bybit-top", type=int, default=300, help="сколько топ-пар Bybit (по turnover24h) рассмотреть")
    p.add_argument("--only-verified", action="store_true", help="только Jupiter isVerified == true")
    p.add_argument("--apply", action="store_true", help="применить в config.yaml")
    args = p.parse_args()

    load_dotenv()

    jup_key = os.environ.get("JUP_API_KEY")
    bybit_key = os.environ.get("BYBIT_API_KEY")
    bybit_secret = os.environ.get("BYBIT_API_SECRET")

    if not jup_key:
        raise RuntimeError("Нет JUP_API_KEY в .env")
    if not bybit_key or not bybit_secret:
        raise RuntimeError("Нет BYBIT_API_KEY / BYBIT_API_SECRET в .env (нужно для coin/query-info)")

    cfg = load_yaml(args.config)

    async with aiohttp.ClientSession() as session:
        instruments = await fetch_bybit_spot_instruments(session)
        tickers = await fetch_bybit_spot_tickers(session)
        coin_to_mint = await fetch_bybit_coininfo_sol_mints(session, bybit_key, bybit_secret)

        # Собираем Bybit spot USDT пары (Trading) и сортируем по turnover24h
        candidates: List[Tuple[Decimal, str, str]] = []  # (turnover, bybit_symbol, base_coin)
        pre_skip = {"stable_base": 0, "denied_base": 0, "multiplier_base": 0}
        for it in instruments:
            if it.get("status") != "Trading":
                continue
            if it.get("quoteCoin") != "USDT":
                continue
            sym = it.get("symbol")
            base = it.get("baseCoin")
            if not sym or not base:
                continue
            if base in BLACKLIST_SYMBOLS:
                pre_skip["stable_base"] += 1
                continue
            if base in DENYLIST_SYMBOLS:
                pre_skip["denied_base"] += 1
                continue
            if MULTIPLIER_RE.match(base):
                pre_skip["multiplier_base"] += 1
                continue
            t = tickers.get(sym, Decimal("0"))
            candidates.append((t, sym, base))

        candidates.sort(key=lambda x: x[0], reverse=True)
        candidates = candidates[: max(1, int(args.bybit_top))]

        # Берём mint'ы только тех baseCoin, у кого есть Solana mint
        coins_order: List[Tuple[str, str]] = []  # (bybit_symbol, base_coin)
        mints: List[str] = []
        seen_coin = set()
        for _, sym, base in candidates:
            if base in seen_coin:
                continue
            seen_coin.add(base)

            mint = coin_to_mint.get(base)
            if not mint:
                continue
            coins_order.append((sym, base))
            mints.append(mint)

        jup_by_mint = await fetch_jup_tokens_by_mints(session, jup_key, mints)

    # Пиким пересечение: Bybit(top turnover) ∩ (есть Solana mint) ∩ Jupiter(search by mint)
    picked: List[dict] = []
    used_keys = set()
    skip = {
        "no_jup": 0,
        "not_verified": 0,
        "blacklist": 0,
        "no_decimals": 0,
    }

    for bybit_symbol, base in coins_order:
        mint = coin_to_mint.get(base)
        if not mint:
            continue

        jt = jup_by_mint.get(mint)
        if not jt:
            skip["no_jup"] += 1
            continue

        sym = (jt.get("symbol") or "").upper()
        if not sym or sym in BLACKLIST_SYMBOLS or sym in DENYLIST_SYMBOLS:
            skip["blacklist"] += 1
            continue

        if args.only_verified and jt.get("isVerified") is not True:
            skip["not_verified"] += 1
            continue

        dec = jt.get("decimals")
        if dec is None:
            skip["no_decimals"] += 1
            continue

        token_key = unique_token_key(sym, mint, used_keys)

        picked.append(
            {
                "token_key": token_key,
                "bybit_symbol": bybit_symbol,
                "base_coin": base,
                "mint": mint,
                "decimals": int(dec),
                "turnover24h": str(tickers.get(bybit_symbol, Decimal("0"))),
            }
        )

        if len(picked) >= int(args.count):
            break

    print(
        f"Bybit instruments(spot USDT Trading)={len([1 for it in instruments if it.get('status')=='Trading' and it.get('quoteCoin')=='USDT'])} | "
        f"Candidates(top)={len(candidates)} | "
        f"Solana-mapped={len(coins_order)} | "
        f"Jup found={len(jup_by_mint)} | "
        f"picked={len(picked)} | pre_skip={pre_skip} | skip={skip}"
    )

    if len(picked) < int(args.count):
        print("⚠️ Не набралось нужное количество. Попробуй:")
        print("   - увеличить --bybit-top (например 600/900)")
        print("   - убрать --only-verified (если ок брать unverified)")
        print("   - или оставить 30-40 (так тоже норм для MVP)")

    snippet = build_config_snippet(picked)
    out_path = "watchlist.generated.yaml"
    dump_yaml(out_path, snippet)
    print(f"✅ Сгенерировано: {out_path}")

    if args.apply:
        cfg.setdefault("bybit", {})
        cfg["bybit"]["symbols"] = snippet["bybit"]["symbols"]

        cfg.setdefault("trading", {})
        cfg["trading"]["tokens"] = snippet["trading"]["tokens"]

        dump_yaml(args.config, cfg)
        print(f"✅ Применено в {args.config}")


if __name__ == "__main__":
    asyncio.run(main())
