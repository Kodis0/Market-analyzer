import asyncio, aiohttp, yaml

URL = "https://api.bybit.com/v5/market/orderbook"

async def main():
    cfg = yaml.safe_load(open("config.yaml","r",encoding="utf-8"))
    syms = cfg["bybit"]["symbols"]
    ok, bad = [], []

    async with aiohttp.ClientSession() as s:
        for sym in syms:
            params = {"category":"spot","symbol":sym,"limit":"1"}
            async with s.get(URL, params=params) as r:
                j = await r.json()
            if j.get("retCode") != 0:
                bad.append((sym, j.get("retMsg")))
                continue
            lst = (j.get("result") or {}).get("b") or []
            lst2 = (j.get("result") or {}).get("a") or []
            if lst and lst2:
                ok.append(sym)
            else:
                bad.append((sym, "empty book"))

    print("OK:", len(ok))
    print("BAD:", len(bad))
    print("BAD sample:", bad[:15])

asyncio.run(main())
