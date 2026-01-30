from __future__ import annotations

import argparse
import asyncio
import os
import time
from decimal import Decimal
import yaml
import aiohttp
from dotenv import load_dotenv
import logging

from utils.log import setup_logging
from core.config import AppConfig
from core.state import MarketState
from connectors.bybit_ws import BybitWS
from connectors.jupiter import JupiterClient
from core.fees import Thresholds
from core.arb_engine import ArbEngine
from notifier.telegram import TelegramNotifier

log = logging.getLogger("app")


def chunked(lst, n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


async def main(cfg_path: str) -> None:
    load_dotenv()

    with open(cfg_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    cfg = AppConfig.model_validate(raw)
    setup_logging(cfg.logging.level)

    tg_token = os.environ.get("TG_BOT_TOKEN")
    jup_key = os.environ.get("JUP_API_KEY")

    if not tg_token:
        raise RuntimeError("Нет TG_BOT_TOKEN в .env")
    if not jup_key:
        raise RuntimeError("Нет JUP_API_KEY в .env")

    state = MarketState()

    async with aiohttp.ClientSession() as session:
        tg = TelegramNotifier(
            session,
            tg_token,
            cfg.telegram.chat_id,
            cfg.telegram.thread_id,
            edit_min_interval_sec=cfg.notifier.edit_min_interval_sec,
            edit_mode=cfg.notifier.edit_mode,
            stale_ttl_sec=cfg.notifier.stale_ttl_sec,
            delete_stale=cfg.notifier.delete_stale,
        )

        jup = JupiterClient(
            session=session,
            base_url=cfg.jupiter.base_url,
            api_key=jup_key,
            timeout_sec=cfg.jupiter.timeout_sec,
            slippage_bps=cfg.jupiter.slippage_bps,
            restrict_intermediate_tokens=cfg.jupiter.restrict_intermediate_tokens,
            max_accounts=cfg.jupiter.max_accounts,
            rps=cfg.rate_limits.jupiter_rps,
            concurrency=cfg.rate_limits.jupiter_concurrency,
            max_retries=cfg.rate_limits.jupiter_max_retries,
        )

        thresholds = Thresholds(
            bybit_taker_fee_bps=Decimal(str(cfg.thresholds.bybit_taker_fee_bps)),
            solana_tx_fee_usd=Decimal(str(cfg.thresholds.solana_tx_fee_usd)),
            latency_buffer_bps=Decimal(str(cfg.thresholds.latency_buffer_bps)),
            usdt_usdc_buffer_bps=Decimal(str(cfg.thresholds.usdt_usdc_buffer_bps)),
            min_profit_usd=Decimal(str(cfg.thresholds.min_profit_usd)),
        )

        async def on_ob(msg: dict) -> None:
            topic = str(msg.get("topic", "") or "")
            typ = str(msg.get("type", "") or "")
            data = msg.get("data")

            if data is None:
                return

            # иногда data бывает листом
            if isinstance(data, list):
                # v5 orderbook обычно не лист, но на всякий
                parts = [x for x in data if isinstance(x, dict)]
            elif isinstance(data, dict):
                parts = [data]
            else:
                return

            now_ms = state.now_ms()

            for it in parts:
                # 1) symbol
                symbol = it.get("s") or it.get("symbol")
                if not symbol:
                    # fallback из topic: orderbook.50.SOLUSDT
                    if topic:
                        symbol = topic.split(".")[-1]

                if not symbol:
                    continue

                # 2) bids/asks в разных формах
                bids = it.get("b") or it.get("bids") or []
                asks = it.get("a") or it.get("asks") or []

                # 3) если вдруг bids/asks лежат внутри it["data"]
                if (not bids and not asks) and isinstance(it.get("data"), dict):
                    inner = it["data"]
                    bids = inner.get("b") or inner.get("bids") or []
                    asks = inner.get("a") or inner.get("asks") or []

                ob = await state.upsert_orderbook(symbol)

                if typ == "snapshot":
                    ob.apply_snapshot(bids, asks, now_ms, now_ms)
                elif typ == "delta":
                    ob.apply_delta(bids, asks, now_ms, now_ms)
                else:
                    # если type нет — попробуем по наличию ключей
                    # snapshot обычно содержит больше уровней, но нам пофиг
                    if bids or asks:
                        # пусть будет delta
                        ob.apply_delta(bids, asks, now_ms, now_ms)


        MAX_SYMBOLS_PER_WS = 100

        bybit_clients = []
        symbol_to_client: dict[str, BybitWS] = {}
        for i, syms in enumerate(chunked(cfg.bybit.symbols, MAX_SYMBOLS_PER_WS), start=1):
            client = BybitWS(
                ws_url=cfg.bybit.ws_url,
                symbols=syms,
                depth=cfg.bybit.depth,
                ping_interval_sec=cfg.bybit.ping_interval_sec,
                on_orderbook_message=on_ob,
            )
            bybit_clients.append(client)
            for s in syms:
                symbol_to_client[s] = client

        log.info("Bybit WS clients=%d, symbols=%d", len(bybit_clients), len(cfg.bybit.symbols))

        token_cfgs = {
            token_key: {
                "bybit_symbol": t.bybit_symbol,
                "mint": t.mint,
                "decimals": t.decimals,
            }
            for token_key, t in cfg.trading.tokens.items()
        }

        engine = ArbEngine(
            state=state,
            jup=jup,
            thresholds=thresholds,
            notional_usd=Decimal(str(cfg.trading.notional_usd)),
            stable_mint=cfg.trading.stable.mint,
            stable_decimals=cfg.trading.stable.decimals,
            token_cfgs=token_cfgs,
            max_cex_slippage_bps=Decimal(str(cfg.filters.max_cex_slippage_bps)),
            max_dex_price_impact_pct=Decimal(str(cfg.filters.max_dex_price_impact_pct)),
            persistence_hits=int(cfg.filters.persistence_hits),
            cooldown_sec=int(cfg.filters.cooldown_sec),
            min_delta_profit_usd_to_resend=Decimal(str(cfg.filters.min_delta_profit_usd_to_resend)),
            engine_tick_hz=int(cfg.runtime.engine_tick_hz),
            jupiter_poll_interval_sec=float(cfg.jupiter.poll_interval_sec),
            price_ratio_max=Decimal(str(cfg.filters.price_ratio_max)),
            gross_profit_cap_pct=Decimal(str(cfg.filters.gross_profit_cap_pct)),
            max_spread_bps=Decimal(str(cfg.filters.max_spread_bps)),
            min_depth_coverage_pct=Decimal(str(cfg.filters.min_depth_coverage_pct)),
            denylist_symbols=cfg.filters.denylist_symbols,
            denylist_regex=cfg.filters.denylist_regex,
        )

        async def on_signal(sig):
            reply_markup = None
            if hasattr(sig, "to_reply_markup"):
                reply_markup = sig.to_reply_markup()
            await tg.upsert(sig.key, sig.text, reply_markup=reply_markup)

        token_by_symbol = {t.bybit_symbol: tk for tk, t in cfg.trading.tokens.items()}
        last_skip_summary = {"text": "n/a"}

        async def status_loop():
            while True:
                symbols = list(cfg.bybit.symbols)
                total = len(symbols)

                fresh_cnt = 0
                non_empty_cnt = 0
                jup_buy_ok = 0
                jup_sell_ok = 0

                FRESH_MS = 2000

                sample_syms = symbols[:5]
                sample_parts = []

                for sym in symbols:
                    ob = await state.get_orderbook(sym)
                    if ob is not None and ob.bids and ob.asks:
                        non_empty_cnt += 1
                        if ob.age_ms() <= FRESH_MS:
                            fresh_cnt += 1

                    token_key = token_by_symbol.get(sym)
                    if token_key:
                        qp = await state.get_quote_pair(token_key)
                        if qp.buy_quote is not None:
                            jup_buy_ok += 1
                        if qp.sell_quote is not None:
                            jup_sell_ok += 1

                for sym in sample_syms:
                    ob = await state.get_orderbook(sym)
                    if ob is None or not ob.bids or not ob.asks:
                        sample_parts.append(f"{sym} OB empty")
                    else:
                        best_bid = max(ob.bids.keys())
                        best_ask = min(ob.asks.keys())
                        sample_parts.append(
                            f"{sym} bid={best_bid} ask={best_ask} age={ob.age_ms()}ms"
                        )

                stats = engine.drain_debug_stats()
                if stats is not None:
                    if stats:
                        top = sorted(stats.items(), key=lambda kv: kv[1], reverse=True)[:5]
                        last_skip_summary["text"] = ", ".join([f"{k}={v}" for k, v in top])
                    else:
                        last_skip_summary["text"] = "none"

                log.info(
                    "[STATUS] OB non-empty %d/%d | OB fresh %d/%d (<=%dms) | JUP buy %d/%d | JUP sell %d/%d | skips(30s): %s | sample: %s",
                    non_empty_cnt, total,
                    fresh_cnt, total, FRESH_MS,
                    jup_buy_ok, total,
                    jup_sell_ok, total,
                    last_skip_summary["text"],
                    " | ".join(sample_parts),
                )

                await asyncio.sleep(5)

        async def stale_loop():
            while True:
                await tg.expire_stale()
                await asyncio.sleep(5)

        async def ws_health_loop():
            timeout_sec = float(cfg.runtime.ws_snapshot_timeout_sec)
            if timeout_sec <= 0:
                return
            last_reconnect: dict[BybitWS, float] = {}
            start_ts = time.time()
            while True:
                await asyncio.sleep(max(5.0, timeout_sec / 2))
                if (time.time() - start_ts) < timeout_sec:
                    continue

                now_ms = state.now_ms()
                stale_syms = []
                for sym in cfg.bybit.symbols:
                    ob = await state.get_orderbook(sym)
                    last_msg_ms = 0
                    if ob is not None:
                        last_msg_ms = int(ob.last_cts_ms or ob.last_update_ms or ob.last_snapshot_ms or 0)

                    if last_msg_ms <= 0 or (now_ms - last_msg_ms) > timeout_sec * 1000:
                        stale_syms.append(sym)

                if stale_syms:
                    clients = set()
                    for sym in stale_syms:
                        client = symbol_to_client.get(sym)
                        if client:
                            clients.add(client)

                    for client in clients:
                        last = last_reconnect.get(client, 0.0)
                        if (time.time() - last) < 5.0:
                            continue
                        client.request_reconnect()
                        last_reconnect[client] = time.time()

                    sample = ", ".join(stale_syms[:5])
                    log.warning(
                        "[HEALTH] stale snapshots %d/%d (>%.0fs) | sample=%s | reconnecting=%d",
                        len(stale_syms),
                        len(cfg.bybit.symbols),
                        timeout_sec,
                        sample,
                        len(clients),
                    )

        tasks = []

        for idx, client in enumerate(bybit_clients, start=1):
            tasks.append(asyncio.create_task(client.run(), name=f"bybit_ws_{idx}"))

        tasks += [
            asyncio.create_task(engine.quote_poller(), name="jup_poller"),
            asyncio.create_task(engine.run(on_signal), name="arb_engine"),
            asyncio.create_task(status_loop(), name="status"),
            asyncio.create_task(stale_loop(), name="tg_stale"),
        ]

        if cfg.runtime.ws_snapshot_timeout_sec > 0:
            tasks.append(asyncio.create_task(ws_health_loop(), name="ws_health"))

        log.info("Bot started. Press Ctrl+C to stop.")

        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            log.info("Stopping...")
        finally:
            for client in bybit_clients:
                await client.stop()
            await engine.stop()
            for t in tasks:
                t.cancel()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("-c", "--config", default="config.yaml", help="Path to config.yaml")
    args = p.parse_args()
    asyncio.run(main(args.config))