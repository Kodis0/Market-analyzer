"""
Пересчёт текста сигнала для обновления сообщения в Telegram без emit (dedup/persistence).
Возвращает Signal с актуальными ценами и временем «Последнее изменение / проверка бирж».
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import TYPE_CHECKING

from core.arb.engine import B_AMOUNT_RATIO_MAX, B_AMOUNT_RATIO_MIN, CUSTOM_ALERT_EMOJI_HTML
from core.arb.types import Buttons, Signal
from core.arb.utils import bybit_spot_url, from_raw, jup_swap_url_by_symbol, snapshot_book, to_raw
from core.calc import calc_mid_spread, coverage_pct, gross_cap_ok, net_profit, price_ratio_ok
from core.vwap import simulate_buy_with_notional, simulate_sell_base

if TYPE_CHECKING:
    from core.arb.engine import ArbEngine


def _tz_msk():
    return timezone(timedelta(hours=3))


async def build_snapshot_signal_for_refresh(
    engine: ArbEngine,
    token_key: str,
    direction: str,
) -> tuple[Signal | None, bool]:
    """
    (Signal, False) — обновить сообщение в TG.
    (None, False) — данных нет / временный сбой (не трогать сообщение).
    (None, True) — прибыль <= 0 при валидном расчёте (удалить сообщение).
    """
    cfg = engine.token_cfgs.get(token_key)
    if not cfg:
        return None, False

    mint = str(cfg.get("mint", ""))
    bybit_symbol = str(cfg.get("bybit_symbol", ""))
    decimals = int(cfg.get("decimals", 0) or 0)

    if engine._is_pump_mint(mint):
        return None, False
    if engine.denylist.is_denied(token_key, bybit_symbol):
        return None, False
    if decimals <= 0 or decimals > 18:
        return None, False

    ob = await engine.state.get_orderbook(bybit_symbol)
    if ob is None or not ob.bids or not ob.asks:
        return None, False
    if ob.age_ms() > engine.max_ob_age_ms:
        return None, False

    bids, asks = snapshot_book(ob)
    mid, spread_bps = calc_mid_spread(bids, asks)
    if mid is None or spread_bps is None:
        return None, False
    if spread_bps > engine.max_spread_bps:
        return None, False

    qp = await engine.state.get_quote_pair(token_key)
    now_ms = engine.state.now_ms()
    bybit_last_cts_ms = int(ob.last_cts_ms or ob.last_update_ms or now_ms)
    tz_msk = _tz_msk()

    required = engine.thresholds.required_profit_usd(engine.notional)
    stable_mint = engine.stable_mint
    stable_decimals = engine.stable_decimals
    stable_raw = to_raw(engine.notional, stable_decimals)

    bybit_url = bybit_spot_url(bybit_symbol)
    jup_buy_url = jup_swap_url_by_symbol(bybit_symbol, buy=True)
    jup_sell_url = jup_swap_url_by_symbol(bybit_symbol, buy=False)

    if direction == "JUP->BYBIT":
        async with qp.lock:
            j_buy = qp.buy_quote
            buy_updated_ms = int(qp.buy_updated_ms or 0)

        if (
            j_buy is None
            or (now_ms - buy_updated_ms) > int(engine.max_quote_age_ms)
            or j_buy.input_mint != stable_mint
            or j_buy.output_mint != mint
        ):
            j_buy = await engine.jup.quote_exact_in(stable_mint, mint, stable_raw)
            buy_updated_ms = now_ms if j_buy is not None else 0

        if (
            j_buy is None
            or j_buy.input_mint != stable_mint
            or j_buy.output_mint != mint
            or Decimal(str(j_buy.price_impact_pct)) > engine.max_dex_price_impact_pct
        ):
            return None, False

        token_out = from_raw(int(j_buy.out_amount_raw), decimals)
        if token_out <= 0:
            return None, False

        sim_sell = simulate_sell_base(bids, token_out)
        if sim_sell is None:
            return None, False

        depth_cov = coverage_pct(Decimal(str(sim_sell.base_out)), token_out)
        if depth_cov < engine.min_depth_coverage_pct:
            return None, False
        if Decimal(str(sim_sell.slippage_bps)) > engine.max_cex_slippage_bps:
            return None, False

        stable_out = Decimal(str(sim_sell.quote_out))
        if stable_out <= 0:
            return None, False

        jup_implied = engine.notional / token_out
        if not price_ratio_ok(jup_implied, mid, engine.max_price_ratio):
            return None, False
        if not gross_cap_ok(stable_out, engine.notional, engine.max_gross_profit_pct):
            return None, False

        profit = net_profit(stable_out, engine.notional, required)
        if profit <= 0:
            return None, True

        net_pct = (profit / engine.notional) * Decimal("100")
        price_jup = engine.notional / token_out
        price_bybit = sim_sell.avg_price
        tg_updated_ts = int(time.time())
        tg_updated_str = datetime.fromtimestamp(tg_updated_ts, tz=tz_msk).strftime("%Y-%m-%d %H:%M:%S")
        jup_last_ts = int(buy_updated_ms // 1000) if buy_updated_ms else 0
        bybit_last_ts = int(bybit_last_cts_ms // 1000) if bybit_last_cts_ms else 0
        jup_age_ms = max(0, now_ms - buy_updated_ms) if buy_updated_ms else 0
        bybit_age_ms = max(0, now_ms - bybit_last_cts_ms) if bybit_last_cts_ms else 0
        jup_last_str = (
            datetime.fromtimestamp(jup_last_ts, tz=tz_msk).strftime("%Y-%m-%d %H:%M:%S") if jup_last_ts else "n/a"
        )
        bybit_last_str = (
            datetime.fromtimestamp(bybit_last_ts, tz=tz_msk).strftime("%Y-%m-%d %H:%M:%S")
            if bybit_last_ts
            else "n/a"
        )
        text = (
            f"{CUSTOM_ALERT_EMOJI_HTML} <b>АРБИТРАЖ</b> • <b>{token_key}</b>\n"
            f"Маршрут: <b>Jupiter → Bybit</b>\n"
            f"Объём: <code>{engine.notional:.0f} USDC</code>\n"
            f"Ожидаемый выход: <code>{stable_out:.2f} USDT</code>\n"
            f"Чистая прибыль: <b>{profit:.2f}$</b> (<b>{net_pct:.2f}%</b>)\n"
            f"Комиссии/запас: <code>{required:.2f}$</code>\n"
            f"Цена на Jupiter: <code>{price_jup:.6f}$</code>\n"
            f"Цена на Bybit: <code>{price_bybit:.6f}$</code>"
            f"\n\n<b>Последнее изменение (Telegram):</b> <code>{tg_updated_str} MSK</code>"
            f"\n<b>Последняя проверка бирж:</b>\n<code>Jupiter: {jup_last_str} ({jup_age_ms}ms)</code>\n<code>Bybit: {bybit_last_str} ({bybit_age_ms}ms)</code>"
        )
        buttons: Buttons = [[("Купить на Jupiter", jup_buy_url), ("Продать на Bybit", bybit_url)]]
        key = f"{token_key}:JUP->BYBIT"
        sig = Signal(
            key,
            token_key,
            "JUP->BYBIT",
            profit,
            engine.notional,
            text,
            buttons,
            mint=mint,
        )
        return sig, False

    if direction == "BYBIT->JUP":
        async with qp.lock:
            j_sell = qp.sell_quote
            sell_updated_ms = int(qp.sell_updated_ms or 0)
            sell_amount_raw = int(qp.sell_amount_raw or 0)

        sim_buy2 = simulate_buy_with_notional(asks, engine.notional)
        if sim_buy2 is None:
            return None, False

        depth_cov2 = coverage_pct(Decimal(str(sim_buy2.quote_out)), engine.notional)
        if depth_cov2 < engine.min_depth_coverage_pct:
            return None, False
        if Decimal(str(sim_buy2.slippage_bps)) > engine.max_cex_slippage_bps:
            return None, False

        token_out2 = Decimal(str(sim_buy2.base_out))
        if token_out2 <= 0:
            return None, False

        expected_raw = to_raw(token_out2, decimals)
        expected_raw_int = int(expected_raw)
        if expected_raw_int <= 0:
            return None, False

        need_requote = False
        if (
            j_sell is None
            or sell_updated_ms <= 0
            or (now_ms - sell_updated_ms) > int(engine.max_quote_age_ms)
            or sell_amount_raw <= 0
            or j_sell.input_mint != mint
            or j_sell.output_mint != stable_mint
        ):
            need_requote = True
        elif Decimal(str(j_sell.price_impact_pct)) > engine.max_dex_price_impact_pct:
            need_requote = True
        else:
            ratio = Decimal(expected_raw_int) / Decimal(sell_amount_raw)
            if not (B_AMOUNT_RATIO_MIN <= ratio <= B_AMOUNT_RATIO_MAX):
                need_requote = True

        if need_requote:
            j_sell = await engine.jup.quote_exact_in(mint, stable_mint, expected_raw_int)
            sell_updated_ms = now_ms if j_sell is not None else 0

        if (
            j_sell is None
            or j_sell.input_mint != mint
            or j_sell.output_mint != stable_mint
            or Decimal(str(j_sell.price_impact_pct)) > engine.max_dex_price_impact_pct
        ):
            return None, False

        stable_out2 = from_raw(int(j_sell.out_amount_raw), stable_decimals)
        if stable_out2 <= 0:
            return None, False

        jup_implied2 = stable_out2 / token_out2
        if not price_ratio_ok(jup_implied2, mid, engine.max_price_ratio):
            return None, False
        if not gross_cap_ok(stable_out2, engine.notional, engine.max_gross_profit_pct):
            return None, False

        profit2 = net_profit(stable_out2, engine.notional, required)
        if profit2 <= 0:
            return None, True

        net_pct2 = (profit2 / engine.notional) * Decimal("100")
        price_bybit2 = sim_buy2.avg_price
        price_jup2 = stable_out2 / token_out2
        tg_updated_ts2 = int(time.time())
        tg_updated_str2 = datetime.fromtimestamp(tg_updated_ts2, tz=tz_msk).strftime("%Y-%m-%d %H:%M:%S")
        jup_last_ts2 = int(sell_updated_ms // 1000) if sell_updated_ms else 0
        bybit_last_ts2 = int(bybit_last_cts_ms // 1000) if bybit_last_cts_ms else 0
        jup_age_ms2 = max(0, now_ms - sell_updated_ms) if sell_updated_ms else 0
        bybit_age_ms2 = max(0, now_ms - bybit_last_cts_ms) if bybit_last_cts_ms else 0
        jup_last_str2 = (
            datetime.fromtimestamp(jup_last_ts2, tz=tz_msk).strftime("%Y-%m-%d %H:%M:%S") if jup_last_ts2 else "n/a"
        )
        bybit_last_str2 = (
            datetime.fromtimestamp(bybit_last_ts2, tz=tz_msk).strftime("%Y-%m-%d %H:%M:%S")
            if bybit_last_ts2
            else "n/a"
        )
        text2 = (
            f"{CUSTOM_ALERT_EMOJI_HTML} <b>АРБИТРАЖ</b> • <b>{token_key}</b>\n"
            f"Маршрут: <b>Bybit → Jupiter</b>\n"
            f"Объём: <code>{engine.notional:.0f} USDC</code>\n"
            f"Ожидаемый выход: <code>{stable_out2:.2f} USDT</code>\n"
            f"Чистая прибыль: <b>{profit2:.2f}$</b> (<b>{net_pct2:.2f}%</b>)\n"
            f"Комиссии/запас: <code>{required:.2f}$</code>\n"
            f"Цена на Bybit: <code>{price_bybit2:.6f}$</code>\n"
            f"Цена на Jupiter: <code>{price_jup2:.6f}$</code>"
            f"\n\n<b>Последнее изменение (Telegram):</b> <code>{tg_updated_str2} MSK</code>"
            f"\n<b>Последняя проверка бирж:</b>\n<code>Jupiter: {jup_last_str2} ({jup_age_ms2}ms)</code>\n<code>Bybit: {bybit_last_str2} ({bybit_age_ms2}ms)</code>"
        )
        buttons2: Buttons = [[("Купить на Bybit", bybit_url), ("Продать на Jupiter", jup_sell_url)]]
        key2 = f"{token_key}:BYBIT->JUP"
        sig2 = Signal(
            key2,
            token_key,
            "BYBIT->JUP",
            profit2,
            engine.notional,
            text2,
            buttons2,
            mint=mint,
        )
        return sig2, False

    return None, False
