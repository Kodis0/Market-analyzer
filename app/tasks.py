"""Background task loops: status, stale, auto_tune, stats_heartbeat, ws_health."""

from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from core.auto_tune.tuner import AutoTuner, TunerBounds, TunerConfig
from core.arb.engine import B_AMOUNT_RATIO_MAX, B_AMOUNT_RATIO_MIN
from core.arb.utils import from_raw, snapshot_book, to_raw
from core.calc import calc_mid_spread, coverage_pct, gross_cap_ok, net_profit, price_ratio_ok
from core.vwap import simulate_buy_with_notional, simulate_sell_base
from core.runtime_settings import save_runtime_settings

from app.handlers import AUTO_TUNE_HISTORY_MAX, make_apply_settings_reload
from app.signal_message_parse import parse_arb_signal_from_message

if TYPE_CHECKING:
    from app.context import AppContext

log = logging.getLogger("app.tasks")

# Один ключ за раз; Jupiter/стакан не должны блокировать /cleanup бесконечно.
CLEANUP_PROFIT_CHECK_TIMEOUT_SEC = 40.0
# Ручной /cleanup all: сколько слотов максимум за один проход (остальное — повторить команду).
HOURLY_PROFIT_CHECK_MAX_ROWS = 120
# Фоновая перепроверка прибыли: интервал и размер пачки (round-robin по всем слотам).
TG_PROFIT_RECHECK_INTERVAL_SEC = 45.0
TG_PROFIT_RECHECK_MAX_PER_TICK = 8
# Если отслеживаемых слотов нет — реже будим цикл (меньше пустых запросов к БД).
TG_PROFIT_RECHECK_IDLE_INTERVAL_SEC = 120.0


def _rotate_signal_batch(rows: list[dict], offset: int, take: int) -> tuple[list[dict], int]:
    """Стабильный round-robin: take слотов из rows, offset сдвигается по кругу."""
    n = len(rows)
    if n == 0:
        return [], 0
    take = min(int(take), n)
    batch = [rows[(offset + i) % n] for i in range(take)]
    new_offset = (offset + take) % n
    return batch, new_offset


def make_status_loop(ctx: AppContext):
    async def status_loop():
        _n = len(ctx.cfg.bybit.symbols)
        status_sample_step = max(1, min(10, _n // 50)) if _n > 100 else 1
        FRESH_MS = 2000

        while True:
            symbols = list(ctx.cfg.bybit.symbols)
            total = len(symbols)
            fresh_cnt = 0
            non_empty_cnt = 0
            sampled_n = 0
            sample_syms = symbols[:5]
            sample_parts: list[str] = []

            for i in range(0, total, status_sample_step):
                sym = symbols[i]
                ob = await ctx.state.get_orderbook(sym)
                sampled_n += 1
                if ob is not None and ob.bids and ob.asks:
                    non_empty_cnt += 1
                    if ob.age_ms() <= FRESH_MS:
                        fresh_cnt += 1

            if sampled_n > 0 and status_sample_step > 1:
                scale = total / sampled_n
                non_empty_cnt = min(total, int(non_empty_cnt * scale))
                fresh_cnt = min(total, int(fresh_cnt * scale))

            for sym in sample_syms:
                ob = await ctx.state.get_orderbook(sym)
                if ob is None or not ob.bids or not ob.asks:
                    sample_parts.append(f"{sym} OB empty")
                else:
                    best_bid = max(ob.bids.keys())
                    best_ask = min(ob.asks.keys())
                    sample_parts.append(f"{sym} bid={best_bid} ask={best_ask} age={ob.age_ms()}ms")

            stats = ctx.engine.drain_debug_stats()
            if stats is not None:
                ctx.metrics_collector.record_skips(stats)
                skip_text = (
                    ", ".join(
                        f"{k}={v}"
                        for k, v in sorted(stats.items(), key=lambda kv: kv[1], reverse=True)[:5]
                    )
                    if stats
                    else "none"
                )
            else:
                skip_text = "n/a"

            status_interval = float(getattr(ctx.cfg.runtime, "status_interval_sec", 15))
            log.info(
                "[STATUS] active=%d | quarantined=%d | OB non-empty %d/%d | OB fresh %d/%d (<=%dms) | skips(30s): %s | sample: %s",
                total,
                len(ctx.q_manager.quarantined_set),
                non_empty_cnt,
                total,
                fresh_cnt,
                total,
                FRESH_MS,
                skip_text,
                " | ".join(sample_parts),
            )
            await asyncio.sleep(status_interval)

    return status_loop


def make_stale_loop(ctx: AppContext):
    async def stale_loop():
        while True:
            await ctx.tg.expire_stale()
            await asyncio.sleep(5)

    return stale_loop


TG_VERIFY_INTERVAL_SEC = 30 * 60  # 30 min — редкая перепроверка


def make_tg_verify_loop(ctx: AppContext):
    """Periodic verify: delete stale messages from DB that we missed (e.g. crash). Cleans DB."""

    async def tg_verify_loop():
        while True:
            await asyncio.sleep(TG_VERIFY_INTERVAL_SEC)
            if not ctx.settings.delete_stale:
                continue
            try:
                await ctx.tg.verify_and_cleanup_stale()
            except Exception as e:
                log.warning("tg_verify_loop error: %s", e)

    return tg_verify_loop


async def get_tg_cleanup_counts(ctx: AppContext) -> dict[str, int | float | bool]:
    """Числа для диагностики /cleanup (один проход — без лишних дублей запросов)."""
    try:
        from api.db import get_tg_messages_async

        db_rows = await get_tg_messages_async()
        n_db = len(db_rows)
    except Exception:
        n_db = -1
    n_mem = len(getattr(ctx.tg, "_msg_ids", {}) or {})
    ttl = float(getattr(ctx.tg, "stale_ttl_sec", 0) or 0)
    try:
        stale = await ctx.tg.list_stale_cleanup_candidates_async()
        n_stale = len(stale)
    except Exception:
        n_stale = -1
    try:
        all_trk = await ctx.tg.list_all_tracked_signal_slots_async()
        n_all = len(all_trk)
    except Exception:
        n_all = -1
    return {
        "n_db": n_db,
        "n_mem": n_mem,
        "stale_ttl_sec": ttl,
        "n_stale": n_stale,
        "n_all": n_all,
        "exchange_enabled": bool(ctx.settings.exchange_enabled),
    }


def format_tg_cleanup_diagnostics_from_counts(c: dict[str, int | float | bool]) -> str:
    """Текст сводки из результата get_tg_cleanup_counts (без повторных запросов к БД)."""
    exch = "вкл" if c["exchange_enabled"] else "выкл"
    ttl = float(c["stale_ttl_sec"])
    return (
        f"• строк в БД <code>tg_messages</code>: {c['n_db']}\n"
        f"• слотов в памяти бота: {c['n_mem']}\n"
        f"• <code>stale_ttl_sec</code>: {ttl:.0f} (0 = режим /cleanup по TTL пустой)\n"
        f"• кандидатов «устаревших по TTL» (только для <code>/cleanup</code>): {c['n_stale']}\n"
        f"• всего отслеживаемых слотов (<code>/cleanup all</code>): {c['n_all']}\n"
        f"• биржа <code>exchange_enabled</code>: {exch}"
    )


async def format_tg_cleanup_diagnostics(ctx: AppContext) -> str:
    """Сводка для ответа в Telegram (один round-trip к данным)."""
    c = await get_tg_cleanup_counts(ctx)
    return format_tg_cleanup_diagnostics_from_counts(c)


async def cleanup_non_profitable_msgs_profit_based_once(
    ctx: AppContext,
    max_rows: int = 60,
    progress_cb: Callable[[str], Awaitable[None]] | None = None,
    *,
    only_ttl_stale: bool = True,
    rows_override: list[dict] | None = None,
    quiet: bool = False,
) -> tuple[int, int]:
    """
    Пересчёт арбитражной прибыли (Bybit OB + Jupiter) как в движке; при profit <= 0 — delete в Telegram.

    only_ttl_stale=True (/cleanup по TTL): только слоты старше stale_ttl_sec (БД+память).
    only_ttl_stale=False (фоновый job): все отслеживаемые слоты из tg_messages — без фильтра TTL,
    либо явный список rows_override (round-robin пачками).

    Важно: Bot API не отдаёт список всех сообщений чата — работаем только с тем, что сами сохранили
    при send/edit (таблица tg_messages + кэш notifier).

    Возвращает (deleted_count, checked_count).
    """
    if rows_override is None:
        if only_ttl_stale and ctx.tg.stale_ttl_sec <= 0:
            if progress_cb:
                await progress_cb(
                    "⚠️ Режим <code>/cleanup</code>: в настройках <code>stale_ttl_sec</code> = 0 — "
                    "по возрасту кандидатов нет. Используйте <code>/cleanup all</code>."
                )
            return 0, 0

    if not only_ttl_stale and not ctx.settings.exchange_enabled:
        log.warning(
            "[TG_PROFIT] recheck skipped: exchange_enabled=false (нужны стакан Bybit + Jupiter). "
            "Включите: /exchange on или в runtime settings."
        )
        if progress_cb:
            await progress_cb(
                "🚫 <code>exchange_enabled=false</code> — без стакана Bybit и Jupiter пересчёт прибыли "
                "невозможен. Включите: <code>/exchange on</code>."
            )
        return 0, 0

    if not quiet and progress_cb:
        await progress_cb(
            "🔄 Сбор кандидатов (TTL)…"
            if only_ttl_stale
            else "🔄 Сбор всех отслеживаемых слотов (БД + память)…"
        )

    if rows_override is not None:
        rows = rows_override
    elif only_ttl_stale:
        rows = await ctx.tg.list_stale_cleanup_candidates_async()
    else:
        rows = await ctx.tg.list_all_tracked_signal_slots_async()
        log.info(
            "[TG_PROFIT] recheck: отслеживаемых слотов=%d, за этот проход проверим до %d",
            len(rows),
            max_rows,
        )

    if not rows:
        if progress_cb:
            await progress_cb(
                "ℹ️ Нечего проверять (нет записей в tg_messages / слотов в памяти)."
            )
        if not only_ttl_stale:
            log.info(
                "[TG_PROFIT] recheck: слотов нет — бот ещё не сохранял message_id в tg_messages "
                "(или таблица пуста после чистки)."
            )
        return 0, 0

    if progress_cb and not quiet and rows and ctx.settings.exchange_enabled:
        keys = [str(r.get("key") or "") for r in rows[:max_rows] if r.get("key")]
        head = keys[:10]
        tail = len(keys) - len(head)
        sample = ", ".join(f"<code>{k}</code>" for k in head)
        if tail > 0:
            sample += f"\n… и ещё <b>{tail}</b> в этом проходе"
        mode = "по TTL" if only_ttl_stale else "все слоты"
        await progress_cb(
            f"📋 Режим: <b>{mode}</b>. В очереди <b>{len(rows)}</b> слот(ов), "
            f"за проход проверю до <b>{min(len(rows), max_rows)}</b>.\n{sample}"
        )

    if not ctx.settings.exchange_enabled:
        # Только режим «по TTL» при выключенном обмене: удалить по возрасту без пересчёта прибыли.
        deleted_count = 0
        checked_count = 0
        total = min(len(rows), max_rows)
        for row in rows[:max_rows]:
            key = row.get("key")
            msg_id = row.get("message_id")
            if not key or msg_id is None:
                continue
            checked_count += 1
            await ctx.tg.delete_message_for_key(str(key), int(msg_id))
            deleted_count += 1
            left_count = checked_count - deleted_count
            if progress_cb:
                await progress_cb(
                    "🔄 Ручная проверка (обмен выключен — только TTL из БД)…\n"
                    f"Обработано: {checked_count}/{total}\n"
                    f"Удалено: {deleted_count}\n"
                    f"Оставлено: {left_count}"
                )
        return deleted_count, checked_count

    deleted_count = 0
    checked_count = 0

    now_ms = ctx.state.now_ms()
    stable_mint = ctx.engine.stable_mint
    stable_decimals = ctx.engine.stable_decimals
    notional = ctx.engine.notional
    stable_raw = to_raw(notional, stable_decimals)

    required = ctx.engine.thresholds.required_profit_usd(notional)

    async def is_profitable_key(key: str, message_hint: str | None = None) -> bool:
        """
        Профит как у движка: Bybit стакан + Jupiter quote.
        Токен и направление: из текста сообщения (если распознано и токен есть в конфиге), иначе из key.
        """
        if ":" not in key:
            return False
        left, direction = key.split(":", 1)
        if direction not in ("JUP->BYBIT", "BYBIT->JUP"):
            return False
        token_key = left
        if token_key not in ctx.cfg.trading.tokens:
            for tk, tc in ctx.cfg.trading.tokens.items():
                if getattr(tc, "mint", "") == token_key:
                    token_key = tk
                    break
        if message_hint:
            parsed = parse_arb_signal_from_message(message_hint)
            if parsed:
                tk, dr = parsed
                if ctx.cfg.trading.tokens.get(tk):
                    token_key, direction = tk, dr
        token_cfg = ctx.cfg.trading.tokens.get(token_key)
        if not token_cfg:
            return False

        bybit_symbol = getattr(token_cfg, "bybit_symbol", "")
        mint = getattr(token_cfg, "mint", "")
        decimals = int(getattr(token_cfg, "decimals", 0) or 0)
        if not bybit_symbol or not mint or decimals <= 0 or decimals > 18:
            return False

        ob = await ctx.state.get_orderbook(bybit_symbol)
        if ob is None or not ob.bids or not ob.asks:
            return False
        if ob.age_ms() > int(getattr(ctx.engine, "max_ob_age_ms", 2000)):
            return False

        bids, asks = snapshot_book(ob)
        mid, spread_bps = calc_mid_spread(bids, asks)
        if mid is None or spread_bps is None:
            return False
        if spread_bps > ctx.engine.max_spread_bps:
            return False

        qp = await ctx.state.get_quote_pair(token_key)

        # Snapshot quote under lock for consistency
        async with qp.lock:
            j_buy = qp.buy_quote
            buy_updated_ms = int(qp.buy_updated_ms or 0)
            j_sell = qp.sell_quote
            sell_updated_ms = int(qp.sell_updated_ms or 0)
            sell_amount_raw = int(qp.sell_amount_raw or 0)

        if direction == "JUP->BYBIT":
            # stable -> token quote (Jupiter)
            if (
                j_buy is None
                or (now_ms - buy_updated_ms) > int(ctx.engine.max_quote_age_ms)
                or j_buy.input_mint != stable_mint
                or j_buy.output_mint != mint
            ):
                j_buy = await ctx.jup.quote_exact_in(stable_mint, mint, stable_raw)
            if (
                j_buy is None
                or j_buy.input_mint != stable_mint
                or j_buy.output_mint != mint
                or Decimal(str(j_buy.price_impact_pct)) > ctx.engine.max_dex_price_impact_pct
            ):
                return False

            token_out = from_raw(int(j_buy.out_amount_raw), decimals)
            if token_out <= 0:
                return False

            sim_sell = simulate_sell_base(bids, token_out)
            if sim_sell is None:
                return False

            depth_cov = coverage_pct(Decimal(str(sim_sell.base_out)), token_out)
            if depth_cov < ctx.engine.min_depth_coverage_pct:
                return False
            if Decimal(str(sim_sell.slippage_bps)) > ctx.engine.max_cex_slippage_bps:
                return False

            stable_out = Decimal(str(sim_sell.quote_out))
            if stable_out <= 0:
                return False

            jup_implied = notional / token_out
            if not price_ratio_ok(jup_implied, mid, ctx.engine.max_price_ratio):
                return False
            if not gross_cap_ok(stable_out, notional, ctx.engine.max_gross_profit_pct):
                return False

            profit = net_profit(stable_out, notional, required)
            return profit > 0

        if direction == "BYBIT->JUP":
            # Bybit -> stable estimate (CEX side)
            sim_buy = simulate_buy_with_notional(asks, notional)
            if sim_buy is None:
                return False

            depth_cov = coverage_pct(Decimal(str(sim_buy.quote_out)), notional)
            if depth_cov < ctx.engine.min_depth_coverage_pct:
                return False
            if Decimal(str(sim_buy.slippage_bps)) > ctx.engine.max_cex_slippage_bps:
                return False

            token_out2 = Decimal(str(sim_buy.base_out))
            if token_out2 <= 0:
                return False

            expected_raw = to_raw(token_out2, decimals)
            expected_raw_int = int(expected_raw)
            if expected_raw_int <= 0:
                return False

            # token -> stable quote (Jupiter)
            need_requote = False
            if (
                j_sell is None
                or sell_updated_ms <= 0
                or (now_ms - sell_updated_ms) > int(ctx.engine.max_quote_age_ms)
                or sell_amount_raw <= 0
                or j_sell.input_mint != mint
                or j_sell.output_mint != stable_mint
            ):
                need_requote = True
            elif Decimal(str(j_sell.price_impact_pct)) > ctx.engine.max_dex_price_impact_pct:
                need_requote = True
            else:
                ratio = Decimal(expected_raw_int) / Decimal(sell_amount_raw)
                if not (B_AMOUNT_RATIO_MIN <= ratio <= B_AMOUNT_RATIO_MAX):
                    need_requote = True

            if need_requote:
                j_sell = await ctx.jup.quote_exact_in(mint, stable_mint, expected_raw_int)

            if (
                j_sell is None
                or j_sell.input_mint != mint
                or j_sell.output_mint != stable_mint
                or Decimal(str(j_sell.price_impact_pct)) > ctx.engine.max_dex_price_impact_pct
            ):
                return False

            stable_out2 = from_raw(int(j_sell.out_amount_raw), stable_decimals)
            if stable_out2 <= 0:
                return False

            jup_implied2 = stable_out2 / token_out2
            if not price_ratio_ok(jup_implied2, mid, ctx.engine.max_price_ratio):
                return False
            if not gross_cap_ok(stable_out2, notional, ctx.engine.max_gross_profit_pct):
                return False

            profit2 = net_profit(stable_out2, notional, required)
            return profit2 > 0

        return False

    total = min(len(rows), max_rows)
    for idx, row in enumerate(rows[:max_rows], start=1):
        key = row.get("key")
        msg_id = row.get("message_id")
        if not key or msg_id is None:
            continue

        checked_count += 1
        text_hint = ctx.tg._last_sent_text.get(str(key)) or ctx.tg._last_text.get(str(key))
        try:
            profitable = await asyncio.wait_for(
                is_profitable_key(str(key), text_hint),
                timeout=CLEANUP_PROFIT_CHECK_TIMEOUT_SEC,
            )
        except asyncio.TimeoutError:
            log.warning(
                "cleanup: таймаут проверки прибыли key=%s (>%ss), сообщение оставляем",
                key,
                CLEANUP_PROFIT_CHECK_TIMEOUT_SEC,
            )
            profitable = True
        if not profitable:
            await ctx.tg.delete_message_for_key(str(key), int(msg_id))
            deleted_count += 1
        left_count = checked_count - deleted_count

        if progress_cb and not quiet:
            # Обновляем состояние после каждого проверенного сообщения
            await progress_cb(
                "🔄 Проверка прибыли (как движок)…\n"
                f"Сейчас: <code>{key}</code>\n"
                f"Проверено: {checked_count}/{total}\n"
                f"Удалено: {deleted_count}\n"
                f"Оставлено: {left_count}"
            )

    if not only_ttl_stale:
        if rows_override is not None:
            log.debug(
                "[TG_PROFIT] recheck batch: checked=%d deleted=%d",
                checked_count,
                deleted_count,
            )
        else:
            log.info(
                "[TG_PROFIT] recheck full: checked=%d deleted=%d",
                checked_count,
                deleted_count,
            )

    return deleted_count, checked_count


def make_tg_hourly_cleanup_loop(ctx: AppContext):
    """
    Фоновая перепроверка прибыли по открытым сигналам в Telegram (как движок).
    Round-robin: каждые TG_PROFIT_RECHECK_INTERVAL_SEC проверяется до TG_PROFIT_RECHECK_MAX_PER_TICK
    слотов — нагрузка ограничена, все слоты проходят очередь без «дыры» в час.
    """

    async def tg_hourly_cleanup_loop():
        rotation_offset = 0
        log.info(
            "[TG_PROFIT] recheck: старт, интервал=%.0fs, до %d слотов за тик, round-robin",
            TG_PROFIT_RECHECK_INTERVAL_SEC,
            TG_PROFIT_RECHECK_MAX_PER_TICK,
        )
        while True:
            sleep_sec = TG_PROFIT_RECHECK_INTERVAL_SEC
            try:
                rows = await ctx.tg.list_all_tracked_signal_slots_async()
                rows.sort(key=lambda r: str(r.get("key") or ""))
                n = len(rows)
                if n == 0:
                    sleep_sec = TG_PROFIT_RECHECK_IDLE_INTERVAL_SEC
                else:
                    batch, rotation_offset = _rotate_signal_batch(
                        rows,
                        rotation_offset,
                        TG_PROFIT_RECHECK_MAX_PER_TICK,
                    )
                    await cleanup_non_profitable_msgs_profit_based_once(
                        ctx,
                        max_rows=len(batch),
                        only_ttl_stale=False,
                        rows_override=batch,
                        quiet=True,
                    )
            except Exception as e:
                log.warning("tg_profit_recheck_loop error: %s", e)
            await asyncio.sleep(sleep_sec)

    return tg_hourly_cleanup_loop


def make_auto_tune_loop(ctx: AppContext):
    tuner = AutoTuner(config=TunerConfig())
    apply_settings_reload = make_apply_settings_reload(ctx)

    async def auto_tune_loop():
        last_eval_ts = 0.0
        while True:
            await asyncio.sleep(60)
            if not ctx.settings.auto_tune_enabled or not ctx.settings.exchange_enabled:
                continue
            now = time.time()
            if (now - last_eval_ts) < ctx.auto_tune_interval_sec:
                continue
            last_eval_ts = now
            try:
                metrics = ctx.metrics_collector.get_window_stats()
                bounds = TunerBounds.from_dict(ctx.settings.auto_tune_bounds)
                changes = tuner.evaluate(metrics, ctx.settings, bounds)
                if not changes:
                    continue
                for c in changes:
                    if ctx.settings.update(c.param, c.new_value):
                        apply_settings_reload(ctx.settings)
                        save_runtime_settings(str(ctx.settings_path), ctx.settings)
                        entry = {
                            "ts": time.time(),
                            "source": "auto",
                            "param": c.param,
                            "old_value": c.old_value,
                            "new_value": c.new_value,
                            "reason": c.reason,
                        }
                        ctx.auto_tune_history.append(entry)
                        if len(ctx.auto_tune_history) > AUTO_TUNE_HISTORY_MAX:
                            ctx.auto_tune_history.pop(0)
                        log.info(
                            "[AUTO_TUNE] %s: %s -> %s (%s)",
                            c.param,
                            c.old_value,
                            c.new_value,
                            c.reason,
                        )
            except Exception as e:
                log.warning("Auto-tune loop error: %s", e, exc_info=True)

    return auto_tune_loop


def make_stats_heartbeat_loop(ctx: AppContext):
    async def stats_heartbeat_loop():
        while True:
            await asyncio.sleep(60)
            if not ctx.settings.exchange_enabled:
                continue
            try:
                from api.db import record_async

                await asyncio.gather(
                    record_async("jupiter", 1),
                    record_async("bybit", 1),
                )
            except Exception as e:
                log.warning("Stats heartbeat failed: %s", e)

    return stats_heartbeat_loop


def make_ws_health_loop(ctx: AppContext):
    AUTO_Q_TTL_SEC = 6 * 3600

    async def ws_health_loop():
        timeout_sec = float(ctx.cfg.runtime.ws_snapshot_timeout_sec)
        if timeout_sec <= 0:
            return
        start_ts = time.time()

        while True:
            await asyncio.sleep(max(5.0, timeout_sec / 2))
            if (time.time() - start_ts) < timeout_sec:
                continue

            now_ms = ctx.state.now_ms()
            symbols = list(ctx.cfg.bybit.symbols)
            stale_syms: list[str] = []
            for sym in symbols:
                ob = await ctx.state.get_orderbook(sym)
                last_msg_ms = 0
                if ob is not None:
                    last_msg_ms = int(ob.last_cts_ms or ob.last_update_ms or ob.last_snapshot_ms or 0)
                if last_msg_ms <= 0 or (now_ms - last_msg_ms) > timeout_sec * 1000:
                    stale_syms.append(sym)

            if stale_syms:
                for sym in stale_syms[:50]:
                    await ctx.q_manager.add(sym, "WS_STALE", AUTO_Q_TTL_SEC)
                log.warning(
                    "[HEALTH] stale snapshots %d/%d (>%.0fs) sample=%s",
                    len(stale_syms),
                    len(symbols),
                    timeout_sec,
                    ", ".join(stale_syms[:5]),
                )

    return ws_health_loop
