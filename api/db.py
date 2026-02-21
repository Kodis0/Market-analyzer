"""
SQLite хранилище статистики запросов к Jupiter и Bybit.
Оптимизировано: батчинг записей, WAL mode — снижает нагрузку на диск.
"""
from __future__ import annotations

import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

DB_PATH: Optional[Path] = None
_conn: Optional[sqlite3.Connection] = None
_buffer: dict[tuple[int, str], int] = {}
_buffer_lock = threading.Lock()
_flush_interval_sec = 10.0  # сброс буфера в БД (сек)
_last_flush = 0.0

SCHEMA = """
CREATE TABLE IF NOT EXISTS request_stats (
    ts_bucket INTEGER NOT NULL,
    source TEXT NOT NULL,
    count INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (ts_bucket, source)
);

CREATE INDEX IF NOT EXISTS idx_request_stats_ts ON request_stats(ts_bucket);

CREATE TABLE IF NOT EXISTS signal_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    token TEXT NOT NULL,
    direction TEXT NOT NULL,
    profit_usd REAL NOT NULL,
    notional_usd REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_signal_history_ts ON signal_history(ts);
"""


def init(db_path: Path) -> None:
    global DB_PATH, _conn
    DB_PATH = Path(db_path)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    _conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    _conn.executescript(SCHEMA)
    try:
        _conn.execute("ALTER TABLE signal_history ADD COLUMN status TEXT DEFAULT 'active'")
        _conn.commit()
    except sqlite3.OperationalError:
        pass
    _conn.execute("PRAGMA journal_mode=WAL")
    _conn.execute("PRAGMA synchronous=NORMAL")
    _conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
    _conn.commit()


def _flush() -> None:
    """Сбросить буфер в БД."""
    global _buffer, _last_flush
    if _conn is None:
        return
    with _buffer_lock:
        if not _buffer:
            _last_flush = time.monotonic()
            return
        to_write = dict(_buffer)
        _buffer.clear()
    try:
        with _conn:
            for (ts_bucket, source), count in to_write.items():
                _conn.execute(
                    """
                    INSERT INTO request_stats (ts_bucket, source, count)
                    VALUES (?, ?, ?)
                    ON CONFLICT(ts_bucket, source) DO UPDATE SET count = count + excluded.count
                    """,
                    (ts_bucket, source, count),
                )
        _last_flush = time.monotonic()
    except Exception as e:
        log.warning("request_stats flush failed: %s", e)
        # Вернуть в буфер при ошибке
        with _buffer_lock:
            for k, v in to_write.items():
                _buffer[k] = _buffer.get(k, 0) + v


def record(source: str, count: int = 1) -> None:
    """Записать запросы. source: 'jupiter' | 'bybit'. Батчинг — сброс каждые 5 сек."""
    if _conn is None:
        return
    ts = int(time.time() // 60) * 60
    with _buffer_lock:
        key = (ts, source)
        _buffer[key] = _buffer.get(key, 0) + count
    # Периодический сброс
    now = time.monotonic()
    if now - _last_flush >= _flush_interval_sec:
        _flush()


def get_stats(period: str) -> list[dict]:
    """
    period: '1h' | '1d' | '1w' | 'all'
    Возвращает [{ts, jupiter, bybit}, ...] отсортировано по ts.
    """
    if _conn is None:
        return []
    _flush()  # Сбросить буфер перед чтением
    now = int(time.time())
    if period == "1h":
        since = now - 3600
        bucket_sec = 60
    elif period == "1d":
        since = now - 86400
        bucket_sec = 3600
    elif period == "1w":
        since = now - 7 * 86400
        bucket_sec = 86400
    else:  # all
        since = now - 30 * 86400  # макс 30 дней
        bucket_sec = 86400

    try:
        cur = _conn.execute(
            """
            SELECT ts_bucket, source, count
            FROM request_stats
            WHERE ts_bucket >= ?
            ORDER BY ts_bucket
            """,
            (since,),
        )
        rows = cur.fetchall()
    except Exception as e:
        log.warning("request_stats get_stats failed: %s", e)
        return []

    # Агрегируем по bucket
    buckets: dict[int, dict[str, int]] = {}
    for ts_bucket, source, count in rows:
        if bucket_sec > 60:
            key = (ts_bucket // bucket_sec) * bucket_sec
        else:
            key = ts_bucket
        if key not in buckets:
            buckets[key] = {"ts": key, "jupiter": 0, "bybit": 0}
        if source in ("jupiter", "bybit"):
            buckets[key][source] = buckets[key].get(source, 0) + count

    result = sorted(buckets.values(), key=lambda x: x["ts"])
    return result


def record_signal(token: str, direction: str, profit_usd: float, notional_usd: float) -> None:
    """Записать сигнал в историю. Пропускает дубликаты (тот же token+direction+profit в последние 60 сек)."""
    if _conn is None:
        return
    ts = int(time.time())
    profit_rounded = round(float(profit_usd), 2)
    try:
        with _conn:
            cur = _conn.execute(
                """
                SELECT 1 FROM signal_history
                WHERE token = ? AND direction = ?
                AND profit_usd BETWEEN ? AND ?
                AND ts >= ?
                LIMIT 1
                """,
                (token, direction, profit_rounded - 0.005, profit_rounded + 0.005, ts - 60),
            )
            if cur.fetchone():
                return
            _conn.execute(
                """
                INSERT INTO signal_history (ts, token, direction, profit_usd, notional_usd)
                VALUES (?, ?, ?, ?, ?)
                """,
                (ts, token, direction, profit_rounded, float(notional_usd)),
            )
    except Exception as e:
        log.warning("signal_history record failed: %s", e)


STALE_AGE_SEC = 900  # 15 мин — сигнал считается устаревшим по возрасту


def get_signal_history(period: str, limit: int = 200) -> list[dict]:
    """
    period: '1h' | '1d' | '1w' | 'all'
    Возвращает [{id, ts, token, direction, profit_usd, notional_usd, status, is_stale}, ...].
    is_stale = True если status='stale' или возраст > STALE_AGE_SEC.
    """
    if _conn is None:
        return []
    _flush()
    now = int(time.time())
    if period == "1h":
        since = now - 3600
    elif period == "1d":
        since = now - 86400
    elif period == "1w":
        since = now - 7 * 86400
    else:
        since = 0

    try:
        cur = _conn.execute(
            """
            SELECT id, ts, token, direction, profit_usd, notional_usd,
                   COALESCE(status, 'active') as status
            FROM signal_history
            WHERE ts >= ?
            ORDER BY ts DESC
            LIMIT ?
            """,
            (since, limit),
        )
        rows = cur.fetchall()
    except Exception as e:
        log.warning("signal_history get failed: %s", e)
        return []

    result = []
    for r in rows:
        sid, ts, token, direction, profit_usd, notional_usd, status = r
        age_sec = now - ts
        is_stale = status == "stale" or age_sec > STALE_AGE_SEC
        result.append({
            "id": sid,
            "ts": ts,
            "token": token,
            "direction": direction,
            "profit_usd": profit_usd,
            "notional_usd": notional_usd,
            "status": status,
            "is_stale": is_stale,
        })
    return result


def update_signal_status(signal_id: int, status: str) -> bool:
    """Обновить статус сигнала. status: 'active' | 'stale'. Возвращает True если обновлено."""
    if _conn is None:
        return False
    if status not in ("active", "stale"):
        return False
    try:
        cur = _conn.execute(
            "UPDATE signal_history SET status = ? WHERE id = ?",
            (status, signal_id),
        )
        _conn.commit()
        return cur.rowcount > 0
    except Exception as e:
        log.warning("signal_history update failed: %s", e)
        return False


def delete_signal(signal_id: int) -> bool:
    """Удалить сигнал по id. Возвращает True если удалено."""
    if _conn is None:
        return False
    try:
        cur = _conn.execute("DELETE FROM signal_history WHERE id = ?", (signal_id,))
        _conn.commit()
        return cur.rowcount > 0
    except Exception as e:
        log.warning("signal_history delete failed: %s", e)
        return False
