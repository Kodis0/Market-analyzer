"""
SQLite хранилище статистики запросов к Jupiter и Bybit.
"""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Optional

log = None  # set by app

DB_PATH: Optional[Path] = None
_conn: Optional[sqlite3.Connection] = None

SCHEMA = """
CREATE TABLE IF NOT EXISTS request_stats (
    ts_bucket INTEGER NOT NULL,
    source TEXT NOT NULL,
    count INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (ts_bucket, source)
);

CREATE INDEX IF NOT EXISTS idx_request_stats_ts ON request_stats(ts_bucket);
"""


def init(db_path: Path) -> None:
    global DB_PATH, _conn
    DB_PATH = Path(db_path)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    _conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    _conn.executescript(SCHEMA)
    _conn.commit()


def record(source: str, count: int = 1) -> None:
    """Записать запросы. source: 'jupiter' | 'bybit'."""
    if _conn is None:
        return
    ts = int(time.time() // 60) * 60  # округление до минуты
    try:
        _conn.execute(
            """
            INSERT INTO request_stats (ts_bucket, source, count)
            VALUES (?, ?, ?)
            ON CONFLICT(ts_bucket, source) DO UPDATE SET count = count + excluded.count
            """,
            (ts, source, count),
        )
        _conn.commit()
    except Exception as e:
        if log:
            log.warning("request_stats record failed: %s", e)


def get_stats(period: str) -> list[dict]:
    """
    period: '1h' | '1d' | '1w' | 'all'
    Возвращает [{ts, jupiter, bybit}, ...] отсортировано по ts.
    """
    if _conn is None:
        return []

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
        if log:
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
