from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import yaml


@dataclass
class QuarantineEntry:
    reason: str
    until_ts: int  # unix seconds


def now_ts() -> int:
    return int(time.time())


def load_quarantine(path: str) -> Dict[str, QuarantineEntry]:
    """
    Reads quarantine YAML:
      symbols:
        BTCUSDT: { reason: "...", until: 1730000000 }
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
    except FileNotFoundError:
        return {}
    except Exception:
        return {}

    sym_map = (raw.get("symbols") or {}) if isinstance(raw, dict) else {}
    out: Dict[str, QuarantineEntry] = {}
    if not isinstance(sym_map, dict):
        return {}

    for sym, v in sym_map.items():
        if not isinstance(sym, str) or not isinstance(v, dict):
            continue
        reason = str(v.get("reason") or "").strip() or "unknown"
        until = v.get("until")
        try:
            until_ts = int(until)
        except Exception:
            continue
        out[sym] = QuarantineEntry(reason=reason, until_ts=until_ts)

    return out


def save_quarantine(path: str, q: Dict[str, QuarantineEntry]) -> None:
    payload = {
        "version": 1,
        "updated_at_ts": now_ts(),
        "symbols": {k: {"reason": v.reason, "until": int(v.until_ts)} for k, v in sorted(q.items())},
    }
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(payload, f, sort_keys=False, allow_unicode=True)


def prune_expired(q: Dict[str, QuarantineEntry], ts: Optional[int] = None) -> Dict[str, QuarantineEntry]:
    ts = now_ts() if ts is None else int(ts)
    return {sym: ent for sym, ent in q.items() if int(ent.until_ts) > ts}


def is_quarantined(q: Dict[str, QuarantineEntry], symbol: str, ts: Optional[int] = None) -> Tuple[bool, str]:
    ts = now_ts() if ts is None else int(ts)
    ent = q.get(symbol)
    if not ent:
        return False, ""
    if int(ent.until_ts) <= ts:
        return False, ""
    return True, ent.reason
