"""Tests for core.quarantine."""
from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from core.quarantine import QuarantineEntry, load_quarantine, prune_expired, save_quarantine


def test_prune_expired_removes_expired():
    now = 1000
    q = {
        "A": QuarantineEntry(reason="x", until_ts=999),
        "B": QuarantineEntry(reason="y", until_ts=1001),
        "C": QuarantineEntry(reason="z", until_ts=1000),
    }
    result = prune_expired(q, ts=now)
    assert result == {"B": QuarantineEntry(reason="y", until_ts=1001)}
    assert "A" not in result
    assert "C" not in result


def test_prune_expired_keeps_all_when_none_expired():
    now = 100
    q = {
        "A": QuarantineEntry(reason="x", until_ts=200),
        "B": QuarantineEntry(reason="y", until_ts=300),
    }
    result = prune_expired(q, ts=now)
    assert result == q


def test_prune_expired_empty():
    assert prune_expired({}, ts=100) == {}


def test_load_quarantine_missing_file():
    result = load_quarantine("/nonexistent/path/xyz.yaml")
    assert result == {}


def test_load_save_quarantine_roundtrip():
    with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
        path = f.name
    try:
        q = {
            "BTCUSDT": QuarantineEntry(reason="test", until_ts=1730000000),
            "ETHUSDT": QuarantineEntry(reason="other", until_ts=1730000100),
        }
        save_quarantine(path, q)
        loaded = load_quarantine(path)
        assert len(loaded) == 2
        assert loaded["BTCUSDT"].reason == "test"
        assert loaded["BTCUSDT"].until_ts == 1730000000
        assert loaded["ETHUSDT"].reason == "other"
    finally:
        Path(path).unlink(missing_ok=True)
