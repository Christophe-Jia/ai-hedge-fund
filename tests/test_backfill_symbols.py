"""Tests for the out-of-universe symbol backfill script.

The Nasdaq store is replaced by an in-memory fake so the tests exercise the
skip / fetch / failure bookkeeping without touching the network or the real DB.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "backfill_symbols.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("backfill_symbols", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


bs = _load_module()


class FakeStore:
    """Coverage table keyed by symbol; fetch returns a fixed row count."""

    def __init__(self, coverage=None, fail=(), writes=5):
        self._coverage = dict(coverage or {})
        self._fail = set(fail)
        self._writes = writes
        self.fetched: list[str] = []
        FakeStore.last = self

    def get_coverage(self, sym):
        first, last, n = self._coverage.get(sym, (None, None, 0))
        return first, last, n

    def fetch_and_store(self, sym, from_date=None, to_date=None):
        if sym in self._fail:
            raise RuntimeError(f"boom {sym}")
        self.fetched.append(sym)
        first, last = "2016-09-19", "2026-09-18"
        self._coverage[sym] = (first, last, self._writes)
        return self._writes


def _run(monkeypatch, store, tmp_path, symbols="MRVL"):
    monkeypatch.setattr(bs, "NasdaqDailyStore", lambda **kw: store)
    monkeypatch.setattr(sys, "argv", [
        "backfill_symbols.py", "--symbols", symbols,
        "--politeness", "0", "--report", str(tmp_path / "status.json"),
    ])
    bs.main()


def test_already_covered_symbol_is_skipped_without_fetching(monkeypatch, tmp_path):
    store = FakeStore(coverage={"MRVL": ("2016-09-19", "2026-09-18", 2514)})
    _run(monkeypatch, store, tmp_path)
    assert store.fetched == []
    status = json.loads((tmp_path / "status.json").read_text())
    assert status["symbols"]["MRVL"]["status"] == "already_covered"
    assert status["symbols"]["MRVL"]["rows"] == 2514


def test_under_covered_symbol_is_fetched_and_recorded(monkeypatch, tmp_path):
    store = FakeStore(coverage={"MRVL": (None, None, 0)}, writes=2514)
    _run(monkeypatch, store, tmp_path)
    assert store.fetched == ["MRVL"]
    status = json.loads((tmp_path / "status.json").read_text())
    entry = status["symbols"]["MRVL"]
    assert entry["status"] == "ok"
    assert entry["rows_written"] == 2514
    assert entry["rows"] == 2514
    assert entry["first"] == "2016-09-19"


def test_force_refetches_a_covered_symbol(monkeypatch, tmp_path):
    store = FakeStore(coverage={"MRVL": ("2016-09-19", "2026-09-18", 2514)})
    monkeypatch.setattr(bs, "NasdaqDailyStore", lambda **kw: store)
    monkeypatch.setattr(sys, "argv", [
        "backfill_symbols.py", "--symbols", "MRVL", "--force",
        "--politeness", "0", "--report", str(tmp_path / "status.json"),
    ])
    bs.main()
    assert store.fetched == ["MRVL"]


def test_failure_is_recorded_and_exit_is_nonzero(monkeypatch, tmp_path):
    store = FakeStore(coverage={"MRVL": (None, None, 0)}, fail={"MRVL"})
    with pytest.raises(SystemExit) as exc:
        _run(monkeypatch, store, tmp_path)
    assert exc.value.code == 1
    entry = json.loads((tmp_path / "status.json").read_text())["symbols"]["MRVL"]
    assert entry["status"] == "failed"
    assert "boom" in entry["error"]


def test_multiple_symbols_are_processed_independently(monkeypatch, tmp_path):
    store = FakeStore(coverage={"MU": ("2016-09-12", "2026-09-10", 2513)},
                      writes=2510)
    _run(monkeypatch, store, tmp_path, symbols="MU,MRVL")
    assert store.fetched == ["MRVL"]
    status = json.loads((tmp_path / "status.json").read_text())["symbols"]
    assert status["MU"]["status"] == "already_covered"
    assert status["MRVL"]["status"] == "ok"
