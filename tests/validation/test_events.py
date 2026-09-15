"""Unit tests for event-level significance and time segmentation."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from src.validation import event_significance, event_window_stats
from src.validation.stats import normal_two_sided_p

REPORTS = Path(__file__).resolve().parents[2] / "reports"
EXIT_RULES = REPORTS / "exit_rules_backtest.json"


def test_positive_event_set_is_significant():
    returns = [1.0, 1.1, 0.9, 1.2, 1.05, 0.95, 1.15, 1.0]
    res = event_significance(returns, n_resamples=2000)
    assert res["verdict"] == "PASS"
    assert res["n_events"] == 8
    assert res["win_rate"] == 1.0
    assert res["bootstrap_mean_low"] > 0.0
    assert res["bootstrap_ci_excludes_zero"] is True
    assert res["p_value"] < 0.05


def test_mixed_event_set_is_noise():
    returns = [2.0, -1.0, 3.0, 1.0, -2.0]
    res = event_significance(returns, n_resamples=2000)
    assert res["verdict"] == "NOISE"
    assert res["n_events"] == 5
    assert res["win_rate"] == pytest.approx(0.6)
    assert res["win_rate_wilson_low"] < 0.5 < res["win_rate_wilson_high"]
    assert res["win_rate_beats_chance"] is False
    assert res["small_sample"] is True


def test_negative_event_set_fails():
    returns = [-1.0, -1.2, -0.8, -1.5, -0.9]
    res = event_significance(returns, n_resamples=1000)
    assert res["verdict"] == "FAIL"
    assert res["bootstrap_mean_high"] < 0.0


def test_single_event_is_insufficient_but_reports_win_rate():
    res = event_significance([1.5])
    assert res["verdict"] == "INSUFFICIENT"
    assert res["n_events"] == 1
    assert res["win_rate"] == 1.0
    assert res["t_stat"] is None


def test_empty_events_is_insufficient():
    res = event_significance([])
    assert res["verdict"] == "INSUFFICIENT"
    assert res["n_events"] == 0


def test_nan_events_are_dropped():
    res = event_significance([1.0, 2.0, float("nan"), 3.0], n_resamples=500)
    assert res["n_events"] == 3


def test_bootstrap_is_deterministic():
    r = [1.0, -2.0, 3.0, 0.5, -1.0]
    a = event_significance(r, n_resamples=1000, seed=11)
    b = event_significance(r, n_resamples=1000, seed=11)
    assert a["bootstrap_mean_low"] == b["bootstrap_mean_low"]


def test_win_threshold_changes_win_rate():
    returns = [0.1, 0.2, 0.3, 5.0]
    assert event_significance(returns, win_threshold=0.0)["win_rate"] == 1.0
    assert event_significance(returns, win_threshold=1.0)["win_rate"] == pytest.approx(0.25)


def test_p_value_uses_student_t_not_normal():
    returns = [1.0, -0.5, 1.5, -1.0, 0.5, -0.2, 0.8, -0.6, 1.2, -0.9]
    res = event_significance(returns, n_resamples=500)
    assert res["df"] == 9
    # heavier tails than the normal approximation at n=10
    assert res["p_value"] > normal_two_sided_p(res["t_stat"])


# --- time segmentation -----------------------------------------------------


def _events(rows):
    return [{"date": d, "return_pct": r} for d, r in rows]


def test_event_window_stats_splits_by_period():
    events = _events(
        [
            ("2021-06-01", 2.0),
            ("2022-06-01", 1.0),
            ("2023-06-01", 3.0),
            ("2024-06-03", -2.0),
            ("2025-06-02", -1.0),
            ("2026-06-01", -1.5),
        ]
    )
    res = event_window_stats(events, {"2021-23": ("2021-01-01", "2023-12-31"), "2024-26": ("2024-01-01", "2026-12-31")})
    rows = {r["window"]: r for r in res["windows"]}
    assert rows["2021-23"]["n_events"] == 3
    assert rows["2021-23"]["mean"] == pytest.approx(2.0)
    assert rows["2024-26"]["n_events"] == 3
    assert rows["2024-26"]["mean"] < 0
    assert res["n_events_total"] == 6
    assert res["window_sign_consistency"] == 0.5
    assert res["verdict"] == "UNSTABLE"


def test_event_window_stats_consistent_windows_are_stable():
    events = _events([("2021-06-01", 2.0), ("2022-06-01", 1.0), ("2024-06-03", 3.0)])
    res = event_window_stats(events, {"a": ("2021-01-01", "2022-12-31"), "b": ("2024-01-01", "2024-12-31")})
    assert res["window_sign_consistency"] == 1.0
    assert res["verdict"] == "STABLE"


def test_event_window_stats_empty_window_is_reported():
    events = _events([("2021-06-01", 2.0)])
    res = event_window_stats(events, {"a": ("2021-01-01", "2021-12-31"), "b": ("2024-01-01", "2024-12-31")})
    rows = {r["window"]: r for r in res["windows"]}
    assert rows["b"]["n_events"] == 0
    assert rows["b"]["verdict"] == "INSUFFICIENT"
    assert res["n_windows_with_events"] == 1


def test_event_window_stats_accepts_dataframe_and_list_triples():
    df = pd.DataFrame({"entry_date": ["2021-06-01", "2024-06-03"], "ret_pct": [2.0, -1.0]})
    res = event_window_stats(df, [("a", "2021-01-01", "2021-12-31"), ("b", "2024-01-01", "2024-12-31")], date_col="entry_date", ret_col="ret_pct")
    assert {r["window"] for r in res["windows"]} == {"a", "b"}
    assert res["n_events_total"] == 2


def test_event_window_stats_bad_input():
    res = event_window_stats([], {"a": ("2021-01-01", "2021-12-31")})
    assert res["verdict"] == "INSUFFICIENT"


# --- calibration on the real weekend_gap trades ----------------------------


@pytest.mark.skipif(not EXIT_RULES.exists(), reason="exit_rules report not present")
def test_real_weekend_gap_trades_are_noise_at_event_level():
    rep = json.loads(EXIT_RULES.read_text())
    trades = rep["trades_full_window"]["long_short"]["t_plus_1"]
    returns = [t["ret_pct"] for t in trades]
    res = event_significance(returns, n_resamples=4000)
    assert res["n_events"] == 25
    assert res["win_rate"] == pytest.approx(0.64, abs=0.01)  # matches the report's 64.0%
    assert res["mean"] == pytest.approx(1.02, abs=0.05)  # matches avg_event_ret_pct
    assert res["verdict"] == "NOISE"  # 25 trades is not enough for a t>2
    assert res["small_sample"] is True


@pytest.mark.skipif(not EXIT_RULES.exists(), reason="exit_rules report not present")
def test_real_weekend_gap_event_windows():
    rep = json.loads(EXIT_RULES.read_text())
    trades = rep["trades_full_window"]["long_short"]["t_plus_1"]
    events = [{"date": t["entry_date"], "return_pct": t["ret_pct"]} for t in trades]
    res = event_window_stats(
        events,
        {"2021-23": ("2021-01-01", "2023-12-31"), "2024-26": ("2024-01-01", "2026-12-31")},
        n_resamples=2000,
    )
    assert res["n_events_total"] == 25
    rows = {r["window"]: r for r in res["windows"]}
    assert rows["2021-23"]["n_events"] + rows["2024-26"]["n_events"] == 25
    assert res["verdict"] in {"STABLE", "UNSTABLE"}
