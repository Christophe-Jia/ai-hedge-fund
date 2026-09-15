"""Calibration regression: the framework MUST flag the GBM credibility crisis.

If these tests ever pass while the reported numbers stay the same, the framework
is not doing its job.  They read the frozen reports under reports/ (read-only).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.validation import boundary_stability, internal_consistency, multi_window, red_team_checklist, significance_from_stats

REPORTS = Path(__file__).resolve().parents[2] / "reports"
SP100 = REPORTS / "xsec_gbm_results.json"
SP500 = REPORTS / "xsec_gbm_sp500.json"
ATTR = REPORTS / "gbm_attribution.json"
PICKS = REPORTS / "gbm_picks" / "2026-09.json"

pytestmark = pytest.mark.skipif(not SP100.exists(), reason="frozen GBM reports not present")


def _load(path: Path) -> dict:
    return json.loads(path.read_text())


def test_internal_consistency_flags_the_flagship_report():
    rep = _load(SP100)
    ic = rep["monthly_ic"]["mean"]
    sharpe = rep["comparison_full_test_window"]["gbm_top10"]["sharpe"]
    benchmark = rep["comparison_full_test_window"]["qqq_buy_hold"]["sharpe"]
    n_months = rep["walk_forward"]["n_test_months"]

    res = internal_consistency(ic, sharpe, 0.0, n_months, benchmark_sharpe=benchmark)
    assert res["verdict"] == "IMPLAUSIBLE"
    assert res["ratio"] > 3.0
    # raw (unaligned to benchmark) the gap is an order of magnitude
    raw = internal_consistency(ic, sharpe, 0.0, n_months)
    assert raw["ratio"] > 10.0


def test_monthly_excess_significance_is_noise():
    """gbm_attribution stores mean/std/t of pre-2024 monthly excess: t=1.84 < 2."""
    attr = _load(ATTR)
    boot = attr["matrix6_bootstrap_noise"]["pre_2024_monthly_excess"]
    res = significance_from_stats(boot["mean_pct"], boot["std_pct"], boot["n_months"])
    assert res["verdict"] == "NOISE"
    assert abs(res["t_stat"] - boot["t_stat"]) < 0.05
    assert res["ci_low"] < 0 < res["ci_high"]


def test_ic_across_years_flips_sign():
    """Yearly mean IC changes sign 3 times in 7 years -> UNSTABLE."""
    attr = _load(ATTR)
    by_year = attr["matrix2_ic_drift"]["by_year"]
    series = {y: v["mean_ic"] for y, v in by_year.items()}
    res = multi_window(lambda y: {"ic": series[y]}, sorted(series), metric="ic")
    assert res["verdict"] == "UNSTABLE"
    assert res["sign_consistency"] < 0.8


def test_sp500_windows_flip_sign():
    rep = _load(SP500)
    windows = {
        "full": rep["comparison_full_test_window"]["gbm_top10"]["sharpe"],
        "since_2021": rep["comparison_since_2021"]["gbm_top10"]["sharpe"],
        "since_2024": rep["comparison_since_2024"]["gbm_top10"]["sharpe"],
    }
    res = multi_window(lambda w: {"sharpe": windows[w]}, list(windows), metric="sharpe")
    assert res["verdict"] == "UNSTABLE"
    assert res["n_negative"] == 1


def test_flagship_report_fails_the_red_team_checklist():
    rep = _load(SP100)
    res = red_team_checklist(rep)
    assert res["verdict"] == "FAIL"
    missing = set(res["high_severity_unanswered"])
    assert {"significance", "window_stability", "score_distribution", "baseline_significance"} <= missing


def test_shipped_picks_contain_exact_score_ties():
    """The artefact that caused the 2/10 MISMATCH is visible in the picks file."""
    picks = _load(PICKS)["picks"]
    scores = [p["score"] for p in picks]
    # UBER and ADBE shipped with byte-identical scores
    assert len(scores) - len(set(scores)) >= 1
    # feeding the shipped score vector into the boundary probe surfaces the tie
    import pandas as pd

    df = pd.DataFrame([scores], columns=[p["symbol"] for p in picks])
    res = boundary_stability(df, top_n=len(picks), n_perturb=5, drop_frac=0.0)
    assert res["has_exact_ties"] is True
    assert res["per_section"][0]["duplicates_in_top_n"] >= 1
