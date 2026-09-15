"""Unit tests for the IC / Sharpe internal-consistency check."""

from __future__ import annotations

import pytest

from src.validation import internal_consistency


def test_gbm_crisis_is_implausible():
    """IC 0.0082 with Sharpe 0.963 over 71 months -> ratio ~14 -> IMPLAUSIBLE."""
    res = internal_consistency(0.0082, 0.963, 0.0, 71)
    assert res["verdict"] == "IMPLAUSIBLE"
    assert res["expected_ir"] == pytest.approx(0.0691, abs=0.001)
    assert res["ratio"] > 3.0
    # required IC to justify the Sharpe: ~14x the observed IC
    assert res["required_ic"] == pytest.approx(0.1143, abs=0.002)


def test_benchmark_adjusted_still_flags_gbm():
    """Against QQQ (Sharpe 0.695) the gap narrows but stays implausible."""
    res = internal_consistency(0.0082, 0.963, 0.0, 71, benchmark_sharpe=0.695)
    assert res["active_sharpe"] == pytest.approx(0.268, abs=0.001)
    assert res["verdict"] == "IMPLAUSIBLE"
    assert res["ratio"] > 3.0


def test_plausible_case_is_consistent():
    # IC 0.05 over 64 months implies IR 0.40; Sharpe 0.40 exactly matches
    res = internal_consistency(0.05, 0.40, 0.0, 64)
    assert res["verdict"] == "CONSISTENT"
    assert res["ratio"] == pytest.approx(1.0)


def test_slightly_lucky_case_is_consistent():
    # IC 0.04 over 144 months -> IR 0.48; Sharpe 1.0 is 2.08x -> within tolerance
    res = internal_consistency(0.04, 1.0, 0.0, 144)
    assert res["verdict"] == "CONSISTENT"
    assert res["ratio"] < 3.0


def test_benchmark_can_explain_the_whole_sharpe():
    res = internal_consistency(0.01, 0.7, 0.0, 36, benchmark_sharpe=0.7)
    assert res["active_sharpe"] == 0.0
    assert res["verdict"] == "CONSISTENT"


def test_positive_sharpe_without_positive_ic_is_implausible():
    res = internal_consistency(-0.02, 0.5, 0.0, 36)
    assert res["verdict"] == "IMPLAUSIBLE"
    assert res["ratio"] is None


def test_negative_ic_and_negative_sharpe_is_consistent():
    res = internal_consistency(-0.02, -0.5, 0.0, 36)
    assert res["verdict"] == "CONSISTENT"


def test_insufficient_periods():
    assert internal_consistency(0.05, 0.5, 0.0, 1)["verdict"] == "INSUFFICIENT"


def test_high_turnover_note():
    res = internal_consistency(0.05, 0.4, 6.0, 64)
    assert any("high turnover" in n for n in res["notes"])
    assert res["turnover"] == 6.0


def test_threshold_is_configurable():
    res = internal_consistency(0.0082, 0.963, 0.0, 71, max_ratio=20.0)
    assert res["verdict"] == "CONSISTENT"
