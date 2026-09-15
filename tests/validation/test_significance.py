"""Unit tests for the significance check (synthetic data, known answers)."""

from __future__ import annotations

import numpy as np
import pytest

from src.validation import significance, significance_from_stats


def _series_with(mean: float, std: float, n: int, seed: int = 0) -> np.ndarray:
    """Array of length n whose sample mean/std match exactly (ddof=1)."""
    rng = np.random.default_rng(seed)
    raw = rng.normal(size=n)
    return (raw - raw.mean()) / raw.std(ddof=1) * std + mean


def test_gbm_calibration_is_noise():
    """The exact GBM numbers: mean 0.0082, std ~0.08, n 71 -> NOISE."""
    res = significance(_series_with(0.0082, 0.08, 71))
    assert res["verdict"] == "NOISE"
    assert res["significant"] is False
    assert res["n"] == 71
    # t ~ 0.84, standard error ~ 0.0095 (the number nobody cross-checked)
    assert res["t_stat"] < 1.0
    assert abs(res["se"] - 0.0095) < 0.0005


def test_from_stats_gbm_numbers_directly():
    res = significance_from_stats(0.0082, 0.08, 71)
    assert res["verdict"] == "NOISE"
    assert abs(res["se"] - 0.009493) < 1e-5
    assert abs(res["t_stat"] - 0.8638) < 0.01
    assert res["ci_low"] < 0 < res["ci_high"]


def test_p_values_are_reported():
    res = significance_from_stats(0.0082, 0.08, 71)
    assert res["df"] == 70
    assert res["p_value"] == pytest.approx(0.391, abs=0.01)  # two-sided Student-t
    assert res["p_value_normal"] == pytest.approx(0.388, abs=0.01)
    # t distribution has fatter tails -> larger p at the same t
    assert res["p_value"] > res["p_value_normal"]


def test_p_value_zero_for_noiseless_series():
    res = significance_from_stats(0.03, 0.0, 10)
    assert res["p_value"] == 0.0


def test_strong_positive_is_pass():
    res = significance(_series_with(0.05, 0.08, 71))
    assert res["verdict"] == "PASS"
    assert res["significant"] is True
    assert res["direction"] == "positive"
    assert res["ci_low"] > 0


def test_strong_negative_is_fail():
    res = significance(_series_with(-0.05, 0.08, 71))
    assert res["verdict"] == "FAIL"
    assert res["significant"] is True
    assert res["direction"] == "negative"
    assert res["ci_high"] < 0


def test_ci_shrinks_with_sample_size():
    small = significance(_series_with(0.03, 0.08, 12))
    large = significance(_series_with(0.03, 0.08, 400))
    assert (large["ci_high"] - large["ci_low"]) < (small["ci_high"] - small["ci_low"])
    assert large["verdict"] == "PASS"
    assert small["verdict"] == "NOISE"


def test_insufficient_samples():
    assert significance([])["verdict"] == "INSUFFICIENT"
    assert significance([0.01])["verdict"] == "INSUFFICIENT"
    assert significance_from_stats(0.01, None, 10)["verdict"] == "INSUFFICIENT"


def test_nan_is_dropped():
    arr = _series_with(0.05, 0.08, 40).tolist() + [float("nan"), float("nan")]
    res = significance(arr)
    assert res["n"] == 40
    assert res["verdict"] == "PASS"


def test_zero_variance_nonzero_mean_is_significant():
    res = significance_from_stats(0.03, 0.0, 10)
    assert res["verdict"] == "PASS"
    assert res["t_stat"] is None  # infinite t is not representable


def test_zero_variance_zero_mean_is_noise():
    res = significance_from_stats(0.0, 0.0, 10)
    assert res["verdict"] == "NOISE"
    assert res["t_stat"] == 0.0


def test_threshold_is_configurable():
    arr = _series_with(0.02, 0.08, 40)  # t ~ 1.58
    assert significance(arr)["verdict"] == "NOISE"
    assert significance(arr, t_threshold=1.5)["verdict"] == "PASS"


def test_kind_events_routes_to_event_significance():
    returns = [1.0, 1.1, 0.9, 1.2, 1.05, 0.95, 1.15, 1.0]
    res = significance(returns, kind="events")
    assert res["verdict"] == "PASS"
    assert res["n_events"] == 8
    assert "win_rate_wilson_low" in res  # event-level extras are present
    assert "bootstrap_mean_low" in res


def test_kind_period_is_the_default():
    res = significance(_series_with(0.05, 0.08, 40))
    assert "n" in res and "n_events" not in res


def test_unknown_kind_raises():
    with pytest.raises(ValueError):
        significance([0.1, 0.2], kind="nonsense")
