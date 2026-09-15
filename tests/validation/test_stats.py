"""Unit tests for the small-sample statistics helpers."""

from __future__ import annotations

import math

import numpy as np
import pytest

from src.validation import (
    bootstrap_ci_mean,
    fdr_bh,
    proportion_z,
    two_sided_t_p,
    wilson_interval,
    z_two_sided,
)
from src.validation.stats import betainc_reg, normal_two_sided_p, student_t_sf


def test_z_two_sided_matches_known_critical_value():
    assert z_two_sided(0.05) == pytest.approx(1.959964, abs=1e-5)
    assert z_two_sided(0.10) == pytest.approx(1.644854, abs=1e-5)


def test_normal_two_sided_p_reference():
    assert normal_two_sided_p(1.959964) == pytest.approx(0.05, abs=1e-5)
    assert normal_two_sided_p(0.0) == pytest.approx(1.0)


@pytest.mark.parametrize(
    "t, df, expected",
    [
        (2.0, 20, 0.0593),
        (1.837, 37, 0.0744),
        (1.0, 10, 0.3409),
        (2.5, 99, 0.0140),
        (0.0, 10, 1.0),
    ],
)
def test_two_sided_t_p_reference_values(t, df, expected):
    assert two_sided_t_p(t, df) == pytest.approx(expected, abs=5e-4)


def test_t_distribution_has_fatter_tails_than_normal():
    """Exactly why event strategies (n=13-25) must not use the normal approximation."""
    for t in (1.5, 2.0, 3.0):
        assert two_sided_t_p(t, 20) > normal_two_sided_p(t)


def test_student_t_sf_symmetry_and_edges():
    assert student_t_sf(0.0, 10) == pytest.approx(0.5)
    assert student_t_sf(-2.0, 20) + student_t_sf(2.0, 20) == pytest.approx(1.0)
    assert math.isnan(student_t_sf(1.0, 0))


def test_betainc_reg_edges():
    assert betainc_reg(1.0, 1.0, 0.5) == pytest.approx(0.5)
    assert betainc_reg(1.0, 1.0, 0.0) == 0.0
    assert betainc_reg(1.0, 1.0, 1.0) == 1.0


def test_wilson_interval_known_value():
    """13 wins out of 19 (the weekend_gap high-volume bucket) -> CI around 0.46-0.85."""
    lo, hi = wilson_interval(13, 19)
    assert lo == pytest.approx(0.460, abs=0.01)
    assert hi == pytest.approx(0.846, abs=0.01)
    assert lo < 0.5 < hi  # cannot reject a coin flip


def test_wilson_interval_never_leaves_unit_range():
    assert wilson_interval(0, 10)[0] == 0.0
    assert wilson_interval(0, 10)[1] == pytest.approx(0.2775, abs=0.002)
    assert wilson_interval(10, 10)[1] == 1.0
    lo, hi = wilson_interval(10, 10)
    assert lo == pytest.approx(0.7225, abs=0.002)


def test_wilson_interval_empty():
    lo, hi = wilson_interval(0, 0)
    assert math.isnan(lo) and math.isnan(hi)


def test_proportion_z_reference():
    assert proportion_z(16, 25) == pytest.approx(1.4)
    assert proportion_z(25, 25) > 0
    assert math.isnan(proportion_z(1, 0))


def test_bootstrap_ci_mean_deterministic_and_contains_mean():
    sample = [1.0, 2.0, 3.0, 4.0, 5.0]
    a = bootstrap_ci_mean(sample, n_resamples=2000, seed=1)
    b = bootstrap_ci_mean(sample, n_resamples=2000, seed=1)
    assert a["low"] == b["low"] and a["high"] == b["high"]
    assert a["low"] <= 3.0 <= a["high"]
    assert a["p_one_sided_leq_zero"] == 0.0


def test_bootstrap_ci_excludes_zero_for_strictly_positive_sample():
    res = bootstrap_ci_mean([1.0, 2.0, 3.0], n_resamples=1000, seed=3)
    assert res["low"] > 0.0


def test_bootstrap_ci_custom_statistic():
    res = bootstrap_ci_mean([1.0, 2.0, 3.0, 4.0], n_resamples=500, seed=0, statistic=lambda row: float(np.median(row)))
    assert res["low"] <= res["high"]


def test_bootstrap_ci_empty():
    res = bootstrap_ci_mean([float("nan")], n_resamples=10)
    assert res["n"] == 0 and res["low"] is None


def test_fdr_bh_reference():
    res = fdr_bh([0.001, 0.02, 0.5], alpha=0.05)
    assert res["n"] == 3
    assert res["adjusted"] == pytest.approx([0.003, 0.03, 0.5])
    assert res["n_rejected"] == 2


def test_fdr_bh_empty_and_all_significant():
    assert fdr_bh([])["n"] == 0
    assert fdr_bh([0.001, 0.002])["n_rejected"] == 2
