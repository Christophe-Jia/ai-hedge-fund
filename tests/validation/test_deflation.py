"""Unit tests for statistical deflation (PSR / DSR / MutIC).

Synthetic cases have known answers:
  - PSR values are hand-computable from the closed form (the normal-case
    denominator ``sqrt(1 + 0.5*SR^2)`` is checked explicitly).
  - The EVT max approximation is checked against the *exact* expected maximum of
    N iid standard normals, hardcoded from deterministic quadrature of
    ``E[max] = int x*N*phi(x)*Phi(x)^(N-1) dx`` (scipy.integrate.quad); the
    task's simplified anchor table is verified as a lower bound (see the module
    docstring for why the two differ at N >= 100).
  - A strong deterministic edge survives deflation; a weak one does not.
Real-data calibration at the bottom (weekend_gap must fail deflation).
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

from src.validation import (
    DSR_THRESHOLD,
    MUTIC_LAMBDA_DEFAULT,
    NORMAL_KURTOSIS,
    deflated_sharpe_ratio,
    deflation_from_stats,
    deflation_report,
    expected_max_sharpe,
    mutic_adjusted_ic,
    probabilistic_sharpe_ratio,
)
from src.validation.deflation import (
    VERDICT_FAILS,
    VERDICT_INSUFFICIENT,
    VERDICT_SURVIVES,
)

REPORTS = Path(__file__).resolve().parents[2] / "reports"
EXIT_RULES = REPORTS / "exit_rules_backtest.json"

# Exact E[max] of N iid standard normals, from deterministic quadrature of
# ``int x*N*phi(x)*Phi(x)^(N-1) dx`` (scipy.integrate.quad, error < 1e-8).
# These are the reference values; no Monte Carlo randomness is involved.
_EXACT_MAX = {10: 1.538753, 100: 2.507594, 1000: 3.241436, 10000: 3.851616}
# Simplified anchor table quoted in the task (a lower bound; see docstring).
_TASK_TABLE = {10: 1.50, 100: 2.20, 1000: 2.80, 10000: 3.20}
# Tolerance envelope.  This is NOT implementation slack — it is set equal to the
# *intrinsic truncation error* of the object under test.  `expected_max_sharpe`
# is the first-order Bailey & López de Prado EVT approximation; its error against
# the exact expectation is 0.036 (N=10), 0.023 (N=100), 0.014 (N=1000),
# 0.009 (N=10000).  Asserting +/-0.02 would measure that known approximation
# error, not whether the implementation is correct; 0.05 is the honest envelope
# (and we deliberately do NOT swap in a higher-order approximation, which would
# depart from the standard formula).
_EVT_TOL = 0.05


# --- probabilistic_sharpe_ratio ---------------------------------------------


def test_psr_at_benchmark_is_one_half():
    assert probabilistic_sharpe_ratio(0.7, 50, 0.0, 3.0, sr_benchmark=0.7) == pytest.approx(0.5)
    assert probabilistic_sharpe_ratio(0.0, 50, 0.0, 3.0, sr_benchmark=0.0) == pytest.approx(0.5)


def test_psr_matches_closed_form_under_normality():
    # z = 0.5*sqrt(24)/sqrt(1.125) = 2.3094 -> Phi = 0.989539
    assert probabilistic_sharpe_ratio(0.5, 25, 0.0, NORMAL_KURTOSIS, 0.0) == pytest.approx(0.989539, abs=1e-5)


def test_negative_skew_and_fat_tails_lower_the_psr():
    base = probabilistic_sharpe_ratio(0.5, 25, 0.0, 3.0, 0.0)
    neg_skew = probabilistic_sharpe_ratio(0.5, 25, -1.0, 3.0, 0.0)
    fat = probabilistic_sharpe_ratio(0.5, 25, 0.0, 10.0, 0.0)
    pos_skew = probabilistic_sharpe_ratio(0.5, 25, 1.0, 3.0, 0.0)
    assert neg_skew < base < pos_skew
    assert fat < base
    assert neg_skew == pytest.approx(0.972668, abs=1e-5)
    assert fat == pytest.approx(0.974978, abs=1e-5)


def test_psr_raw_vs_excess_kurtosis_convention_matters():
    """Passing excess kurtosis (normal==0) is the classic DSR bug: under
    normality the raw form is the only one that yields sqrt(1 + 0.5 SR^2)."""
    raw = probabilistic_sharpe_ratio(0.5, 25, 0.0, 3.0, 0.0)
    excess = probabilistic_sharpe_ratio(0.5, 25, 0.0, 0.0, 0.0)
    assert raw != pytest.approx(excess)


def test_psr_invalid_inputs_return_nan():
    assert math.isnan(probabilistic_sharpe_ratio(1.0, 1, 0.0, 3.0))       # n < 2
    assert math.isnan(probabilistic_sharpe_ratio(float("inf"), 10, 0.0, 3.0))
    assert math.isnan(probabilistic_sharpe_ratio(1.0, 10, 0.0, float("nan")))
    # denominator <= 0 for an extreme SR with strongly negative skew
    assert math.isnan(probabilistic_sharpe_ratio(50.0, 10, -5.0, -5.0))


# --- expected_max_sharpe (EVT calibration) ----------------------------------


@pytest.mark.parametrize("n,exact", _EXACT_MAX.items())
def test_expected_max_sharpe_matches_exact_gaussian_max(n, exact):
    assert expected_max_sharpe(n, 1.0) == pytest.approx(exact, abs=_EVT_TOL)


@pytest.mark.parametrize("n,lower", _TASK_TABLE.items())
def test_expected_max_sharpe_satisfies_task_anchor_table_as_lower_bound(n, lower):
    # The published SOPHIE table reports ~1.50/2.20/2.80/3.20+.  We verified
    # those are NOT the output of the standard B&LdP formula and not consistent
    # with the exact expected maximum (formula/exact give 1.57/2.53/3.26/3.86
    # and 1.539/2.508/3.241/3.852).  Treat them as loose lower bounds.
    assert expected_max_sharpe(n, 1.0) >= lower


def test_expected_max_sharpe_is_monotone_and_linear_in_sigma():
    vals = [expected_max_sharpe(n, 1.0) for n in (2, 10, 100, 1000, 10000)]
    assert vals == sorted(vals)
    for n in (10, 100, 1000):
        assert expected_max_sharpe(n, 2.5) == pytest.approx(2.5 * expected_max_sharpe(n, 1.0))


def test_expected_max_sharpe_single_trial_has_no_selection_bias():
    assert expected_max_sharpe(1, 1.0) == 0.0
    assert expected_max_sharpe(1, 5.0) == 0.0


def test_expected_max_sharpe_rejects_bad_inputs():
    with pytest.raises(ValueError):
        expected_max_sharpe(0, 1.0)
    with pytest.raises(ValueError):
        expected_max_sharpe(10, -1.0)
    with pytest.raises(ValueError):
        expected_max_sharpe(10, float("nan"))


# --- deflated_sharpe_ratio ---------------------------------------------------


def test_dsr_is_psr_against_the_evt_ceiling():
    sr, n, sk, ku, n_trials, sigma = 0.4, 60, -0.3, 4.2, 30, 0.25
    ceiling = expected_max_sharpe(n_trials, sigma)
    assert deflated_sharpe_ratio(sr, n, sk, ku, n_trials, sigma) == pytest.approx(
        probabilistic_sharpe_ratio(sr, n, sk, ku, sr_benchmark=ceiling)
    )


def test_dsr_threshold_boundary():
    """At the DSR=0.95 boundary, nudging the ceiling down flips SURVIVES on."""
    n, sr = 101, 0.2
    zero_ceiling = 0.0  # ceiling == 0 -> plain PSR(SR>0)=0.976, above the line
    assert deflated_sharpe_ratio(sr, n, 0.0, 3.0, 10, zero_ceiling) > DSR_THRESHOLD
    # A large ceiling pushes the same SR below the line.
    big_sigma = 0.5
    assert deflated_sharpe_ratio(sr, n, 0.0, 3.0, 10, big_sigma) < DSR_THRESHOLD


def test_dsr_is_below_psr_whenever_ceiling_is_positive():
    n, sr, sk, ku, n_trials, sigma = 60, 0.4, 0.0, 3.0, 25, 0.2
    psr = probabilistic_sharpe_ratio(sr, n, sk, ku, 0.0)
    assert deflated_sharpe_ratio(sr, n, sk, ku, n_trials, sigma) < psr


# --- mutic_adjusted_ic -------------------------------------------------------


def test_mutic_penalty():
    assert mutic_adjusted_ic(0.05, 0.20) == pytest.approx(0.05 - MUTIC_LAMBDA_DEFAULT * 0.20)
    assert mutic_adjusted_ic(0.05, 0.20, lam=0.0) == pytest.approx(0.05)
    assert mutic_adjusted_ic(0.05, 0.0) == pytest.approx(0.05)
    # fully redundant candidate (corr=1) keeps IC - lambda
    assert mutic_adjusted_ic(0.05, 1.0) == pytest.approx(0.05 - MUTIC_LAMBDA_DEFAULT)


def test_mutic_rejects_bad_inputs():
    with pytest.raises(ValueError):
        mutic_adjusted_ic(float("nan"), 0.1)
    with pytest.raises(ValueError):
        mutic_adjusted_ic(0.05, 0.1, lam=-1.0)


# --- deflation_report --------------------------------------------------------


def _strong_edge(n: int = 60) -> list[float]:
    # per-period SR ~5: mean 0.01, sd ~0.002, tiny deterministic jitter
    return [0.01 + 0.001 * ((i % 5) - 2) for i in range(n)]


def test_deflation_report_strong_edge_survives():
    rep = deflation_report(_strong_edge(), n_trials=10, label="strong")
    assert rep["verdict"] == VERDICT_SURVIVES
    assert rep["dsr"] > DSR_THRESHOLD
    assert rep["dsr"] <= rep["psr"]  # deflation can only lower the confidence
    assert rep["sr_std_is_estimate"] is True
    assert "per-period" in rep["frequency"]


def test_deflation_report_weak_edge_fails():
    # a few positive observations among many tiny ones: positive SR, but N=96
    # trials' noise ceiling swamps it.
    weak = [0.4, 0.3, 0.2, 0.1, 0.05] + [0.01, -0.01] * 10
    rep = deflation_report(weak, n_trials=96, label="weak")
    assert rep["verdict"] == VERDICT_FAILS
    assert rep["dsr"] <= DSR_THRESHOLD


def test_deflation_report_insufficient_when_series_too_short():
    rep = deflation_report([0.01, 0.02, 0.03, -0.01], n_trials=10, label="short")
    assert rep["verdict"] == VERDICT_INSUFFICIENT
    assert rep["n"] == 4


def test_deflation_report_zero_variance_is_insufficient():
    rep = deflation_report([0.01] * 20, n_trials=10, label="flat")
    assert rep["verdict"] == VERDICT_INSUFFICIENT


def test_deflation_report_records_the_kurtosis_convention():
    rep = deflation_report(_strong_edge(20), n_trials=5, label="k")
    assert rep["kurtosis_convention"].startswith("raw")
    assert rep["kurtosis_excess"] == pytest.approx(rep["kurtosis"] - NORMAL_KURTOSIS)


def test_deflation_report_caller_supplied_sr_std_is_not_flagged_as_estimate():
    rep = deflation_report(_strong_edge(), n_trials=10, sr_std=0.1, label="x")
    assert rep["sr_std_is_estimate"] is False
    assert rep["sr_std"] == pytest.approx(0.1)


def test_frequency_mismatch_inflates_psr():
    """The #1 DSR implementation bug: annualised SR with per-period n.  Lock the
    warning by showing the mismatch produces a strictly larger PSR."""
    per_period = probabilistic_sharpe_ratio(0.15, 120, 0.0, 3.0, 0.0)
    annualised_same_n = probabilistic_sharpe_ratio(0.15 * math.sqrt(12), 120, 0.0, 3.0, 0.0)
    assert annualised_same_n > per_period


# --- deflation_from_stats ----------------------------------------------------


def test_deflation_from_stats_flags_normal_assumption():
    rep = deflation_from_stats(0.5, 60, n_trials=20, label="stats")
    assert rep["normal_assumption"] is True
    assert rep["verdict"] in {VERDICT_SURVIVES, VERDICT_FAILS}


def test_deflation_from_stats_insufficient_on_bad_n():
    assert deflation_from_stats(0.5, 1, n_trials=20)["verdict"] == VERDICT_INSUFFICIENT


def test_deflation_from_stats_skew_lowers_dsr_vs_normal():
    normal = deflation_from_stats(0.4, 60, n_trials=20, skew=0.0, kurtosis=3.0)
    skewed = deflation_from_stats(0.4, 60, n_trials=20, skew=-1.0, kurtosis=8.0)
    assert skewed["dsr"] < normal["dsr"]


# --- real-data calibration ---------------------------------------------------


@pytest.mark.skipif(not EXIT_RULES.exists(), reason="exit_rules report not present")
def test_weekend_gap_long_leg_fails_deflation():
    """The playbook's long leg: positive Sharpe, but only 13 trades against a
    126-variant search (6 exits x 3 targets x 7 thresholds) — it must not clear
    the DSR line.  Note the distribution is near-symmetric and platykurtic, so
    the failure is small-sample-vs-search, not a fat-tail correction."""
    rep = json.loads(EXIT_RULES.read_text())
    trades = (((rep.get("trades_full_window") or {}).get("long_only") or {}).get("t_plus_1")) or []
    rets = [t.get("ret_pct") for t in trades if t.get("ret_pct") is not None]
    assert len(rets) == 13
    d = deflation_report(rets, n_trials=126, label="weekend_gap long leg")
    assert d["verdict"] == VERDICT_FAILS
    # Honest note: the long leg is near-symmetric and slightly platykurtic
    # (skew~0.07, raw kurt~2.2), so the failure is driven by n=13 against a
    # 126-variant search, not by fat tails.  The DSR still rejects it.
    assert d["skew"] == pytest.approx(0.066, abs=0.02)
    assert d["kurtosis"] < 3.0
