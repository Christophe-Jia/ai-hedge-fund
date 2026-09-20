"""Calibration tests for the Kramers-Moyal drift / diffusion estimator
(scripts/econophysics_km_drift.py).

These tests are GROUND-TRUTH CALIBRATIONS, not market claims:

1. The estimator must recover a KNOWN negative drift slope from a synthetic
   mean-reverting (OU-like) process, with a block-bootstrap CI excluding zero.
2. It must recover a KNOWN positive slope from a synthetic feedback/trend
   (escape) process.
3. The rolling z-score state variable must be trailing-only (no look-ahead).
4. The return-shuffle null must ABSORB the mechanical mean reversion that the
   rolling-window z-score induces on a pure random walk — this is the whole
   reason verdicts in the script are decided against the null, not against 0.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scripts.econophysics_km_drift import (
    MIN_BIN_OBS,
    RENAME_MAP,
    VERDICT_ESCAPE,
    VERDICT_UNIDENTIFIABLE,
    VERDICT_WELL,
    _se,
    _std,
    bin_km,
    bootstrap_km_slopes,
    ci,
    classify,
    classify_absent_ticker,
    drift_slope,
    estimate_symbol,
    half_life_days,
    km_sample,
    n_bins_for,
    rolling_zscore,
    shuffle_null_slopes,
)


# ---------------------------------------------------------------------------
# Synthetic ground truth
# ---------------------------------------------------------------------------
def synthetic_feedback(a1: float, a0: float = 0.0, sigma: float = 1.0,
                       n: int = 6000, phi: float = 0.9, seed: int = 0):
    """(y, dy) with dy = a0 + a1*y + sigma*eps.

    y is a stationary AR(1) drawn BEFORE the drift noise, so the true
    conditional mean is exactly a0 + a1*y and the estimator's target slope is
    a1 by construction.  a1 < 0 -> mean reversion (well); a1 > 0 -> feedback
    (escape/trend).
    """
    rng = np.random.default_rng(seed)
    y = np.zeros(n)
    for t in range(1, n):
        y[t] = phi * y[t - 1] + rng.normal(0.0, 1.0)
    dy = a0 + a1 * y + sigma * rng.normal(0.0, 1.0, n)
    return y, dy


def synthetic_price(kappa: float | None, n: int = 3000, sigma: float = 0.02,
                    seed: int = 7) -> np.ndarray:
    """Log-price path.  kappa is None -> pure random walk (GBM); otherwise a
    discretised OU: dlogP = -kappa*(logP - 0) dt + sigma dW."""
    rng = np.random.default_rng(seed)
    if kappa is None:
        return np.cumsum(rng.normal(0.0, sigma, n))
    x = np.zeros(n)
    for t in range(1, n):
        x[t] = x[t - 1] * (1.0 - kappa) + rng.normal(0.0, sigma)
    return x


# ---------------------------------------------------------------------------
# 1 / 2 : slope recovery
# ---------------------------------------------------------------------------
def test_recovers_negative_slope_from_mean_reverting_process():
    y, dy = synthetic_feedback(a1=-0.05, seed=1)
    nb = n_bins_for(y.size)
    bins = bin_km(y, dy, nb)
    slope = drift_slope(bins, 1)

    assert slope == pytest.approx(-0.05, abs=0.01), "estimator must recover the known slope"
    boot = bootstrap_km_slopes(y, dy, block=21, n_boot=300, n_bins=nb,
                               min_obs=MIN_BIN_OBS, seed=2)
    lo, hi, _ = ci(boot)
    assert hi < 0.0, "block-bootstrap CI must exclude 0 for a strong true slope"
    # half-life = ln2 / |kappa| with kappa = -slope
    assert half_life_days(slope) == pytest.approx(np.log(2.0) / 0.05, rel=0.25)


def test_recovers_positive_slope_from_feedback_trend():
    y, dy = synthetic_feedback(a1=+0.05, seed=3)
    nb = n_bins_for(y.size)
    bins = bin_km(y, dy, nb)
    slope = drift_slope(bins, 1)

    assert slope == pytest.approx(+0.05, abs=0.01), "trend/escape slope must be recovered"
    boot = bootstrap_km_slopes(y, dy, block=21, n_boot=300, n_bins=nb,
                               min_obs=MIN_BIN_OBS, seed=4)
    lo, hi, _ = ci(boot)
    assert lo > 0.0, "CI must exclude 0 for a true positive feedback slope"
    assert half_life_days(slope) is None, "no restoring force -> infinite half-life"


def test_estimator_is_unbiased_between_well_and_escape():
    """Symmetric calibration: +a1 and -a1 must give slopes of equal magnitude."""
    y_pos, dy_pos = synthetic_feedback(a1=+0.03, seed=5)
    y_neg, dy_neg = synthetic_feedback(a1=-0.03, seed=5)
    s_pos = drift_slope(bin_km(y_pos, dy_pos, n_bins_for(y_pos.size)), 1)
    s_neg = drift_slope(bin_km(y_neg, dy_neg, n_bins_for(y_neg.size)), 1)
    assert s_pos > 0 > s_neg
    assert abs(s_pos + s_neg) < 0.01, "sign symmetry should hold by construction"


# ---------------------------------------------------------------------------
# 3 : no look-ahead
# ---------------------------------------------------------------------------
def test_rolling_zscore_is_trailing_only():
    """Mutating a FUTURE log-price must not change any earlier y_t."""
    rng = np.random.default_rng(11)
    logp = pd.Series(np.cumsum(rng.normal(0.0, 0.02, 500)))
    y = rolling_zscore(logp, 60).to_numpy()

    perturbed = logp.copy()
    perturbed.iloc[-1] += 0.5  # shock only the very last observation
    y2 = rolling_zscore(perturbed, 60).to_numpy()

    assert np.allclose(y[:-1], y2[:-1], equal_nan=True)
    assert not np.isclose(y[-1], y2[-1])


def test_km_sample_drops_the_undefined_last_step():
    logp = pd.Series(np.cumsum(np.random.default_rng(0).normal(0.0, 0.02, 300)))
    s = km_sample(logp, 60)
    assert s.y.size == 300 - 60, "60-window warm-up plus the one-step target"
    assert np.all(np.isfinite(s.y)) and np.all(np.isfinite(s.dy))


# ---------------------------------------------------------------------------
# 4 : the rolling-window artifact and its null control
# ---------------------------------------------------------------------------
def test_random_walk_shows_the_mechanical_negative_slope_artifact():
    """Documentation-as-test: a pure random walk yields a NEGATIVE drift slope
    purely because y is measured against its own trailing mean.  This is why
    a raw negative slope cannot be read as a potential well."""
    logp = synthetic_price(kappa=None, n=3000, seed=7)
    s = km_sample(pd.Series(logp), 60)
    slope = drift_slope(bin_km(s.y, s.dy, n_bins_for(s.y.size)), 1)
    assert slope < -0.02, f"expected the artifact (about -0.04), got {slope:.4f}"

    # A genuine OU with kappa=0.05 must be MORE negative than the random walk.
    ou = synthetic_price(kappa=0.05, n=3000, seed=7)
    so = km_sample(pd.Series(ou), 60)
    slope_ou = drift_slope(bin_km(so.y, so.dy, n_bins_for(so.y.size)), 1)
    assert slope_ou < slope, "real mean reversion must exceed the mechanical artifact"


def test_shuffle_null_absorbs_the_random_walk_artifact():
    """The return-shuffle null band must contain the random walk's own slope,
    so the null-adjusted verdict for a random walk is UNIDENTIFIABLE."""
    logp = pd.Series(synthetic_price(kappa=None, n=1500, seed=13))
    s = km_sample(logp, 60)
    nb = n_bins_for(s.y.size)
    observed = drift_slope(bin_km(s.y, s.dy, nb), 1)

    null = shuffle_null_slopes(logp, 60, n_null=60, seed=14, n_bins=nb)
    lo, hi, mean = ci(null)
    assert lo <= observed <= hi, "the mechanical artifact must be inside the null band"
    assert classify(observed, lo, hi, lo, hi, mean) == VERDICT_UNIDENTIFIABLE


# ---------------------------------------------------------------------------
# Verdict logic
# ---------------------------------------------------------------------------
def test_classify_null_adjusted_verdicts():
    # observed slope entirely above the null band and below the null mean -> well
    assert classify(-0.10, -0.11, -0.09, -0.05, -0.03, -0.04) == VERDICT_WELL
    # entirely above the null band and above the null mean -> escape / trend
    assert classify(+0.10, 0.09, 0.11, -0.05, -0.03, -0.04) == VERDICT_ESCAPE
    # inside the null band -> unidentifiable (the honest default)
    assert classify(-0.04, -0.05, -0.03, -0.05, -0.03, -0.04) == VERDICT_UNIDENTIFIABLE
    # below the null band but the CI still reaches the null mean -> unidentifiable
    assert classify(-0.06, -0.07, -0.035, -0.05, -0.03, -0.04) == VERDICT_UNIDENTIFIABLE


def test_half_life_definition():
    assert half_life_days(-0.05) == pytest.approx(np.log(2.0) / 0.05)
    assert half_life_days(0.0) is None
    assert half_life_days(0.02) is None
    assert half_life_days(None) is None


# ---------------------------------------------------------------------------
# excess_z must use the statistic's SAMPLING spread, not the MC error of the
# bootstrap mean (using the latter inflates z by ~sqrt(n_boot) and once made a
# pure random walk look like a multiplicity-clearing effect).
# ---------------------------------------------------------------------------
def test_sampling_spread_is_not_mc_error():
    a = np.random.default_rng(0).normal(0.0, 1.0, 500)
    assert _std(a) == pytest.approx(1.0, abs=0.15)
    assert _se(a) == pytest.approx(_std(a) / np.sqrt(a.size))
    assert _std(a) > 10 * _se(a), "the two must not be conflated"


def test_random_walk_excess_z_stays_within_noise():
    """End-to-end guard: on a pure random walk the corrected excess_z must be
    O(1), never a multiplicity-clearing number."""
    logp = pd.Series(synthetic_price(kappa=None, n=1500, seed=21))
    r = estimate_symbol("SYN", logp, window=60, n_boot=200, n_null=60,
                        block=21, seed=1)
    assert r["drift"]["verdict"] == VERDICT_UNIDENTIFIABLE
    assert abs(r["drift"]["excess_z"]) < 3.0, r["drift"]["excess_z"]
    # the reported sampling spread must dwarf the null-mean MC error
    assert r["drift"]["boot_sd"] > 5 * r["drift"]["null_se"]


# ---------------------------------------------------------------------------
# Binning contract
# ---------------------------------------------------------------------------
def test_bins_are_equal_frequency_and_thin_bins_are_dropped():
    y, dy = synthetic_feedback(a1=-0.05, seed=8)
    nb = 10
    bins = bin_km(y, dy, nb, min_obs=MIN_BIN_OBS)
    kept_counts = bins["counts"][bins["kept"]]
    assert kept_counts.size >= 5
    assert kept_counts.min() >= MIN_BIN_OBS
    assert np.all(np.diff(bins["centers"][bins["kept"]]) > 0), "centers are ordered"
    # equal-frequency: interior bins should be far more uniform than the tails
    interior = bins["counts"][2:-2]
    assert interior.std() / interior.mean() < 0.15


def test_n_bins_adapts_to_small_samples():
    assert n_bins_for(400) == 4
    assert n_bins_for(10000) == 20  # capped
    assert n_bins_for(250) == 3     # floor


# ---------------------------------------------------------------------------
# Panel survivorship: never map a takeover onto its acquirer, never double-count
# ---------------------------------------------------------------------------
def test_takeover_and_restructuring_names_are_never_mapped_to_the_acquirer():
    """ABBV/BMY/DD/BAYRY/RTX/T are DIFFERENT firms from the tickers that left
    the index, so a takeover target must never be replaced by the acquirer's
    price series.  Pass them as present members to prove the structural guard
    wins over alias resolution."""
    members = {"ABBV", "BMY", "DD", "BAYRY", "RTX", "T", "META"}
    for sym in ("AGN", "CELG", "MON", "TWX", "RTN", "UTX", "DWDP"):
        verdict, _ = classify_absent_ticker(sym, RENAME_MAP.get(sym), members)
        assert verdict == "structurally_unrecoverable", sym


def test_same_security_rename_is_added_only_when_the_target_is_new():
    # target not otherwise in the panel -> a genuine new series
    assert classify_absent_ticker("BK", "BNY", set())[0] == "add"
    # target already a panel member -> adding the alias would double-weight it
    assert classify_absent_ticker("BK", "BNY", {"BNY"})[0] == "already_represented"
    assert classify_absent_ticker("FB", "META", {"META"})[0] == "already_represented"
    assert classify_absent_ticker("PCLN", "BKNG", {"BKNG"})[0] == "already_represented"


def test_absent_ticker_without_an_alias_is_recorded_not_guessed():
    assert classify_absent_ticker("WBA", None, set())[0] == "no_alias"


def test_alias_targets_resolve_through_the_repo_rename_map():
    """The study must not carry a parallel rename map: every union-gap ticker
    that can be aliased at all must resolve through RENAME_MAP."""
    for sym in ("BK", "FB", "PCLN", "AGN", "CELG", "MON", "TWX", "RTN", "UTX", "DWDP"):
        assert sym in RENAME_MAP, f"{sym} missing from the repo RENAME_MAP"
