"""Calibration + discipline tests for the LPPLS bubble-criticality diagnostic
(scripts/lppls_crypto.py).

These are GROUND-TRUTH CALIBRATIONS, not market claims:

1. The profile-likelihood fitter must RECOVER a known (t_c, m, omega) from a
   synthetic LPPLS series — otherwise every confidence number downstream is
   meaningless.
2. The filter tests must implement the sourced definitions exactly
   (damping D = m|B| / (omega|C|); the |C|/|B| >= 0.05 pre-condition on O).
3. The confidence at as-of index ``i`` must be INVARIANT to the bars after
   ``i`` — that is the no-look-ahead guarantee the whole pre-registration
   argument rests on.  The strong form (corrupt the future, get the same
   answer) is tested, not the weak form.
4. ``--as-of`` must REFUSE a date before the hypothesis was registered.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from scripts.lppls_crypto import (
    HYPOTHESIS_ID,
    LPPLS_CONSTRAINTS,
    N_TRIALS_PLANNED,
    PARAM_CONFIGS,
    PRIMARY,
    WINDOWS,
    PreRegistrationDataError,
    calendar_days,
    check_filters,
    cluster_episodes,
    confidence_at,
    forward_mdd,
    main,
    oscillation_precondition_holds,
    run_as_of,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
REGISTRY = str(REPO_ROOT / "hypotheses" / "registry.jsonl")

# small deterministic grid: keeps the tests fast without changing the logic
FAST_GRID = {"n_m": 8, "n_omega": 8, "n_tc": 12, "refine_maxiter": 60}


# ---------------------------------------------------------------------------
# synthetic LPPLS construction
# ---------------------------------------------------------------------------


def make_lppls_series(
    n: int = 420,
    *,
    m: float = 0.45,
    omega: float = 9.2,
    tc_ahead: float = 40.0,
    A: float = 4.0,
    B: float = -0.6,
    C: float = 0.05,
    phi: float = 1.3,
    noise: float = 0.004,
    seed: int = 0,
) -> tuple[np.ndarray, np.ndarray, float]:
    """Return (log_price, times, true t_c) for a synthetic LPPLS bubble."""
    rng = np.random.default_rng(seed)
    t = np.arange(n, dtype=float)
    tc = float(t[-1] + tc_ahead)
    tau = tc - t
    logp = A + B * tau**m + C * tau**m * np.cos(omega * np.log(tau) - phi)
    return logp + rng.normal(0.0, noise, n), t, tc


# ---------------------------------------------------------------------------
# 1. parameter recovery
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "m_true, omega_true, ahead_true",
    [(0.45, 9.2, 40.0), (0.30, 7.0, 25.0), (0.70, 11.5, 60.0)],
)
def test_fit_recovers_known_parameters(m_true, omega_true, ahead_true):
    """The fitter must recover the generating (m, omega, t_c) from clean data."""
    from scripts.lppls_crypto import fit_lppls

    logp, t, tc_true = make_lppls_series(
        m=m_true, omega=omega_true, tc_ahead=ahead_true, noise=0.003
    )
    fit = fit_lppls(
        logp, t, m_bounds=PARAM_CONFIGS["standard"]["m"], omega_bounds=PARAM_CONFIGS["standard"]["omega"]
    )
    assert fit["rel_error"] < 0.01, "a nearly noiseless synthetic LPPLS must fit tightly"
    assert abs(fit["m"] - m_true) < 0.10
    assert abs(fit["omega"] - omega_true) < 1.0
    assert abs(fit["t_c"] - tc_true) < 20.0
    # sign convention: a positive (upward) bubble has B < 0
    assert fit["B"] < 0


def test_damping_formula_matches_hand_computation():
    """D = m|B| / (omega|C|) — the sourced definition, checked by hand."""
    from scripts.lppls_crypto import fit_lppls

    logp, t, _ = make_lppls_series(B=-3.0, C=0.10, noise=0.002)
    fit = fit_lppls(
        logp, t, m_bounds=PARAM_CONFIGS["standard"]["m"], omega_bounds=PARAM_CONFIGS["standard"]["omega"]
    )
    expected = fit["m"] * abs(fit["B"]) / (fit["omega"] * abs(fit["C"]))
    assert fit["damping"] == pytest.approx(expected, rel=1e-12)
    # a strongly-damped synthetic bubble must clear the D >= 0.5 bound
    assert fit["damping"] > 0.5
    assert fit["B"] < 0


# ---------------------------------------------------------------------------
# 2. filter definitions
# ---------------------------------------------------------------------------


def _fit(**over):
    base = {
        "m": 0.4,
        "omega": 9.0,
        "B": -1.0,
        "C": 0.1,
        "damping": 0.444,
        "n_oscillations": 3.0,
        "rel_error": 0.02,
    }
    base.update(over)
    return base


def test_check_filters_accepts_a_clean_fit():
    ok, fails = check_filters(
        _fit(damping=0.6), m_bounds=(0.1, 0.9), omega_bounds=(6.0, 13.0)
    )
    assert ok and fails == []


def test_check_filters_rejects_positive_b():
    ok, fails = check_filters(
        _fit(B=+1.0, damping=0.6), m_bounds=(0.1, 0.9), omega_bounds=(6.0, 13.0)
    )
    assert not ok and "F0_positive_bubble_B_ge_0" in fails


def test_check_filters_rejects_low_damping():
    ok, fails = check_filters(
        _fit(damping=0.49), m_bounds=(0.1, 0.9), omega_bounds=(6.0, 13.0)
    )
    assert not ok and "F3_damping" in fails


def test_damping_bound_is_the_sourced_half():
    """The bound must be 0.5 (rsos.180643 table / lppls default), not 0.8."""
    assert LPPLS_CONSTRAINTS["damping_min"] == 0.5


def test_high_relative_error_rejected():
    ok, fails = check_filters(
        _fit(damping=0.6, rel_error=0.06), m_bounds=(0.1, 0.9), omega_bounds=(6.0, 13.0)
    )
    assert not ok and "F5_rel_error" in fails


def test_oscillation_filter_is_gated_by_its_precondition():
    """O < 2.5 must NOT reject a fit whose |C|/|B| < 0.05 (pre-condition fails)."""
    low_osc = _fit(damping=0.6, n_oscillations=1.0, B=-1.0, C=0.01)
    assert not oscillation_precondition_holds(low_osc)  # 0.01/1.0 = 0.01 < 0.05
    ok, fails = check_filters(low_osc, m_bounds=(0.1, 0.9), omega_bounds=(6.0, 13.0))
    assert ok and "F4_oscillations" not in fails

    # the same fit with a meaningful oscillation amplitude IS rejected
    high_osc = _fit(damping=0.6, n_oscillations=1.0, B=-1.0, C=0.10)
    assert oscillation_precondition_holds(high_osc)
    ok2, fails2 = check_filters(high_osc, m_bounds=(0.1, 0.9), omega_bounds=(6.0, 13.0))
    assert not ok2 and "F4_oscillations" in fails2


# ---------------------------------------------------------------------------
# 3. no look-ahead — the discipline the registration rests on
# ---------------------------------------------------------------------------


def _series(n=260, seed=3):
    rng = np.random.default_rng(seed)
    close = 100.0 * np.exp(np.cumsum(rng.normal(0.001, 0.02, n)))
    idx = pd.bdate_range("2021-01-04", periods=n)
    return close, idx


def test_calendar_days_is_zero_based_and_monotone():
    _, idx = _series(50)
    d = calendar_days(idx)
    assert d[0] == 0.0
    assert np.all(np.diff(d) > 0)
    # 10 business days == 14 calendar days
    assert d[10] == pytest.approx(14.0)


def test_confidence_is_invariant_to_future_bars():
    """Corrupting every bar AFTER i must not change the as-of-i confidence."""
    close, idx = _series()
    times = calendar_days(idx)
    i = 220

    base = confidence_at(close, times, i, grid=FAST_GRID, apply_shrinking=False)

    corrupted = close.copy()
    corrupted[i + 1 :] *= 37.0
    after = confidence_at(corrupted, times, i, grid=FAST_GRID, apply_shrinking=False)

    assert after["confidence"] == base["confidence"]
    assert after["n_accepted"] == base["n_accepted"]
    assert after["n_attempted"] == base["n_attempted"]
    assert after["windows"] == base["windows"]


def test_confidence_at_matches_a_truncated_series():
    """Equal to running on the truncated history — no hidden full-history state."""
    close, idx = _series()
    times = calendar_days(idx)
    i = 200
    full = confidence_at(close, times, i, grid=FAST_GRID, apply_shrinking=False)
    trunc = confidence_at(close[: i + 1], times[: i + 1], i, grid=FAST_GRID, apply_shrinking=False)
    assert full["confidence"] == trunc["confidence"]
    assert full["windows"] == trunc["windows"]


def test_fit_requires_a_strictly_increasing_time_axis():
    from scripts.lppls_crypto import fit_lppls

    logp, t, _ = make_lppls_series(n=120)
    bad = t.copy()
    bad[10] = bad[9]
    with pytest.raises(ValueError):
        fit_lppls(
            logp, bad, m_bounds=(0.1, 0.9), omega_bounds=(6.0, 13.0), grid=FAST_GRID, refine=False
        )


# ---------------------------------------------------------------------------
# 4. forward outcome + episode helpers
# ---------------------------------------------------------------------------


def test_forward_mdd_hand_computation():
    close = np.array([100.0, 90.0, 95.0, 80.0, 105.0])
    # from index 0 over 3 bars: min(90,95,80)/100 - 1 = -0.20
    assert forward_mdd(close, 0, 3) == pytest.approx(-0.20)
    # incomplete forward window -> None
    assert forward_mdd(close, 3, 3) is None
    assert forward_mdd(close, 1, 2) == pytest.approx(min(95.0, 80.0) / 90.0 - 1.0)


def test_cluster_episodes_groups_contiguous_dates():
    dates = pd.to_datetime(
        ["2021-01-04", "2021-01-05", "2021-01-06", "2021-03-01", "2021-03-02"]
    )
    eps = cluster_episodes(dates, gap_days=5)
    assert len(eps) == 2
    assert eps[0]["start"] == "2021-01-04" and eps[0]["end"] == "2021-01-06"
    assert eps[0]["n_days"] == 3
    assert eps[1]["start"] == "2021-03-01" and eps[1]["n_days"] == 2


# ---------------------------------------------------------------------------
# 5. registry discipline
# ---------------------------------------------------------------------------


def test_registered_search_grid_multiplies_to_n_trials():
    from scripts.lppls_crypto import GRID  # noqa: F401  (import keeps module loaded)

    assert N_TRIALS_PLANNED == 4 * 2 * 2 * 3 * 3 == 144


def test_as_of_before_registration_is_refused():
    """The mechanical pre-registration guard must fire (not be bypassed)."""
    with pytest.raises(PreRegistrationDataError):
        run_as_of("COIN", "2021-06-01", registry=REGISTRY, grid=FAST_GRID, apply_shrinking=False)


def test_main_as_of_before_registration_exits_2(capsys):
    rc = main(
        ["--as-of", "2021-06-01", "--symbol", "COIN", "--registry", REGISTRY, "--out", "/dev/null"]
    )
    assert rc == 2
    assert "REFUSED" in capsys.readouterr().err


def test_as_of_uses_only_bars_at_or_before_the_date():
    """A post-registration as-of must not return a bar later than the request."""
    res = run_as_of("COIN", "2026-09-25", registry=REGISTRY, grid=FAST_GRID, apply_shrinking=False)
    assert res["pre_registration_gate"]["pre_registration_check"] == "PASSED"
    assert res["last_bar_used"] <= res["requested_as_of"]
    # the store's COIN history ends before the request, so the guard is exercised
    assert res["last_bar_used"] < res["requested_as_of"]
    assert res["n_attempted"] == len(WINDOWS)
    assert 0.0 <= res["confidence"] <= 1.0
    assert res["primary_threshold"] == PRIMARY["theta"]


def test_hypothesis_id_is_the_live_corrected_registration():
    assert HYPOTHESIS_ID == "lppls_crypto_bubble_criticality_v3"
