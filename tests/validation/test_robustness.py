"""Unit tests for the robustness battery (perturbation / anti-fragility checks).

Synthetic cases have known answers:
  - ``TWO_WINNERS`` is deliberately built so the headline PASS survives
    dropping the single best observation but dies when the best two are
    removed -> ``FRAGILE_BY_FEW_WINNERS`` with k=2.
  - ``UNIFORM`` is a tight positive cloud that survives every knock-out.
Real-data calibration lives at the bottom (weekend_gap must be caught).
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

from src.validation import (
    FRAGILE,
    ROBUST,
    SINGLE_EVENT_DRIVEN,
    concentration_profile,
    drop_fraction_sweep,
    leave_k_best_out,
    leave_k_worst_out,
    leave_one_era_out,
    robustness_battery,
)
from src.validation.robustness import (
    FLAG_BY_ERA,
    FLAG_FEW_WINNERS,
    FLAG_UNSTABLE_RESAMPLE,
    INSUFFICIENT,
    SINGLE_LOSS_DRIVEN,
    loss_concentration_profile,
)

REPORTS = Path(__file__).resolve().parents[2] / "reports"
EXIT_RULES = REPORTS / "exit_rules_backtest.json"

# mean 0.45, sd ~0.86 over 10 observations -> not significant on its own.
_NOISE_CLOUD = [1.5, -0.5, 1.3, -0.4, 1.1, -0.2, 1.4, -0.3, 1.0, -0.4]
# two winners on top of the noise cloud: baseline PASS, drop-1 still PASS,
# drop-2 -> NOISE (hand-verified: t = 2.197 / 2.069 / 1.639).
TWO_WINNERS = [4.5, 2.0] + _NOISE_CLOUD
UNIFORM = [1.0, 1.1, 0.9, 1.05, 0.95, 1.02, 0.98, 1.03, 0.97, 1.01]
SINGLE_EVENT = [20.0, 1.0, 1.0, 1.0, 0.9, 1.1, 1.0, 1.0, 1.0, 1.0]


# --- leave-k-best-out -------------------------------------------------------


def test_two_winners_are_flagged_with_k_equals_2():
    res = leave_k_best_out(TWO_WINNERS)
    assert res["baseline"]["verdict"] == "PASS"
    assert res["curve"][1]["verdict"] == "PASS"      # dropping the best 1 is not enough
    assert res["curve"][2]["verdict"] == "NOISE"     # dropping the best 2 kills it
    assert res["first_failure_k"] == 2
    assert res["n_best_to_sustain"] == 2
    assert res["fragile"] is True
    assert res["flag"] == FLAG_FEW_WINNERS


def test_uniform_series_survives_leave_k_best_out():
    res = leave_k_best_out(UNIFORM)
    assert res["baseline"]["verdict"] == "PASS"
    assert res["fragile"] is False
    assert res["flag"] is None
    assert res["first_failure_k"] is None


def test_leave_k_best_out_default_k_is_min_five_quarter():
    res = leave_k_best_out(UNIFORM)
    assert res["k_max"] == min(5, len(UNIFORM) // 4) == 2


def test_leave_k_best_out_reports_removed_values():
    res = leave_k_best_out(TWO_WINNERS, k=2)
    assert res["curve"][1]["removed"] == [4.5]
    assert res["curve"][2]["removed"] == [4.5, 2.0]


def test_leave_k_best_out_noise_baseline_has_no_claim_to_destroy():
    res = leave_k_best_out(_NOISE_CLOUD)
    assert res["baseline"]["verdict"] == "NOISE"
    assert res["fragile"] is False
    assert "no claim" in res["note"]


def test_leave_k_best_out_empty_is_insufficient():
    res = leave_k_best_out([])
    assert res["n"] == 0
    assert res["fragile"] is False


# --- leave-k-worst-out ------------------------------------------------------


def test_leave_k_worst_out_strengthens_a_winners_series():
    """Removing losses cannot destroy a positive conclusion -> no flag."""
    res = leave_k_worst_out(TWO_WINNERS)
    assert res["fragile"] is False
    assert res["flag"] is None
    assert res["curve"][-1]["mean"] > res["baseline"]["mean"]


def test_leave_k_worst_out_reports_curve_without_enough_leverage_to_flip():
    """A positive series whose sign is genuinely positive: no loss-driven fake."""
    series = [0.4, 0.4, 0.4, 0.4, 0.4, 0.4, -0.1, -0.1, -0.1, -0.15]
    res = leave_k_worst_out(series, k=2)
    assert res["curve"][0]["sign"] == 1
    assert res["sign_flip_k"] is None
    assert res["fragile"] is False


def test_leave_k_worst_out_empty_is_insufficient():
    assert leave_k_worst_out([])["n"] == 0


# --- drop-fraction sweep ----------------------------------------------------


def test_drop_fraction_sweep_is_deterministic_for_a_seed():
    a = drop_fraction_sweep(TWO_WINNERS, n_trials=25, seed=7)
    b = drop_fraction_sweep(TWO_WINNERS, n_trials=25, seed=7)
    assert a == b
    c = drop_fraction_sweep(TWO_WINNERS, n_trials=25, seed=8)
    assert a["curve"] != c["curve"]


def test_uniform_series_keeps_its_sign_after_70pct_drop():
    res = drop_fraction_sweep(UNIFORM)
    last = res["curve"][-1]
    assert last["fraction"] == 0.7
    assert last["sign_flip_rate"] == 0.0
    assert res["flag"] is None
    assert res["sign_stable_through"] == 0.7


def test_two_winners_are_sign_stable_but_lose_significance():
    """The 2-winner series never flips sign, but its significance collapses."""
    res = drop_fraction_sweep(TWO_WINNERS)
    last = res["curve"][-1]
    assert last["sign_flip_rate"] < 0.10
    assert last["pass_rate"] < 1.0
    assert res["flag"] is None


def test_drop_fraction_sweep_flags_unstable_series():
    # slightly positive baseline carried by the tie between two huge legs;
    # random subsets of it flip sign constantly
    series = [3.2, -3.0] * 15
    res = drop_fraction_sweep(series)
    assert res["baseline_sign"] == 1
    assert res["flag"] == FLAG_UNSTABLE_RESAMPLE
    assert res["sign_stable_through"] is None


# --- leave-one-era-out ------------------------------------------------------


def test_leave_one_era_out_flags_era_dependence():
    values = [5.0, 5.0, 5.0, -1.0, -1.0, -1.0]
    eras = ["A", "A", "A", "B", "B", "B"]
    res = leave_one_era_out(values, eras)
    assert res["n"] == 6
    assert res["fragile"] is True
    assert res["flag"] == FLAG_BY_ERA
    assert res["worst_era"] in {"A", "B"}


def test_leave_one_era_out_accepts_mapping():
    res = leave_one_era_out([], {"a": [1.0, 1.1, 0.9], "b": [1.0, 1.05, 0.95]})
    assert res["n"] == 6
    assert res["fragile"] is False


def test_leave_one_era_out_aligned_labels():
    res = leave_one_era_out([1.0, 2.0, 3.0, 4.0], ["x", "x", "y", "y"])
    assert {r["era"] for r in res["eras"]} == {"x", "y"}


# --- concentration profile --------------------------------------------------


def test_concentration_flags_single_event_driven():
    res = concentration_profile(SINGLE_EVENT)
    assert res["single_event_driven"] is True
    assert res["top1_share"] > 0.5
    assert res["flag"] == SINGLE_EVENT_DRIVEN


def test_concentration_measures_top2_share():
    # the red-team construction: 2 events carry 63% of gross positive return
    res = concentration_profile([6.3, 3.7, 1.0, 1.0, 1.0, 1.0, -2.0, -1.0, 0.5, 0.5])
    assert res["top2_share"] == pytest.approx(0.63, abs=0.05)
    assert res["top2_concentrated"] is True
    assert res["effective_n"] is not None


def test_concentration_ex_top_frac_removes_the_largest():
    res = concentration_profile(UNIFORM)
    assert res["n_dropped_top_frac"] == 1  # ceil(5% of 10)
    # the remaining mean must be below the largest observation, and below the
    # full-sample mean (the single largest value was removed)
    assert res["return_ex_top_frac_mean"] < max(UNIFORM)
    assert res["return_ex_top_frac_mean"] < sum(UNIFORM) / len(UNIFORM)


def test_concentration_empty_is_safe():
    res = concentration_profile([])
    assert res["n"] == 0
    assert res["single_event_driven"] is False


# --- battery ----------------------------------------------------------------


def test_battery_two_winners_is_fragile_by_few_winners():
    res = robustness_battery(TWO_WINNERS, label="synthetic two-winner")
    assert res["verdict"] == FRAGILE
    assert FLAG_FEW_WINNERS in res["flags"]
    assert res["leave_k_best_out"]["n_best_to_sustain"] == 2


def test_battery_uniform_is_robust():
    res = robustness_battery(UNIFORM, label="synthetic uniform")
    assert res["verdict"] == ROBUST
    assert res["flags"] == []


def test_battery_single_event_is_single_event_driven():
    res = robustness_battery(SINGLE_EVENT)
    assert res["verdict"] == SINGLE_EVENT_DRIVEN
    assert SINGLE_EVENT_DRIVEN in res["flags"]


def test_battery_too_small_is_insufficient():
    res = robustness_battery([1.0, 2.0, 3.0])
    assert res["verdict"] == INSUFFICIENT
    assert res["flags"] == []


def test_battery_accepts_period_kind():
    res = robustness_battery(UNIFORM, kind="period")
    assert res["kind"] == "period"
    assert res["unit"] == "per-period return"


def test_battery_is_strictly_json_serialisable():
    for series in (TWO_WINNERS, UNIFORM, SINGLE_EVENT, [1.0, 2.0]):
        payload = json.dumps(robustness_battery(series), allow_nan=False)
        assert isinstance(payload, str)


def test_battery_with_eras_runs_era_check():
    res = robustness_battery([5.0, 5.0, 5.0, -1.0, -1.0, -1.0], eras=["A", "A", "A", "B", "B", "B"])
    assert res["leave_one_era_out"] is not None
    assert res["leave_one_era_out"]["n_eras"] == 2


# --- calibration on the real weekend_gap long leg ---------------------------


@pytest.mark.skipif(not EXIT_RULES.exists(), reason="exit_rules report not present")
def test_real_weekend_gap_long_leg_is_fragile_by_few_winners():
    """The red-team finding, automated: the 13 long trades need their best 2."""
    rep = json.loads(EXIT_RULES.read_text())
    returns = [t["ret_pct"] for t in rep["trades_full_window"]["long_only"]["t_plus_1"]]
    assert len(returns) == 13
    res = robustness_battery(returns, label="weekend_gap long_only")
    assert res["verdict"] == FRAGILE
    assert FLAG_FEW_WINNERS in res["flags"]
    assert res["leave_k_best_out"]["first_failure_k"] == 2
    assert res["leave_k_best_out"]["n_best_to_sustain"] == 2
    # matches the red-team's "remove the best 2 -> p goes from 0.05 to ~0.11+"
    assert res["leave_k_best_out"]["curve"][2]["p_value"] > 0.10


@pytest.mark.skipif(not EXIT_RULES.exists(), reason="exit_rules report not present")
def test_real_weekend_gap_long_leg_concentration():
    rep = json.loads(EXIT_RULES.read_text())
    returns = [t["ret_pct"] for t in rep["trades_full_window"]["long_only"]["t_plus_1"]]
    res = concentration_profile(returns)
    assert 0.0 < res["top1_share"] < 0.5
    assert res["single_event_driven"] is False


# --- loss concentration profile (the downside mirror; ledger Gap 1) ---------

# funding's best variant events: the 2022-11-11 FTX collapse is -21.093%.
_FUNDING_EVENTS = [-5.538, 7.622, -21.093, -4.211, 41.454, 5.319, 8.499, 14.7, 2.688, 10.447]


def test_loss_concentration_flags_single_loss_driven():
    res = loss_concentration_profile(_FUNDING_EVENTS)
    assert res["n_losses"] == 3
    assert res["single_loss_driven"] is True
    assert res["top1_loss_share"] == pytest.approx(0.684, abs=0.01)
    assert res["diagnosis"] == "SINGLE_LOSS_DOMINATES"
    assert res["flag"] == SINGLE_LOSS_DRIVEN


def test_loss_concentration_does_not_flag_accumulated_small_losses():
    """Many evenly-spread losses are NOT a single-event problem."""
    series = [-1.0] * 10 + [1.0] * 10
    res = loss_concentration_profile(series)
    assert res["single_loss_driven"] is False
    assert res["top1_loss_share"] == pytest.approx(0.10, abs=0.01)
    assert res["diagnosis"] == "LOSSES_ARE_SPREAD"


def test_loss_concentration_top2_flag_is_a_warning_not_a_verdict():
    # one 20 and one 19 among five -1s: top-2 = 39/43 = 0.91 concentrated,
    # but no single loss >= 50% -> warning only
    res = loss_concentration_profile([-1.0] * 5 + [-20.0, -19.0, 50.0])
    assert res["single_loss_driven"] is False
    assert res["top2_loss_concentrated"] is True
    assert res["diagnosis"] == "TOP2_LOSS_CONCENTRATED"
    assert res["flag"] is None


def test_loss_concentration_without_losses_is_safe():
    res = loss_concentration_profile([1.0, 2.0, 3.0, 4.0, 5.0])
    assert res["diagnosis"] == "NO_LOSSES"
    assert res["single_loss_driven"] is False
    assert res["gross_loss"] == 0.0
    assert res["flag"] is None


def test_loss_concentration_empty_is_safe():
    res = loss_concentration_profile([])
    assert res["n"] == 0
    assert res["single_loss_driven"] is False


def test_battery_carries_the_loss_flag_for_funding():
    res = robustness_battery(_FUNDING_EVENTS, label="funding best variant")
    assert SINGLE_LOSS_DRIVEN in res["flags"]
    assert res["loss_concentration"]["diagnosis"] == "SINGLE_LOSS_DOMINATES"


def test_battery_verdict_unchanged_for_exit_rules():
    """Adding the loss flag must not reclassify the accepted FRAGILE verdict."""
    rep = json.loads(EXIT_RULES.read_text())
    returns = [t["ret_pct"] for t in rep["trades_full_window"]["long_only"]["t_plus_1"]]
    res = robustness_battery(returns, label="weekend_gap long_only")
    assert res["verdict"] == FRAGILE
    assert FLAG_FEW_WINNERS in res["flags"]


def test_loss_concentration_is_json_serialisable():
    for series in (_FUNDING_EVENTS, [1.0, 2.0, 3.0], [-1.0] * 10 + [1.0] * 10):
        json.dumps(loss_concentration_profile(series), allow_nan=False)
