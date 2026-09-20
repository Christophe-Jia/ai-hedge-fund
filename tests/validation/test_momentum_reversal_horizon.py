"""Unit tests for the momentum/reversal horizon-decomposition estimators.

The estimators are validated against *synthetic known processes*: a panel with
persistent (drifting) symbol returns must give a positive IC; an AR(-1)
mean-reverting panel must give a negative IC. Look-ahead is tested by mutating
a suffix of the price panel and asserting the earlier ``past_k`` rows are
untouched.
"""

from __future__ import annotations

import importlib.util
import math
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "momentum_reversal_horizon.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("momentum_reversal_horizon", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


mrh = _load_module()


# ---------------------------------------------------------------------------
# synthetic panels
# ---------------------------------------------------------------------------

def _panel(rets: np.ndarray, start: str = "2020-01-01") -> pd.DataFrame:
    idx = pd.bdate_range(start, periods=rets.shape[0])
    cols = [f"S{i}" for i in range(rets.shape[1])]
    return pd.DataFrame(100.0 * np.cumprod(1.0 + rets, axis=0), index=idx, columns=cols)


def _all_true_universe(close: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(True, index=close.index, columns=close.columns)


def _momentum_panel(seed: int = 0, n_days: int = 400, n_sym: int = 40) -> pd.DataFrame:
    """Constant per-symbol drift: past and forward windows share the drift."""
    rng = np.random.default_rng(seed)
    drift = rng.normal(0.0, 0.002, n_sym)
    noise = rng.normal(0.0, 0.004, (n_days, n_sym))
    return _panel(drift[None, :] + noise)


def _reversal_panel(seed: int = 0, n_days: int = 400, n_sym: int = 40,
                    phi: float = 0.6) -> pd.DataFrame:
    """AR(-1) returns: r_t = -phi * r_{t-1} + e_t  -> negative autocorrelation."""
    rng = np.random.default_rng(seed)
    e = rng.normal(0.0, 0.01, (n_days, n_sym))
    r = np.zeros_like(e)
    for t in range(1, n_days):
        r[t] = -phi * r[t - 1] + e[t]
    return _panel(r)


# ---------------------------------------------------------------------------
# estimator behaviour on known processes
# ---------------------------------------------------------------------------

def test_ic_positive_on_persistent_momentum_process():
    close = _momentum_panel()
    uni = _all_true_universe(close)
    ic = mrh.cross_sectional_ic(
        mrh.past_return_matrix(close, 10), mrh.forward_return_matrix(close, 5), uni
    )
    assert len(ic) > 100
    assert ic.mean() > 0.1, f"expected positive momentum IC, got {ic.mean():.4f}"
    assert mrh.newey_west_t(ic.values, 4) > 3.0


def test_ic_negative_on_mean_reverting_process():
    close = _reversal_panel()
    uni = _all_true_universe(close)
    ic = mrh.cross_sectional_ic(
        mrh.past_return_matrix(close, 10), mrh.forward_return_matrix(close, 5), uni
    )
    assert len(ic) > 100
    assert ic.mean() < -0.05, f"expected negative reversal IC, got {ic.mean():.4f}"
    assert mrh.newey_west_t(ic.values, 4) < -3.0


def test_ic_is_rank_based_monotone_transform_invariant():
    close = _momentum_panel()
    uni = _all_true_universe(close)
    past = mrh.past_return_matrix(close, 10)
    fwd = mrh.forward_return_matrix(close, 5)
    base = mrh.cross_sectional_ic(past, fwd, uni)
    # an order-preserving transform of the signal must not change Spearman IC
    warped = np.exp(past.clip(-0.9, 5.0))
    shifted = mrh.cross_sectional_ic(warped, fwd, uni)
    pd.testing.assert_series_equal(base, shifted)


def test_min_names_guard_drops_thin_cross_sections():
    close = _momentum_panel(n_sym=5)
    uni = _all_true_universe(close)
    ic = mrh.cross_sectional_ic(
        mrh.past_return_matrix(close, 10), mrh.forward_return_matrix(close, 5),
        uni, min_names=20,
    )
    assert len(ic) == 0


def test_universe_mask_excludes_non_members():
    close = _momentum_panel(n_sym=40)
    uni = _all_true_universe(close)
    uni.iloc[:, 20:] = False  # only 20 members -> below the 20-name floor (strict)
    ic = mrh.cross_sectional_ic(
        mrh.past_return_matrix(close, 5), mrh.forward_return_matrix(close, 5), uni,
        min_names=21,
    )
    assert len(ic) == 0


# ---------------------------------------------------------------------------
# look-ahead
# ---------------------------------------------------------------------------

def test_past_return_at_t_uses_only_bars_before_t():
    close = _momentum_panel(n_days=200)
    past = mrh.past_return_matrix(close, 5)
    t = close.index[100]

    mutated = close.copy()
    mutated.loc[t:] *= 3.0  # change t and everything after
    past_mut = mrh.past_return_matrix(mutated, 5)

    pd.testing.assert_series_equal(past.loc[t], past_mut.loc[t])
    # and it *must* react to a change to one of its own window endpoints
    mutated2 = close.copy()
    mutated2.loc[close.index[99]] *= 2.0  # the signal bar t-1
    past_mut2 = mrh.past_return_matrix(mutated2, 5)
    assert not np.allclose(past.loc[t].values, past_mut2.loc[t].values)
    mutated3 = close.copy()
    mutated3.loc[close.index[94]] *= 2.0  # the base bar t-1-k
    past_mut3 = mrh.past_return_matrix(mutated3, 5)
    assert not np.allclose(past.loc[t].values, past_mut3.loc[t].values)


def test_past_return_uses_the_day_before_t():
    close = _panel(np.zeros((50, 3)) + 0.001)
    # an explicit non-flat series so the endpoints are distinguishable
    close.iloc[:, :] = np.linspace(100.0, 200.0, 50)[:, None]
    past = mrh.past_return_matrix(close, 3)
    t = 10
    expected = close.iloc[t - 1] / close.iloc[t - 1 - 3] - 1.0
    pd.testing.assert_series_equal(past.iloc[t], expected, check_names=False)


def test_forward_window_is_contiguous_with_the_past_window():
    """past covers r[t-k..t-1], forward covers r[t..t+h-1]: no gap, no overlap."""
    close = _momentum_panel(n_days=60, n_sym=3)
    k, h, t = 4, 5, 30
    past = mrh.past_return_matrix(close, k)
    fwd = mrh.forward_return_matrix(close, h)

    # past[t] * (1 + fwd[t]) == C[t-1+h] / C[t-1-k]  (the windows tile exactly)
    lhs = (1.0 + past.iloc[t]) * (1.0 + fwd.iloc[t])
    rhs = close.iloc[t - 1 + h] / close.iloc[t - 1 - k]
    pd.testing.assert_series_equal(lhs, rhs, check_names=False)


def test_forward_return_uses_future_bars():
    close = _momentum_panel(n_days=200)
    fwd = mrh.forward_return_matrix(close, 5)
    t = close.index[100]
    mutated = close.copy()
    mutated.loc[close.index[104]] *= 5.0  # the exit bar t-1+h
    fwd_mut = mrh.forward_return_matrix(mutated, 5)
    assert not np.isclose(fwd.loc[t].iloc[0], fwd_mut.loc[t].iloc[0])
    # a change strictly before the entry bar must not move the forward window of t
    mutated2 = close.copy()
    mutated2.loc[:close.index[98]] *= 5.0
    fwd_mut2 = mrh.forward_return_matrix(mutated2, 5)
    pd.testing.assert_series_equal(fwd.loc[t], fwd_mut2.loc[t])


def test_execution_lag_one_drops_the_shared_boundary_price():
    close = _momentum_panel(n_days=200)
    fwd0 = mrh.forward_return_matrix(close, 5, execution_lag=0)
    fwd1 = mrh.forward_return_matrix(close, 5, execution_lag=1)
    t = 100
    # L=0 shares C[t-1] with the past window; L=1 does not
    assert np.isclose(fwd0.iloc[t].iloc[0],
                      close.iloc[t - 1 + 5].iloc[0] / close.iloc[t - 1].iloc[0] - 1.0)
    assert np.isclose(fwd1.iloc[t].iloc[0],
                      close.iloc[t + 5].iloc[0] / close.iloc[t].iloc[0] - 1.0)


# ---------------------------------------------------------------------------
# HAC / bootstrap
# ---------------------------------------------------------------------------

def test_newey_west_matches_naive_t_without_autocorrelation():
    rng = np.random.default_rng(3)
    x = rng.normal(0.01, 0.1, 4000)
    t_naive = x.mean() / (x.std(ddof=1) / math.sqrt(x.size))
    t_nw = mrh.newey_west_t(x, 1)
    assert abs(t_nw - t_naive) / abs(t_naive) < 0.15


def test_newey_west_shrinks_t_for_positively_overlapping_series():
    # a strongly autocorrelated mean-shifted series: naive t is inflated
    rng = np.random.default_rng(4)
    x = rng.normal(0.0, 1.0, 3000)
    x = np.convolve(x, np.ones(20) / 20.0, mode="same") + 0.05
    t_naive = x.mean() / (x.std(ddof=1) / math.sqrt(x.size))
    t_nw = mrh.newey_west_t(x, 19)
    assert abs(t_nw) < abs(t_naive)


def test_block_bootstrap_ci_brackets_the_mean():
    rng = np.random.default_rng(5)
    x = rng.normal(0.02, 0.1, 600)
    lo, hi = mrh.block_bootstrap_ci(x, block=21, n_boot=500, seed=7)
    assert lo < x.mean() < hi
    assert hi - lo > 0


def test_block_bootstrap_is_deterministic_for_a_fixed_seed():
    rng = np.random.default_rng(6)
    x = rng.normal(0.0, 0.1, 300)
    a = mrh.block_bootstrap_ci(x, block=21, n_boot=200, seed=42)
    b = mrh.block_bootstrap_ci(x, block=21, n_boot=200, seed=42)
    assert a == b


def test_block_bootstrap_handles_series_shorter_than_the_block():
    x = np.array([0.01, -0.02, 0.03, 0.0, 0.01])
    lo, hi = mrh.block_bootstrap_ci(x, block=21, n_boot=200, seed=1)
    assert math.isfinite(lo) and math.isfinite(hi)


# ---------------------------------------------------------------------------
# multiplicity labels
# ---------------------------------------------------------------------------

def _standardised_ic(target_t: float, n: int = 2500, seed: int = 9) -> pd.Series:
    """IC series with an *exact* naive t = ``target_t`` (mean/std fixed)."""
    rng = np.random.default_rng(seed)
    z = rng.normal(0.0, 1.0, n)
    z = (z - z.mean()) / z.std(ddof=1)
    return pd.Series(z * 0.1 + target_t * 0.1 / math.sqrt(n))


def test_nominal_but_not_corrected_cell_is_labelled_not_surviving():
    stats = mrh.ic_stats(_standardised_ic(3.0), 1, n_boot=0)
    assert abs(stats["t_naive"] - 3.0) < 1e-6
    stats = mrh.deflate_cell(stats, n_trials=40, sr_std_overlap=1.0,
                             sr_std_nonoverlap=1.0, bonf_z=3.227)
    assert stats["correction_status"] == "NOT_SURVIVING_CORRECTION"


def test_weak_cell_is_not_significant():
    stats = mrh.ic_stats(_standardised_ic(0.5), 1, n_boot=0)
    stats = mrh.deflate_cell(stats, n_trials=40, sr_std_overlap=1.0,
                             sr_std_nonoverlap=1.0, bonf_z=3.227)
    assert stats["correction_status"] == "NOT_SIGNIFICANT"


def test_huge_cell_survives_bonferroni():
    stats = mrh.ic_stats(_standardised_ic(25.0), 1, n_boot=0)
    stats = mrh.deflate_cell(stats, n_trials=40, sr_std_overlap=1.0,
                             sr_std_nonoverlap=1.0, bonf_z=3.227)
    assert stats["correction_status"].startswith("SURVIVES")


def test_nonoverlap_subsample_stride_equals_horizon():
    ic = pd.Series(np.arange(100, dtype=float))
    stats = mrh.ic_stats(ic, 21, n_boot=0)
    assert stats["nonoverlap_stride"] == 21
    assert stats["nonoverlap_n"] == math.ceil(100 / 21)


# ---------------------------------------------------------------------------
# universe matrix + effective N
# ---------------------------------------------------------------------------

def test_build_universe_matrix_respects_year_snapshots():
    idx = pd.bdate_range("2020-06-01", "2021-06-30")
    pit = {2020: ["A", "B"], 2021: ["B", "C"]}
    uni = mrh.build_universe_matrix(pit, idx, ["A", "B", "C"])
    d2020 = uni.loc["2020-12-31"]
    d2021 = uni.loc["2021-06-30"]
    assert d2020["A"] and d2020["B"] and not d2020["C"]
    assert d2021["B"] and d2021["C"] and not d2021["A"]


def test_effective_n_falls_when_cells_are_correlated():
    rng = np.random.default_rng(12)
    base = rng.normal(0.0, 0.1, 2000)
    indep = pd.DataFrame({f"a{i}": rng.normal(0.0, 0.1, 2000) for i in range(4)})
    dup = pd.DataFrame({f"b{i}": base + rng.normal(0.0, 0.001, 2000) for i in range(4)})
    n_indep = mrh.effective_n(indep)["n_effective"]
    n_dup = mrh.effective_n(dup)["n_effective"]
    assert n_dup < n_indep


def test_sign_map_and_decay_curve_wire_up_consistently():
    cells = {}
    for k in (21, 252):
        for h in (1, 21):
            cells[f"k{k}_h{h}"] = {
                "ic_mean": -0.01 if k == 21 else 0.02,
                "t_newey_west": -0.5 if k == 21 else 2.0,
                "n_days": 1000,
                "correction_status": "NOT_SIGNIFICANT",
                "k": k, "h": h,
            }
    sm = mrh.sign_map(cells, (21, 252), (1, 21))
    assert sm["grid"]["21,1"]["ic_mean"] == pytest.approx(-0.01)
    curve = mrh.decay_curve(cells, "h21", (21, 252))
    assert curve["sign_flip_between"] == {"from": 21, "to": 252}
