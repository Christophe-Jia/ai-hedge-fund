"""Tests for the leveraged-ETF decay identity (scripts/leveraged_decay.py).

The identity is exact to second order in the daily log return, so these tests
check three separate things and none of them is a market claim:

1. the closed form against HAND-COMPUTED values;
2. that the second-order term really is the realized log-growth gap between an
   ideal daily-reset L-times fund and an L-times-log-growth exposure (the
   accounting check, not an assertion);
3. that a missing instrument surfaces as an explicit DATA_GAP rather than as a
   silently shorter history.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scripts.leveraged_decay import (
    INSTRUMENTS,
    TRADING_DAYS,
    annualize_arithmetic,
    annualize_compound,
    build_report,
    identity_drag_per_day,
    path_gap,
    per_year_table,
    second_order_realized_drag,
)


# ---------------------------------------------------------------------------
# 1. closed form vs hand computation
# ---------------------------------------------------------------------------


def test_identity_drag_hand_values():
    # (1/2) * L(L-1) * sigma^2
    assert identity_drag_per_day(0.02, 2.0) == pytest.approx(0.5 * 2 * 1 * 0.0004)
    assert identity_drag_per_day(0.02, 2.0) == pytest.approx(0.0004)
    # L=3: (1/2)*3*2 = 3
    assert identity_drag_per_day(0.02, 3.0) == pytest.approx(3 * 0.0004)
    # an unlevered fund has NO rebalancing drag
    assert identity_drag_per_day(0.02, 1.0) == 0.0
    # drag is quadratic in sigma: doubling sigma quadruples it
    assert identity_drag_per_day(0.04, 2.0) == pytest.approx(4 * identity_drag_per_day(0.02, 2.0))
    # and quadratic in L(L-1)
    assert identity_drag_per_day(0.02, 3.0) == pytest.approx(3 * identity_drag_per_day(0.02, 2.0))


def test_annualization_hand_values():
    daily = identity_drag_per_day(0.02, 2.0)  # 0.0004
    assert annualize_arithmetic(daily) == pytest.approx(0.0004 * 252)
    assert annualize_arithmetic(daily) == pytest.approx(0.1008)
    # compound = 1 - exp(-arith)
    assert annualize_compound(daily) == pytest.approx(1 - np.exp(-0.1008))
    assert annualize_compound(daily) == pytest.approx(0.09585, abs=1e-4)
    # compound is always smaller than arithmetic for a positive drag
    assert annualize_compound(daily) < annualize_arithmetic(daily)
    assert annualize_arithmetic(daily, trading_days=365) == pytest.approx(0.146)


def test_second_order_realized_drag_hand_value():
    r = np.array([0.01, -0.02, 0.03])
    # (1/2)*2*1*sum(r^2) = (0.0001 + 0.0004 + 0.0009) = 0.0014
    assert second_order_realized_drag(r, 2.0, log_gap=True) == pytest.approx(0.0014)
    assert second_order_realized_drag(r, 2.0, log_gap=False) == pytest.approx(1 - np.exp(-0.0014))


# ---------------------------------------------------------------------------
# 2. the accounting check: identity == realized gap to third order
# ---------------------------------------------------------------------------


def test_identity_matches_realized_log_gap_on_a_choppy_path():
    rng = np.random.default_rng(7)
    r = rng.normal(0.0, 0.02, 252)
    pg = path_gap(r, 2.0)
    second_order = second_order_realized_drag(r, 2.0, log_gap=True)

    # gap_log is negative (the product underperforms) and equals -second_order
    # up to the neglected third-and-higher order terms
    assert pg["gap_log"] < 0
    assert pg["gap_log"] == pytest.approx(-second_order, rel=0.05)
    # the ideal product underperforms the geometric benchmark
    assert pg["ideal_daily_reset"] < pg["benchmark_L_times_loggrowth"]


def test_alternating_path_decays_while_the_underlying_is_flat():
    """+10%/-10% repeated: the underlying is flat, the 2x product is not."""
    pairs = 50
    r = np.tile(np.array([0.10, -0.10]), pairs)
    pg = path_gap(r, 2.0)

    # the geometric benchmark compounds L times the underlying LOG growth:
    # exp(2 * 50 * ln(0.99)) - 1
    assert pg["benchmark_L_times_loggrowth"] == pytest.approx(
        np.exp(2.0 * pairs * np.log(0.99)) - 1.0
    )
    # the 2x product decays: (1.2 * 0.8) = 0.96 per pair
    assert pg["ideal_daily_reset"] == pytest.approx(0.96**pairs - 1.0)
    assert pg["ideal_daily_reset"] < pg["benchmark_L_times_loggrowth"] < 0

    # both benchmarks understate the true decay on this path; the simple-return
    # gap vs the practitioner benchmark is negative but modest
    assert pg["gap_vs_Lx_cumulative_simple"] < 0
    assert pg["gap_vs_Lx_cumulative_simple"] == pytest.approx(-0.0801, abs=5e-3)
    # the log gap is the interpretable one: ~ -1, i.e. the product loses about
    # one unit of log growth against the geometric benchmark
    assert pg["gap_log"] == pytest.approx(-1.0356, abs=1e-3)


def test_path_dependence_is_monotone_in_choppiness():
    """Same-ish cumulative move, choppier path -> strictly larger drag."""
    smooth = np.full(60, 0.01)
    choppy = np.tile(np.array([0.06, -0.04]), 30)
    d_smooth = second_order_realized_drag(smooth, 2.0)
    d_choppy = second_order_realized_drag(choppy, 2.0)
    assert choppy.sum() == pytest.approx(smooth.sum())  # same first-order path
    assert d_choppy > d_smooth


# ---------------------------------------------------------------------------
# 3. per-year table + data gaps
# ---------------------------------------------------------------------------


def _constant_vol_closes(sigma_daily: float, n_years: int = 2, seed: int = 1) -> pd.Series:
    rng = np.random.default_rng(seed)
    n = int(TRADING_DAYS * n_years)
    idx = pd.bdate_range("2020-01-01", periods=n)
    r = rng.normal(0.0, sigma_daily, n)
    return pd.Series(100.0 * np.exp(np.cumsum(r)), index=idx)


def test_per_year_sigma_and_drag_reproduce_the_identity():
    sigma = 0.02
    closes = _constant_vol_closes(sigma, n_years=2)
    rows = per_year_table(closes, leveraged=2.0)
    assert len(rows) == 2
    for row in rows:
        assert row["sigma_daily"] == pytest.approx(sigma, rel=0.15)
        # drag must equal the identity applied to the year's OWN realized sigma
        assert row["drag_per_day"] == pytest.approx(
            identity_drag_per_day(row["sigma_daily"], 2.0), rel=1e-12
        )
        assert row["annual_drag_arithmetic"] == pytest.approx(row["drag_per_day"] * TRADING_DAYS)
        assert row["identity_vs_realized_log_gap_diff"] == pytest.approx(
            row["realized_second_order_drag_log"] - row["annual_drag_arithmetic"], rel=1e-12
        )
        assert abs(row["identity_residual_3rd_order"]) < 0.10


def test_financing_drag_is_L_minus_1_times_the_rate():
    closes = _constant_vol_closes(0.02, n_years=2)
    rate = pd.Series(
        [0.04] * 500,
        index=pd.bdate_range("2020-01-01", periods=500),
    )
    rows = per_year_table(closes, leveraged=2.0, short_rate=rate)
    assert all("financing_drag_annual" in r for r in rows)
    for r in rows:
        assert r["financing_drag_annual"] == pytest.approx(1.0 * 0.04)


def test_missing_instrument_is_reported_as_a_data_gap(monkeypatch):
    """A symbol absent from the store must surface as DATA_GAP, never as a proxy."""
    import scripts.leveraged_decay as ld

    labels = [label for label, _, _ in INSTRUMENTS]
    assert {"MU", "MRVL", "COIN"} <= set(labels)

    fake = INSTRUMENTS + (("__NOT_IN_STORE__", "__NOT_IN_STORE__", "stocks"),)
    monkeypatch.setattr(ld, "INSTRUMENTS", fake)
    report = ld.build_report(2.0)

    block = report["instruments"]["__NOT_IN_STORE__"]
    assert block["status"] == "DATA_GAP"
    assert "not present" in block["message"]
    assert "data_needed" in block
    # the other instruments are unaffected
    assert report["instruments"]["MU"]["status"] == "OK"


def test_report_separates_the_four_costs_beyond_the_identity():
    report = build_report(2.0)
    keys = report["extra_costs_beyond_the_identity"]
    assert set(keys) == {
        "1_expense_ratio",
        "2_tracking_error",
        "3_financing_swap_cost",
        "4_path_dependence",
    }
    assert keys["1_expense_ratio"]["status"].startswith("DATA_GAP")
    assert keys["2_tracking_error"]["status"].startswith("DATA_GAP")
    # financing is partially computed from the repo-resident short rate
    assert "DTB3" in report["financing"]["proxy"]


def test_coin_carries_a_usable_conclusion():
    report = build_report(2.0)
    coin = report["instruments"]["COIN"]
    if coin["status"] != "OK":
        pytest.skip("COIN series not in the local store")
    fp = coin["full_period"]
    assert fp["mean_sigma_annualized"] > 0.5  # COIN is a high-vol single name
    assert fp["full_period_annual_drag_arithmetic"] > fp["full_period_annual_drag_compound"] > 0
    assert report["conclusion"]["committed_part"]["mean_sigma_annualized"] == pytest.approx(
        fp["mean_sigma_annualized"]
    )
    assert "COIN" in report["conclusion"]["answer"]
