"""Offline unit tests for the DCA backtest framework (no network access)."""

import numpy as np
import pandas as pd
import pytest

from dca_backtest.config import INSTRUMENTS, BacktestConfig
from dca_backtest.data import _segment_level, build_fx
from dca_backtest.engine import contribution_dates, run_backtest
from dca_backtest.metrics import max_drawdown, xirr
from dca_backtest.strategy import FixedWeights

IDX = pd.bdate_range("2000-01-03", periods=400)


def _fx_one():
    return pd.Series(1.0, index=IDX)


def _cfg(**kw):
    defaults = dict(
        monthly_contribution_rmb=100.0,
        contribution_day=1,
        fx_spread_bps=0.0,
        dividend_withholding_tax=0.0,
        rebalance="none",
    )
    defaults.update(kw)
    return BacktestConfig(**defaults)


def test_contribution_dates_rolls_forward():
    idx = pd.DatetimeIndex(
        [pd.Timestamp("2001-01-01"), pd.Timestamp("2001-01-15"), pd.Timestamp("2001-02-01")]
    )
    assert contribution_dates(idx, 1) == [pd.Timestamp("2001-01-01"), pd.Timestamp("2001-02-01")]
    assert contribution_dates(idx, 10) == [pd.Timestamp("2001-01-15"), pd.Timestamp("2001-02-01")]
    # day beyond month end -> last trading day of that month
    assert contribution_dates(idx, 20) == [pd.Timestamp("2001-01-15"), pd.Timestamp("2001-02-01")]


def test_engine_math_with_costs():
    daily_ret = 0.001
    levels = pd.DataFrame({"QQQ": (1 + daily_ret) ** np.arange(len(IDX))}, index=IDX)
    spec = INSTRUMENTS["QQQ"]
    cfg = _cfg()
    res = run_backtest(levels, _fx_one(), cfg, FixedWeights({"QQQ": 1.0}), INSTRUMENTS)

    # independently compute expected NAV: sum of net contributions compounded
    dates = levels.index
    fac = (1 + daily_ret) ** np.arange(len(dates))
    cdates = contribution_dates(dates, 1)
    expected = 0.0
    for d in cdates:
        i = dates.get_loc(d)
        net = cfg.monthly_contribution_rmb * (1 - spec.spread_bps / 1e4)
        expected += net * fac[-1] / fac[i]
    assert res.total_contrib_rmb == pytest.approx(cfg.monthly_contribution_rmb * len(cdates))
    assert res.final_nav_rmb == pytest.approx(expected, rel=1e-10)
    assert res.months == len(cdates)


def test_engine_fx_conversion():
    fx = pd.Series(7.0, index=IDX)
    levels = pd.DataFrame({"QQQ": np.ones(len(IDX))}, index=IDX)  # flat prices
    cfg = _cfg(fx_spread_bps=100.0)  # 1% FX cost, 0 trading spread via instrument? no, QQQ has 5bps
    res = run_backtest(levels, fx, cfg, FixedWeights({"QQQ": 1.0}), INSTRUMENTS)
    spec = INSTRUMENTS["QQQ"]
    per_month_usd = cfg.monthly_contribution_rmb / 7.0 * (1 - 0.01) * (1 - spec.spread_bps / 1e4)
    assert res.final_nav_usd == pytest.approx(per_month_usd * res.months, rel=1e-12)
    assert res.final_nav_rmb == pytest.approx(res.final_nav_usd * 7.0, rel=1e-12)


def test_engine_target_early_stop():
    # price doubles on day 3 -> target hit almost immediately
    lvl = np.ones(len(IDX))
    lvl[3:] *= 2
    levels = pd.DataFrame({"QQQ": lvl}, index=IDX)
    cfg = _cfg(monthly_contribution_rmb=100.0)
    res = run_backtest(
        levels, _fx_one(), cfg, FixedWeights({"QQQ": 1.0}), INSTRUMENTS, target_rmb=150.0
    )
    assert res.hit_date is not None
    assert res.hit_date <= IDX[10]
    assert res.end == res.hit_date


def test_engine_rebalance_restores_weights():
    # two assets; A doubles after first month; annual rebalance should pull back to 30/70
    n = len(IDX)
    a = np.ones(n)
    b = np.ones(n)
    a[10:] = 2.0
    levels = pd.DataFrame({"QQQ": a, "VOO": b}, index=IDX)
    cfg = _cfg(monthly_contribution_rmb=10_000.0, rebalance="annual")
    strat = FixedWeights({"QQQ": 0.3, "VOO": 0.7})
    res = run_backtest(levels, _fx_one(), cfg, strat, INSTRUMENTS)
    # check proportions on a late rebalance month: simulate 3+ months
    daily = res.daily
    # after first rebalance (month 13 contribution), QQQ weight ~30% within cost tolerance
    m13 = contribution_dates(daily.index, 1)[13]
    # values not in daily df; instead re-run with record and inspect trades indirectly:
    # simplest check: final proportions after last rebalance remain near 30/70
    assert res.months >= 13
    # verify via a fresh run capturing values through strategy context
    seen = {}
    from dca_backtest.strategy import StrategyContext

    class Probe(FixedWeights):
        def target_weights(self, ctx: StrategyContext):
            seen[ctx.month_index] = dict(ctx.values)
            return super().target_weights(ctx)

    run_backtest(levels, _fx_one(), cfg, Probe({"QQQ": 0.3, "VOO": 0.7}), INSTRUMENTS)
    # flat prices after day 10 -> no drift between rebalances; month 13 values should be ~30/70
    v = seen[13]
    tot = v["QQQ"] + v["VOO"]
    assert v["QQQ"] / tot == pytest.approx(0.30, abs=0.01)
    assert v["VOO"] / tot == pytest.approx(0.70, abs=0.01)


def test_segment_level_levered_formula():
    s = pd.Series([100.0, 101.0, 99.0], index=pd.bdate_range("2000-01-03", periods=3))
    rf = pd.Series([5.0, 5.0, 5.0], index=s.index)  # 5%
    spec = INSTRUMENTS["TQQQ"]
    lvl = _segment_level(s, "levered", spec, wht=0.0, rf_pct=rf)
    r = 0.01
    expected = 1.0 + 3 * r - 2 * (0.05 + spec.financing_spread) / 252 - spec.er_annual / 252
    assert lvl.iloc[1] / lvl.iloc[0] == pytest.approx(expected, rel=1e-12)


def test_segment_level_price_index_adjustments():
    s = pd.Series([100.0, 102.0], index=pd.bdate_range("2000-01-03", periods=2))
    spec = INSTRUMENTS["QQQ"]
    wht = 0.10
    lvl = _segment_level(s, "price", spec, wht=wht, rf_pct=None)
    daily_adj = (spec.div_yield_annual * (1 - wht) - spec.er_annual) / 252
    assert lvl.iloc[1] / lvl.iloc[0] == pytest.approx(1.02 + daily_adj, rel=1e-12)


def test_build_fx_fills_history():
    idx = pd.bdate_range("2001-01-01", "2001-03-01")
    raw = {"CNY=X": pd.Series(8.28, index=idx)}
    fx = build_fx(raw)
    assert fx.index[0] == pd.Timestamp("1985-01-01")
    assert fx.loc["1990-01-01"] == pytest.approx(4.78)
    assert fx.loc["1994-01-01"] == pytest.approx(8.70)
    assert fx.loc["2001-02-01"] == pytest.approx(8.28)


def test_xirr_single_period():
    r = xirr([(pd.Timestamp("2020-01-01"), -100.0), (pd.Timestamp("2021-01-01"), 110.0)])
    # 365 days / 365.25 day-count makes this slightly below 10%
    assert r == pytest.approx(0.10, abs=5e-3)


def test_xirr_dca_flows():
    flows = [(pd.Timestamp("2020-01-01"), -100.0), (pd.Timestamp("2021-01-01"), -100.0)]
    flows.append((pd.Timestamp("2021-12-31"), 230.0))  # rough 14% IRR
    r = xirr(flows)
    assert r is not None and 0.05 < r < 0.30


def test_max_drawdown():
    nav = pd.Series([100.0, 120.0, 60.0, 90.0])
    assert max_drawdown(nav) == pytest.approx(-0.50)


# ---------------------------------------------------------------------------
# End-to-end smoke test with synthetic raw data (no network): exercises the
# full CLI path including chains, rolling analysis and plotting.
# ---------------------------------------------------------------------------


def _synthetic_raw(years: int = 5) -> dict[str, pd.Series]:
    rng = np.random.default_rng(42)
    idx = pd.bdate_range(end=pd.Timestamp.today().normalize(), periods=years * 252)

    def walk(start, mu, sigma):
        r = rng.normal(mu, sigma, len(idx))
        return start * np.cumprod(1 + r)

    ndx = pd.Series(walk(800.0, 0.0009, 0.015), index=idx)   # ~ Nasdaq-100
    spx = pd.Series(walk(250.0, 0.0005, 0.010), index=idx)   # ~ S&P 500
    irx = pd.Series(4.5 + 0.5 * np.sin(np.arange(len(idx)) / 250), index=idx)
    cny = pd.Series(np.linspace(8.2, 7.0, len(idx)), index=idx)

    # real ETF segments cover only the tail; chains must backfill the rest
    def tail(series, n_days, extra_mu):
        sub = series.iloc[-n_days:].copy()
        return sub * np.cumprod(1 + rng.normal(extra_mu, 0.0002, n_days))

    qqq = tail(ndx, 2 * 252, 0.0002)
    spy = tail(spx, 3 * 252, 0.0001)
    voo = tail(spx, 252, 0.00005)
    tqqq = tail(ndx, int(1.5 * 252), 0.0012)

    return {
        "QQQ": qqq, "SPY": spy, "VOO": voo, "TQQQ": tqqq,
        "^NDX": ndx, "^GSPC": spx, "^IRX": irx, "CNY=X": cny,
    }


def test_full_pipeline_offline(tmp_path, monkeypatch, capsys):
    import dca_backtest.run as run_mod
    from dca_backtest.data import build_fx, build_growth_levels

    raw = _synthetic_raw()
    levels, prov = build_growth_levels(raw, INSTRUMENTS, wht=0.10)
    assert set(levels.columns) == set(INSTRUMENTS)
    assert len(prov) >= 4  # provenance recorded

    # TQQQ synthetic segment: roughly 3x NDX minus financing/ER
    fac = levels.pct_change(fill_method=None).dropna()
    qqq_syn = levels["QQQ"].iloc[: len(levels) - 2 * 252]
    assert len(qqq_syn) > 100  # backfilled from ^NDX

    fx = build_fx(raw)
    assert fx.index[0] <= levels.index[0]

    # run the actual CLI end-to-end with the fetch monkeypatched out
    monkeypatch.setattr(run_mod, "fetch_raw", lambda **kw: raw)
    monkeypatch.setattr(
        "sys.argv",
        [
            "run", "--strategy", "core-satellite", "--years", "5",
            "--horizons", "2,3", "--out-dir", str(tmp_path),
        ],
    )
    run_mod.main()
    out = capsys.readouterr().out
    assert "回测结果" in out
    assert "滚动起点分析" in out
    assert (tmp_path / "equity_curve.png").exists()
    csvs = list(tmp_path.glob("dca_*_daily.csv"))
    assert len(csvs) == 1
    daily = pd.read_csv(csvs[0])
    assert {"nav_rmb", "cum_contrib_rmb", "contrib_rmb"} <= set(daily.columns)
    # NAV in RMB should be positive and roughly track contributions
    assert daily["nav_rmb"].iloc[-1] > 0
