"""Unit tests for the cross-sectional selection engine (synthetic data)."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.selection import (
    IbkrCostModel,
    SelectionBacktest,
    SelectionConfig,
    momentum_12_1,
)


def _make_candles(dates, symbols, seed=7):
    """Synthetic close prices: every symbol has a deterministic drift."""
    rng = np.random.default_rng(seed)
    data = {}
    for i, s in enumerate(symbols):
        drift = 0.0002 * (i + 1)  # later symbols drift harder -> rankable
        rets = drift + rng.normal(0, 0.01, len(dates))
        data[s] = 100.0 * np.cumprod(1 + rets)
    return pd.DataFrame(data, index=dates)


@pytest.fixture
def dates():
    # ~3 years of trading days (730 calendar days x ~1.4)
    return pd.bdate_range("2023-01-02", "2025-12-31", tz="UTC")


@pytest.fixture
def closes(dates):
    return _make_candles(dates, [f"S{i:02d}" for i in range(20)])


class TestFactors:
    def test_momentum_orders_by_drift(self, closes):
        scores = momentum_12_1(closes, closes.index[400])
        # later symbols (higher drift) should score higher on average
        top5 = set(scores.sort_values(ascending=False).head(5).index)
        assert top5 <= {f"S{i:02d}" for i in range(15, 20)}

    def test_momentum_insufficient_history(self, closes):
        # as_of very early -> empty
        assert momentum_12_1(closes, closes.index[100]).empty

    def test_momentum_exclusive_of_asof(self, closes):
        """Poison the as_of row: scores must not change (strictly-before data)."""
        as_of = closes.index[400]
        base = momentum_12_1(closes, as_of)
        poisoned = closes.copy()
        poisoned.loc[as_of] = poisoned.loc[as_of] * 10.0
        assert momentum_12_1(poisoned, as_of).equals(base)


class TestEngine:
    def _cfg(self, closes, **kw):
        return SelectionConfig(
            universe=list(closes.columns),
            start_date=str(closes.index[300].date()),
            end_date=str(closes.index[-1].date()),
            top_n=5,
            **kw,
        )

    def test_engine_runs_and_selects(self, closes):
        bt = SelectionBacktest(closes, self._cfg(closes))
        res = bt.run()
        # window starts at index 300 (~14 months of warm-up for the factor),
        # leaving ~22 month-end signals in 2024-03..2025-12
        assert len(res.holdings) >= 20
        assert res.holdings[0][1]               # non-empty selection
        assert len(res.holdings[0][1]) == 5
        # drift-ordered synthetic data: winners are high-index symbols
        assert len(set(res.holdings[0][1]) & {f"S{i:02d}" for i in range(15, 20)}) >= 3
        assert res.final_value > 0
        assert res.metrics["total_return"] is not None

    def test_no_lookahead_in_selection(self, closes):
        """Poison all data AFTER the first signal date: first selection
        must be identical (engine only reads strictly-before prices)."""
        cfg = self._cfg(closes)
        first_signal = pd.date_range(
            cfg.start_date, cfg.end_date, freq="ME", tz="UTC"
        )[0]
        bt1 = SelectionBacktest(closes, cfg)
        res1 = bt1.run()

        poisoned = closes.copy()
        mask = poisoned.index > first_signal
        poisoned[mask] = poisoned[mask] * 5.0
        bt2 = SelectionBacktest(poisoned, cfg)
        res2 = bt2.run()

        assert res1.holdings[0][1] == res2.holdings[0][1]

    def test_costs_reduce_value(self, closes):
        free = SelectionBacktest(closes, self._cfg(closes, cost_model=IbkrCostModel(spread_bps=0.0, commission_per_share=0.0, min_commission_per_order=0.0))).run()
        costly = SelectionBacktest(closes, self._cfg(closes, cost_model=IbkrCostModel(spread_bps=50.0))).run()
        assert costly.total_costs > free.total_costs
        assert costly.final_value < free.final_value

    def test_equity_conserves_value_when_idle(self, closes):
        """top_n larger than universe -> hold everything, minimal trades."""
        cfg = SelectionConfig(
            universe=list(closes.columns),
            start_date=str(closes.index[300].date()),
            end_date=str(closes.index[-1].date()),
            top_n=25,  # > 20 symbols
        )
        res = SelectionBacktest(closes, cfg).run()
        # first rebalance buys everything once; later rebalances barely trade
        assert res.holdings[0][1] == sorted(closes.columns)
        assert res.total_costs > 0

    def test_cash_never_negative(self, closes):
        res = SelectionBacktest(closes, self._cfg(closes)).run()
        assert res.final_value > 0
        assert not (res.equity <= 0).any()

    def test_ibkr_cost_model_math(self):
        cm = IbkrCostModel()
        # 100 shares @ $50: commission max(0.5, 1.0) = 1.0; spread 50*100*5bps = 2.5
        assert cm.trade_cost(100, 50.0) == pytest.approx(3.5)
        # 1000 shares @ $500: commission 5.0; spread 500*1000*5bps = 250
        assert cm.trade_cost(1000, 500.0) == pytest.approx(255.0)
        assert cm.trade_cost(0, 100.0) == 0.0
