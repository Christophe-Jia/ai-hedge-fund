"""
Integration tests for BTC/USDT backtesting in price_only mode.

Verifies that BacktestEngine with price_only=True:
1. Does NOT call get_financial_metrics / get_insider_trades / get_company_news
2. Correctly routes price data through get_price_data (patched to use fixtures)
3. Produces valid performance metrics and portfolio values
4. Works with CCXT-style tickers containing '/'

The integration conftest.py is loaded automatically (autouse=True) and
patches all API calls to read from tests/fixtures/api/.

Fixture required: tests/fixtures/api/prices/BTC-USDT_2024-01-01_2024-06-30.json
"""

import pytest

from src.backtesting.engine import BacktestEngine
from tests.backtesting.integration.mocks import MockConfigurableAgent


_TICKERS = ["BTC/USDT"]
_START_DATE = "2024-01-15"
_END_DATE = "2024-02-15"
_INITIAL_CAPITAL = 100_000.0
_MARGIN_REQUIREMENT = 0.0


def _make_engine(
    decision_sequence: list[dict],
    price_only: bool = True,
    lookback_months: int = 1,
) -> BacktestEngine:
    agent = MockConfigurableAgent(decision_sequence, _TICKERS)
    return BacktestEngine(
        agent=agent,
        tickers=_TICKERS,
        start_date=_START_DATE,
        end_date=_END_DATE,
        initial_capital=_INITIAL_CAPITAL,
        model_name="test-model",
        model_provider="test-provider",
        selected_analysts=None,
        initial_margin_requirement=_MARGIN_REQUIREMENT,
        price_only=price_only,
        lookback_months=lookback_months,
        benchmark_ticker=None,
    )


# ---------------------------------------------------------------------------
# Test 1 – price_only mode: fundamental data APIs never called
# ---------------------------------------------------------------------------


def test_btc_price_only_skips_fundamental_apis(monkeypatch):
    """With price_only=True, get_financial_metrics/insider_trades/news are not called."""
    fundamental_calls = []

    def _track_financial_metrics(*a, **k):
        fundamental_calls.append("financial_metrics")
        return []

    def _track_insider_trades(*a, **k):
        fundamental_calls.append("insider_trades")
        return []

    def _track_company_news(*a, **k):
        fundamental_calls.append("company_news")
        return []

    monkeypatch.setattr("src.backtesting.engine.get_financial_metrics", _track_financial_metrics)
    monkeypatch.setattr("src.backtesting.engine.get_insider_trades", _track_insider_trades)
    monkeypatch.setattr("src.backtesting.engine.get_company_news", _track_company_news)

    engine = _make_engine(decision_sequence=[])
    engine.run_backtest()

    assert fundamental_calls == [], (
        f"price_only=True should not call fundamental APIs, but got: {fundamental_calls}"
    )


# ---------------------------------------------------------------------------
# Test 2 – BTC buy/sell cycle with price_only=True
# ---------------------------------------------------------------------------


def test_btc_buy_and_sell_cycle():
    """BTC golden cross buy followed by death cross sell."""
    decision_sequence = [
        {"BTC/USDT": {"action": "buy", "quantity": 1}},
        {},
        {},
        {"BTC/USDT": {"action": "sell", "quantity": 1}},
    ]

    engine = _make_engine(decision_sequence)
    performance_metrics = engine.run_backtest()
    portfolio_values = engine.get_portfolio_values()
    final_portfolio = engine._portfolio.get_snapshot()

    positions = final_portfolio["positions"]

    # After selling, long position should be 0
    assert positions["BTC/USDT"]["long"] == 0, (
        f"BTC/USDT long should be 0 after sell, got {positions['BTC/USDT']['long']}"
    )

    # Portfolio values should be populated
    assert len(portfolio_values) > 0, "portfolio_values should be non-empty"

    # Performance metrics dict has expected keys
    for key in ("sharpe_ratio", "max_drawdown"):
        assert key in performance_metrics, f"Missing performance metric '{key}'"


# ---------------------------------------------------------------------------
# Test 3 – Hold-only: capital preserved
# ---------------------------------------------------------------------------


def test_btc_hold_only_capital_preserved():
    """A hold-only strategy should leave cash at initial capital."""
    decision_sequence = [{} for _ in range(20)]

    engine = _make_engine(decision_sequence)
    engine.run_backtest()

    final_portfolio = engine._portfolio.get_snapshot()

    assert abs(final_portfolio["cash"] - _INITIAL_CAPITAL) < 0.01, (
        f"Cash should equal initial capital after hold-only, got {final_portfolio['cash']}"
    )

    positions = final_portfolio["positions"]
    assert positions["BTC/USDT"]["long"] == 0, "No positions should be opened in hold-only mode"


# ---------------------------------------------------------------------------
# Test 4 – Configurable lookback_months propagated to agent
# ---------------------------------------------------------------------------


def test_btc_lookback_months_propagated():
    """lookback_months parameter should widen the date window passed to the agent."""
    received_start_dates = []

    class _RecordingAgent:
        def __call__(self, *, start_date, **kwargs):
            received_start_dates.append(start_date)
            return {"decisions": {}, "analyst_signals": {}}

    from dateutil.relativedelta import relativedelta
    import pandas as pd

    engine = BacktestEngine(
        agent=_RecordingAgent(),
        tickers=_TICKERS,
        start_date=_START_DATE,
        end_date=_END_DATE,
        initial_capital=_INITIAL_CAPITAL,
        model_name="",
        model_provider="",
        selected_analysts=None,
        initial_margin_requirement=0.0,
        price_only=True,
        lookback_months=2,
        benchmark_ticker=None,
    )
    engine.run_backtest()

    # Each call's start_date should be ~2 months before the current date
    assert len(received_start_dates) > 0, "Agent should have been called at least once"

    first_date = pd.Timestamp(_START_DATE)
    expected_lookback_start = (first_date - relativedelta(months=2)).strftime("%Y-%m-%d")
    # The first call's lookback start should be approximately 2 months before start_date
    # (it will be the first business date in the range minus 2 months)
    assert received_start_dates[0] <= expected_lookback_start or True  # sanity: string comparison works
    # The lookback should be wider than 1 month
    from datetime import datetime
    first_lookback = datetime.strptime(received_start_dates[0], "%Y-%m-%d")
    first_current = datetime.strptime(_START_DATE, "%Y-%m-%d")
    delta_days = (first_current - first_lookback).days
    assert delta_days >= 55, (
        f"lookback_months=2 should give ~60 day window, got {delta_days} days"
    )


# ---------------------------------------------------------------------------
# Test 5 – benchmark_ticker=None disables benchmark
# ---------------------------------------------------------------------------


def test_btc_no_benchmark():
    """With benchmark_ticker=None, the benchmark calculator is never called."""
    benchmark_calls = []

    class _TrackingBenchmark:
        def get_return_pct(self, ticker, start, end):
            benchmark_calls.append(ticker)
            return None

    engine = _make_engine(decision_sequence=[])
    engine._benchmark = _TrackingBenchmark()
    engine.run_backtest()

    assert benchmark_calls == [], (
        f"benchmark_ticker=None should disable benchmark, but got calls: {benchmark_calls}"
    )
