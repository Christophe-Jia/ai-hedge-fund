"""
Integration tests for EMA crossover strategy backtesting.

Uses MockConfigurableAgent to simulate EMA crossover signals against
the AAPL/MSFT/TSLA fixture data (2024-03-01 to 2024-03-08).

The integration conftest.py is loaded automatically (autouse=True) and
patches all API calls to read from tests/fixtures/api/.
"""

from src.backtesting.engine import BacktestEngine
from tests.backtesting.integration.mocks import MockConfigurableAgent


# Shared test parameters
_TICKERS = ["AAPL", "MSFT", "TSLA"]
_START_DATE = "2024-03-01"
_END_DATE = "2024-03-08"
_INITIAL_CAPITAL = 100_000.0
_MARGIN_REQUIREMENT = 0.5


def _make_engine(decision_sequence: list[dict]) -> BacktestEngine:
    """Helper: build and return a configured BacktestEngine."""
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
    )


# ---------------------------------------------------------------------------
# Test 1 – EMA crossover: buy on golden cross, sell on death cross
# ---------------------------------------------------------------------------


def test_ema_crossover_buy_and_sell_cycle():
    """Full EMA crossover cycle: buy on Day 1 (golden cross), sell on Day 4 (death cross).

    Simulated signal sequence:
      Day 1 (2024-03-05): EMA-8 crosses above EMA-21  → buy AAPL 50, MSFT 20
      Day 2 (2024-03-06): EMAs not crossed yet         → hold
      Day 3 (2024-03-07): hold                         → hold
      Day 4 (2024-03-08): EMA-8 crosses below EMA-21   → sell AAPL 50, MSFT 20
    """
    decision_sequence = [
        # Day 1 – golden cross: buy
        {
            "AAPL": {"action": "buy", "quantity": 50},
            "MSFT": {"action": "buy", "quantity": 20},
        },
        # Day 2 – hold
        {},
        # Day 3 – hold
        {},
        # Day 4 – death cross: sell everything
        {
            "AAPL": {"action": "sell", "quantity": 50},
            "MSFT": {"action": "sell", "quantity": 20},
        },
    ]

    engine = _make_engine(decision_sequence)
    performance_metrics = engine.run_backtest()
    portfolio_values = engine.get_portfolio_values()
    final_portfolio = engine._portfolio.get_snapshot()

    positions = final_portfolio["positions"]
    realized_gains = final_portfolio["realized_gains"]

    # After selling everything, long positions should be 0
    assert positions["AAPL"]["long"] == 0, (
        f"AAPL should be fully sold, got {positions['AAPL']['long']}"
    )
    assert positions["MSFT"]["long"] == 0, (
        f"MSFT should be fully sold, got {positions['MSFT']['long']}"
    )
    assert positions["TSLA"]["long"] == 0, (
        f"TSLA was never traded, should remain 0"
    )

    # A round-trip trade should produce non-zero realized gains
    assert realized_gains["AAPL"]["long"] != 0.0, (
        "AAPL round-trip should produce non-zero realized gains"
    )

    # Performance metrics dict should contain the expected keys
    for key in ("sharpe_ratio", "max_drawdown"):
        assert key in performance_metrics, f"Missing performance metric '{key}'"

    # Sharpe ratio should be a float (not None) when there are enough data points
    if performance_metrics["sharpe_ratio"] is not None:
        assert isinstance(performance_metrics["sharpe_ratio"], float), (
            "sharpe_ratio should be a float"
        )

    # Portfolio values list should have been populated
    assert len(portfolio_values) > 0, "portfolio_values should be non-empty"


# ---------------------------------------------------------------------------
# Test 2 – Hold-only strategy: no trades, capital unchanged
# ---------------------------------------------------------------------------


def test_ema_hold_only_strategy():
    """A strategy that never trades should leave the portfolio at initial_capital."""
    decision_sequence = [
        {},  # Day 1 – hold
        {},  # Day 2 – hold
        {},  # Day 3 – hold
        {},  # Day 4 – hold
    ]

    engine = _make_engine(decision_sequence)
    engine.run_backtest()

    final_portfolio = engine._portfolio.get_snapshot()
    positions = final_portfolio["positions"]
    realized_gains = final_portfolio["realized_gains"]

    # No positions should have been opened
    for ticker in _TICKERS:
        assert positions[ticker]["long"] == 0, (
            f"{ticker} long position should be 0 (hold-only), got {positions[ticker]['long']}"
        )
        assert positions[ticker]["short"] == 0, (
            f"{ticker} short position should be 0 (hold-only), got {positions[ticker]['short']}"
        )

    # No trades means no realized gains
    for ticker in _TICKERS:
        assert realized_gains[ticker]["long"] == 0.0, (
            f"{ticker} should have 0 realized gains (hold-only)"
        )

    # Cash should equal initial capital (no trades consumed cash)
    assert abs(final_portfolio["cash"] - _INITIAL_CAPITAL) < 0.01, (
        f"Cash should equal initial capital after hold-only strategy, "
        f"got {final_portfolio['cash']}"
    )


# ---------------------------------------------------------------------------
# Test 3 – Portfolio value consistency checks
# ---------------------------------------------------------------------------


def test_ema_strategy_portfolio_value_consistency():
    """portfolio_values entries should have positive Portfolio Value throughout the run."""
    decision_sequence = [
        {
            "AAPL": {"action": "buy", "quantity": 50},
            "MSFT": {"action": "buy", "quantity": 20},
        },
        {},
        {},
        {},
    ]

    engine = _make_engine(decision_sequence)
    engine.run_backtest()
    portfolio_values = engine.get_portfolio_values()

    assert len(portfolio_values) > 0, "portfolio_values list should not be empty"

    for point in portfolio_values:
        assert "Portfolio Value" in point, "Each point should have a 'Portfolio Value' key"
        assert point["Portfolio Value"] > 0, (
            f"Portfolio Value should be positive, got {point['Portfolio Value']}"
        )
