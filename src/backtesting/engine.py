from __future__ import annotations

import warnings
from datetime import datetime
from typing import Sequence, Dict, Callable

import pandas as pd
from dateutil.relativedelta import relativedelta

from .controller import AgentController
from .trader import TradeExecutor
from .metrics import PerformanceMetricsCalculator
from .portfolio import Portfolio
from .types import DataGapError, PerformanceMetrics, PortfolioValuePoint
from .valuation import calculate_portfolio_value, compute_exposures
from .output import OutputBuilder
from .benchmarks import BenchmarkCalculator

from src.tools.api import (
    get_company_news,
    get_price_data,
    get_prices,
    get_financial_metrics,
    get_insider_trades,
)

# Consecutive days of missing price data tolerated before aborting the run.
_MAX_CONSECUTIVE_DATA_GAPS = 5


class BacktestEngine:
    """Coordinates the backtest loop using the new components.

    This implementation mirrors the semantics of src/backtester.py while
    avoiding any changes to that file. It orchestrates agent decisions,
    trade execution, valuation, exposures and performance metrics.

    Args:
        price_only: When True, skip fundamental data prefetch
            (get_financial_metrics, get_insider_trades, get_company_news)
            and skip the SPY benchmark prefetch. Suitable for crypto or
            pure technical strategies where fundamental data is not needed.
        lookback_months: Number of months of lookback window passed to the
            agent on each backtest day. Defaults to 3 (was hardcoded at 1).
        benchmark_ticker: Ticker used for benchmark comparison. Defaults to
            "SPY". Set to None to disable benchmark tracking.
    """

    def __init__(
        self,
        *,
        agent,
        tickers: list[str],
        start_date: str,
        end_date: str,
        initial_capital: float,
        model_name: str,
        model_provider: str,
        selected_analysts: list[str] | None,
        initial_margin_requirement: float,
        price_only: bool = False,
        lookback_months: int = 3,
        benchmark_ticker: str | None = "SPY",
        price_data_fn: Callable[[str, str, str], pd.DataFrame] | None = None,
        verbose: bool = True,
    ) -> None:
        self._agent = agent
        self._tickers = tickers
        self._start_date = start_date
        self._end_date = end_date
        self._initial_capital = float(initial_capital)
        self._model_name = model_name
        self._model_provider = model_provider
        self._selected_analysts = selected_analysts
        self._price_only = price_only
        self._lookback_months = lookback_months
        self._benchmark_ticker = benchmark_ticker
        self._verbose = verbose
        # Use provided price fetcher, or fall back to the default API-backed one
        self._price_data_fn: Callable[[str, str, str], pd.DataFrame] = price_data_fn or get_price_data

        self._portfolio = Portfolio(
            tickers=tickers,
            initial_cash=initial_capital,
            margin_requirement=initial_margin_requirement,
        )
        self._executor = TradeExecutor()
        self._agent_controller = AgentController()
        self._perf = PerformanceMetricsCalculator()
        self._results = OutputBuilder(initial_capital=self._initial_capital)

        # Benchmark calculator
        self._benchmark = BenchmarkCalculator()

        self._portfolio_values: list[PortfolioValuePoint] = []
        self._table_rows: list[list] = []
        self._performance_metrics: PerformanceMetrics = {
            "sharpe_ratio": None,
            "sortino_ratio": None,
            "max_drawdown": None,
            "long_short_ratio": None,
            "gross_exposure": None,
            "net_exposure": None,
        }

    def _prefetch_data(self) -> None:
        end_date_dt = datetime.strptime(self._end_date, "%Y-%m-%d")
        start_date_dt = end_date_dt - relativedelta(years=1)
        start_date_str = start_date_dt.strftime("%Y-%m-%d")

        for ticker in self._tickers:
            get_prices(ticker, start_date_str, self._end_date)
            if not self._price_only:
                get_financial_metrics(ticker, self._end_date, limit=10)
                get_insider_trades(ticker, self._end_date, start_date=self._start_date, limit=1000)
                get_company_news(ticker, self._end_date, start_date=self._start_date, limit=1000)

        # Preload data for benchmark ticker
        if self._benchmark_ticker:
            get_prices(self._benchmark_ticker, self._start_date, self._end_date)


    def run_backtest(self) -> PerformanceMetrics:
        self._prefetch_data()

        dates = pd.date_range(self._start_date, self._end_date, freq="B")
        if len(dates) > 0:
            self._portfolio_values = [
                {"Date": dates[0], "Portfolio Value": self._initial_capital}
            ]
        else:
            self._portfolio_values = []

        consecutive_gaps = 0
        for current_date in dates:
            lookback_start = (current_date - relativedelta(months=self._lookback_months)).strftime("%Y-%m-%d")
            current_date_str = current_date.strftime("%Y-%m-%d")
            previous_date_str = (current_date - relativedelta(days=1)).strftime("%Y-%m-%d")
            if lookback_start == current_date_str:
                continue

            try:
                current_prices: Dict[str, float] = {}
                missing_data = False
                for ticker in self._tickers:
                    try:
                        price_data = self._price_data_fn(ticker, previous_date_str, current_date_str)
                        if price_data.empty:
                            missing_data = True
                            break
                        current_prices[ticker] = float(price_data.iloc[-1]["close"])
                    except (KeyError, ValueError) as exc:
                        warnings.warn(
                            f"Bad price data for {ticker} on {current_date_str}: {exc}",
                            stacklevel=2,
                        )
                        missing_data = True
                        break
                if missing_data:
                    consecutive_gaps += 1
                    if consecutive_gaps > _MAX_CONSECUTIVE_DATA_GAPS:
                        raise DataGapError(
                            f"{consecutive_gaps} consecutive days of missing price data "
                            f"ending {current_date_str} — aborting to avoid a distorted "
                            f"equity curve. Check the data source/backfill."
                        )
                    continue
                consecutive_gaps = 0
            except DataGapError:
                raise
            except (KeyError, ValueError) as exc:
                warnings.warn(f"Price fetch failed on {current_date_str}: {exc}", stacklevel=2)
                continue

            agent_output = self._agent_controller.run_agent(
                self._agent,
                tickers=self._tickers,
                start_date=lookback_start,
                end_date=current_date_str,
                portfolio=self._portfolio,
                model_name=self._model_name,
                model_provider=self._model_provider,
                selected_analysts=self._selected_analysts,
            )
            decisions = agent_output["decisions"]

            executed_trades: Dict[str, int] = {}
            for ticker in self._tickers:
                d = decisions.get(ticker, {"action": "hold", "quantity": 0})
                action = d.get("action", "hold")
                qty = d.get("quantity", 0)
                executed_qty = self._executor.execute_trade(ticker, action, qty, current_prices[ticker], self._portfolio)
                executed_trades[ticker] = executed_qty

            total_value = calculate_portfolio_value(self._portfolio, current_prices)
            exposures = compute_exposures(self._portfolio, current_prices)

            point: PortfolioValuePoint = {
                "Date": current_date,
                "Portfolio Value": total_value,
                "Long Exposure": exposures["Long Exposure"],
                "Short Exposure": exposures["Short Exposure"],
                "Gross Exposure": exposures["Gross Exposure"],
                "Net Exposure": exposures["Net Exposure"],
                "Long/Short Ratio": exposures["Long/Short Ratio"],
            }
            self._portfolio_values.append(point)

            if self._verbose:
                # Build daily rows (stateless usage)
                rows = self._results.build_day_rows(
                    date_str=current_date_str,
                    tickers=self._tickers,
                    agent_output=agent_output,
                    executed_trades=executed_trades,
                    current_prices=current_prices,
                    portfolio=self._portfolio,
                    performance_metrics=self._performance_metrics,
                    total_value=total_value,
                    benchmark_return_pct=self._benchmark.get_return_pct(self._benchmark_ticker, self._start_date, current_date_str) if self._benchmark_ticker else None,
                )
                # Prepend today's rows to historical rows so latest day is on top
                self._table_rows = rows + self._table_rows
                # Print full history with latest day first (matches backtester.py behavior)
                self._results.print_rows(self._table_rows)

            # Update performance metrics after printing (match original timing)
            if len(self._portfolio_values) > 3:
                computed = self._perf.compute_metrics(self._portfolio_values)
                if computed:
                    self._performance_metrics.update(computed)

        # Final metrics enrichment: total return, live exposures, trade stats
        if self._portfolio_values:
            final_point = self._portfolio_values[-1]
            self._performance_metrics["total_return"] = (
                final_point["Portfolio Value"] / self._initial_capital - 1.0
            ) * 100.0
            self._performance_metrics["long_short_ratio"] = final_point.get("Long/Short Ratio")
            self._performance_metrics["gross_exposure"] = final_point.get("Gross Exposure")
            self._performance_metrics["net_exposure"] = final_point.get("Net Exposure")
        self._performance_metrics.update(
            self._perf.compute_trade_stats(self._portfolio.trade_pnl)
        )

        return self._performance_metrics

    def get_portfolio_values(self) -> Sequence[PortfolioValuePoint]:
        return list(self._portfolio_values)


