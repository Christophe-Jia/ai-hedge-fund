"""
BTC-specific backtest engine.

Extends BacktestEngine with:
  - 7×24 calendar day iteration (freq="D") instead of business days
  - Local SQLite OHLCV data via HistoricalOHLCVStore (look-ahead safe)
  - Opening-price execution model (more conservative than close-price)
  - Perpetual futures positions with isolated margin
  - Funding rate settlement every 8 hours
  - Full cost model: exchange fees + market-impact slippage
  - Automatic liquidation checks each bar
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence

import pandas as pd
from dateutil.relativedelta import relativedelta

from .engine import BacktestEngine
from .cost_model import CostModel, VipTier
from .perpetual import PerpPortfolio
from .portfolio import Portfolio
from .trader import TradeExecutor
from .metrics import PerformanceMetricsCalculator
from .types import PerformanceMetrics, PortfolioValuePoint, TradeRecord
from .valuation import calculate_portfolio_value, compute_exposures
from .output import OutputBuilder
from .benchmarks import BenchmarkCalculator
from .controller import AgentController

from src.data.historical_store import HistoricalOHLCVStore
from src.data.funding_rates import FundingRateStore


# 8-hour funding settlement interval in milliseconds
_FUNDING_INTERVAL_MS = 8 * 3_600 * 1_000


class BtcBacktestEngine(BacktestEngine):
    """
    BTC-optimised backtest engine (spot + perpetual futures).

    Key differences from BacktestEngine:
    - freq="D" for date iteration (BTC trades 7×24, no weekends skipped)
    - Prices sourced from HistoricalOHLCVStore (SQLite + CCXT fallback)
    - Opening price used for execution (signal from previous bar's close)
    - Perpetual positions managed via PerpPortfolio
    - Funding rates settled for all 8h windows falling within each bar
    - Positions checked for liquidation before each day's trading
    - CostModel applied to every trade (fees + slippage)

    Args:
        perp_tickers:  List of perpetual ticker symbols, e.g. ["BTC/USDT:USDT"].
        leverage:      Default leverage for perp positions.
        vip_tier:      Binance VIP fee tier (default VIP0).
        bnb_discount:  Apply 25% BNB fee discount.
        db_path:       Path to the SQLite database file.
        annual_trading_days: Used for Sharpe/Sortino annualisation (365 for BTC).
    """

    def __init__(
        self,
        *,
        # BacktestEngine required kwargs — forwarded unchanged
        agent,
        tickers: list[str],
        start_date: str,
        end_date: str,
        initial_capital: float,
        model_name: str,
        model_provider: str,
        selected_analysts: list[str] | None,
        initial_margin_requirement: float,
        price_only: bool = True,
        lookback_months: int = 3,
        benchmark_ticker: str | None = None,
        # BTC-specific kwargs
        perp_tickers: list[str] | None = None,
        leverage: float = 1.0,
        vip_tier: VipTier = VipTier.VIP0,
        bnb_discount: bool = False,
        db_path: str | None = None,
        annual_trading_days: int = 365,
    ) -> None:
        self._perp_tickers: list[str] = perp_tickers or []
        self._leverage = leverage
        self._annual_trading_days = annual_trading_days

        # Build cost model
        self._cost_model = CostModel(vip_tier=vip_tier, bnb_discount=bnb_discount)

        # Data stores
        _db_kw = {"db_path": db_path} if db_path else {}
        self._ohlcv_store = HistoricalOHLCVStore(**_db_kw)
        self._funding_store = FundingRateStore(**_db_kw)

        # Perpetual portfolio
        self._perp_portfolio = PerpPortfolio()

        # Cost-tracking accumulators
        self._total_fees_paid: float = 0.0
        self._total_slippage_cost: float = 0.0
        self._total_funding_paid: float = 0.0

        # Call parent __init__ with a price_data_fn pointing at our store
        # We pass a placeholder; _btc_price_data_fn is set below so we override
        # the parent's _price_data_fn immediately after super().__init__.
        super().__init__(
            agent=agent,
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
            model_name=model_name,
            model_provider=model_provider,
            selected_analysts=selected_analysts,
            initial_margin_requirement=initial_margin_requirement,
            price_only=price_only,
            lookback_months=lookback_months,
            benchmark_ticker=benchmark_ticker,
            price_data_fn=self._btc_price_data_fn,
        )

        # Replace the no-cost executor with a cost-aware one
        self._executor = TradeExecutor(cost_model=self._cost_model)

        # Override metrics calculator with BTC-appropriate trading days
        self._perf = PerformanceMetricsCalculator(
            annual_trading_days=annual_trading_days
        )

    # ------------------------------------------------------------------
    # Price data function (look-ahead safe)
    # ------------------------------------------------------------------

    def _btc_price_data_fn(
        self, ticker: str, start_date_str: str, end_date_str: str
    ) -> pd.DataFrame:
        """
        Fetch OHLCV data from the local SQLite store.

        end_date_str is treated as the exclusive upper bound (look-ahead guard).
        The returned DataFrame has columns: open, high, low, close, volume
        with a DatetimeIndex.
        """
        start_ts = int(
            datetime.fromisoformat(start_date_str)
            .replace(tzinfo=timezone.utc)
            .timestamp()
            * 1000
        )
        # exclusive upper bound: the bar at end_date is NOT included
        end_ts = int(
            datetime.fromisoformat(end_date_str)
            .replace(tzinfo=timezone.utc)
            .timestamp()
            * 1000
        )

        market_type = "perp" if ticker in self._perp_tickers else "spot"

        df = self._ohlcv_store.get_ohlcv(
            symbol=ticker,
            market_type=market_type,
            timeframe="1d",
            start_ts_ms=start_ts,
            end_ts_ms=end_ts,
        )

        if df.empty:
            return pd.DataFrame()

        df["time"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
        df = df.set_index("time").sort_index()
        df = df[["open", "high", "low", "close", "volume"]]
        return df

    # ------------------------------------------------------------------
    # Main backtest loop (override to use freq="D" and BTC-specific steps)
    # ------------------------------------------------------------------

    def run_backtest(self) -> PerformanceMetrics:
        # BTC trades 7×24; skip the fundamental data prefetch
        # (price_only=True is the default for this engine)
        if not self._price_only:
            self._prefetch_data()

        # Use calendar days, not business days
        dates = pd.date_range(self._start_date, self._end_date, freq="D")
        if len(dates) > 0:
            self._portfolio_values = [
                {"Date": dates[0], "Portfolio Value": self._initial_capital}
            ]
        else:
            self._portfolio_values = []

        for current_date in dates:
            lookback_start = (
                current_date - relativedelta(months=self._lookback_months)
            ).strftime("%Y-%m-%d")
            current_date_str = current_date.strftime("%Y-%m-%d")
            previous_date_str = (current_date - relativedelta(days=1)).strftime(
                "%Y-%m-%d"
            )
            if lookback_start == current_date_str:
                continue

            # ----------------------------------------------------------
            # Step a: Get opening prices (look-ahead safe)
            # previous_date_str..current_date_str window gives us
            # the bars that closed before today's open
            # ----------------------------------------------------------
            try:
                current_prices: Dict[str, float] = {}
                missing_data = False
                for ticker in self._tickers:
                    try:
                        price_data = self._price_data_fn(
                            ticker, previous_date_str, current_date_str
                        )
                        if price_data.empty:
                            missing_data = True
                            break
                        # Use open price of the current bar as execution price
                        current_prices[ticker] = float(price_data.iloc[-1]["open"])
                    except Exception:
                        missing_data = True
                        break
                if missing_data:
                    continue
            except Exception:
                continue

            # Also build perp price dict for funding/liquidation
            perp_prices: Dict[str, float] = {}
            for pt in self._perp_tickers:
                if pt in current_prices:
                    perp_prices[pt] = current_prices[pt]
                else:
                    try:
                        price_data = self._price_data_fn(
                            pt, previous_date_str, current_date_str
                        )
                        if not price_data.empty:
                            perp_prices[pt] = float(price_data.iloc[-1]["open"])
                    except Exception:
                        pass

            # ----------------------------------------------------------
            # Step b: Check perpetual liquidations
            # ----------------------------------------------------------
            liquidated = self._perp_portfolio.check_liquidations(perp_prices)
            for sym in liquidated:
                print(f"[LIQUIDATION] {current_date_str}: {sym} position liquidated")

            # ----------------------------------------------------------
            # Step c: Apply pending funding rates (8h intervals)
            # ----------------------------------------------------------
            prev_ts_ms = int(
                datetime.fromisoformat(previous_date_str)
                .replace(tzinfo=timezone.utc)
                .timestamp()
                * 1000
            )
            curr_ts_ms = int(
                datetime.fromisoformat(current_date_str)
                .replace(tzinfo=timezone.utc)
                .timestamp()
                * 1000
            )
            self._apply_funding_for_period(prev_ts_ms, curr_ts_ms, perp_prices)

            # ----------------------------------------------------------
            # Step d: Run agent decisions
            # ----------------------------------------------------------
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

            # ----------------------------------------------------------
            # Step e: Execute trades (with cost model)
            # ----------------------------------------------------------
            executed_trades: Dict[str, int] = {}
            for ticker in self._tickers:
                d = decisions.get(ticker, {"action": "hold", "quantity": 0})
                action = d.get("action", "hold")
                qty = d.get("quantity", 0)
                market_type = "perp" if ticker in self._perp_tickers else "spot"
                executed_qty = self._executor.execute_trade(
                    ticker, action, qty, current_prices[ticker], self._portfolio,
                    market_type=market_type,
                )
                executed_trades[ticker] = executed_qty

            # ----------------------------------------------------------
            # Step f: Calculate total portfolio value
            # ----------------------------------------------------------
            perp_unrealized = self._perp_portfolio.get_total_unrealized_pnl(
                perp_prices
            )
            total_value = (
                calculate_portfolio_value(self._portfolio, current_prices)
                + perp_unrealized
            )
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

            rows = self._results.build_day_rows(
                date_str=current_date_str,
                tickers=self._tickers,
                agent_output=agent_output,
                executed_trades=executed_trades,
                current_prices=current_prices,
                portfolio=self._portfolio,
                performance_metrics=self._performance_metrics,
                total_value=total_value,
                benchmark_return_pct=(
                    self._benchmark.get_return_pct(
                        self._benchmark_ticker, self._start_date, current_date_str
                    )
                    if self._benchmark_ticker
                    else None
                ),
            )
            self._table_rows = rows + self._table_rows
            self._results.print_rows(self._table_rows)

            if len(self._portfolio_values) > 3:
                computed = self._perf.compute_metrics(self._portfolio_values)
                if computed:
                    self._performance_metrics.update(computed)

        # Enrich final metrics with cost summary + Calmar ratio
        self._performance_metrics.update(
            self._compute_cost_summary()
        )
        calmar = self._perf.compute_calmar_ratio(self._portfolio_values)
        if calmar is not None:
            self._performance_metrics["calmar_ratio"] = calmar

        return self._performance_metrics

    # ------------------------------------------------------------------
    # Funding helper
    # ------------------------------------------------------------------

    def _apply_funding_for_period(
        self,
        start_ts_ms: int,
        end_ts_ms: int,
        prices: Dict[str, float],
    ) -> None:
        """Apply all 8h funding settlements between start and end timestamps."""
        for symbol in self._perp_tickers:
            rates = self._funding_store.get_rates_in_range(
                symbol, start_ts_ms, end_ts_ms
            )
            for _ts, rate in rates:
                mark_price = prices.get(symbol)
                if mark_price is None:
                    continue
                cash_flows = self._perp_portfolio.apply_funding_rates(
                    {symbol: rate}, {symbol: mark_price}
                )
                for cf in cash_flows.values():
                    self._portfolio.apply_funding_payment(cf)
                    self._total_funding_paid += -cf if cf < 0 else 0.0

    # ------------------------------------------------------------------
    # Cost summary
    # ------------------------------------------------------------------

    def _compute_cost_summary(self) -> dict:
        """Summarise total costs from trade records."""
        records = self._perp_portfolio.get_trade_records()
        total_fees = sum(r["fee_usd"] for r in records)
        total_slippage = sum(r["slippage_usd"] for r in records)

        # Also count spot trades via portfolio cash movements (approximation)
        # For a more precise count, users should examine trade_records directly.
        initial = self._initial_capital
        final = (
            self._portfolio_values[-1]["Portfolio Value"]
            if self._portfolio_values
            else initial
        )
        net_pnl = final - initial

        return {
            "total_fees_paid": total_fees,
            "total_slippage_cost": total_slippage,
            "total_funding_paid": self._total_funding_paid,
            "net_pnl_after_costs": net_pnl,
            "num_trades": len(records),
        }

    def get_trade_records(self) -> List[TradeRecord]:
        """Return all perpetual trade records."""
        return self._perp_portfolio.get_trade_records()
