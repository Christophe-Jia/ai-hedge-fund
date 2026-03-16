"""
CryptoEmaStrategy — src/strategies/crypto_ema_strategy.py

EMA crossover strategy for crypto markets using CCXT price data.
Designed to work with BacktestEngine in price_only=True mode.

Usage:
    from src.strategies.crypto_ema_strategy import CryptoEmaStrategy
    from src.backtesting.engine import BacktestEngine
    from src.data.crypto import get_crypto_prices, crypto_prices_to_df

    def crypto_price_data(ticker, start_date, end_date):
        prices = get_crypto_prices(ticker, start_date, end_date)
        return crypto_prices_to_df(prices)

    engine = BacktestEngine(
        agent=CryptoEmaStrategy(fast_period=10, slow_period=30),
        tickers=["BTC/USDT"],
        start_date="2024-01-01",
        end_date="2024-12-31",
        initial_capital=100_000,
        model_name="",
        model_provider="",
        selected_analysts=None,
        initial_margin_requirement=0.0,
        price_only=True,
        lookback_months=3,
        benchmark_ticker=None,
        price_data_fn=crypto_price_data,
    )
    metrics = engine.run_backtest()
"""

from __future__ import annotations

import math
import pandas as pd

from src.agents.technicals import calculate_ema
from src.data.crypto import get_crypto_prices, crypto_prices_to_df
from src.strategies.ema_strategy import _long_shares


class CryptoEmaStrategy:
    """
    Dual EMA crossover strategy for crypto (uses CCXT data).

    Parameters:
        fast_period  : Fast EMA period, default 10
        slow_period  : Slow EMA period, default 30
        quantity_pct : Fraction of available cash to deploy per buy signal,
                       default 0.10 (10%)

    Signal logic:
        Golden cross (fast crosses above slow) → buy
        Death cross  (fast crosses below slow) → sell (close long)
        Otherwise                              → hold

    Quantity unit is whole units of the base asset (e.g. 0.001 BTC minimum
    on Binance). For simplicity the strategy trades in integer units; for
    fractional crypto positions the caller should use a fractional-capable
    executor.
    """

    def __init__(
        self,
        fast_period: int = 10,
        slow_period: int = 30,
        quantity_pct: float = 0.10,
        exchange_id: str | None = None,
        lot_size: float = 0.001,
    ) -> None:
        if fast_period >= slow_period:
            raise ValueError(
                f"fast_period ({fast_period}) must be less than slow_period ({slow_period})"
            )
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.quantity_pct = quantity_pct
        self.exchange_id = exchange_id
        # Minimum tradeable unit (e.g. 0.001 BTC). Quantity is rounded down
        # to a multiple of lot_size so that fractional crypto works correctly.
        self.lot_size = lot_size

    def __call__(
        self,
        *,
        tickers: list[str],
        start_date: str,
        end_date: str,
        portfolio,
        **kwargs,
    ) -> dict:
        if isinstance(portfolio, dict):
            cash: float = float(portfolio.get("cash", 0.0))
            positions: dict = portfolio.get("positions", {})
        else:
            cash = float(portfolio.cash)
            positions = portfolio.positions

        decisions: dict = {}

        for symbol in tickers:
            try:
                action, quantity = self._decide(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    cash=cash,
                    positions=positions,
                )
            except Exception:
                action, quantity = "hold", 0

            decisions[symbol] = {"action": action, "quantity": quantity}

        return {"decisions": decisions, "analyst_signals": {}}

    def _decide(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        cash: float,
        positions: dict,
    ) -> tuple[str, int]:
        prices = get_crypto_prices(symbol, start_date, end_date, exchange_id=self.exchange_id)
        if not prices or len(prices) < 3:
            return "hold", 0

        df = crypto_prices_to_df(prices)
        # crypto_prices_to_df sets a timezone-aware UTC index; technicals
        # functions work on numeric columns and don't use the index directly.
        fast_ema: pd.Series = calculate_ema(df, self.fast_period)
        slow_ema: pd.Series = calculate_ema(df, self.slow_period)

        if len(fast_ema) < 2 or len(slow_ema) < 2:
            return "hold", 0

        current_price = float(df["close"].iloc[-1])
        if current_price <= 0:
            return "hold", 0

        prev_fast, curr_fast = float(fast_ema.iloc[-2]), float(fast_ema.iloc[-1])
        prev_slow, curr_slow = float(slow_ema.iloc[-2]), float(slow_ema.iloc[-1])

        # Golden cross → buy
        if prev_fast <= prev_slow and curr_fast > curr_slow:
            budget = cash * self.quantity_pct
            raw_qty = budget / current_price
            # Round down to nearest lot_size multiple
            qty = math.floor(raw_qty / self.lot_size) * self.lot_size
            qty = round(qty, 8)  # avoid floating point noise
            if qty > 0:
                return "buy", qty
            return "hold", 0

        # Death cross → sell (close long)
        if prev_fast >= prev_slow and curr_fast < curr_slow:
            long_qty = _long_shares(positions, symbol)
            if long_qty > 0:
                return "sell", long_qty
            return "hold", 0

        return "hold", 0


# ---------------------------------------------------------------------------
# Agent factory (for use with GridSearchOptimizer)
# ---------------------------------------------------------------------------

def make_ema_agent(
    fast_period: int = 10,
    slow_period: int = 30,
    quantity_pct: float = 0.10,
    **kwargs,
) -> "CryptoEmaStrategy":
    """
    Factory function that creates a CryptoEmaStrategy callable agent.

    Compatible with GridSearchOptimizer's strategy_factory parameter.

    Args:
        fast_period:  Fast EMA lookback (default 10)
        slow_period:  Slow EMA lookback (default 30)
        quantity_pct: Cash fraction to deploy per buy signal (default 0.10)

    Returns:
        A CryptoEmaStrategy instance that behaves as an agent callable.
    """
    return CryptoEmaStrategy(
        fast_period=fast_period,
        slow_period=slow_period,
        quantity_pct=quantity_pct,
    )
