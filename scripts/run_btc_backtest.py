#!/usr/bin/env python3
"""
BTC/USDT EMA crossover backtest — scripts/run_btc_backtest.py

Runs a dual-EMA crossover strategy on BTC/USDT spot using local SQLite
historical data (seeded by seed_btc_history.py). Uses BtcBacktestEngine
for realistic cost modelling: exchange fees, slippage, and 7×24 iteration.

Two modes:
  --no-costs  Use plain BacktestEngine with no fees/slippage (baseline)
  (default)   Use BtcBacktestEngine with Binance VIP0 cost model

Usage:
    poetry run python scripts/run_btc_backtest.py
    poetry run python scripts/run_btc_backtest.py --start 2023-03-01 --end 2026-01-31
    poetry run python scripts/run_btc_backtest.py --fast 10 --slow 30 --capital 50000
    poetry run python scripts/run_btc_backtest.py --no-costs   # baseline (no fees)
"""

import argparse
import sys
import math
from datetime import datetime, timezone
from pathlib import Path

# Allow imports from repo root when running directly
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from src.backtesting.btc_engine import BtcBacktestEngine
from src.backtesting.engine import BacktestEngine
from src.backtesting.cost_model import VipTier
from src.data.historical_store import HistoricalOHLCVStore
from src.agents.technicals import calculate_ema


# ---------------------------------------------------------------------------
# EMA strategy backed by local HistoricalOHLCVStore
# ---------------------------------------------------------------------------

class LocalEmaStrategy:
    """
    Dual EMA crossover strategy that reads prices from HistoricalOHLCVStore.

    Uses the same golden-cross / death-cross logic as CryptoEmaStrategy but
    sources price data from the local SQLite DB instead of live CCXT calls,
    ensuring look-ahead safety and offline operation.
    """

    def __init__(
        self,
        store: HistoricalOHLCVStore,
        fast_period: int = 10,
        slow_period: int = 30,
        quantity_pct: float = 0.95,
        lot_size: float = 0.001,
    ) -> None:
        if fast_period >= slow_period:
            raise ValueError(f"fast_period ({fast_period}) must be < slow_period ({slow_period})")
        self._store = store
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.quantity_pct = quantity_pct
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
        if hasattr(portfolio, "get_cash"):
            cash = portfolio.get_cash()
            positions = dict(portfolio.get_positions())
        else:
            cash = float(portfolio.get("cash", 0.0))
            positions = portfolio.get("positions", {})

        decisions: dict = {}
        for symbol in tickers:
            try:
                action, quantity = self._decide(symbol, start_date, end_date, cash, positions)
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
    ) -> tuple[str, float]:
        start_ts = int(datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc).timestamp() * 1000)
        end_ts = int(datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc).timestamp() * 1000)
        market_type = "spot"

        df = self._store.get_ohlcv(symbol, market_type, "1d", start_ts, end_ts)
        if df.empty or len(df) < self.slow_period + 2:
            return "hold", 0

        prices = df.set_index(pd.to_datetime(df["ts"], unit="ms", utc=True))["close"]
        price_df = prices.to_frame(name="close")

        fast_ema: pd.Series = calculate_ema(price_df, self.fast_period)
        slow_ema: pd.Series = calculate_ema(price_df, self.slow_period)

        if len(fast_ema) < 2 or len(slow_ema) < 2:
            return "hold", 0

        current_price = float(price_df["close"].iloc[-1])
        if current_price <= 0:
            return "hold", 0

        prev_fast, curr_fast = float(fast_ema.iloc[-2]), float(fast_ema.iloc[-1])
        prev_slow, curr_slow = float(slow_ema.iloc[-2]), float(slow_ema.iloc[-1])

        # Golden cross → buy
        if prev_fast <= prev_slow and curr_fast > curr_slow:
            budget = cash * self.quantity_pct
            raw_qty = budget / current_price
            qty = math.floor(raw_qty / self.lot_size) * self.lot_size
            qty = round(qty, 8)
            if qty > 0:
                return "buy", qty
            return "hold", 0

        # Death cross → sell (close entire long)
        if prev_fast >= prev_slow and curr_fast < curr_slow:
            pos = positions.get(symbol, {})
            long_qty = float(pos.get("long", 0)) if isinstance(pos, dict) else float(getattr(pos, "long", 0))
            if long_qty > 0:
                return "sell", long_qty
            return "hold", 0

        return "hold", 0


# ---------------------------------------------------------------------------
# Main backtest runner
# ---------------------------------------------------------------------------

def run_backtest(
    start_date: str,
    end_date: str,
    fast_period: int,
    slow_period: int,
    initial_capital: float,
    quantity_pct: float,
    with_costs: bool,
    db_path: str | None = None,
) -> None:
    ticker = "BTC/USDT"

    print(f"\n{'='*60}")
    print(f"BTC/USDT EMA Crossover Backtest  (LOCAL DATA)")
    print(f"{'='*60}")
    print(f"  Period       : {start_date} → {end_date}")
    print(f"  EMA          : fast={fast_period}, slow={slow_period}")
    print(f"  Capital      : ${initial_capital:,.0f}")
    print(f"  Buy size     : {quantity_pct*100:.0f}% of cash per signal")
    print(f"  Cost model   : {'Binance VIP0 (fees + slippage)' if with_costs else 'None (baseline)'}")
    print()

    db_kwargs = {"db_path": db_path} if db_path else {}
    store = HistoricalOHLCVStore(**db_kwargs)

    strategy = LocalEmaStrategy(
        store=store,
        fast_period=fast_period,
        slow_period=slow_period,
        quantity_pct=quantity_pct,
    )

    if with_costs:
        engine = BtcBacktestEngine(
            agent=strategy,
            tickers=[ticker],
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
            model_name="",
            model_provider="",
            selected_analysts=None,
            initial_margin_requirement=0.0,
            price_only=True,
            lookback_months=3,
            benchmark_ticker=None,
            vip_tier=VipTier.VIP0,
            bnb_discount=False,
            **db_kwargs,
        )
    else:
        # Baseline engine — uses the same local data source but no cost model
        def _price_fn(sym: str, sd: str, ed: str) -> pd.DataFrame:
            s_ts = int(datetime.fromisoformat(sd).replace(tzinfo=timezone.utc).timestamp() * 1000)
            e_ts = int(datetime.fromisoformat(ed).replace(tzinfo=timezone.utc).timestamp() * 1000)
            df = store.get_ohlcv(sym, "spot", "1d", s_ts, e_ts)
            if df.empty:
                return pd.DataFrame()
            df["time"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
            return df.set_index("time")[["open", "high", "low", "close", "volume"]]

        engine = BacktestEngine(
            agent=strategy,
            tickers=[ticker],
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
            model_name="",
            model_provider="",
            selected_analysts=None,
            initial_margin_requirement=0.0,
            price_only=True,
            lookback_months=3,
            benchmark_ticker=None,
            price_data_fn=_price_fn,
        )

    metrics = engine.run_backtest()

    portfolio_values = engine.get_portfolio_values()
    final_snapshot = engine._portfolio.get_snapshot()

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("BACKTEST SUMMARY")
    print("=" * 60)

    if portfolio_values:
        initial_val = portfolio_values[0]["Portfolio Value"]
        final_val = portfolio_values[-1]["Portfolio Value"]
        total_return_pct = (final_val / initial_val - 1.0) * 100
        print(f"  Initial value    : ${initial_val:>12,.2f}")
        print(f"  Final value      : ${final_val:>12,.2f}")
        print(f"  Total return     : {total_return_pct:>+.2f}%")

    print()
    print("Performance Metrics:")
    for key, label in [
        ("sharpe_ratio",   "Sharpe ratio "),
        ("sortino_ratio",  "Sortino ratio"),
        ("max_drawdown",   "Max drawdown "),
        ("calmar_ratio",   "Calmar ratio "),
        ("win_rate",       "Win rate     "),
        ("profit_factor",  "Profit factor"),
    ]:
        v = metrics.get(key)
        if v is not None:
            suffix = "%" if key == "max_drawdown" else ""
            print(f"  {label} : {v:.4f}{suffix}")

    if with_costs:
        print()
        print("Cost breakdown:")
        for key, label in [
            ("total_fees_paid",    "Exchange fees  "),
            ("total_slippage_cost","Slippage       "),
            ("total_funding_paid", "Funding rates  "),
        ]:
            v = metrics.get(key)
            if v is not None:
                print(f"  {label} : ${v:>10,.2f}")
        net = metrics.get("net_pnl_after_costs")
        if net is not None:
            print(f"  Net P&L after costs: ${net:>10,.2f}")

    print()
    print("Final Positions:")
    pos = final_snapshot["positions"].get(ticker, {})
    long_qty = float(pos.get("long", 0)) if isinstance(pos, dict) else float(getattr(pos, "long", 0))
    print(f"  {ticker} long  : {long_qty:.4f} BTC")
    print(f"  Cash remaining : ${final_snapshot['cash']:,.2f}")

    realized = final_snapshot.get("realized_gains", {})
    btc_gain = realized.get(ticker, {})
    long_gain = float(btc_gain.get("long", 0.0)) if isinstance(btc_gain, dict) else 0.0
    print(f"  Realized P&L   : ${long_gain:+,.2f}")
    print("=" * 60)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="BTC/USDT EMA crossover backtest using local SQLite data"
    )
    parser.add_argument("--start",   default="2023-03-01", help="Start date YYYY-MM-DD")
    parser.add_argument("--end",     default="2026-01-31", help="End date YYYY-MM-DD")
    parser.add_argument("--fast",    type=int,   default=10,        help="Fast EMA period (default: 10)")
    parser.add_argument("--slow",    type=int,   default=30,        help="Slow EMA period (default: 30)")
    parser.add_argument("--capital", type=float, default=100_000.0, help="Initial capital USD")
    parser.add_argument("--qty-pct", type=float, default=0.95,      help="Fraction of cash per buy (default: 0.95)")
    parser.add_argument("--db",      type=str,   default=None,      help="SQLite DB path")
    parser.add_argument("--no-costs", action="store_true",          help="Disable cost model (baseline run)")
    args = parser.parse_args()

    run_backtest(
        start_date=args.start,
        end_date=args.end,
        fast_period=args.fast,
        slow_period=args.slow,
        initial_capital=args.capital,
        quantity_pct=args.qty_pct,
        with_costs=not args.no_costs,
        db_path=args.db,
    )


if __name__ == "__main__":
    main()
