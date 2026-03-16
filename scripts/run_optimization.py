#!/usr/bin/env python3
"""
Grid search parameter optimizer for BTC EMA crossover strategy.

Usage:
    poetry run python scripts/run_optimization.py \
        --fast-periods 8,10,12,15 \
        --slow-periods 20,25,30,40 \
        --quantity-pcts 0.05,0.10,0.15 \
        --start 2024-01-01 --end 2024-12-31 \
        --workers 4 \
        --objective sharpe_ratio \
        --plot
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

# Add repo root to path so src.* imports work
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))


def parse_float_list(s: str) -> list[float]:
    return [float(x.strip()) for x in s.split(",") if x.strip()]


def parse_int_list(s: str) -> list[int]:
    return [int(x.strip()) for x in s.split(",") if x.strip()]


def main():
    parser = argparse.ArgumentParser(description="Grid search for BTC EMA strategy")
    parser.add_argument("--fast-periods", default="8,10,12", type=str)
    parser.add_argument("--slow-periods", default="20,25,30", type=str)
    parser.add_argument("--quantity-pcts", default="0.10", type=str)
    parser.add_argument("--start", default="2024-01-01", type=str, dest="start_date")
    parser.add_argument("--end", default="2024-12-31", type=str, dest="end_date")
    parser.add_argument("--capital", default=100_000.0, type=float)
    parser.add_argument("--workers", default=4, type=int)
    parser.add_argument("--objective", default="sharpe_ratio", type=str)
    parser.add_argument("--plot", action="store_true", help="Generate heatmap (2 param grids only)")
    parser.add_argument("--save-db", action="store_true", help="Save results to hedge_fund.db")
    parser.add_argument(
        "--tickers", default="BTC/USDT", type=str, help="Comma-separated tickers"
    )
    args = parser.parse_args()

    from src.backtesting.optimizer import GridSearchOptimizer
    from src.backtesting.btc_engine import BtcBacktestEngine
    from src.strategies.crypto_ema_strategy import make_ema_agent

    fast_periods = parse_int_list(args.fast_periods)
    slow_periods = parse_int_list(args.slow_periods)
    quantity_pcts = parse_float_list(args.quantity_pcts)
    tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]

    param_grid = {
        "fast_period": fast_periods,
        "slow_period": slow_periods,
        "quantity_pct": quantity_pcts,
    }

    n_combos = len(fast_periods) * len(slow_periods) * len(quantity_pcts)
    print(f"Grid search: {n_combos} combinations | tickers={tickers}")
    print(f"  fast_periods: {fast_periods}")
    print(f"  slow_periods: {slow_periods}")
    print(f"  quantity_pcts: {quantity_pcts}")
    print(f"  {args.start_date} → {args.end_date} | capital=${args.capital:,.0f}")
    print(f"  objective: {args.objective} | workers: {args.workers}\n")

    optimizer = GridSearchOptimizer(
        engine_class=BtcBacktestEngine,
        strategy_factory=make_ema_agent,
        base_config={
            "tickers": tickers,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "initial_capital": args.capital,
        },
        param_grid=param_grid,
        objective=args.objective,
        n_workers=args.workers,
        save_to_db=args.save_db,
    )

    results = optimizer.run()

    print(f"\nTop 5 Results (by {args.objective}):")
    display_cols = list(param_grid.keys()) + [args.objective, "max_drawdown", "calmar_ratio"]
    display_cols = [c for c in display_cols if c in results.columns]
    print(results[display_cols].head(5).to_string(index=False, float_format="{:.4f}".format))

    best = optimizer.best_params(3)
    print(f"\nBest params: {best[0]}")

    if args.plot:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        outputs_dir = REPO_ROOT / "outputs"
        outputs_dir.mkdir(exist_ok=True)
        heatmap_path = str(outputs_dir / f"optimization_heatmap_{timestamp}.png")

        param_keys = list(param_grid.keys())
        if len(param_keys) >= 2:
            optimizer.plot_heatmap(
                x_param=param_keys[0],
                y_param=param_keys[1],
                metric=args.objective,
                save_path=heatmap_path,
            )
        else:
            print("[optimizer] Need at least 2 param dimensions for heatmap")


if __name__ == "__main__":
    main()
