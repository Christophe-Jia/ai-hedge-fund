#!/usr/bin/env python3
"""Walk-forward validation for the momentum selection strategy.

Splits the backtest into rolling 3-year-train / 1-year-test folds.
On each training window, grid-searches top_n × lookback_bars. Applies
the best parameters to the test window. Stitched test windows = the
honest out-of-sample equity curve.

Usage:
    poetry run python scripts/run_walk_forward.py
    poetry run python scripts/run_walk_forward.py --pit
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from functools import partial
from itertools import product
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from src.data.nasdaq_store import NasdaqDailyStore
from src.selection import SelectionBacktest, SelectionConfig, momentum_12_1
from scripts.run_selection_backtest import load_closes, load_universe

OUT_DIR = Path(__file__).resolve().parents[1] / "outputs" / "walk_forward"

# Parameter grid
TOP_N_OPTIONS = [5, 10, 15, 20]
LOOKBACK_OPTIONS = [126, 189, 252]  # 6mo, 9mo, 12mo
SKIP_RECENT = 21  # fixed (standard 12-1 convention)


def run_one(closes, universe, start, end, top_n, lookback) -> dict:
    """Run a single backtest with given parameters. Returns key metrics."""
    factor = partial(momentum_12_1, lookback_bars=lookback, skip_recent_bars=SKIP_RECENT)
    cfg = SelectionConfig(
        universe=universe,
        start_date=start,
        end_date=end,
        top_n=top_n,
        initial_capital=100_000,
    )
    try:
        result = SelectionBacktest(closes, cfg, factor=factor).run()
        return {
            "total_return": result.metrics.get("total_return", 0),
            "sharpe": result.metrics.get("sharpe_ratio", 0) or 0,
            "max_dd": result.metrics.get("max_drawdown", 0) or 0,
            "calmar": result.metrics.get("calmar_ratio", 0) or 0,
        }
    except Exception:
        return {"total_return": 0, "sharpe": 0, "max_dd": 0, "calmar": 0}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--pit", action="store_true", help="point-in-time universe")
    p.add_argument("--train-years", type=int, default=3)
    p.add_argument("--test-years", type=int, default=1)
    args = p.parse_args()

    end = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    universe = load_universe("pit" if args.pit else "today")
    if isinstance(universe, dict):
        all_symbols = sorted(set().union(*universe.values()))
    else:
        all_symbols = universe

    store = NasdaqDailyStore(assetclass="stocks")
    print(f"Loading {len(all_symbols)} stocks...")
    closes = load_closes(store, all_symbols, "2016-09-12", end)
    print(f"  {closes.shape[0]} days × {closes.shape[1]} symbols")

    # Generate folds
    start_year = 2018  # first full year after warm-up
    end_year = int(end[:4])
    folds = []
    for ty in range(start_year + args.train_years, end_year + 1, args.test_years):
        train_start = f"{ty - args.train_years}-01-01"
        train_end = f"{ty}-01-01"
        test_start = train_end
        test_end = f"{min(ty + args.test_years, end_year + 1)}-01-01"
        if ty + args.test_years > end_year + 1:
            test_end = end
        folds.append({
            "train": (train_start, train_end),
            "test": (test_start, test_end),
            "test_year": ty,
        })

    print(f"\n{'=' * 70}")
    print(f"  Walk-Forward 验证  |  {args.train_years}年训练 + {args.test_years}年测试 × {len(folds)} 折")
    print(f"  参数网格: top_n {TOP_N_OPTIONS} × lookback {LOOKBACK_OPTIONS}")
    print(f"  宇宙: {'point-in-time' if args.pit else '今日名单'}")
    print(f"{'=' * 70}")

    grid = list(product(TOP_N_OPTIONS, LOOKBACK_OPTIONS))
    fold_results = []
    oos_returns = []

    for i, fold in enumerate(folds, 1):
        ts, te = fold["train"]
        vs, ve = fold["test"]

        # Grid search on training window
        best_score, best_params = -999, None
        for top_n, lb in grid:
            m = run_one(closes, universe, ts, te, top_n, lb)
            score = m["sharpe"]
            if score > best_score:
                best_score = score
                best_params = (top_n, lb)

        # Run with best params on test window
        test_m = run_one(closes, universe, vs, ve, *best_params)

        # Also run default params (top_n=10, lookback=252) for comparison
        default_m = run_one(closes, universe, vs, ve, 10, 252)

        fold_results.append({
            "fold": i,
            "test_year": fold["test_year"],
            "train_period": f"{ts[:7]}~{te[:7]}",
            "test_period": f"{vs[:7]}~{ve[:7]}",
            "best_top_n": best_params[0],
            "best_lookback": best_params[1],
            "train_sharpe": best_score,
            "test_sharpe": test_m["sharpe"],
            "test_return": test_m["total_return"],
            "test_max_dd": test_m["max_dd"],
            "default_sharpe": default_m["sharpe"],
            "default_return": default_m["total_return"],
        })
        oos_returns.append(test_m["total_return"])

        print(f"\n  折 {i} (测试 {fold['test_year']} 年)")
        print(f"    训练最优: top_n={best_params[0]}, lookback={best_params[1]}d "
              f"(训练Sharpe {best_score:.2f})")
        print(f"    样本外:   Sharpe {test_m['sharpe']:.2f} | "
              f"收益 {test_m['total_return']:+.1f}% | 回撤 {test_m['max_dd']:.1f}%")
        print(f"    固定参数: Sharpe {default_m['sharpe']:.2f} | "
              f"收益 {default_m['total_return']:+.1f}%")

    # Summary
    avg_train_sharpe = sum(f["train_sharpe"] for f in fold_results) / len(fold_results)
    avg_test_sharpe = sum(f["test_sharpe"] for f in fold_results) / len(fold_results)
    avg_default_sharpe = sum(f["default_sharpe"] for f in fold_results) / len(fold_results)
    degradation = (avg_train_sharpe - avg_test_sharpe) / abs(avg_train_sharpe) * 100 if avg_train_sharpe != 0 else 0

    # Parameter stability
    top_n_counts = {}
    lb_counts = {}
    for f in fold_results:
        top_n_counts[f["best_top_n"]] = top_n_counts.get(f["best_top_n"], 0) + 1
        lb_counts[f["best_lookback"]] = lb_counts.get(f["best_lookback"], 0) + 1

    print(f"\n{'=' * 70}")
    print(f"  Walk-Forward 汇总")
    print(f"{'=' * 70}")
    print(f"  平均训练 Sharpe:  {avg_train_sharpe:.2f}")
    print(f"  平均测试 Sharpe:  {avg_test_sharpe:.2f}")
    print(f"  固定参数 Sharpe:  {avg_default_sharpe:.2f}")
    print(f"  过拟合退化率:     {degradation:.0f}%")
    print(f"\n  参数稳定性:")
    print(f"    top_n 分布:    {dict(sorted(top_n_counts.items()))}")
    print(f"    lookback 分布: {dict(sorted(lb_counts.items()))}")

    if degradation > 50:
        print(f"\n  ⚠ 过拟合退化 >50% — 策略在样本外大幅劣化，参数不可信")
    elif degradation > 25:
        print(f"\n  ⚠ 中度过拟合 — 谨慎使用最优参数")
    else:
        print(f"\n  ✓ 过拟合可控 — 参数相对稳健")

    # Save
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M")
    with open(OUT_DIR / f"walk_forward_{stamp}.json", "w") as f:
        json.dump({"folds": fold_results, "summary": {
            "avg_train_sharpe": avg_train_sharpe,
            "avg_test_sharpe": avg_test_sharpe,
            "avg_default_sharpe": avg_default_sharpe,
            "degradation_pct": degradation,
        }}, f, indent=2, default=str)
    print(f"\n  已保存: {OUT_DIR}/walk_forward_{stamp}.json\n")


if __name__ == "__main__":
    main()
