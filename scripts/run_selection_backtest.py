#!/usr/bin/env python3
"""M1 momentum selection backtest: S&P 100 universe vs QQQ/VOO/SPY.

12-1 momentum ranking, monthly rebalance, top-N equal weight, IBKR costs.
Signal at month-end close, execution at next trading day's close (no
look-ahead). Benchmarks are total-return approximations (price + accrued
dividend yield).

Usage:
    poetry run python scripts/run_selection_backtest.py
    poetry run python scripts/run_selection_backtest.py --top-n 5 --freq ME
    poetry run python scripts/run_selection_backtest.py --no-costs
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from src.data.nasdaq_store import NasdaqDailyStore
from src.selection import IbkrCostModel, SelectionBacktest, SelectionConfig

UNIVERSE_PATH = Path(__file__).resolve().parents[1] / "data" / "universe" / "sp100.json"
OUT_DIR = Path(__file__).resolve().parents[1] / "outputs" / "selection"


def load_universe(mode: str = "today") -> list[str] | dict[int, list[str]]:
    """Load universe: 'today' = current list; 'pit' = point-in-time by year."""
    uni_dir = Path(__file__).resolve().parents[1] / "data" / "universe"
    # Old ticker -> current ticker (price data lives under the new symbol)
    RENAME = {
        "FB": "META", "PCLN": "BKNG", "UTX": "RTX", "RTN": "RTX",
        "BK": "BNY", "DWDP": "DD", "TWX": "T", "CELG": "BMY",
        "MON": "BAYRY", "AGN": "ABBV", "HON": "HON", "KHC": "KHC",
    }
    if mode == "pit":
        pit = {}
        for f in sorted(uni_dir.glob("sp100_*.json")):
            if f.name == "sp100_union.json":
                continue
            year = int(f.stem.split("_")[1])
            syms = [RENAME.get(c["symbol"], c["symbol"]) for c in json.loads(f.read_text())]
            pit[year] = syms
        return pit
    with open(uni_dir / "sp100.json") as f:
        return [c["symbol"] for c in json.load(f)]


def load_closes(store: NasdaqDailyStore, symbols: list[str],
                start: str, end: str) -> pd.DataFrame:
    frames = {}
    for sym in symbols:
        df = store.get_daily(sym, start, end)
        if not df.empty:
            frames[sym] = df["close"]
    return pd.DataFrame(frames)


def bench_stats(series: pd.Series) -> dict:
    if series.empty or len(series) < 2:
        return {}
    ret = series.iloc[-1] / series.iloc[0] - 1.0
    days = (series.index[-1] - series.index[0]).days
    cagr = (1.0 + ret) ** (365.25 / max(days, 1)) - 1.0
    dd = (series / series.cummax() - 1.0).min()
    return {"total_return": ret * 100.0, "cagr": cagr * 100.0, "max_dd": dd * 100.0}


def main() -> None:
    p = argparse.ArgumentParser(description="S&P 100 momentum selection backtest")
    p.add_argument("--top-n", type=int, default=10)
    p.add_argument("--freq", type=str, default="ME", help="pandas offset alias (ME/WE)")
    p.add_argument("--start", type=str, default="2016-09-12")
    p.add_argument("--end", type=str, default=None)
    p.add_argument("--no-costs", action="store_true")
    p.add_argument("--pit", action="store_true",
                   help="point-in-time universe (year-specific constituents)")
    p.add_argument("--capital", type=float, default=100_000.0)
    args = p.parse_args()
    end = args.end or datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

    universe = load_universe("pit" if args.pit else "today")
    if isinstance(universe, dict):
        all_symbols = sorted(set().union(*universe.values()))
        uni_note = f"point-in-time ({len(universe)} 年度名单, 联合 {len(all_symbols)} 只)"
    else:
        all_symbols = universe
        uni_note = f"今日名单 ({len(universe)} 只)"

    stocks = NasdaqDailyStore(assetclass="stocks")
    etfs = NasdaqDailyStore(assetclass="etf")

    print(f"Loading {len(all_symbols)} stocks ... ({uni_note})")
    closes = load_closes(stocks, all_symbols, args.start, end)
    print(f"  price matrix: {closes.shape[0]} days x {closes.shape[1]} symbols")

    cost_model = IbkrCostModel() if not args.no_costs else IbkrCostModel(
        commission_per_share=0.0, min_commission_per_order=0.0, spread_bps=0.0
    )
    cfg = SelectionConfig(
        universe=universe,
        start_date=args.start,
        end_date=end,
        rebalance_freq=args.freq,
        top_n=args.top_n,
        initial_capital=args.capital,
        cost_model=cost_model,
    )
    result = SelectionBacktest(closes, cfg).run()

    # effective backtest span (from first rebalance)
    if result.holdings:
        eff_start = result.holdings[0][0]
    else:
        eff_start = result.equity.index[0]
    eff_start_str = str(eff_start.date())

    # benchmarks over the same effective window (total-return approx)
    benchmarks = {}
    for sym in ("QQQ", "VOO", "SPY"):
        s = etfs.get_close_series(sym, eff_start_str, end)  # div-accrued
        benchmarks[sym] = s

    m = result.metrics
    strat_days = (result.equity.index[-1] - result.equity.index[0]).days
    strat_cagr = ((1 + m.get("total_return", 0) / 100.0) ** (365.25 / max(strat_days, 1)) - 1) * 100

    print("\n" + "=" * 66)
    print(f"  S&P 100 动量选股回测  |  12-1动量 · {args.freq}调仓 · Top{args.top_n}等权")
    print(f"  区间: {eff_start_str} ~ {str(result.equity.index[-1].date())}"
          f"  ({strat_days/365.25:.1f} 年, {len(result.holdings)} 次调仓)")
    print(f"  成本: {'IBKR 佣金+5bps价差' if not args.no_costs else '无(理想化)'}")
    print("=" * 66)
    print(f"\n  期末市值        : {result.final_value:>12,.0f}")
    print(f"  总收益率        : {m.get('total_return', 0):>11.2f}%")
    print(f"  年化 CAGR       : {strat_cagr:>11.2f}%")
    print(f"  Sharpe (rf=4.3%): {m.get('sharpe_ratio') or 0:>11.2f}")
    print(f"  最大回撤        : {m.get('max_drawdown') or 0:>11.2f}%")
    print(f"  Calmar          : {m.get('calmar_ratio') or 0:>11.2f}")
    print(f"  累计交易成本    : {result.total_costs:>12,.0f}")
    if result.turnover_notional:
        avg_to = sum(result.turnover_notional) / len(result.turnover_notional)
        print(f"  平均单次换手(单边): {avg_to:>11,.0f}")

    print(f"\n  {'基准':<8}{'总收益':>10}{'CAGR':>9}{'最大回撤':>10}")
    for sym, s in benchmarks.items():
        b = bench_stats(s)
        if not b:
            print(f"  {sym:<8}{'(无数据)':>10}")
            continue
        print(f"  {sym:<8}{b['total_return']:>9.1f}%{b['cagr']:>8.1f}%{b['max_dd']:>9.1f}%")

    print("\n  最近一次持仓:", ", ".join(result.holdings[-1][1]) if result.holdings else "-")

    # ---------------------------------------------------------------- save
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M")
    result.equity.to_csv(OUT_DIR / f"equity_{stamp}.csv")
    with open(OUT_DIR / f"summary_{stamp}.json", "w") as f:
        json.dump({
            "config": {"top_n": args.top_n, "freq": args.freq,
                       "start": args.start, "end": end,
                       "costs": not args.no_costs},
            "metrics": {k: (v if not hasattr(v, "item") else v.item()) for k, v in m.items()},
            "cagr": strat_cagr,
            "total_costs": result.total_costs,
            "holdings": [(str(d.date()), syms) for d, syms in result.holdings],
            "benchmarks": {s: bench_stats(b) for s, b in benchmarks.items()},
        }, f, indent=2, default=str)
    print(f"\n  已保存: {OUT_DIR}/summary_{stamp}.json + equity_{stamp}.csv\n")


if __name__ == "__main__":
    main()
