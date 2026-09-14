#!/usr/bin/env python3
"""Signal 2: BTC funding rate extreme → equity risk signal.

Hypothesis: when BTC funding rates are extremely positive (over-levered
longs), the crypto market will de-leverage, spilling over to risk assets
(QQQ, NVDA, COIN). Conversely, extremely negative funding (over-levered
shorts) may signal capitulation bottoms.

Usage:
    poetry run python scripts/backtest_funding_signal.py
    poetry run python scripts/backtest_funding_signal.py --high 50 --low -20
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd

from src.data.nasdaq_store import NasdaqDailyStore


def load_funding() -> pd.DataFrame:
    """BTC/USDT:USDT funding rates (8h)."""
    con = sqlite3.connect("file:data/btc_history.db?mode=ro", uri=True)
    df = pd.read_sql(
        "SELECT ts, rate FROM funding_rates WHERE symbol='BTC/USDT:USDT'",
        con
    )
    con.close()
    df["date"] = pd.to_datetime(df["ts"], unit="ms").dt.tz_localize(None).dt.normalize()
    df["rate_annualized"] = df["rate"] * 3 * 365 * 100  # % per year
    return df


def load_stock(symbol: str, start: str = "2023-01-01") -> pd.DataFrame:
    """Stock daily returns."""
    store = NasdaqDailyStore(assetclass="stocks" if symbol not in ("QQQ","SPY","VOO") else "etf")
    df = store.get_daily(symbol, start, "2026-09-12")
    df.index = df.index.tz_localize(None)
    return df


def daily_funding(funding: pd.DataFrame) -> pd.DataFrame:
    """Aggregate 8h funding to daily average."""
    return funding.groupby("date").agg(
        rate_avg=("rate_annualized", "mean"),
        rate_max=("rate_annualized", "max"),
        rate_min=("rate_annualized", "min"),
    ).reset_index()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--high", type=float, default=50.0, help="extreme high threshold (annualized pct)")
    p.add_argument("--low", type=float, default=-20.0, help="extreme low threshold (annualized pct)")
    p.add_argument("--window", type=int, default=3, help="rolling avg days")
    args = p.parse_args()

    print("=" * 76)
    print(f"  信号二验证：BTC 资金费率极端 → 美股风险")
    print(f"  高杠杆阈值: {args.high}% 年化 | 低杠杆/空头挤压: {args.low}% 年化")
    print(f"  滚动窗口: {args.window} 天平均")
    print("=" * 76)

    funding = load_funding()
    daily = daily_funding(funding)

    # Rolling average funding
    daily["rate_ma"] = daily["rate_avg"].rolling(args.window).mean()

    # Show distribution
    print(f"\n资金费率分布 ({len(daily)} 天):")
    print(f"  中位数: {daily['rate_avg'].median():+.1f}%")
    print(f"  75分位: {daily['rate_avg'].quantile(0.75):+.1f}%")
    print(f"  90分位: {daily['rate_avg'].quantile(0.90):+.1f}%")
    print(f"  95分位: {daily['rate_avg'].quantile(0.95):+.1f}%")
    print(f"  99分位: {daily['rate_avg'].quantile(0.99):+.1f}%")

    # Flag extreme days
    daily["extreme_high"] = daily["rate_ma"] > args.high
    daily["extreme_low"] = daily["rate_ma"] < args.low

    n_high = daily["extreme_high"].sum()
    n_low = daily["extreme_low"].sum()
    print(f"\n极端高杠杆 (> {args.high}%): {n_high} 天 ({n_high/len(daily)*100:.0f}%)")
    print(f"极端低/空头 (> {args.low}%): {n_low} 天 ({n_low/len(daily)*100:.0f}%)")

    # Test against stocks
    stocks = [("QQQ", "etf"), ("COIN", "stocks"), ("MSTR", "stocks"), ("NVDA", "stocks")]

    for sym, _ in stocks:
        try:
            stock = load_stock(sym)
        except Exception:
            print(f"\n  {sym}: 无数据")
            continue

        stock["ret_1d"] = stock["close"].shift(-1) / stock["close"] - 1
        stock["ret_3d"] = stock["close"].shift(-3) / stock["close"] - 1
        stock["ret_5d"] = stock["close"].shift(-5) / stock["close"] - 1
        stock["ret_10d"] = stock["close"].shift(-10) / stock["close"] - 1
        stock["ret_20d"] = stock["close"].shift(-20) / stock["close"] - 1

        # Merge with funding
        merged = pd.merge(
            daily[["date", "rate_ma", "extreme_high", "extreme_low"]],
            stock[["ret_1d", "ret_3d", "ret_5d", "ret_10d", "ret_20d"]],
            left_on="date", right_index=True, how="inner"
        )

        # Remove consecutive signals (keep first trigger)
        merged["new_high"] = merged["extreme_high"] & ~merged["extreme_high"].shift(1, fill_value=False)
        merged["new_low"] = merged["extreme_low"] & ~merged["extreme_low"].shift(1, fill_value=False)

        n_high_events = merged["new_high"].sum()
        n_low_events = merged["new_low"].sum()

        if n_high_events < 2:
            print(f"\n  [{sym}] 高杠杆触发不足 ({n_high_events} 次)")
            continue

        print(f"\n  [{sym}] ({len(merged)} 天, 高杠杆{n_high_events}次, 低杠杆{n_low_events}次)")

        # High funding → expect negative returns (de-leverage)
        high = merged[merged["new_high"]]
        for horizon, label in [("ret_1d", "1天"), ("ret_5d", "5天"),
                                 ("ret_10d", "10天"), ("ret_20d", "20天")]:
            r = high[horizon].dropna() * 100
            if len(r) < 2:
                continue
            sig = "显著" if abs(r.mean()) > 2 * r.std() / np.sqrt(len(r)) else ""
            print(f"    高杠杆后{label}:  平均 {r.mean():+.1f}% | "
                  f"胜率 {(r > 0).mean()*100:.0f}% {sig}")

        # Low/negative funding → expect positive returns (short squeeze)
        if n_low_events >= 2:
            low = merged[merged["new_low"]]
            for horizon, label in [("ret_1d", "1天"), ("ret_5d", "5天"), ("ret_10d", "10天")]:
                r = low[horizon].dropna() * 100
                if len(r) < 2:
                    continue
                print(f"    低杠杆后{label}:  平均 {r.mean():+.1f}% | "
                      f"胜率 {(r > 0).mean()*100:.0f}%")

        # Baseline
        normal = merged[~merged["extreme_high"] & ~merged["extreme_low"]]
        base_r5 = normal["ret_5d"].dropna() * 100
        print(f"    基准 5天:  平均 {base_r5.mean():+.1f}%")

        # Correlation between funding level and forward returns
        valid = merged[["rate_ma", "ret_5d"]].dropna()
        if len(valid) > 50:
            corr = valid["rate_ma"].corr(valid["ret_5d"])
            print(f"    资金费率↔5天后回报相关性: {corr:.3f}")

    print()


if __name__ == "__main__":
    main()
