#!/usr/bin/env python3
"""Signal 1: Weekend BTC move → Monday crypto-stock gap.

Hypothesis: BTC trades 24/7, stocks only 6.5h/day. When BTC makes a
large move over the weekend, crypto-related stocks (COIN, MSTR, MARA)
should gap at Monday open. The question: is the gap predictable, and
does it over- or under-react?

Usage:
    poetry run python scripts/backtest_weekend_gap.py
    poetry run python scripts/backtest_weekend_gap.py --threshold 5
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd

from src.data.nasdaq_store import NasdaqDailyStore
from src.data.historical_store import HistoricalOHLCVStore


def load_btc_daily() -> pd.DataFrame:
    """BTC/USDT spot daily — trades 7 days/week."""
    store = HistoricalOHLCVStore(allow_fetch=False)
    import sqlite3, datetime
    con = sqlite3.connect("file:data/btc_history.db?mode=ro", uri=True)
    df = pd.read_sql(
        "SELECT ts, open, high, low, close FROM ohlcv "
        "WHERE symbol='BTC/USDT' AND market_type='spot' AND timeframe='1d'",
        con
    )
    con.close()
    df["date"] = pd.to_datetime(df["ts"], unit="ms")
    return df.set_index("date").sort_index()


def load_stock_daily(symbol: str) -> pd.DataFrame:
    """Stock daily from Nasdaq store (weekdays only) — timezone-naive index."""
    store = NasdaqDailyStore(assetclass="stocks")
    df = store.get_daily(symbol, "2016-09-12", "2026-09-12")
    # Strip timezone to match BTC data (tz-naive from SQLite)
    df.index = df.index.tz_localize(None)
    return df


def compute_weekend_signals(btc: pd.DataFrame, threshold: float = 5.0):
    """For each weekend, compute BTC return and flag if > threshold."""
    results = []
    # Group by ISO week
    btc["week"] = btc.index.isocalendar().week.astype(int)
    btc["year"] = btc.index.year
    btc["dow"] = btc.index.dayofweek  # 0=Mon, 4=Fri, 5=Sat, 6=Sun

    for (year, week), grp in btc.groupby(["year", "week"]):
        fri = grp[grp["dow"] == 4]
        sat = grp[grp["dow"] == 5]
        sun = grp[grp["dow"] == 6]

        if len(fri) == 0 or len(sun) == 0:
            continue

        fri_close = float(fri["close"].iloc[-1])
        sun_close = float(sun["close"].iloc[-1])
        if fri_close <= 0:
            continue

        wknd_ret = (sun_close / fri_close - 1) * 100
        fri_date = fri.index[-1]

        results.append({
            "year": year,
            "week": week,
            "friday": fri_date,
            "btc_weekend_ret": wknd_ret,
            "flagged": abs(wknd_ret) >= threshold,
            "direction": "up" if wknd_ret > 0 else "down" if wknd_ret < 0 else "flat",
        })

    return pd.DataFrame(results)


def compute_stock_gaps(stock: pd.DataFrame, weekends: pd.DataFrame) -> pd.DataFrame:
    """For each weekend, compute Monday gap for a stock."""
    stock = stock.copy()
    stock["friday_close"] = stock["close"].shift(1)  # previous trading day close
    stock["gap_pct"] = (stock["open"] / stock["friday_close"] - 1) * 100
    # Forward returns after gap
    stock["ret_1d"] = stock["close"].shift(-1) / stock["close"] - 1
    stock["ret_3d"] = stock["close"].shift(-3) / stock["close"] - 1
    stock["ret_5d"] = stock["close"].shift(-5) / stock["close"] - 1
    # Gap fill (does price return to Friday close?)
    stock["gap_fill_1d"] = (stock["close"].shift(-1) / stock["friday_close"] - 1) * 100

    # Match each weekend to the next Monday (or next trading day)
    results = []
    stock_dates = stock.index
    for _, row in weekends.iterrows():
        fri = row["friday"]
        # Find the next trading day after Friday
        next_days = stock_dates[stock_dates > fri]
        if len(next_days) == 0:
            continue
        monday = next_days[0]
        stock_row = stock.loc[monday]
        results.append({
            "friday": fri,
            "monday": monday,
            "btc_weekend_ret": row["btc_weekend_ret"],
            "flagged": row["flagged"],
            "direction": row["direction"],
            "gap_pct": float(stock_row["gap_pct"]) if pd.notna(stock_row["gap_pct"]) else np.nan,
            "ret_1d": float(stock_row["ret_1d"]) if pd.notna(stock_row["ret_1d"]) else np.nan,
            "ret_3d": float(stock_row["ret_3d"]) if pd.notna(stock_row["ret_3d"]) else np.nan,
            "ret_5d": float(stock_row["ret_5d"]) if pd.notna(stock_row["ret_5d"]) else np.nan,
            "gap_fill_1d": float(stock_row["gap_fill_1d"]) if pd.notna(stock_row["gap_fill_1d"]) else np.nan,
        })
    return pd.DataFrame(results)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--threshold", type=float, default=5.0,
                   help="BTC weekend move threshold to flag (%%)")
    args = p.parse_args()

    print("=" * 76)
    print(f"  信号一验证：周末 BTC 变动 > {args.threshold}% → 周一加密股跳空")
    print("=" * 76)

    btc = load_btc_daily()
    print(f"\nBTC 数据: {len(btc)} 天 ({btc.index[0].date()} ~ {btc.index[-1].date()})")

    weekends = compute_weekend_signals(btc, args.threshold)
    n_flagged = weekends["flagged"].sum()
    print(f"周末总数: {len(weekends)} | 触发: {n_flagged} "
          f"({n_flagged/len(weekends)*100:.0f}%)")

    stocks = ["COIN", "MSTR", "MARA", "RIOT"]
    for sym in stocks:
        try:
            stock = load_stock_daily(sym)
        except Exception:
            print(f"\n  {sym}: 无数据，跳过")
            continue

        gaps = compute_stock_gaps(stock, weekends)
        flagged = gaps[gaps["flagged"]]
        normal = gaps[~gaps["flagged"]]

        if len(flagged) < 3:
            print(f"\n  {sym}: 触发样本不足 ({len(flagged)} 次)")
            continue

        print(f"\n  [{sym}] ({len(stock)} 天数据, {len(flagged)} 次触发)")

        # Overall stats
        for label, mask in [("BTC大涨→做多", flagged["direction"] == "up"),
                              ("BTC大跌→做空", flagged["direction"] == "down")]:
            subset = flagged[mask]
            if len(subset) < 2:
                continue
            gap = subset["gap_pct"]
            r1 = subset["ret_1d"] * 100
            r5 = subset["ret_5d"] * 100

            print(f"    {label} ({len(subset)} 次):")
            print(f"      周一 gap:  平均 {gap.mean():+.1f}% | 中位 {gap.median():+.1f}%")
            print(f"      gap 后1天:  平均 {r1.mean():+.1f}% | 胜率 {(r1 > 0).mean()*100:.0f}%")
            print(f"      gap 后5天:  平均 {r5.mean():+.1f}% | 胜率 {(r5 > 0).mean()*100:.0f}%")

        # Baseline comparison
        base_gap = normal["gap_pct"]
        base_r1 = normal["ret_1d"] * 100
        print(f"    基准 (无大变动周末):")
        print(f"      周一 gap:  平均 {base_gap.mean():+.1f}%")
        print(f"      后1天:    平均 {base_r1.mean():+.1f}%")

        # Signal quality: gap correlation with BTC weekend return
        corr = flagged["btc_weekend_ret"].corr(flagged["gap_pct"])
        print(f"    BTC周末收益 ↔ 周一gap 相关性: {corr:.3f}")

        # Over/under reaction
        # If gap is in BTC direction and then reverses → over-reaction
        same_dir = flagged[(
            (flagged["btc_weekend_ret"] > 0) & (flagged["gap_pct"] > 0) |
            (flagged["btc_weekend_ret"] < 0) & (flagged["gap_pct"] < 0)
        )]
        if len(same_dir) >= 3:
            fill = same_dir["gap_fill_1d"]
            reverses = (fill * same_dir["gap_pct"] < 0).mean() * 100
            print(f"    gap后1天回补率: {reverses:.0f}% (回补=过度反应)")

    print()


if __name__ == "__main__":
    main()
