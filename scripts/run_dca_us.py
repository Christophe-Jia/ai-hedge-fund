#!/usr/bin/env python3
"""US ETF DCA backtest: fixed monthly buys on a chosen day-of-month.

Answers: "N years ago, invest $X every month on the ~Nth, what's my
return by now?" — for QQQ/VOO/SPY, with day-of-month sensitivity.

Costs: IBKR tiered commission (max($0.005/share, $1) per order) — on a
$2000 order that's the $1 minimum. Dividends: the default series accrues
an approximate dividend yield (total-return); --price-only disables.

Usage:
    poetry run python scripts/run_dca_us.py
    poetry run python scripts/run_dca_us.py --monthly 2000 --day 5 --symbols QQQ,VOO
    poetry run python scripts/run_dca_us.py --price-only
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from src.data.nasdaq_store import NasdaqDailyStore


def xirr(flows: list[tuple[pd.Timestamp, float]]) -> float | None:
    """Money-weighted annualized return (bisection). flows: (date, ±amount)."""
    if len(flows) < 2:
        return None
    t0 = flows[0][0]

    def npv(rate: float) -> float:
        return sum(a / ((1 + rate) ** ((t - t0).days / 365.25)) for t, a in flows)

    lo, hi = -0.9999, 10.0
    f_lo, f_hi = npv(lo), npv(hi)
    if f_lo * f_hi > 0:
        return None
    for _ in range(200):
        mid = (lo + hi) / 2
        f = npv(mid)
        if abs(f) < 1e-7:
            return mid
        if f_lo * f < 0:
            hi = f_hi = mid
        else:
            lo, f_lo = mid, f
    return (lo + hi) / 2


def run_dca(series: pd.Series, monthly: float, day: int,
            commission_per_order: float = 1.0) -> dict:
    """Buy `monthly` USD at the first trading day on/after `day` each month."""
    s = series.dropna()
    if s.empty:
        return {}
    # target dates: day-of-month of each month in the data range
    months = pd.date_range(s.index[0].replace(day=1), s.index[-1], freq="MS")
    invested, shares, costs, n_buys = 0.0, 0.0, 0.0, 0
    for m in months:
        target = m.replace(day=min(day, m.days_in_month))
        idx = s.index[s.index >= target]
        if len(idx) == 0:
            continue
        px = float(s.iloc[s.index.get_loc(idx[0])])
        if px <= 0:
            continue
        net = monthly - commission_per_order
        shares += net / px
        invested += monthly
        costs += commission_per_order
        n_buys += 1
    final = shares * float(s.iloc[-1])
    flows = []
    # rebuild flows for XIRR (one per purchase)
    for m in months:
        target = m.replace(day=min(day, m.days_in_month))
        idx = s.index[s.index >= target]
        if len(idx):
            flows.append((idx[0], -monthly))
    flows.append((s.index[-1], final))
    return {
        "months": n_buys,
        "invested": invested,
        "final": final,
        "moic": final / invested if invested else 0.0,
        "xirr": xirr(flows),
        "costs": costs,
    }


def main() -> None:
    p = argparse.ArgumentParser(description="US ETF monthly DCA backtest")
    p.add_argument("--monthly", type=float, default=2000.0)
    p.add_argument("--day", type=int, default=5, help="day of month (rolls forward)")
    p.add_argument("--symbols", type=str, default="QQQ,VOO,SPY")
    p.add_argument("--start", type=str, default="2016-09-12")
    p.add_argument("--end", type=str, default=None)
    p.add_argument("--price-only", action="store_true",
                   help="disable dividend accrual (price return only)")
    args = p.parse_args()
    end = args.end or datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

    store = NasdaqDailyStore(assetclass="etf")
    symbols = [s.strip().upper() for s in args.symbols.split(",")]

    print("=" * 70)
    print(f"  美股 ETF 定投回测  |  每月 ${args.monthly:,.0f} · {args.day}号(顺延到下一交易日)")
    print(f"  区间: {args.start} ~ {end}   |  {'纯价格' if args.price_only else '含股息再投资(近似)'}")
    print(f"  成本: IBKR 佣金 $1/笔")
    print("=" * 70)

    for sym in symbols:
        series = store.get_close_series(
            sym, args.start, end,
            div_yield_annual=0.0 if args.price_only else None,
        )
        r = run_dca(series, args.monthly, args.day)
        if not r:
            print(f"\n  {sym}: 无数据")
            continue
        years = r["months"] / 12.0
        print(f"\n  {sym}  ({r['months']} 个月, 累计投入 ${r['invested']:,.0f})")
        print(f"    期末市值   : ${r['final']:>12,.0f}")
        print(f"    净收益     : ${r['final'] - r['invested']:>12,.0f}")
        print(f"    收益倍数   : {r['moic']:>12.3f}x")
        print(f"    XIRR 年化  : {r['xirr'] * 100:>11.2f}%" if r["xirr"] is not None else "    XIRR      : n/a")

    # ---- day-of-month sensitivity (answers "随便找一天重要吗") ----------
    if len(symbols) >= 1:
        print("\n" + "-" * 70)
        print(f"  日期敏感性: 同样定投 {symbols[0]}，换不同的每月投入日")
        print(f"  {'投入日':<8}{'收益倍数':>10}{'XIRR':>10}")
        series = store.get_close_series(
            symbols[0], args.start, end,
            div_yield_annual=0.0 if args.price_only else None,
        )
        for d in (1, 5, 10, 15, 20, 25):
            r = run_dca(series, args.monthly, d)
            if r:
                x = f"{r['xirr']*100:.2f}%" if r["xirr"] is not None else "n/a"
                print(f"  {d}号{'':<5}{r['moic']:>9.3f}x{x:>10}")
        print()


if __name__ == "__main__":
    main()
