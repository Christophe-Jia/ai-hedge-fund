#!/usr/bin/env python3
"""Lifetime leverage under an UNKNOWN horizon: entry-cohort distribution.

The question: "I don't know when I'll need the money — for my finite
life, what maximizes returns?" Single backtests can't answer it; regime
luck dominates. Instead: start a 20-year $2000/month DCA in EVERY January
from 1972 to 2006 (36 cohorts), under several leverage policies, and look
at the DISTRIBUTION of outcomes — median, best, and the worst cohort
(the "bought the peak" scenario, your BTC fear).

Strategies (daily-reset leverage on the Nasdaq Composite, synthetic):
  1x     — pure index
  1.5x   — moderate constant leverage
  2x     — aggressive constant leverage (approx Kelly for equities)
  3x     — TQQQ-style
  glide  — 2x at start, linearly down to 1x by year 20 (lifecycle logic)

Costs: financing at (3M T-bill + 0.5%) on borrowed notional, 0.85%/yr ER
on leveraged legs. Price-only index (no dividends — conservative).

Usage:
    poetry run python scripts/run_lifetime_leverage.py
    poetry run python scripts/run_lifetime_leverage.py --years 20 --monthly 2000
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd

from src.data.fred_store import FredSeries

FIN_SPREAD = 0.005       # financing spread over T-bill
ER_LEV = 0.0085 / 365.0  # leveraged ETF expense ratio, daily
ER_1X = 0.0020 / 365.0   # index fund ER, daily


def build_universe():
    fred = FredSeries()
    idx = fred.get("NASDAQCOM")
    tb = (fred.get("DTB3") / 100.0).reindex(idx.index).ffill().fillna(0.03)
    r = idx.pct_change().fillna(0.0)
    fin_daily = (tb + FIN_SPREAD) / 365.0
    return idx, r, fin_daily


def nav_for_leverage(r: pd.Series, fin: pd.Series, L: pd.Series) -> pd.Series:
    """Daily-reset leveraged NAV: L(t) applies to day t's return."""
    net = L * r - (L - 1.0) * fin - np.where(L > 1.0, ER_LEV, ER_1X)
    return (1.0 + net).cumprod()


def dca_moic(nav: pd.Series, monthly: float) -> tuple[float, float]:
    """20-year DCA into this NAV. Returns (moic, final_per_dollar_invested)."""
    months = pd.date_range(nav.index[0], nav.index[-1], freq="MS")
    units, invested = 0.0, 0.0
    for m in months:
        pos = nav.index[nav.index >= m]
        if len(pos) == 0:
            continue
        px = float(nav.loc[pos[0]])
        units += monthly / px
        invested += monthly
    return (units * float(nav.iloc[-1])) / invested, units * float(nav.iloc[-1])


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--years", type=int, default=20)
    p.add_argument("--monthly", type=float, default=2000.0)
    p.add_argument("--first-cohort", type=int, default=1972)
    args = p.parse_args()

    idx, r, fin = build_universe()
    last_cohort = int(idx.index[-1].year) - args.years

    strategies = {
        "1x": lambda s, e, rng: pd.Series(1.0, index=rng),
        "1.5x": lambda s, e, rng: pd.Series(1.5, index=rng),
        "2x": lambda s, e, rng: pd.Series(2.0, index=rng),
        "3x": lambda s, e, rng: pd.Series(3.0, index=rng),
        "glide2→1": lambda s, e, rng: pd.Series(
            2.0 - np.linspace(0, 1.0, len(rng)), index=rng
        ),
    }

    cohorts = list(range(args.first_cohort, last_cohort + 1))
    results = {name: [] for name in strategies}
    worst_cohort = {}
    best_cohort = {}

    for year in cohorts:
        start = pd.Timestamp(f"{year}-01-01")
        end = pd.Timestamp(f"{year + args.years}-01-01")
        rng = r[(r.index >= start) & (r.index < end)]
        if len(rng) < args.years * 200:
            continue
        f_rng = fin[(fin.index >= start) & (fin.index < end)]
        for name, mk in strategies.items():
            nav = nav_for_leverage(rng, f_rng, mk(start, end, rng.index))
            nav = pd.Series(nav.values, index=rng.index)  # start at 1.0
            moic, _ = dca_moic(nav, args.monthly)
            results[name].append((year, moic))

    print("=" * 76)
    print(f"  入场合伙人实验  |  每年1月入场 · {args.years}年月投 ${args.monthly:,.0f} · "
          f"{cohorts[0]}-{last_cohort} 共{len(results['1x'])}个队伍")
    print(f"  合成杠杆: 日重置 · 融资=3月国库券+{FIN_SPREAD*100:.1f}% · 价格指数(不含股息)")
    print("=" * 76)

    header = f"  {'策略':<10}{'中位MOIC':>10}{'最好':>9}{'最差':>9}{'P10':>9}"
    print(header)
    for name, rows in results.items():
        if not rows:
            continue
        moics = pd.Series({y: m for y, m in rows})
        med, mx, mn = moics.median(), moics.max(), moics.min()
        p10 = moics.quantile(0.10)
        worst_cohort[name] = (moics.idxmin(), mn)
        best_cohort[name] = (moics.idxmax(), mx)
        print(f"  {name:<10}{med:>9.2f}x{mx:>8.2f}x{mn:>8.2f}x{p10:>8.2f}x")

    print(f"\n  最差队伍(买到山顶的那批人):")
    for name in strategies:
        if name in worst_cohort:
            y, m = worst_cohort[name]
            print(f"    {name:<10} {y} 年入场 → {m:.2f}x")

    print(f"\n  对照: 同期每个队伍总共投入 ${args.monthly*12*args.years:,.0f}")
    print(f"  1x 的最差队伍若满仓持有到期不动(非DCA): 略 — DCA已含下跌中持续买入")
    print()


if __name__ == "__main__":
    main()
