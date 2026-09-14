#!/usr/bin/env python3
"""Full distribution stats for the lifetime leverage cohort experiment.

Answers: "what is the EXPECTED outcome — the extreme cohorts are rare,
what represents the majority?" Computes mean/median/geometric-mean MOIC,
quartiles, P(losing money), P(beating 1x) per strategy, and states the
overlapping-sample caveat honestly.

Usage:
    poetry run python scripts/run_lifetime_stats.py
    poetry run python scripts/run_lifetime_stats.py --years 10
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd

from scripts.run_lifetime_leverage import build_universe, nav_for_leverage
from scripts.run_dca_us import xirr


def cohort_dca(nav: pd.Series, monthly: float) -> tuple[float, float]:
    """(moic, xirr) for a DCA into this NAV."""
    months = pd.date_range(nav.index[0].replace(day=1), nav.index[-1], freq="MS")
    units, invested, flows = 0.0, 0.0, []
    for m in months:
        pos = nav.index[nav.index >= m]
        if len(pos) == 0:
            continue
        px = float(nav.loc[pos[0]])
        units += monthly / px
        invested += monthly
        flows.append((pos[0], -monthly))
    final = units * float(nav.iloc[-1])
    flows.append((nav.index[-1], final))
    return final / invested, (xirr(flows) or 0.0)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--years", type=int, default=20)
    p.add_argument("--monthly", type=float, default=2000.0)
    args = p.parse_args()

    idx, r, fin = build_universe()
    last_cohort = int(idx.index[-1].year) - args.years

    strategies = {
        "1x": lambda rng: pd.Series(1.0, index=rng.index),
        "1.5x": lambda rng: pd.Series(1.5, index=rng.index),
        "2x": lambda rng: pd.Series(2.0, index=rng.index),
        "3x": lambda rng: pd.Series(3.0, index=rng.index),
        "glide2→1": lambda rng: pd.Series(
            2.0 - np.linspace(0, 1.0, len(rng)), index=rng.index
        ),
    }

    rows = {name: [] for name in strategies}
    for year in range(1972, last_cohort + 1):
        start = pd.Timestamp(f"{year}-01-01")
        end = pd.Timestamp(f"{year + args.years}-01-01")
        rng = r[(r.index >= start) & (r.index < end)]
        if len(rng) < args.years * 200:
            continue
        f_rng = fin[(fin.index >= start) & (fin.index < end)]
        for name, mk in strategies.items():
            nav = pd.Series(
                nav_for_leverage(rng, f_rng, mk(rng)).values, index=rng.index
            )
            moic, x = cohort_dca(nav, args.monthly)
            rows[name].append({"year": year, "moic": moic, "xirr": x})

    base = pd.DataFrame(rows["1x"]).set_index("year")["moic"]

    print("=" * 86)
    print(f"  入场队伍成绩分布  |  {args.years}年月投 · 1972-{last_cohort} 共{len(rows['1x'])}个队伍（相互重叠）")
    print("=" * 86)
    print(f"  {'策略':<9}{'均值':>7}{'中位数':>8}{'几何均值':>9}{'P25':>7}{'P75':>8}"
          f"{'亏钱概率':>9}{'胜过1x':>9}{'均值XIRR':>10}")
    for name, data in rows.items():
        df = pd.DataFrame(data)
        m = df["moic"]
        geo = float(np.exp(np.log(m).mean()))  # log-utility expectation
        lose = float((m < 1.0).mean()) * 100
        beat = float((m.values > base.values).mean()) * 100 if name != "1x" else float("nan")
        print(f"  {name:<9}{m.mean():>6.2f}x{m.median():>7.2f}x{geo:>8.2f}x"
              f"{m.quantile(0.25):>6.2f}x{m.quantile(0.75):>7.2f}x"
              f"{lose:>8.0f}%{beat:>8.0f}%{df['xirr'].mean()*100:>9.2f}%")

    n_ind = (last_cohort - 1972) // args.years + 1
    print(f"\n  ⚠ 样本独立性: {len(rows['1x'])} 个队伍共享同一市场历史，真正不重叠的")
    print(f"    {args.years}年窗口只有约 {n_ind} 个。上面的均值/分位数把同一段行情")
    print(f"    重复计了 {len(rows['1x'])/n_ind:.0f} 遍，置信区间远比表面看起来宽。")


if __name__ == "__main__":
    main()
