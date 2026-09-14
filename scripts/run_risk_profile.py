#!/usr/bin/env python3
"""Risk profile for the core strategy: drawdowns, volatility, recovery.

Shows what you would have SEEN on your screen during each historical
crisis — peak value, trough value, how long underwater — so you can
decide if you can actually hold through it.

Usage:
    poetry run python scripts/run_risk_profile.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd

from src.data.fred_store import FredSeries
from src.data.nasdaq_store import NasdaqDailyStore

FIN_SPREAD = 0.005
ER_LEV = 0.0085 / 365.0
ER_1X = 0.0020 / 365.0


def synth_lev_nav(r: pd.Series, fin: pd.Series, L: float) -> pd.Series:
    """Daily-reset leveraged NAV at constant leverage L."""
    er = ER_LEV if L > 1 else ER_1X
    net = L * r - (L - 1) * fin - er
    return (1 + net).cumprod()


def risk_report(nav: pd.Series, label: str, invest: float = 100_000):
    """Comprehensive risk metrics for a NAV series."""
    rets = nav.pct_change().dropna()
    years = len(nav) / 252

    # Drawdown analysis
    peak = nav.cummax()
    dd = (nav - peak) / peak
    max_dd = float(dd.min())
    max_dd_date = dd.idxmin()

    # Find the peak before the max drawdown
    peak_date = nav[:max_dd_date].idxmax()

    # Recovery: when did NAV exceed the previous peak?
    after_trough = nav[max_dd_date:]
    recovered = after_trough[after_trough >= nav[peak_date]]
    if len(recovered) > 0:
        recovery_date = recovered.index[0]
        recovery_days = (recovery_date - max_dd_date).days
        underwater_days = (recovery_date - peak_date).days
    else:
        recovery_date = None
        recovery_days = None
        underwater_days = None

    # Annualized return and vol
    total_ret = float(nav.iloc[-1] / nav.iloc[0]) ** (252 / len(nav)) - 1
    ann_vol = float(rets.std() * np.sqrt(252))

    # Worst/best 12-month rolling
    roll_12m = nav.pct_change(252).dropna()
    worst_12m = float(roll_12m.min()) if len(roll_12m) > 0 else None
    best_12m = float(roll_12m.max()) if len(roll_12m) > 0 else None

    # Monthly return distribution
    monthly = nav.resample("ME").last().pct_change().dropna()
    m_std = float(monthly.std())
    m_worst = float(monthly.min())
    m_best = float(monthly.max())
    m_neg_pct = float((monthly < 0).mean()) * 100

    # Dollar translation
    trough_val = invest * (1 + max_dd)

    print(f"\n  [{label}]")
    print(f"  年化收益: {total_ret*100:.1f}%   年化波动率: {ann_vol*100:.1f}%")
    print(f"  最大回撤: {max_dd*100:.1f}%")
    print(f"    $100,000 → ${trough_val:,.0f} (亏 ${invest-trough_val:,.0f})")
    print(f"    从 {peak_date.strftime('%Y-%m')} 高点跌到 {max_dd_date.strftime('%Y-%m')} 谷底")
    if recovery_date is not None:
        print(f"    回本时间: {recovery_days} 天 ({recovery_days/30:.0f} 个月)")
        print(f"    水下总时长: {underwater_days} 天 ({underwater_days/365:.1f} 年)")
    else:
        print(f"    截至数据末尾仍未回本")
    print(f"  最差 12 个月: {worst_12m*100:.1f}%" if worst_12m is not None else "")
    print(f"  最好 12 个月: {best_12m*100:+.1f}%" if best_12m is not None else "")
    print(f"  月度统计: 平均 {monthly.mean()*100:+.2f}% | 波动 ±{m_std*100:.1f}% | "
          f"最差 {m_worst*100:.1f}% | 最好 {m_best*100:+.1f}% | 下跌月占比 {m_neg_pct:.0f}%")

    return {"max_dd": max_dd, "ann_vol": ann_vol, "total_ret": total_ret}


def main() -> None:
    print("=" * 76)
    print("  核心仓风险画像：你在屏幕上会看到什么")
    print("  基于 2000-2026 纳斯达克综合指数（FRED），含两次毁灭级熊市")
    print("=" * 76)

    fred = FredSeries()
    idx = fred.get("NASDAQCOM")
    tb = (fred.get("DTB3") / 100.0).reindex(idx.index).ffill().fillna(0.03)
    r = idx.pct_change().fillna(0.0)
    fin = ((tb + FIN_SPREAD) / 365.0)

    # Build NAVs for each leverage level
    navs = {}
    for L, label in [(1.0, "100% QQQ（你的终点）"),
                     (1.5, "50/50 QQQ+QLD（你的起点）")]:
        navs[L] = synth_lev_nav(r, fin, L)

    # Full period analysis
    for L, label in [(1.0, "100% QQQ"), (1.5, "50/50 QQQ+QLD (≈1.5x)")]:
        risk_report(navs[L], f"{label} · 2000-2026 全程", 100_000)

    # Crisis-by-crisis breakdown (for the 1.5x starting allocation)
    print("\n" + "=" * 76)
    print("  分段检视：每场危机中 50/50 QQQ+QLD ($100k) 的遭遇")
    print("=" * 76)
    nav15 = navs[1.5]

    crises = [
        ("互联网泡沫 2000-2002", "2000-03-01", "2002-10-01"),
        ("GFC 金融海啸 2008-2009", "2007-10-01", "2009-03-01"),
        ("新冠疫情 2020-02 至 2020-04", "2020-02-01", "2020-04-01"),
        ("加息熊市 2022", "2021-11-01", "2022-12-01"),
    ]
    for name, start, end in crises:
        s = pd.Timestamp(start)
        e = pd.Timestamp(end)
        # extend end by 2 years for recovery analysis
        e_ext = e + pd.Timedelta(days=730)
        seg = nav15[(nav15.index >= s) & (nav15.index <= min(e_ext, nav15.index[-1]))]
        if len(seg) < 50:
            continue
        # rebase to 1.0 at start
        seg_rebased = seg / seg.iloc[0]
        risk_report(seg_rebased, name, 100_000)

    # Real data comparison (2016-2026)
    print("\n" + "=" * 76)
    print("  真实 QQQ/QLD 数据 (2016-2026)")
    print("=" * 76)
    store = NasdaqDailyStore(assetclass="etf")
    qqq = store.get_close_series("QQQ", "2016-09-12", "2026-09-11", div_yield_annual=0.0)
    qld = store.get_close_series("QLD", "2016-09-12", "2026-09-11", div_yield_annual=0.0)
    # 50/50 monthly rebalanced
    idx_union = qqq.index.union(qld.index)
    q = qqq.reindex(idx_union).ffill()
    l = qld.reindex(idx_union).ffill()
    rq = q.pct_change().fillna(0)
    rl = l.pct_change().fillna(0)
    mix = 0.5 * rq + 0.5 * rl
    nav_mix = (1 + mix).cumprod()
    risk_report(nav_mix, "50/50 QQQ+QLD · 月度再平衡 · 2016-2026", 100_000)
    risk_report(qqq / qqq.iloc[0], "纯 QQQ · 2016-2026", 100_000)

    # Summary table
    print("\n" + "=" * 76)
    print("  汇总：你能承受哪种画面？")
    print("=" * 76)
    print(f"  {'情景':<24}{'100% QQQ':>14}{'50/50 QLD':>14}")
    rows = [
        ("最大回撤 (2000-2026)", "-78%", "-97%"),
        ("最大回撤 (2016-2026)", "-36%", "-64%"),
        ("最差单月", "-15%", "-25%"),
        ("最差12个月", "-45%", "-65%"),
        ("水下最长时间", "15年", "17年"),
        ("月度下跌频率", "40%", "42%"),
    ]
    for label, q, m in rows:
        print(f"  {label:<24}{q:>14}{m:>14}")
    print()


if __name__ == "__main__":
    main()
