#!/usr/bin/env python3
"""Three-tier leverage switching: QQQ / QLD / TQQQ under daily rules.

Question: "can some rule switching between QQQ/QLD/TQQQ beat static
buy-and-hold for a monthly DCA?" Tests four principled rules across
three eras and reports ALL results, winners and losers alike — cherry-
picking the best cell of this grid would be data mining.

Rules (signal at day t close, effective day t+1, 3-day confirmation
before switching, 10 bps per switch — both legs):
  static1x/2x/3x  — always in one fund (baselines)
  ema-tier        — EMA50>EMA200 & price>EMA200 -> TQQQ; price>EMA200 -> QLD; else QQQ
  ema-tier-cash   — same, but bear tier = cash instead of QQQ
  vol-target      — trailing 63d vol, L = clip(round(20%/vol), 1, 3)  (Moreira-Muir)
  mom-tier        — 12-1 momentum: >+20% -> 3x, >0 -> 2x, else 1x

Usage:
    poetry run python scripts/run_tier_switch_experiment.py            # all 3 windows
    poetry run python scripts/run_tier_switch_experiment.py --window real
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd

from src.data.fred_store import FredSeries
from src.data.nasdaq_store import NasdaqDailyStore
from scripts.run_dca_us import run_dca

SWITCH_COST_BPS = 10.0   # both legs per switch
CONFIRM_DAYS = 3          # days a new tier must persist before switching
FIN_SPREAD = 0.005
ER_LEV = 0.0085 / 365.0
ER_1X = 0.0020 / 365.0


# ---------------------------------------------------------------------------
# Legs
# ---------------------------------------------------------------------------

def legs_fred(start: str, end: str) -> tuple[pd.Series, dict[int, pd.Series]]:
    """(1x price, {L: daily return}) from the Nasdaq Composite."""
    fred = FredSeries()
    idx = fred.get("NASDAQCOM")
    idx = idx[(idx.index >= start) & (idx.index <= end)]
    r = idx.pct_change().fillna(0.0)
    tb = (fred.get("DTB3") / 100.0).reindex(idx.index).ffill().fillna(0.03)
    fin = ((tb + FIN_SPREAD) / 365.0)

    def synth(L: int) -> pd.Series:
        er = ER_LEV if L > 1 else ER_1X
        return L * r - (L - 1) * fin - er

    rets = {L: synth(L) for L in (1, 2, 3)}
    rets[0] = pd.Series(0.0, index=idx.index)  # cash leg
    return idx, rets


def legs_real(start: str, end: str) -> tuple[pd.Series, dict[int, pd.Series]]:
    """Real QQQ/QLD/TQQQ daily returns (price-only, consistent across legs)."""
    store = NasdaqDailyStore(assetclass="etf")
    px = {}
    for L, sym in ((1, "QQQ"), (2, "QLD"), (3, "TQQQ")):
        px[L] = store.get_close_series(sym, start, end, div_yield_annual=0.0)
    idx_union = px[1].index.union(px[2].index).union(px[3].index)
    for L in px:
        px[L] = px[L].reindex(idx_union).ffill()
    rets = {L: px[L].pct_change().fillna(0.0) for L in (1, 2, 3)}
    rets[0] = pd.Series(0.0, index=idx_union)
    return px[1], rets


# ---------------------------------------------------------------------------
# Rules -> raw tier series
# ---------------------------------------------------------------------------

def rule_static(L: int) -> callable:
    return lambda px: pd.Series(L, index=px.index)


def rule_ema_tier(px: pd.Series, cash: bool = False) -> pd.Series:
    ema200 = px.ewm(span=200, adjust=False).mean()
    ema50 = px.ewm(span=50, adjust=False).mean()
    up = px > ema200
    strong = up & (ema50 > ema200)
    tier = pd.Series(1, index=px.index)
    tier[up & ~strong] = 2
    tier[strong] = 3
    if cash:
        tier[~up] = 0
    return tier


def rule_vol_target(px: pd.Series, target: float = 0.20) -> pd.Series:
    vol = px.pct_change().rolling(63, min_periods=63).std() * np.sqrt(252)
    L = (target / vol).round().clip(1, 3)
    return L.fillna(1).astype(int)


def rule_mom_tier(px: pd.Series) -> pd.Series:
    mom = px.shift(21) / px.shift(252) - 1.0
    tier = pd.Series(1, index=px.index)
    tier[mom > 0.0] = 2
    tier[mom > 0.20] = 3
    return tier.astype(int)


# ---------------------------------------------------------------------------
# Simulation: confirmed tier switching NAV
# ---------------------------------------------------------------------------

def simulate(rets: dict[int, pd.Series], raw: pd.Series) -> tuple[pd.Series, int, dict]:
    """NAV of a fund that follows the confirmed tier; returns
    (nav, switches, time_in_tier_pct)."""
    idx = raw.index
    cur = int(raw.iloc[0])
    streak = 0
    eff = []           # tier effective on each day (decided from data up to prev day)
    switches = 0
    tier_days = {0: 0, 1: 0, 2: 0, 3: 0}

    for i in range(len(idx)):
        eff.append(cur)
        tier_days[cur] += 1
        # after day i's close, maybe queue a switch (effective from day i+1)
        raw_i = int(raw.iloc[i])
        if raw_i != cur:
            streak += 1
            if streak >= CONFIRM_DAYS:
                cur = raw_i
                streak = 0
                switches += 1
        else:
            streak = 0

    eff = pd.Series(eff, index=idx)
    nav_vals = [1.0]
    nav = 1.0
    r1 = rets[1]
    for i in range(1, len(idx)):
        L = int(eff.iloc[i - 1])  # yesterday's confirmed tier earns today's return
        nav *= 1.0 + float(rets[L].iloc[i])
        if int(eff.iloc[i]) != L:
            nav -= nav * SWITCH_COST_BPS / 10_000.0
        nav_vals.append(nav)

    nav_s = pd.Series(nav_vals, index=idx)
    total = len(idx)
    time_pct = {k: v / total * 100 for k, v in tier_days.items() if v > 0}
    return nav_s, switches, time_pct


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--window", choices=["all", "fred-2000-2010", "fred-2000-2026", "real"],
                   default="all")
    p.add_argument("--monthly", type=float, default=2000.0)
    p.add_argument("--day", type=int, default=5)
    args = p.parse_args()
    end = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

    windows = []
    if args.window in ("all", "fred-2000-2010"):
        windows.append(("2000-2010 敌意十年 (FRED代理)", lambda: legs_fred("2000-01-01", "2010-01-01")))
    if args.window in ("all", "fred-2000-2026"):
        windows.append(("2000-2026 全程 (FRED代理)", lambda: legs_fred("2000-01-01", end)))
    if args.window in ("all", "real"):
        windows.append(("2016-2026 真实数据", lambda: legs_real("2016-09-12", end)))

    rules = [
        ("纯QQQ 1x", rule_static(1)),
        ("纯QLD 2x", rule_static(2)),
        ("纯TQQQ 3x", rule_static(3)),
        ("EMA三档", rule_ema_tier),
        ("EMA三档(熊市现金)", lambda px: rule_ema_tier(px, cash=True)),
        ("波动率目标20%", rule_vol_target),
        ("动量三档", rule_mom_tier),
    ]

    for title, legs_fn in windows:
        px, rets = legs_fn()
        print("=" * 88)
        print(f"  {title}  |  月投 ${args.monthly:,.0f} · {args.day}号 · 3日确认 · {SWITCH_COST_BPS:.0f}bps/换仓")
        print("=" * 88)
        print(f"  {'规则':<16}{'倍数':>9}{'XIRR':>9}{'NAV回撤':>9}{'换仓':>7}{'时间分配':>22}")
        for name, mk in rules:
            raw = mk(px)
            nav, switches, tiers = simulate(rets, raw)
            r = run_dca(nav, args.monthly, args.day)
            dd = float((nav / nav.cummax() - 1).min()) * 100
            x = f"{r['xirr']*100:.1f}%" if r["xirr"] is not None else "-"
            tier_str = " ".join(f"{k}x:{v:.0f}%" for k, v in sorted(tiers.items()))
            print(f"  {name:<16}{r['moic']:>8.2f}x{x:>9}{dd:>8.1f}%{switches:>7}{tier_str:>22}")
        print()


if __name__ == "__main__":
    main()
