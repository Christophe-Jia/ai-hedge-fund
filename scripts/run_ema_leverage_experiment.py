#!/usr/bin/env python3
"""EMA-gated partial leverage: DCA into QQQ core + TQQQ sleeve.

Question: "when QQQ crosses its 120 EMA, switch PART of the position to
TQQQ; switch back on the opposite cross — does that improve returns?"

Model:
  - Core/sleeve book tracked as a unit NAV (like a fund you DCA into).
  - Signal: QQQ raw close vs EMA(P), evaluated at day t's close; the mix
    change executes at day t's close and affects day t+1's return
    (strictly no look-ahead).
  - Above EMA -> target mix (1-λ) QQQ + λ TQQQ. Below -> 100% QQQ.
  - Switch cost: both legs, 5 bps each of the switched sleeve notional
    (spread+slippage; IBKR $1 commission negligible at book size).
  - Monthly DCA $2000 on the 5th buys NAV units ($1 commission).
  - QQQ leg is dividend-accrued; TQQQ price-only (ER+financing embedded).

Usage:
    poetry run python scripts/run_ema_leverage_experiment.py
    poetry run python scripts/run_ema_leverage_experiment.py --ema 120 --lambdas 0.25,0.5,0.75
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from src.data.nasdaq_store import NasdaqDailyStore
from scripts.run_dca_us import xirr

SWITCH_COST_BPS = 5.0  # per leg, per switched notional

# Synthetic 3x leveraged ETF parameters (mirrors dca_backtest/config.py):
# daily return = 3*index_return - 2*(riskfree + financing_spread)/365 - ER/365
_SYNTH_ER_ANNUAL = 0.0086        # TQQQ expense ratio
_SYNTH_FIN_SPREAD = 0.005        # financing spread over risk-free on 2x borrowed


def build_fred_legs(start: str, end: str) -> tuple[pd.Series, pd.Series, pd.Series]:
    """QQQ proxy + synthetic TQQQ from FRED long-history data.

    Returns (index_price, synth_3x_nav, raw_index_for_signal).
    - QQQ proxy: NASDAQ Composite (price index; no dividends — tech dividend
      yields were ~0.2-0.5% in the 2000s, noted caveat).
    - Synthetic TQQQ: 3x daily composite return minus financing on 2x
      borrowed notional at (3M T-bill + spread) minus ER.
    """
    from src.data.fred_store import FredSeries

    fred = FredSeries()
    idx = fred.get("NASDAQCOM")
    tbill = fred.get("DTB3")  # annualized percent

    idx = idx[(idx.index >= start) & (idx.index <= end)]
    r = idx.pct_change().fillna(0.0)

    # daily financing drag aligned to the index calendar (ffill rates)
    tb_daily = (tbill / 100.0).reindex(idx.index).ffill().fillna(0.02)
    drag = 2.0 * (tb_daily + _SYNTH_FIN_SPREAD) / 365.0 + _SYNTH_ER_ANNUAL / 365.0

    synth = (1.0 + 3.0 * r - drag).cumprod()
    return idx, synth, idx


def simulate_nav(
    r_q: pd.Series, r_t: pd.Series, signal: pd.Series, lam: float
) -> tuple[pd.Series, int, float]:
    """Unit NAV of the (1-λ)QQQ + λTQQQ-if-signal book.

    signal[t] (bool, known at t's close) sets the mix effective for t+1.
    Returns (nav_series, n_switches, total_cost_drag_fraction).
    """
    nav = 1.0
    navs = [1.0]
    prev_mix_levered = False
    switches = 0
    cost_drag = 0.0
    idx = r_q.index

    for i in range(1, len(idx)):
        # mix effective today = f(signal at yesterday's close)
        levered = bool(signal.iloc[i - 1])
        w_t = lam if levered else 0.0

        r = (1.0 - w_t) * float(r_q.iloc[i]) + w_t * float(r_t.iloc[i])
        if pd.isna(r):
            r = 0.0
        nav *= 1.0 + r

        # state change executes at today's close -> pay switching costs
        if levered != prev_mix_levered and i >= 2:
            traded = (lam * nav) if levered else (lam * nav)
            cost = 2.0 * traded * SWITCH_COST_BPS / 10_000.0
            nav -= cost
            cost_drag += cost
            switches += 1
        prev_mix_levered = levered
        navs.append(nav)

    return pd.Series(navs, index=idx), switches, cost_drag


def dca_into_nav(nav: pd.Series, monthly: float, day: int,
                 commission: float = 1.0) -> dict:
    s = nav.dropna()
    months = pd.date_range(s.index[0].replace(day=1), s.index[-1], freq="MS")
    units, invested, n = 0.0, 0.0, 0
    flows = []
    for m in months:
        target = m.replace(day=min(day, m.days_in_month))
        pos = s.index[s.index >= target]
        if len(pos) == 0:
            continue
        px = float(s.iloc[s.index.get_loc(pos[0])])
        units += (monthly - commission) / px
        invested += monthly
        n += 1
        flows.append((pos[0], -monthly))
    final = units * float(s.iloc[-1])
    flows.append((s.index[-1], final))
    return {"months": n, "invested": invested, "final": final,
            "moic": final / invested if invested else 0.0, "xirr": xirr(flows)}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--ema", type=int, default=120)
    p.add_argument("--lambdas", type=str, default="0,0.25,0.5,0.75,1.0")
    p.add_argument("--monthly", type=float, default=2000.0)
    p.add_argument("--day", type=int, default=5)
    p.add_argument("--start", type=str, default="2016-09-12")
    p.add_argument("--end", type=str, default=None)
    p.add_argument("--ema-sensitivity", action="store_true",
                   help="also sweep EMA period at the middle lambda")
    p.add_argument("--source", choices=["nasdaq", "fred"], default="nasdaq",
                   help="nasdaq: real QQQ/TQQQ (10y) | fred: NASDAQ Composite "
                        "proxy + synthetic 3x (1971→, price-only)")
    args = p.parse_args()
    end = args.end or datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

    if args.source == "fred":
        idx_price, synth, _ = build_fred_legs(args.start, end)
        # QQQ leg: the composite index itself (price proxy for QQQ)
        qqq_tr = idx_price
        # TQQQ leg: synthetic 3x NAV
        tqqq = synth
        src_note = "FRED代理: 纳斯达克综合指数 + 合成3x(3×日收益−2×(国库券+0.5%)−0.86%ER)"
    else:
        store = NasdaqDailyStore(assetclass="etf")
        qqq_tr = store.get_close_series("QQQ", args.start, end)                    # returns leg
        tqqq = store.get_close_series("TQQQ", args.start, end, div_yield_annual=0.0)
        src_note = "真实数据: QQQ(含股息) + TQQQ"

    qqq_raw = qqq_tr  # signal leg: price series in both modes
    idx = qqq_tr.index.union(tqqq.index)
    q = qqq_tr.reindex(idx).ffill()
    t = tqqq.reindex(idx).ffill()
    qr = qqq_raw.reindex(idx).ffill()
    r_q = q.pct_change().fillna(0.0)
    r_t = t.pct_change().fillna(0.0)

    def ema_signal(span: int) -> pd.Series:
        ema = qr.ewm(span=span, adjust=False).mean()
        return (qr > ema).fillna(True)

    print("=" * 74)
    print(f"  EMA杠杆袖筒实验  |  信号: QQQ vs EMA{args.ema} · 月投 ${args.monthly:,.0f} · {args.day}号")
    print(f"  区间: {args.start} ~ {end} · 换仓成本 {SWITCH_COST_BPS:.0f}bps/腿 · 无前视")
    print(f"  数据: {src_note}")
    print("=" * 74)

    sig = ema_signal(args.ema)
    pct_levered = float(sig.mean()) * 100.0

    print(f"\n  {'λ(袖筒)':<10}{'期末市值':>12}{'净收益':>12}{'倍数':>9}{'XIRR':>9}"
          f"{'换仓次数':>8}{'NAV回撤':>9}")
    rows = []
    for lam in [float(x) for x in args.lambdas.split(",")]:
        nav, switches, drag = simulate_nav(r_q, r_t, sig, lam)
        r = dca_into_nav(nav, args.monthly, args.day)
        dd = float((nav / nav.cummax() - 1.0).min()) * 100
        x = f"{r['xirr']*100:.2f}%" if r["xirr"] is not None else "-"
        label = f"{lam*100:.0f}%" if lam else "0%(纯QQQ)"
        print(f"  {label:<10}{r['final']:>12,.0f}{r['final']-r['invested']:>12,.0f}"
              f"{r['moic']:>8.3f}x{x:>9}{switches:>8}{dd:>8.1f}%")
        rows.append((lam, r, switches, dd))

    print(f"\n  信号在杠杆态的时间占比: {pct_levered:.0f}%")

    if args.ema_sensitivity:
        lam_mid = 0.5
        print(f"\n  EMA周期敏感性 (λ={lam_mid}):")
        print(f"  {'EMA':<8}{'倍数':>10}{'XIRR':>10}{'换仓':>8}")
        for span in (100, 120, 150, 200):
            s2 = ema_signal(span)
            nav, sw, _ = simulate_nav(r_q, r_t, s2, lam_mid)
            r = dca_into_nav(nav, args.monthly, args.day)
            x = f"{r['xirr']*100:.2f}%" if r["xirr"] is not None else "-"
            print(f"  {span:<8}{r['moic']:>9.3f}x{x:>10}{sw:>8}")
    print()


if __name__ == "__main__":
    main()
