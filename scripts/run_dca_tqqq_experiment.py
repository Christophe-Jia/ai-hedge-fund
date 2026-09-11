#!/usr/bin/env python3
"""Leverage experiment: monthly DCA with QQQ/TQQQ switching schemes.

All variants: $2000 on the 5th (next trading day), 2016-09 ~ now,
IBKR $1/order. QQQ series is dividend-accrued; TQQQ price-only (its 0.86%
ER + financing is already embedded in the market price; dividends ~0).

Variants:
  qqq          — 100% QQQ (baseline)
  tqqq         — 100% TQQQ
  alt-months   — literal alternation: odd months TQQQ, even months QQQ
  trend-qqq    — 200d-MA trend switch: QQQ above MA -> buy TQQQ, else QQQ
  trend-cash   — 200d-MA switch: above MA -> buy TQQQ, else hold cash
                 (cash accumulates, deploys when the trend turns back on)

Usage:
    poetry run python scripts/run_dca_tqqq_experiment.py
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from src.data.nasdaq_store import NasdaqDailyStore
from scripts.run_dca_us import run_dca, xirr  # reuse the tested helpers


def build_variant_series(qqq_tr: pd.Series, tqqq: pd.Series, mode: str) -> pd.Series:
    """Return a single 'synthetic asset' series per mode: the price you'd get
    by holding the scheme's asset each day. For monthly-DCA purposes each
    day's value = that month's chosen asset's normalised price path.

    Simpler and equivalent for our question: reindex both to the union
    calendar; for each month pick the active asset; the synthetic series is
    the active asset's daily return compounded month by month (rebalanced
    at month boundaries to 100% of that month's asset)."""
    idx = qqq_tr.index.union(tqqq.index)
    q = qqq_tr.reindex(idx).ffill()
    t = tqqq.reindex(idx).ffill()

    q_raw = qqq_tr.reindex(idx).ffill()
    months = idx.to_period("M")
    out = pd.Series(index=idx, dtype=float)
    prev_switch_val = 1.0
    cur_month = None
    cur_asset = None
    ma_window = 200

    q_raw_vals = q_raw.values
    ma = pd.Series(q_raw_vals, index=idx).rolling(ma_window, min_periods=ma_window).mean()

    for i, day in enumerate(idx):
        m = months[i]
        if m != cur_month:
            # first trading day of a new month: pick this month's asset
            cur_month = m
            if mode == "qqq":
                cur_asset = "q"
            elif mode == "tqqq":
                cur_asset = "t"
            elif mode == "alt-months":
                cur_asset = "t" if (m.month % 2 == 1) else "q"
            elif mode in ("trend-qqq", "trend-cash"):
                # signal from YESTERDAY's close vs trailing 200d MA (no look-ahead)
                sig_pos = i - 1
                if sig_pos >= ma_window:
                    above = q_raw_vals[sig_pos] > ma.iloc[sig_pos]
                else:
                    above = True
                if above:
                    cur_asset = "t"
                else:
                    cur_asset = "q" if mode == "trend-qqq" else "cash"
            base_q, base_t = q.iloc[i], t.iloc[i]

        if cur_asset == "q":
            out.iloc[i] = prev_switch_val * (q.iloc[i] / base_q)
        elif cur_asset == "t":
            out.iloc[i] = prev_switch_val * (t.iloc[i] / base_t)
        else:  # cash
            out.iloc[i] = prev_switch_val
        # roll the base at month end handled by month change above

        if i + 1 < len(idx) and months[i + 1] != m:
            prev_switch_val = out.iloc[i]

    return out.dropna()


def synth_dca(synth: pd.Series, monthly: float, day: int,
              commission: float = 1.0) -> dict:
    """DCA into the synthetic (mode) series. Same logic as run_dca."""
    s = synth.dropna()
    months = pd.date_range(s.index[0].replace(day=1), s.index[-1], freq="MS")
    invested, units, n = 0.0, 0.0, 0
    flows = []
    for m in months:
        target = m.replace(day=min(day, m.days_in_month))
        idx = s.index[s.index >= target]
        if len(idx) == 0:
            continue
        px = float(s.iloc[s.index.get_loc(idx[0])])
        units += (monthly - commission) / px
        invested += monthly
        n += 1
        flows.append((idx[0], -monthly))
    final = units * float(s.iloc[-1])
    flows.append((s.index[-1], final))
    return {"months": n, "invested": invested, "final": final,
            "moic": final / invested if invested else 0.0, "xirr": xirr(flows)}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--monthly", type=float, default=2000.0)
    p.add_argument("--day", type=int, default=5)
    p.add_argument("--start", type=str, default="2016-09-12")
    p.add_argument("--end", type=str, default=None)
    args = p.parse_args()
    end = args.end or datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

    store = NasdaqDailyStore(assetclass="etf")
    qqq_tr = store.get_close_series("QQQ", args.start, end)      # div-accrued
    tqqq = store.get_close_series("TQQQ", args.start, end, div_yield_annual=0.0)

    print("=" * 70)
    print(f"  QQQ/TQQQ 定投实验  |  每月 ${args.monthly:,.0f} · {args.day}号 · {args.start} ~ {end}")
    print("=" * 70)

    # baseline straight DCA (real series, same as run_dca_us)
    base = run_dca(qqq_tr, args.monthly, args.day)
    print(f"\n  {'方案':<14}{'期末市值':>12}{'净收益':>12}{'倍数':>9}{'XIRR':>9}")
    b_x = f"{base['xirr']*100:.2f}%" if base["xirr"] is not None else "-"
    print(f"  {'100% QQQ':<14}{base['final']:>12,.0f}{base['final']-base['invested']:>12,.0f}"
          f"{base['moic']:>8.3f}x{b_x:>9}")

    for mode, label in [
        ("tqqq", "100% TQQQ"),
        ("alt-months", "奇偶月交替"),
        ("trend-qqq", "趋势开关→TQQQ/QQQ"),
        ("trend-cash", "趋势开关→TQQQ/现金"),
    ]:
        synth = build_variant_series(qqq_tr, tqqq, mode)
        r = synth_dca(synth, args.monthly, args.day)
        x = f"{r['xirr']*100:.2f}%" if r["xirr"] is not None else "-"
        print(f"  {label:<14}{r['final']:>12,.0f}{r['final']-r['invested']:>12,.0f}"
              f"{r['moic']:>8.3f}x{x:>9}")

    # lump-sum reference for the same window (for the DCA-vs-lump discussion)
    lump = float(qqq_tr.iloc[-1] / qqq_tr.iloc[0]) * base["invested"]
    print(f"\n  [参照] 同样总额第一天全仓 QQQ: ${lump:,.0f}")
    print()


if __name__ == "__main__":
    main()
