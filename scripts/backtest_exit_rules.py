#!/usr/bin/env python3
"""Exit-rule comparison backtest for the weekend_gap signal.

The weekend_gap ENTRY is validated (BTC Fri close -> Sun close |move| >= 5%
-> Monday-open trade on COIN/MSTR/MARA equal-weight; the Monday gap mostly
does not fill and the edge concentrates in the first 1-2 days). This script
answers the remaining question for the trading playbook: WHICH EXIT RULE.

Entry (identical across all rules — never mixed):
  - Signal trigger day (first trading day the signal fires for a weekend:
    Monday, or Tuesday when Monday is a market holiday) OPEN price.
  - Targets: signal metadata targets (COIN/MSTR/MARA); a target without
    data on the entry day (COIN IPO 2021-04-14) is skipped and recorded.
  - Sizing: notional = |score| * current equity (score in [0.5, 1]).

Exit rules (per trade, per leg — same rule for every leg of a trade):
  t_plus_1      sell at T+1 close
  t_plus_2      sell at T+2 close
  t_plus_5      sell at T+5 close
  stop_tp       hard stop -8% / take-profit +15% / max hold T+5 close
                (intraday, stop checked before TP = conservative; a day
                opening beyond the level exits at the open)
  trail_half    exit at close when close gives back 50% of the best close
                profit, hard stop -8%, max hold T+5
  signal_decay  hold while the signal keeps firing. max_eval_lag_days=2
                means the last possible fire day is T+1 (the signal fires
                on both Monday AND Tuesday for the same weekend — verified
                empirically), so this exits at the LAST fire day's close:
                identical to t_plus_1 except when Tuesday is a holiday.

Direction modes: long_short (full signal) and long_only (no margin).

Windows (entry-date based, mirroring scripts/backtest_outofsample.py):
  full          2021-01-01 .. 2026-09-11
  out_of_sample 2021-01-01 .. 2023-03-02  (true OOS, spans 2021 top / 2022 bear / FTX)
  training      2023-04-01 .. 2026-09-11  (07e03d0 training window)
Plus per-year slices and a dedicated SHORT-side breakdown.

Costs: IBKR Pro tiered on both sides — $0.005/share, min $1/order,
5bps half-spread (src.selection.costs.IbkrCostModel).

Output: stdout summary + reports/exit_rules_backtest.json (all windows,
per-trade detail, conventions). This script only READS src/signals/*.

Usage:
    poetry run python scripts/backtest_exit_rules.py
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from src.data.nasdaq_store import NasdaqDailyStore
from src.selection.costs import IbkrCostModel
from src.signals import WeekendGapSignal

TARGETS = ("COIN", "MSTR", "MARA")
INITIAL = 100_000.0

# [start, end] inclusive on calendar days; events assigned by ENTRY date.
WINDOWS = {
    "full": ("2021-01-01", "2026-09-11"),
    "out_of_sample": ("2021-01-01", "2023-03-02"),
    "training": ("2023-04-01", "2026-09-11"),
}

RULES = ("t_plus_1", "t_plus_2", "t_plus_5", "stop_tp", "trail_half", "signal_decay")
MODES = ("long_short", "long_only")

STOP_PCT = 0.08
TP_PCT = 0.15
TRAIL_GIVEBACK = 0.50   # exit when close gives back 50% of best close profit
MAX_HOLD_DAYS = 5       # trading days after entry (stop_tp / trail_half)

# Data load range: a little lead-in before the first window.
LOAD_START, LOAD_END = "2020-12-01", "2026-09-15"


# ---------------------------------------------------------------------------
# Data & signal precomputation
# ---------------------------------------------------------------------------


def load_bars() -> dict[str, pd.DataFrame]:
    """Daily OHLC per target, tz-naive normalized date index."""
    store = NasdaqDailyStore(assetclass="stocks")
    bars = {}
    for sym in TARGETS:
        df = store.get_daily(sym, LOAD_START, LOAD_END)
        if df.empty:
            raise RuntimeError(f"no data for {sym}")
        df = df.copy()
        df.index = pd.DatetimeIndex(df.index).tz_localize(None).normalize()
        bars[sym] = df
    return bars


def build_calendar(bars: dict[str, pd.DataFrame]) -> list[pd.Timestamp]:
    days: set[pd.Timestamp] = set()
    for df in bars.values():
        days.update(df.index)
    return sorted(days)


def precompute_signal(calendar: list[pd.Timestamp]) -> dict:
    """{day: SignalOutput | None} for the weekend_gap signal."""
    sig = WeekendGapSignal()
    cache = {}
    for day in calendar:
        try:
            cache[day] = sig.generate(day)
        except Exception as exc:  # noqa: BLE001 — a failed day must not kill the run
            print(f"  [warn] weekend_gap failed on {day.date()}: {exc}")
            cache[day] = None
    return cache


def collect_events(cache: dict, calendar: list[pd.Timestamp]) -> list[dict]:
    """One event per flagged weekend; entry = first trading day it fires on."""
    by_sunday: dict[str, dict] = {}
    for day in calendar:
        out = cache.get(day)
        if out is None:
            continue
        sunday = out.metadata["sunday"]
        if sunday not in by_sunday:  # calendar is sorted -> first fire day wins
            by_sunday[sunday] = {"entry_day": day, "out": out}
    events = []
    for sunday in sorted(by_sunday):
        e = by_sunday[sunday]
        out = e["out"]
        events.append({
            "sunday": sunday,
            "entry_day": e["entry_day"],
            "direction": out.direction,
            "score": float(out.score),
            "btc_weekend_return_pct": out.metadata["btc_weekend_return_pct"],
            "targets": list(out.metadata.get("targets", TARGETS)),
        })
    return events


# ---------------------------------------------------------------------------
# Per-leg exit simulation
# ---------------------------------------------------------------------------


def simulate_leg(
    rule: str,
    event: dict,
    sym: str,
    bars: dict[str, pd.DataFrame],
    calendar: list[pd.Timestamp],
    idx: dict[pd.Timestamp, int],
    cache: dict,
) -> dict | None:
    """Simulate one leg (entry at trigger-day open) under `rule`.

    Returns None when the symbol has no bar on the entry day (skipped leg).
    """
    df = bars[sym]
    day0 = event["entry_day"]
    if day0 not in df.index:
        return None
    row0 = df.loc[day0]
    entry_price = float(row0["open"])
    if not (entry_price > 0):
        return None
    i0 = idx[day0]
    last_i = len(calendar) - 1
    long = event["direction"] == "long"
    sign = 1.0 if long else -1.0
    stop_price = entry_price * (1.0 - STOP_PCT * sign)
    tp_price = entry_price * (1.0 + TP_PCT * sign)

    def bar(i: int):
        day = calendar[i]
        return df.loc[day] if day in df.index else None

    def exit_rec(i: int, price: float, reason: str) -> dict:
        return {
            "symbol": sym,
            "entry_i": i0,
            "exit_i": i,
            "hold_days": i - i0,
            "entry_price": entry_price,
            "exit_price": price,
            "exit_reason": reason,
        }

    # --- time-based rules --------------------------------------------------
    if rule in ("t_plus_1", "t_plus_2", "t_plus_5"):
        n = int(rule.split("_")[-1])
        target_i = min(i0 + n, last_i)
        i = target_i
        while bar(i) is None and i > i0:  # safety: walk back to a traded bar
            i -= 1
        row = bar(i)
        reason = "time" if i0 + n <= last_i else "end_of_data"
        return exit_rec(i, float(row["close"]), reason)

    if rule == "signal_decay":
        # Hold while the signal keeps firing for this weekend. Bounded by
        # max_eval_lag_days=2 -> last fire day is at most T+1 (Tuesday).
        sunday = event["sunday"]
        last_fire_i = i0
        for i in range(i0, min(i0 + 2, last_i) + 1):
            out = cache.get(calendar[i])
            if out is not None and out.metadata.get("sunday") == sunday:
                last_fire_i = i
        i = last_fire_i
        while bar(i) is None and i > i0:
            i -= 1
        row = bar(i)
        reason = "signal_last_fire" if last_fire_i < last_i else "end_of_data"
        return exit_rec(i, float(row["close"]), reason)

    # --- path-dependent rules: stop_tp / trail_half ------------------------
    max_i = min(i0 + MAX_HOLD_DAYS, last_i)
    peak_close = entry_price  # best close for the trade's direction

    i = i0
    while i <= max_i:
        row = bar(i)
        if row is not None:
            o, h, low, c = (float(row[k]) for k in ("open", "high", "low", "close"))

            # 1) hard stop (intraday; a gap through the level exits at open)
            if long and o <= stop_price:
                return exit_rec(i, o, "stop_gap_open")
            if not long and o >= stop_price:
                return exit_rec(i, o, "stop_gap_open")
            if long and low <= stop_price:
                return exit_rec(i, stop_price, "stop")
            if not long and h >= stop_price:
                return exit_rec(i, stop_price, "stop")

            if rule == "stop_tp":
                # 2) take profit (intraday; gap through exits at open)
                if long:
                    if o >= tp_price:
                        return exit_rec(i, o, "tp_gap_open")
                    if h >= tp_price:
                        return exit_rec(i, tp_price, "tp")
                else:
                    if o <= tp_price:
                        return exit_rec(i, o, "tp_gap_open")
                    if low <= tp_price:
                        return exit_rec(i, tp_price, "tp")

            if rule == "trail_half":
                # 2) close-based trail on best close profit
                if sign * (c - peak_close) > 0:
                    peak_close = c
                if sign * (peak_close - entry_price) > 0:
                    trail = entry_price + TRAIL_GIVEBACK * (peak_close - entry_price)
                    if sign * (c - trail) < 0:
                        return exit_rec(i, c, "trail_half")

            # 3) max hold / end of data
            if i == max_i:
                reason = "time" if i0 + MAX_HOLD_DAYS <= last_i else "end_of_data"
                return exit_rec(i, c, reason)
        i += 1

    # unreachable (max_i loop always returns); kept for safety
    row = bar(max_i)
    return exit_rec(max_i, float(row["close"]), "time")


# ---------------------------------------------------------------------------
# Scenario: rule x mode x window
# ---------------------------------------------------------------------------


def run_scenario(
    rule: str,
    mode: str,
    window: tuple[str, str],
    events: list[dict],
    bars: dict[str, pd.DataFrame],
    calendar: list[pd.Timestamp],
    idx: dict[pd.Timestamp, int],
    cache: dict,
    closes_ff: dict[str, pd.Series],
    cost_model: IbkrCostModel,
) -> dict:
    """Simulate all events of the window under (rule, mode); return metrics + detail."""
    w_start = pd.Timestamp(window[0])
    w_end = pd.Timestamp(window[1])
    win_events = [
        e for e in events
        if w_start <= e["entry_day"] <= w_end
        and (mode == "long_short" or e["direction"] == "long")
    ]
    win_days = [d for d in calendar if w_start <= d <= w_end]
    win_idx = [idx[d] for d in win_days]
    last_win_i = win_idx[-1]

    cashflows: list[tuple[int, float]] = []   # (calendar_i, amount)
    open_legs: list[dict] = []                # legs for daily marking
    detail: list[dict] = []                   # per-event JSON detail
    skipped_legs: list[dict] = []
    total_costs = 0.0

    # current equity proxy for sizing: replay cashflows + marks chronologically.
    # Marks use the PREVIOUS day's close (no look-ahead: sizing happens at
    # the entry-day open, the last known price is yesterday's close).
    def equity_at(i: int) -> float:
        eq = INITIAL
        for (di, amt) in cashflows:
            if di <= i:
                eq += amt
        prev_day = calendar[max(i - 1, 0)]
        for leg in open_legs:
            if leg["entry_i"] < i <= leg["exit_i"]:
                px = closes_ff[leg["symbol"]].get(prev_day)
                if px is not None and not pd.isna(px):
                    eq += leg["shares"] * float(px)
        return eq

    for ev in win_events:
        i0 = idx[ev["entry_day"]]
        # simulate legs first (exits are price-determined, independent of size)
        leg_sims = []
        skipped = []
        for sym in ev["targets"]:
            sim = simulate_leg(rule, ev, sym, bars, calendar, idx, cache)
            if sim is None:
                skipped.append(sym)
                continue
            leg_sims.append(sim)
        if not leg_sims:
            continue  # nothing tradable that day

        notional_total = abs(ev["score"]) * equity_at(i0)
        per_leg = notional_total / len(leg_sims)
        event_entry_cf = 0.0
        event_exit_cf = 0.0
        event_cost = 0.0
        event_notional = 0.0
        detail_legs = []

        for sim in leg_sims:
            shares = (per_leg / sim["entry_price"]) * (1.0 if ev["direction"] == "long" else -1.0)
            entry_cost = cost_model.trade_cost(abs(shares), sim["entry_price"])
            exit_cost = cost_model.trade_cost(abs(shares), sim["exit_price"])
            entry_cf = -shares * sim["entry_price"] - entry_cost
            exit_cf = shares * sim["exit_price"] - exit_cost
            cashflows.append((i0, entry_cf))
            if sim["exit_i"] <= last_win_i:
                cashflows.append((sim["exit_i"], exit_cf))
            # legs still open at the window end stay marked at the last close
            open_legs.append({
                "symbol": sim["symbol"], "shares": shares,
                "entry_i": i0, "exit_i": sim["exit_i"],
            })
            pnl = entry_cf + exit_cf
            event_entry_cf += entry_cf
            event_exit_cf += exit_cf
            event_cost += entry_cost + exit_cost
            event_notional += abs(shares) * sim["entry_price"]
            detail_legs.append({
                "symbol": sim["symbol"],
                "shares": round(shares, 4),
                "entry_price": round(sim["entry_price"], 4),
                "exit_date": calendar[sim["exit_i"]].date().isoformat(),
                "exit_price": round(sim["exit_price"], 4),
                "exit_reason": sim["exit_reason"],
                "hold_trading_days": sim["hold_days"],
                "pnl_usd": round(pnl, 2),
                "ret_pct": round(pnl / (abs(shares) * sim["entry_price"]) * 100, 3),
            })
        for sym in skipped:
            skipped_legs.append({
                "entry_date": ev["entry_day"].date().isoformat(), "symbol": sym,
            })
            detail_legs.append({"symbol": sym, "skipped": True,
                                "reason": "no_data_on_entry_day"})

        event_pnl = event_entry_cf + event_exit_cf
        total_costs += event_cost
        detail.append({
            "entry_date": ev["entry_day"].date().isoformat(),
            "sunday": ev["sunday"],
            "direction": ev["direction"],
            "score": round(ev["score"], 4),
            "btc_weekend_return_pct": ev["btc_weekend_return_pct"],
            "n_legs": len(leg_sims),
            "notional_usd": round(event_notional, 2),
            "pnl_usd": round(event_pnl, 2),
            "ret_pct": round(event_pnl / event_notional * 100, 3) if event_notional else 0.0,
            "hold_trading_days_max": max(l["hold_trading_days"] for l in detail_legs if not l.get("skipped")),
            "costs_usd": round(event_cost, 2),
            "legs": detail_legs,
        })

    # --- daily equity curve over the window ---------------------------------
    # equity(t) = INITIAL + cashflows settled by t + marks of legs open at t
    # (a leg is open for entry_i <= t < exit_i; on exit_i its exit cashflow
    # replaces the mark; boundary legs still open at the window end are
    # marked at the last close).
    equity_vals = []
    for t in win_idx:
        day = calendar[t]
        eq = INITIAL
        for di, amt in cashflows:
            if di <= t:
                eq += amt
        for leg in open_legs:
            if leg["entry_i"] <= t < leg["exit_i"]:
                px = closes_ff[leg["symbol"]].get(day)
                if px is not None and not pd.isna(px):
                    eq += leg["shares"] * float(px)
        equity_vals.append(eq)

    equity = pd.Series(equity_vals, index=pd.DatetimeIndex(win_days), name="equity")

    # --- event-level stats ----------------------------------------------------
    pnls = [d["pnl_usd"] for d in detail]
    rets = [d["ret_pct"] for d in detail]
    m = metrics(equity)
    n_long = sum(1 for d in detail if d["direction"] == "long")
    n_short = sum(1 for d in detail if d["direction"] == "short")
    short_pnls = [d["pnl_usd"] for d in detail if d["direction"] == "short"]
    short_rets = [d["ret_pct"] for d in detail if d["direction"] == "short"]

    return {
        "metrics": m,
        "n_events": len(detail),
        "n_long_events": n_long,
        "n_short_events": n_short,
        "n_positions": sum(d["n_legs"] for d in detail),
        "win_rate_event_pct": round(100.0 * sum(1 for p in pnls if p > 0) / len(pnls), 2) if pnls else 0.0,
        "avg_event_pnl_usd": round(sum(pnls) / len(pnls), 2) if pnls else 0.0,
        "avg_event_ret_pct": round(sum(rets) / len(rets), 3) if rets else 0.0,
        "worst_event_pnl_usd": round(min(pnls), 2) if pnls else 0.0,
        "worst_event_ret_pct": round(min(rets), 3) if rets else 0.0,
        "avg_hold_trading_days": round(
            sum(d["hold_trading_days_max"] for d in detail) / len(detail), 2
        ) if detail else 0.0,
        "total_costs_usd": round(total_costs, 2),
        "short_side": {
            "n_events": n_short,
            "win_rate_pct": round(100.0 * sum(1 for p in short_pnls if p > 0) / len(short_pnls), 2) if short_pnls else None,
            "avg_ret_pct": round(sum(short_rets) / len(short_rets), 3) if short_rets else None,
            "total_pnl_usd": round(sum(short_pnls), 2) if short_pnls else 0.0,
        },
        "skipped_legs": skipped_legs,
        "detail": detail,
        "equity": equity,
    }


def metrics(equity: pd.Series) -> dict:
    ret = equity.pct_change().dropna()
    total_return = equity.iloc[-1] / equity.iloc[0] - 1.0
    years = len(equity) / 252.0
    cagr = (equity.iloc[-1] / equity.iloc[0]) ** (1.0 / years) - 1.0 if years > 0 else 0.0
    sharpe = float(ret.mean() / ret.std() * math.sqrt(252)) if len(ret) > 1 and ret.std() > 0 else 0.0
    drawdown = equity / equity.cummax() - 1.0
    return {
        "total_return_pct": round(total_return * 100, 2),
        "cagr_pct": round(cagr * 100, 2),
        "sharpe": round(sharpe, 3),
        "max_drawdown_pct": round(float(drawdown.min()) * 100, 2),
        "n_trading_days": len(equity),
    }


# ---------------------------------------------------------------------------
# Analysis helpers
# ---------------------------------------------------------------------------


def stop_vs_t5(rule: str, detail: list[dict], t5_detail: list[dict]) -> list[dict]:
    """For every stop-exited leg: counterfactual pnl had it ridden to T+5 close."""
    t5_map = {
        (ev["entry_date"], leg["symbol"]): leg
        for ev in t5_detail for leg in ev["legs"] if not leg.get("skipped")
    }
    out = []
    for ev in detail:
        for leg in ev["legs"]:
            if leg.get("skipped") or not leg["exit_reason"].startswith("stop"):
                continue
            t5 = t5_map.get((ev["entry_date"], leg["symbol"]))
            rec = {
                "rule": rule,
                "entry_date": ev["entry_date"], "symbol": leg["symbol"],
                "direction": ev["direction"], "score": ev["score"],
                "entry_price": leg["entry_price"], "stop_exit_price": leg["exit_price"],
                "stop_pnl_usd": leg["pnl_usd"],
                "stop_ret_pct": leg["ret_pct"],
            }
            if t5 is not None:
                rec["t_plus_5_exit_price"] = t5["exit_price"]
                rec["t_plus_5_ret_pct"] = t5["ret_pct"]
                rec["stop_saved_pnl_usd"] = round(t5["pnl_usd"] - leg["pnl_usd"], 2)
                rec["stop_saved"] = t5["pnl_usd"] < leg["pnl_usd"]
            out.append(rec)
    return out


def print_table(title: str, rows: dict[str, dict]) -> None:
    print(f"\n  {title}")
    header = (f"  {'规则':14s} {'总收益':>9s} {'CAGR':>8s} {'Sharpe':>7s} {'MDD':>8s} "
              f"{'交易':>4s} {'L/S':>7s} {'胜率':>7s} {'均笔$':>9s} {'均笔%':>7s} "
              f"{'最差$':>9s} {'持仓d':>6s} {'成本$':>7s}")
    print(header)
    for rule in RULES:
        r = rows[rule]
        m = r["metrics"]
        print(f"  {rule:14s} {m['total_return_pct']:>+8.2f}% {m['cagr_pct']:>+7.2f}% "
              f"{m['sharpe']:>7.3f} {m['max_drawdown_pct']:>7.2f}% "
              f"{r['n_events']:>4d} {r['n_long_events']:>3d}/{r['n_short_events']:<3d} "
              f"{r['win_rate_event_pct']:>6.1f}% {r['avg_event_pnl_usd']:>+9.0f} "
              f"{r['avg_event_ret_pct']:>+6.3f}% {r['worst_event_pnl_usd']:>+9.0f} "
              f"{r['avg_hold_trading_days']:>6.1f} {r['total_costs_usd']:>7.0f}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    print("=" * 108)
    print("  weekend_gap 出场规则对比回测")
    print(f"  入场: 触发日开盘 等权 COIN/MSTR/MARA | 仓位 |score|×权益 | IBKR 成本 | 初始 ${INITIAL:,.0f}")
    print(f"  止损 -{STOP_PCT*100:.0f}% / 止盈 +{TP_PCT*100:.0f}% / trail 回撤 {TRAIL_GIVEBACK*100:.0f}%浮盈 / 最长 T+{MAX_HOLD_DAYS}")
    print("=" * 108)

    bars = load_bars()
    calendar = build_calendar(bars)
    idx = {d: i for i, d in enumerate(calendar)}
    closes_ff = {
        sym: df["close"].reindex(pd.DatetimeIndex(calendar)).ffill()
        for sym, df in bars.items()
    }
    print(f"\n交易日历: {len(calendar)} 天 ({calendar[0].date()} ~ {calendar[-1].date()})")
    print("评估 weekend_gap 信号（每日，缓存）...")
    cache = precompute_signal(calendar)
    events = collect_events(cache, calendar)
    n_long = sum(1 for e in events if e["direction"] == "long")
    n_short = sum(1 for e in events if e["direction"] == "short")
    print(f"事件总数: {len(events)} (long {n_long} / short {n_short}), "
          f"首次 {events[0]['entry_day'].date()}, 最近 {events[-1]['entry_day'].date()}")

    cost_model = IbkrCostModel()

    report: dict = {
        "config": {
            "signal": "weekend_gap (threshold 5%, targets COIN/MSTR/MARA, max_eval_lag_days=2)",
            "entry": "trigger-day OPEN (Monday, or Tuesday when Monday is a holiday), equal weight across targets with data",
            "sizing": "notional = |score| * equity at entry (score in [0.5, 1.0]); overlapping T+5 trades can stack gross exposure",
            "rules": {
                "t_plus_1": "sell at T+1 close",
                "t_plus_2": "sell at T+2 close",
                "t_plus_5": "sell at T+5 close",
                "stop_tp": f"hard stop -{STOP_PCT*100:.0f}% / TP +{TP_PCT*100:.0f}% / max hold T+{MAX_HOLD_DAYS} close; intraday, stop checked before TP; gap through level exits at open",
                "trail_half": f"exit at close when close gives back {TRAIL_GIVEBACK*100:.0f}% of best close profit; hard stop -{STOP_PCT*100:.0f}%; max hold T+{MAX_HOLD_DAYS}",
                "signal_decay": "hold while the signal fires; exits at the LAST fire day close (signal fires Mon AND Tue for the same weekend, so normally T+1 close)",
            },
            "modes": {"long_short": "full signal", "long_only": "skip short events (no margin)"},
            "initial_capital": INITIAL,
            "cost_model": {
                "commission_per_share": cost_model.commission_per_share,
                "min_commission_per_order": cost_model.min_commission_per_order,
                "spread_bps": cost_model.spread_bps,
            },
            "max_eval_lag_note": "verified empirically: the signal fires on BOTH Monday and Tuesday for the same flagged weekend",
        },
        "windows": {},
        "trades_full_window": {},
        "conventions": {
            "look_ahead": "entry uses the trigger day's open after the signal fired (signal reads only data strictly before as_of); exits use only bars up to the exit day",
            "windows": "events assigned by ENTRY date; OOS 2021-01-01..2023-03-02 and training 2023-04-01..2026-09-11 mirror scripts/backtest_outofsample.py (gap in 2023-03 intentional)",
            "boundary_trades": "a trade entered near a window end keeps its REAL exit for stats but is marked at the window's last close in the equity curve",
            "end_of_data": "trades whose exit would fall beyond 2026-09-11 exit at the last close, reason end_of_data",
            "shorting": "allowed in long_short mode; borrow costs not modeled",
            "skipped_legs": "COIN before its 2021-04-14 IPO is skipped and recorded",
            "win_rate": "event-level: an event wins when its net-of-cost pnl (all legs) > 0",
            "tax_note": "holding period <= 1 year -> short-term capital gains taxed as ordinary income; all rules here hold <= ~1 week",
        },
    }

    for wname, window in WINDOWS.items():
        for mode in MODES:
            rows = {}
            for rule in RULES:
                rows[rule] = run_scenario(
                    rule, mode, window, events, bars, calendar, idx, cache,
                    closes_ff, cost_model,
                )
            wlabel = f"{window[0]} ~ {window[1]}"
            print_table(f"{wname} [{wlabel}] | {mode}", rows)

            report["windows"].setdefault(wname, {"range": list(window)})[mode] = {
                rule: {k: v for k, v in rows[rule].items() if k not in ("equity", "detail")}
                for rule in RULES
            }
            if wname == "full":
                report["trades_full_window"][mode] = {
                    rule: rows[rule]["detail"] for rule in RULES
                }
                if mode == "long_short":
                    # stop-exit analysis with a T+5 counterfactual
                    t5 = rows["t_plus_5"]["detail"]
                    stops = (stop_vs_t5("stop_tp", rows["stop_tp"]["detail"], t5)
                             + stop_vs_t5("trail_half", rows["trail_half"]["detail"], t5))
                    report["stop_exits_long_short_full"] = stops
                    n_saved = sum(1 for s in stops if s.get("stop_saved"))
                    print(f"\n  被止损离场的 legs（stop_tp + trail_half, long_short 全窗口）: {len(stops)}"
                          f"（其中优于 T+5 持有的 {n_saved} 个）")
                    for s in stops:
                        t5r = s.get("t_plus_5_ret_pct")
                        saved = s.get("stop_saved")
                        print(f"    {s['entry_date']} {s['symbol']:5s} {s['direction']:5s} "
                              f"score {s['score']:+.2f} 入场 {s['entry_price']:9.2f} -> "
                              f"止损价 {s['stop_exit_price']:9.2f} 单腿 {s['stop_ret_pct']:+7.2f}%"
                              + (f" | T+5 {t5r:+7.2f}% {'止损更优' if saved else 'T+5更优'}"
                                 if t5r is not None else ""))

    # --- signal_decay vs t_plus_1 equivalence check -------------------------
    sd = report["windows"]["full"]["long_short"]["signal_decay"]
    t1 = report["windows"]["full"]["long_short"]["t_plus_1"]
    identical = (
        sd["n_events"] == t1["n_events"]
        and sd["metrics"] == t1["metrics"]
    )
    # compare per-leg exits (date + price); reasons legitimately differ
    sd_det = report["trades_full_window"]["long_short"]["signal_decay"]
    t1_det = report["trades_full_window"]["long_short"]["t_plus_1"]
    diff = 0
    for a, b in zip(sd_det, t1_det):
        ea = {l["symbol"]: (l.get("exit_date"), l.get("exit_price")) for l in a["legs"] if not l.get("skipped")}
        eb = {l["symbol"]: (l.get("exit_date"), l.get("exit_price")) for l in b["legs"] if not l.get("skipped")}
        if ea != eb:
            diff += 1
            print(f"    [signal_decay≠t_plus_1] {a['entry_date']}: "
                  f"sd {ea} vs t1 {eb}")
    print(f"\n  signal_decay vs t_plus_1（long_short 全窗口）: "
          f"指标完全一致={identical}, 逐笔出场不同的交易数={diff}")
    report["signal_decay_check"] = {
        "metrics_identical_to_t_plus_1": identical,
        "events_with_different_exits": diff,
        "note": "signal_decay exits at the LAST fire day close; the signal fires Mon+Tue, "
                "so this equals T+1 close except when the entry itself was a Tuesday "
                "(Monday holiday: the 2-day eval window is exhausted on entry day, so "
                "it exits at the entry-day close = T+0)",
    }

    # --- SHORT side summary (rule-independent trigger counts) ----------------
    print("\n  SHORT 侧单独统计（按窗口，触发数与方向分布）")
    report["short_side_triggers"] = {}
    for wname, window in WINDOWS.items():
        w_start, w_end = pd.Timestamp(window[0]), pd.Timestamp(window[1])
        we = [e for e in events if w_start <= e["entry_day"] <= w_end]
        shorts = [e for e in we if e["direction"] == "short"]
        per_rule = report["windows"][wname]["long_short"]
        best_short = max(
            RULES, key=lambda r: (per_rule[r]["short_side"]["total_pnl_usd"] or 0))
        trig = {
            "n_events": len(we), "n_short": len(shorts),
            "short_triggers_per_year": round(len(shorts) / (len(calendar_window(calendar, window)) / 252.0), 2),
            "best_rule_by_short_pnl": best_short,
            "per_rule": {r: per_rule[r]["short_side"] for r in RULES},
        }
        report["short_side_triggers"][wname] = trig
        bs = trig["per_rule"][best_short]
        print(f"    {wname:14s}: 触发 {len(we):3d} (short {len(shorts)}) | "
              f"short 最佳规则 {best_short}: 胜率 {bs['win_rate_pct']}%, "
              f"均笔 {bs['avg_ret_pct']}%, 总P&L ${bs['total_pnl_usd']}")

    # --- per-year slices ------------------------------------------------------
    print("\n  分年切片（long_short, 事件数/胜率/总P&L$ 按入场年）")
    header = f"  {'年份':6s}" + "".join(f" | {r:>24s}" for r in RULES)
    print(header)
    report["per_year"] = {}
    years = sorted({
        e["entry_day"].year for e in events
        if pd.Timestamp(WINDOWS["full"][0]) <= e["entry_day"] <= pd.Timestamp(WINDOWS["full"][1])
    })
    for yr in years:
        report["per_year"][str(yr)] = {}
        row = f"  {yr:<6d}"
        for rule in RULES:
            det = [
                d for d in report["trades_full_window"]["long_short"][rule]
                if d["entry_date"].startswith(str(yr))
            ]
            pnl = sum(d["pnl_usd"] for d in det)
            wr = 100.0 * sum(1 for d in det if d["pnl_usd"] > 0) / len(det) if det else 0.0
            report["per_year"][str(yr)][rule] = {
                "n_events": len(det),
                "win_rate_pct": round(wr, 1),
                "total_pnl_usd": round(pnl, 2),
                "n_short": sum(1 for d in det if d["direction"] == "short"),
            }
            row += f" | {len(det):3d}/{wr:5.1f}%/{pnl:+9.0f}"
        print(row)

    out_path = Path("reports/exit_rules_backtest.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    print(f"\nJSON 已写入: {out_path}")


def calendar_window(calendar: list[pd.Timestamp], window: tuple[str, str]) -> list:
    w_start, w_end = pd.Timestamp(window[0]), pd.Timestamp(window[1])
    return [d for d in calendar if w_start <= d <= w_end]


if __name__ == "__main__":
    main()
