#!/usr/bin/env python3
"""Overnight BTC gap backtest — daily-frequency generalization of weekend_gap.

Research question: weekend_gap (BTC Fri close -> Sun close |move| >= 5% ->
Monday-open trade on COIN/MSTR/MARA) is the platform's only dual-window
validated signal (OOS Sharpe 0.68) but fires only 4-5x/year. The mechanism
(BTC trades 7x24, US equities 6.5h/day -> information-diffusion lag) exists
EVERY trading day: between the 16:00 ET close and the next 9:30 ET open BTC
has 17.5h to move. Does a large BTC overnight move -> same-day open
continuation in crypto stocks? If yes, events go from 4-5/year to 30-60+.

Event definitions (both computed and reported):
  market_close   prev TRADING day 16:00 ET -> today 9:30 ET
                 (Monday's window spans the whole weekend, 65.5h — under
                 this definition weekend_gap is a near-special case)
  overnight_only prev CALENDAR day 16:00 ET -> today 9:30 ET
                 (always ~17.5h; Monday measures only Sun 16:00 -> Mon 9:30,
                 isolating the pure weekday-overnight effect)

BTC price at an ET timestamp = close of the last 1h bar whose close time is
at or before the timestamp (look-ahead safe; at 16:00 ET this is exact, at
9:30 ET it is the 9:00 ET bar close, 30min stale). DST handled via
ZoneInfo("America/New_York"). Endpoints with no bar within 2h are treated as
missing and the day is skipped for that definition (counted + reported).

Signal convention (mirrors src/signals/weekend_gap.py):
  |overnight return| >= threshold -> trade in BTC's direction at today's
  OPEN, equal weight COIN/MSTR/MARA (legs without data skipped, e.g. COIN
  pre-IPO); score = sign * min(|ret| / (2*threshold), 1) in [0.5, 1].

Exits: t_plus_1 / t_plus_2 close. Modes: long_only / long_short.
Sizing: notional = |score| * equity at entry (marked at prev-day close);
overlapping trades stack. Costs: IBKR ($0.005/share, min $1, 5bps
half-spread) both sides — src.selection.costs.IbkrCostModel.

Windows (entry-date based, mirroring scripts/backtest_exit_rules.py):
  full           2019-01-01 .. 2026-09-11 (BTC 1h coverage starts 2019-01)
  front_half / back_half   split at the median trading day of full
  pre_2023_03    2019-01-01 .. 2023-03-02  (weekend_gap true-OOS era)
  post_2023_04   2023-04-01 .. 2026-09-11  (weekend_gap training era)

Also reports: threshold scan 2/3/4/5% (ALL shown), gap continuation vs
fill diagnostics (weekend version: 80-100% no-fill), direction/magnitude
buckets, overlap with the weekend_gap signal, and a merged signal
(weekend_gap on its fire days + overnight on all other days).

Output: stdout summary + reports/overnight_gap_backtest.json.

Usage:
    poetry run python scripts/backtest_overnight_gap.py
"""

from __future__ import annotations

import json
import math
import sys
from bisect import bisect_right
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from src.data.historical_store import HistoricalOHLCVStore
from src.data.nasdaq_store import NasdaqDailyStore
from src.selection.costs import IbkrCostModel
from src.signals import WeekendGapSignal

TARGETS = ("COIN", "MSTR", "MARA")
INITIAL = 100_000.0
NY = ZoneInfo("America/New_York")

THRESHOLDS = (2.0, 3.0, 4.0, 5.0)   # % — all reported, no cherry-picking
EXITS = ("t_plus_1", "t_plus_2")
MODES = ("long_only", "long_short")
DEFINITIONS = ("market_close", "overnight_only")
GROSS_CAP = 2.0          # max gross exposure as a multiple of equity at entry
MIN_NOTIONAL_FRAC = 0.05  # below this the event is skipped as fully crowded out

WINDOWS = {
    "full": ("2019-01-01", "2026-09-11"),
    "pre_2023_03": ("2019-01-01", "2023-03-02"),
    "post_2023_04": ("2023-04-01", "2026-09-11"),
}
# front/back halves are computed from the median trading day of full.

LOAD_START, LOAD_END = "2018-12-01", "2026-09-15"
BTC_MAX_STALE_MS = 2 * 3600 * 1000  # BTC endpoint must have a bar close within 2h

EXIT_N = {"t_plus_1": 1, "t_plus_2": 2}


# ---------------------------------------------------------------------------
# Data loading
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


def load_btc_1h() -> tuple[list[int], list[float], dict]:
    """BTC/USDT spot 1h closes.

    Returns (close_times_ms, closes, coverage) where close_times_ms is the
    bar OPEN ts + 1h — the time at which the close price becomes known.
    """
    store = HistoricalOHLCVStore(allow_fetch=False)
    start = int(
        datetime(2018, 12, 1, tzinfo=timezone.utc).timestamp() * 1000
    )
    end = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    df = store.get_ohlcv("BTC/USDT", "spot", "1h", start, end)
    if df.empty:
        raise RuntimeError("no BTC/USDT spot 1h data")
    times = (df["ts"] + 3600_000).astype(int).tolist()
    closes = df["close"].astype(float).tolist()

    gaps = []
    ts = df["ts"].astype(int).tolist()
    for i in range(1, len(ts)):
        if ts[i] - ts[i - 1] != 3600_000:
            gaps.append({
                "after_utc": datetime.fromtimestamp(
                    ts[i - 1] / 1000, tz=timezone.utc
                ).strftime("%Y-%m-%d %H:%M"),
                "gap_hours": (ts[i] - ts[i - 1]) / 3600_000,
            })
    coverage = {
        "first_bar_utc": datetime.fromtimestamp(
            ts[0] / 1000, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M"),
        "last_bar_utc": datetime.fromtimestamp(
            ts[-1] / 1000, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M"),
        "n_bars": len(ts),
        "n_gaps": len(gaps),
        "max_gap_hours": max((g["gap_hours"] for g in gaps), default=0.0),
        "gaps": gaps,
    }
    return times, closes, coverage


def et_ts(day: pd.Timestamp, hh: int, mm: int) -> int:
    """Epoch ms of `day` at hh:mm America/New_York."""
    dt = datetime(day.year, day.month, day.day, hh, mm, tzinfo=NY)
    return int(dt.timestamp() * 1000)


def price_at(times: list[int], closes: list[float], t_ms: int) -> float | None:
    """Last 1h close known at or before t_ms (None if stale/missing)."""
    i = bisect_right(times, t_ms) - 1
    if i < 0:
        return None
    if t_ms - times[i] > BTC_MAX_STALE_MS:
        return None
    return closes[i]


# ---------------------------------------------------------------------------
# Event construction
# ---------------------------------------------------------------------------


def build_day_returns(
    calendar: list[pd.Timestamp],
    times: list[int],
    closes: list[float],
) -> tuple[dict[int, float], dict[int, float], dict[str, int]]:
    """Per trading-day BTC overnight returns for both definitions.

    Returns (ret_a, ret_b, skipped) keyed by calendar index.
    """
    ret_a: dict[int, float] = {}   # prev TRADING day 16:00 ET -> 9:30 ET
    ret_b: dict[int, float] = {}   # prev CALENDAR day 16:00 ET -> 9:30 ET
    skipped = {"market_close": 0, "overnight_only": 0, "no_open_price": 0}

    for i in range(1, len(calendar)):
        day = calendar[i]
        p_open = price_at(times, closes, et_ts(day, 9, 30))
        if p_open is None or p_open <= 0:
            skipped["no_open_price"] += 1
            continue

        p_prev_trading = price_at(times, closes, et_ts(calendar[i - 1], 16, 0))
        if p_prev_trading is None or p_prev_trading <= 0:
            skipped["market_close"] += 1
        else:
            ret_a[i] = p_open / p_prev_trading - 1.0

        prev_cal = day - pd.Timedelta(days=1)
        p_prev_cal = price_at(times, closes, et_ts(prev_cal, 16, 0))
        if p_prev_cal is None or p_prev_cal <= 0:
            skipped["overnight_only"] += 1
        else:
            ret_b[i] = p_open / p_prev_cal - 1.0

    return ret_a, ret_b, skipped


def simulate_legs(
    day_i: int,
    bars: dict[str, pd.DataFrame],
    calendar: list[pd.Timestamp],
) -> tuple[list[dict], list[str]]:
    """Leg-level entry/exit prices for an event on calendar index day_i.

    Exits precomputed for both t_plus_1 and t_plus_2.
    """
    day = calendar[day_i]
    last_i = len(calendar) - 1
    legs: list[dict] = []
    skipped: list[str] = []
    for sym in TARGETS:
        df = bars[sym]
        if day not in df.index:
            skipped.append(sym)
            continue
        row0 = df.loc[day]
        entry = float(row0["open"])
        if not entry > 0:
            skipped.append(sym)
            continue
        prev_stock_close = float(df["close"].iloc[df.index.get_loc(day) - 1]) \
            if df.index.get_loc(day) > 0 else None
        leg = {
            "symbol": sym,
            "entry_price": entry,
            "prev_stock_close": prev_stock_close,
        }
        for k in (1, 2):
            target_i = min(day_i + k, last_i)
            j = target_i
            while calendar[j] not in df.index and j > day_i:
                j -= 1
            exit_day = calendar[j]
            leg[f"exit_i_{k}"] = j
            leg[f"exit_price_{k}"] = float(df.loc[exit_day]["close"])
            leg[f"exit_reason_{k}"] = (
                "time" if day_i + k <= last_i else "end_of_data"
            )
        # open -> same-day close (gap continuation diagnostic)
        leg["close_0"] = float(row0["close"])
        legs.append(leg)
    return legs, skipped


def build_events(
    day_returns: dict[int, float],
    threshold: float,
    calendar: list[pd.Timestamp],
    bars: dict[str, pd.DataFrame],
) -> list[dict]:
    """Events for one (definition, threshold): |ret| >= threshold."""
    events = []
    for i, ret in sorted(day_returns.items()):
        if abs(ret) * 100.0 < threshold:
            continue
        legs, skipped = simulate_legs(i, bars, calendar)
        if not legs:
            continue
        score = min(abs(ret) * 100.0 / (2.0 * threshold), 1.0)
        events.append({
            "entry_i": i,
            "entry_date": calendar[i].date().isoformat(),
            "dayofweek": int(calendar[i].dayofweek),
            "btc_ret": ret,
            "btc_ret_pct": round(ret * 100.0, 3),
            "direction": "long" if ret > 0 else "short",
            "score": score if ret > 0 else -score,
            "legs": legs,
            "skipped_legs": skipped,
        })
    return events


# ---------------------------------------------------------------------------
# Portfolio simulation (mirrors scripts/backtest_exit_rules.py conventions)
# ---------------------------------------------------------------------------


def run_portfolio(
    events: list[dict],
    window: tuple[str, str],
    mode: str,
    exit_rule: str,
    bars: dict[str, pd.DataFrame],
    calendar: list[pd.Timestamp],
    closes_ff: dict[str, pd.Series],
    cost_model: IbkrCostModel,
    initial: float = INITIAL,
) -> dict:
    """Equity-curve simulation of `events` (already direction-filtered by mode).

    Per-event rets/PnL are path-independent; the equity path only affects
    sizing (notional = |score| * equity at prev-day-close marks).
    """
    k = EXIT_N[exit_rule]
    w_start, w_end = pd.Timestamp(window[0]), pd.Timestamp(window[1])
    win_events = [
        e for e in events
        if w_start <= pd.Timestamp(e["entry_date"]) <= w_end
        and (mode == "long_short" or e["direction"] == "long")
    ]
    cal_start = next(i for i, d in enumerate(calendar) if pd.Timestamp(d) >= w_start)
    cal_end = next(
        (i for i, d in enumerate(calendar) if pd.Timestamp(d) > w_end),
        len(calendar),
    ) - 1
    n_days = cal_end - cal_start + 1
    if n_days < 2 or not win_events:
        return {
            "metrics": empty_metrics(), "n_events": 0, "n_capped_events": 0,
            "detail": [], "n_long_events": 0, "n_short_events": 0,
        }

    daily_cf = [0.0] * (cal_end + 2)     # cashflows posted on day index
    marks_by_day = [[] for _ in range(cal_end + 2)]
    detail: list[dict] = []
    active: list[dict] = []              # legs open at the current moment
    n_capped = 0
    ev_iter = iter(win_events)
    pending = next(ev_iter, None)

    for t in range(cal_start, cal_end + 1):
        # retire legs that exited before today (their exit cf is already posted)
        active = [lg for lg in active if lg["exit_i"] > t]
        # enter events whose entry day is today
        while pending is not None and pending["entry_i"] == t:
            ev = pending
            pending = next(ev_iter, None)
            # equity at the PREVIOUS day's close (no look-ahead at the open)
            prev_t = t - 1
            eq_prev = (
                initial + sum(daily_cf[cal_start : prev_t + 1])
                if prev_t >= cal_start else initial
            )
            for lg in active:
                if lg["entry_i"] <= prev_t < lg["exit_i"]:
                    px = closes_ff[lg["symbol"]].get(calendar[prev_t])
                    if px is not None and not pd.isna(px):
                        eq_prev += lg["shares"] * float(px)
            sign = 1.0 if ev["direction"] == "long" else -1.0
            # gross exposure cap: new notional limited so that total gross
            # (prev-day-close marks of open legs + new entries) <= GROSS_CAP x
            # equity; events crowded out below MIN_NOTIONAL_FRAC are skipped
            gross_open = 0.0
            for lg in active:
                if lg["entry_i"] <= prev_t < lg["exit_i"]:
                    px = closes_ff[lg["symbol"]].get(calendar[prev_t])
                    if px is not None and not pd.isna(px):
                        gross_open += abs(lg["shares"]) * float(px)
            notional_total = min(
                abs(ev["score"]) * eq_prev,
                max(GROSS_CAP * eq_prev - gross_open, 0.0),
            )
            if notional_total < MIN_NOTIONAL_FRAC * eq_prev:
                n_capped += 1
                continue
            per_leg = notional_total / len(ev["legs"])
            ev_entry_cf = ev_exit_cf = ev_cost = ev_notional = 0.0
            detail_legs = []
            for leg in ev["legs"]:
                entry_px = leg["entry_price"]
                exit_px = leg[f"exit_price_{k}"]
                exit_i = leg[f"exit_i_{k}"]
                shares = sign * per_leg / entry_px
                entry_cost = cost_model.trade_cost(abs(shares), entry_px)
                exit_cost = cost_model.trade_cost(abs(shares), exit_px)
                entry_cf = -shares * entry_px - entry_cost
                exit_cf = shares * exit_px - exit_cost
                daily_cf[t] += entry_cf
                if exit_i <= cal_end:
                    daily_cf[exit_i] += exit_cf
                active.append({
                    "symbol": leg["symbol"], "shares": shares,
                    "entry_i": t, "exit_i": exit_i,
                })
                pnl = entry_cf + exit_cf
                ev_entry_cf += entry_cf
                ev_exit_cf += exit_cf
                ev_cost += entry_cost + exit_cost
                ev_notional += abs(shares) * entry_px
                detail_legs.append({
                    "symbol": leg["symbol"],
                    "entry_price": round(entry_px, 4),
                    "exit_price": round(exit_px, 4),
                    "ret_pct": round(pnl / (abs(shares) * entry_px) * 100, 3),
                })
            ev_pnl = ev_entry_cf + ev_exit_cf
            detail.append({
                "entry_date": ev["entry_date"],
                "direction": ev["direction"],
                "score": round(ev["score"], 4),
                "btc_overnight_ret_pct": ev["btc_ret_pct"],
                "n_legs": len(ev["legs"]),
                "skipped_legs": ev["skipped_legs"],
                "pnl_usd": round(ev_pnl, 2),
                "ret_pct": round(ev_pnl / ev_notional * 100, 3) if ev_notional else 0.0,
                "costs_usd": round(ev_cost, 2),
                "legs": detail_legs,
            })
        # marks at today's close for legs still open today; a leg exiting
        # today is replaced by its exit cashflow, a leg whose exit falls
        # beyond the window stays marked at the window's last close
        for lg in active:
            if lg["entry_i"] <= t < lg["exit_i"] or (
                lg["exit_i"] > cal_end and t == cal_end
            ):
                px = closes_ff[lg["symbol"]].get(calendar[t])
                if px is not None and not pd.isna(px):
                    marks_by_day[t].append(lg["shares"] * float(px))

    cum_cf = initial
    equity_series = []
    for t in range(cal_start, cal_end + 1):
        cum_cf += daily_cf[t]
        equity_series.append(cum_cf + sum(marks_by_day[t]))
    equity = pd.Series(
        equity_series,
        index=pd.DatetimeIndex(calendar[cal_start : cal_end + 1]),
    )

    rets = [d["ret_pct"] for d in detail]
    pnls = [d["pnl_usd"] for d in detail]
    n_long = sum(1 for d in detail if d["direction"] == "long")
    n_short = sum(1 for d in detail if d["direction"] == "short")
    short_rets = [d["ret_pct"] for d in detail if d["direction"] == "short"]

    return {
        "metrics": metrics(equity),
        "n_events": len(detail),
        "n_capped_events": n_capped,
        "n_long_events": n_long,
        "n_short_events": n_short,
        "events_per_year": round(len(detail) / (n_days / 252.0), 2),
        "win_rate_event_pct": round(
            100.0 * sum(1 for p in pnls if p > 0) / len(pnls), 2
        ) if pnls else 0.0,
        "avg_event_ret_pct": round(sum(rets) / len(rets), 3) if rets else 0.0,
        "median_event_ret_pct": round(float(pd.Series(rets).median()), 3) if rets else 0.0,
        "t_stat": round(
            float(pd.Series(rets).mean() / pd.Series(rets).std(ddof=1)
                  * math.sqrt(len(rets))), 3
        ) if len(rets) > 1 and pd.Series(rets).std(ddof=1) > 0 else 0.0,
        "worst_event_ret_pct": round(min(rets), 3) if rets else 0.0,
        "total_costs_usd": round(sum(d["costs_usd"] for d in detail), 2),
        "short_side": {
            "n_events": n_short,
            "win_rate_pct": round(
                100.0 * sum(1 for d in detail if d["direction"] == "short"
                            and d["pnl_usd"] > 0) / n_short, 2
            ) if n_short else None,
            "avg_ret_pct": round(sum(short_rets) / len(short_rets), 3)
            if short_rets else None,
        },
        "detail": detail,
    }


def empty_metrics() -> dict:
    return {
        "total_return_pct": 0.0, "cagr_pct": 0.0, "sharpe": 0.0,
        "max_drawdown_pct": 0.0, "n_trading_days": 0,
    }


def metrics(equity: pd.Series) -> dict:
    if len(equity) < 2:
        return empty_metrics()
    ret = equity.pct_change().dropna()
    total_return = equity.iloc[-1] / equity.iloc[0] - 1.0
    years = len(equity) / 252.0
    base = equity.iloc[-1] / equity.iloc[0]
    cagr = (
        base ** (1.0 / years) - 1.0
        if years > 0 and equity.iloc[0] > 0 and base > 0 else 0.0
    )
    sharpe = (
        float(ret.mean() / ret.std() * math.sqrt(252))
        if len(ret) > 1 and ret.std() > 0 else 0.0
    )
    drawdown = equity / equity.cummax() - 1.0
    return {
        "total_return_pct": round(total_return * 100, 2),
        "cagr_pct": round(cagr * 100, 2),
        "sharpe": round(sharpe, 3),
        "max_drawdown_pct": round(float(drawdown.min()) * 100, 2),
        "n_trading_days": len(equity),
    }


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------


def gap_diagnostics(
    events: list[dict],
    all_day_returns: dict[int, float],
    calendar: list[pd.Timestamp],
    bars: dict[str, pd.DataFrame],
    threshold: float,
) -> dict:
    """Gap continuation vs fill + stock-gap/BTC correlation + buckets."""
    btc_rets, avg_stock_gaps, cont_open_close, cont_open_t1, cont_open_t2 = (
        [], [], [], [], []
    )
    fill_flags, sign_match, used_events = [], [], []
    for ev in events:
        sign = 1.0 if ev["direction"] == "long" else -1.0
        gaps, oc, o1, o2, fills, sm = [], [], [], [], [], []
        for leg in ev["legs"]:
            if leg["prev_stock_close"] is None:
                continue
            gap = leg["entry_price"] / leg["prev_stock_close"] - 1.0
            gaps.append(gap)
            sm.append(1 if gap * sign > 0 else 0)
            oc.append(sign * (leg["close_0"] / leg["entry_price"] - 1.0))
            o1.append(sign * (leg["exit_price_1"] / leg["entry_price"] - 1.0))
            o2.append(sign * (leg["exit_price_2"] / leg["entry_price"] - 1.0))
            # gap fully given back: close returns through the pre-gap level
            if ev["direction"] == "long":
                fills.append(1 if leg["close_0"] < leg["prev_stock_close"] else 0)
            else:
                fills.append(1 if leg["close_0"] > leg["prev_stock_close"] else 0)
        if not gaps:
            continue
        used_events.append(ev)
        btc_rets.append(ev["btc_ret"])
        avg_stock_gaps.append(sum(gaps) / len(gaps))
        cont_open_close.append(sum(oc) / len(oc))
        cont_open_t1.append(sum(o1) / len(o1))
        cont_open_t2.append(sum(o2) / len(o2))
        fill_flags.extend(fills)
        sign_match.extend(sm)

    def corr_beta(xs, ys):
        if len(xs) < 3:
            return None, None
        s = pd.DataFrame({"x": xs, "y": ys})
        cov = float(s.cov().iloc[0, 1])
        var = float(s["x"].var())
        return (
            round(float(s["x"].corr(s["y"])), 3),
            round(cov / var, 3) if var > 0 else None,
        )

    corr_ev, beta_ev = corr_beta(btc_rets, avg_stock_gaps)

    # all-days baseline (market_close definition, no threshold)
    base_btc, base_gap = [], []
    for i in range(1, len(calendar)):
        if i not in all_day_returns:
            continue
        day = calendar[i]
        gaps = []
        for sym in TARGETS:
            df = bars[sym]
            if day not in df.index:
                continue
            pos = df.index.get_loc(day)
            if pos == 0:
                continue
            gaps.append(float(df.iloc[pos]["open"]) / float(df.iloc[pos - 1]["close"]) - 1.0)
        if gaps:
            base_btc.append(all_day_returns[i])
            base_gap.append(sum(gaps) / len(gaps))
    corr_all, beta_all = corr_beta(base_btc, base_gap)

    n = len(cont_open_close)
    diag = {
        "n_events": n,
        "stock_gap_vs_btc_overnight": {
            "corr_events": corr_ev,
            "beta_events": beta_ev,
            "corr_all_days": corr_all,
            "beta_all_days": beta_all,
            "open_gap_sign_match_rate_pct": round(
                100.0 * sum(sign_match) / len(sign_match), 2
            ) if sign_match else None,
            "note": "avg equal-weight stock opening gap vs BTC overnight return; "
                    "beta = stock gap per unit BTC move (under-reaction < 1)",
        },
        "gap_continuation": {
            "open_to_close_avg_pct": round(100 * sum(cont_open_close) / n, 3) if n else None,
            "open_to_close_positive_rate_pct": round(
                100.0 * sum(1 for x in cont_open_close if x > 0) / n, 2
            ) if n else None,
            "open_to_t1_avg_pct": round(100 * sum(cont_open_t1) / n, 3) if n else None,
            "open_to_t1_positive_rate_pct": round(
                100.0 * sum(1 for x in cont_open_t1 if x > 0) / n, 2
            ) if n else None,
            "open_to_t2_avg_pct": round(100 * sum(cont_open_t2) / n, 3) if n else None,
            "open_to_t2_positive_rate_pct": round(
                100.0 * sum(1 for x in cont_open_t2 if x > 0) / n, 2
            ) if n else None,
            "full_fill_rate_pct": round(
                100.0 * sum(fill_flags) / len(fill_flags), 2
            ) if fill_flags else None,
            "note": "returns signed by trade direction; positive = gap continues. "
                    "full_fill = same-day close crosses back through the pre-gap "
                    "stock close (weekend_gap reference: 0-20% fill)",
        },
        "by_direction": {},
        "buckets": [],
    }

    for direction in ("long", "short"):
        sub = [
            x for ev, x in zip(used_events, cont_open_close)
            if ev["direction"] == direction
        ]
        diag["by_direction"][direction] = {
            "n": len(sub),
            "open_to_close_avg_pct": round(
                100 * sum(sub) / len(sub), 3
            ) if sub else None,
            "open_to_close_positive_rate_pct": round(
                100.0 * sum(1 for x in sub if x > 0) / len(sub), 2
            ) if sub else None,
        }

    # magnitude buckets (relative to threshold) x direction, open->T+2
    buckets = []
    usable = list(zip(used_events, cont_open_t2))
    for direction in ("long", "short"):
        for lo, hi, label in ((1.0, 1.5, "1.0-1.5x"), (1.5, 2.0, "1.5-2.0x"), (2.0, 99.0, ">=2x")):
            vals = [
                x for ev, x in usable
                if ev["direction"] == direction
                and lo <= abs(ev["btc_ret"]) * 100.0 / threshold < hi
            ]
            buckets.append({
                "direction": direction, "magnitude": label,
                "n": len(vals),
                "open_to_t2_avg_pct": round(100 * sum(vals) / len(vals), 3) if vals else None,
                "open_to_t2_positive_rate_pct": round(
                    100.0 * sum(1 for x in vals if x > 0) / len(vals), 2
                ) if vals else None,
            })
    diag["buckets"] = buckets
    return diag


# ---------------------------------------------------------------------------
# weekend_gap overlap + merged signal
# ---------------------------------------------------------------------------


def collect_weekend_events(
    calendar: list[pd.Timestamp], bars: dict[str, pd.DataFrame],
) -> tuple[list[dict], set[int]]:
    """weekend_gap events (first fire day) + the set of ALL fire-day indices."""
    sig = WeekendGapSignal()
    by_sunday: dict[str, dict] = {}
    fire_days: set[int] = set()
    for i, day in enumerate(calendar):
        try:
            out = sig.generate(day)
        except Exception:
            out = None
        if out is None:
            continue
        fire_days.add(i)
        sunday = out.metadata["sunday"]
        if sunday not in by_sunday:
            legs, skipped = simulate_legs(i, bars, calendar)
            if not legs:
                continue
            by_sunday[sunday] = {
                "entry_i": i,
                "entry_date": day.date().isoformat(),
                "dayofweek": int(day.dayofweek),
                "btc_ret": out.metadata["btc_weekend_return_pct"] / 100.0,
                "btc_ret_pct": out.metadata["btc_weekend_return_pct"],
                "direction": out.direction,
                "score": float(out.score),
                "legs": legs,
                "skipped_legs": skipped,
                "weekend": True,
            }
    return sorted(by_sunday.values(), key=lambda e: e["entry_i"]), fire_days


def pearson(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 3:
        return None
    s = pd.Series(xs)
    t = pd.Series(ys)
    if s.std() == 0 or t.std() == 0:
        return None
    return round(float(s.corr(t)), 3)


# ---------------------------------------------------------------------------
# Printing helpers
# ---------------------------------------------------------------------------


def fmt(v, suffix=""):
    if v is None:
        return "  n/a"
    return f"{v:+.2f}{suffix}" if isinstance(v, (int, float)) else str(v)


def print_results_table(
    title: str,
    results: dict,   # {thr: {exit: {mode: {window: {...}}}}}
    windows: list[str],
) -> None:
    print(f"\n  {title}")
    for thr in THRESHOLDS:
        print(f"\n  阈值 {thr:.0f}%")
        header = (f"    {'出场':10s} {'模式':11s} {'窗口':13s} {'事件':>5s} {'事件/年':>7s} "
                  f"{'胜率%':>7s} {'均笔%':>7s} {'t值':>6s} {'总收益%':>9s} {'Sharpe':>7s} {'MDD%':>8s}")
        print(header)
        for exit_rule in EXITS:
            for mode in MODES:
                for wname in windows:
                    r = results[thr][exit_rule][mode].get(wname)
                    if r is None or r["n_events"] == 0:
                        continue
                    m = r["metrics"]
                    print(f"    {exit_rule:10s} {mode:11s} {wname:13s} "
                          f"{r['n_events']:>5d} {r.get('events_per_year', 0):>7.1f} "
                          f"{r['win_rate_event_pct']:>7.1f} {r['avg_event_ret_pct']:>+7.3f} "
                          f"{r['t_stat']:>6.2f} {m['total_return_pct']:>+9.1f} "
                          f"{m['sharpe']:>7.3f} {m['max_drawdown_pct']:>8.1f}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    print("=" * 118)
    print("  隔夜 BTC 缺口回测 — weekend_gap 的日频泛化")
    print(f"  事件: 美股前一日 16:00 ET 收盘 -> 当日 9:30 ET 开盘的 BTC 收益 |r|>=阈值")
    print(f"  交易: 事件日开盘 等权 COIN/MSTR/MARA | 仓位 |score|×权益 | IBKR 成本 | 初始 ${INITIAL:,.0f}")
    print("=" * 118)

    bars = load_bars()
    calendar = sorted({d for df in bars.values() for d in df.index})
    calendar = [d for d in calendar if d >= pd.Timestamp("2019-01-01")]
    idx = {d: i for i, d in enumerate(calendar)}
    closes_ff = {
        sym: df["close"].reindex(pd.DatetimeIndex(calendar)).ffill()
        for sym, df in bars.items()
    }
    times, closes, btc_cov = load_btc_1h()
    print(f"\n交易日历: {len(calendar)} 天 ({calendar[0].date()} ~ {calendar[-1].date()})")
    print(f"BTC 1h: {btc_cov['n_bars']} bars, {btc_cov['first_bar_utc']} ~ "
          f"{btc_cov['last_bar_utc']}, 缺口 {btc_cov['n_gaps']} 处 (最大 {btc_cov['max_gap_hours']:.0f}h)")

    ret_a, ret_b, skipped_days = build_day_returns(calendar, times, closes)
    print(f"隔夜收益可用天数: market_close {len(ret_a)} / overnight_only {len(ret_b)} "
          f"(跳过: {skipped_days})")

    # half split at the median trading day
    mid = calendar[len(calendar) // 2]
    all_windows = dict(WINDOWS)
    all_windows["front_half"] = ("2019-01-01", mid.date().isoformat())
    all_windows["back_half"] = ((mid + pd.Timedelta(days=1)).date().isoformat(), "2026-09-11")
    win_names = ["full", "front_half", "back_half", "pre_2023_03", "post_2023_04"]

    day_returns = {"market_close": ret_a, "overnight_only": ret_b}

    cost_model = IbkrCostModel()
    report: dict = {
        "config": {
            "signal": "overnight BTC gap -> same-day open trade on COIN/MSTR/MARA",
            "definitions": {
                "market_close": "prev TRADING day 16:00 ET -> today 9:30 ET "
                                "(Monday spans the weekend, 65.5h; weekend_gap is a near-special case)",
                "overnight_only": "prev CALENDAR day 16:00 ET -> today 9:30 ET "
                                  "(always ~17.5h; Monday = Sunday 16:00 -> Monday 9:30 only)",
            },
            "btc_price_convention": "close of the last 1h bar known at or before the ET "
                                    "timestamp (exact at 16:00 ET; the 9:00 ET bar close at 9:30 ET — "
                                    "look-ahead safe); endpoints with no bar within 2h skip the day",
            "thresholds_pct": list(THRESHOLDS),
            "entry": "event-day OPEN, equal weight across targets with data (COIN pre-IPO skipped)",
            "sizing": "score = sign * min(|ret|/(2*threshold), 1) in [0.5,1]; notional = |score| * equity at entry",
            "gross_exposure_cap": (
                f"new entries limited so total gross (prev-day-close marks of open legs + "
                f"new notional) <= {GROSS_CAP}x equity at entry; events crowded out below "
                f"{MIN_NOTIONAL_FRAC:.0%} of equity are skipped and counted as n_capped_events "
                "(matters mainly at the 2% threshold where events cluster on consecutive days)"
            ),
            "exits": {"t_plus_1": "sell at T+1 close", "t_plus_2": "sell at T+2 close"},
            "modes": {"long_short": "trade both directions", "long_only": "skip short events"},
            "initial_capital": INITIAL,
            "cost_model": {
                "commission_per_share": cost_model.commission_per_share,
                "min_commission_per_order": cost_model.min_commission_per_order,
                "spread_bps": cost_model.spread_bps,
            },
            "windows": {k: list(v) for k, v in all_windows.items()},
            "conventions": {
                "windows": "events assigned by ENTRY date; pre/post 2023-03 split mirrors the "
                           "weekend_gap training/OOS boundary (gap in 2023-03 intentional)",
                "half_split": f"front/back halves split at the median trading day {mid.date()}",
                "per_event_returns": "net of IBKR costs, from the full-window equity path "
                                     "(PnL is path-independent; sizing path only affects min-commission, negligible)",
                "boundary_trades": "exits beyond the window are kept for stats; equity marked at the window's last close",
                "stock_data": "Nasdaq daily, split-adjusted (MSTR 2024-08 10:1 verified continuous)",
                "shorting": "borrow costs not modeled",
            },
        },
        "data_coverage": {
            "btc_1h": {k: v for k, v in btc_cov.items() if k != "gaps"},
            "btc_1h_gaps": btc_cov["gaps"],
            "calendar": {"first": str(calendar[0].date()), "last": str(calendar[-1].date()),
                         "n_days": len(calendar)},
            "days_skipped_stale_btc": skipped_days,
        },
        "event_counts": {},
        "results": {},
        "gap_diagnostics": {},
        "weekend_gap_overlap": {},
        "merged_signal": {},
        "per_year": {},
        "events_detail": {},
    }

    # ------------------------------------------------------------------
    # Core scan: definition x threshold x exit x mode x window
    # ------------------------------------------------------------------
    events_by_def_thr: dict[tuple[str, float], list[dict]] = {}
    results = {d: {} for d in DEFINITIONS}
    for definition in DEFINITIONS:
        for thr in THRESHOLDS:
            events = build_events(day_returns[definition], thr, calendar, bars)
            events_by_def_thr[(definition, thr)] = events
            n_long = sum(1 for e in events if e["direction"] == "long")
            years = len(calendar) / 252.0
            by_month = {}
            for e in events:
                by_month[e["entry_date"][:7]] = by_month.get(e["entry_date"][:7], 0) + 1
            report["event_counts"].setdefault(definition, {})[str(thr)] = {
                "n_events": len(events),
                "events_per_year": round(len(events) / years, 2),
                "n_long": n_long,
                "n_short": len(events) - n_long,
                "n_monday": sum(1 for e in events if e["dayofweek"] == 0),
                "n_by_month": dict(sorted(by_month.items())),
            }
            results[definition][thr] = {ex: {m: {} for m in MODES} for ex in EXITS}
            for exit_rule in EXITS:
                for mode in MODES:
                    for wname, window in all_windows.items():
                        r = run_portfolio(
                            events, window, mode, exit_rule,
                            bars, calendar, closes_ff, cost_model,
                        )
                        results[definition][thr][exit_rule][mode][wname] = {
                            kk: vv for kk, vv in r.items() if kk != "detail"
                        }
                    # stash full detail once (full window) for JSON + per-year
                    results[definition][thr][exit_rule][mode]["_detail_full"] = \
                        run_portfolio(events, all_windows["full"], mode, exit_rule,
                                      bars, calendar, closes_ff, cost_model)["detail"]

            print(f"\n  [{definition} | 阈值 {thr:.0f}%] 事件 {len(events)} "
                  f"({len(events)/years:.1f}/年, long {n_long} / short {len(events)-n_long}, "
                  f"周一占比 {100*sum(1 for e in events if e['dayofweek']==0)/max(len(events),1):.0f}%)")

    report["results"] = {
        d: {
            str(thr): {
                ex: {m: {w: r for w, r in results[d][thr][ex][m].items()
                         if not w.startswith("_")}
                     for m in MODES}
                for ex in EXITS
            }
            for thr in THRESHOLDS
        }
        for d in DEFINITIONS
    }

    for definition in DEFINITIONS:
        print_results_table(
            f"定义: {definition} — 全窗口/切分",
            results[definition], win_names,
        )

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------
    print("\n" + "=" * 118)
    print("  缺口延续 vs 回补诊断（全窗口，T+2 口径, open->T+2 为方向化收益）")
    for definition in DEFINITIONS:
        report["gap_diagnostics"][definition] = {}
        for thr in THRESHOLDS:
            diag = gap_diagnostics(
                events_by_def_thr[(definition, thr)],
                day_returns[definition], calendar, bars, thr,
            )
            report["gap_diagnostics"][definition][str(thr)] = diag
            g = diag["gap_continuation"]
            s = diag["stock_gap_vs_btc_overnight"]
            print(f"\n  [{definition} | {thr:.0f}%] n={diag['n_events']}")
            print(f"    股票开盘缺口 vs BTC隔夜: corr={s['corr_events']} beta={s['beta_events']} "
                  f"方向一致率 {s['open_gap_sign_match_rate_pct']}% (全样本 corr={s['corr_all_days']} "
                  f"beta={s['beta_all_days']})")
            print(f"    open->close: 均值 {g['open_to_close_avg_pct']}% / 正率 {g['open_to_close_positive_rate_pct']}% "
                  f"| open->T+1: {g['open_to_t1_avg_pct']}% / {g['open_to_t1_positive_rate_pct']}% "
                  f"| open->T+2: {g['open_to_t2_avg_pct']}% / {g['open_to_t2_positive_rate_pct']}%")
            print(f"    完全回补率(当日收盘穿回缺口前价位): {g['full_fill_rate_pct']}%  "
                  f"(weekend_gap 参照: 0-20%)")
            for direction in ("long", "short"):
                dd = diag["by_direction"][direction]
                print(f"      {direction:5s}: n={dd['n']} open->close 均值 {dd['open_to_close_avg_pct']}% "
                      f"正率 {dd['open_to_close_positive_rate_pct']}%")

    # ------------------------------------------------------------------
    # weekend_gap overlap
    # ------------------------------------------------------------------
    print("\n" + "=" * 118)
    print("  与 weekend_gap 的关系")
    weekend_events, fire_days = collect_weekend_events(calendar, bars)
    wg_years = len(calendar) / 252.0
    print(f"weekend_gap 事件: {len(weekend_events)} "
          f"({len(weekend_events)/wg_years:.1f}/年, "
          f"{weekend_events[0]['entry_date']} ~ {weekend_events[-1]['entry_date']})")
    wg_by_date = {e["entry_date"]: e for e in weekend_events}

    overlap_report: dict = {
        "weekend_gap_events": {
            "n": len(weekend_events),
            "events_per_year": round(len(weekend_events) / wg_years, 2),
            "n_long": sum(1 for e in weekend_events if e["direction"] == "long"),
            "n_short": sum(1 for e in weekend_events if e["direction"] == "short"),
            "entry_days": [e["entry_date"] for e in weekend_events],
        },
        "per_threshold": {},
    }

    # weekend_gap reference performance (t_plus_2, both modes) for the merged view
    wg_ref = {}
    for exit_rule in EXITS:
        for mode in MODES:
            r = run_portfolio(weekend_events, all_windows["full"], mode, exit_rule,
                              bars, calendar, closes_ff, cost_model)
            wg_ref[(exit_rule, mode)] = r

    for definition in DEFINITIONS:
        overlap_report["per_threshold"][definition] = {}
        for thr in THRESHOLDS:
            events = events_by_def_thr[(definition, thr)]
            ev_by_date = {e["entry_date"]: e for e in events}
            same_day = [d for d in ev_by_date if d in wg_by_date]
            wg_caught = [d for d in wg_by_date if d in ev_by_date]
            # ret correlation on same-day events (t_plus_2, long_short)
            xs, ys = [], []
            for d in same_day:
                wg = wg_by_date[d]
                ov = ev_by_date[d]
                if wg["direction"] != ov["direction"]:
                    continue
                def ev_ret(ev):
                    k = 2
                    sign = 1.0 if ev["direction"] == "long" else -1.0
                    rs = [
                        sign * (leg[f"exit_price_{k}"] / leg["entry_price"] - 1.0)
                        for leg in ev["legs"]
                    ]
                    return sum(rs) / len(rs)
                xs.append(ev_ret(wg))
                ys.append(ev_ret(ov))
            rec = {
                "overnight_events": len(events),
                "same_day_as_weekend_gap": len(same_day),
                "weekend_gap_events_caught_by_overnight": len(wg_caught),
                "weekend_gap_events_missed": len(weekend_events) - len(wg_caught),
                "missed_dates": [d for d in wg_by_date if d not in ev_by_date],
                "same_day_return_corr_t2": pearson(xs, ys),
                "n_same_day_used_for_corr": len(xs),
                "corr_note": "trivially ~1.0 when directions match: both signals trade "
                             "the same targets at the same day's open/close prices",
            }
            overlap_report["per_threshold"][definition][str(thr)] = rec
            print(f"  [{definition} | {thr:.0f}%] 同日触发 {len(same_day)}/{len(events)}; "
                  f"weekend_gap 被覆盖 {len(wg_caught)}/{len(weekend_events)} "
                  f"(漏 {rec['weekend_gap_events_missed']}); "
                  f"同日事件收益相关(T+2) {rec['same_day_return_corr_t2']}")

    # weekend_gap events that overnight@5% market_close misses — why?
    for definition in ("market_close",):
        missed = overlap_report["per_threshold"][definition]["5.0"]["missed_dates"]
        if missed:
            print(f"  weekend_gap 触发但 overnight(market_close, 5%) 未触发的日期 "
                  f"({len(missed)} 个): 周末涨但周一 9:30 前回吐的情形")
            for d in missed:
                wg = wg_by_date[d]
                print(f"    {d} weekend_ret {wg['btc_ret_pct']:+.2f}% "
                      f"({wg['direction']})")

    report["weekend_gap_overlap"] = overlap_report

    # ------------------------------------------------------------------
    # Merged signal: weekend_gap on its fire days + overnight (market_close)
    # on all other days
    # ------------------------------------------------------------------
    print("\n" + "=" * 118)
    print("  合并信号: weekend_gap 触发日用周末版, 其余日子用隔夜版 (market_close 定义)")
    for thr in THRESHOLDS:
        ov = [e for e in events_by_def_thr[("market_close", thr)]
              if e["entry_i"] not in fire_days]
        merged = weekend_events + ov
        merged.sort(key=lambda e: e["entry_i"])
        report["merged_signal"][str(thr)] = {
            "n_events": len(merged),
            "events_per_year": round(len(merged) / wg_years, 2),
            "n_weekend": len(weekend_events),
            "n_overnight": len(ov),
        }
        for exit_rule in EXITS:
            for mode in MODES:
                r = run_portfolio(merged, all_windows["full"], mode, exit_rule,
                                  bars, calendar, closes_ff, cost_model)
                report["merged_signal"][str(thr)].setdefault(exit_rule, {})[mode] = {
                    kk: vv for kk, vv in r.items() if kk != "detail"
                }
        best = report["merged_signal"][str(thr)]["t_plus_2"]["long_only"]["metrics"]
        print(f"  阈值 {thr:.0f}%: 事件 {len(merged)} ({len(merged)/wg_years:.1f}/年) "
              f"[weekend {len(weekend_events)} + overnight {len(ov)}] | "
              f"T+2 long_only: 总收益 {best['total_return_pct']:+.1f}% "
              f"Sharpe {best['sharpe']:.3f} MDD {best['max_drawdown_pct']:.1f}%")

    # weekend_gap standalone reference (for the merge comparison)
    report["weekend_gap_overlap"]["standalone_full_window"] = {
        ex: {m: {k: v for k, v in wg_ref[(ex, m)].items() if k != "detail"}
             for m in MODES}
        for ex in EXITS
    }
    wg_t2_lo = wg_ref[("t_plus_2", "long_only")]["metrics"]
    print(f"  参照: weekend_gap 单独 (T+2 long_only): 事件 {len(weekend_events)} "
          f"({len(weekend_events)/wg_years:.1f}/年) 总收益 {wg_t2_lo['total_return_pct']:+.1f}% "
          f"Sharpe {wg_t2_lo['sharpe']:.3f}")

    # ------------------------------------------------------------------
    # Per-year breakdown (both modes, t_plus_2)
    # ------------------------------------------------------------------
    for definition in DEFINITIONS:
        report["per_year"][definition] = {}
        for thr in THRESHOLDS:
            report["per_year"][definition][str(thr)] = {}
            for mode in MODES:
                detail = results[definition][thr]["t_plus_2"][mode]["_detail_full"]
                per_year = {}
                for d in detail:
                    yr = d["entry_date"][:4]
                    rec = per_year.setdefault(
                        yr, {"n": 0, "pnl_usd": 0.0, "wins": 0, "n_short": 0}
                    )
                    rec["n"] += 1
                    rec["pnl_usd"] += d["pnl_usd"]
                    rec["wins"] += 1 if d["pnl_usd"] > 0 else 0
                    rec["n_short"] += 1 if d["direction"] == "short" else 0
                for yr, rec in per_year.items():
                    rec["pnl_usd"] = round(rec["pnl_usd"], 2)
                    rec["win_rate_pct"] = round(100.0 * rec["wins"] / rec["n"], 1)
                report["per_year"][definition][str(thr)][mode] = dict(
                    sorted(per_year.items())
                )

    print("\n  分年 (market_close, T+2, long_short):")
    for thr in THRESHOLDS:
        py = report["per_year"]["market_close"][str(thr)]["long_short"]
        row = " ".join(
            f"{yr}:{rec['n']}({rec['pnl_usd']:+.0f}$)" for yr, rec in py.items()
        )
        print(f"    {thr:.0f}%: {row}")
    print("\n  分年 (market_close, T+2, long_only):")
    for thr in THRESHOLDS:
        py = report["per_year"]["market_close"][str(thr)]["long_only"]
        row = " ".join(
            f"{yr}:{rec['n']}({rec['pnl_usd']:+.0f}$)" for yr, rec in py.items()
        )
        print(f"    {thr:.0f}%: {row}")

    # ------------------------------------------------------------------
    # Event detail for the JSON
    # ------------------------------------------------------------------
    for definition in DEFINITIONS:
        report["events_detail"][definition] = {}
        for thr in THRESHOLDS:
            events = events_by_def_thr[(definition, thr)]
            report["events_detail"][definition][str(thr)] = [
                {
                    "date": e["entry_date"],
                    "dow": ["Mon", "Tue", "Wed", "Thu", "Fri"][e["dayofweek"]],
                    "btc_ret_pct": e["btc_ret_pct"],
                    "direction": e["direction"],
                    "score": round(e["score"], 3),
                    "skipped_legs": e["skipped_legs"],
                    "legs": [
                        {
                            "sym": lg["symbol"],
                            "entry": round(lg["entry_price"], 2),
                            "exit_t1": round(lg["exit_price_1"], 2),
                            "exit_t2": round(lg["exit_price_2"], 2),
                            "ret_t1_pct": round(
                                (1 if e["direction"] == "long" else -1)
                                * (lg["exit_price_1"] / lg["entry_price"] - 1) * 100, 2),
                            "ret_t2_pct": round(
                                (1 if e["direction"] == "long" else -1)
                                * (lg["exit_price_2"] / lg["entry_price"] - 1) * 100, 2),
                        }
                        for lg in e["legs"]
                    ],
                }
                for e in events
            ]

    out_path = Path("reports/overnight_gap_backtest.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    print(f"\nJSON 已写入: {out_path}")


if __name__ == "__main__":
    main()
