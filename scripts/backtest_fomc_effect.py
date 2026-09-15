#!/usr/bin/env python3
"""FOMC calendar effect: crypto-concept stocks, BTC, and QQQ around Fed decision days.

Hypothesis (from literature, e.g. Lucca & Moench 2015 "pre-FOMC drift"):
US equities earn outsized returns in the hours before the 14:00 ET rate
announcement. Crypto-concept stocks (MSTR/MARA/RIOT/COIN) are a high-beta
risk-appetite corner — do they behave differently from QQQ around FOMC?

Method: descriptive event study. No ML, no significance claims.
- Windows T-3..T+2 daily returns around each decision day
  (trading-day offsets for stocks, calendar-day offsets for BTC).
- Buckets by rate action (hike/cut/hold/unknown). Buckets are small
  (n=11..51) — descriptive statistics only.
- BTC 1h spot data gives an exact decomposition around the 14:00 ET
  announcement (pre-24h vs post-24h), 2019+ only.
- Multiple-comparison guard: 89 events x 6 windows x 3 asset views means
  some cells will look "significant" by chance. A real effect should be
  consistent across adjacent windows and adjacent assets; isolated
  single-cell miracles are treated as noise.

Data: data/btc_history.db (stocks/etf/spot series; split-adjusted —
verified MSTR 10:1 split of 2024-08-08 is continuous in this data).

Usage:
    poetry run python scripts/backtest_fomc_effect.py
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from statistics import mean, median
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "data" / "btc_history.db"
OUT = ROOT / "reports" / "fomc_effect.json"

STOCKS = ["MSTR", "MARA", "RIOT", "COIN"]

# ---------------------------------------------------------------------------
# FOMC decision calendar 2016-2026 (decision day = second day of meeting).
# Source: federalreserve.gov historical meeting dates + rate decisions.
# confidence: "high" = certain from public record; "medium" = from memory,
# plausible but not double-checked; action "unknown" = not verifiable.
# Note: the regularly scheduled 2020-03-17/18 meeting was CANCELLED after
# the two emergency actions; both emergency decisions are included.
# ---------------------------------------------------------------------------
FOMC_EVENTS = [
    # (decision_date, action, bp, scheduled, date_conf, action_conf, note)
    ("2016-01-27", "hold", 0, True, "high", "high", ""),
    ("2016-03-16", "hold", 0, True, "high", "high", ""),
    ("2016-04-27", "hold", 0, True, "high", "high", ""),
    ("2016-06-15", "hold", 0, True, "high", "high", ""),
    ("2016-07-27", "hold", 0, True, "high", "high", ""),
    ("2016-09-21", "hold", 0, True, "high", "high", ""),
    ("2016-11-02", "hold", 0, True, "high", "high", ""),
    ("2016-12-14", "hike", 25, True, "high", "high", "first hike of cycle"),
    ("2017-02-01", "hold", 0, True, "high", "high", ""),
    ("2017-03-15", "hike", 25, True, "high", "high", ""),
    ("2017-05-03", "hold", 0, True, "high", "high", ""),
    ("2017-06-14", "hike", 25, True, "high", "high", ""),
    ("2017-07-26", "hold", 0, True, "high", "high", ""),
    ("2017-09-20", "hold", 0, True, "high", "high", "balance-sheet runoff start announced"),
    ("2017-11-01", "hold", 0, True, "high", "high", ""),
    ("2017-12-13", "hike", 25, True, "high", "high", ""),
    ("2018-01-31", "hold", 0, True, "high", "high", ""),
    ("2018-03-21", "hike", 25, True, "high", "high", ""),
    ("2018-05-02", "hold", 0, True, "high", "high", ""),
    ("2018-06-13", "hike", 25, True, "high", "high", ""),
    ("2018-08-01", "hold", 0, True, "high", "high", ""),
    ("2018-09-26", "hike", 25, True, "high", "high", ""),
    ("2018-11-08", "hold", 0, True, "high", "high", ""),
    ("2018-12-19", "hike", 25, True, "high", "high", ""),
    ("2019-01-30", "hold", 0, True, "high", "high", ""),
    ("2019-03-20", "hold", 0, True, "high", "high", ""),
    ("2019-04-30", "hold", 0, True, "high", "high", ""),
    ("2019-06-19", "hold", 0, True, "high", "high", ""),
    ("2019-07-31", "cut", 25, True, "high", "high", "first cut since 2008"),
    ("2019-09-18", "cut", 25, True, "high", "high", ""),
    ("2019-10-30", "cut", 25, True, "high", "high", ""),
    ("2019-12-11", "hold", 0, True, "high", "high", ""),
    ("2020-01-29", "hold", 0, True, "high", "high", ""),
    ("2020-03-03", "cut", 50, False, "high", "high", "EMERGENCY cut, unscheduled 1-day meeting"),
    ("2020-03-15", "cut", 100, False, "high", "high", "EMERGENCY cut to 0-0.25, announced Sunday; 3/17-18 meeting cancelled"),
    ("2020-04-29", "hold", 0, True, "high", "high", ""),
    ("2020-06-10", "hold", 0, True, "high", "high", ""),
    ("2020-07-29", "hold", 0, True, "high", "high", ""),
    ("2020-09-16", "hold", 0, True, "high", "high", ""),
    ("2020-11-05", "hold", 0, True, "high", "high", ""),
    ("2020-12-16", "hold", 0, True, "high", "high", ""),
    ("2021-01-27", "hold", 0, True, "high", "high", ""),
    ("2021-03-17", "hold", 0, True, "high", "high", ""),
    ("2021-04-28", "hold", 0, True, "high", "high", ""),
    ("2021-06-16", "hold", 0, True, "high", "high", ""),
    ("2021-07-28", "hold", 0, True, "high", "high", ""),
    ("2021-09-22", "hold", 0, True, "high", "high", ""),
    ("2021-11-03", "hold", 0, True, "high", "high", "taper announced"),
    ("2021-12-15", "hold", 0, True, "high", "high", "taper accelerated"),
    ("2022-01-26", "hold", 0, True, "high", "high", ""),
    ("2022-03-16", "hike", 25, True, "high", "high", "first hike of cycle"),
    ("2022-05-04", "hike", 50, True, "high", "high", ""),
    ("2022-06-15", "hike", 75, True, "high", "high", "first 75bp since 1994"),
    ("2022-07-27", "hike", 75, True, "high", "high", ""),
    ("2022-09-21", "hike", 75, True, "high", "high", ""),
    ("2022-11-02", "hike", 75, True, "high", "high", ""),
    ("2022-12-14", "hike", 50, True, "high", "high", ""),
    ("2023-02-01", "hike", 25, True, "high", "high", ""),
    ("2023-03-22", "hike", 25, True, "high", "high", ""),
    ("2023-05-03", "hike", 25, True, "high", "high", ""),
    ("2023-06-14", "hold", 0, True, "high", "high", "skip after banking stress"),
    ("2023-07-26", "hike", 25, True, "high", "high", "last hike of cycle"),
    ("2023-09-20", "hold", 0, True, "high", "high", ""),
    ("2023-11-01", "hold", 0, True, "high", "high", ""),
    ("2023-12-13", "hold", 0, True, "high", "high", ""),
    ("2024-01-31", "hold", 0, True, "high", "high", ""),
    ("2024-03-20", "hold", 0, True, "high", "high", ""),
    ("2024-05-01", "hold", 0, True, "high", "high", ""),
    ("2024-06-12", "hold", 0, True, "high", "high", ""),
    ("2024-07-31", "hold", 0, True, "high", "high", ""),
    ("2024-09-18", "cut", 50, True, "high", "high", "first cut of cycle, 50bp"),
    ("2024-11-07", "cut", 25, True, "high", "high", ""),
    ("2024-12-18", "cut", 25, True, "high", "high", ""),
    ("2025-01-29", "hold", 0, True, "high", "high", ""),
    ("2025-03-19", "hold", 0, True, "high", "high", ""),
    ("2025-05-07", "hold", 0, True, "high", "medium", ""),
    ("2025-06-18", "hold", 0, True, "high", "medium", ""),
    ("2025-07-30", "hold", 0, True, "high", "medium", ""),
    ("2025-09-17", "cut", 25, True, "high", "medium", "first cut of 2025"),
    ("2025-10-29", "cut", 25, True, "high", "medium", ""),
    ("2025-12-10", "cut", 25, True, "high", "medium", ""),
    # 2026: scheduled dates from the Fed's published calendar (memory,
    # medium confidence); decisions UNKNOWN (post-training) -> excluded
    # from action buckets, included in all-events windows.
    ("2026-01-28", "unknown", None, True, "medium", "unknown", "2026 decision unknown"),
    ("2026-03-18", "unknown", None, True, "medium", "unknown", "2026 decision unknown"),
    ("2026-04-29", "unknown", None, True, "medium", "unknown", "2026 decision unknown"),
    ("2026-06-17", "unknown", None, True, "medium", "unknown", "2026 decision unknown"),
    ("2026-07-29", "unknown", None, True, "medium", "unknown", "2026 decision unknown"),
    ("2026-09-16", "unknown", None, True, "medium", "unknown", "decision due 2026-09-16, beyond data"),
    ("2026-10-28", "unknown", None, True, "medium", "unknown", "future"),
    ("2026-12-09", "unknown", None, True, "medium", "unknown", "future"),
]

WINDOWS = ["T-3", "T-2", "T-1", "T", "T+1", "T+2"]
NY = ZoneInfo("America/New_York")


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
def load_daily(symbol: str, market_type: str) -> pd.DataFrame:
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    df = pd.read_sql(
        "SELECT ts, open, close FROM ohlcv "
        "WHERE symbol=? AND market_type=? AND timeframe='1d'",
        con, params=(symbol, market_type),
    )
    con.close()
    df["date"] = pd.to_datetime(df["ts"], unit="ms").dt.normalize()
    return df.set_index("date").sort_index()[["open", "close"]]


def load_btc_1h() -> pd.DataFrame:
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    df = pd.read_sql(
        "SELECT ts, open FROM ohlcv "
        "WHERE symbol='BTC/USDT' AND market_type='spot' AND timeframe='1h'",
        con,
    )
    con.close()
    df["dt"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    return df.set_index("dt").sort_index()[["open"]]


# ---------------------------------------------------------------------------
# Stats helpers (descriptive only)
# ---------------------------------------------------------------------------
def describe(values: list[float]) -> dict:
    if not values:
        return {"n": 0}
    arr = np.asarray(values, dtype=float)
    sd = float(arr.std(ddof=1)) if len(arr) > 1 else None
    return {
        "n": int(len(arr)),
        "mean_pct": round(float(arr.mean()), 3),
        "median_pct": round(float(np.median(arr)), 3),
        "std_pct": round(sd, 3) if sd is not None else None,
        "hit_rate": round(float((arr > 0).mean()), 3),
        # naive t-stat, DESCRIPTIVE ONLY (windows overlap, multiple comparisons)
        "tstat_naive": round(float(arr.mean() / (sd / np.sqrt(len(arr)))), 2)
        if sd and sd > 0
        else None,
    }


def main() -> None:
    # --- load prices ---
    stock_px = {s: load_daily(s, "stocks") for s in STOCKS}
    qqq = load_daily("QQQ", "etf")
    btc_d = load_daily("BTC/USDT", "spot")
    btc_1h = load_btc_1h()

    # master trading calendar from QQQ (all series share it)
    cal = qqq.index
    cal_set = set(cal)

    # daily close-to-close returns per stock (on trading calendar)
    stock_ret = {}
    for s in STOCKS:
        px = stock_px[s]
        r = (px["close"] / px["close"].shift(1) - 1) * 100
        stock_ret[s] = r

    # equal-weight basket of available constituents
    ret_frame = pd.concat([stock_ret[s].rename(s) for s in STOCKS], axis=1)
    basket_ret = ret_frame.mean(axis=1, skipna=True)
    basket_n = ret_frame.notna().sum(axis=1)

    qqq_ret = (qqq["close"] / qqq["close"].shift(1) - 1) * 100
    basket_ex_qqq = basket_ret - qqq_ret  # relative behaviour vs market

    btc_ret = (btc_d["close"] / btc_d["close"].shift(1) - 1) * 100

    baselines = {
        "basket": round(float(basket_ret.mean()), 4),
        "MSTR": round(float(stock_ret["MSTR"].mean()), 4),
        "MARA": round(float(stock_ret["MARA"].mean()), 4),
        "RIOT": round(float(stock_ret["RIOT"].mean()), 4),
        "COIN": round(float(stock_ret["COIN"].mean()), 4),
        "BTC": round(float(btc_ret.mean()), 4),
        "QQQ": round(float(qqq_ret.mean()), 4),
    }

    # --- event table ---
    events = []
    for (d, action, bp, sched, dconf, aconf, note) in FOMC_EVENTS:
        ev = {
            "date": d,
            "action": action,
            "bp": bp,
            "scheduled": sched,
            "date_confidence": dconf,
            "action_confidence": aconf,
            "note": note,
        }
        dt = datetime.strptime(d, "%Y-%m-%d").date()

        # stocks: map to next trading day if decision day not a trading day
        tdx = None
        for i, c in enumerate(cal):
            if c.date() >= dt:
                tdx = i
                break
        ev["stock_day_T"] = str(cal[tdx].date()) if tdx is not None else None
        ev["stock_day_T_mapped"] = bool(
            tdx is not None and cal[tdx].date() != dt
        )
        ev["stocks_coverage"] = bool(
            tdx is not None and 3 <= tdx <= len(cal) - 3
        )
        ev["btc_coverage"] = bool(
            btc_d.index.min().date() <= dt + timedelta(days=2)
            and btc_d.index.max().date() >= dt + timedelta(days=2)
        )
        events.append(ev)

    # --- window stats (all events) ---
    def stock_window_vals(col: pd.Series):
        vals = {w: [] for w in WINDOWS}
        cum_pre, cum_post = [], []
        for ev in events:
            if not ev["stocks_coverage"]:
                continue
            d = datetime.strptime(ev["stock_day_T"], "%Y-%m-%d")
            tdx = cal.get_loc(d)
            idxs = {"T-3": tdx - 3, "T-2": tdx - 2, "T-1": tdx - 1,
                    "T": tdx, "T+1": tdx + 1, "T+2": tdx + 2}
            for w, i in idxs.items():
                v = col.get(cal[i])
                if v is not None and not (isinstance(v, float) and np.isnan(v)):
                    vals[w].append(float(v))
            # cumulative windows (pure pre-announcement vs pure post)
            r_pre = [col.get(cal[i]) for i in (idxs["T-3"], idxs["T-2"], idxs["T-1"])]
            r_post = [col.get(cal[i]) for i in (idxs["T+1"], idxs["T+2"])]
            if all(v is not None and not np.isnan(v) for v in r_pre):
                cum_pre.append(((1 + np.array(r_pre) / 100).prod() - 1) * 100)
            if all(v is not None and not np.isnan(v) for v in r_post):
                cum_post.append(((1 + np.array(r_post) / 100).prod() - 1) * 100)
        return vals, cum_pre, cum_post

    def btc_window_vals():
        vals = {w: [] for w in WINDOWS}
        for ev in events:
            if not ev["btc_coverage"]:
                continue
            dt = datetime.strptime(ev["date"], "%Y-%m-%d").date()
            offsets = {"T-3": -3, "T-2": -2, "T-1": -1,
                       "T": 0, "T+1": 1, "T+2": 2}
            for w, off in offsets.items():
                d = dt + timedelta(days=off)
                ts = pd.Timestamp(d)
                v = btc_ret.get(ts)
                if v is not None and not np.isnan(v):
                    vals[w].append(float(v))
        return vals

    window_stats = {}
    for name, col in [
        ("crypto_basket", basket_ret),
        ("QQQ", qqq_ret),
        ("basket_minus_QQQ", basket_ex_qqq),
        ("MSTR", stock_ret["MSTR"]),
        ("MARA", stock_ret["MARA"]),
        ("RIOT", stock_ret["RIOT"]),
        ("COIN", stock_ret["COIN"]),
    ]:
        vals, cum_pre, cum_post = stock_window_vals(col)
        window_stats[name] = {
            **{w: describe(vals[w]) for w in WINDOWS},
            "cum_T-3_to_T-1": describe(cum_pre),
            "cum_T+1_to_T+2": describe(cum_post),
            "baseline_daily_pct": baselines.get(name if name != "crypto_basket" else "basket"),
        }
    window_stats["BTC"] = {
        **{w: describe(v) for w, v in btc_window_vals().items()},
        "baseline_daily_pct": baselines["BTC"],
    }

    # --- decision-day OHLC decomposition (basket & QQQ, using open prices) ---
    def day_T_decomposition():
        out = {"crypto_basket": [], "QQQ": []}
        opens = {"crypto_basket": None, "QQQ": qqq}
        # basket open: average of constituent opens (equal weight, those with data)
        stock_opens = pd.concat(
            [stock_px[s]["open"].rename(s) for s in STOCKS], axis=1
        )
        opens["crypto_basket"] = stock_opens.mean(axis=1, skipna=True)
        for ev in events:
            if not ev["stocks_coverage"]:
                continue
            d = pd.Timestamp(ev["stock_day_T"])
            d_prev = cal[cal.get_loc(d) - 1]
            for name in opens:
                o_t = (opens[name].get(d) if name == "crypto_basket"
                       else qqq["open"].get(d))
                if o_t is None or np.isnan(o_t):
                    continue
                if name == "QQQ":
                    c_prev, c_t = qqq["close"].get(d_prev), qqq["close"].get(d)
                else:
                    # basket closes = mean of constituent closes
                    cs = [stock_px[s]["close"].get(d) for s in STOCKS]
                    cs = [c for c in cs if c is not None and not np.isnan(c)]
                    cp = [stock_px[s]["close"].get(d_prev) for s in STOCKS]
                    cp = [c for c in cp if c is not None and not np.isnan(c)]
                    c_t = mean(cs) if cs else None
                    c_prev = mean(cp) if cp else None
                if c_t is None or c_prev is None or np.isnan(c_t) or np.isnan(c_prev):
                    continue
                out[name].append({
                    "full_day_pct": (c_t / c_prev - 1) * 100,   # close(T-1)->close(T)
                    "overnight_gap_pct": (o_t / c_prev - 1) * 100,  # close(T-1)->open(T)
                    "intraday_pct": (c_t / o_t - 1) * 100,       # open(T)->close(T), contains 14:00 announcement
                })
        return out

    decomp_raw = day_T_decomposition()
    decomposition = {
        name: {k: describe([r[k] for r in rows]) for k in
               ("full_day_pct", "overnight_gap_pct", "intraday_pct")}
        for name, rows in decomp_raw.items()
    }

    # --- buckets by rate action ---
    buckets = {}
    for action in ("hike", "cut", "hold", "unknown"):
        sel = [e for e in events if e["action"] == action and e["stocks_coverage"]]
        b = {"n_events": len(sel)}
        for name, col in [
            ("crypto_basket", basket_ret), ("QQQ", qqq_ret),
            ("basket_minus_QQQ", basket_ex_qqq),
        ]:
            cells = {}
            for w in ("T-1", "T", "T+1"):
                vals = []
                for ev in sel:
                    d = pd.Timestamp(ev["stock_day_T"])
                    i = cal.get_loc(d) + (WINDOWS.index(w) - 3)
                    v = col.get(cal[i])
                    if v is not None and not np.isnan(v):
                        vals.append(float(v))
                cells[w] = describe(vals)
            b[name] = cells
        # BTC bucket
        cells = {}
        for w in ("T-1", "T", "T+1"):
            vals = []
            for ev in sel:
                d = pd.Timestamp(datetime.strptime(ev["date"], "%Y-%m-%d").date()
                                 + timedelta(days=WINDOWS.index(w) - 3))
                v = btc_ret.get(d)
                if v is not None and not np.isnan(v):
                    vals.append(float(v))
            cells[w] = describe(vals)
        b["BTC"] = cells
        buckets[action] = b

    # --- BTC exact announcement windows (1h data, 2019+) ---
    def btc_announcement_windows():
        rows = []
        for ev in events:
            dt = datetime.strptime(ev["date"], "%Y-%m-%d").date()
            t_ann = datetime(dt.year, dt.month, dt.day, 14, 0,
                             tzinfo=NY).astimezone(timezone.utc)

            def px_at(t):
                ts = pd.Timestamp(t)
                if ts in btc_1h.index:
                    return float(btc_1h.loc[ts, "open"])
                idx = btc_1h.index.searchsorted(ts)
                if idx == 0:
                    return None
                prev_ts = btc_1h.index[idx - 1]
                if ts - prev_ts > timedelta(hours=2):
                    return None
                return float(btc_1h["open"].iloc[idx - 1])

            p_pre, p_ann = px_at(t_ann - timedelta(hours=24)), px_at(t_ann)
            p_24, p_4 = px_at(t_ann + timedelta(hours=24)), px_at(t_ann + timedelta(hours=4))
            if None in (p_pre, p_ann, p_24, p_4):
                continue
            rows.append({
                "date": ev["date"],
                "action": ev["action"],
                "pre_24h_pct": round((p_ann / p_pre - 1) * 100, 3),
                "post_4h_pct": round((p_4 / p_ann - 1) * 100, 3),
                "post_24h_pct": round((p_24 / p_ann - 1) * 100, 3),
            })
        return rows

    btc_rows = btc_announcement_windows()
    btc_hourly = {
        "note": "exact windows around 14:00 ET announcement, BTC spot 1h opens; 2019+",
        "pre_24h": describe([r["pre_24h_pct"] for r in btc_rows]),
        "post_4h": describe([r["post_4h_pct"] for r in btc_rows]),
        "post_24h": describe([r["post_24h_pct"] for r in btc_rows]),
        "per_event": btc_rows,
    }

    # --- robustness: era splits (is the headline driven by one regime?) ---
    def basket_cell(ev, off):
        d = pd.Timestamp(ev["stock_day_T"])
        v = basket_ret.get(cal[cal.get_loc(d) + off])
        return float(v) if v is not None and not np.isnan(v) else None

    def btc_cell(ev, off):
        d = pd.Timestamp(datetime.strptime(ev["date"], "%Y-%m-%d").date()
                         + timedelta(days=off))
        v = btc_ret.get(d)
        return float(v) if v is not None and not np.isnan(v) else None

    era_split = {}
    for ename, (y0, y1) in {"2016-2019": (2016, 2019),
                            "2020-2022": (2020, 2022),
                            "2023-2026": (2023, 2026)}.items():
        sel = [e for e in events
               if y0 <= int(e["date"][:4]) <= y1 and e["stocks_coverage"]]
        era_split[ename] = {
            "n": len(sel),
            "basket_T": describe([v for v in (basket_cell(e, 0) for e in sel) if v is not None]),
            "basket_T+1": describe([v for v in (basket_cell(e, 1) for e in sel) if v is not None]),
            "basket_T+2": describe([v for v in (basket_cell(e, 2) for e in sel) if v is not None]),
            "BTC_T": describe([v for v in (btc_cell(e, 0) for e in sel) if v is not None]),
        }
    era_split["BTC_pre24h_by_era"] = {
        ename: describe([r["pre_24h_pct"] for r in btc_rows
                         if y0 <= int(r["date"][:4]) <= y1])
        for ename, (y0, y1) in {"2019-2022": (2019, 2022),
                                "2023-2026": (2023, 2026)}.items()
    }

    # 2026 events: per-event detail (action unknown, regime check)
    events_2026 = []
    for e in events:
        if not e["date"].startswith("2026") or not e["stocks_coverage"]:
            continue
        v = basket_cell(e, 0)
        events_2026.append({"date": e["date"], "basket_T_pct": v})

    # --- tradeability screen ---
    # best simple candidate: basket day-T excess vs baseline, and T->T+1
    b_T = window_stats["crypto_basket"]["T"]
    b_T1 = window_stats["crypto_basket"]["T+1"]
    q_T = window_stats["QQQ"]["T"]
    ex_T = window_stats["basket_minus_QQQ"]["T"]
    edge_T = b_T["mean_pct"] - baselines["basket"]
    edge_T1 = b_T1["mean_pct"] - baselines["basket"]
    tradeability = {
        "events_per_year": 8,
        "basket_T_mean_pct": b_T.get("mean_pct"),
        "basket_T_excess_vs_baseline_pct": round(edge_T, 3) if edge_T else None,
        "basket_T_excess_vs_QQQ_same_day_pct": ex_T.get("mean_pct"),
        "basket_T+1_mean_pct": b_T1.get("mean_pct"),
        "basket_T+1_excess_vs_baseline_pct": round(edge_T1, 3) if edge_T1 is not None else None,
        "benchmark_weekend_gap": {"events_per_year": 9, "mean_per_trade_pct": 5.3},
        "rule_of_thumb": "edge_per_event(%) x events/yr vs weekend_gap 4-5%/yr cadence; require consistency across adjacent windows before believing any single cell",
    }

    report = {
        "meta": {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "method": "descriptive FOMC event study; no ML; naive t-stats are "
                      "descriptive only (overlapping windows, massive multiple comparisons)",
            "fomc_calendar_source": "hardcoded from federalreserve.gov public record; "
                                    "2025 action confidence=medium, 2026 actions unknown",
            "n_events_total": len(FOMC_EVENTS),
            "stock_data": "MSTR/MARA/RIOT 2016-09-12+, COIN 2021-04-14+, QQQ 2016-09-12+ (split-adjusted, verified MSTR 2024-08-08)",
            "btc_data": "BTC/USDT spot 1d 2018-12-31+, 1h 2019-01-01+",
            "windows": "stocks: trading-day offsets; BTC: calendar-day offsets; "
                       "non-trading decision days (2020-03-15) mapped to next trading day for stocks",
            "multiple_comparison_warning": "89 events x 6 windows x 7 series = ~3700 cells; "
                                          "expect a few 'significant-looking' cells by chance; "
                                          "trust only patterns consistent across adjacent windows/assets",
        },
        "baselines_daily_pct": baselines,
        "events": events,
        "window_stats": window_stats,
        "day_T_decomposition": decomposition,
        "buckets_by_action": buckets,
        "era_split_robustness": era_split,
        "events_2026_detail": events_2026,
        "btc_announcement_windows_hourly": btc_hourly,
        "tradeability_screen": tradeability,
    }

    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text(json.dumps(report, indent=2, default=str))
    print(f"wrote {OUT}")

    # --- console summary ---
    print("\n=== Baseline mean daily return (%) ===")
    for k, v in baselines.items():
        print(f"  {k:18s} {v:+.4f}")

    print("\n=== Event-window mean daily return (% | hit rate | n) ===")
    hdr = f"{'series':18s}" + "".join(f"{w:>16s}" for w in WINDOWS)
    print(hdr)
    for name in ("crypto_basket", "QQQ", "basket_minus_QQQ", "MSTR", "MARA",
                 "RIOT", "COIN", "BTC"):
        s = window_stats[name]
        row = f"{name:18s}"
        for w in WINDOWS:
            c = s[w]
            row += f"{c.get('mean_pct', float('nan')):+7.2f}/{c.get('hit_rate', float('nan')):.2f}/{c.get('n', 0):<5d}"
        print(row)

    print("\n=== Cumulative windows ===")
    for name in ("crypto_basket", "QQQ"):
        s = window_stats[name]
        print(f"  {name:18s} cum T-3..T-1 {s['cum_T-3_to_T-1'].get('mean_pct', float('nan')):+.2f}%"
              f" | cum T+1..T+2 {s['cum_T+1_to_T+2'].get('mean_pct', float('nan')):+.2f}%")

    print("\n=== Day-T decomposition (basket) ===")
    d = decomposition["crypto_basket"]
    for k in ("overnight_gap_pct", "intraday_pct", "full_day_pct"):
        c = d[k]
        print(f"  {k:20s} {c.get('mean_pct', float('nan')):+.3f}%  hit {c.get('hit_rate')}  n={c.get('n')}")

    print("\n=== BTC exact announcement windows (1h) ===")
    for k in ("pre_24h", "post_4h", "post_24h"):
        c = btc_hourly[k]
        print(f"  {k:10s} {c.get('mean_pct', float('nan')):+.3f}%  hit {c.get('hit_rate')}  n={c.get('n')}")

    print("\n=== Era splits (mean %, hit, n) ===")
    for ename, s in era_split.items():
        if ename == "BTC_pre24h_by_era":
            for en2, c in s.items():
                print(f"  BTC pre24h {en2:8s} {c.get('mean_pct', float('nan')):+.3f}%  hit {c.get('hit_rate')}  n={c.get('n')}")
        else:
            print(f"  {ename}: basket T {s['basket_T'].get('mean_pct'):+.2f} (hit {s['basket_T'].get('hit_rate')}, n={s['basket_T'].get('n')})"
                  f" | T+1 {s['basket_T+1'].get('mean_pct'):+.2f} (hit {s['basket_T+1'].get('hit_rate')})"
                  f" | T+2 {s['basket_T+2'].get('mean_pct'):+.2f} (hit {s['basket_T+2'].get('hit_rate')})"
                  f" | BTC T {s['BTC_T'].get('mean_pct'):+.2f}")

    print("\n=== 2026 events (action unknown, regime check) ===")
    for e in events_2026:
        print(f"  {e['date']}  basket day-T {e['basket_T_pct']:+.2f}%")

    print("\n=== Buckets (mean % T-1 / T / T+1) ===")
    for action, b in buckets.items():
        for name in ("crypto_basket", "QQQ", "BTC"):
            cells = b[name]
            vals = " / ".join(
                f"{cells[w].get('mean_pct', float('nan')):+.2f}" for w in ("T-1", "T", "T+1")
            )
            print(f"  {action:8s} {name:18s} {vals}   (n={b['n_events']})")


if __name__ == "__main__":
    main()
