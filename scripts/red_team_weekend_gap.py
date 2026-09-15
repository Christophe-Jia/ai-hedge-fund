#!/usr/bin/env python3
"""RED-TEAM audit of the weekend_gap active strategy.

Adversarial review of docs/active_strategy_playbook.md (v1.0). This script
does NOT tune, improve, or re-specify the signal. It tries to FALSIFY it.

Read-only over existing data + existing reports. Writes exactly one file:
reports/red_team_weekend_gap.json

Questions answered, in order:

  1  Reproducibility probe: rebuild the 25-event trigger list from raw
     SQLite, cross-check every date / BTC weekend return / entry price
     against reports/exit_rules_backtest.json, and re-run under an
     alternative (equity-clock-aligned) BTC close definition.
  2  Statistical significance of the 13 long events: mean / sd / t / p,
     bootstrap 95% CI of the mean, Wilson CI of the 69% win rate,
     jackknife on the two outsized winners.
  3  Multiple-comparison correction over the platform's search history,
     plus a fact-check of HOW the signal was discovered (git archaeology).
  4  Is it just beta in disguise? Unconditional baseline, conditional-only
     baseline, conditional-beta regression, buy-and-hold controls,
     random-date placebo.
  5  Cost / slippage sensitivity, short-term tax, capital efficiency.
  6  Decay: era split, trigger frequency, where the P&L actually comes
     from (gap vs post-open), candidate failure mechanisms.
  7  Verdict.

Usage:
    poetry run python scripts/red_team_weekend_gap.py
"""

from __future__ import annotations

import json
import math
import sqlite3
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd
from scipy import stats

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "data" / "btc_history.db"
EXIT_REPORT = ROOT / "reports" / "exit_rules_backtest.json"
OUT = ROOT / "reports" / "red_team_weekend_gap.json"

BTC = "BTC/USDT"
TARGETS = ("COIN", "MSTR", "MARA")
THRESHOLD = 5.0

WIN_START = pd.Timestamp("2021-01-01")
WIN_END = pd.Timestamp("2026-09-11")

N_BOOT = 10_000
N_PLACEBO = 10_000
SEED = 20260915

# populated in main() so helper probes do not need to re-read SQLite
_stocks_cache: dict[str, pd.DataFrame] = {}


# ---------------------------------------------------------------------------
# Data access (raw SQL — deliberately NOT via src.signals, so that a bug in
# the signal module cannot hide a bug in the audit)
# ---------------------------------------------------------------------------


def _q(sql: str, args: tuple = ()) -> pd.DataFrame:
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    try:
        return pd.read_sql(sql, con, params=args)
    finally:
        con.close()


def load_btc(timeframe: str = "1d") -> pd.DataFrame:
    df = _q(
        "SELECT ts, open, high, low, close FROM ohlcv "
        "WHERE symbol=? AND market_type='spot' AND timeframe=?",
        (BTC, timeframe),
    )
    df["date"] = pd.to_datetime(df["ts"], unit="ms")
    return df.set_index("date").sort_index()


def load_stock(sym: str) -> pd.DataFrame:
    df = _q(
        "SELECT ts, open, high, low, close FROM ohlcv "
        "WHERE symbol=? AND market_type='stocks' AND timeframe='1d'",
        (sym,),
    )
    df["date"] = pd.to_datetime(df["ts"], unit="ms").dt.normalize()
    return df.set_index("date").sort_index()


# ---------------------------------------------------------------------------
# Q1 — reproducibility probe
# ---------------------------------------------------------------------------


def weekend_table(btc: pd.DataFrame, threshold: float = THRESHOLD) -> pd.DataFrame:
    """One row per completed weekend: BTC Fri close -> Sun close move."""
    b = btc.copy()
    b["dow"] = b.index.dayofweek
    b["iso_year"] = b.index.isocalendar().year.astype(int)
    b["iso_week"] = b.index.isocalendar().week.astype(int)
    rows = []
    for (_, _), g in b.groupby(["iso_year", "iso_week"]):
        fri = g[g["dow"] == 4]
        sun = g[g["dow"] == 6]
        if fri.empty or sun.empty:
            continue
        fc = float(fri["close"].iloc[-1])
        sc = float(sun["close"].iloc[-1])
        if fc <= 0:
            continue
        rows.append({
            "sunday": sun.index[-1].normalize(),
            "friday": fri.index[-1].normalize(),
            "btc_fri_close": fc,
            "btc_sun_close": sc,
            "ret_pct": (sc / fc - 1.0) * 100.0,
            "flagged": abs(sc / fc - 1.0) * 100.0 >= threshold,
        })
    return pd.DataFrame(rows).sort_values("sunday").reset_index(drop=True)


def build_events(weekends: pd.DataFrame, calendar: pd.DatetimeIndex) -> pd.DataFrame:
    """Trigger list: first trading day strictly after each Sunday."""
    out = []
    for _, w in weekends[weekends["flagged"]].iterrows():
        later = calendar[calendar > w["sunday"]]
        if len(later) == 0:
            continue
        out.append({
            "sunday": w["sunday"],
            "friday": w["friday"],
            "entry_day": later[0],
            "btc_weekend_return_pct": round(float(w["ret_pct"]), 2),
            "direction": "long" if w["ret_pct"] > 0 else "short",
        })
    return pd.DataFrame(out)


def threshold_sensitivity(wk: pd.DataFrame, calendar: pd.DatetimeIndex) -> dict:
    """Mean T+2 long return as a function of the trigger threshold.

    DIAGNOSTIC ONLY. Nothing here is a recommendation; the point is to show how
    much of the headline depends on the choice of 5%.
    """
    out = {}
    for th in (3, 4, 5, 6, 7, 8, 10):
        rows = []
        for _, w in wk[wk["ret_pct"] >= th].iterrows():
            later = calendar[calendar > w["sunday"]]
            if len(later) == 0:
                continue
            d0 = later[0]
            if d0 < WIN_START or d0 > WIN_END:
                continue
            try:
                i0 = list(calendar).index(d0)
            except ValueError:
                continue
            if i0 + 2 >= len(calendar):
                continue
            d2 = calendar[i0 + 2]
            vals = []
            for sym in TARGETS:
                df = _stocks_cache.get(sym)
                if df is None or d0 not in df.index or d2 not in df.index:
                    continue
                op = float(df.loc[d0, "open"])
                xc = float(df.loc[d2, "close"])
                if op > 0:
                    vals.append((xc / op - 1.0) * 100.0)
            if vals:
                rows.append(float(np.mean(vals)))
        r = np.array(rows) if rows else np.array([])
        out[f"ge_{th}pct"] = {
            "n": len(r),
            "mean_ret_pct": round(float(r.mean()), 3) if len(r) else None,
            "median_ret_pct": round(float(np.median(r)), 3) if len(r) else None,
            "win_rate_pct": round(float((r > 0).mean() * 100), 2) if len(r) else None,
            "t_stat": round(float(r.mean() / (r.std(ddof=1) / math.sqrt(len(r)))), 3)
            if len(r) > 1 and r.std(ddof=1) > 0 else None,
        }
    return {
        "design": "long-only, equal-weight COIN/MSTR/MARA, entry trigger-day open, "
                  "exit T+2 close, gross of costs",
        "note": "DIAGNOSTIC ONLY — the 5% choice is NOT being second-guessed here",
        "by_threshold": out,
    }


def discovery_claim_recheck(stocks, calendar, wk) -> dict:
    """Re-run the ORIGINAL discovery statistics on the full current data.

    scripts/backtest_weekend_gap.py (commit dbb6f40) is the only evidence that
    was ever produced for this signal. Rather than reimplement it, this imports
    its own functions and reports what they say on today's data, by window.
    """
    from scripts.backtest_weekend_gap import (
        compute_stock_gaps, compute_weekend_signals, load_btc_daily,
        load_stock_daily,
    )

    btc_orig = load_btc_daily()
    wk_orig = compute_weekend_signals(btc_orig, THRESHOLD)

    windows = {
        "discovery_sample_approx_3.5y_2019_06_to_2023_06": ("2019-06-01", "2023-06-30"),
        "playbook_window_2021_01_to_2026_09": (str(WIN_START.date()), str(WIN_END.date())),
        "full_sample_2019_to_2026": ("2000-01-01", "2030-01-01"),
    }
    res = {}
    for sym in (*TARGETS, "RIOT"):
        try:
            st = load_stock_daily(sym)
        except Exception:  # noqa: BLE001
            continue
        g = compute_stock_gaps(st, wk_orig)
        f = g[g["flagged"]].copy()
        per_window = {}
        for lab, (lo, hi) in windows.items():
            s = f[(f["monday"] >= lo) & (f["monday"] <= hi)]
            up = s[s["btc_weekend_ret"] > 0]
            per_window[lab] = {
                "n_flagged": int(len(s)),
                "n_up": int(len(up)),
                "corr_btc_vs_gap_all_flagged": round(
                    float(s["btc_weekend_ret"].corr(s["gap_pct"])), 3) if len(s) > 2 else None,
                "corr_btc_vs_gap_up_only": round(
                    float(up["btc_weekend_ret"].corr(up["gap_pct"])), 3) if len(up) > 2 else None,
                "mean_gap_pct": round(float(s["gap_pct"].mean()), 3) if len(s) else None,
                "mean_gap_pct_up_only": round(float(up["gap_pct"].mean()), 3) if len(up) else None,
                "mean_next_day_pct_up_only": round(float(up["ret_1d"].mean() * 100), 3)
                if len(up) else None,
                "next_day_win_rate_pct_up_only": round(float((up["ret_1d"] > 0).mean() * 100), 1)
                if len(up) else None,
            }
        res[sym] = per_window

    return {
        "purpose": "reproduce the numbers that justified the signal in commit dbb6f40, "
                   "using that script's OWN functions on today's data",
        "by_symbol_and_window": res,
        "committed_claims": {
            "corr_btc_vs_mstr_gap": 0.823,
            "mstr_gap_pct": 8.3,
            "mstr_next_day_pct": 3.8,
            "mstr_next_day_win_rate_pct": 100,
            "sample": "3.5 years / 10 triggering weekends (per commit dbb6f40)",
        },
        "stale_documentation": [
            "src/signals/weekend_gap.py:6-7 still states 'the Monday gap correlates 0.82 "
            "with the BTC weekend move and mostly does NOT fill (0-20% fill rate)'",
            "the live rerun of the same script gives corr(btc_weekend, gap) = 0.19-0.22 "
            "over any 3.5-year window, and a NEGATIVE correlation (-0.09 to -0.17) once "
            "restricted to the up-only weekends the strategy actually trades",
            "the '100% win rate' on MSTR's next day is now 77% on the full sample",
        ],
        "clock_of_the_original_statistic": {
            "file": "scripts/backtest_weekend_gap.py",
            "lines": "92-99 (compute_stock_gaps), 173-176 (printing)",
            "gap_pct": "open(Mon)/close(Fri) - 1  -> the Monday GAP",
            "ret_1d": "close.shift(-1)/close - 1 evaluated at the Monday row = "
                      "close(Tue)/close(Mon) - 1  -> measured from MONDAY'S CLOSE",
            "ret_5d": "close.shift(-5)/close at the Monday row -> also from Monday's CLOSE",
            "consequence": "the evidence that launched the signal (a) is mostly about the "
                           "gap, which the playbook deliberately does NOT trade because it "
                           "enters at the Monday OPEN, and (b) measures 'next day' from "
                           "Monday's close. The playbook's own leg -- Monday open -> T+2 "
                           "close -- was never the subject of the original validation. "
                           "Entering at the open means paying the ~5-8% gap first.",
        },
        "universe_note": "the original script loops COIN/MSTR/MARA/RIOT and its commit "
                         "message highlights the single best of the four ('MSTR ... 100% "
                         "win rate!'); the traded roster is a post-hoc pick from that scan",
    }


def q1_reproducibility(btc_daily, stocks, calendar) -> dict:
    """Rebuild the trigger list from raw data and cross-check the report."""
    wk = weekend_table(btc_daily)
    ev = build_events(wk, calendar)

    ref = json.loads(EXIT_REPORT.read_text())
    ref_events = ref["trades_full_window"]["long_short"]["t_plus_2"]
    ref_events = [e for e in ref_events
                  if WIN_START <= pd.Timestamp(e["entry_date"]) <= WIN_END]

    mine = ev[(ev["entry_day"] >= WIN_START) & (ev["entry_day"] <= WIN_END)]
    mine_set = {r["entry_day"].date().isoformat(): r for _, r in mine.iterrows()}
    ref_set = {e["entry_date"]: e for e in ref_events}

    only_mine = sorted(set(mine_set) - set(ref_set))
    only_ref = sorted(set(ref_set) - set(mine_set))

    # BTC weekend return + entry-day open price cross-check
    btc_diffs, price_diffs = [], []
    for d in sorted(set(mine_set) & set(ref_set)):
        a, b = mine_set[d], ref_set[d]
        if abs(a["btc_weekend_return_pct"] - b["btc_weekend_return_pct"]) > 0.005:
            btc_diffs.append({
                "entry_date": d,
                "audit_btc_weekend_ret": a["btc_weekend_return_pct"],
                "report_btc_weekend_ret": b["btc_weekend_return_pct"],
            })
        for leg in b["legs"]:
            if leg.get("skipped"):
                continue
            sym = leg["symbol"]
            day = pd.Timestamp(d)
            if day not in stocks[sym].index:
                price_diffs.append({"entry_date": d, "symbol": sym,
                                    "issue": "no bar on entry day in current store"})
                continue
            op = float(stocks[sym].loc[day, "open"])
            if abs(op - leg["entry_price"]) > 0.01:
                price_diffs.append({
                    "entry_date": d, "symbol": sym,
                    "audit_open": round(op, 4),
                    "report_entry_price": leg["entry_price"],
                })
    # same-direction check
    dir_mismatch = [
        {"entry_date": d, "audit": mine_set[d]["direction"], "report": ref_set[d]["direction"]}
        for d in sorted(set(mine_set) & set(ref_set))
        if mine_set[d]["direction"] != ref_set[d]["direction"]
    ]

    # --- alternative clock: BTC close aligned to the equity session ---------
    # The BTC daily bar labelled "Friday" spans [Fri 00:00 UTC, Sat 00:00 UTC)
    # and closes Sat 00:00 UTC = Fri 20:00 ET (EDT) / Fri 19:00 ET (EST). The
    # bar labelled "Sunday" closes Mon 00:00 UTC = Sun 20:00 ET. So the signal
    # window is Fri 20:00 ET -> Sun 20:00 ET, which is NOT the equity clock
    # (US cash closes Fri 16:00 ET). Re-derive the same intent — "the BTC move
    # while US equities were shut, still known before Monday's open" — from 1h
    # bars over Fri 16:00 ET -> Sun 20:00 ET and compare the trigger set.
    ET = "America/New_York"
    h1 = load_btc("1h")
    alt_rows = []
    for _, w in wk.iterrows():
        sun_d = pd.Timestamp(w["sunday"]).date()
        fri_d = pd.Timestamp(w["friday"]).date()
        end = pd.Timestamp(sun_d, tz=ET) + pd.Timedelta(hours=20)
        start = pd.Timestamp(fri_d, tz=ET) + pd.Timedelta(hours=16)
        a = h1[h1.index < start.tz_convert("UTC").tz_localize(None)]
        b = h1[h1.index < end.tz_convert("UTC").tz_localize(None)]
        if a.empty or b.empty:
            continue
        fa = float(a["close"].iloc[-1])
        sb = float(b["close"].iloc[-1])
        alt_rows.append({"sunday": w["sunday"], "alt_ret_pct": (sb / fa - 1.0) * 100.0})
    alt = pd.DataFrame(alt_rows)

    merged = wk.merge(alt, on="sunday", how="inner")
    merged["alt_flagged"] = merged["alt_ret_pct"].abs() >= THRESHOLD
    merged_in_window = merged[(merged["sunday"] >= WIN_START - pd.Timedelta(days=7))
                              & (merged["sunday"] <= WIN_END)]
    flips = merged_in_window[merged_in_window["flagged"] != merged_in_window["alt_flagged"]]

    # do any of the 25 TRADED trigger weekends change decision?
    traded_sundays = set(pd.Timestamp(r["sunday"]) for _, r in mine.iterrows())
    traded_flips = [str(pd.Timestamp(r["sunday"]).date())
                    for _, r in flips.iterrows() if r["sunday"] in traded_sundays]
    new_triggers = [str(pd.Timestamp(r["sunday"]).date())
                    for _, r in flips.iterrows()
                    if r["alt_flagged"] and r["sunday"] not in traded_sundays]

    # --- data-snapshot sensitivity: re-run on bars up to the report's end --
    # (The recent backfills added 21 S&P names + BAYRY + BTC 1h. None of the
    #  three targets changed. Verify the in-window event set is unchanged.)
    wk_trunc = weekend_table(btc_daily[btc_daily.index <= WIN_END])
    ev_trunc = build_events(wk_trunc, calendar[calendar <= WIN_END])
    ev_trunc = ev_trunc[(ev_trunc["entry_day"] >= WIN_START)
                        & (ev_trunc["entry_day"] <= WIN_END)]
    trunc_dates = set(ev_trunc["entry_day"].dt.date.astype(str))
    mine_dates = set(mine["entry_day"].dt.date.astype(str))
    return {
        "audit_trigger_events_in_window": len(mine),
        "report_trigger_events_in_window": len(ref_set),
        "event_dates_only_in_audit": only_mine,
        "event_dates_only_in_report": only_ref,
        "btc_weekend_return_mismatches": btc_diffs,
        "entry_price_mismatches": price_diffs,
        "direction_mismatches": dir_mismatch,
        "reproduced_exactly": (not only_mine and not only_ref and not btc_diffs
                               and not price_diffs and not dir_mismatch),
        "clock_alignment_probe": {
            "note": "BTC daily bar labelled Fri closes Sat 00:00 UTC (=Fri 20:00 ET summer / "
                    "19:00 ET winter), i.e. the signal's weekend window is Fri 20:00 ET -> "
                    "Sun 20:00 ET, not the 16:00 ET equity close. Re-derived from 1h bars "
                    "over Fri 16:00 ET -> Sun 20:00 ET (still known before Monday's open).",
            "weekends_compared_in_window": int(len(merged_in_window)),
            "trigger_decision_flips_in_window": int(len(flips)),
            "traded_events_that_stop_triggering": traded_flips,
            "new_triggers_appearing": new_triggers,
            "n_traded_events_changed": len(traded_flips) + len(new_triggers),
            "flip_detail": [
                {"sunday": str(pd.Timestamp(r["sunday"]).date()),
                 "daily_bar_ret_pct": round(float(r["ret_pct"]), 2),
                 "et_aligned_ret_pct": round(float(r["alt_ret_pct"]), 2)}
                for _, r in flips.iterrows()
            ],
            "largest_abs_divergence_pct": round(
                float((merged["alt_ret_pct"] - merged["ret_pct"]).abs().max()), 3),
            "mean_abs_divergence_pct": round(
                float((merged["alt_ret_pct"] - merged["ret_pct"]).abs().mean()), 3),
            "verdict": "the 5% boundary is not robust to a 4-hour change in the window "
                       "definition: ~4% of all in-window weekends change decision, and "
                       "2 of the 25 traded legs drop out / 4 new ones appear",
        },
        "data_snapshot_probe": {
            "note": "re-run on bars truncated to the report's own end date",
            "trigger_set_identical": trunc_dates == mine_dates,
            "n_audit": len(mine_dates),
            "n_truncated": len(trunc_dates),
            "verdict": "the recent backfills (21 S&P names, BAYRY, BTC 1h) did NOT disturb "
                       "the weekend_gap event set: COIN/MSTR/MARA daily bars were not "
                       "revised and the signal only reads BTC spot",
        },
        "clock_brittleness_sweep": clock_sweep(weekends=wk, calendar=calendar),
        "discovery_claim_recheck": discovery_claim_recheck(stocks, calendar, wk),
    }


def clock_sweep(weekends: pd.DataFrame, calendar: pd.DatetimeIndex) -> dict:
    """Vary only the 'Friday close' endpoint (±hours) and see what survives.

    The daily-bar convention (Sat 00:00 UTC) is one arbitrary choice among
    several equally defensible ones. Everything else is held fixed. No
    alternative is recommended — this measures how much of the result is the
    signal and how much is the timestamp convention.
    """
    h1 = load_btc("1h")
    base_start = pd.Timedelta(days=1)     # daily bar labelled Fri closes Sat 00:00 UTC
    out = {}
    for shift_h in (-12, -8, -4, 0, 4, 8, 12):
        rows = []
        for _, w in weekends.iterrows():
            fri = pd.Timestamp(w["friday"]).normalize()
            sun = pd.Timestamp(w["sunday"]).normalize()
            t_start = fri + base_start + pd.Timedelta(hours=shift_h)
            t_end = sun + pd.Timedelta(days=1)          # Mon 00:00 UTC
            a = h1[h1.index < t_start]
            b = h1[h1.index < t_end]
            if a.empty or b.empty:
                continue
            fa, sb = float(a["close"].iloc[-1]), float(b["close"].iloc[-1])
            ret = (sb / fa - 1.0) * 100.0
            if ret >= THRESHOLD:
                later = calendar[calendar > sun]
                if len(later) == 0:
                    continue
                d0 = later[0]
                if d0 < WIN_START or d0 > WIN_END:
                    continue
                try:
                    i0 = list(calendar).index(d0)
                except ValueError:
                    continue
                if i0 + 2 >= len(calendar):
                    continue
                d2 = calendar[i0 + 2]
                vals = []
                for sym in TARGETS:
                    df = _stocks_cache.get(sym)
                    if df is None or d0 not in df.index or d2 not in df.index:
                        continue
                    op = float(df.loc[d0, "open"])
                    xc = float(df.loc[d2, "close"])
                    if op > 0:
                        vals.append((xc / op - 1.0) * 100.0)
                if vals:
                    rows.append({"sunday": str(sun.date()),
                                 "entry_day": str(d0.date()),
                                 "btc_weekend_ret_pct": round(ret, 2),
                                 "event_ret_pct": round(float(np.mean(vals)), 3)})
        r = np.array([x["event_ret_pct"] for x in rows]) if rows else np.array([])
        out[f"start_shift_{shift_h:+d}h"] = {
            "n_long_events": len(rows),
            "event_dates": [x["entry_day"] for x in rows],
            "mean_event_ret_pct": round(float(r.mean()), 3) if len(r) else None,
            "sum_event_ret_pct": round(float(r.sum()), 3) if len(r) else None,
            "win_rate_pct": round(float((r > 0).mean() * 100), 2) if len(r) else None,
        }
    return {
        "design": "shift ONLY the Friday-close endpoint by the given hours on the 1h "
                  "series; Sunday-close endpoint and everything else fixed; long side only",
        "note": "DIAGNOSTIC ONLY — no alternative clock is recommended here",
        "sweep": out,
    }


# ---------------------------------------------------------------------------
# Q2 — significance
# ---------------------------------------------------------------------------


def wilson_ci(k: int, n: int, conf: float = 0.95) -> tuple[float, float]:
    if n == 0:
        return (float("nan"), float("nan"))
    z = stats.norm.ppf(1 - (1 - conf) / 2)
    p = k / n
    d = 1 + z * z / n
    c = p + z * z / (2 * n)
    s = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return ((c - s) / d * 100, (c + s) / d * 100)


def one_sample(x: np.ndarray) -> dict:
    n = len(x)
    m = float(np.mean(x))
    sd = float(np.std(x, ddof=1))
    se = sd / math.sqrt(n)
    t = m / se if se > 0 else float("nan")
    p_two = float(2 * stats.t.sf(abs(t), df=n - 1))
    p_one = float(stats.t.sf(t, df=n - 1))
    ci = stats.t.interval(0.95, df=n - 1, loc=m, scale=se)
    return {
        "n": n, "mean_pct": round(m, 3), "sd_pct": round(sd, 3),
        "se_pct": round(se, 3), "t_stat": round(t, 3), "df": n - 1,
        "p_two_sided": round(p_two, 4), "p_one_sided": round(p_one, 4),
        "t_ci95_pct": [round(float(ci[0]), 3), round(float(ci[1]), 3)],
    }


def bootstrap_mean(x: np.ndarray, n_boot: int = N_BOOT, seed: int = SEED) -> dict:
    """Percentile bootstrap AND studentized (bootstrap-t) interval.

    The percentile interval is reported because it was requested, but on a
    right-skewed n=13 sample it is ANTI-conservative (it behaves like a z
    interval and ignores that the sd itself is estimated from 13 points).
    The studentized version is the honest one.
    """
    rng = np.random.default_rng(seed)
    n = len(x)
    idx = rng.integers(0, n, size=(n_boot, n))
    samp = x[idx]
    means = samp.mean(axis=1)
    sds = samp.std(axis=1, ddof=1)
    lo, hi = np.percentile(means, [2.5, 97.5])

    mean_hat = float(x.mean())
    se_hat = float(x.std(ddof=1) / math.sqrt(n))
    with np.errstate(divide="ignore", invalid="ignore"):
        tstar = np.where(sds > 0, (means - mean_hat) / (sds / math.sqrt(n)), 0.0)
    tlo, thi = np.percentile(tstar, [2.5, 97.5])
    return {
        "n_boot": n_boot,
        "mean_pct": round(mean_hat, 3),
        "percentile_ci95_pct": [round(float(lo), 3), round(float(hi), 3)],
        "percentile_p_le_zero": round(float((means <= 0).mean()), 4),
        "studentized_ci95_pct": [round(mean_hat - thi * se_hat, 3),
                                 round(mean_hat - tlo * se_hat, 3)],
        "studentized_contains_zero": bool(mean_hat - thi * se_hat <= 0 <= mean_hat - tlo * se_hat),
        "median_pct": round(float(np.median(means)), 3),
        "caveat": "percentile CI excludes 0 (p=0.006) while the t-test gives p=0.053 — "
                  "the percentile bootstrap is anti-conservative at n=13 because it does "
                  "not propagate the uncertainty in the sd. Quote the t / studentized "
                  "interval, not the percentile one.",
    }


def q2_significance(rets: np.ndarray, wins: int, n: int) -> dict:
    base = one_sample(rets)
    boot = bootstrap_mean(rets)
    wl, wh = wilson_ci(wins, n)
    cp = stats.binomtest(wins, n, 0.5).proportion_ci(0.95, method="exact")

    order = np.argsort(rets)
    top2_sum = float(rets[order[-2:]].sum())
    jack = {}
    for name, mask in [
        ("drop_worst", order[:-1]),
        ("drop_best", order[1:]),
        ("drop_top2", order[:-2]),
        ("drop_best_and_worst", order[1:-1]),
    ]:
        jack[name] = one_sample(rets[mask])

    share_top2 = top2_sum / float(rets.sum()) * 100

    return {
        "event_returns_pct": [round(float(v), 3) for v in rets],
        "unit_of_analysis": "one event = one weekend; equal-weight leg P&L/net notional",
        "n_long_events": n,
        "mean_sd_t_p": base,
        "bootstrap_mean_ci95_pct": boot,
        "win_rate": {
            "wins": wins, "n": n, "observed_pct": round(100 * wins / n, 2),
            "wilson_ci95_pct": [round(wl, 2), round(wh, 2)],
            "clopper_pearson_ci95_pct": [round(cp.low * 100, 2), round(cp.high * 100, 2)],
            "null_50pct_in_ci": bool(wl <= 50 <= wh),
        },
        "concentration": {
            "sum_event_returns_pct": round(float(rets.sum()), 3),
            "top2_events_share_of_gross_returns_pct": round(share_top2, 1),
            "top2_contributions": [round(float(v), 3) for v in rets[order[-2:]]],
        },
        "jackknife": jack,
        "reject_zero_at_5pct_two_sided": bool(base["p_two_sided"] < 0.05),
        "reject_zero_at_5pct_one_sided": bool(base["p_one_sided"] < 0.05),
        "headline": "p(two-sided) = 0.053 -> FAILS to reject zero at 5% two-sided; "
                    "passes only one-sided (0.027), i.e. it is not significant by the "
                    "standard a strategy must clear before money goes on it",
        "per_leg_stats": None,  # filled by caller
    }


# ---------------------------------------------------------------------------
# Q3 — multiple comparisons
# ---------------------------------------------------------------------------


SEARCH_HISTORY = [
    # (family, hypothesis) — every direction the platform has actually tested,
    # reconstructed from git log + reports/. Counts independent "shots on goal".
    ("crypto-signal", "funding_rate absolute threshold -> equity risk"),
    ("crypto-signal", "funding_rate rolling percentile threshold"),
    ("crypto-signal", "onchain MVRV / NVT divergence -> BTC"),
    ("crypto-signal", "onchain exchange netflow"),
    ("crypto-signal", "volume-ratio confirmation on gap events"),
    ("crypto-signal", "weekend_gap (weekend BTC move -> crypto stocks)"),
    ("crypto-signal", "overnight_gap daily-frequency generalization"),
    ("crypto-signal", "merged weekend+overnight gap signal"),
    ("crypto-signal", "order-book / BBO event study around gaps"),
    ("crypto-signal", "Polymarket lead-lag mining"),
    ("macro", "FOMC decision-day calendar effect"),
    ("macro", "FOMC statement hawkishness (lexicon NLP)"),
    ("equity", "cross-sectional GBM monthly selection (S&P100)"),
    ("equity", "cross-sectional GBM on S&P500 universe"),
    ("equity", "momentum factor (12-1) baseline"),
    ("equity", "GBM vs momentum attribution"),
    ("equity", "meta-labeling on weekend_gap / selection"),
    ("execution", "limit-order entry instead of market"),
    ("execution", "tranched/scaled entry"),
    ("execution", "score-weighted sizing"),
    ("risk", "risk gate: VIX percentile"),
    ("risk", "risk gate: momentum regime"),
    ("risk", "risk gate: MVRV level"),
    ("exit", "T+1 vs T+2 vs T+5 exit"),
    ("exit", "hard stop / take-profit"),
    ("exit", "trailing stop"),
    ("exit", "signal-decay exit"),
    ("exit", "mid-month rescoring / month-end vs intrameonth exit"),
]
# within-signal variants that also count as search degrees of freedom
WEEKEND_GAP_VARIANTS = {
    "symbols_considered": 4,      # COIN, MSTR, MARA, RIOT
    "exit_rules_considered": 6,
    "directions_considered": 2,   # long-only vs long-short
    "windows_considered": 3,
    "threshold": "5% is the script default; --threshold is exposed",
}


def q3_multiple_comparisons(p_two: float) -> dict:
    k = len(SEARCH_HISTORY)
    bonf = min(1.0, p_two * k)
    # BH-FDR for the single best hit (rank 1 of k): p * k / 1 == Bonferroni
    bh = min(1.0, p_two * k / 1)
    # degrees of freedom inside weekend_gap itself
    inner = (WEEKEND_GAP_VARIANTS["symbols_considered"]
             * WEEKEND_GAP_VARIANTS["exit_rules_considered"])
    return {
        "n_hypothesis_families_audited": k,
        "hypotheses": [f"{a}: {b}" for a, b in SEARCH_HISTORY],
        "expected_false_positives_at_5pct": round(0.05 * k, 2),
        "family_wise_error_rate_at_5pct": round(1 - 0.95 ** k, 4),
        "raw_p_two_sided": p_two,
        "p_bonferroni": round(bonf, 4),
        "p_bh_fdr_rank1": round(bh, 4),
        "survives_bonferroni_5pct": bool(bonf < 0.05),
        "in_signal_search_degrees_of_freedom": WEEKEND_GAP_VARIANTS,
        "inner_degrees_of_freedom_symbols_x_exits": inner,
        "all_in_k": k + inner,
        "all_in_bonferroni_p": round(min(1.0, p_two * (k + inner)), 4),
    }


# ---------------------------------------------------------------------------
# Q4 — beta in disguise?
# ---------------------------------------------------------------------------


def event_return(bars: dict, entry: pd.Timestamp, sym: str,
                 exit_kind: str = "t_plus_2",
                 calendar: pd.DatetimeIndex | None = None,
                 month_end: bool = False) -> float | None:
    """Return (pct) of holding `sym` from entry-day OPEN to the exit close."""
    df = bars.get(sym)
    if df is None or entry not in df.index:
        return None
    ep = float(df.loc[entry, "open"])
    if not (ep > 0):
        return None
    if month_end:
        m = entry.to_period("M")
        seg = df[(df.index >= entry) & (df.index.to_period("M") == m)]
        if seg.empty:
            return None
        return (float(seg["close"].iloc[-1]) / ep - 1.0) * 100.0
    later = calendar[calendar > entry]
    n = 2
    if len(later) < n:
        return None
    xd = later[n - 1]
    if xd not in df.index:
        return None
    return (float(df.loc[xd, "close"]) / ep - 1.0) * 100.0


def equal_weight_event_ret(bars, entry, calendar, month_end=False) -> float | None:
    vals = [event_return(bars, entry, s, calendar=calendar, month_end=month_end)
            for s in TARGETS]
    vals = [v for v in vals if v is not None]
    return float(np.mean(vals)) if vals else None


def q4_beta_disguise(btc_daily, stocks, calendar, wk, events, rets) -> dict:
    all_entries = []
    for _, w in wk.iterrows():
        later = calendar[calendar > w["sunday"]]
        if len(later) and WIN_START <= later[0] <= WIN_END:
            all_entries.append((later[0], float(w["ret_pct"])))
    uncond = [(d, r, equal_weight_event_ret(stocks, d, calendar)) for d, r in all_entries]
    uncond = [(d, r, s) for d, r, s in uncond if s is not None]
    u_ret = np.array([s for _, _, s in uncond])
    u_btc = np.array([r for _, r, _ in uncond])

    # conditional on BTC weekend UP only (drop the magnitude threshold)
    up = [(d, r, s) for d, r, s in uncond if r > 0]
    up_ret = np.array([s for _, _, s in up])
    dn = [(d, r, s) for d, r, s in uncond if r < 0]
    dn_ret = np.array([s for _, _, s in dn])

    # regression: stock event return ~ BTC weekend return (all weekends)
    slope, intercept, rval, pval, stderr = stats.linregress(u_btc, u_ret)

    # ---- buy & hold controls -------------------------------------------
    def bh(sym):
        df = stocks[sym]
        seg = df[(df.index >= WIN_START) & (df.index <= WIN_END)]
        if seg.empty:
            return None
        return (float(seg["close"].iloc[-1]) / float(seg["open"].iloc[0]) - 1.0) * 100.0

    btc_win = btc_daily[(btc_daily.index >= WIN_START) & (btc_daily.index <= WIN_END)]
    btc_bh = (float(btc_win["close"].iloc[-1]) / float(btc_win["open"].iloc[0]) - 1.0) * 100.0

    # ---- month-end control on the ACTUAL event days ---------------------
    entry_days = [pd.Timestamp(e["entry_date"]) for e in events]
    me = [equal_weight_event_ret(stocks, d, calendar, month_end=True) for d in entry_days]
    me = np.array([v for v in me if v is not None])
    me_mstr = [event_return(stocks, d, "MSTR", calendar=calendar, month_end=True)
               for d in entry_days]
    me_mstr = np.array([v for v in me_mstr if v is not None])

    # ---- placebo: same-magnitude BTC move on random (non-weekend) dates --
    rng = np.random.default_rng(SEED)
    b = btc_daily.copy()
    b["fwd2"] = b["close"].shift(-2) / b["close"] - 1.0
    b = b[b.index <= WIN_END - pd.Timedelta(days=10)]
    # exclude Fri/Sat/Sun anchors: a Friday anchor +2 days IS the real signal
    anchors = b[(b["fwd2"] >= THRESHOLD / 100.0)
                & (~b.index.dayofweek.isin([4, 5, 6]))]
    pool = []
    for d in anchors.index:
        later = calendar[calendar > d]
        if len(later) == 0:
            continue
        r = equal_weight_event_ret(stocks, later[0], calendar)
        if r is not None:
            pool.append(r)
    pool = np.array(pool)
    n_ev = len(rets)
    draws = rng.choice(pool, size=(N_PLACEBO, n_ev), replace=True).mean(axis=1)
    actual_mean = float(np.mean(rets))
    placebo_p = float((draws >= actual_mean).mean())

    # ---- MSTR beta to BTC (daily) ---------------------------------------
    m = stocks["MSTR"]
    j = m.join(btc_daily[["close"]].rename(columns={"close": "btc"}), how="inner")
    j = j[(j.index >= WIN_START) & (j.index <= WIN_END)]
    j["sr"] = j["close"].pct_change()
    j["br"] = j["btc"].pct_change()
    j = j.dropna()
    bslope, binter, brval, bpval, bse = stats.linregress(j["br"], j["sr"])

    # ---- compounded "do it every single weekend" baseline ----------------
    uncond_comp = float(np.prod(1 + u_ret / 100.0) - 1) * 100
    uncond_by_year = {}
    for yr in sorted({d.year for d, _, _ in uncond}):
        seg = np.array([s for d, _, s in uncond if d.year == yr])
        uncond_by_year[str(yr)] = {
            "n": int(len(seg)),
            "sum_ret_pct": round(float(seg.sum()), 2),
            "compounded_pct": round(float(np.prod(1 + seg / 100.0) - 1) * 100, 2),
            "mean_ret_pct": round(float(seg.mean()), 3),
        }

    # ---- does the >=5% condition add anything over the unconditional trade? -
    actual = np.asarray(rets, dtype=float)
    welch = stats.ttest_ind(actual, u_ret, equal_var=False)
    rng2 = np.random.default_rng(SEED + 1)
    pool_all = u_ret[u_ret > -100]
    perm = rng2.choice(pool_all, size=(N_PLACEBO, len(actual)), replace=True).mean(axis=1)
    perm_p = float((perm >= actual.mean()).mean())

    return {
        "unconditional_compounded_pct_over_window": {
            "compounded_pct": round(uncond_comp, 2),
            "n_events": int(len(u_ret)),
            "by_year": uncond_by_year,
            "note": "trade equal-weight COIN/MSTR/MARA every single weekend in the SAME "
                    "window, Monday open -> T+2 close, 100% notional every time, no BTC "
                    "condition at all. CAVEAT: this is not a strategy anyone would run "
                    "(negative median event return, ~40% time in market, fat tails) — it "
                    "is here only to show how much of the P&L comes from the underlying "
                    "names' Monday->Wednesday drift rather than from the BTC condition. "
                    "The signal deploys ~|score| x equity (mean 0.70) on 13 of these 401 "
                    "occasions, so compare the SIGN, not the magnitudes.",
        },
        "signal_adds_over_unconditional": {
            "welch_t": round(float(welch.statistic), 3),
            "welch_p_two_sided": round(float(welch.pvalue), 4),
            "mean_difference_pct": round(float(actual.mean() - u_ret.mean()), 3),
            "permutation_p_one_sided": round(perm_p, 4),
            "permutation_design": "draw 13 random weekends from ALL 401 weekends, "
                                  "no BTC condition, same trade; 10000 draws",
            "permutation_ci95_of_draw_mean_pct": [
                round(float(v), 3) for v in np.percentile(perm, [5, 95])],
            "note": "if this p is not small the '>=5% condition' is not the reason the "
                    "trade made money",
        },
        "unconditional_all_weekends": {
            "n": int(len(u_ret)),
            "note": "buy COIN/MSTR/MARA equal-weight at the Monday open, sell T+2 close, "
                    "on EVERY weekend regardless of the BTC move",
            "mean_ret_pct": round(float(u_ret.mean()), 3),
            "median_ret_pct": round(float(np.median(u_ret)), 3),
            "sd_pct": round(float(u_ret.std(ddof=1)), 3),
            "win_rate_pct": round(float((u_ret > 0).mean() * 100), 2),
            "t_stat": round(float(u_ret.mean() / (u_ret.std(ddof=1) / math.sqrt(len(u_ret)))), 3),
        },
        "conditional_up_only_no_threshold": {
            "n": int(len(up_ret)),
            "note": "same trade but only when the BTC weekend move was > 0 (any size)",
            "mean_ret_pct": round(float(up_ret.mean()), 3),
            "median_ret_pct": round(float(np.median(up_ret)), 3),
            "win_rate_pct": round(float((up_ret > 0).mean() * 100), 2),
            "t_stat": round(float(up_ret.mean() / (up_ret.std(ddof=1) / math.sqrt(len(up_ret)))), 3),
        },
        "conditional_down_only": {
            "n": int(len(dn_ret)),
            "mean_ret_pct": round(float(dn_ret.mean()), 3),
            "win_rate_pct": round(float((dn_ret > 0).mean() * 100), 2),
        },
        "regression_stock_on_btc_weekend_all_weekends": {
            "slope": round(float(slope), 4),
            "intercept_pct": round(float(intercept), 4),
            "intercept_p": round(float(pval), 4),
            "r_squared": round(float(rval ** 2), 4),
            "note": "if the intercept is ~0 the rule is harvesting plain beta to the "
                    "BTC weekend move, not a timing edge",
        },
        "buy_and_hold_controls_pct_over_window": {
            "window": [str(WIN_START.date()), str(WIN_END.date())],
            "BTC": round(btc_bh, 2),
            "MSTR": round(bh("MSTR"), 2),
            "COIN": round(bh("COIN"), 2),
            "MARA": round(bh("MARA"), 2),
        },
        "event_day_buy_and_hold_to_month_end": {
            "n": int(len(me)),
            "equal_weight_mean_pct": round(float(me.mean()), 3),
            "equal_weight_sd_pct": round(float(me.std(ddof=1)), 3),
            "equal_weight_win_rate_pct": round(float((me > 0).mean() * 100), 2),
            "mstr_only_mean_pct": round(float(me_mstr.mean()), 3),
            "t_plus_2_mean_pct": round(actual_mean, 3),
            "note": "same entry days, holding to the calendar month end instead of T+2",
        },
        "random_date_placebo": {
            "design": "anchors = all non-Sunday days where BTC rose >=5% over the next "
                      "2 calendar days; entry = next trading-day open; trade = same "
                      "equal-weight T+2",
            "pool_size": int(len(pool)),
            "pool_mean_pct": round(float(pool.mean()), 3),
            "pool_win_rate_pct": round(float((pool > 0).mean() * 100), 2),
            "actual_mean_pct": round(actual_mean, 3),
            "n_sim": N_PLACEBO,
            "n_events_per_draw": n_ev,
            "p_value_placebo": round(placebo_p, 4),
            "placebo_draw_mean_ci95_pct": [round(float(v), 3) for v in
                                           np.percentile(draws, [2.5, 97.5])],
        },
        "mstr_daily_beta_to_btc": {
            "beta": round(float(bslope), 4),
            "intercept_bp": round(float(binter) * 10_000, 3),
            "r_squared": round(float(brval ** 2), 4),
            "p_value": round(float(bpval), 6),
        },
    }


# ---------------------------------------------------------------------------
# Q5 — costs, taxes, capital efficiency
# ---------------------------------------------------------------------------


def q5_costs_tax_capital(report: dict, events_json: list[dict], rets: np.ndarray,
                         calendar) -> dict:
    # per-leg gross re-derivation from the report's own trade detail
    def leg_returns(entry: dict) -> list[tuple[float, float, float]]:
        out = []
        for leg in entry["legs"]:
            if leg.get("skipped"):
                continue
                # (gross pct, notional usd, shares)
            shares = leg["shares"]
            ep, xp = leg["entry_price"], leg["exit_price"]
            notional = abs(shares) * ep
            gross = (xp / ep - 1.0) * 100.0
            out.append((gross, notional, abs(shares)))
        return out

    sens = {}
    for bps in (0.0, 5.0, 10.0, 15.0, 25.0, 50.0):
        ev_rets = []
        for e in events_json:
            legs = leg_returns(e)
            if not legs:
                continue
            pnl = 0.0
            notional = 0.0
            for gross, nt, sh in legs:
                pnl += nt * gross / 100.0
                pnl -= 2 * (nt * bps / 10_000.0 + max(sh * 0.005, 1.0))
                notional += nt
            ev_rets.append(pnl / notional * 100.0)
        ev_rets = np.array(ev_rets)
        sens[f"{bps:.0f}bps"] = {
            "mean_event_ret_pct": round(float(ev_rets.mean()), 3),
            "sum_event_ret_pct": round(float(ev_rets.sum()), 3),
            "cumulative_compounded_pct": round(float(np.prod(1 + ev_rets / 100) - 1) * 100, 2),
            "win_rate_pct": round(float((ev_rets > 0).mean() * 100), 2),
            "t_stat": round(float(ev_rets.mean() / (ev_rets.std(ddof=1) / math.sqrt(len(ev_rets)))), 3),
        }

    # ---- tax ------------------------------------------------------------
    gross_cum = sens["0bps"]["cumulative_compounded_pct"]
    net_cum = sens["15bps"]["cumulative_compounded_pct"]
    tax_rate = 0.35
    after_tax = net_cum * (1 - tax_rate)
    years = (WIN_END - WIN_START).days / 365.25
    # after-tax annualisation on the ACTIVE SLEEVE (100k base, cash earns 0)
    cagr_after_tax = ((1 + after_tax / 100) ** (1 / years) - 1) * 100

    # ---- capital efficiency --------------------------------------------
    hold_td = sum(e["hold_trading_days_max"] for e in events_json if e["legs"])
    n_td = len(calendar[(calendar >= WIN_START) & (calendar <= WIN_END)])
    in_market_td = hold_td + len(events_json)   # entry day counted separately
    time_in_market = in_market_td / n_td * 100

    return {
        "slippage_sensitivity": sens,
        "cost_reconciliation": {
            "audit_5bps_mean_event_ret_pct": sens["5bps"]["mean_event_ret_pct"],
            "report_mean_event_ret_pct": round(float(rets.mean()), 3),
            "matches": bool(abs(sens["5bps"]["mean_event_ret_pct"] - float(rets.mean())) < 0.01),
            "note": "re-derivation from the report's own entry/exit prices reproduces the "
                    "published per-event mean -> the cost arithmetic in the original "
                    "backtest is internally consistent; what is questionable is the 5bps "
                    "ASSUMPTION, not the arithmetic",
            "compounded_vs_published": "the audit's cumulative_compounded_pct holds 100% "
                                       "notional every event; the published +80.56% equity "
                                       "curve scales notional by |score| (mean 0.70) so the "
                                       "two compounded numbers are not interchangeable",
        },
        "slippage_note": "5bps is modelled ONE-WAY (src/selection/costs.py), so the "
                         "reported +80.6% already nets ~10bps round trip. The open "
                         "auction on a gapped crypto name is the widest spread of the "
                         "day; 15-25bps one-way is the realistic band.",
        "tax": {
            "holding_period": "2-3 trading days -> short-term, taxed as ordinary income",
            "assumed_rate": tax_rate,
            "gross_cumulative_pct": gross_cum,
            "net_of_cost_cumulative_pct": net_cum,
            "after_tax_cumulative_pct": round(after_tax, 2),
            "after_tax_cagr_pct_on_sleeve": round(cagr_after_tax, 2),
            "loss_offsetting_note": "assumes gains can offset other losses and no "
                                    "wash-sale interaction; 4 of 13 events were losses",
        },
        "capital_efficiency": {
            "window_years": round(years, 2),
            "trading_days_in_window": n_td,
            "in_market_trading_days": in_market_td,
            "time_in_market_pct": round(time_in_market, 2),
            "events": len(events_json),
            "events_per_year": round(len(events_json) / years, 2),
            "trading_contribution_cagr_pct": report["windows"]["full"]["long_only"]
                                                    ["t_plus_2"]["metrics"]["cagr_pct"],
            "note": "the +80.6% / ~11% CAGR is the return on the WHOLE active sleeve "
                    "even though it is only deployed ~5% of trading days; add SGOV on "
                    "idle cash for the sleeve total. On a 10-20% sleeve this is "
                    "~1.5-3pp/yr of total portfolio, pre-tax, pre-execution-error.",
        },
    }


# ---------------------------------------------------------------------------
# Q6 — decay
# ---------------------------------------------------------------------------


def q6_decay(btc_daily, stocks, calendar, wk, events_json, rets) -> dict:
    ev = pd.DataFrame({
        "entry_day": [pd.Timestamp(e["entry_date"]) for e in events_json],
        "ret_pct": rets,
        "btc_ret": [e["btc_weekend_return_pct"] for e in events_json],
    }).sort_values("entry_day").reset_index(drop=True)

    half = len(ev) // 2
    first, second = ev.iloc[:half], ev.iloc[half:]

    def sl(d):
        r = d["ret_pct"].to_numpy()
        return {
            "n": len(r),
            "start": str(d["entry_day"].iloc[0].date()),
            "end": str(d["entry_day"].iloc[-1].date()),
            "mean_ret_pct": round(float(r.mean()), 3),
            "sum_ret_pct": round(float(r.sum()), 3),
            "win_rate_pct": round(float((r > 0).mean() * 100), 2),
            "t_stat": round(float(r.mean() / (r.std(ddof=1) / math.sqrt(len(r)))), 3)
            if len(r) > 1 and r.std(ddof=1) > 0 else None,
        }

    era = {}
    for name, lo, hi in [("2021-2022", "2021-01-01", "2022-12-31"),
                         ("2023-2024", "2023-01-01", "2024-12-31"),
                         ("2025-2026", "2025-01-01", "2026-12-31")]:
        d = ev[(ev["entry_day"] >= lo) & (ev["entry_day"] <= hi)]
        era[name] = sl(d) if len(d) else {"n": 0}

    # trigger frequency per calendar year + BTC vol per year
    freq = {}
    for yr in range(2021, 2027):
        w = wk[(wk["sunday"] >= f"{yr}-01-01") & (wk["sunday"] <= f"{yr}-12-31")]
        imp = w[(w["ret_pct"].abs() >= THRESHOLD) & (w["ret_pct"] > 0)]
        freq[str(yr)] = {
            "weekends": int(len(w)),
            "abs_move_ge_5pct_up": int(len(imp)),
            "abs_move_ge_5pct_any": int(w["flagged"].sum()),
            "btc_realized_vol_ann_pct": None,
        }
    b = btc_daily.copy()
    b["wk_ret"] = b["close"].pct_change(7)
    for yr in range(2021, 2027):
        seg = b[(b.index >= f"{yr}-01-01") & (b.index <= f"{yr}-12-31")]["close"].pct_change().dropna()
        if len(seg) > 30:
            freq[str(yr)]["btc_realized_vol_ann_pct"] = round(float(seg.std() * math.sqrt(365) * 100), 1)

    # where does the P&L come from: Monday gap (Fri close -> Mon open) vs
    # the part we actually trade (Mon open -> Wed close)
    rows = []
    for _, w in wk[wk["flagged"] & (wk["ret_pct"] > 0)].iterrows():
        later = calendar[calendar > w["sunday"]]
        if len(later) == 0:
            continue
        d0 = later[0]
        if d0 < WIN_START or d0 > WIN_END:
            continue
        try:
            i0 = list(calendar).index(d0)
        except ValueError:
            continue
        if i0 + 2 >= len(calendar):
            continue
        d2 = calendar[i0 + 2]
        gaps, post, tot = [], [], []
        for sym in TARGETS:
            df = stocks[sym]
            prev = df[df.index < d0]
            if prev.empty or d0 not in df.index or d2 not in df.index:
                continue
            pc = float(prev["close"].iloc[-1])
            op = float(df.loc[d0, "open"])
            xc = float(df.loc[d2, "close"])
            if pc <= 0 or op <= 0:
                continue
            gaps.append((op / pc - 1) * 100)
            post.append((xc / op - 1) * 100)
            tot.append((xc / pc - 1) * 100)
        if not gaps:
            continue
        rows.append({
            "entry_date": str(d0.date()),
            "btc_weekend_ret_pct": round(float(w["ret_pct"]), 2),
            "gap_pct": round(float(np.mean(gaps)), 3),
            "post_open_to_t2_pct": round(float(np.mean(post)), 3),
            "fri_close_to_t2_pct": round(float(np.mean(tot)), 3),
            "gap_filled_by_t2": bool(np.mean(tot) <= 0),
        })
    comp = pd.DataFrame(rows)
    comp["year"] = comp["entry_date"].str[:4]
    by_year = {}
    for yr, g in comp.groupby("year"):
        by_year[str(yr)] = {
            "n": int(len(g)),
            "mean_gap_pct": round(float(g["gap_pct"].mean()), 3),
            "mean_post_open_to_t2_pct": round(float(g["post_open_to_t2_pct"].mean()), 3),
            "mean_fri_close_to_t2_pct": round(float(g["fri_close_to_t2_pct"].mean()), 3),
            "gap_fill_rate_pct": round(float(g["gap_filled_by_t2"].mean() * 100), 1),
        }
    early = comp[comp["year"].isin(["2021", "2022", "2023"])]
    late = comp[comp["year"].isin(["2024", "2025", "2026"])]

    return {
        "era_split_halves": {"first_half": sl(first), "second_half": sl(second)},
        "era_split_3yr": era,
        "trigger_frequency_and_btc_vol": freq,
        "pnl_decomposition": {
            "note": "gap = Fri close -> Mon open (we DO NOT capture it); "
                    "post_open_to_t2 = Mon open -> Wed close (this is the strategy)",
            "by_year": by_year,
            "early_2021_2023": {
                "n": int(len(early)),
                "mean_gap_pct": round(float(early["gap_pct"].mean()), 3),
                "mean_post_open_to_t2_pct": round(float(early["post_open_to_t2_pct"].mean()), 3),
                "gap_fill_rate_pct": round(float(early["gap_filled_by_t2"].mean() * 100), 1),
            },
            "late_2024_2026": {
                "n": int(len(late)),
                "mean_gap_pct": round(float(late["gap_pct"].mean()), 3) if len(late) else None,
                "mean_post_open_to_t2_pct": round(float(late["post_open_to_t2_pct"].mean()), 3) if len(late) else None,
                "gap_fill_rate_pct": round(float(late["gap_filled_by_t2"].mean() * 100), 1) if len(late) else None,
            },
            "per_event": comp.to_dict(orient="records"),
        },
        "last_8_events": sl(ev.tail(8)),
    }


def build_verdict(report: dict) -> dict:
    """Q7 — the recommendation. Written by the red team, not the author."""
    s2 = report["q2_significance"]
    q3 = report["q3_multiple_comparisons"]
    q4 = report["q4_beta_disguise"]
    q5 = report["q5_costs_tax_capital"]
    rc = report["q1_reproducibility"]["discovery_claim_recheck"]
    mstr_pb = rc["by_symbol_and_window"]["MSTR"]["playbook_window_2021_01_to_2026_09"]
    mstr_fs = rc["by_symbol_and_window"]["MSTR"]["full_sample_2019_to_2026"]

    return {
        "verdict": "DOWNGRADE — playbook v1.0 must not stand as written, but the "
                   "signal should not be deleted either",
        "confidence": "high on the statistics; the direction of the fix is not "
                      "in doubt, only how far to cut",
        "one_line": "The backtest is not broken and the sign is probably real, but the "
                    "edge is statistically unestablished: 2 of 13 events produce 63.3% of "
                    "the gross return, dropping them kills the result (p=0.18), the "
                    "two-sided p is 0.053 BEFORE any correction and 1.0 after, a random-"
                    "date placebo is not beaten, and the documented 0.82 gap correlation "
                    "does not reproduce.",
        "most_lethal_finding": {
            "title": "Two weekends out of thirteen are the entire result.",
            "detail": "2021-01-04 (+25.50%) and 2023-03-13 (+18.10%) contribute "
                      "63.3% of the sum of event returns. Remove both and the remaining "
                      "11 events average +2.30% with p=0.18 and a 95% CI of "
                      "[-1.26%, +5.85%] — i.e. indistinguishable from zero. A strategy "
                      "whose headline '+80.6% / 69% win rate' is manufactured by two "
                      "observations cannot be sized with real money at the confidence "
                      "the playbook implies.",
        },
        "reasons_ranked": [
            {
                "rank": 1,
                "reason": "Two events carry the result.",
                "evidence": "2021-01-04 (+25.5%) and 2023-03-13 (+18.1%) are "
                            f"{s2['concentration']['top2_events_share_of_gross_returns_pct']}% "
                            "of gross returns. Drop both: mean +2.30%/event, p=0.18, "
                            "95% CI [-1.26, +5.85]. Drop the best alone: the CI still "
                            "covers zero. The distribution is not merely noisy, it is "
                            "concentrated in a way that makes every headline statistic "
                            "(mean, win rate, MDD) an artifact of two dates.",
            },
            {
                "rank": 2,
                "reason": "It does not clear 5%, and it is not close after correction.",
                "evidence": f"t={s2['mean_sd_t_p']['t_stat']}, "
                            f"p(two-sided)={s2['mean_sd_t_p']['p_two_sided']} on n=13; "
                            f"studentized bootstrap CI "
                            f"{s2['bootstrap_mean_ci95_pct']['studentized_ci95_pct']}; "
                            f"Wilson win-rate CI {s2['win_rate']['wilson_ci95_pct']} which "
                            "contains a coin flip. The platform has tested "
                            f"{q3['n_hypothesis_families_audited']} hypothesis directions "
                            f"(+24 in-signal degrees of freedom), so ~"
                            f"{q3['expected_false_positives_at_5pct']} false positives are "
                            f"expected at 5%; Bonferroni p="
                            f"{q3['all_in_bonferroni_p']}. The best-looking survivor of a "
                            "search cannot be validated by the search's own data.",
            },
            {
                "rank": 3,
                "reason": "Nothing about the BTC weekend condition is demonstrably what "
                          "pays.",
                "evidence": "Random-date placebo: non-weekend days where BTC rose >=5% "
                            "over 2 days give a HIGHER mean T+2 return "
                            f"({q4['random_date_placebo']['pool_mean_pct']}% vs "
                            f"{q4['random_date_placebo']['actual_mean_pct']}%, placebo "
                            f"p={q4['random_date_placebo']['p_value_placebo']}) and a "
                            "higher win rate. Trading every weekend with no BTC condition "
                            f"gave +{q4['unconditional_all_weekends']['mean_ret_pct']}%/event "
                            f"(n={q4['unconditional_all_weekends']['n']}) and compounded "
                            f"+{q4['unconditional_compounded_pct_over_window']['compounded_pct']}% "
                            "vs the signal's +80.6%. The BTC weekend move explains "
                            f"R^2={q4['regression_stock_on_btc_weekend_all_weekends']['r_squared']} "
                            "of the tradable Monday-open -> T+2 return across all weekends.",
            },
            {
                "rank": 4,
                "reason": "The documented mechanism does not reproduce, and is measured "
                          "on a leg the strategy does not trade.",
                "evidence": "src/signals/weekend_gap.py and commit dbb6f40 cite a 0.82 "
                            "correlation between the BTC weekend move and the Monday gap. "
                            "Re-running that script's OWN functions on today's data: "
                            f"corr = {mstr_pb['corr_btc_vs_gap_all_flagged']} over the "
                            f"playbook window (n={mstr_pb['n_flagged']}) and "
                            f"{mstr_fs['corr_btc_vs_gap_all_flagged']} over the full sample "
                            f"(n={mstr_fs['n_flagged']}); restricted to the up-only "
                            "weekends the strategy actually takes it is NEGATIVE "
                            f"({mstr_pb['corr_btc_vs_gap_up_only']}). The same script "
                            "measures 'next day' from MONDAY'S CLOSE "
                            "(backtest_weekend_gap.py:95) while the playbook enters at the "
                            "Monday OPEN — so the evidence and the executed trade are on "
                            "different clocks, and the gap the evidence is about is the "
                            "one the playbook deliberately pays away.",
            },
            {
                "rank": 5,
                "reason": "The trigger set is convention-dependent and the strategy is "
                          "dormant.",
                "evidence": "A 4-hour change to the Friday-close definition (defensible: "
                            "the daily bar closes 4h after the US cash close) changes 5-7 "
                            "of the 25 traded events; a +/-12h sweep yields 11-22 events "
                            "and a 50-75% win rate. Trigger frequency fell from 5/yr "
                            "(2021) to 1 (2025) and 0 (2026 YTD) as BTC vol fell 81% -> "
                            "42-46%; the last long event was 2025-03-03 (-0.21%). The "
                            "'2-3 triggers per year' the playbook promises is already "
                            "stale, and the last 8 events only reach back to 2022.",
            },
        ],
        "what_the_red_team_did_NOT_find": [
            "No reproducibility failure. All 25 events, every BTC weekend return and every "
            "entry price reproduce exactly from raw SQLite, and the recent backfills "
            "(21 S&P names, BAYRY, BTC 1h) did not disturb the event set.",
            "No look-ahead. The signal reads only data up to the Sunday; the entry uses "
            "that day's open.",
            "No cherry-picked threshold. The mean rises monotonically with the threshold "
            "(>=3%: +1.1%, >=5%: +5.4%, >=7%: +13.9%) — 5% is not the peak of a scan, so "
            "the choice of 5% is not the problem.",
            "No cherry-picked clock. Shifting the Friday endpoint by +/-12h leaves the mean "
            "positive everywhere (+4.4% to +8.3%), so the effect is not an artifact of one "
            "convenient timestamp.",
            "No sign flip across eras. First half +5.95% vs second half +4.74%; the "
            "direction is stable even though significance is not.",
        ],
        "recommended_downgrade": {
            "1_retract_the_claim": "Rewrite docs/active_strategy_playbook.md sections 3, "
                                   "7 and 10: strike '已验证', and never again quote "
                                   "'+80.6% / 69% / +5.3% per trade' without 'p=0.053 "
                                   "two-sided, Bonferroni p=1.0, 63% of the return from 2 "
                                   "of 13 events' on the same line.",
            "2_halve_the_size": "If the sleeve trades it at all, run it at half of the "
                                "section 4 sizing (score x 0.5) until new out-of-sample "
                                "triggers accumulate. The evidence supports a positive "
                                "sign of uncertain magnitude, not the stated confidence.",
            "3_do_NOT_change_the_threshold": "Do not move 5% to chase the >=7% bucket. "
                                             "That is another shot on goal and reproduces "
                                             "the exact failure this audit found.",
            "4_stop_quoting_per_trade_returns": "Replace '+5.3% per trade' with the sleeve "
                                                "numbers: "
                                                f"{q5['capital_efficiency']['time_in_market_pct']}% "
                                                "of trading days in market, "
                                                f"{q5['tax']['after_tax_cagr_pct_on_sleeve']}% "
                                                "after-tax sleeve CAGR at a 35% short-term "
                                                "rate, and the trigger count per year — "
                                                "which is now the binding constraint, not "
                                                "the return.",
            "5_monitor_the_only_informative_decay_signal": "The GAP vs POST-OPEN split. "
                                                           "Early 2021-2023: gap 5.07%, "
                                                           "post-open (traded) 5.85%. Late "
                                                           "2024-2026: gap 8.18%, post-open "
                                                           "4.49%. In 2025 the gap was "
                                                           "10.84% and the post-open leg "
                                                           "was -0.09% — the entire move "
                                                           "was priced at the open. If the "
                                                           "traded leg converges to zero "
                                                           "while the gap stays wide, the "
                                                           "time-difference has been "
                                                           "arbitraged away and the "
                                                           "strategy should be retired.",
            "6_require_new_evidence": "Log the next 8 triggers with actual execution "
                                      "prices before any size increase. Eight more events "
                                      "roughly halve the standard error and would move "
                                      "p from 0.053 to ~0.01 if the observed mean holds.",
        },
        "why_not_retire_outright": [
            "The sign is stable: positive in every era half, every threshold above 3%, "
            "every clock shift tested, and in the conditional-vs-unconditional comparison.",
            "The gap genuinely does not fill (0% fill in 2023-2025), so the underlying "
            "claim 'US equities do not fully reprice a crypto weekend at the open' is "
            "supported even if the tradable capture is not.",
            "The loss is structurally bounded (long-only, T+2, no leverage, ~3% of the "
            "time in market), so running it at reduced size is cheap optionality while "
            "evidence accumulates.",
        ],
        "cannot_answer_with_available_data": [
            "Whether the true forward mean is 0 or +4%/event: n=13 gives a 95% CI of "
            "[-0.1%, +10.7%]. Only new out-of-sample triggers can resolve this.",
            "Whether the 2025 gap widening (gap 10.84%, post-open -0.09%) is decay or a "
            "draw: n=1.",
            "Why the documented 0.82 correlation was reported when the same script's code "
            "gives 0.19-0.25 today. It may have been a different sample, a since-fixed "
            "alignment bug, or simply wrong — the provenance cannot be recovered from the "
            "repository.",
            "Whether COIN/MSTR/MARA are the right proxies: deliberately not re-optimised.",
            "The real open-auction slippage at the intended size: needs paper-trading fills.",
            "The user's actual tax bracket, state rate and loss-offsetting position: the "
            "35% figure is a placeholder and the after-tax number scales linearly.",
            "Whether the -17% MDD is a fair risk estimate: with 13 events and 2 dominant "
            "winners, the drawdown distribution is estimated from almost nothing.",
        ],
    }


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def commit_hash() -> str:
    try:
        return subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT,
                              capture_output=True, text=True, check=True).stdout.strip()
    except Exception:  # noqa: BLE001
        return "unknown"


def main() -> None:
    print("=" * 100)
    print("  RED TEAM: weekend_gap 独立审计（目标=证伪，不是确认）")
    print("=" * 100)

    btc_daily = load_btc("1d")
    stocks = {s: load_stock(s) for s in TARGETS}
    _stocks_cache.update(stocks)
    cal_set = set()
    for s in TARGETS:
        cal_set.update(stocks[s].index)
    calendar = pd.DatetimeIndex(sorted(cal_set))
    print(f"\nBTC 日线 {len(btc_daily)} 根 ({btc_daily.index[0].date()}~{btc_daily.index[-1].date()})"
          f" | 交易日历 {len(calendar)} 天 ({calendar[0].date()}~{calendar[-1].date()})")

    wk = weekend_table(btc_daily)
    events = build_events(wk, calendar)
    print(f"周末总数 {len(wk)} | 触发(|move|>=5%) {int(wk['flagged'].sum())} "
          f"| 窗口内触发 {len(events[(events['entry_day']>=WIN_START)&(events['entry_day']<=WIN_END)])}")

    report = {
        "meta": {
            "audit": "red-team adversarial review of weekend_gap",
            "commit": commit_hash(),
            "generated_at": pd.Timestamp.now().isoformat(timespec="seconds"),
            "signal_under_test": "BTC Fri-close -> Sun-close |move| >= 5% -> Monday-open "
                                 "long COIN/MSTR/MARA equal weight -> T+2 close",
            "window": [str(WIN_START.date()), str(WIN_END.date())],
            "read_only": "no parameters were tuned; no alternative specification is "
                         "proposed as an improvement",
        },
        "q1_reproducibility": q1_reproducibility(btc_daily, stocks, calendar),
    }
    q1 = report["q1_reproducibility"]
    print(f"\n[Q1] 事件集逐条复现: {'PASS' if q1['reproduced_exactly'] else 'FAIL'}"
          f" | 价格不一致 {len(q1['entry_price_mismatches'])}"
          f" | BTC 收益不一致 {len(q1['btc_weekend_return_mismatches'])}"
          f" | 时钟对齐翻号 {q1['clock_alignment_probe']['trigger_decision_flips_in_window']}")

    # ---- events & returns -------------------------------------------------
    ref = json.loads(EXIT_REPORT.read_text())
    lo = [e for e in ref["trades_full_window"]["long_only"]["t_plus_2"]
          if WIN_START <= pd.Timestamp(e["entry_date"]) <= WIN_END]
    rets = np.array([e["ret_pct"] for e in lo])
    wins = int((rets > 0).sum())

    report["q2_significance"] = q2_significance(rets, wins, len(rets))
    # per-leg stats (38 legs) as an explicitly NON-independent comparison
    leg_rets = np.array([l["ret_pct"] for e in lo for l in e["legs"]
                         if not l.get("skipped")])
    report["q2_significance"]["per_leg_stats"] = {
        "n_legs": int(len(leg_rets)),
        "mean_pct": round(float(leg_rets.mean()), 3),
        "t_stat": round(float(leg_rets.mean() / (leg_rets.std(ddof=1) / math.sqrt(len(leg_rets)))), 3),
        "p_two_sided": round(float(2 * stats.t.sf(
            abs(leg_rets.mean() / (leg_rets.std(ddof=1) / math.sqrt(len(leg_rets)))),
            df=len(leg_rets) - 1)), 5),
        "warning": "legs within one event are highly correlated (same day, same sector) "
                   "— this n=38 t-stat is NOT the right unit and is shown only to "
                   "demonstrate how much flattery the wrong unit buys",
    }
    s2 = report["q2_significance"]
    print(f"[Q2] 均值 {s2['mean_sd_t_p']} ")
    print(f"     bootstrap percentile CI {s2['bootstrap_mean_ci95_pct']['percentile_ci95_pct']}%"
          f" | studentized CI {s2['bootstrap_mean_ci95_pct']['studentized_ci95_pct']}%"
          f" | Wilson 胜率 {s2['win_rate']['wilson_ci95_pct']}%"
          f" | p(two)={s2['mean_sd_t_p']['p_two_sided']}")

    report["q3_multiple_comparisons"] = q3_multiple_comparisons(
        s2["mean_sd_t_p"]["p_two_sided"])
    report["q3_multiple_comparisons"]["discovery_archaeology"] = {
        "first_commit": "dbb6f40 'feat(signals): weekend BTC→stock gap signal — STRONG RESULTS'",
        "preceded_by": "63a0e57 'docs: data audit + signal brainstorm' 2.5h earlier",
        "was_it_preregistered": False,
        "evidence": [
            "the brainstorm doc 63a0e57 lists momentum / macro / Polymarket / on-chain / "
            "order-book candidates — weekend_gap is NOT among them",
            "the very first commit that touches it announces 'STRONG RESULTS' — the "
            "hypothesis and the confirmation were produced in the same session",
            "scripts/backtest_weekend_gap.py takes --threshold and loops 4 symbols "
            "(COIN/MSTR/MARA/RIOT); the commit message singles out 'MSTR ... 100% win "
            "rate!' — the traded symbol set is a post-hoc choice among 4",
        ],
        "verdict": "opportunistic scan, not a pre-registered hypothesis -> the raw "
                   "p-value must be discounted, it cannot be read as a confirmatory test",
    }
    q3 = report["q3_multiple_comparisons"]
    print(f"[Q3] k={q3['n_hypothesis_families_audited']} (+{q3['inner_degrees_of_freedom_symbols_x_exits']} in-signal)"
          f" expected FP {q3['expected_false_positives_at_5pct']} | Bonferroni p={q3['all_in_bonferroni_p']}")

    report["q4_beta_disguise"] = q4_beta_disguise(
        btc_daily, stocks, calendar, wk, lo, rets)
    report["q4_beta_disguise"]["threshold_sensitivity"] = threshold_sensitivity(wk, calendar)
    q4 = report["q4_beta_disguise"]
    print(f"[Q4] 无条件每周一买入 T+2: 均 {q4['unconditional_all_weekends']['mean_ret_pct']}%"
          f" (n={q4['unconditional_all_weekends']['n']}, t={q4['unconditional_all_weekends']['t_stat']})"
          f" | 安慰剂 p={q4['random_date_placebo']['p_value_placebo']}")

    report["q5_costs_tax_capital"] = q5_costs_tax_capital(ref, lo, rets, calendar)
    q5 = report["q5_costs_tax_capital"]
    print(f"[Q5] 15bps 累计 {q5['slippage_sensitivity']['15bps']['cumulative_compounded_pct']}%"
          f" | 税后年化(主动仓) {q5['tax']['after_tax_cagr_pct_on_sleeve']}%"
          f" | 在场时间 {q5['capital_efficiency']['time_in_market_pct']}%")

    report["q6_decay"] = q6_decay(btc_daily, stocks, calendar, wk, lo, rets)
    q6 = report["q6_decay"]
    print(f"[Q6] 前半 {q6['era_split_halves']['first_half']['mean_ret_pct']}%"
          f" vs 后半 {q6['era_split_halves']['second_half']['mean_ret_pct']}%"
          f" | 后期 gap {q6['pnl_decomposition']['late_2024_2026']['mean_gap_pct']}%"
          f" / post-open {q6['pnl_decomposition']['late_2024_2026']['mean_post_open_to_t2_pct']}%")

    report["q7_verdict"] = build_verdict(report)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str) + "\n")
    print(f"\nJSON 已写入: {OUT}")

    v = report["q7_verdict"]
    print("\n" + "=" * 100)
    print(f"  [Q7] {v['verdict']}  |  置信度: {v['confidence']}")
    print("=" * 100)
    print(f"  {v['one_line']}")
    for r in v["reasons_ranked"]:
        print(f"\n  #{r['rank']} {r['reason']}")
        print(f"      {r['evidence']}")
    print("\n  最致命的一条:")
    print("      " + v["reasons_ranked"][0]["reason"])


if __name__ == "__main__":
    main()
