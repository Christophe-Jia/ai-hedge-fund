"""Order-book event study around big BTC weekends (weekend_gap trigger events).

Research question
-----------------
Around the 25 big BTC weekends (|Fri close -> Sun close| >= 5%, the
weekend_gap signal trigger), does the Binance USDT-M futures order book
show *anticipatory* behaviour in the hours before the weekend move --
depth imbalance tilting, book thinning, or aggressive taker flow?

Data
----
- Binance public dumps (data.binance.vision):
  - futures um daily bookDepth/BTCUSDT (30s snapshots, cumulative
    notional within +/-1..5% of mid; starts 2023-01-01). This is the
    core dataset -> depth imbalance + book size.
  - futures um daily aggTrades/BTCUSDT -> hourly taker buy ratio.
- bookTicker (spread) is NOT used: futures dumps are sparse (many event
  days 404) and ~150 MB/day where present. Spread is therefore not
  analysed -- documented as a coverage gap.
- 2021-2022 trigger events (14 of 25) predate bookDepth dumps and are
  excluded; the study covers the 10 events from 2023-03 onward plus a
  control group of calm weekends (|weekend return| < 1%).

Design
------
For each weekend (event or control), window = Friday 00:00 UTC -> next
Tuesday 00:00 UTC, anchored at t0 = Saturday 00:00 UTC (the moment the
Friday daily close is struck and the weekend return window opens).
Hourly aggregation of:
  - imb_k  = bid_notional(+/-k%) / (bid + ask)  for k = 1, 2, 5
  - imb1 hourly std (intra-hour variability)
  - total2 = bid+ask notional within 2% (book size proxy)
  - taker_buy_ratio from aggTrades

Analyses (descriptive only, n=10 events vs n=12 controls):
  1. Friday pre-weekend levels (t in [-24h, 0)) events vs controls,
     and long vs short event split.
  2. Early-weekend (t in [0h, 12h) / [0h, 24h)) imbalance vs weekend
     direction.
  3. Event-aligned hourly mean curves (t = -24h .. +71h).
  4. Book size: Friday depth vs full-window depth, events vs controls.

Honesty notes: small samples, several metrics x windows examined
(multiple comparisons) -- findings report sign-consistency counts and
group means/medians, no significance claims.

Usage:  poetry run python scripts/book_event_study.py
Downloads are cached in data/cache/book_event_study/ (gitignored).
"""

from __future__ import annotations

import csv
import io
import json
import random
import sqlite3
import time
import urllib.request
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CACHE = ROOT / "data" / "cache" / "book_event_study"
BOOK_CACHE = CACHE / "bookDepth"
TRADES_CACHE = CACHE / "aggTrades"
REPORT = ROOT / "reports" / "book_event_study.json"
BTC_DB = ROOT / "data" / "btc_history.db"
EVENTS_SOURCE = ROOT / "reports" / "exit_rules_backtest.json"

BASE = "https://data.binance.vision/data/futures/um/daily"
# First day with bookDepth coverage on data.binance.vision.
BOOKDEPTH_START = datetime(2023, 1, 1, tzinfo=timezone.utc)

N_CONTROLS = 12
CONTROL_MAX_ABS_RET = 1.0  # |weekend return| < 1% defines a calm weekend
CONTROL_EXCLUDE_DAYS = 7  # calm weekend must be >= 7 days from any event
SEED = 42
DOWNLOAD_PAUSE_S = 0.3  # politeness between sequential downloads


# ---------------------------------------------------------------------------
# Events and controls
# ---------------------------------------------------------------------------

def load_events() -> list[dict]:
    """25 trigger weekends from the exit-rules backtest report."""
    data = json.loads(EVENTS_SOURCE.read_text())
    seen: dict[str, dict] = {}
    for rule in ("t_plus_1", "t_plus_2", "t_plus_5", "stop_tp", "trail_half"):
        for t in data["trades_full_window"]["long_short"][rule]:
            seen[t["sunday"]] = {
                "sunday": t["sunday"],
                "direction": t["direction"],
                "weekend_ret_pct": t["btc_weekend_return_pct"],
            }
    return sorted(seen.values(), key=lambda e: e["sunday"])


def weekend_returns_from_db() -> dict[str, float]:
    """Friday close -> Sunday close (%) for every full Sat/Sun pair."""
    con = sqlite3.connect(BTC_DB)
    rows = con.execute(
        "SELECT ts, close FROM ohlcv WHERE symbol='BTC/USDT' "
        "AND timeframe='1d' ORDER BY ts"
    ).fetchall()
    con.close()
    bars = [
        (datetime.fromtimestamp(ts / 1000, tz=timezone.utc), close)
        for ts, close in rows
    ]
    by_date = {b[0].date(): b[1] for b in bars}
    out: dict[str, float] = {}
    for d, close in by_date.items():
        if d.weekday() != 6:  # Sunday
            continue
        fri = d - timedelta(days=2)
        if fri not in by_date:
            continue
        ret = (close / by_date[fri] - 1.0) * 100.0
        out[d.isoformat()] = ret
    return out


def select_controls(
    all_returns: dict[str, float], events: list[dict]
) -> list[dict]:
    rng = random.Random(SEED)
    event_dates = [datetime.fromisoformat(e["sunday"]).date() for e in events]
    candidates = []
    for sunday, ret in all_returns.items():
        d = datetime.fromisoformat(sunday).date()
        # need full Fri..Mon inside the bookDepth era
        fri = d - timedelta(days=1)
        if fri < BOOKDEPTH_START.date():
            continue
        if abs(ret) >= CONTROL_MAX_ABS_RET:
            continue
        if any(abs((d - ed).days) <= CONTROL_EXCLUDE_DAYS for ed in event_dates):
            continue
        candidates.append({"sunday": sunday, "weekend_ret_pct": ret})
    candidates.sort(key=lambda c: c["sunday"])
    rng.shuffle(candidates)
    picked = sorted(candidates[:N_CONTROLS], key=lambda c: c["sunday"])
    return picked


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def ensure_file(kind: str, day: datetime) -> Path | None:
    """Download one daily dump if missing; None if the remote 404s."""
    cache = BOOK_CACHE if kind == "bookDepth" else TRADES_CACHE
    cache.mkdir(parents=True, exist_ok=True)
    fname = f"BTCUSDT-{kind}-{day:%Y-%m-%d}.zip"
    dest = cache / fname
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    url = f"{BASE}/{kind}/BTCUSDT/{fname}"
    req = urllib.request.Request(url, method="HEAD")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            if r.status != 200:
                return None
    except Exception:
        return None
    try:
        urllib.request.urlretrieve(url, dest)
    except Exception:
        if dest.exists():
            dest.unlink()
        return None
    time.sleep(DOWNLOAD_PAUSE_S)
    return dest


# ---------------------------------------------------------------------------
# Parsing / hourly aggregation
# ---------------------------------------------------------------------------

def window_days(sunday: datetime) -> list[datetime]:
    """Friday..Monday (4 UTC days) for a weekend anchored on its Sunday."""
    sun = sunday.replace(hour=0, minute=0, second=0, microsecond=0)
    return [sun - timedelta(days=d) for d in (2, 1, 0, -1)]  # Fri Sat Sun Mon


def parse_bookdepth_hourly(zips: list[Path]) -> dict[int, dict]:
    """Hour bucket (epoch hour) -> aggregated book metrics."""
    # hour_ts -> list of per-snapshot values
    snaps: dict[int, list[dict]] = defaultdict(list)
    for zp in zips:
        with zipfile.ZipFile(zp) as zf:
            for name in zf.namelist():
                if not name.endswith(".csv"):
                    continue
                with zf.open(name) as raw:
                    reader = csv.DictReader(io.TextIOWrapper(raw, "utf-8"))
                    cur_ts = None
                    cur: dict[float, float] = {}
                    for row in reader:
                        ts = row["timestamp"]
                        if ts != cur_ts:
                            if cur_ts is not None and _has_levels(cur):
                                _push_snapshot(snaps, cur_ts, cur)
                            cur_ts, cur = ts, {}
                        pct = float(row["percentage"])
                        cur[pct] = float(row["notional"])
                    if cur_ts is not None and _has_levels(cur):
                        _push_snapshot(snaps, cur_ts, cur)
    return _aggregate_hours(snaps)


_REQUIRED_LEVELS = (-5.0, -2.0, -1.0, 1.0, 2.0, 5.0)


def _has_levels(cur: dict[float, float]) -> bool:
    """A usable snapshot must contain the +/-1,2,5% levels (2026 dumps add
    a +/-0.20% level; extra levels are simply ignored)."""
    return all(k in cur for k in _REQUIRED_LEVELS)


def _push_snapshot(snaps: dict, ts_str: str, cur: dict) -> None:
    t = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=timezone.utc
    )
    hour = int(t.timestamp() // 3600)
    bid1, ask1 = cur[-1], cur[1]
    bid2, ask2 = cur[-2], cur[2]
    bid5, ask5 = cur[-5], cur[5]
    snaps[hour].append(
        {
            "imb1": bid1 / (bid1 + ask1),
            "imb2": bid2 / (bid2 + ask2),
            "imb5": bid5 / (bid5 + ask5),
            "total2": bid2 + ask2,
        }
    )


def _aggregate_hours(snaps: dict[int, list[dict]]) -> dict[int, dict]:
    out = {}
    for hour, rows in snaps.items():
        n = len(rows)
        out[hour] = {
            "imb1_mean": sum(r["imb1"] for r in rows) / n,
            "imb1_std": _std([r["imb1"] for r in rows]),
            "imb2_mean": sum(r["imb2"] for r in rows) / n,
            "imb5_mean": sum(r["imb5"] for r in rows) / n,
            "total2_mean": sum(r["total2"] for r in rows) / n,
            "n_snapshots": n,
        }
    return out


def _std(xs: list[float]) -> float:
    if len(xs) < 2:
        return 0.0
    m = sum(xs) / len(xs)
    return (sum((x - m) ** 2 for x in xs) / (len(xs) - 1)) ** 0.5


def parse_aggtrades_hourly(zips: list[Path]) -> dict[int, dict]:
    """Hour bucket -> taker buy qty / total qty."""
    hours: dict[int, list[float]] = defaultdict(lambda: [0.0, 0.0])  # buy, tot
    for zp in zips:
        with zipfile.ZipFile(zp) as zf:
            for name in zf.namelist():
                if not name.endswith(".csv"):
                    continue
                with zf.open(name) as raw:
                    reader = csv.DictReader(io.TextIOWrapper(raw, "utf-8"))
                    for row in reader:
                        h = int(int(row["transact_time"]) // 3_600_000)
                        qty = float(row["quantity"])
                        hours[h][1] += qty
                        if row["is_buyer_maker"] == "false":
                            hours[h][0] += qty
    return {
        h: {"taker_buy_ratio": b / t if t > 0 else None, "taker_qty": t}
        for h, (b, t) in hours.items()
    }


# ---------------------------------------------------------------------------
# Per-weekend processing
# ---------------------------------------------------------------------------

def build_weekend_hourly(sunday_iso: str) -> tuple[dict[int, dict], dict]:
    """Merged hourly metrics keyed by t (hours since Saturday 00:00 UTC,
    t in [-24, 72)). Also returns a data-coverage note."""
    sunday = datetime.fromisoformat(sunday_iso).replace(tzinfo=timezone.utc)
    t0 = int((sunday - timedelta(days=1)).timestamp() // 3600)  # Sat 00:00
    days = window_days(sunday)
    note = {"book_days": 0, "trades_days": 0, "missing": []}

    bz, az = [], []
    for d in days:
        f = ensure_file("bookDepth", d)
        if f:
            bz.append(f)
            note["book_days"] += 1
        else:
            note["missing"].append(f"bookDepth {d:%Y-%m-%d}")
        f = ensure_file("aggTrades", d)
        if f:
            az.append(f)
            note["trades_days"] += 1
        else:
            note["missing"].append(f"aggTrades {d:%Y-%m-%d}")

    book = parse_bookdepth_hourly(bz) if bz else {}
    trades = parse_aggtrades_hourly(az) if az else {}

    merged: dict[int, dict] = {}
    for h_abs, m in book.items():
        t = h_abs - t0
        if -24 <= t < 72:
            merged[t] = dict(m)
    for h_abs, m in trades.items():
        t = h_abs - t0
        if -24 <= t < 72 and t in merged:
            merged[t]["taker_buy_ratio"] = m["taker_buy_ratio"]
            merged[t]["taker_qty"] = m["taker_qty"]
    return merged, note


def summarize_weekend(hourly: dict[int, dict]) -> dict:
    def w(lo: int, hi: int, key: str, agg: str = "mean") -> float | None:
        vals = [hourly[t][key] for t in range(lo, hi) if t in hourly]
        vals = [v for v in vals if v is not None]
        if not vals:
            return None
        if agg == "mean":
            return sum(vals) / len(vals)
        return sorted(vals)[len(vals) // 2]

    fri_total2 = w(-24, 0, "total2_mean")
    win_total2 = w(-24, 72, "total2_mean")
    return {
        "friday_imb1": w(-24, 0, "imb1_mean"),
        "friday_imb2": w(-24, 0, "imb2_mean"),
        "friday_imb5": w(-24, 0, "imb5_mean"),
        "friday_imb1_std": w(-24, 0, "imb1_std"),
        "friday_taker_ratio": w(-24, 0, "taker_buy_ratio"),
        "friday_total2": fri_total2,
        "depth_ratio_fri_vs_window": (
            fri_total2 / win_total2 if fri_total2 and win_total2 else None
        ),
        "early12h_imb1": w(0, 12, "imb1_mean"),
        "early24h_imb1": w(0, 24, "imb1_mean"),
        "early24h_imb5": w(0, 24, "imb5_mean"),
        "early24h_taker_ratio": w(0, 24, "taker_buy_ratio"),
        "weekend_imb1": w(0, 48, "imb1_mean"),
        "post24h_imb1": w(48, 72, "imb1_mean"),
        "n_hours": len(hourly),
    }


# ---------------------------------------------------------------------------
# Analysis helpers
# ---------------------------------------------------------------------------

def _stats(vals: list[float]) -> dict:
    clean = sorted(v for v in vals if v is not None)
    if not clean:
        return {"n": 0}
    n = len(clean)
    return {
        "n": n,
        "mean": sum(clean) / n,
        "median": clean[n // 2] if n % 2 else (clean[n // 2 - 1] + clean[n // 2]) / 2,
        "min": clean[0],
        "max": clean[-1],
        "std": _std(clean),
    }


def aligned_curve(weekends: list[dict], key: str) -> list[float | None]:
    """Mean across weekends of hourly metric `key`, t = -24..71."""
    out = []
    for t in range(-24, 72):
        vals = [w["hourly"][t][key] for w in weekends if t in w["hourly"]]
        vals = [v for v in vals if v is not None]
        out.append(round(sum(vals) / len(vals), 6) if vals else None)
    return out


def fmt(v, nd=4):
    return "-" if v is None else f"{v:.{nd}f}"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    all_events = load_events()
    returns = weekend_returns_from_db()

    events = [
        e
        for e in all_events
        if datetime.fromisoformat(e["sunday"]).date()
        - timedelta(days=2)
        >= BOOKDEPTH_START.date()
    ]
    controls = select_controls(returns, all_events)
    print(f"Total trigger events: {len(all_events)}")
    print(
        f"bookDepth-era events (>= {BOOKDEPTH_START.date()}): {len(events)}"
    )
    print(f"Controls (|ret| < {CONTROL_MAX_ABS_RET}%): {len(controls)}")

    processed: list[dict] = []
    for group, items in (("event", events), ("control", controls)):
        for it in items:
            sunday = it["sunday"]
            print(f"[{group}] processing {sunday} ...", flush=True)
            hourly, note = build_weekend_hourly(sunday)
            summ = summarize_weekend(hourly)
            processed.append(
                {
                    "group": group,
                    "sunday": sunday,
                    "direction": it.get("direction"),
                    "weekend_ret_pct": it["weekend_ret_pct"],
                    "summary": summ,
                    "hourly": hourly,
                    "coverage": note,
                }
            )

    ev = [w for w in processed if w["group"] == "event"]
    ct = [w for w in processed if w["group"] == "control"]
    ev_long = [w for w in ev if w["direction"] == "long"]
    ev_short = [w for w in ev if w["direction"] == "short"]

    metrics = [
        "friday_imb1",
        "friday_imb5",
        "friday_imb1_std",
        "friday_taker_ratio",
        "depth_ratio_fri_vs_window",
        "early12h_imb1",
        "early24h_imb1",
        "early24h_taker_ratio",
    ]
    group_stats = {
        m: {
            "events": _stats([w["summary"][m] for w in ev]),
            "controls": _stats([w["summary"][m] for w in ct]),
            "events_long": _stats([w["summary"][m] for w in ev_long]),
            "events_short": _stats([w["summary"][m] for w in ev_short]),
        }
        for m in metrics
    }

    # sign-consistency: does Friday imbalance tilt with weekend direction?
    def sign_counts(group: list[dict], key: str) -> dict:
        above = below = none = 0
        for w in group:
            v = w["summary"][key]
            if v is None:
                none += 1
            elif v > 0.5:
                above += 1
            elif v < 0.5:
                below += 1
        return {"above_0.5": above, "below_0.5": below, "missing": none}

    consistency = {
        "friday_imb1_long_events": sign_counts(ev_long, "friday_imb1"),
        "friday_imb1_short_events": sign_counts(ev_short, "friday_imb1"),
        "friday_imb5_long_events": sign_counts(ev_long, "friday_imb5"),
        "friday_imb5_short_events": sign_counts(ev_short, "friday_imb5"),
        "early12h_imb1_long_events": sign_counts(ev_long, "early12h_imb1"),
        "early12h_imb1_short_events": sign_counts(ev_short, "early12h_imb1"),
        # taker ratio > 0.5 means aggressive buying dominates
        "friday_taker_long_events": sign_counts(ev_long, "friday_taker_ratio"),
        "friday_taker_short_events": sign_counts(
            ev_short, "friday_taker_ratio"
        ),
    }

    aligned = {
        "t_hours": list(range(-24, 72)),
        "events_mean": {
            "imb1": aligned_curve(ev, "imb1_mean"),
            "imb5": aligned_curve(ev, "imb5_mean"),
            "taker_buy_ratio": aligned_curve(ev, "taker_buy_ratio"),
            "total2": aligned_curve(ev, "total2_mean"),
            "imb1_std": aligned_curve(ev, "imb1_std"),
        },
        "controls_mean": {
            "imb1": aligned_curve(ct, "imb1_mean"),
            "imb5": aligned_curve(ct, "imb5_mean"),
            "taker_buy_ratio": aligned_curve(ct, "taker_buy_ratio"),
            "total2": aligned_curve(ct, "total2_mean"),
            "imb1_std": aligned_curve(ct, "imb1_std"),
        },
    }

    # ------------------------------------------------------------------
    # stdout tables
    # ------------------------------------------------------------------
    print("\n=== Friday pre-weekend (t=-24..0) and early weekend metrics ===")
    hdr = (
        f"{'sunday':<12}{'grp':<5}{'dir':<6}{'ret%':>7}"
        f"{'fri_imb1':>9}{'fri_imb5':>9}{'fri_std':>8}"
        f"{'fri_taker':>10}{'depR':>6}{'e12_imb1':>9}{'e24_taker':>10}"
    )
    print(hdr)
    for w in processed:
        s = w["summary"]
        print(
            f"{w['sunday']:<12}{w['group'][:4]:<5}"
            f"{(w['direction'] or '-')[:4]:<6}{w['weekend_ret_pct']:>7.2f}"
            f"{fmt(s['friday_imb1']):>9}{fmt(s['friday_imb5']):>9}"
            f"{fmt(s['friday_imb1_std']):>8}"
            f"{fmt(s['friday_taker_ratio']):>10}"
            f"{fmt(s['depth_ratio_fri_vs_window'], 3):>6}"
            f"{fmt(s['early12h_imb1']):>9}"
            f"{fmt(s['early24h_taker_ratio']):>10}"
        )

    print("\n=== Group stats (mean / median [min..max]) ===")
    for m in metrics:
        for gname in ("events", "controls", "events_long", "events_short"):
            st = group_stats[m][gname]
            if st.get("n"):
                print(
                    f"{m:<26}{gname:<13}n={st['n']:<3}"
                    f"mean={st['mean']:.4f} med={st['median']:.4f} "
                    f"[{st['min']:.4f}..{st['max']:.4f}]"
                )

    print("\n=== Sign consistency (count of weekends with metric > / < 0.5) ===")
    for k, v in consistency.items():
        print(f"{k:<36}> 0.5: {v['above_0.5']:<3}< 0.5: {v['below_0.5']:<3}"
              f"missing: {v['missing']}")

    print("\n=== Event-aligned hourly mean: imb1 (t = -24..71, step 6h) ===")
    print(f"{'t':>5} {'events':>8} {'controls':>9} {'ev_taker':>9} {'ct_taker':>9}")
    for i, t in enumerate(range(-24, 72, 6)):
        ei = aligned["t_hours"].index(t)
        print(
            f"{t:>5} "
            f"{fmt(aligned['events_mean']['imb1'][ei]):>8} "
            f"{fmt(aligned['controls_mean']['imb1'][ei]):>9} "
            f"{fmt(aligned['events_mean']['taker_buy_ratio'][ei]):>9} "
            f"{fmt(aligned['controls_mean']['taker_buy_ratio'][ei]):>9}"
        )

    # ------------------------------------------------------------------
    # report JSON
    # ------------------------------------------------------------------
    findings = {
        "one_line": (
            "No anticipatory order-book behaviour before big BTC weekends: "
            "Friday depth imbalance, book size and taker flow carry no "
            "directional information about the upcoming weekend move."
        ),
        "key_numbers": {
            "friday_imb1_events_vs_controls": [
                round(group_stats["friday_imb1"]["events"]["mean"], 4),
                round(group_stats["friday_imb1"]["controls"]["mean"], 4),
            ],
            "friday_imb1_long_vs_short_events": [
                round(group_stats["friday_imb1"]["events_long"]["mean"], 4),
                round(group_stats["friday_imb1"]["events_short"]["mean"], 4),
            ],
            "friday_imb5_events_vs_controls": [
                round(group_stats["friday_imb5"]["events"]["mean"], 4),
                round(group_stats["friday_imb5"]["controls"]["mean"], 4),
            ],
            "friday_imb5_long_vs_short_events": [
                round(group_stats["friday_imb5"]["events_long"]["mean"], 4),
                round(group_stats["friday_imb5"]["events_short"]["mean"], 4),
            ],
            "depth_ratio_events_vs_controls": [
                round(
                    group_stats["depth_ratio_fri_vs_window"]["events"][
                        "mean"
                    ],
                    4,
                ),
                round(
                    group_stats["depth_ratio_fri_vs_window"]["controls"][
                        "mean"
                    ],
                    4,
                ),
            ],
        },
        "interpretation": [
            "Directional test fails: if the book were anticipatory, long "
            "weekends should show bid-heavy Friday books and short weekends "
            "ask-heavy ones. Observed: short events have Friday imb1 as "
            "high or higher than long events (0.516 vs 0.509); imb5 is "
            "identical (0.547 vs 0.550). No directional content.",
            "The +/-5% band is structurally bid-heavy in BOTH groups "
            "(10/10 events and 9/12 controls above 0.5) - a baseline market "
            "structure feature, not an event precursor.",
            "'Anticipatory withdrawal' (book thinning) is NOT supported: "
            "Friday depth relative to the 4-day window is, if anything, "
            "slightly HIGHER for events (0.99) than controls (0.95), and "
            "highest for short events (1.04).",
            "Friday taker flow leans mildly sell-heavy for BOTH event "
            "directions (~0.494 vs control 0.501) - non-directional and "
            "tiny.",
            "Even the largest event (+11.76% weekend, 2025-03-02) shows a "
            "near-neutral Friday book (imb1 0.512, taker 0.492).",
            "Multiple-comparison caveat: 8 metrics x several windows were "
            "examined; the small event-vs-control gaps noted above (1-1.5 "
            "percentage points) are within overlapping group ranges and "
            "carry no direction information, so none survive as signal.",
        ],
        "verdict": (
            "Not worth building into weekend_gap confidence features at "
            "this data granularity (30s snapshots, innermost band +/-1%). "
            "The weekend moves arrive without measurable book precursors."
        ),
    }

    report = {
        "meta": {
            "question": (
                "Does the Binance USDT-M BTC order book show anticipatory "
                "behaviour (depth imbalance / book size / taker flow) before "
                "big weekend_gap trigger moves?"
            ),
            "events_total": len(all_events),
            "events_analyzed": len(ev),
            "controls_analyzed": len(ct),
            "coverage_caveats": [
                "bookDepth dumps start 2023-01-01: 14 of 25 trigger events "
                "(2021-2022) are NOT analysable for depth.",
                "Spread NOT analysed: futures bookTicker daily dumps are "
                "sparse (404 on many event days) and ~150MB/day where "
                "present.",
                "Small samples (events n=%d, controls n=%d); descriptive "
                "stats only, no significance claims."
                % (len(ev), len(ct)),
            ],
            "anchor": "t0 = Saturday 00:00 UTC (Friday close struck, "
                      "weekend return window opens)",
            "data_sources": [
                "data.binance.vision futures um daily bookDepth BTCUSDT "
                "(30s snapshots, cumulative notional within +/-1..5% of mid)",
                "data.binance.vision futures um daily aggTrades BTCUSDT",
                "data/btc_history.db daily closes (control selection)",
            ],
            "control_rule": (
                f"|weekend ret| < {CONTROL_MAX_ABS_RET}%, >= "
                f"{CONTROL_EXCLUDE_DAYS} days from any of the 25 trigger "
                f"weekends, n={N_CONTROLS}, seed={SEED}"
            ),
        },
        "weekends": [
            {
                "group": w["group"],
                "sunday": w["sunday"],
                "direction": w["direction"],
                "weekend_ret_pct": w["weekend_ret_pct"],
                "coverage": w["coverage"],
                "summary": w["summary"],
                "hourly": {
                    str(t): v for t, v in sorted(w["hourly"].items())
                },
            }
            for w in processed
        ],
        "group_stats": group_stats,
        "sign_consistency": consistency,
        "aligned_hourly": aligned,
        "findings": findings,
    }
    REPORT.parent.mkdir(exist_ok=True)
    REPORT.write_text(json.dumps(report, indent=1))
    print(f"\nWrote {REPORT}")


if __name__ == "__main__":
    main()
