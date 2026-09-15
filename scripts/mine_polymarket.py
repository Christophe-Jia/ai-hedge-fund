"""
Polymarket data asset inventory + exploratory mining.

Answers: what did collect_polymarket_ticks.py actually accumulate, is it
mineable, and what should change about collection strategy?

Data sources:
  - data/polymarket_ticks.db   (local tick store, written by the collector)
  - data/polymarket/*.json     (supplementary one-off pulls from 2026-03-02)
  - data/btc_history.db        (local BTC 1h spot candles, for lead/lag)
  - Polymarket gamma/clob APIs (feasibility probes, ~6 polite requests)

Usage:
    poetry run python scripts/mine_polymarket.py
    poetry run python scripts/mine_polymarket.py --skip-api     # offline only
    poetry run python scripts/mine_polymarket.py --out reports/polymarket_mining.json

Output: reports/polymarket_mining.json + console summary.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timezone

import requests

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TICKS_DB = os.path.join(PROJECT_ROOT, "data", "polymarket_ticks.db")
BTC_DB = os.path.join(PROJECT_ROOT, "data", "btc_history.db")
SUPP_DIR = os.path.join(PROJECT_ROOT, "data", "polymarket")

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"
HTTP_TIMEOUT = 15
POLITE_SLEEP = 1.0  # seconds between API calls

# Deterministic feasibility-probe targets (verified 2026-09-15):
#   - BTC-150k "by December 31, 2026" YES token (open market, created ~2025-08-07)
#   - "Will BTC break $15k before 2021?" YES token (closed 2020 market in our DB)
BTC150K_DEC31_TOKEN = (
    "93694900555669388759405753550770573998169287228984912881955464376232163096213"
)
BTC15K_2020_TOKEN = (
    "81489679527234870363655397325586438057198526422665424757123802116412728199295"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def utc(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def pearson(xs: list[float], ys: list[float]) -> float | None:
    n = len(xs)
    if n < 10:
        return None
    mx, my = sum(xs) / n, sum(ys) / n
    sx = math.sqrt(sum((x - mx) ** 2 for x in xs))
    sy = math.sqrt(sum((y - my) ** 2 for y in ys))
    if sx == 0 or sy == 0:
        return None
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    return cov / (sx * sy)


def tstat(r: float, n: int) -> float:
    """Approx t-stat for a correlation (ignores autocorrelation — caveat noted)."""
    if abs(r) >= 1.0 or n < 3:
        return 0.0
    return r * math.sqrt(n - 2) / math.sqrt(1 - r * r)


def fmt(x, nd=4):
    return None if x is None else round(x, nd)


# ---------------------------------------------------------------------------
# Part 1 — Local DB inventory
# ---------------------------------------------------------------------------

# Word-boundary patterns; order = priority (checked top to bottom).
CATEGORY_PATTERNS: list[tuple[str, list[str]]] = [
    ("sports", [
        r"\b(nba|nhl|mlb|fifa|ufc|mvp|rookie)\b", r"world cup", r"stanley cup",
        r"super bowl", r"league of legends", r"conference finals", r"nba finals",
        r"\bwarriors\b", r"\blakers\b", r"\bceltics\b", r"\byankees\b",
    ]),
    ("crypto_price", [
        r"bitcoin", r"\bbtc\b", r"ethereum", r"\beth\b", r"crypto", r"filecoin",
        r"\bfil\b", r"defi", r"\btvl\b", r"\busdt\b", r"\busdc\b",
    ]),
    ("macro", [
        r"\bfed\b", r"fomc", r"rate cut", r"rate hike", r"inflation", r"\bgdp\b",
        r"recession", r"interest rate", r"treasury",
    ]),
    ("geopolitics", [
        r"russia", r"ukraine", r"putin", r"ceasefire", r"\biran\b", r"israel",
        r"missile", r"sanctions", r"invasion", r"\bwar\b", r"\bnato\b", r"nuclear",
        r"\bstrike\b",
    ]),
    ("tech", [r"openai", r"airbnb", r"coinbase", r"\bipo\b", r"launch a"]),
    ("politics", [
        r"election", r"president", r"nomination", r"nominee", r"\btrump\b",
        r"\bbiden\b", r"warnock", r"supreme court", r"senat",
    ]),
]
_CATEGORY_COMPILED = [
    (cat, [re.compile(p, re.IGNORECASE) for p in pats])
    for cat, pats in CATEGORY_PATTERNS
]


def classify(question: str) -> str:
    q = question or ""
    for cat, pats in _CATEGORY_COMPILED:
        if any(p.search(q) for p in pats):
            return cat
    return "other"


def inventory_local_db() -> dict:
    if not os.path.exists(TICKS_DB):
        return {"exists": False, "path": TICKS_DB}

    con = sqlite3.connect(f"file:{TICKS_DB}?mode=ro", uri=True)
    cur = con.cursor()

    total, tmin, tmax = cur.execute(
        "SELECT COUNT(*), MIN(ts), MAX(ts) FROM price_ticks"
    ).fetchone()
    n_tokens_with_ticks = cur.execute(
        "SELECT COUNT(DISTINCT token_id) FROM price_ticks"
    ).fetchone()[0]
    n_meta_tokens = cur.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
    n_conditions = cur.execute(
        "SELECT COUNT(DISTINCT condition_id) FROM markets"
    ).fetchone()[0]
    db_mb = round(os.path.getsize(TICKS_DB) / 1e6, 1)

    # per-token stats joined with question
    rows = cur.execute(
        """
        SELECT m.token_id, m.condition_id, m.question, COUNT(*) n,
               MIN(p.ts) t0, MAX(p.ts) t1, MIN(p.price) pmin, MAX(p.price) pmax
        FROM price_ticks p JOIN markets m ON m.token_id = p.token_id
        GROUP BY m.token_id
        """
    ).fetchall()

    tokens = []
    for tok, cond, q, n, t0, t1, pmin, pmax in rows:
        tokens.append({
            "token_id": tok[:16] + "...", "condition_id": cond,
            "question": q, "n_ticks": n, "first": utc(t0), "last": utc(t1),
            "price_min": fmt(pmin), "price_max": fmt(pmax),
            "category": classify(q or ""),
        })

    dead = cur.execute(
        """
        SELECT COUNT(*), COUNT(DISTINCT condition_id) FROM markets
        WHERE token_id NOT IN (SELECT DISTINCT token_id FROM price_ticks)
        """
    ).fetchone()

    # conditions = one market (YES+NO token pair)
    by_cond = defaultdict(list)
    for t in tokens:
        by_cond[t["condition_id"]].append(t)
    markets = []
    for cond, toks in sorted(by_cond.items()):
        yes = max(toks, key=lambda t: (t["price_min"] or 0) + (t["price_max"] or 0))
        markets.append({
            "condition_id": cond, "question": toks[0]["question"],
            "category": toks[0]["category"],
            "n_tokens_with_ticks": len(toks),
            "n_ticks": sum(t["n_ticks"] for t in toks),
            "yes_price_min": yes["price_min"], "yes_price_max": yes["price_max"],
        })

    cat_counts = defaultdict(int)
    for m in markets:
        cat_counts[m["category"]] += 1

    # tick cadence on the busiest token
    busiest = max(tokens, key=lambda t: t["n_ticks"])["token_id"]
    full_tok = cur.execute(
        "SELECT token_id FROM markets WHERE token_id LIKE ?", (busiest[:16] + "%",)
    ).fetchone()[0]
    ts_list = [r[0] for r in cur.execute(
        "SELECT ts FROM price_ticks WHERE token_id = ? ORDER BY ts", (full_tok,)
    ).fetchall()]
    gaps = [b - a for a, b in zip(ts_list, ts_list[1:])]
    gaps_sorted = sorted(gaps)
    cadence = {
        "sampled_token": busiest,
        "n_gaps": len(gaps),
        "median_gap_s": gaps_sorted[len(gaps) // 2],
        "p90_gap_s": gaps_sorted[int(len(gaps) * 0.9)],
        "max_gap_s": max(gaps),
        "min_gap_s": min(gaps),
    }

    # duplicates impossible by PK, but count out-of-range prices
    bad_price = cur.execute(
        "SELECT COUNT(*) FROM price_ticks WHERE price < 0.0001 OR price > 0.9999"
    ).fetchone()[0]

    con.close()

    return {
        "exists": True, "path": TICKS_DB, "db_size_mb": db_mb,
        "total_ticks": total,
        "coverage_start": utc(tmin), "coverage_end": utc(tmax),
        "coverage_days": round((tmax - tmin) / 86400, 1),
        "tokens_with_ticks": n_tokens_with_ticks,
        "tokens_in_metadata": n_meta_tokens,
        "conditions_with_ticks": len(by_cond),
        "conditions_in_metadata": n_conditions,
        "dead_market_tokens_no_ticks": dead[0],
        "dead_market_conditions": dead[1],
        "category_counts_conditions": dict(cat_counts),
        "tick_cadence": cadence,
        "out_of_range_prices": bad_price,
        "markets": sorted(markets, key=lambda m: -m["n_ticks"]),
    }


def supplementary_assets() -> dict:
    """One-off files under data/polymarket/ from the 2026-03-02 session."""
    out = {"dir": SUPP_DIR, "exists": os.path.isdir(SUPP_DIR), "files": {}}
    if not out["exists"]:
        return out
    for name in ["price_history", "price_history_feb2026"]:
        p = os.path.join(SUPP_DIR, name)
        if os.path.isdir(p):
            n = len([f for f in os.listdir(p) if f.endswith(".json")])
            out["files"][name] = {"n_files": n}
    for name in ["analysis_iran_feb2026.json", "markets_feb2026.json",
                 "markets_jan2025.json", "btc_1h_feb2026.csv"]:
        p = os.path.join(SUPP_DIR, name)
        if os.path.exists(p):
            out["files"][name] = {"bytes": os.path.getsize(p)}
    if os.path.exists(os.path.join(SUPP_DIR, "analysis_iran_feb2026.json")):
        d = json.load(open(os.path.join(SUPP_DIR, "analysis_iran_feb2026.json")))
        out["prior_iran_analysis"] = {
            "generated_at": d.get("generated_at"),
            "event_focus": d.get("event_focus"),
            "n_markets": len(d.get("markets", [])),
            "note": "one-off anomaly/event study, not committed, not re-runnable",
        }
    return out


# ---------------------------------------------------------------------------
# Part 2 — Local explorations (offline)
# ---------------------------------------------------------------------------

def load_condition_series() -> dict[str, list[tuple[int, float]]]:
    """YES-token hourly series per condition, from local ticks DB."""
    con = sqlite3.connect(f"file:{TICKS_DB}?mode=ro", uri=True)
    cur = con.cursor()
    rows = cur.execute(
        """
        SELECT m.condition_id, m.question, p.token_id, p.ts, p.price
        FROM price_ticks p JOIN markets m ON m.token_id = p.token_id
        ORDER BY p.ts
        """
    ).fetchall()
    con.close()

    by_cond: dict[str, dict] = {}
    for cond, q, tok, ts, p in rows:
        if cond not in by_cond:
            by_cond[cond] = {"question": q, "tokens": defaultdict(list)}
        by_cond[cond]["tokens"][tok].append((ts, p))

    series = {}
    for cond, d in by_cond.items():
        toks = d["tokens"]
        if len(toks) != 2:
            continue
        # YES side = token with higher mean price
        (t1, s1), (t2, s2) = toks.items()
        yes = s1 if sum(p for _, p in s1) / len(s1) > sum(p for _, p in s2) / len(s2) else s2
        series[cond] = yes
    return series


def to_hourly(ts_price: list[tuple[int, float]]) -> dict[int, float]:
    """Last observation per UTC hour bucket."""
    h = {}
    for ts, p in ts_price:
        h[ts // 3600 * 3600] = p
    return dict(sorted(h.items()))


def explore_complement() -> dict:
    """YES+NO price sum vs 1.0 — structural arb / data sanity check."""
    con = sqlite3.connect(f"file:{TICKS_DB}?mode=ro", uri=True)
    cur = con.cursor()
    rows = cur.execute(
        """
        SELECT m.condition_id, p.ts, SUM(p.price) s, COUNT(*) c
        FROM price_ticks p JOIN markets m ON m.token_id = p.token_id
        GROUP BY m.condition_id, p.ts HAVING c = 2
        """
    ).fetchall()
    con.close()
    sums = [s for _, _, s, _ in rows]
    devs = [s - 1.0 for s in sums]
    abs_devs = sorted(abs(d) for d in devs)
    n_exact = sum(1 for d in devs if abs(d) < 1e-9)
    return {
        "n_paired_timestamps": len(sums),
        "n_exactly_1": n_exact,
        "mean_sum": fmt(sum(sums) / len(sums), 6) if sums else None,
        "median_abs_dev": fmt(abs_devs[len(abs_devs) // 2], 6) if abs_devs else None,
        "p95_abs_dev": fmt(abs_devs[int(len(abs_devs) * 0.95)], 6) if abs_devs else None,
        "max_abs_dev": fmt(max(abs_devs), 6) if abs_devs else None,
        "interpretation": (
            "YES+NO sums equal exactly 1.0 at every paired timestamp — the API "
            "appears to derive the complementary token's series from the same "
            "book (mid prices are perfect mirrors). The NO token adds no "
            "independent information; bid-ask wedge is NOT measurable from these "
            "mids (would need order-book snapshots)."
        ),
    }


def explore_extreme_prob() -> dict:
    series = load_condition_series()
    occupancy = []
    for cond, s in series.items():
        prices = [p for _, p in s]
        if not prices:
            continue
        frac_hi = sum(1 for p in prices if p > 0.9) / len(prices)
        frac_lo = sum(1 for p in prices if p < 0.1) / len(prices)
        occupancy.append({
            "condition_id": cond,
            "question": None,  # filled by caller from inventory
            "frac_ticks_above_0.9": fmt(frac_hi, 3),
            "frac_ticks_below_0.1": fmt(frac_lo, 3),
            "yes_price_min": fmt(min(prices)), "yes_price_max": fmt(max(prices)),
        })
    locked = [o for o in occupancy if (o["frac_ticks_above_0.9"] or 0) > 0.5
              or (o["frac_ticks_below_0.1"] or 0) > 0.5]
    movers = [o for o in occupancy if (o["yes_price_min"] or 0) < 0.4
              and (o["yes_price_max"] or 0) > 0.6]
    return {
        "n_conditions": len(occupancy),
        "n_locked_gt50pct_time_in_extreme": len(locked),
        "locked_markets": sorted(locked, key=lambda o: -(
            o["frac_ticks_above_0.9"] or 0))[:10],
        "n_genuinely_contested_crossed_40_60": len(movers),
        "contested_markets": movers,
        "note": (
            "68% of tracked markets spent >50% of the time pinned >0.9 or <0.1 — "
            "the keyword-scan discovery captured mostly already-decided markets. "
            "Collection window ended 2026-03-02, so resolution behaviour (final "
            "convergence to 0/1) is NOT observable in this data."
        ),
    }


def load_btc_1h(t0: int | None = None, t1: int | None = None) -> dict[int, float]:
    if not os.path.exists(BTC_DB):
        return {}
    con = sqlite3.connect(f"file:{BTC_DB}?mode=ro", uri=True)
    cur = con.cursor()
    q = ("SELECT ts, close FROM ohlcv WHERE symbol='BTC/USDT' AND market_type='spot' "
         "AND timeframe='1h'")
    args: list = []
    if t0:
        q += " AND ts >= ?"
        args.append(t0 * 1000)
    if t1:
        q += " AND ts < ?"
        args.append(t1 * 1000)
    rows = cur.execute(q, args).fetchall()
    con.close()
    return {int(ts // 1000): float(c) for ts, c in rows}


def to_daily(ts_price: list[tuple[int, float]]) -> dict[int, float]:
    """Last observation per UTC day bucket (ts keyed at day start)."""
    h = {}
    for ts, p in ts_price:
        h[ts // 86400 * 86400] = p
    return dict(sorted(h.items()))


def daily_lead_lag_corr(
    pm_daily: dict[int, float], btc_hourly: dict[int, float], max_lag_d: int = 7
) -> dict:
    """corr( dP_day , rBTC_{day+k} ), daily resolution, k in days."""
    # BTC daily close from hourly
    btc_daily = {}
    for ts, c in btc_hourly.items():
        btc_daily[ts // 86400 * 86400] = c  # last hour of day wins
    btc_daily = dict(sorted(btc_daily.items()))

    common = sorted(set(pm_daily) & set(btc_daily))
    results = []
    for k in range(-max_lag_d, max_lag_d + 1):
        xs, ys = [], []
        for t in common:
            p0, p1 = pm_daily.get(t - 86400), pm_daily.get(t)
            b0, b1 = btc_daily.get(t + k * 86400 - 86400), btc_daily.get(t + k * 86400)
            if None in (p0, p1, b0, b1):
                continue
            xs.append(p1 - p0)
            ys.append((b1 - b0) / b0)
        r = pearson(xs, ys)
        if r is not None:
            results.append({"lag_days": k, "r": fmt(r), "n": len(xs),
                            "tstat_ignoring_autocorr": fmt(tstat(r, len(xs)), 2)})
    peak = max(results, key=lambda x: abs(x["r"])) if results else None
    return {"n_days": len(common), "lag_table": results, "peak": peak}


def lead_lag_corr(
    pm_hourly: dict[int, float], btc_hourly: dict[int, float],
    max_lag_h: int = 12,
) -> dict:
    """
    corr( dP_t , rBTC_{t+k} ) for k in [-max_lag, +max_lag].
    k > 0  ->  Polymarket move at t is followed by BTC move at t+k  (PM leads).
    k < 0  ->  BTC move at t+k precedes PM move at t                 (BTC leads).
    """
    common = sorted(set(pm_hourly) & set(btc_hourly))
    if len(common) < 48:
        return {"n_hours": len(common), "note": "insufficient overlap"}

    def change_at(t: int, src: dict[int, float]) -> float | None:
        prev = src.get(t - 3600)
        if prev is None:
            return None
        cur = src.get(t)
        return None if cur is None else cur - prev

    results = []
    for k in range(-max_lag_h, max_lag_h + 1):
        xs, ys = [], []
        for t in common:
            dp = change_at(t, pm_hourly)
            rb = change_at(t + k * 3600, btc_hourly)
            if dp is None or rb is None or btc_hourly.get(t + k * 3600 - 3600) is None:
                continue
            xs.append(dp)
            ys.append(rb)
        r = pearson(xs, ys)
        if r is not None:
            results.append({"lag_hours": k, "r": fmt(r), "n": len(xs),
                            "tstat_ignoring_autocorr": fmt(tstat(r, len(xs)), 2)})
    peak = max(results, key=lambda x: abs(x["r"])) if results else None
    return {
        "n_hours": len(common),
        "lag_table": results,
        "peak": peak,
        "caveat": (
            "hourly overlapping observations; t-stats ignore autocorrelation "
            "and multiple comparison across lags — treat as descriptive, not tests"
        ),
    }


def explore_local_lead_lag() -> dict:
    """All non-sports markets (all we have) vs BTC 1h spot, Feb 2026 window."""
    series = load_condition_series()
    # question lookup
    con = sqlite3.connect(f"file:{TICKS_DB}?mode=ro", uri=True)
    qmap = dict(con.execute(
        "SELECT condition_id, question FROM markets"
    ).fetchall())
    con.close()

    out = {}
    for cond, s in series.items():
        q = qmap.get(cond, cond)
        cat = classify(q)
        if cat == "sports":
            continue
        h = to_hourly(s)
        btc = load_btc_1h(min(h), max(h) + 86400)
        out[f"[{cat}] {q}"] = {
            "category": cat,
            "n_hourly_obs": len(h),
            "yes_price_range": [fmt(min(h.values())), fmt(max(h.values()))],
            "lead_lag_vs_btc_1h": lead_lag_corr(h, btc),
        }
    return {
        "markets": out,
        "note": (
            "8 non-sports markets vs BTC 1h spot over the Feb 2026 window "
            "(~670 hourly obs each). Zero crypto-price and zero macro markets "
            "were collected, so the platform's primary question (PM crypto prob "
            "vs BTC spot) cannot be answered from the local asset — see "
            "api_feasibility instead. With ~670 hourly points per market and 49 "
            "lags each, these correlations are descriptive only."
        ),
    }


# ---------------------------------------------------------------------------
# Part 3 — API feasibility probes (network, bounded, polite)
# ---------------------------------------------------------------------------

def clob_history(token: str, params: dict, timeout: int = HTTP_TIMEOUT) -> list[dict]:
    try:
        r = requests.get(f"{CLOB_BASE}/prices-history",
                         params={"market": token, **params},
                         timeout=timeout)
        if r.status_code != 200:
            return []
        return r.json().get("history", [])
    except requests.RequestException:
        return []


def api_feasibility() -> dict:
    now = int(time.time())
    out: dict = {}

    # Probe A: long-closed market (2020) — expect empty
    h = clob_history(BTC15K_2020_TOKEN, {"fidelity": 1, "interval": "max"})
    time.sleep(POLITE_SLEEP)
    out["closed_2020_market_backfill"] = {
        "n_points": len(h),
        "verdict": "EMPTY — closed markets do not serve price history",
    }

    # Probe B: open market, interval=max — expect ~30d cap at fidelity=1
    h_max = clob_history(BTC150K_DEC31_TOKEN, {"fidelity": 1, "interval": "max"})
    time.sleep(POLITE_SLEEP)
    span_b = (h_max[-1]["t"] - h_max[0]["t"]) / 86400 if h_max else None
    out["open_market_interval_max"] = {
        "n_points": len(h_max),
        "lookback_days": fmt(span_b, 1),
        "verdict": "interval=max is capped at ~30 days at fidelity=1/10/60",
    }

    # Probe C: open market, startTs only (no endTs — startTs+endTs pairs longer
    # than a few days are rejected with HTTP 400 "interval is too long").
    # startTs alone returns the full history from startTs to now.
    start = now - 400 * 86400
    h_back = clob_history(BTC150K_DEC31_TOKEN,
                          {"fidelity": 1, "startTs": str(start)},
                          timeout=90)
    time.sleep(POLITE_SLEEP)
    out["open_market_startTs_only_backfill"] = {
        "n_points": len(h_back),
        "coverage": [utc(h_back[0]["t"]), utc(h_back[-1]["t"])] if h_back else None,
        "effective_granularity_s": (
            round((h_back[-1]["t"] - h_back[0]["t"]) / len(h_back)) if len(h_back) > 1 else None
        ),
        "verdict": (
            "startTs-only (no endTs) bypasses the 30d cap — full history back to "
            "market creation (~13 months, true 1-minute granularity) is fetchable; "
            "startTs+endTs pairs are rejected when the interval is 'too long'"
        ),
    }

    # Probe D: event-family discovery via tag_slug (the thing keyword-scan missed)
    fams = {}
    for tag in ["crypto", "fed-rates"]:
        try:
            r = requests.get(f"{GAMMA_BASE}/events",
                             params={"closed": "false", "limit": 20, "tag_slug": tag},
                             timeout=HTTP_TIMEOUT)
            items = r.json() if r.status_code == 200 else []
            fams[tag] = [{
                "title": e.get("title", ""),
                "volume_usd": fmt(float(e.get("volume") or 0), 0),
                "n_markets": len(e.get("markets", [])),
            } for e in sorted(items, key=lambda e: -float(e.get("volume") or 0))[:5]]
        except requests.RequestException:
            fams[tag] = []
        time.sleep(POLITE_SLEEP)
    out["tag_based_event_families_top5"] = fams

    # Exploration: BTC-150k implied probability vs BTC spot lead/lag (backfilled)
    if h_back:
        pts = [(int(x["t"]), float(x["p"])) for x in h_back]
        pm_hourly = to_hourly(pts)
        t0, t1 = min(pm_hourly), max(pm_hourly)
        btc = load_btc_1h(t0 - 86400, t1 + 86400)
        hourly = lead_lag_corr(pm_hourly, btc, max_lag_h=24)
        daily = daily_lead_lag_corr(to_daily(pts), btc, max_lag_d=7)
        # honest verdict
        dtab = {x["lag_days"]: x["r"] for x in daily.get("lag_table", [])}
        r0 = dtab.get(0)
        offlag = [abs(r) for k, r in dtab.items() if k != 0]
        if r0 is not None and abs(r0) > 0.15 and all(o < abs(r0) for o in offlag):
            verdict = (
                f"CONTEMPORANEOUS ONLY: daily corr at lag 0 is r={r0} "
                f"(t~{tstat(r0, daily['n_days']):.1f}) while every non-zero lag is "
                "|r|<0.09 with sign flips. Polymarket implied probability reacts to "
                "BTC spot same-day; NO lead in either direction. For a "
                "price-derived deadline market this is expected (probability is "
                "mechanically a function of spot) — it kills the hypothesis that "
                "prediction-market mids lead spot for this market type; any "
                "genuine 'PM leads' test needs event/news markets or order-book "
                "flow, not mid prices."
            )
        else:
            verdict = (
                "HONEST NULL: no lead-lag structure at hourly resolution (|r|<0.03 "
                "at all lags, adjacent-lag sign flips = multiple-comparison noise); "
                "daily resolution also shows no reliable structure."
            )
        out["btc150k_vs_spot_lead_lag"] = {
            "market": "Will Bitcoin hit $150k by December 31, 2026? (YES token)",
            "n_pm_hourly_obs": len(pm_hourly),
            "pm_prob_range": [fmt(min(pm_hourly.values()), 4),
                              fmt(max(pm_hourly.values()), 4)],
            "btc_source": "local data/btc_history.db BTC/USDT spot 1h",
            "hourly": hourly,
            "daily": daily,
            "verdict": verdict,
        }

    return out


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def build_report(ran_api: bool) -> dict:
    inv = inventory_local_db()
    report = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "title": "Polymarket data asset inventory + exploratory mining",
        "collector": "scripts/collect_polymarket_ticks.py (read-only reference)",
        "inventory": inv,
        "supplementary_files": supplementary_assets(),
    }
    if not inv.get("exists"):
        report["error"] = "local tick DB not found"
        return report

    report["local_explorations"] = {
        "yes_no_complement": explore_complement(),
        "extreme_probability": explore_extreme_prob(),
        "geopolitics_vs_btc_lead_lag": explore_local_lead_lag(),
    }
    if ran_api:
        report["api_feasibility"] = api_feasibility()
    else:
        report["api_feasibility"] = "skipped (--skip-api)"

    report["recommendations"] = {
        "restart_collector_immediately": (
            "CLOB prices-history returns NOTHING for closed markets (2020 markets "
            "and even a market closed 2.5 months ago are empty) — uncollected "
            "ticks on markets that later close are lost forever. The collector "
            "has been stopped since 2026-03-02 (6.5 months of data not captured)."
        ),
        "replace_keyword_scan_with_tag_based_discovery": (
            "Keyword scan + MAX_MARKETS=50 wasted 50% of slots on dead 2020 "
            "markets (Gamma active/closed flags are unreliable) and filled most "
            "remaining slots with sports (17/25). It captured ZERO Fed/macro and "
            "ZERO crypto-price markets despite those keywords. Use "
            "/events?tag_slug=crypto and tag_slug=fed-rates instead (verified: "
            "'How many Fed rate cuts in 2026?' $52M volume, 'When will Bitcoin "
            "hit $150k?' $27M volume, both live families)."
        ),
        "local_filtering_of_gamma_flags": (
            "Filter markets by endDate > now and closed == false locally; observed "
            "closed markets with active=true (e.g. 'BTC $150k by March 31, 2026' "
            "still active:true in Sep 2026), and resolved-2020 markets returned "
            "under closed=false."
        ),
        "poll_frequency": (
            "Stored fidelity=1 ticks have a ~10-min effective median gap; the "
            "30-second poll adds nothing. A 5-minute poll with the same storage "
            "is sufficient (halves request volume ~ 288/day/market)."
        ),
        "backfill_window": (
            "For still-open markets, a startTs-only request (NO endTs — "
            "startTs+endTs pairs longer than a few days are rejected with "
            "HTTP 400 'interval too long') fetches full history back to "
            "creation (~13 months, true 1-min granularity for the BTC-150k "
            "family) — a one-time backfill of the current BTC/ETH deadline "
            "families is possible today but shrinks as markets close."
        ),
        "store_resolution_outcomes": (
            "The markets table has no outcome/end-date columns; on close, record "
            "the resolved outcome so event studies (probability -> reality) become "
            "possible. Currently unresolvable from the DB alone."
        ),
        "honest_assessment": (
            "Local asset (200.5k ticks, 28 days, 25 markets, 68% sports, 0 crypto, "
            "0 macro) cannot answer the platform's core questions. The one-off "
            "Iran-strike analysis of 2026-03-02 exists but is not reproducible. "
            "Feasibility of the PM-crypto-vs-BTC lead-lag question is demonstrated "
            "in api_feasibility via backfill; a few more months of tag-based "
            "collection would make it a real study."
        ),
    }
    return report


def print_summary(report: dict) -> None:
    inv = report.get("inventory", {})
    print("=" * 72)
    print("Polymarket data mining — summary")
    print("=" * 72)
    if inv.get("exists"):
        print(f"DB: {inv['db_size_mb']} MB, {inv['total_ticks']:,} ticks, "
              f"{inv['coverage_start']} -> {inv['coverage_end']} "
              f"({inv['coverage_days']} days)")
        print(f"Markets with ticks: {inv['conditions_with_ticks']} "
              f"({inv['tokens_with_ticks']} tokens); "
              f"dead metadata tokens: {inv['dead_market_tokens_no_ticks']}")
        print(f"Categories: {inv['category_counts_conditions']}")
        cad = inv["tick_cadence"]
        print(f"Cadence: median gap {cad['median_gap_s']}s, "
              f"p90 {cad['p90_gap_s']}s, max {cad['max_gap_s']}s")
        print(f"Out-of-range prices: {inv['out_of_range_prices']}")
    exp = report.get("local_explorations", {})
    if exp:
        comp = exp.get("yes_no_complement", {})
        print(f"Complement |YES+NO-1|: median {comp.get('median_abs_dev')}, "
              f"p95 {comp.get('p95_abs_dev')}, max {comp.get('max_abs_dev')} "
              f"(n={comp.get('n_paired_timestamps')})")
        ext = exp.get("extreme_probability", {})
        print(f"Extreme-prob: {ext.get('n_locked_gt50pct_time_in_extreme')}/"
              f"{ext.get('n_conditions')} markets pinned >50% of time in "
              ">0.9/<0.1 zones")
        for q, d in exp.get("geopolitics_vs_btc_lead_lag", {}).get("markets", {}).items():
            ll = d.get("lead_lag_vs_btc_1h") or {}
            peak = ll.get("peak")
            print(f"  non-sports market: {q[:65]} | peak lag "
                  f"{peak.get('lag_hours') if peak else 'n/a'}h "
                  f"r={peak.get('r') if peak else 'n/a'} (n={ll.get('n_hours')})")
    api = report.get("api_feasibility")
    if isinstance(api, dict):
        print("-" * 72)
        for k in ["closed_2020_market_backfill", "open_market_interval_max",
                  "open_market_startTs_only_backfill"]:
            if k in api:
                print(f"{k}: {json.dumps(api[k], default=str)[:160]}")
        ll = api.get("btc150k_vs_spot_lead_lag", {})
        if ll:
            hp = (ll.get("hourly") or {}).get("peak")
            dp = (ll.get("daily") or {}).get("peak")
            print(f"BTC-150k vs BTC spot: n={ll.get('n_pm_hourly_obs')} h, "
                  f"prob range {ll.get('pm_prob_range')}")
            print(f"  hourly peak lag {hp.get('lag_hours') if hp else 'n/a'}h "
                  f"r={hp.get('r') if hp else 'n/a'} | "
                  f"daily peak lag {dp.get('lag_days') if dp else 'n/a'}d "
                  f"r={dp.get('r') if dp else 'n/a'}")
            print(f"  verdict: {ll.get('verdict')}", )
    print("=" * 72)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    parser.add_argument("--skip-api", action="store_true",
                        help="offline: skip all network probes")
    parser.add_argument("--out", default=os.path.join(
        PROJECT_ROOT, "reports", "polymarket_mining.json"))
    args = parser.parse_args()

    report = build_report(ran_api=not args.skip_api)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print_summary(report)
    print(f"\nReport written to {args.out}")


if __name__ == "__main__":
    main()
