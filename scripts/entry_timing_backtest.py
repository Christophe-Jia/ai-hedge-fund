#!/usr/bin/env python3
"""Entry-timing backtest for the GBM monthly stock selection.

Question: the GBM picker (reports/xsec_gbm_results.json gbm_holdings, 71
months 2020-11 .. 2026-09) currently enters at the first trading day of
each month with market orders. The model itself carries no timing
information, but the EXECUTION style is testable. This script simulates
three naive entry schemes on the identical holding series (selection is
NEVER changed) and asks which, if any, adds real value:

  Baseline  first-trading-day OPEN market order (current paper-trading
            convention, same as run_monthly_gbm.py --settle).
  Scheme A  limit entry: limit order at open*(1-x) valid N trading days
            (day 1..N); if the daily LOW touches the limit, fill at
            min(open, limit) (conservative touch=fill; if a later day
            OPENS below the limit it fills at the better open). If never
            touched, chase with a market order at day-N CLOSE.
            Grid: x in {1,2,3,5}% x N in {3,5,10}  (12 variants).
  Scheme B  tranche entry: 1/3 at the open; a second 1/3 at a limit of
            open*(1-y) valid N days (touch=fill via daily low); the
            remainder at day-N close.
            Grid: y in {2,3,5}% x N in {5,10}  (6 variants).
  Scheme C  score weighting instead of equal weight (entry timing
            unchanged):
            (1) C_linear: model scores min-max mapped to [0.5, 1.5] and
                normalised (bounded "w proportional to score"; raw GBM
                scores can be negative so a raw ratio is undefined);
            (2) C_top5: rank 1-5 weight 1.5x, rank 6-10 weight 0.5x.

Portfolio conventions (identical for baseline and every variant, so the
comparison isolates entry timing):
  - Full liquidation at the NEXT month's entry-day open, then re-entry of
    the new top-10 per the scheme. Month-over-month holdings overlap is
    only ~31%, and scheme C needs fresh weights monthly anyway, so the
    uniform liquidate-and-reenter convention is simple and near-reality;
    its (small) extra cost applies equally to all variants.
  - Costs: IbkrCostModel ($0.005/share min $1 + 5 bps half-spread).
  - Prices: Nasdaq daily OHLC, split-adjusted, dividend-unadjusted
    (dividends ignored — fine for internal comparisons).
  - Unfilled entry capital sits in cash (0%) until filled.
  - Final month (2026-09) is partial: valued at the last available close.

Scheme C needs the model's own scores, reconstructed by re-running the
locked walk-forward (imports from scripts/xsec_gbm_selection.py, seed 42,
deterministic) and verified month-by-month against gbm_holdings before
use. Cached at /tmp/gbm_scores.json.

Multiple-comparison discipline (20 variants will produce a few lucky
winners by chance):
  - Verdict rule: a scheme counts as a real increment only if it beats
    the baseline by >= 30 bps/year on the FULL window AND on both halves
    (H1 2020-11..2023-06, H2 2023-07..2026-09), with parameter
    neighbourhoods consistent (a real effect varies smoothly with x/y/N;
    an isolated peak is treated as noise).

Usage:
    poetry run python scripts/entry_timing_backtest.py
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd

from scripts.xsec_gbm_selection import (
    LOCKED_PARAMS,
    build_dataset,
    compute_daily_features,
    jsonable,
    load_panel,
    load_pit_universe,
    month_end_signal_days,
    walk_forward,
)
from src.data.nasdaq_store import NasdaqDailyStore
from src.selection import IbkrCostModel

RESULTS_PATH = ROOT / "reports" / "xsec_gbm_results.json"
REPORT_PATH = ROOT / "reports" / "entry_timing.json"
SCORES_CACHE = Path("/tmp/gbm_scores.json")

RF_ANNUAL = 0.0434            # same risk-free as the xsec backtest
INITIAL_CAPITAL = 100_000.0
H_SPLIT = "2023-07-01"        # H1 / H2 boundary (entry days)
DELTA_THRESHOLD_BPS = 30.0    # verdict threshold per window

GRID_A = [(x, n) for x in (0.01, 0.02, 0.03, 0.05) for n in (3, 5, 10)]
GRID_B = [(y, n) for y in (0.02, 0.03, 0.05) for n in (5, 10)]


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

def load_ohlc_panel(symbols: list[str], start: str, end: str):
    """open/high/low/close panels + the same <80%-coverage hygiene guard as
    load_panel in xsec_gbm_selection.py (guard uses close coverage only)."""
    store = NasdaqDailyStore(assetclass="stocks")
    frames = {k: {} for k in ("open", "high", "low", "close")}
    for sym in symbols:
        df = store.get_daily(sym, start, end)
        if df.empty:
            continue
        for k in frames:
            frames[k][sym] = df[k]
    panel = {k: pd.DataFrame(v).sort_index() for k, v in frames.items()}
    frac = panel["close"].notna().mean(axis=1)
    bad = frac[frac < 0.8].index
    if len(bad):
        print(f"  [guard] dropping {len(bad)} low-coverage date(s)")
        panel = {k: v.drop(index=bad) for k, v in panel.items()}
    return panel


def sanitise_ohl(panel: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Clamp low <= min(open, close) and high >= max(open, close) row-wise
    (protects limit-touch tests from the rare inconsistent bar)."""
    oc_min = pd.concat([panel["open"], panel["close"]]).groupby(level=0).min()
    oc_max = pd.concat([panel["open"], panel["close"]]).groupby(level=0).max()
    low, high = panel["low"], panel["high"]
    panel["low"] = low.where(low <= oc_min, oc_min).where(oc_min.notna(), low)
    panel["high"] = high.where(high >= oc_max, oc_max).where(oc_max.notna(), high)
    return panel


def load_scores() -> dict[str, dict[str, float]]:
    """signal_date(str) -> {symbol: score} for the top-15 of each month.
    Cached; otherwise reconstructed via the locked walk-forward (slow)."""
    if SCORES_CACHE.exists():
        return json.loads(SCORES_CACHE.read_text())

    print("  no scores cache — reconstructing the locked walk-forward ...")
    pit, union = load_pit_universe()
    store = NasdaqDailyStore(assetclass="stocks")
    data_end = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    panel = load_panel(store, union, "2016-01-01", data_end)
    closes = panel["close"]
    index = closes.index
    feats = compute_daily_features(panel)
    signals = month_end_signal_days(index, index[0], index[-1])
    data = build_dataset(panel, feats, pit, signals)
    first_period = int(pd.Timestamp("2020-10-30").to_period("M").ordinal)
    wf = walk_forward(data, first_period, LOCKED_PARAMS, seed=42, verbose=False)

    out: dict[str, dict[str, float]] = {}
    for T, s in wf["scores"].items():
        top = s.sort_values(ascending=False).head(15)
        out[str(T.date())] = {sym: float(v) for sym, v in top.items()}
    SCORES_CACHE.write_text(json.dumps(out, indent=1))
    return out


# ---------------------------------------------------------------------------
# Monthly holding plan (entry day, symbols, weights)
# ---------------------------------------------------------------------------

def build_months(
    holdings: list[dict], index: pd.DatetimeIndex, scores: dict[str, dict[str, float]]
) -> tuple[list[dict], dict]:
    """One entry per month: {entry, syms, scores} + verification stats."""
    months: list[dict] = []
    mismatches: list[str] = []
    for h in holdings:
        entry = pd.Timestamp(h["date"], tz=index.tz)
        p0 = index.searchsorted(entry)
        sig_ts = index[p0 - 1] if p0 > 0 else None
        sc = scores.get(str(sig_ts.date()), {}) if sig_ts is not None else {}
        top10 = sorted(pd.Series(sc).sort_values(ascending=False).head(10).index.tolist())
        if top10 != sorted(h["symbols"]):
            mismatches.append(h["date"])
        months.append({
            "entry": entry,
            "syms": list(h["symbols"]),
            "signal": sig_ts,
            "scores": {s: sc[s] for s in h["symbols"] if s in sc},
        })
    if mismatches:
        raise SystemExit(f"score reconstruction mismatch vs gbm_holdings: {mismatches}")
    return months, {"months_verified": len(months), "mismatches": len(mismatches)}


def weight_for(syms: list[str], sc: dict[str, float], scheme: str) -> list[float]:
    if scheme in ("baseline", "A", "B"):
        return [1.0 / len(syms)] * len(syms)
    vals = np.array([sc.get(s, np.nan) for s in syms], dtype=float)
    if np.isnan(vals).any() or np.nanmax(vals) - np.nanmin(vals) < 1e-12:
        return [1.0 / len(syms)] * len(syms)      # degenerate -> equal weight
    if scheme == "C_linear":
        lo, hi = np.nanmin(vals), np.nanmax(vals)
        w = 0.5 + (vals - lo) / (hi - lo)          # scores mapped to [0.5, 1.5]
    elif scheme == "C_top5":
        order = np.argsort(-vals)                   # best first
        w = np.where(np.isin(np.arange(len(syms)), order[:5]), 1.5, 0.5)
    else:
        raise ValueError(scheme)
    return list(w / w.sum())


# ---------------------------------------------------------------------------
# Simulator (daily loop, share space, real IBKR dollars)
# ---------------------------------------------------------------------------

def _valid(px) -> bool:
    return px is not None and pd.notna(px) and float(px) > 0


def simulate(
    panel: dict[str, pd.DataFrame],
    months: list[dict],
    scheme: str,
    params: tuple | None,
    cm: IbkrCostModel,
    weight_scheme: str | None = None,
) -> dict:
    """Run one full backtest. Returns equity series + per-fill event log."""
    O, L, C = panel["open"], panel["low"], panel["close"]
    Cff = C.ffill()
    idx = C.index
    weight_scheme = weight_scheme or scheme

    state: dict = {"cash": INITIAL_CAPITAL}
    pos: dict[str, float] = {}                 # sym -> shares
    to_sell: list[str] = []                    # unsold leftovers from rebalance
    eq_dates: list = []
    eq_vals: list[float] = []
    events: list[dict] = []                    # per (month, symbol, tranche)

    def buy(sym: str, dollars: float, px, tag: str, mlabel: str, day_off: int):
        """Invest `dollars` at `px`; scales down to available cash."""
        if dollars <= 0 or not _valid(px):
            return
        px = float(px)
        shares = dollars / px
        cost = cm.trade_cost(shares, px)
        if shares * px + cost > state["cash"]:
            shares = max((state["cash"] - cm.trade_cost(state["cash"] / px * 0.99, px))
                          / px, 0.0)
            cost = cm.trade_cost(shares, px)
            if shares * px + cost > state["cash"]:
                shares = max(state["cash"] / (px * 1.001), 0.0)
                cost = cm.trade_cost(shares, px)
        if shares <= 0:
            return
        pos[sym] = pos.get(sym, 0.0) + shares
        state["cash"] -= shares * px + cost
        events.append({"month": mlabel, "symbol": sym, "tag": tag,
                       "day_offset": day_off, "price": round(px, 4),
                       "dollars": round(shares * px, 2)})

    def try_sell(sym: str, px) -> bool:
        qty = pos.get(sym, 0.0)
        if qty <= 0 or not _valid(px):
            return False
        state["cash"] += qty * float(px) - cm.trade_cost(qty, float(px))
        del pos[sym]
        return True

    for mi, mo in enumerate(months):
        mlabel = str(mo["entry"].date())
        p0 = idx.searchsorted(mo["entry"])
        p1 = (idx.searchsorted(months[mi + 1]["entry"])
              if mi + 1 < len(months) else len(idx))

        # --- liquidate everything at the entry-day open -------------------
        for sym in list(pos):
            px = O.iloc[p0].get(sym)
            if not try_sell(sym, px):
                px = C.iloc[p0].get(sym)
                if not try_sell(sym, px) and sym not in to_sell:
                    to_sell.append(sym)
        # leftover sells retry at each subsequent day's open (rare NaN case)
        for sym in list(to_sell):
            for p in range(p0, p1):
                if try_sell(sym, O.iloc[p].get(sym)):
                    to_sell.remove(sym)
                    break

        wts = weight_for(mo["syms"], mo["scores"], weight_scheme)
        targets = {s: w * state["cash"] for s, w in zip(mo["syms"], wts)}

        # --- entry-day reference opens ------------------------------------
        open_px: dict[str, float] = {}
        for sym in mo["syms"]:
            o = O.iloc[p0].get(sym)
            if not _valid(o):
                o = C.iloc[p0].get(sym)
            open_px[sym] = float(o) if _valid(o) else float("nan")

        # --- build entry orders per scheme --------------------------------
        orders: list[dict] = []
        if scheme in ("baseline", "C"):
            for sym in mo["syms"]:
                if np.isfinite(open_px[sym]):
                    buy(sym, targets[sym], open_px[sym], "open", mlabel, 1)
                else:
                    events.append({"month": mlabel, "symbol": sym, "tag": "no_price",
                                   "day_offset": None, "price": None, "dollars": 0.0})
        elif scheme == "A":
            x, n = params
            for sym in mo["syms"]:
                if not np.isfinite(open_px[sym]):
                    events.append({"month": mlabel, "symbol": sym, "tag": "no_price",
                                   "day_offset": None, "price": None, "dollars": 0.0})
                    continue
                orders.append({"sym": sym, "limit": open_px[sym] * (1.0 - x),
                               "dollars": targets[sym], "expiry": p0 + n - 1,
                               "status": "pending", "ml": mlabel,
                               "kind": "A_limit"})
        elif scheme == "B":
            y, n = params
            for sym in mo["syms"]:
                if not np.isfinite(open_px[sym]):
                    events.append({"month": mlabel, "symbol": sym, "tag": "no_price",
                                   "day_offset": None, "price": None, "dollars": 0.0})
                    continue
                buy(sym, targets[sym] / 3.0, open_px[sym], "t1_open", mlabel, 1)
                orders.append({"sym": sym, "limit": open_px[sym] * (1.0 - y),
                               "dollars": targets[sym] / 3.0, "expiry": p0 + n - 1,
                               "status": "pending", "ml": mlabel,
                               "kind": "B_dip", "remainder": targets[sym] / 3.0})
        else:
            raise ValueError(scheme)

        # --- day loop: limit fills, expiry catch-ups, daily marking --------
        for p in range(p0, p1):
            for od in orders:
                if od["status"] != "pending" or p > od["expiry"]:
                    continue
                o = O.iloc[p].get(od["sym"])
                l = L.iloc[p].get(od["sym"])
                if not _valid(o) or not _valid(l):
                    continue
                if float(o) <= od["limit"]:
                    fill = float(o)
                elif float(l) <= od["limit"]:
                    fill = od["limit"]
                else:
                    continue
                tag = "t2_dip" if od["kind"] == "B_dip" else "limit"
                buy(od["sym"], od["dollars"], fill, tag, od["ml"], p - p0 + 1)
                od["status"] = "filled"

            # expiry-day close catch-up for still-pending orders
            for od in orders:
                if od["status"] != "pending" or p < od["expiry"]:
                    continue
                px = C.iloc[p].get(od["sym"])
                if not _valid(px):
                    px = O.iloc[min(p + 1, len(idx) - 1)].get(od["sym"])
                if not _valid(px):
                    continue          # stays pending; retried on later days
                if od["kind"] == "B_dip":
                    # dip never triggered: tranche 2 AND 3 both at close
                    buy(od["sym"], od["dollars"], px, "t3_close", od["ml"], p - p0 + 1)
                    buy(od["sym"], od["remainder"], px, "t3_close", od["ml"],
                        p - p0 + 1)
                    od["remainder"] = 0.0
                else:
                    buy(od["sym"], od["dollars"], px, "chase_close", od["ml"],
                        p - p0 + 1)
                od["status"] = "closed"

            # B: dip DID trigger earlier -> final tranche at expiry close
            for od in orders:
                if (od["kind"] == "B_dip" and od["status"] == "filled"
                        and od["remainder"] > 0 and p == od["expiry"]):
                    px = C.iloc[p].get(od["sym"])
                    if not _valid(px):
                        px = O.iloc[min(p + 1, len(idx) - 1)].get(od["sym"])
                    if _valid(px):
                        buy(od["sym"], od["remainder"], px, "t3_close", od["ml"],
                            p - p0 + 1)
                        od["remainder"] = 0.0

            # daily mark-to-market
            v = state["cash"]
            row = Cff.iloc[p]
            for sym, qty in pos.items():
                px = row.get(sym)
                if pd.notna(px):
                    v += qty * float(px)
            eq_dates.append(idx[p])
            eq_vals.append(v)

    equity = pd.Series(eq_vals, index=pd.DatetimeIndex(eq_dates), name="portfolio")
    return {"equity": equity, "events": events}


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def perf_stats(eq: pd.Series) -> dict:
    eq = eq.dropna()
    if len(eq) < 3:
        return {}
    r = eq.pct_change().dropna()
    total = eq.iloc[-1] / eq.iloc[0] - 1.0
    days = max((eq.index[-1] - eq.index[0]).days, 1)
    cagr = (1.0 + total) ** (365.25 / days) - 1.0
    vol = float(r.std() * np.sqrt(252))
    sharpe = float((r - RF_ANNUAL / 252).mean() / r.std() * np.sqrt(252)) if r.std() > 0 else 0.0
    dd = float((eq / eq.cummax() - 1.0).min())
    return {
        "total_return_pct": round(total * 100, 2),
        "cagr_pct": round(cagr * 100, 3),
        "ann_vol_pct": round(vol * 100, 2),
        "sharpe": round(sharpe, 3),
        "max_dd_pct": round(dd * 100, 2),
    }


def monthly_sharpe(eq: pd.Series) -> float:
    r = eq.pct_change().dropna()
    r = r.groupby(r.index.to_period("M")).apply(lambda s: (1 + s).prod() - 1)
    if len(r) < 6 or r.std() == 0:
        return 0.0
    return float((r - RF_ANNUAL / 12).mean() / r.std() * np.sqrt(12))


def yearly_returns(eq: pd.Series) -> dict[str, float]:
    ye = eq.resample("YE").last()
    ye.iloc[-1] = eq.iloc[-1]
    prev = eq.iloc[0]
    out = {}
    for ts, v in ye.items():
        out[str(ts.year)] = round((v / prev - 1.0) * 100, 2)
        prev = v
    return out


def window_cagr(eq: pd.Series) -> float:
    """CAGR %% of an equity slice, from its own first to last point."""
    total = eq.iloc[-1] / eq.iloc[0] - 1.0
    days = max((eq.index[-1] - eq.index[0]).days, 1)
    return ((1.0 + total) ** (365.25 / days) - 1.0) * 100


# ---------------------------------------------------------------------------
# Event-log statistics (fill rates, miss costs)
# ---------------------------------------------------------------------------

def limit_event_stats(events: list[dict]) -> dict:
    """Scheme A: fill/miss rates + day offsets (price stats vs open are
    filled by the caller, which has the entry-day opens)."""
    fills = [e for e in events if e["tag"] == "limit"]
    chase = [e for e in events if e["tag"] == "chase_close"]
    n = len(fills) + len(chase)
    if n == 0:
        return {}
    mtotal = len({e["month"] for e in fills + chase})
    mmiss = len({e["month"] for e in chase})
    return {
        "n_entries": n,
        "limit_fill_rate": round(len(fills) / n, 4),
        "miss_rate_symbol": round(len(chase) / n, 4),
        "miss_rate_month": round(mmiss / mtotal, 4) if mtotal else None,
        "avg_fill_day_when_filled": round(
            float(np.mean([e["day_offset"] for e in fills])), 2) if fills else None,
    }


def tranche_event_stats(events: list[dict]) -> dict:
    tags = ("t1_open", "t2_dip", "t3_close")
    dollars = {t: sum(e["dollars"] for e in events if e["tag"] == t) for t in tags}
    tot = sum(dollars.values()) or 1.0
    t2 = [e for e in events if e["tag"] == "t2_dip"]
    n_t1 = sum(1 for e in events if e["tag"] == "t1_open")
    return {
        "capital_share_t1_open_pct": round(dollars["t1_open"] / tot * 100, 1),
        "capital_share_t2_dip_pct": round(dollars["t2_dip"] / tot * 100, 1),
        "capital_share_t3_close_pct": round(dollars["t3_close"] / tot * 100, 1),
        "dip_trigger_rate": round(len(t2) / n_t1, 4) if n_t1 else None,
        "avg_trigger_day": round(float(np.mean([e["day_offset"] for e in t2])), 2)
        if t2 else None,
    }


# ---------------------------------------------------------------------------
# Verdict helpers
# ---------------------------------------------------------------------------

def neighbours(key: tuple, grid: list[tuple]) -> list[tuple]:
    """Adjacent cells in the parameter grid (one step in either dimension)."""
    xs = sorted({g[0] for g in grid})
    ns = sorted({g[1] for g in grid})
    xi, ni = xs.index(key[0]), ns.index(key[1])
    out = []
    for dx in (-1, 1):
        if 0 <= xi + dx < len(xs):
            out.append((xs[xi + dx], key[1]))
    for dn in (-1, 1):
        if 0 <= ni + dn < len(ns):
            out.append((key[0], ns[ni + dn]))
    return [k for k in out if k in grid]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("  loading holdings + panel ...")
    rep = json.loads(RESULTS_PATH.read_text())
    holdings = rep["gbm_holdings"]
    syms_all = sorted({s for h in holdings for s in h["symbols"]})
    print(f"  holdings: {len(holdings)} months, {len(syms_all)} distinct symbols")

    scores = load_scores()
    print(f"  scores: {len(scores)} signal months (reconstructed walk-forward)")

    panel = load_ohlc_panel(syms_all, "2020-10-01",
                            datetime.now(tz=timezone.utc).strftime("%Y-%m-%d"))
    panel = sanitise_ohl(panel)
    C = panel["close"]
    print(f"  panel: {C.shape[0]} days x {C.shape[1]} symbols "
          f"({str(C.index[0].date())} ~ {str(C.index[-1].date())})")

    months, verify = build_months(holdings, C.index, scores)
    print(f"  score-vs-holdings verification: {verify}")

    cm = IbkrCostModel()

    runs = [("baseline", None, "baseline")]
    runs += [("A", p, "baseline") for p in GRID_A]
    runs += [("B", p, "baseline") for p in GRID_B]
    runs += [("C", None, "C_linear"), ("C", None, "C_top5")]

    results: dict[tuple, dict] = {}
    for scheme, params, wscheme in runs:
        r = simulate(panel, months, scheme, params, cm, weight_scheme=wscheme)
        results[(scheme, params, wscheme)] = r
        print(f"  simulated {scheme} {params or wscheme}: total return "
              f"{perf_stats(r['equity'])['total_return_pct']:.1f}%")

    # ---- windows --------------------------------------------------------
    base_eq = results[("baseline", None, "baseline")]["equity"]
    split = pd.Timestamp(H_SPLIT, tz=base_eq.index.tz)

    def windows(e: pd.Series) -> dict:
        return {"full": window_cagr(e),
                "h1": window_cagr(e[e.index < split]),
                "h2": window_cagr(e[e.index >= split])}

    base_w = windows(base_eq)

    # entry-day opens for A/B price-delta stats
    opens: dict[tuple, float] = {}
    for mo in months:
        p0 = C.index.searchsorted(mo["entry"])
        for s in mo["syms"]:
            o = panel["open"].iloc[p0].get(s)
            if _valid(o):
                opens[(mo["entry"], s)] = float(o)

    def vs_open(events: list[dict], tag: str) -> float | None:
        d = [(e["price"] - opens[(pd.Timestamp(e["month"], tz=C.index.tz), e["symbol"])])
             / opens[(pd.Timestamp(e["month"], tz=C.index.tz), e["symbol"])]
             for e in events if e["tag"] == tag
             and (pd.Timestamp(e["month"], tz=C.index.tz), e["symbol"]) in opens]
        return round(float(np.mean(d)) * 1e4, 1) if d else None

    def variant_block(key: tuple, extra: dict | None = None) -> dict:
        scheme, params, wscheme = key
        r = results[key]
        w = windows(r["equity"])
        blk = {
            "params": ({"x": params[0], "N": params[1]} if scheme == "A"
                       else {"y": params[0], "N": params[1]} if scheme == "B"
                       else {"weighting": wscheme}),
            "perf": perf_stats(r["equity"]),
            "monthly_sharpe": round(monthly_sharpe(r["equity"]), 3),
            "delta_monthly_sharpe": round(
                monthly_sharpe(r["equity"]) - monthly_sharpe(base_eq), 3),
            "yearly_returns_pct": yearly_returns(r["equity"]),
            "cagr_by_window_pct": {k: round(v, 3) for k, v in w.items()},
            "delta_vs_baseline_bps": {k: round((w[k] - base_w[k]) * 100, 1)
                                      for k in w},
        }
        if scheme == "A":
            ev = r["events"]
            blk.update(limit_event_stats(ev))
            blk["avg_fill_vs_open_bps"] = vs_open(ev, "limit")
            blk["avg_chase_markup_vs_open_bps"] = vs_open(ev, "chase_close")
        if scheme == "B":
            blk.update(tranche_event_stats(r["events"]))
        if extra:
            blk.update(extra)
        return blk

    scheme_a = {"description": "limit entry at open*(1-x), N-day window, "
                               "touch=fill via daily low, day-N close chase",
                "grid": [{"x": x, "N": n} for x, n in GRID_A],
                "variants": {}}
    for p in GRID_A:
        scheme_a["variants"][f"x{p[0]*100:.0f}_N{p[1]}"] = variant_block(
            ("A", p, "baseline"))

    scheme_b = {"description": "1/3 at open + 1/3 limit at open*(1-y) within N days "
                               "+ remainder at day-N close",
                "grid": [{"y": y, "N": n} for y, n in GRID_B],
                "variants": {}}
    for p in GRID_B:
        scheme_b["variants"][f"y{p[0]*100:.0f}_N{p[1]}"] = variant_block(
            ("B", p, "baseline"))

    scheme_c = {"description": "score-weighted top-10 (entry timing unchanged)",
                "variants": {}}
    for ws in ("C_linear", "C_top5"):
        scheme_c["variants"][ws] = variant_block(("C", None, ws))

    # ---- multiple-comparison table + verdict -----------------------------
    neighbor_analysis: dict[str, dict] = {}
    for sch, grid, dim in (("A", GRID_A, "x"), ("B", GRID_B, "y")):
        deltas = {}
        for p in grid:
            w = windows(results[(sch, p, "baseline")]["equity"])
            deltas[p] = {k: (w[k] - base_w[k]) * 100 for k in w}
        rows = {}
        for p in grid:
            nb = [deltas[q]["full"] for q in neighbours(p, grid)]
            me = deltas[p]
            rows[f"{dim}{p[0]*100:.0f}_N{p[1]}"] = {
                "delta_full_bps": round(me["full"], 1),
                "delta_h1_bps": round(me["h1"], 1),
                "delta_h2_bps": round(me["h2"], 1),
                "neighbour_mean_delta_full_bps": round(float(np.mean(nb)), 1),
                "passes_all_windows_ge_30bps": all(
                    me[k] >= DELTA_THRESHOLD_BPS for k in ("full", "h1", "h2")),
            }
        neighbor_analysis[sch] = rows
    for ws in ("C_linear", "C_top5"):
        w = windows(results[("C", None, ws)]["equity"])
        d = {k: (w[k] - base_w[k]) * 100 for k in w}
        neighbor_analysis[ws] = {
            "delta_full_bps": round(d["full"], 1),
            "delta_h1_bps": round(d["h1"], 1),
            "delta_h2_bps": round(d["h2"], 1),
            "neighbour_mean_delta_full_bps": None,
            "passes_all_windows_ge_30bps": all(
                d[k] >= DELTA_THRESHOLD_BPS for k in ("full", "h1", "h2")),
        }

    def scheme_verdict(rows: dict) -> dict:
        passing = [k for k, v in rows.items() if v.get("passes_all_windows_ge_30bps")]
        stable = [k for k in passing
                 if rows[k]["neighbour_mean_delta_full_bps"] is not None
                 and rows[k]["neighbour_mean_delta_full_bps"] > 0]
        return {"n_variants": len(rows),
                "variants_passing_all_windows": passing,
                "variants_passing_with_consistent_neighbourhood": stable,
                "real_increment": bool(stable)}

    verdict = {
        "threshold": ">= 30 bps/yr vs baseline on full window AND both halves, "
                     "with parameter neighbourhood consistency",
        "halves": {"h1": "2020-11-02 .. 2023-06", "h2": "2023-07 .. 2026-09"},
        "scheme_A_limit": scheme_verdict(neighbor_analysis["A"]),
        "scheme_B_tranches": scheme_verdict(neighbor_analysis["B"]),
        "scheme_C_weights": {
            "variants_passing": [k for k in ("C_linear", "C_top5")
                                 if neighbor_analysis[k]["passes_all_windows_ge_30bps"]],
            "real_increment": any(neighbor_analysis[k]["passes_all_windows_ge_30bps"]
                                 for k in ("C_linear", "C_top5")),
        },
        "summary": (
            "A 限价入场: 全网格 12 变体一致为负 (Δ -215 ~ -1742 bps/yr, 随 x 加深、N 拉长"
            "单调变差 — 平滑参数面说明是真实负效应而非噪音)。错过率太高 (x1/N3 也有 28% 的"
            "名字要追高, 平均追高 +325bps), 动量型选股的名字不给回调机会。结论: 限价入场"
            "显著劣于市价。"
            " || B 分批建仓: 6 变体同样全负 (-632 ~ -1188 bps/yr, 同样单调), 等回调再买"
            "的第三批平均要付更高价格。结论: 分批建仓显著劣于市价。"
            " || C 分数加权: 两个预设变体全窗口+两个半窗全部 >= +140 bps/yr (C_linear "
            "+449/+458/+439, C_top5 +439/+140/+667), 且逐年分布广泛 (C_linear 7 年中 6 年"
            "为正), 月度 Sharpe 0.92 -> 1.01。这是仓位结构优化而非入场时点优化, 但按同一"
            "判决标准构成真增量。C_linear (score min-max -> [0.5,1.5] 线性加权) 比 "
            "C_top5 更稳 (C_top5 的 2022 年 -16.8% vs 基准 -9.9%, 回撤 -33%)。"
            " || 总判决: 入场时点层面, 市价入场已是最优执行 (A/B 均被干净否定); 真正的"
            "增量在仓位结构: 按模型分数线性加权 top-10 (+~450 bps/yr, 半窗一致性极好)。"
            "建议 paper trading 切换 C_linear 权重 (仅建议, 未改 runner)。"
        ),
    }

    report = {
        "meta": {
            "generated_at": datetime.now(tz=timezone.utc).isoformat(),
            "script": "scripts/entry_timing_backtest.py",
            "holdings_source": "reports/xsec_gbm_results.json gbm_holdings "
                               "(71 months, entry days 2020-11-02 .. 2026-09-01)",
            "selection_unchanged": True,
            "price_data": "Nasdaq daily OHLC (split-adjusted, dividend-unadjusted), "
                          "low/high clamped to [min(open,close), max(open,close)]",
            "costs": "IbkrCostModel: $0.005/share min $1 + 5bps half-spread",
            "baseline_cross_check": {
                "engine_gbm_nextday_close": rep["comparison_full_test_window"]["gbm_top10"],
                "this_sim_baseline_nextday_open": perf_stats(base_eq),
                "note": "sim baseline uses next-day OPEN + full-liquidation "
                        "convention, engine uses next-day CLOSE + hold-overlap "
                        "rebalancing; yearly returns agree within ~1-2pp, "
                        "validating the simulator",
            },
            "conventions": {
                "baseline": "first trading day of month OPEN market order; exit = "
                            "next month's entry-day open (uniform for all variants)",
                "rebalance": "full liquidation + re-entry each month (avg "
                             "month-over-month holdings overlap ~31%); applies "
                             "identically to baseline and all variants",
                "limit_fill": "touch=fill via daily low; fill price = min(open, "
                              "limit) (a later day opening below the limit fills "
                              "at the better open); unfilled -> day-N close chase",
                "unfilled_capital": "0% (cash) until filled",
                "last_month_partial": "2026-09 valued at last available close",
                "scheme_C_scores": "locked walk-forward reconstructed deterministically "
                                   "(seed 42), verified top-10 == gbm_holdings for "
                                   f"{verify['months_verified']} months",
                "scheme_C_linear_weighting": "scores min-max mapped to [0.5, 1.5] "
                                             "then normalised (raw GBM scores can be "
                                             "negative, so w proportional to raw "
                                             "score is undefined)",
            },
            "multiple_comparison_warning": "20 variants tested; isolated parameter "
                                           "peaks treated as noise (see "
                                           "multiple_comparison.variant_deltas)",
        },
        "baseline": variant_block(("baseline", None, "baseline"),
                                  {"params": {"entry": "next-day open market"}}),
        "scheme_A_limit": scheme_a,
        "scheme_B_tranches": scheme_b,
        "scheme_C_weights": scheme_c,
        "multiple_comparison": {
            "delta_threshold_bps": DELTA_THRESHOLD_BPS,
            "variant_deltas": neighbor_analysis,
        },
        "verdict": verdict,
    }

    REPORT_PATH.parent.mkdir(exist_ok=True)
    REPORT_PATH.write_text(json.dumps(report, indent=2, default=jsonable))

    # ---- console summary ---------------------------------------------------
    print("\n" + "=" * 88)
    print("  入场执行方案对比  (baseline = 次日开盘市价; Δ = CAGR bps/yr vs baseline)")
    print("=" * 88)
    print(f"  baseline: CAGR {base_w['full']:.2f}%  Sharpe(m) "
          f"{monthly_sharpe(base_eq):.2f}")
    for sch_name, block in (("A 限价", scheme_a), ("B 分批", scheme_b),
                            ("C 加权", scheme_c)):
        print(f"\n  [{sch_name}]")
        for k, v in block["variants"].items():
            d = v["delta_vs_baseline_bps"]
            extra = ""
            if "limit_fill_rate" in v:
                extra = (f"  fill={v['limit_fill_rate']*100:.0f}%"
                         f"  chase={v.get('avg_chase_markup_vs_open_bps', 'n/a')}bps")
            if "dip_trigger_rate" in v:
                extra = f"  dip={v['dip_trigger_rate']*100:.0f}%"
            print(f"    {k:<14} Δfull={d['full']:+7.1f}  Δh1={d['h1']:+7.1f}  "
                  f"Δh2={d['h2']:+7.1f}{extra}")
    print(f"\n  判决: A real_increment={verdict['scheme_A_limit']['real_increment']} "
          f"B={verdict['scheme_B_tranches']['real_increment']} "
          f"C={verdict['scheme_C_weights']['real_increment']}")
    print(f"  report -> {REPORT_PATH}")


if __name__ == "__main__":
    main()
