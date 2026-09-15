#!/usr/bin/env python3
"""Monthly cross-sectional fundamental features from EDGAR facts.

Turns `data/fundamentals.db` (see scripts/fetch_edgar_fundamentals.py) into a
symbol x month panel of value / quality / growth / investment features, with
every number derived ONLY from facts whose `filed_date` is on or before the
signal date. This is the point-in-time discipline the ingester exists for:
`as_of_facts` is applied as an event-driven replay, so a value restated in
2024 cannot enter a 2021 signal.

Universe
--------
PIT S&P 500 membership (data/universe/sp500_YYYY.json) intersected with the
symbols that have BOTH local price data and EDGAR facts. Prices are required
because market-cap factors need one; names without either are recorded in the
coverage report rather than silently dropped.

Features
--------
value        ep_ttm   TTM net income / market cap          (earnings yield)
             bp       book equity / market cap              (book-to-price)
             sp_ttm   TTM revenue / market cap
quality      roe      TTM net income / book equity
             gross_margin      TTM gross profit / TTM revenue
             asset_turnover    TTM revenue / total assets
growth       rev_yoy  TTM revenue vs 12 months earlier
             ni_yoy   TTM net income vs 12 months earlier
investment   asset_growth   total assets vs 12 months earlier
             (the academic "investment factor": high asset growth has
              historically predicted LOW subsequent returns)
accounting   accruals  (TTM net income - TTM operating cash flow) / assets
                         (high accruals = lower earnings quality)
extras       ocf_yield TTM operating cash flow / market cap
             leverage  total liabilities / total assets

Market cap and the split trap
-----------------------------
Nasdaq serves SPLIT-ADJUSTED closes; EDGAR reports share counts as-filed
(unadjusted). `adjusted_price * as_reported_shares` is wrong by the cumulative
split factor, which would make pre-2024 NVDA look 10x cheaper than it was.
We undo it with the split table: market cap is
    adj_price(signal) * as_reported_shares(period P) * f(P)
where f(P) is the product of split ratios dated AFTER the share count's own
period_end P — not after the signal date. Using the signal date would drop a
split that happened between P and the signal and mis-scale exactly those
names. Dollar amounts (net income, book equity) are split-invariant and need
no adjustment, so only the share count is corrected.

TTM
---
Trailing-twelve-month flows prefer the sum of the last four consecutive
quarterly facts (~90d each, non-overlapping, ~1 year total) and fall back to
the most recent annual fact. Which was used is stored per row
(`ttm_method`) and counted in the coverage report, so a reader can tell how
much of the panel is quarterly-precision versus annual-only.

Output
------
data/fundamentals.db -> table `fundamental_features` (date, symbol, ...),
plus data/fundamental_features.csv and data/fundamental_features_coverage.json.
(No parquet: neither pyarrow nor fastparquet is installed and a new
dependency was not worth it — the task allows a sqlite table.)

Usage
-----
    poetry run python scripts/build_fundamental_features.py
    poetry run python scripts/build_fundamental_features.py --start 2016-01-01
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from scripts.fetch_edgar_fundamentals import connect  # noqa: E402

CSV_PATH = ROOT / "data" / "fundamental_features.csv"
COVERAGE_PATH = ROOT / "data" / "fundamental_features_coverage.json"

QUARTER_DAYS = (75, 105)
ANNUAL_DAYS = (340, 400)
TTM_SPAN_DAYS = (300, 400)
QUARTER_GAP_DAYS = (55, 130)
# A filer that has not reported in ~2.5 years is no longer a going concern for
# this panel. Without this cutoff the as-filed replay happily carries a
# decade-old net income forward (observed max staleness: 16 years), which
# yields nonsense E/P and B/P for delisted names whose prices still exist.
MAX_STALE_DAYS = 900

# Preferred unit per tag so a multi-unit tag cannot mix dollars with shares.
TAG_UNIT = {
    "EarningsPerShareDiluted": "USD/shares",
    "EarningsPerShareBasic": "USD/shares",
    "CommonStockSharesOutstanding": "shares",
    "EntityCommonStockSharesOutstanding": "shares",
    "WeightedAverageNumberOfDilutedSharesOutstanding": "shares",
    "WeightedAverageNumberOfSharesOutstandingBasic": "shares",
}

SHARES_TAGS = ("EntityCommonStockSharesOutstanding", "CommonStockSharesOutstanding")
EQUITY_TAGS = ("StockholdersEquity",
               "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest")
REVENUE_TAGS = ("RevenueFromContractWithCustomerExcludingAssessedTax", "Revenues",
                "SalesRevenueNet", "RevenueFromContractWithCustomerIncludingAssessedTax")

FEATURES = [
    "ep_ttm", "bp", "sp_ttm",
    "roe", "gross_margin", "asset_turnover",
    "rev_yoy", "ni_yoy", "asset_growth",
    "accruals", "ocf_yield", "leverage",
]
# Sign of the expected cross-sectional relation to forward returns, used by
# the factor report to interpret IC direction (not to flip signs).
EXPECTED_SIGN = {
    "ep_ttm": +1, "bp": +1, "sp_ttm": +1,
    "roe": +1, "gross_margin": +1, "asset_turnover": +1,
    "rev_yoy": +1, "ni_yoy": +1,
    "asset_growth": -1, "accruals": -1,
    "ocf_yield": +1, "leverage": -1,
}


# ---------------------------------------------------------------------------
# As-of replay
# ---------------------------------------------------------------------------

def _days(a: str, b: str) -> int:
    return (date.fromisoformat(b) - date.fromisoformat(a)).days


def load_symbol_facts(conn: sqlite3.Connection, symbol: str) -> pd.DataFrame:
    """Every curated fact for one symbol, unit-filtered, filed-date ordered."""
    df = pd.read_sql_query(
        "SELECT tag, unit, period_start, period_end, value, filed_date, form"
        " FROM facts WHERE symbol = ? ORDER BY filed_date ASC, accn ASC",
        conn, params=(symbol,),
    )
    if df.empty:
        return df
    want = df["tag"].map(lambda t: TAG_UNIT.get(t, "USD"))
    return df[df["unit"] == want].reset_index(drop=True)


def replay_state(facts: pd.DataFrame, month_ends: list[str]) -> dict:
    """Event-driven as-filed replay -> state per month-end.

    Returns {(month_end): {tag: {(period_start, period_end): value}}}.
    Each filing is applied once, in filed-date order, and the FIRST filing of
    a period wins — the as-filed value, never a later restatement.
    """
    snaps: dict[str, dict] = {}
    state: dict[str, dict] = {}
    if facts.empty:
        return {m: {} for m in month_ends}
    groups = list(facts.groupby("filed_date", sort=True))
    gi = 0
    for m in month_ends:
        while gi < len(groups) and groups[gi][0] <= m:
            for row in groups[gi][1].itertuples(index=False):
                per_tag = state.setdefault(row.tag, {})
                key = (row.period_start, row.period_end)
                if key not in per_tag:
                    per_tag[key] = row.value
            gi += 1
        snaps[m] = {tag: dict(d) for tag, d in state.items()}
    return snaps


def latest_instant(per_tag: dict, tags: tuple[str, ...]) -> tuple[float, str, str] | None:
    """(value, tag, period_end) for the freshest instantaneous fact."""
    best = None
    for tag in tags:
        for (pstart, pend), val in per_tag.get(tag, {}).items():
            if pstart:            # duration fact, not a balance-sheet item
                continue
            if best is None or pend > best[2]:
                best = (val, tag, pend)
    return best


# Tags the panel actually reads — used to keep the staleness filter cheap.
NEEDED_TAGS = (SHARES_TAGS + EQUITY_TAGS + REVENUE_TAGS
               + ("Assets", "Liabilities", "CashAndCashEquivalentsAtCarryingValue",
                  "NetIncomeLoss", "GrossProfit",
                  "NetCashProvidedByUsedInOperatingActivities"))


def fresh_state(per_tag: dict, month_end: str,
                max_age_days: int = MAX_STALE_DAYS) -> dict:
    """Drop facts whose period ended more than `max_age_days` before the signal.

    Applied to every input (share count, balance sheet and flows) so a
    delisted filer's ancient numbers cannot masquerade as current ones.
    """
    out: dict = {}
    for tag in NEEDED_TAGS:
        obs = per_tag.get(tag)
        if not obs:
            continue
        kept = {k: v for k, v in obs.items() if _days(k[1], month_end) <= max_age_days}
        if kept:
            out[tag] = kept
    return out


def ttm(per_tag: dict, tag: str) -> tuple[float | None, str, str | None]:
    """(value, method, latest_period_end) for a trailing-twelve-month flow."""
    quarters, annuals = [], []
    for (pstart, pend), val in per_tag.get(tag, {}).items():
        if not pstart:
            continue
        d = _days(pstart, pend)
        if QUARTER_DAYS[0] <= d <= QUARTER_DAYS[1]:
            quarters.append((pstart, pend, val))
        elif ANNUAL_DAYS[0] <= d <= ANNUAL_DAYS[1]:
            annuals.append((pend, val))
    quarters.sort(key=lambda x: x[1])

    if len(quarters) >= 4:
        last = quarters[-4:]
        ok = True
        for i in range(1, 4):
            gap = _days(last[i - 1][1], last[i][1])
            if not (QUARTER_GAP_DAYS[0] <= gap <= QUARTER_GAP_DAYS[1]):
                ok = False
                break
            if last[i][0] < last[i - 1][1]:   # overlapping windows
                ok = False
                break
        span = _days(last[0][0], last[3][1])
        if ok and TTM_SPAN_DAYS[0] <= span <= TTM_SPAN_DAYS[1]:
            return float(sum(x[2] for x in last)), "ttm_quarters", last[3][1]

    if annuals:
        annuals.sort()
        return float(annuals[-1][1]), "annual", annuals[-1][0]
    return None, "none", None


def first_ttm(per_tag: dict, tags: tuple[str, ...]) -> tuple[float | None, str, str | None, str | None]:
    """First tag in `tags` that yields a TTM value (tag fallback chain)."""
    for tag in tags:
        val, method, pend = ttm(per_tag, tag)
        if val is not None:
            return val, method, pend, tag
    return None, "none", None, None


# ---------------------------------------------------------------------------
# Panel
# ---------------------------------------------------------------------------

def build_raw_panel(symbols: list[str], month_ends: list[str],
                    conn: sqlite3.Connection) -> pd.DataFrame:
    """symbol x month-end panel of raw PIT quantities + market cap."""
    from scripts.fetch_edgar_fundamentals import get_splits, split_factor_as_of

    rows: list[dict] = []
    n_stale = 0
    for n, sym in enumerate(symbols, 1):
        facts = load_symbol_facts(conn, sym)
        snaps = replay_state(facts, month_ends)
        splits = get_splits(sym, conn)
        for m in month_ends:
            raw = snaps.get(m) or {}
            if not raw:
                continue
            per_tag = fresh_state(raw, m)
            if not per_tag:
                n_stale += 1
                continue
            rec: dict = {"date": m, "symbol": sym}

            sh = latest_instant(per_tag, SHARES_TAGS)
            if sh:
                rec["shares"], rec["shares_tag"], shares_pend = sh
                # factor measured from the share count's OWN period_end, so a
                # split between that date and the signal is still applied
                rec["split_factor"] = split_factor_as_of(splits, shares_pend)
                rec["shares_age_days"] = _days(shares_pend, m)

            eq = latest_instant(per_tag, EQUITY_TAGS)
            if eq:
                rec["equity"], rec["equity_tag"], rec["equity_pend"] = eq

            asst = latest_instant(per_tag, ("Assets",))
            if asst:
                rec["assets"], _, rec["assets_pend"] = asst
            liab = latest_instant(per_tag, ("Liabilities",))
            if liab:
                rec["liabilities"] = liab[0]
            cash = latest_instant(per_tag, ("CashAndCashEquivalentsAtCarryingValue",))
            if cash:
                rec["cash"] = cash[0]

            ni, ni_m, ni_pend, ni_tag = first_ttm(per_tag, ("NetIncomeLoss",))
            rec["ttm_net_income"], rec["ni_method"] = ni, ni_m
            if ni_pend:
                rec["flow_age_days"] = _days(ni_pend, m)
            rev, rev_m, _, rev_tag = first_ttm(per_tag, REVENUE_TAGS)
            rec["ttm_revenue"] = rev
            rec["revenue_tag"] = rev_tag
            gp, _, _, _ = first_ttm(per_tag, ("GrossProfit",))
            rec["ttm_gross_profit"] = gp
            ocf, _, _, _ = first_ttm(
                per_tag, ("NetCashProvidedByUsedInOperatingActivities",))
            rec["ttm_ocf"] = ocf
            rows.append(rec)
        if n % 100 == 0:
            print(f"    panel {n}/{len(symbols)} symbols", flush=True)
    print(f"    staleness guard (>{MAX_STALE_DAYS}d): dropped {n_stale} "
          f"symbol-months entirely", flush=True)
    return pd.DataFrame(rows)


def attach_prices(panel: pd.DataFrame, closes: pd.DataFrame,
                  month_ends: list[str]) -> pd.DataFrame:
    """Add split-adjusted close and the forward 1-month return.

    Forward return mirrors the repo's execution convention
    (execution_lag_bars=1): enter at the close of the trading day AFTER the
    signal and exit at the same point one month later.
    """
    tz = closes.index.tz

    def ts(d: str) -> pd.Timestamp:
        t = pd.Timestamp(d)
        return t.tz_localize(tz) if tz is not None else t

    exec_pos = {d: closes.index.searchsorted(ts(d)) + 1 for d in month_ends}
    close_at, fwd_at = {}, {}
    for i, d in enumerate(month_ends):
        e = exec_pos[d]
        if e >= len(closes.index):
            continue
        entry = closes.iloc[e]
        close_at[d] = entry
        if i + 1 < len(month_ends):
            x = exec_pos[month_ends[i + 1]]
            if x < len(closes.index):
                fwd_at[d] = closes.iloc[x] / entry - 1.0

    panel["close"] = [float(close_at[d].get(s, np.nan)) if d in close_at else np.nan
                      for d, s in zip(panel["date"], panel["symbol"])]
    panel["fwd_ret_1m"] = [float(fwd_at[d].get(s, np.nan)) if d in fwd_at else np.nan
                           for d, s in zip(panel["date"], panel["symbol"])]
    return panel


def derive_features(panel: pd.DataFrame) -> pd.DataFrame:
    """Add ratios and 12-month growth terms on the monthly grid."""
    df = panel.sort_values(["symbol", "date"]).copy()
    for col in ("close", "shares", "split_factor", "ttm_net_income", "ttm_revenue",
                "ttm_gross_profit", "ttm_ocf", "equity", "assets", "liabilities"):
        if col not in df:
            df[col] = np.nan
        df[col] = pd.to_numeric(df[col], errors="coerce")

    mc = df["close"] * df["shares"] * df["split_factor"]
    df["market_cap"] = mc.where(mc > 0)

    df["ep_ttm"] = df["ttm_net_income"] / df["market_cap"]
    df["bp"] = df["equity"] / df["market_cap"]
    df["sp_ttm"] = df["ttm_revenue"] / df["market_cap"]
    df["ocf_yield"] = df["ttm_ocf"] / df["market_cap"]

    df["roe"] = df["ttm_net_income"] / df["equity"].where(df["equity"] > 0)
    df["gross_margin"] = df["ttm_gross_profit"] / df["ttm_revenue"].where(
        df["ttm_revenue"] != 0)
    df["asset_turnover"] = df["ttm_revenue"] / df["assets"].where(df["assets"] > 0)
    df["leverage"] = df["liabilities"] / df["assets"].where(df["assets"] > 0)
    df["accruals"] = ((df["ttm_net_income"] - df["ttm_ocf"])
                      / df["assets"].where(df["assets"] > 0))

    # 12-month growth. shift(12) is only one year if the monthly grid is
    # complete per symbol, so reindex onto the full month-end x symbol grid
    # first — otherwise a symbol with a gap would be compared to the wrong
    # month (or to a different fiscal year across a missing row).
    full = pd.MultiIndex.from_product(
        [sorted(df["date"].unique()), sorted(df["symbol"].unique())],
        names=["date", "symbol"])
    idx = df.set_index(["date", "symbol"]).index
    grid = df.set_index(["date", "symbol"]).reindex(full).sort_index()
    for col, src in (("rev_yoy", "ttm_revenue"), ("ni_yoy", "ttm_net_income"),
                     ("asset_growth", "assets")):
        prev = grid.groupby("symbol", sort=False)[src].shift(12)
        df[col] = (grid[src] / prev.where(prev > 0) - 1.0).reindex(idx).values
    return df


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def _pctl(s: pd.Series) -> dict:
    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
        return {"n": 0}
    return {
        "n": int(len(s)),
        "median": float(s.median()),
        "p90": float(s.quantile(0.90)),
        "p95": float(s.quantile(0.95)),
        "p99": float(s.quantile(0.99)),
        "max": float(s.max()),
        "over_120d_pct": round(float((s > 120).mean()) * 100, 2),
        "over_365d_pct": round(float((s > 365).mean()) * 100, 2),
    }


def coverage_report(df: pd.DataFrame, month_ends: list[str]) -> dict:
    """Per-feature and per-year availability — early years are thin, quantify it."""
    per_feature: dict[str, dict] = {}
    for f in FEATURES:
        if f not in df:
            continue
        s = pd.to_numeric(df[f], errors="coerce").replace([np.inf, -np.inf], np.nan)
        per_feature[f] = {
            "non_null_rows": int(s.notna().sum()),
            "coverage_pct": round(float(s.notna().mean()) * 100, 1),
            "expected_sign": EXPECTED_SIGN.get(f),
            "median": round(float(s.median()), 4) if s.notna().any() else None,
        }
    by_year: dict[str, dict] = {}
    d = df.copy()
    d["year"] = d["date"].str[:4]
    for year, chunk in d.groupby("year"):
        by_year[year] = {
            "months": int(chunk["date"].nunique()),
            "mean_symbols_per_month": int(round(
                chunk.groupby("date")["symbol"].nunique().mean())),
            "ep_ttm_coverage_pct": round(
                float(pd.to_numeric(chunk["ep_ttm"], errors="coerce").notna().mean()) * 100, 1),
            "bp_coverage_pct": round(
                float(pd.to_numeric(chunk["bp"], errors="coerce").notna().mean()) * 100, 1),
        }
    return {
        "rows": int(len(df)),
        "months": len(month_ends),
        "symbols": int(df["symbol"].nunique()),
        "ttm_method": ({k: int(v) for k, v in df["ni_method"].value_counts().items()}
                       if "ni_method" in df else {}),
        # How stale is the newest filing behind each month's numbers? Market cap
        # is price x shares, so a stale share count would bias E/P and B/P.
        # Rows beyond MAX_STALE_DAYS are already dropped by fresh_state().
        "input_staleness_days": {
            "shares_age": _pctl(df["shares_age_days"]) if "shares_age_days" in df else {"n": 0},
            "flow_age": _pctl(df["flow_age_days"]) if "flow_age_days" in df else {"n": 0},
            "max_allowed_days": MAX_STALE_DAYS,
            "note": ("每个特征值所依据的最近一期 period_end 距信号日的天数；"
                     "超过 MAX_STALE_DAYS 的输入已在 fresh_state() 丢弃（否则已停报公司"
                     "十年前的财报会被一路带到 2026 年）。flow_age 中位数偏大是正常的："
                     "TTM 在拿不到四个季度时会退到最近年报，年中年报天然滞后数月"),
        },
        "per_feature": per_feature,
        "by_year": dict(sorted(by_year.items())),
    }


def write_table(df: pd.DataFrame, conn: sqlite3.Connection) -> None:
    cols = ["date", "symbol", "market_cap", "close", "shares", "split_factor",
            "shares_age_days", "flow_age_days",
            "ttm_net_income", "ttm_revenue", "ttm_gross_profit", "ttm_ocf",
            "equity", "assets", "liabilities", "cash", "fwd_ret_1m",
            "ni_method"] + FEATURES
    out = df[[c for c in cols if c in df]].copy()
    for c in out.columns:
        if c not in ("date", "symbol", "ni_method"):
            out[c] = pd.to_numeric(out[c], errors="coerce").replace(
                [np.inf, -np.inf], np.nan)
    conn.execute("DROP TABLE IF EXISTS fundamental_features")
    out.to_sql("fundamental_features", conn, index=False)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS ix_ff_date ON fundamental_features(date)")
    conn.commit()


def main() -> None:
    ap = argparse.ArgumentParser(description="Build monthly fundamental features")
    ap.add_argument("--start", default="2016-01-01")
    ap.add_argument("--end", default=None)
    ap.add_argument("--universe", choices=["sp100", "sp500"], default="sp500")
    args = ap.parse_args()

    from datetime import datetime, timezone

    from scripts.fetch_edgar_fundamentals import select_symbols
    from scripts.xsec_gbm_selection import month_end_signal_days
    from src.data.nasdaq_store import NasdaqDailyStore

    conn = connect()
    end = args.end or datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

    # symbols carrying EDGAR facts (the ingester already restricted to names
    # with prices + a CIK mapping, so this is the same universe)
    fact_syms = {r[0] for r in conn.execute(
        "SELECT DISTINCT symbol FROM facts").fetchall()}
    selected, meta = select_symbols(args.universe)
    symbols = sorted(set(selected) & fact_syms)
    print(f"  universe={args.universe}: {len(selected)} selected, "
          f"{len(fact_syms)} with facts, {len(symbols)} usable")

    store = NasdaqDailyStore(assetclass="stocks")
    closes = {}
    for sym in symbols:
        d = store.get_daily(sym, args.start, end)
        if not d.empty:
            closes[sym] = d["close"]
    closes = pd.DataFrame(closes).sort_index()
    print(f"  price panel: {closes.shape[0]} days x {closes.shape[1]} symbols "
          f"({str(closes.index[0].date())} ~ {str(closes.index[-1].date())})")

    month_ends = [str(t.date()) for t in
                  month_end_signal_days(closes.index, closes.index[0], end)]
    print(f"  month-end signal dates: {len(month_ends)} "
          f"({month_ends[0]} ~ {month_ends[-1]})")

    print("  replaying as-filed facts ...")
    panel = build_raw_panel(sorted(closes.columns), month_ends, conn)
    print(f"  raw panel: {panel.shape[0]} rows, {panel['symbol'].nunique()} symbols")

    panel = attach_prices(panel, closes, month_ends)
    feats = derive_features(panel)

    cov = coverage_report(feats, month_ends)
    cov["universe_meta"] = meta
    cov["note"] = ("coverage is computed over (month-end, symbol) rows that have "
                   "EDGAR facts; early years are thinner because fewer XBRL "
                   "facts had been filed yet")
    COVERAGE_PATH.write_text(json.dumps(cov, indent=2))
    write_table(feats, conn)
    keep = ["date", "symbol", "market_cap", "fwd_ret_1m"] + FEATURES + ["ni_method"]
    feats[[c for c in keep if c in feats]].to_csv(CSV_PATH, index=False)

    print(f"\n  features -> fundamental_features table + {CSV_PATH.name}")
    print(f"  rows={cov['rows']}  months={cov['months']}  symbols={cov['symbols']}")
    print(f"  ttm method: {cov['ttm_method']}")
    print(f"\n  {'feature':<16}{'coverage%':>10}{'median':>12}")
    for f, st in cov["per_feature"].items():
        print(f"    {f:<14}{st['coverage_pct']:>10.1f}{str(st['median']):>12}")
    print("\n  by year: ")
    for y, st in cov["by_year"].items():
        print(f"    {y}  months={st['months']:<3} "
              f"symbols/mo={st['mean_symbols_per_month']:<4} "
              f"ep={st['ep_ttm_coverage_pct']:>5.1f}%  bp={st['bp_coverage_pct']:>5.1f}%")
    conn.close()


if __name__ == "__main__":
    main()
