#!/usr/bin/env python3
"""SEC EDGAR XBRL fundamentals ingester — point-in-time (as-filed) correct.

Why this exists
---------------
The cross-sectional stock-selection models in this repo use only price/volume
features. The most robust academic factors (value, quality, investment) live
in financial statements. EDGAR is free, complete (all US filers, including
delisted names) and needs no API key, so it is the natural source.

Data source
-----------
1. `https://www.sec.gov/files/company_tickers.json`  ticker -> CIK (no key)
2. `https://data.sec.gov/api/xbrl/companyfacts/CIK##########.json`
   one JSON per company holding every XBRL fact it ever filed, each tagged
   with the `filed` date of the submission that carried it.

SEC fair-access rules: send a User-Agent identifying you (set the
`SEC_EDGAR_UA` env var to e.g. `my-name research me@example.com`) and stay
at or below 10 requests/second. We default to 5/s.

Why `filed_date` is the whole point
-----------------------------------
`companyfacts` is NOT a time series of "what was true then" — it is the
LATEST view: a value restated in a 2024 filing appears under its original
2021 period. Using it naively leaks the future. Every row here therefore
carries the `filed_date` of the submission it came from, and every
point-in-time read goes through `as_of_facts()`, which keeps only rows with
`filed_date <= as_of_date` and — when the same period was reported more
than once — takes the EARLIEST filing (the value actually visible at the
time, i.e. as-filed, not the later restatement).

Schema (data/fundamentals.db)
-----------------------------
facts(
  cik, symbol, taxonomy, tag, unit, value,
  period_start,      -- '' for instantaneous (balance-sheet) facts
  period_end,
  filed_date,        -- PIT anchor: the date this number became public
  form, accn, fy, fp
)
  PRIMARY KEY(symbol, tag, unit, period_start, period_end, filed_date, value)
  -- `value` is in the key on purpose: a restatement is a *different*
  -- observation for the same period, not an overwrite.
  INDEX(symbol, tag, period_end)   -- as-of scans by period
  INDEX(symbol, filed_date)        -- as-of scans by availability

splits(symbol, ex_date, ratio)     -- detected from share-count steps

Splits: why we detect them ourselves
------------------------------------
The Nasdaq price store serves SPLIT-ADJUSTED closes (verified: AAPL trades
~$124.81 on 2020-08-28, i.e. the real ~$499.23 / 4). EDGAR share counts are
as-reported (unadjusted). `adjusted_price x as_reported_shares` understates
market cap by the cumulative split factor, which would make every pre-split
NVDA look 10x cheaper than it was. Yahoo is rate-limited (429) from this
network, so we recover the split factor from EDGAR itself: an as-reported
share count that steps by a clean ratio (4x, 10x, ...) AND persists is a
split. Validated on AAPL 4:1, NVDA 4:1 & 10:1, TSLA 5:1 & 3:1, GOOGL 20:1,
AMZN 20:1.

Usage
-----
    poetry run python scripts/fetch_edgar_fundamentals.py --limit 5   # smoke
    poetry run python scripts/fetch_edgar_fundamentals.py             # full
    poetry run python scripts/fetch_edgar_fundamentals.py --reparse   # offline
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import sqlite3
import statistics
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

DB_PATH = ROOT / "data" / "fundamentals.db"
RAW_DIR = ROOT / "data" / "edgar_raw"
STATUS_PATH = ROOT / "data" / "edgar_fetch_status.json"

TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json"

# SEC requires an identifying User-Agent and 403s anything it dislikes
# (verified: parenthesised placeholders are rejected). Override with
# SEC_EDGAR_UA, e.g. "your-name research you@yourdomain.com".
DEFAULT_UA = "ai-hedge-fund research research@example.com"
TICKERS_MAX_AGE_DAYS = 7

# Curated fact tags — enough to build value / quality / growth / investment
# factors without dragging ~500 tags per company into the DB.
TAGS = frozenset({
    # --- flows (have a period_start) -------------------------------------
    "Revenues",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
    "SalesRevenueNet",
    "CostOfRevenue",
    "CostOfGoodsAndServicesSold",
    "GrossProfit",
    "OperatingIncomeLoss",
    "NetIncomeLoss",
    "NetIncomeLossAvailableToCommonStockholdersBasic",
    "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
    "IncomeTaxExpenseBenefit",
    "InterestExpense",
    "ResearchAndDevelopmentExpense",
    "SellingGeneralAndAdministrativeExpense",
    "DepreciationDepletionAndAmortization",
    "ShareBasedCompensation",
    "NetCashProvidedByUsedInOperatingActivities",
    "NetCashProvidedByUsedInInvestingActivities",
    "NetCashProvidedByUsedInFinancingActivities",
    "EarningsPerShareDiluted",
    "EarningsPerShareBasic",
    "WeightedAverageNumberOfDilutedSharesOutstanding",
    "WeightedAverageNumberOfSharesOutstandingBasic",
    # --- stocks (instantaneous, period_start == '') ----------------------
    "Assets",
    "AssetsCurrent",
    "Liabilities",
    "LiabilitiesCurrent",
    "StockholdersEquity",
    "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    "CashAndCashEquivalentsAtCarryingValue",
    "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
    "LongTermDebt",
    "LongTermDebtNoncurrent",
    "LongTermDebtCurrent",
    "Goodwill",
    "InventoryNet",
    "AccountsReceivableNetCurrent",
    "PropertyPlantAndEquipmentNet",
    "RetainedEarningsAccumulatedDeficit",
    "CommonStockSharesOutstanding",
    # --- dei -------------------------------------------------------------
    "EntityCommonStockSharesOutstanding",
    "EntityPublicFloat",
})

SHARE_TAGS = frozenset({"CommonStockSharesOutstanding", "EntityCommonStockSharesOutstanding"})
# Any share count, including the weighted-average flows: a split rescales all
# of them retroactively, so all of them carry corroborating evidence.
ALL_SHARE_TAGS = SHARE_TAGS | {
    "WeightedAverageNumberOfDilutedSharesOutstanding",
    "WeightedAverageNumberOfSharesOutstandingBasic",
}

# Clean ratios a real split/combination lands on (r:1 for forward splits).
_SIMPLE_RATIOS = (1.5, 2.0, 2.5, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 10.0,
                  12.0, 15.0, 20.0, 25.0, 30.0, 40.0, 50.0, 100.0)
_SIMPLE_RATIOS = _SIMPLE_RATIOS + tuple(1.0 / r for r in _SIMPLE_RATIOS)
_RATIO_TOL = 0.03        # snap tolerance to a clean ratio (real splits land <1%)
_PERSIST_TOL = 0.25      # the new level must hold vs the next observations

DDL = """
CREATE TABLE IF NOT EXISTS facts (
    cik           INTEGER NOT NULL,
    symbol        TEXT    NOT NULL,
    taxonomy      TEXT    NOT NULL,
    tag           TEXT    NOT NULL,
    unit          TEXT    NOT NULL,
    value         REAL    NOT NULL,
    period_start  TEXT    NOT NULL,   -- '' for instantaneous facts
    period_end    TEXT    NOT NULL,
    filed_date    TEXT    NOT NULL,   -- PIT anchor: when this became public
    form          TEXT,
    accn          TEXT,
    fy            INTEGER,
    fp            TEXT,
    PRIMARY KEY (symbol, tag, unit, period_start, period_end, filed_date, value)
);
-- restatements add rows (value is in the key) rather than overwriting,
-- so as_of_facts() can still see the original as-filed number
CREATE INDEX IF NOT EXISTS ix_facts_sym_tag_period ON facts(symbol, tag, period_end);
CREATE INDEX IF NOT EXISTS ix_facts_sym_filed      ON facts(symbol, filed_date);

CREATE TABLE IF NOT EXISTS splits (
    symbol   TEXT NOT NULL,
    ex_date  TEXT NOT NULL,   -- period_end of the first post-split share count
    ratio    REAL NOT NULL,   -- 4.0 == 4-for-1 forward split
    PRIMARY KEY (symbol, ex_date)
);
"""


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def user_agent() -> str:
    return os.environ.get("SEC_EDGAR_UA", DEFAULT_UA)


def http_get(url: str, timeout: float = 30.0, retries: int = 3) -> bytes:
    """GET with retries + exponential backoff. Raises on final failure."""
    last: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": user_agent(),
                    "Accept": "application/json",
                    "Accept-Encoding": "gzip, deflate",
                },
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                if resp.headers.get("Content-Encoding") == "gzip":
                    raw = gzip.decompress(raw)
                return raw
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                raise  # no XBRL for this CIK — not retryable
            last = exc
        except Exception as exc:  # noqa: BLE001 — network errors vary
            last = exc
        time.sleep(1.5 * (2 ** attempt))
    raise RuntimeError(f"GET failed after {retries} attempts: {url} :: {last}")


def load_ticker_cik_map(refresh: bool = False) -> dict[str, tuple[int, str]]:
    """ticker(upper) -> (cik, title). Cached under data/edgar_raw/."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    cache = RAW_DIR / "company_tickers.json"
    stale = (
        not cache.exists()
        or (time.time() - cache.stat().st_mtime) > TICKERS_MAX_AGE_DAYS * 86400
    )
    if refresh or stale:
        cache.write_bytes(http_get(TICKERS_URL))
    payload = json.loads(cache.read_text())
    return {
        str(v["ticker"]).upper(): (int(v["cik_str"]), str(v.get("title", "")))
        for v in payload.values()
    }


def cik_for(symbol: str, tmap: dict[str, tuple[int, str]]) -> int | None:
    """CIK lookup with the SEC's dot/dash convention (BRK.B -> BRK-B)."""
    for cand in (symbol.upper(), symbol.upper().replace(".", "-")):
        if cand in tmap:
            return tmap[cand][0]
    return None


# ---------------------------------------------------------------------------
# Parse
# ---------------------------------------------------------------------------

def parse_facts(symbol: str, cik: int, payload: dict) -> list[tuple]:
    """Flatten companyfacts JSON into `facts` rows (curated tags only)."""
    rows: list[tuple] = []
    for taxonomy, tmap in (payload.get("facts") or {}).items():
        for tag, node in tmap.items():
            if tag not in TAGS:
                continue
            for unit, recs in (node.get("units") or {}).items():
                for r in recs:
                    val = r.get("val")
                    filed = r.get("filed")
                    end = r.get("end")
                    if val is None or not filed or not end:
                        continue  # without `filed` the row has no PIT anchor
                    rows.append((
                        cik, symbol, taxonomy, tag, unit, float(val),
                        r.get("start") or "", end, filed,
                        r.get("form"), r.get("accn"), r.get("fy"), r.get("fp"),
                    ))
    return rows


# ---------------------------------------------------------------------------
# Split detection (EDGAR-only; see module docstring)
# ---------------------------------------------------------------------------

def _snap_ratio(k: float) -> float | None:
    if k <= 0:
        return None
    best = min(_SIMPLE_RATIOS, key=lambda r: abs(k - r) / r)
    return best if abs(k - best) / best <= _RATIO_TOL else None


def _median(xs: list[float]) -> float | None:
    xs = [x for x in xs if x and x > 0]
    return statistics.median(xs) if xs else None


def restated_share_ratios(rows: list[tuple]) -> set[float]:
    """Clean ratios by which a previously-filed share count was later revised.

    A genuine split retroactively restates the share counts of every
    pre-split period (ASC 260 requires it), so the SAME (tag, period) shows
    a scaled value in a later filing. Paying for an acquisition with stock
    does NOT restate history — which is what lets this test tell a 3:2 split
    from a 1.5x merger issuance (VZ/Vodafone 2014, MAR/Starwood 2016,
    TMUS/Sprint 2020 all leave no trace here; ODFL's 2024 2:1 and AAPL's
    7:1 and 4:1 leave exact ones).
    """
    groups: dict[tuple, list[tuple[str, float]]] = {}
    for r in rows:
        tag, unit, value, pstart, pend, filed = r[3], r[4], r[5], r[6], r[7], r[8]
        if tag not in ALL_SHARE_TAGS or unit != "shares":
            continue
        groups.setdefault((tag, pstart, pend), []).append((filed, value))

    ratios: set[float] = set()
    for obs in groups.values():
        if len(obs) < 2:
            continue
        obs.sort()
        base = obs[0][1]
        if base <= 0:
            continue
        for _filed, val in obs[1:]:
            ratio = _snap_ratio(val / base)
            if ratio is not None and abs(ratio - 1.0) > 1e-9:
                ratios.add(ratio)
    return ratios


def as_filed_share_levels(rows: list[tuple]) -> list[tuple[str, float]]:
    """(period_end, shares) level series, earliest filing per date.

    Uses the as-filed (earliest) value for each period so the level series
    is what an investor would have seen, then drops isolated spikes whose
    neighbours agree with each other (mis-scaled XBRL facts) while keeping
    genuine steps.
    """
    best: dict[tuple, tuple[str, float]] = {}
    for r in rows:
        _cik, _sym, _tax, tag, unit, value, pstart, pend, filed, *_ = r
        if tag not in SHARE_TAGS or unit != "shares" or pstart != "":
            continue
        key = (tag, pend)
        cur = best.get(key)
        if cur is None or filed < cur[0]:
            best[key] = (filed, value)
    levels = sorted((pend, v) for (_tag, pend), (_filed, v) in best.items())

    # de-spike: neighbours before/after agree with each other but not with us
    cleaned: list[tuple[str, float]] = []
    for i, (d, v) in enumerate(levels):
        prev = _median([x[1] for x in levels[max(0, i - 3):i]])
        nxt = _median([x[1] for x in levels[i + 1:i + 4]])
        if prev and nxt and 0.8 <= nxt / prev <= 1.25:
            if not 0.7 <= v / prev <= 1.4:
                continue  # isolated spike → bad fact, drop
        cleaned.append((d, v))
    return cleaned


def detect_splits(levels: list[tuple[str, float]],
                  corroborated: set[float] | None = None) -> list[tuple[str, float]]:
    """(ex_date, ratio) for each clean-ratio step in the share-count series.

    Compares each observation with its immediate predecessor — a split shows
    up as exactly one such step — then skips past the whole new plateau so
    the same split is not re-reported by the following quarters.

    `corroborated` is the set of ratios that also show up as a same-period
    restatement (see `restated_share_ratios`). A step is only accepted if
    its ratio is in that set, which rejects share issuance paid for with
    stock: those step the share count by a clean-looking ratio too, but
    never restate past periods. Pass None to skip the check.
    """
    out: list[tuple[str, float]] = []
    i = 1
    while i < len(levels):
        prev, cur = levels[i - 1][1], levels[i][1]
        ratio = _snap_ratio(cur / prev) if prev > 0 else None
        if ratio is None or abs(ratio - 1.0) < 1e-9:
            i += 1
            continue
        if corroborated is not None and ratio not in corroborated:
            i += 1  # steeper/cleaner than issuance usually is, but unproven
            continue
        nxt = levels[i + 1][1] if i + 1 < len(levels) else None
        if nxt is not None and abs(nxt / cur - 1.0) > _PERSIST_TOL:
            i += 1  # level did not hold → issuance/error, not a split
            continue
        out.append((levels[i][0], ratio))
        j = i + 1
        while j < len(levels) and abs(levels[j][1] / cur - 1.0) <= _PERSIST_TOL:
            j += 1
        i = j
    return out


def detect_symbol_splits(rows: list[tuple]) -> list[tuple[str, float]]:
    """Splits for one symbol: level steps corroborated by a restatement."""
    return detect_splits(as_filed_share_levels(rows), restated_share_ratios(rows))


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

def connect(db_path: Path | str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)
    conn.commit()


def upsert_facts(conn: sqlite3.Connection, rows: list[tuple], batch: int = 5000) -> int:
    sql = ("INSERT OR IGNORE INTO facts (cik, symbol, taxonomy, tag, unit, value,"
           " period_start, period_end, filed_date, form, accn, fy, fp)"
           " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)")
    n = 0
    for i in range(0, len(rows), batch):
        cur = conn.executemany(sql, rows[i:i + batch])
        n += cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
    conn.commit()
    return n


def replace_splits(conn: sqlite3.Connection, symbol: str, splits: list[tuple[str, float]]) -> None:
    conn.execute("DELETE FROM splits WHERE symbol = ?", (symbol,))
    if splits:
        conn.executemany(
            "INSERT OR REPLACE INTO splits (symbol, ex_date, ratio) VALUES (?,?,?)",
            [(symbol, d, r) for d, r in splits],
        )
    conn.commit()


def get_splits(symbol: str, conn: sqlite3.Connection | None = None) -> list[tuple[str, float]]:
    own = conn is None
    conn = conn or connect()
    try:
        cur = conn.execute(
            "SELECT ex_date, ratio FROM splits WHERE symbol = ? ORDER BY ex_date", (symbol,)
        )
        return [(r[0], float(r[1])) for r in cur.fetchall()]
    finally:
        if own:
            conn.close()


def split_factor_as_of(splits: list[tuple[str, float]], as_of_date: str) -> float:
    """Cumulative future split factor f: product of ratios AFTER `as_of_date`.

    Market cap on the price basis is `adj_price x as_reported_shares x f`.
    """
    f = 1.0
    for ex_date, ratio in splits:
        if ex_date > as_of_date:
            f *= ratio
    return f


def as_of_facts(
    symbol: str,
    tag: str,
    as_of_date: str,
    conn: sqlite3.Connection | None = None,
    unit: str | None = None,
    prefer_latest: bool = False,
    db_path: Path | str = DB_PATH,
):
    """Point-in-time facts for (symbol, tag) visible at `as_of_date`.

    Returns a DataFrame with one row per (period_start, period_end, unit),
    chosen from the filings published on or before `as_of_date` — so a value
    restated in 2024 can never be served to an as_of in 2021.

    Which of the visible filings wins is a real modelling choice:

    - `prefer_latest=False` (default, "as-filed"): the EARLIEST publication
      of each period. This is what the period first said; a later
      restatement is ignored. Most conservative — a number is only ever
      read the way it was originally released.
    - `prefer_latest=True` ("latest-visible"): the most recently published
      statement of that period, i.e. what a live reader would have used on
      `as_of_date` after learning about a restatement.

    Both are look-ahead free; they differ only for restated periods
    (a small minority). The factor builder uses the default.
    """
    import pandas as pd

    own = conn is None
    conn = conn or connect(db_path)
    try:
        order = "DESC" if prefer_latest else "ASC"
        sql = f"""
        WITH ranked AS (
            SELECT period_start, period_end, unit, value, filed_date, form, taxonomy,
                   ROW_NUMBER() OVER (
                       PARTITION BY period_start, period_end, unit
                       ORDER BY filed_date {order}, accn {order}
                   ) AS rn
            FROM facts
            WHERE symbol = ? AND tag = ? AND filed_date <= ?
                  AND (? IS NULL OR unit = ?)
        )
        SELECT period_start, period_end, unit, value, filed_date, form, taxonomy
        FROM ranked WHERE rn = 1 ORDER BY period_end ASC
        """
        df = pd.read_sql_query(sql, conn, params=(symbol, tag, as_of_date, unit, unit))
    finally:
        if own:
            conn.close()
    return df


# ---------------------------------------------------------------------------
# Status file (resume support)
# ---------------------------------------------------------------------------

def load_status() -> dict:
    if STATUS_PATH.exists():
        return json.loads(STATUS_PATH.read_text())
    return {}


def save_status(status: dict) -> None:
    STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATUS_PATH.write_text(json.dumps(status, indent=2, sort_keys=True))


# ---------------------------------------------------------------------------
# Symbol selection
# ---------------------------------------------------------------------------

def select_symbols(universe: str) -> tuple[list[str], dict]:
    """PIT-universe symbols that (a) have local prices and (b) map to a CIK.

    Price data is required because market-cap factors need a price; names
    without it are recorded but not fetched.
    """
    from scripts.xsec_gbm_selection import load_pit_universe
    from src.data.nasdaq_store import NasdaqDailyStore

    _pit, union = load_pit_universe(universe)
    tmap = load_ticker_cik_map()
    store = NasdaqDailyStore(assetclass="stocks")

    priced, no_price, no_cik, ok = [], [], [], []
    for sym in union:
        if store.get_coverage(sym)[2] > 0:
            priced.append(sym)
        else:
            no_price.append(sym)
        if cik_for(sym, tmap) is None:
            no_cik.append(sym)
        else:
            ok.append(sym)
    selected = sorted(set(priced) & set(ok))
    meta = {
        "universe": universe,
        "pit_union_size": len(union),
        "with_price": len(priced),
        "with_cik": len(ok),
        "selected": len(selected),
        "no_price_data": sorted(no_price),
        "no_cik_mapping": sorted(no_cik),
    }
    return selected, meta


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

def fetch_one(symbol: str, cik: int, conn: sqlite3.Connection,
              refresh: bool = False) -> dict:
    raw_path = RAW_DIR / f"CIK{cik:010d}.json.gz"
    if refresh or not raw_path.exists():
        raw = http_get(FACTS_URL.format(cik=cik))
        raw_path.write_bytes(gzip.compress(raw))
    payload = json.loads(gzip.decompress(raw_path.read_bytes()))

    rows = parse_facts(symbol, cik, payload)
    if not rows:
        return {"status": "no_facts", "cik": cik, "n_facts": 0, "n_splits": 0}
    upsert_facts(conn, rows)
    splits = detect_symbol_splits(rows)
    replace_splits(conn, symbol, splits)
    return {"status": "ok", "cik": cik, "n_facts": len(rows),
            "n_splits": len(splits), "bytes": raw_path.stat().st_size}


def reparse_all(conn: sqlite3.Connection, status: dict) -> None:
    """Rebuild `facts` from the cached raw JSON without any network call."""
    by_cik = {v["cik"]: v["symbol"] for v in status.values()
              if v.get("cik") and v.get("symbol")}
    for raw_path in sorted(RAW_DIR.glob("CIK*.json.gz")):
        cik = int(raw_path.name[3:].split(".")[0])
        sym = by_cik.get(cik)
        if not sym:
            continue
        rows = parse_facts(sym, cik, json.loads(gzip.decompress(raw_path.read_bytes())))
        if rows:
            upsert_facts(conn, rows)
            replace_splits(conn, sym, detect_symbol_splits(rows))
            print(f"  reparse {sym}: {len(rows)} rows", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch SEC EDGAR XBRL companyfacts")
    ap.add_argument("--universe", choices=["sp100", "sp500"], default="sp500")
    ap.add_argument("--symbols", type=str, default=None,
                    help="comma-separated subset (skips universe discovery)")
    ap.add_argument("--limit", type=int, default=None, help="only first N symbols")
    ap.add_argument("--refresh", action="store_true", help="re-download even if cached")
    ap.add_argument("--reparse", action="store_true",
                    help="rebuild the DB from cached raw JSON, no network")
    ap.add_argument("--sleep", type=float, default=0.2,
                    help="seconds between requests (0.2 == SEC's 5/s guidance)")
    args = ap.parse_args()

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    conn = connect()
    ensure_schema(conn)
    status = load_status()

    if args.reparse:
        reparse_all(conn, status)
        conn.close()
        return

    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        meta = {"universe": "explicit", "selected": len(symbols)}
    else:
        symbols, meta = select_symbols(args.universe)
    if args.limit:
        symbols = symbols[:args.limit]

    tmap = load_ticker_cik_map()
    todo = [s for s in symbols
            if args.refresh or status.get(s, {}).get("status") not in ("ok",)]

    print(f"  universe={meta.get('universe')}  selected={len(symbols)}  "
          f"todo={len(todo)} (resume-aware)  rate={1/args.sleep:.0f}/s")
    if "pit_union_size" in meta:
        print(f"  pit_union={meta['pit_union_size']}  with_price={meta['with_price']}  "
              f"with_cik={meta['with_cik']}  no_price={len(meta['no_price_data'])}  "
              f"no_cik={len(meta['no_cik_mapping'])}")

    t0 = time.time()
    last_req = 0.0
    done = 0
    for sym in todo:
        cik = cik_for(sym, tmap)
        if cik is None:
            status[sym] = {"status": "no_cik", "symbol": sym}
            save_status(status)
            continue
        wait = args.sleep - (time.time() - last_req)
        if wait > 0:
            time.sleep(wait)
        try:
            last_req = time.time()
            info = fetch_one(sym, cik, conn, refresh=args.refresh)
        except urllib.error.HTTPError as exc:
            info = {"status": "no_facts" if exc.code == 404 else f"http_{exc.code}",
                    "cik": cik, "n_facts": 0, "n_splits": 0}
        except Exception as exc:  # noqa: BLE001
            info = {"status": "failed", "cik": cik, "error": str(exc)[:200],
                    "n_facts": 0, "n_splits": 0}
        info["symbol"] = sym
        info["fetched_at"] = datetime.now(tz=timezone.utc).isoformat(timespec="seconds")
        status[sym] = info
        done += 1
        if done % 25 == 0 or done == len(todo):
            save_status(status)
            el = time.time() - t0
            print(f"  [{done}/{len(todo)}] {sym} {info['status']} "
                  f"n={info.get('n_facts', 0)}  {el:.0f}s  "
                  f"({el / done:.2f}s/sym)", flush=True)
    save_status(status)

    elapsed = time.time() - t0
    per_sym = elapsed / max(done, 1)
    n_facts = conn.execute("SELECT COUNT(*) FROM facts").fetchone()[0]
    n_sym = conn.execute("SELECT COUNT(DISTINCT symbol) FROM facts").fetchone()[0]
    n_splits = conn.execute("SELECT COUNT(*) FROM splits").fetchone()[0]
    db_bytes = DB_PATH.stat().st_size if DB_PATH.exists() else 0
    raw_bytes = sum(p.stat().st_size for p in RAW_DIR.glob("CIK*.json.gz"))

    print(f"\n  done in {elapsed:.0f}s ({per_sym:.2f}s/symbol)")
    print(f"  facts rows: {format(n_facts, ',')}   "
          f"symbols with facts: {n_sym}   splits: {n_splits}")
    print(f"  DB {db_bytes/1e6:.1f} MB   raw cache {raw_bytes/1e6:.1f} MB")
    conn.close()


if __name__ == "__main__":
    main()
