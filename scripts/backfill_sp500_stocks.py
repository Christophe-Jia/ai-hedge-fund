#!/usr/bin/env python3
"""Backfill S&P 500 union universe daily history — Nasdaq first, IBKR for misses.

Union universe from fetch_sp500_history.py (data/universe/sp500_union.json,
737 symbols incl. every name that left the index 2016-2026). For each symbol
missing local coverage:
  1. Nasdaq public API (NasdaqDailyStore, 1.5s politeness) — works for
     listed names incl. renamed ones (history lives under the new ticker).
  2. IBKR bridge (--ibkr, requires IB Gateway on :4002) — the only source
     for delisted names (ATVI, FRC, SIVB, TWX-era acquisitions, ...).

Checkpointed: per-symbol status persists to
data/universe/sp500_backfill_status.json so the run is resumable — statuses
"ok"/"empty" are skipped on re-run, "failed" retried (or with
--retry-failed explicitly).

Usage:
    poetry run python scripts/backfill_sp500_stocks.py               # Nasdaq pass
    poetry run python scripts/backfill_sp500_stocks.py --retry-failed
    poetry run python scripts/backfill_sp500_stocks.py --ibkr        # IBKR pass
    poetry run python scripts/backfill_sp500_stocks.py --summary     # coverage only
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.data.nasdaq_store import NasdaqDailyStore  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
UNION_PATH = ROOT / "data" / "universe" / "sp500_union.json"
STATUS_PATH = ROOT / "data" / "universe" / "sp500_backfill_status.json"

# Old ticker -> current ticker. History (Nasdaq/IBKR) lives under the new
# symbol for pure renames. Base = the sp100 convention (backfill_ibkr_stocks.
# RENAME_MAP); extras are S&P-500-specific pure renames 2016-2026 where the
# current ticker still trades AND Nasdaq serves the pre-rename history under
# it (verified: BFH/CBRE/META etc. carry full 2016+ history; VTRS does NOT
# carry the Mylan era -> MYL stays unmapped and goes to IBKR).
# Acquired/merged/bankrupt names (ATVI, PXD, DISCA, CHK, ...) are NOT
# mapped: their own delisted history is fetched via IBKR instead.
RENAME_MAP = {
    # --- sp100 base convention (kept identical for comparability) ---
    "FB": "META", "PCLN": "BKNG", "UTX": "RTX", "RTN": "RTX",
    "BK": "BNY", "DWDP": "DD", "TWX": "T", "CELG": "BMY",
    "MON": "BAYRY", "AGN": "ABBV",
    # --- S&P 500 pure renames (same security, new ticker, still listed) ---
    "ANTM": "ELV",     # Anthem -> Elevance (2022)
    "SYMC": "GEN",     # Symantec -> NortonLifeLock -> Gen Digital
    "NLOK": "GEN",
    "PKI": "RVTY",     # PerkinElmer -> Revvity (2023)
    "WLTW": "WTW",     # Willis Towers Watson -> WTW (2022)
    # NOTE: FISV (Fiserv old ticker) is NOT mapped — Nasdaq serves its full
    # same-lineage history under the old symbol (verified: prices continuous
    # through the 2023 rename), which is better PIT fidelity than FI.
    "FLT": "CPAY",     # FleetCor -> Corpay (2024; NOT Fiserv!)
    "XEC": "CTRA",     # Cimarex -> Coterra (2021, surviving entity renamed)
    "LB": "BBWI",      # L Brands -> Bath & Body Works (2021)
    "BBT": "TFC",      # BB&T -> Truist (2019, surviving entity renamed)
    "CBG": "CBRE",     # CB Richard Ellis -> CBRE (2017)
    "ABC": "COR",      # AmerisourceBergen -> Cencora (2023)
    "ADS": "BFH",      # Alliance Data -> Bread Financial (2022)
    "BLL": "BALL",     # Ball Corp (2022)
    "COH": "TPR",      # Coach -> Tapestry (2017)
    "CTL": "LUMN",     # CenturyLink -> Lumen (2021)
    "CDAY": "DAY",     # Ceridian -> Dayforce (2024)
    "BHGE": "BKR",     # Baker Hughes GE -> Baker Hughes (2019)
    "HRS": "LHX",      # Harris -> L3Harris (2019)
    "LLL": "LHX",      # L-3 Technologies -> L3Harris (2019)
    "HCN": "WELL",     # Welltower (2018)
    "JEC": "J",        # Jacobs Engineering -> Jacobs (2022)
    "KORS": "CPRI",    # Michael Kors -> Capri (2019)
    "LUK": "JEF",      # Leucadia -> Jefferies Financial (2019)
    "MHFI": "SPGI",    # McGraw Hill Financial -> S&P Global (2016)
    "PX": "LIN",       # Praxair -> Linde (2018)
    "RE": "EG",        # Everest Re Group -> Everest (2023)
    "TMK": "GL",       # Torchmark -> Globe Life (2019)
    "HCP": "DOC",      # Healthpeak: HCP -> PEAK (2019) -> DOC (2024); Nasdaq's
    "PEAK": "DOC",     # DOC series is the Healthpeak lineage (verified 2017 px)
}


def load_status() -> dict:
    if STATUS_PATH.exists():
        return json.loads(STATUS_PATH.read_text())
    return {}


def save_status(st: dict) -> None:
    STATUS_PATH.write_text(json.dumps(st, indent=1, sort_keys=True))


def db_coverage(store: NasdaqDailyStore, sym: str) -> int:
    _f, _l, n = store.get_coverage(sym)
    return n


def constituent_year_weights() -> dict[str, int]:
    """Symbol -> number of PIT years it appears in (post-rename)."""
    w: dict[str, int] = {}
    for f in sorted((ROOT / "data" / "universe").glob("sp500_[0-9]*.json")):
        for c in json.loads(f.read_text()):
            s = RENAME_MAP.get(c["symbol"], c["symbol"])
            w[s] = w.get(s, 0) + 1
    return w


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--ibkr", action="store_true", help="IBKR pass for symbols Nasdaq missed")
    p.add_argument("--retry-failed", action="store_true")
    p.add_argument("--port", type=int, default=4002)
    p.add_argument("--min-rows", type=int, default=200,
                   help="symbols below this row count are (re)fetched")
    p.add_argument("--summary", action="store_true", help="print coverage summary only")
    args = p.parse_args()

    union = json.loads(UNION_PATH.read_text())
    fetch_syms = sorted({RENAME_MAP.get(s, s) for s in union})
    status = load_status()
    store = NasdaqDailyStore(assetclass="stocks")

    # pre-seed status from existing DB coverage (idempotent re-runs)
    for sym in fetch_syms:
        if sym not in status:
            n = db_coverage(store, sym)
            if n >= args.min_rows:
                status[sym] = {"status": "ok", "rows": n, "source": "existing"}

    if args.summary:
        args.retry_failed = False

    # ------------------------------------------------------------------ IBKR
    if args.ibkr:
        import ib_async
        from src.data.historical_store import HistoricalOHLCVStore

        def _ibkr_fetch(ib, sym: str, duration: str = "10 Y"):
            """Daily bars incl. DELISTED contracts (includeExpired=True).

            Implemented here rather than via IbkrHistoricalStore because the
            store's qualifyContracts path returns [None] for expired
            contracts (its `if not qualified` guard passes for a non-empty
            list holding None) and crashes. Setting includeExpired BEFORE
            qualifying is what makes delisted tickers resolvable.
            """
            c = ib_async.Stock(sym, "SMART", "USD")
            c.includeExpired = True
            qualified = ib.qualifyContracts(c)
            q = next((x for x in qualified if x is not None), None)
            if q is None:
                raise ValueError("contract not found on IBKR")
            return ib.reqHistoricalData(
                q, endDateTime="", durationStr=duration,
                barSizeSetting="1 day", whatToShow="TRADES",
                useRTH=True, formatDate=1,
            )

        targets = [s for s in fetch_syms
                   if status.get(s, {}).get("status") != "ok"]
        print(f"IBKR pass: {len(targets)} symbols without local data")
        ib = ib_async.IB()
        if not ib.connect("127.0.0.1", args.port, clientId=19, timeout=15):
            raise SystemExit(f"IB Gateway not reachable on :{args.port}")
        hv = HistoricalOHLCVStore(allow_fetch=False)
        ok, empty = 0, 0
        try:
            for i, sym in enumerate(targets, 1):
                try:
                    bars = _ibkr_fetch(ib, sym, duration=args.duration)
                    rows = []
                    for b in bars:
                        ts = int(datetime(
                            b.date.year, b.date.month, b.date.day,
                            tzinfo=timezone.utc).timestamp() * 1000)
                        rows.append([ts, float(b.open), float(b.high),
                                     float(b.low), float(b.close), float(b.volume)])
                    if rows:
                        n = hv.upsert_ohlcv(sym, "stocks", "1d", rows)
                        status[sym] = {"status": "ok", "rows": n, "source": "ibkr"}
                        ok += 1
                        first = datetime.fromtimestamp(rows[0][0] / 1000, tz=timezone.utc).date()
                        last = datetime.fromtimestamp(rows[-1][0] / 1000, tz=timezone.utc).date()
                        print(f"  [{i}/{len(targets)}] {sym}: {n} rows "
                              f"({first} ~ {last})", flush=True)
                    else:
                        status[sym] = {"status": "empty", "source": "ibkr"}
                        empty += 1
                        print(f"  [{i}/{len(targets)}] {sym}: 0 rows", flush=True)
                except Exception as e:  # noqa: BLE001
                    status[sym] = {"status": "failed", "error": str(e)[:120]}
                    print(f"  [{i}/{len(targets)}] {sym}: FAILED {str(e)[:80]}", flush=True)
                save_status(status)
                time.sleep(0.5)
        finally:
            ib.disconnect()
        print(f"IBKR done: {ok} ok, {empty} empty")

    # --------------------------------------------------------------- Nasdaq
    elif not args.summary:
        targets = []
        for sym in fetch_syms:
            st = status.get(sym, {}).get("status")
            if st == "ok":
                continue
            if st == "empty" and not args.retry_failed:
                continue
            if st == "failed" and not args.retry_failed:
                continue
            targets.append(sym)
        print(f"Nasdaq pass: {len(targets)} symbols to fetch "
              f"({len(fetch_syms) - len(targets)} already ok/empty)")
        for i, sym in enumerate(targets, 1):
            try:
                n = store.fetch_and_store(sym)
                status[sym] = {"status": "ok", "rows": n, "source": "nasdaq"}
                print(f"  [{i}/{len(targets)}] {sym}: {n} rows", flush=True)
            except Exception as e:  # noqa: BLE001
                attempts = status.get(sym, {}).get("attempts", 0) + 1
                status[sym] = {"status": "failed", "attempts": attempts,
                               "error": str(e)[:120]}
                print(f"  [{i}/{len(targets)}] {sym}: FAILED "
                      f"(a={attempts}) {str(e)[:70]}", flush=True)
            if i % 10 == 0:
                save_status(status)
        save_status(status)

    # -------------------------------------------------------------- Summary
    w = constituent_year_weights()
    fs = set(fetch_syms)
    covered = {s for s, v in status.items() if v.get("status") == "ok" and s in fs}
    no_data = {s for s, v in status.items() if v.get("status") == "empty" and s in fs}
    failed = {s for s, v in status.items() if v.get("status") == "failed" and s in fs}
    todo = [s for s in fetch_syms if s not in status]

    total_cy = sum(w.values())
    cov_cy = sum(w.get(s, 0) for s in covered)
    print(f"\n  union (post-rename): {len(fetch_syms)} symbols, "
          f"{total_cy} constituent-years")
    print(f"  covered:  {len(covered)} ({len(covered)/len(fetch_syms)*100:.1f}%)")
    print(f"  no data:  {len(no_data)}  {sorted(no_data)}")
    print(f"  failed:   {len(failed)}  {sorted(failed)}")
    if todo:
        print(f"  not yet attempted: {len(todo)}")
    print(f"  constituent-year weighted coverage: "
          f"{cov_cy}/{total_cy} = {cov_cy/total_cy*100:.1f}%")


if __name__ == "__main__":
    main()
