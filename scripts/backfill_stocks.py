#!/usr/bin/env python3
"""Backfill daily prices for a universe of stocks from Nasdaq's public API.

Reads data/universe/sp100.json (see fetch_sp100_universe.py) and pulls
~10y of daily OHLCV per symbol into the shared SQLite store
(market_type='stocks'). Politeness-delayed; resumable (upsert semantics,
skip symbols already fully covered unless --refresh).

Usage:
    poetry run python scripts/backfill_stocks.py
    poetry run python scripts/backfill_stocks.py --universe data/universe/sp100.json
    poetry run python scripts/backfill_stocks.py --refresh   # re-fetch all
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.data.nasdaq_store import NasdaqDailyStore

_DEFAULT_UNIVERSE = Path(__file__).resolve().parents[1] / "data" / "universe" / "sp100.json"


def main() -> None:
    p = argparse.ArgumentParser(description="Backfill stock daily prices (Nasdaq API)")
    p.add_argument("--universe", type=str, default=str(_DEFAULT_UNIVERSE))
    p.add_argument("--refresh", action="store_true", help="re-fetch even if covered")
    args = p.parse_args()

    with open(args.universe) as f:
        constituents = json.load(f)
    symbols = [c["symbol"] for c in constituents]

    store = NasdaqDailyStore(assetclass="stocks", politeness_s=1.2)
    print(f"Backfilling {len(symbols)} stocks from Nasdaq "
          f"(universe: {args.universe})\n")

    ok, skipped, failed = 0, 0, []
    for i, sym in enumerate(symbols, 1):
        first, last, total = store.get_coverage(sym)
        covered = total >= 2000 and (last or "") >= datetime.now(
            tz=timezone.utc
        ).strftime("%Y-%m-01")
        if covered and not args.refresh:
            skipped += 1
            continue
        try:
            n = store.fetch_and_store(sym)
            ok += 1
            if i % 10 == 0 or i == len(symbols):
                print(f"  [{i}/{len(symbols)}] {sym}: +{n} rows")
        except Exception as e:  # noqa: BLE001 — keep going on individual failures
            failed.append(sym)
            print(f"  [{i}/{len(symbols)}] {sym}: FAILED {str(e)[:80]}")

    print(f"\nDone: {ok} fetched, {skipped} already covered, {len(failed)} failed")
    if failed:
        print("failed:", ", ".join(failed))


if __name__ == "__main__":
    main()
