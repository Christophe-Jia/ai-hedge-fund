#!/usr/bin/env python3
"""Backfill US ETF daily prices from Nasdaq's public API into SQLite.

QQQ/VOO are the selection-strategy benchmarks; SPY included as the market
reference. ~10 years of history per symbol (Nasdaq API limit). Closes are
price-only — use EtfDailyStore.get_close_series(div_yield_annual=...) for
total-return benchmarks.

Usage:
    poetry run python scripts/backfill_etf_daily.py              # QQQ VOO SPY
    poetry run python scripts/backfill_etf_daily.py QQQ TLT
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.data.nasdaq_store import NasdaqDailyStore as EtfDailyStore

DEFAULT_SYMBOLS = ["QQQ", "VOO", "SPY"]


def main() -> None:
    symbols = [s.strip().upper() for s in sys.argv[1:]] or DEFAULT_SYMBOLS
    store = EtfDailyStore()

    print(f"Nasdaq ETF daily backfill: {symbols}\n")
    for sym in symbols:
        try:
            n = store.fetch_and_store(sym)
        except ValueError as e:
            print(f"  {sym}: ERROR {e}")
            continue
        except Exception as e:  # noqa: BLE001 — network errors vary
            print(f"  {sym}: FETCH ERROR {e}")
            continue
        first, last, total = store.get_coverage(sym)
        print(f"  {sym}: +{n} rows this run, {total} total, {first} ~ {last}")

    print("\nDone.")


if __name__ == "__main__":
    main()
