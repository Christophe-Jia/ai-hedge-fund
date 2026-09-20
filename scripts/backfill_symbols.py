#!/usr/bin/env python3
"""Backfill arbitrary US daily symbols that are outside the tracked universes.

The platform's stock panel is built from the point-in-time S&P 100 / S&P 500
union files (plus crypto proxies). A symbol that is not — and never was — an
index constituent during the sample (e.g. MRVL, which only joined the S&P 500 on
2026-06-22) is therefore absent from `data/btc_history.db` even though a study
may need it as a single-name diagnostic.

This script fetches such symbols from the Nasdaq daily API via
:class:`NasdaqDailyStore` (same code path as `backfill_sp500_stocks.py`), with
per-symbol politeness pacing; the store itself already retries 3x with backoff.

Usage:
    poetry run python scripts/backfill_symbols.py --symbols MRVL
    poetry run python scripts/backfill_symbols.py --symbols MRVL,MU --force
    poetry run python scripts/backfill_symbols.py --symbols SPY --assetclass etf
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.data.nasdaq_store import NasdaqDailyStore  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPORT = ROOT / "reports" / "backfill_symbols_status.json"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--symbols", required=True, help="comma-separated tickers")
    p.add_argument("--assetclass", default="stocks", choices=["stocks", "etf"])
    p.add_argument("--from-date", default="2016-01-01")
    p.add_argument("--to-date", default=None, help="default: today")
    p.add_argument("--politeness", type=float, default=1.5)
    p.add_argument("--min-rows", type=int, default=2000,
                   help="coverage at or above this is treated as already backfilled")
    p.add_argument("--force", action="store_true", help="refetch even if covered")
    p.add_argument("--report", default=str(DEFAULT_REPORT))
    args = p.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    store = NasdaqDailyStore(assetclass=args.assetclass, politeness_s=args.politeness)
    print(f"backfill_symbols: {len(symbols)} symbol(s), assetclass={args.assetclass}, "
          f"from={args.from_date}, to={args.to_date or 'today'}")

    status: dict[str, dict] = {}
    fetched = 0
    for i, sym in enumerate(symbols, 1):
        before = store.get_coverage(sym)
        n_before = before[2] if before[0] else 0
        if n_before >= args.min_rows and not args.force:
            status[sym] = {"status": "already_covered", "rows": n_before,
                           "first": before[0], "last": before[1]}
            print(f"  [{i}/{len(symbols)}] {sym}: already covered ({n_before} rows, "
                  f"{before[0]} ~ {before[1]})")
            continue
        try:
            written = store.fetch_and_store(sym, from_date=args.from_date,
                                            to_date=args.to_date)
            first, last, total = store.get_coverage(sym)
            status[sym] = {"status": "ok", "rows_written": written, "rows": total,
                           "first": first, "last": last}
            fetched += 1
            print(f"  [{i}/{len(symbols)}] {sym}: +{written} rows -> {total} total "
                  f"({first} ~ {last})")
        except Exception as e:  # noqa: BLE001 — network errors vary
            status[sym] = {"status": "failed", "error": str(e)[:160],
                           "rows": n_before}
            print(f"  [{i}/{len(symbols)}] {sym}: FAILED {str(e)[:90]}")
        if i < len(symbols):
            time.sleep(args.politeness)

    ok = sum(1 for v in status.values() if v["status"] in ("ok", "already_covered"))
    failed = [s for s, v in status.items() if v["status"] == "failed"]
    print(f"\nDone: {fetched} fetched, {ok}/{len(symbols)} covered, "
          f"{len(failed)} failed {failed or ''}")

    out = Path(args.report)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({
        "generated_at": datetime.now().astimezone().isoformat(),
        "script": "scripts/backfill_symbols.py",
        "assetclass": args.assetclass,
        "from_date": args.from_date,
        "to_date": args.to_date or "today",
        "symbols": status,
    }, ensure_ascii=False, indent=1))
    print(f"wrote {out}")

    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
