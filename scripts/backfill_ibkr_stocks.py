#!/usr/bin/env python3
"""Backfill stock history via IBKR — including delisted / pre-2016 names.

Uses the point-in-time universe union (data/universe/sp100_union.json,
built by fetch_sp100_history.py) so the momentum backtest can run without
survivorship bias. Requires IB Gateway running (see IbkrHistoricalStore
docstring for setup).

Rename mapping: renamed companies carry history under the CURRENT ticker
(FB->META, PCLN->BKNG, UTX->RTX), so we fetch the current symbol and the
backtest universe loader maps old->new at load time.

Usage:
    poetry run python scripts/backfill_ibkr_stocks.py             # union universe
    poetry run python scripts/backfill_ibkr_stocks.py --symbols AAPL,META
    poetry run python scripts/backfill_ibkr_stocks.py --port 7497   # TWS paper
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.data.ibkr_store import IbkrHistoricalStore

UNION_PATH = Path(__file__).resolve().parents[1] / "data" / "universe" / "sp100_union.json"

# Old ticker -> current ticker (renames; history lives under the new symbol)
RENAME_MAP = {
    "FB": "META",
    "PCLN": "BKNG",
    "UTX": "RTX",
    "DWDP": "DD",      # DowDuPont breakup -> Dow; approximated
    "TWX": "T",        # acquired by AT&T
    "CELG": "BMY",     # acquired by Bristol-Myers
    "MON": "BAYRY",    # acquired by Bayer (ADR)
    "AGN": "ABBV",     # acquired by AbbVie
}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--symbols", type=str, default=None, help="comma list; default = union universe")
    p.add_argument("--universe", type=str, default=str(UNION_PATH))
    p.add_argument("--port", type=int, default=4002)
    p.add_argument("--host", type=str, default="127.0.0.1")
    p.add_argument("--duration", type=str, default="10 Y")
    args = p.parse_args()

    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",")]
    else:
        symbols = json.loads(Path(args.universe).read_text())
    # fetch under current tickers where renamed
    fetch_symbols = sorted({RENAME_MAP.get(s, s) for s in symbols})

    store = IbkrHistoricalStore(host=args.host, port=args.port)
    print(f"IBKR backfill: {len(fetch_symbols)} symbols (union {len(symbols)}, "
          f"{len(symbols) - len(fetch_symbols)} renames mapped)")
    if not store.connect():
        print(store.connection_help())
        raise SystemExit(1)

    print("connected.\n")
    ok, skipped, failed = 0, 0, []
    try:
        for i, sym in enumerate(fetch_symbols, 1):
            _f, _l, n = store._store.get_coverage(sym)
            if n >= 2000:  # already covered by Nasdaq API backfill
                skipped += 1
                continue
            try:
                n = store.fetch_and_store(sym, duration=args.duration)
                ok += 1
                if i % 10 == 0 or i == len(fetch_symbols):
                    print(f"  [{i}/{len(fetch_symbols)}] {sym}: +{n} rows")
            except Exception as e:  # noqa: BLE001
                failed.append(sym)
                print(f"  [{i}/{len(fetch_symbols)}] {sym}: FAILED {str(e)[:70]}")
            time.sleep(0.5)  # pacing
    finally:
        store.disconnect()

    print(f"\nDone: {ok} fetched, {skipped} already covered, {len(failed)} failed")
    if failed:
        print("failed:", ", ".join(failed))


if __name__ == "__main__":
    main()
