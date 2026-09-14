#!/usr/bin/env python3
"""Backfill crypto-proxy stocks and ETFs via Nasdaq API.

Crypto-related equities that serve as bridge instruments between
the crypto market (24/7) and the stock market (6.5h/day).

Usage:
    poetry run python scripts/backfill_crypto_stocks.py
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.data.nasdaq_store import NasdaqDailyStore

# Crypto-proxy stocks (individual companies with crypto exposure)
CRYPTO_STOCKS = [
    "COIN",   # Coinbase — crypto exchange
    "MSTR",   # MicroStrategy — largest corporate BTC holder
    "MARA",   # Marathon Digital — BTC miner
    "RIOT",   # Riot Platforms — BTC miner
    "HOOD",   # Robinhood — retail crypto trading
    "CLSK",   # CleanSpark — BTC miner
    "SI",     # Silvergate — crypto bank (delisted but history valuable)
]

# Crypto ETFs
CRYPTO_ETFS = [
    "GBTC",   # Grayscale Bitcoin Trust
    "BITB",   # Bitwise Bitcoin ETF
]


def main() -> None:
    store = NasdaqDailyStore(assetclass="stocks", politeness_s=1.5)
    print("Crypto-proxy stocks backfill\n")

    ok, failed = [], []
    for sym in CRYPTO_STOCKS:
        try:
            n = store.fetch_and_store(sym)
            first, last, total = store.get_coverage(sym)
            ok.append(sym)
            print(f"  {sym}: +{n} rows, {total} total, {first} ~ {last}")
        except Exception as e:
            failed.append((sym, str(e)[:80]))
            print(f"  {sym}: FAILED {str(e)[:80]}")
        time.sleep(1.5)

    etf_store = NasdaqDailyStore(assetclass="etf", politeness_s=1.5)
    print("\nCrypto ETFs backfill\n")
    for sym in CRYPTO_ETFS:
        try:
            n = etf_store.fetch_and_store(sym)
            first, last, total = etf_store.get_coverage(sym)
            ok.append(sym)
            print(f"  {sym}: +{n} rows, {total} total, {first} ~ {last}")
        except Exception as e:
            failed.append((sym, str(e)[:80]))
            print(f"  {sym}: FAILED {str(e)[:80]}")
        time.sleep(1.5)

    print(f"\nDone: {len(ok)} ok, {len(failed)} failed")
    if failed:
        for sym, err in failed:
            print(f"  {sym}: {err}")


if __name__ == "__main__":
    main()
