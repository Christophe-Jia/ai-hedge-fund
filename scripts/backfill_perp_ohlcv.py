"""
Backfill BTC/USDT:USDT perpetual futures 1h OHLCV and funding rates
from Gate.io via CCXT REST API.

Fills the gap between the last stored timestamp and now for:
  - Perpetual 1h OHLCV → btc_history.db / ohlcv table
  - Funding rates       → btc_history.db / funding_rates table

Usage:
    poetry run python scripts/backfill_perp_ohlcv.py
    poetry run python scripts/backfill_perp_ohlcv.py --exchange gate --timeframe 1h
    poetry run python scripts/backfill_perp_ohlcv.py --dry-run
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone

# Ensure project root is on path when run directly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    import ccxt
except ImportError as exc:
    raise SystemExit("ccxt is required. Run: poetry install") from exc

from src.data.historical_store import HistoricalOHLCVStore
from src.data.funding_rates import FundingRateStore

SYMBOL_PERP = "BTC/USDT:USDT"
SYMBOL_SPOT = "BTC/USDT"
TIMEFRAME = "1h"
LIMIT = 1000
FUNDING_LIMIT = 500

# Gate.io perp swap — defaultType must be "swap" for perpetuals
_EXCHANGE_KWARGS = {
    "gate": {"defaultType": "swap"},
    "binance": {"defaultType": "future"},
    "bybit": {"defaultType": "linear"},
}


def make_exchange(exchange_id: str) -> ccxt.Exchange:
    kwargs = _EXCHANGE_KWARGS.get(exchange_id, {})
    cls = getattr(ccxt, exchange_id)
    return cls({"enableRateLimit": True, "options": kwargs})


def backfill_ohlcv(
    exchange: ccxt.Exchange,
    store: HistoricalOHLCVStore,
    symbol: str,
    timeframe: str,
    dry_run: bool,
) -> int:
    """Fetch and store OHLCV candles from the last stored ts to now."""
    last_ts = store.get_latest_ts(symbol, "perp", timeframe)
    if last_ts is None:
        # Start from 2 years ago if DB is empty
        since_ms = int((datetime.now(timezone.utc).timestamp() - 2 * 365 * 24 * 3600) * 1000)
        print(f"  No existing data — starting from {datetime.fromtimestamp(since_ms / 1000, tz=timezone.utc)}")
    else:
        # Resume from the candle after the last stored one
        since_ms = last_ts + 1
        print(
            f"  Last stored ts: {datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc)}"
            f" ({last_ts})"
        )

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    if since_ms >= now_ms:
        print("  Already up to date — nothing to fetch.")
        return 0

    total_written = 0
    current_since = since_ms
    last_ts_seen = None

    print(f"  Fetching {symbol} {timeframe} from {datetime.fromtimestamp(since_ms / 1000, tz=timezone.utc)} ...")

    while current_since < now_ms:
        try:
            candles = exchange.fetch_ohlcv(
                symbol, timeframe=timeframe, since=current_since, limit=LIMIT
            )
        except ccxt.NetworkError as e:
            print(f"  [WARN] NetworkError: {e} — retrying in 10s ...")
            time.sleep(10)
            continue
        except ccxt.ExchangeError as e:
            print(f"  [ERROR] ExchangeError: {e}")
            break

        if not candles:
            print("  No more candles returned.")
            break

        # Filter to only closed candles (before now)
        candles = [c for c in candles if c[0] < now_ms]
        if not candles:
            break

        newest_ts = candles[-1][0]

        if not dry_run:
            n = store.upsert_ohlcv(symbol, "perp", timeframe, candles)
            total_written += n

        print(
            f"  batch: {len(candles)} candles  "
            f"from {datetime.fromtimestamp(candles[0][0] / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')}"
            f" → {datetime.fromtimestamp(newest_ts / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')}"
            f"  {'(dry-run)' if dry_run else f'+{len(candles)} rows'}"
        )

        if newest_ts >= now_ms:
            break
        if newest_ts == last_ts_seen:
            print("  [WARN] No progress — exchange returned same last timestamp. Stopping.")
            break

        last_ts_seen = newest_ts
        current_since = newest_ts + 1
        time.sleep(exchange.rateLimit / 1000)

    return total_written


def backfill_funding_rates(
    exchange: ccxt.Exchange,
    store: FundingRateStore,
    symbol: str,
    dry_run: bool,
) -> int:
    """Fetch and store funding rates from the last stored ts to now."""
    last_ts = store.get_latest_ts(symbol)
    if last_ts is None:
        since_ms = int((datetime.now(timezone.utc).timestamp() - 365 * 24 * 3600) * 1000)
        print(f"  No existing funding data — starting from {datetime.fromtimestamp(since_ms / 1000, tz=timezone.utc)}")
    else:
        since_ms = last_ts + 1
        print(
            f"  Last stored funding ts: {datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc)}"
        )

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    if since_ms >= now_ms:
        print("  Funding rates already up to date — nothing to fetch.")
        return 0

    total_written = 0
    current_since = since_ms
    last_ts_seen = None

    print(f"  Fetching {symbol} funding rates from {datetime.fromtimestamp(since_ms / 1000, tz=timezone.utc)} ...")

    while current_since < now_ms:
        try:
            rates = exchange.fetch_funding_rate_history(
                symbol, since=current_since, limit=FUNDING_LIMIT
            )
        except ccxt.NetworkError as e:
            print(f"  [WARN] NetworkError: {e} — retrying in 10s ...")
            time.sleep(10)
            continue
        except ccxt.ExchangeError as e:
            print(f"  [ERROR] ExchangeError fetching funding: {e}")
            break
        except Exception as e:
            # Some exchanges don't support funding rate history for all symbols
            print(f"  [ERROR] Unexpected error fetching funding rates: {e}")
            break

        if not rates:
            print("  No more funding rates returned.")
            break

        filtered = [r for r in rates if r.get("timestamp") and r["timestamp"] < now_ms]
        if not filtered:
            break

        newest_ts = filtered[-1]["timestamp"]
        rows = [
            (int(r["timestamp"]), float(r["fundingRate"]))
            for r in filtered
            if r.get("fundingRate") is not None
        ]

        if not dry_run and rows:
            n = store.upsert_rates(symbol, rows)
            total_written += n

        print(
            f"  funding batch: {len(rows)} records  "
            f"from {datetime.fromtimestamp(filtered[0]['timestamp'] / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')}"
            f" → {datetime.fromtimestamp(newest_ts / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')}"
            f"  {'(dry-run)' if dry_run else f'+{len(rows)} rows'}"
        )

        if newest_ts >= now_ms:
            break
        if newest_ts == last_ts_seen:
            print("  [WARN] No progress — same last timestamp. Stopping.")
            break

        last_ts_seen = newest_ts
        current_since = newest_ts + 1
        time.sleep(exchange.rateLimit / 1000)

    return total_written


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill BTC perp OHLCV + funding rates via Gate.io"
    )
    parser.add_argument(
        "--exchange",
        default="gate",
        help="CCXT exchange ID with perp/swap support (default: gate)",
    )
    parser.add_argument(
        "--timeframe",
        default="1h",
        help="OHLCV timeframe (default: 1h)",
    )
    parser.add_argument(
        "--symbol",
        default=SYMBOL_PERP,
        help=f"Perpetual symbol (default: {SYMBOL_PERP})",
    )
    parser.add_argument(
        "--db",
        default=None,
        help="SQLite DB path (default: data/btc_history.db)",
    )
    parser.add_argument(
        "--skip-funding",
        action="store_true",
        help="Skip funding rate backfill",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch data but do not write to DB",
    )
    args = parser.parse_args()

    db_path_kwarg = {"db_path": args.db} if args.db else {}

    ohlcv_store = HistoricalOHLCVStore(**db_path_kwarg, exchange_id=args.exchange)
    funding_store = FundingRateStore(**db_path_kwarg, exchange_id=args.exchange)

    print(f"\n=== BTC Perp Backfill ===")
    print(f"  Exchange  : {args.exchange}")
    print(f"  Symbol    : {args.symbol}")
    print(f"  Timeframe : {args.timeframe}")
    print(f"  Dry run   : {args.dry_run}")
    print(f"  Started   : {datetime.now(tz=timezone.utc).isoformat()}\n")

    exchange = make_exchange(args.exchange)

    # 1. Backfill OHLCV
    print("[1/2] OHLCV backfill ...")
    n_ohlcv = backfill_ohlcv(exchange, ohlcv_store, args.symbol, args.timeframe, args.dry_run)
    print(f"  Total OHLCV rows written: {n_ohlcv}\n")

    # 2. Backfill funding rates
    if not args.skip_funding:
        print("[2/2] Funding rate backfill ...")
        n_funding = backfill_funding_rates(exchange, funding_store, args.symbol, args.dry_run)
        print(f"  Total funding rows written: {n_funding}\n")
    else:
        print("[2/2] Skipping funding rates (--skip-funding)\n")

    print("Done.")


if __name__ == "__main__":
    main()
