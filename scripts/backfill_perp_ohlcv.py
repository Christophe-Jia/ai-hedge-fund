"""
Backfill perp/spot OHLCV and funding rates via CCXT REST API.

Supports multiple symbols, timeframes and market types, with incremental
resume (fetches only from the last stored timestamp).

Data sources:
  - gate (default): perp OHLCV (4h/1d deep history; 15m/1h limited to the
    most recent 10,000 candles) + funding rates (180-day lookback limit).
  - binance + --hostname data-api.binance.vision: deep spot OHLCV history
    (public market-data mirror, reachable where api.binance.com is blocked).

Examples:
    # Gate perp 4h/1d, 3 years, multiple symbols
    poetry run python scripts/backfill_perp_ohlcv.py \\
        --symbol "BTC/USDT:USDT,ETH/USDT:USDT,SOL/USDT:USDT" \\
        --timeframe "4h,1d" --years 3

    # Binance mirror spot 15m/1h, 3 years (no funding on mirror)
    poetry run python scripts/backfill_perp_ohlcv.py \\
        --exchange binance --hostname data-api.binance.vision \\
        --market spot --symbol "BTC/USDT,ETH/USDT,SOL/USDT" \\
        --timeframe "15m,1h" --years 3 --skip-funding
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


def make_exchange(exchange_id: str, hostname: str | None = None) -> ccxt.Exchange:
    if hostname:
        # Mirror mode (e.g. data-api.binance.vision): spot market data only.
        # Ignore defaultType overrides — the mirror serves no futures endpoints.
        cls = getattr(ccxt, exchange_id)
        ex = cls({"enableRateLimit": True, "options": {"fetchMarkets": ["spot"]}})
        for k, v in list(ex.urls["api"].items()):
            if isinstance(v, str) and "api.binance.com" in v:
                ex.urls["api"][k] = v.replace("api.binance.com", hostname)
        return ex
    kwargs = _EXCHANGE_KWARGS.get(exchange_id, {})
    cls = getattr(ccxt, exchange_id)
    return cls({"enableRateLimit": True, "options": kwargs})


def backfill_ohlcv(
    exchange: ccxt.Exchange,
    store: HistoricalOHLCVStore,
    symbol: str,
    timeframe: str,
    dry_run: bool,
    years: float = 2.0,
    market_type: str = "perp",
) -> int:
    """Fetch and store OHLCV candles from the last stored ts to now."""
    last_ts = store.get_latest_ts(symbol, market_type, timeframe)
    if last_ts is None:
        # Start from `years` ago if DB is empty
        since_ms = int((datetime.now(timezone.utc).timestamp() - years * 365 * 24 * 3600) * 1000)
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
            n = store.upsert_ohlcv(symbol, market_type, timeframe, candles)
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
    years: float = 2.0,
) -> int:
    """Fetch and store funding rates from the last stored ts to now."""
    last_ts = store.get_latest_ts(symbol)
    if last_ts is None:
        since_ms = int((datetime.now(timezone.utc).timestamp() - years * 365 * 24 * 3600) * 1000)
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
        description="Backfill perp OHLCV + funding rates (Gate.io default)"
    )
    parser.add_argument(
        "--exchange",
        default="gate",
        help="CCXT exchange ID with perp/swap support (default: gate)",
    )
    parser.add_argument(
        "--symbol",
        default=SYMBOL_PERP,
        help=(
            "Perpetual symbol, or comma-separated list "
            f"(default: {SYMBOL_PERP}; e.g. 'ETH/USDT:USDT,SOL/USDT:USDT')"
        ),
    )
    parser.add_argument(
        "--timeframe",
        default="1h",
        help="OHLCV timeframe, or comma-separated list (default: 1h; e.g. '15m,1h,4h,1d')",
    )
    parser.add_argument(
        "--years",
        type=float,
        default=2.0,
        help="How far back to start when no existing data (default: 2)",
    )
    parser.add_argument(
        "--market",
        default="perp",
        choices=["perp", "spot"],
        help="Market type for OHLCV storage (default: perp)",
    )
    parser.add_argument(
        "--hostname",
        default=None,
        help="Override exchange hostname (e.g. data-api.binance.vision)",
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

    symbols = [s.strip() for s in args.symbol.split(",") if s.strip()]
    timeframes = [t.strip() for t in args.timeframe.split(",") if t.strip()]

    db_path_kwarg = {"db_path": args.db} if args.db else {}

    ohlcv_store = HistoricalOHLCVStore(**db_path_kwarg, exchange_id=args.exchange)
    funding_store = FundingRateStore(**db_path_kwarg, exchange_id=args.exchange)

    print(f"\n=== Perp Backfill ===")
    print(f"  Exchange  : {args.exchange}")
    print(f"  Symbols   : {symbols}")
    print(f"  Timeframes: {timeframes}")
    print(f"  Years     : {args.years}")
    print(f"  Dry run   : {args.dry_run}")
    print(f"  Started   : {datetime.now(tz=timezone.utc).isoformat()}\n")

    exchange = make_exchange(args.exchange, hostname=args.hostname)

    total_ohlcv = 0
    total_funding = 0

    # 1. Backfill OHLCV for every symbol × timeframe
    for sym in symbols:
        for tf in timeframes:
            print(f"[OHLCV] {sym} {tf} ({args.market}) ...")
            total_ohlcv += backfill_ohlcv(
                exchange, ohlcv_store, sym, tf, args.dry_run,
                years=args.years, market_type=args.market,
            )
    print(f"  Total OHLCV rows written: {total_ohlcv}\n")

    # 2. Backfill funding rates once per symbol (perp only)
    if not args.skip_funding and args.market == "perp":
        for sym in symbols:
            print(f"[FUNDING] {sym} ...")
            total_funding += backfill_funding_rates(
                exchange, funding_store, sym, args.dry_run, years=args.years
            )
        print(f"  Total funding rows written: {total_funding}\n")
    else:
        print("[FUNDING] Skipping funding rates (--skip-funding)\n")

    print("Done.")


if __name__ == "__main__":
    main()
