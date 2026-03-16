"""
Seed BTC historical data into the local SQLite store.

Downloads:
  - BTC/USDT spot: daily + hourly OHLCV (N years)
  - BTC/USDT:USDT perp: daily + hourly OHLCV (N years)
  - BTC/USDT:USDT funding rate history

Data source: data.binance.vision (Binance public S3 data repository).
  - Spot klines: https://data.binance.vision/data/spot/monthly/klines/
  - Perp klines: https://data.binance.vision/data/futures/um/monthly/klines/
  - Funding rates: https://data.binance.vision/data/futures/um/monthly/fundingRate/

For recent data (current month), falls back to data-api.binance.vision
REST API which is also reachable without TLS issues.

Usage:
    poetry run python scripts/seed_btc_history.py --years 3
    poetry run python scripts/seed_btc_history.py --years 3 --db path/to/custom.db
"""

import argparse
import io
import sys
import os
import time
import zipfile
import csv
from datetime import datetime, timezone, timedelta

import requests

# Ensure project root is on the path when run directly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.data.historical_store import HistoricalOHLCVStore
from src.data.funding_rates import FundingRateStore

# ---------------------------------------------------------------------------
# Binance vision S3 base URLs (publicly accessible, no auth, good TLS)
# ---------------------------------------------------------------------------

_VISION_SPOT_BASE = "https://data.binance.vision/data/spot/monthly/klines"
_VISION_PERP_BASE = "https://data.binance.vision/data/futures/um/monthly/klines"
_VISION_FUND_BASE = "https://data.binance.vision/data/futures/um/monthly/fundingRate"

# REST API fallback for current (incomplete) month — spot only; TLS OK
_VISION_API_KLINES = "https://data-api.binance.vision/api/v3/klines"

_SESSION = requests.Session()
_SESSION.headers["User-Agent"] = "btc-seed/1.0"
_RATE_LIMIT_S = 0.25


def _ts_ms(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


# ---------------------------------------------------------------------------
# Monthly zip download helpers
# ---------------------------------------------------------------------------

def _download_zip_csv(url: str) -> list[list[str]] | None:
    """Download a zip from url and return the parsed CSV rows (or None on 404)."""
    resp = _SESSION.get(url, timeout=60)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        csv_name = zf.namelist()[0]
        with zf.open(csv_name) as f:
            reader = csv.reader(io.TextIOWrapper(f))
            rows = list(reader)
    return rows


def fetch_spot_monthly_ohlcv(symbol_bare: str, interval: str, year: int, month: int) -> list[list]:
    """Return [[ts_ms, open, high, low, close, volume], ...] for one month of spot data."""
    url = f"{_VISION_SPOT_BASE}/{symbol_bare}/{interval}/{symbol_bare}-{interval}-{year}-{month:02d}.zip"
    rows = _download_zip_csv(url)
    if rows is None:
        return []
    result = []
    for row in rows:
        if not row or not row[0].isdigit():
            continue  # skip header or empty
        result.append([int(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5])])
    return result


def fetch_perp_monthly_ohlcv(symbol_bare: str, interval: str, year: int, month: int) -> list[list]:
    """Return [[ts_ms, open, high, low, close, volume], ...] for one month of perp data."""
    url = f"{_VISION_PERP_BASE}/{symbol_bare}/{interval}/{symbol_bare}-{interval}-{year}-{month:02d}.zip"
    rows = _download_zip_csv(url)
    if rows is None:
        return []
    result = []
    for row in rows:
        if not row or not row[0].isdigit():
            continue
        result.append([int(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5])])
    return result


def fetch_funding_monthly(symbol_bare: str, year: int, month: int) -> list[tuple[int, float]]:
    """Return [(ts_ms, rate), ...] for one month of funding rate data."""
    url = f"{_VISION_FUND_BASE}/{symbol_bare}/{symbol_bare}-fundingRate-{year}-{month:02d}.zip"
    rows = _download_zip_csv(url)
    if rows is None:
        return []
    result = []
    for row in rows:
        if not row or not row[0].isdigit():
            continue
        result.append((int(row[0]), float(row[2])))  # cols: symbol, fundingTime(ms?), fundingRate
    # Some files use col 1 as timestamp; detect by value magnitude
    if result and result[0][0] < 1_000_000_000_000:
        # Looks like seconds, not ms — but Binance uses ms, so this shouldn't happen
        result = [(ts * 1000, rate) for ts, rate in result]
    return result


# ---------------------------------------------------------------------------
# REST API fallback for recent / current-month data (spot only via vision API)
# ---------------------------------------------------------------------------

def fetch_spot_rest_ohlcv(symbol_bare: str, interval: str, start_ts: int, end_ts: int) -> list[list]:
    """Fetch recent spot OHLCV from data-api.binance.vision REST (no TLS issues)."""
    all_rows: list[list] = []
    since = start_ts
    while since < end_ts:
        params = {
            "symbol": symbol_bare,
            "interval": interval,
            "startTime": since,
            "endTime": end_ts - 1,
            "limit": 1000,
        }
        resp = _SESSION.get(_VISION_API_KLINES, params=params, timeout=30)
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        rows = [[int(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5])]
                for c in batch if int(c[0]) < end_ts]
        all_rows.extend(rows)
        if len(batch) < 1000:
            break
        since = int(batch[-1][0]) + 1
        time.sleep(_RATE_LIMIT_S)
    return all_rows


# ---------------------------------------------------------------------------
# Month iteration helper
# ---------------------------------------------------------------------------

def _months_in_range(start_ts: int, end_ts: int):
    """Yield (year, month) tuples covering [start_ts, end_ts)."""
    start_dt = datetime.fromtimestamp(start_ts / 1000, tz=timezone.utc).replace(day=1)
    end_dt = datetime.fromtimestamp(end_ts / 1000, tz=timezone.utc)
    current = start_dt
    while current <= end_dt:
        yield current.year, current.month
        # advance one month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------

def seed_ohlcv(
    store: HistoricalOHLCVStore,
    symbol: str,
    market_type: str,
    timeframe: str,
    start_ts: int,
    end_ts: int,
    label: str,
) -> None:
    """Download and store OHLCV data, resuming from the last stored bar."""
    latest = store.get_latest_ts(symbol, market_type, timeframe)
    fetch_from = max(start_ts, latest + 1) if latest is not None else start_ts

    if fetch_from >= end_ts:
        print(f"  [{label}] Already up-to-date.")
        return

    from_dt = datetime.fromtimestamp(fetch_from / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    to_dt = datetime.fromtimestamp(end_ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    print(f"  [{label}] Fetching {symbol} {market_type} {timeframe} from {from_dt} to {to_dt} ...")

    # Binance uses no-slash symbol (BTCUSDT); perp strips the ":USDT" suffix
    symbol_bare = symbol.split(":")[0].replace("/", "")
    total_stored = 0

    now_dt = datetime.utcnow().replace(tzinfo=timezone.utc)
    current_year, current_month = now_dt.year, now_dt.month

    for year, month in _months_in_range(fetch_from, end_ts):
        month_start = _ts_ms(datetime(year, month, 1, tzinfo=timezone.utc))
        # Skip months entirely before fetch_from
        if month_start + 31 * 86400 * 1000 <= fetch_from:
            continue

        is_current_month = (year == current_year and month == current_month)

        try:
            if is_current_month:
                # Current month has no complete zip yet — use REST API for spot,
                # skip for perp (acceptable gap; perp zip lags by ~1 day)
                if market_type == "spot":
                    rows = fetch_spot_rest_ohlcv(symbol_bare, timeframe, fetch_from, end_ts)
                else:
                    # For perp, try previous-month zip data; current month incomplete
                    rows = fetch_perp_monthly_ohlcv(symbol_bare, timeframe, year, month)
            else:
                if market_type == "spot":
                    rows = fetch_spot_monthly_ohlcv(symbol_bare, timeframe, year, month)
                else:
                    rows = fetch_perp_monthly_ohlcv(symbol_bare, timeframe, year, month)

            # Filter to requested range
            rows = [r for r in rows if fetch_from <= r[0] < end_ts]
            if rows:
                n = store.upsert_ohlcv(symbol, market_type, timeframe, rows)
                total_stored += n
                print(f"    {year}-{month:02d}: {n} bars", flush=True)
            else:
                print(f"    {year}-{month:02d}: (no data)", flush=True)
        except Exception as exc:
            print(f"    {year}-{month:02d}: ERROR — {exc}", flush=True)

        time.sleep(_RATE_LIMIT_S)

    print(f"  [{label}] Total stored: {total_stored} bars.")


def seed_funding(
    fstore: FundingRateStore,
    symbol: str,
    start_ts: int,
    end_ts: int,
) -> None:
    """Download and store funding rate history, resuming from last stored."""
    latest = fstore.get_latest_ts(symbol)
    fetch_from = max(start_ts, latest + 1) if latest is not None else start_ts

    if fetch_from >= end_ts:
        print(f"  [funding] {symbol} Already up-to-date.")
        return

    from_dt = datetime.fromtimestamp(fetch_from / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    to_dt = datetime.fromtimestamp(end_ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    print(f"  [funding] Fetching {symbol} from {from_dt} to {to_dt} ...")

    symbol_bare = symbol.split(":")[0].replace("/", "")
    total_stored = 0

    for year, month in _months_in_range(fetch_from, end_ts):
        month_start = _ts_ms(datetime(year, month, 1, tzinfo=timezone.utc))
        if month_start + 31 * 86400 * 1000 <= fetch_from:
            continue
        try:
            rates = fetch_funding_monthly(symbol_bare, year, month)
            rates = [(ts, rate) for ts, rate in rates if fetch_from <= ts < end_ts]
            if rates:
                n = fstore.upsert_rates(symbol, rates)
                total_stored += n
                print(f"    {year}-{month:02d}: {n} records", flush=True)
            else:
                print(f"    {year}-{month:02d}: (no data)", flush=True)
        except Exception as exc:
            print(f"    {year}-{month:02d}: ERROR — {exc}", flush=True)
        time.sleep(_RATE_LIMIT_S)

    print(f"  [funding] Total stored: {total_stored} records.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Seed BTC historical OHLCV and funding rate data."
    )
    parser.add_argument(
        "--years", type=int, default=3,
        help="Number of years of history to download (default: 3)"
    )
    parser.add_argument(
        "--db", type=str, default=None,
        help="Path to the SQLite database file (default: data/btc_history.db)"
    )
    args = parser.parse_args()

    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    end_ts = _ts_ms(now)
    start_ts = _ts_ms(now - timedelta(days=args.years * 365))

    db_kwargs = {}
    if args.db:
        db_kwargs["db_path"] = args.db

    ohlcv_store = HistoricalOHLCVStore(**db_kwargs)
    funding_store = FundingRateStore(**db_kwargs)

    print(f"\n=== Seeding BTC history ({args.years} years) ===\n")

    # --- Spot OHLCV ---
    print("BTC/USDT Spot")
    seed_ohlcv(ohlcv_store, "BTC/USDT", "spot", "1d", start_ts, end_ts, "spot daily")
    seed_ohlcv(ohlcv_store, "BTC/USDT", "spot", "1h", start_ts, end_ts, "spot hourly")

    # --- Perpetual OHLCV ---
    print("\nBTC/USDT:USDT Perpetual")
    seed_ohlcv(ohlcv_store, "BTC/USDT:USDT", "perp", "1d", start_ts, end_ts, "perp daily")
    seed_ohlcv(ohlcv_store, "BTC/USDT:USDT", "perp", "1h", start_ts, end_ts, "perp hourly")

    # --- Funding rates (Binance USDT-M perps launched September 2019) ---
    print("\nBTC/USDT:USDT Funding Rates")
    binance_perp_launch_ts = _ts_ms(datetime(2019, 9, 1, tzinfo=timezone.utc))
    funding_start = max(start_ts, binance_perp_launch_ts)
    seed_funding(funding_store, "BTC/USDT:USDT", funding_start, end_ts)

    print("\n=== Seeding complete ===\n")
    print("Run your backtest with:")
    print("  poetry run python scripts/run_btc_backtest.py")


if __name__ == "__main__":
    main()
