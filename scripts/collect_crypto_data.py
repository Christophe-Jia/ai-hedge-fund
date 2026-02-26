"""
Crypto data collection script for GitHub Actions.

Usage:
    python scripts/collect_crypto_data.py

Fetches for each symbol:
  - Ticker snapshot (every run)
  - Daily OHLCV      → ohlcv_daily.json
  - 1-hour OHLCV     → ohlcv_1h.json    (last 200 candles, ~8 days)
  - 15-min OHLCV     → ohlcv_15m.json   (last 200 candles, ~2 days)
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SYMBOLS = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]

# Fallback chain: try each exchange in order until one works.
# Bybit and KuCoin have no US-IP restrictions on public endpoints.
EXCHANGE_FALLBACK = [
    os.environ.get("CCXT_EXCHANGE", "bybit"),
    "kucoin",
    "okx",
]

DATA_ROOT = Path(__file__).resolve().parents[1] / "data" / "crypto"

# Intraday timeframes to collect: (timeframe, fetch_limit, max_stored)
# fetch_limit: how many candles to pull each run (covers lookback + buffer)
# max_stored:  max candles kept in JSON (prevents unbounded growth)
INTRADAY_TIMEFRAMES = [
    ("1h",  200, 5000),   # 200 candles = ~8 days;  5000 stored ≈ 7 months
    ("15m", 200, 5000),   # 200 candles = ~2 days;  5000 stored ≈ 52 days
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def safe_symbol_dir(symbol: str) -> Path:
    """Return (and create) the data directory for a symbol, e.g. BTC-USDT."""
    name = symbol.replace("/", "-")
    d = DATA_ROOT / name
    d.mkdir(parents=True, exist_ok=True)
    return d


def load_json(path: Path) -> list:
    if path.exists():
        with path.open() as f:
            return json.load(f)
    return []


def save_json(path: Path, data) -> None:
    with path.open("w") as f:
        json.dump(data, f, indent=2)


def get_exchange():
    try:
        import ccxt
    except ImportError:
        print("ERROR: ccxt not installed", file=sys.stderr)
        sys.exit(1)

    for exchange_id in EXCHANGE_FALLBACK:
        try:
            exchange_cls = getattr(ccxt, exchange_id)
            exchange = exchange_cls(
                {
                    "apiKey": os.environ.get("CCXT_API_KEY", ""),
                    "secret": os.environ.get("CCXT_API_SECRET", ""),
                    "enableRateLimit": True,
                }
            )
            # Quick connectivity check
            exchange.fetch_ticker("BTC/USDT")
            print(f"Using exchange: {exchange_id}")
            return exchange
        except Exception as e:
            print(f"  [WARN] {exchange_id} not available: {e}", file=sys.stderr)

    print("ERROR: all exchanges failed", file=sys.stderr)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Collection functions
# ---------------------------------------------------------------------------

def collect_ticker(exchange, symbol: str) -> dict | None:
    """Fetch current ticker and return a timestamped record."""
    try:
        t = exchange.fetch_ticker(symbol)
        return {
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "symbol": t["symbol"],
            "last": t.get("last"),
            "bid": t.get("bid"),
            "ask": t.get("ask"),
            "volume_24h": t.get("baseVolume"),
            "change_pct_24h": t.get("percentage"),
        }
    except Exception as e:
        print(f"  [WARN] ticker fetch failed for {symbol}: {e}", file=sys.stderr)
        return None


def collect_ohlcv_daily(exchange, symbol: str) -> dict | None:
    """Fetch yesterday's completed daily candle."""
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe="1d", limit=2)
        if not ohlcv:
            return None
        candle = ohlcv[-2] if len(ohlcv) >= 2 else ohlcv[-1]
        ts_ms, open_, high, low, close, volume = candle
        return {
            "date": datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d"),
            "symbol": symbol,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "collected_at": datetime.now(tz=timezone.utc).isoformat(),
        }
    except Exception as e:
        print(f"  [WARN] daily OHLCV fetch failed for {symbol}: {e}", file=sys.stderr)
        return None


def collect_ohlcv_intraday(exchange, symbol: str, timeframe: str, limit: int) -> list[dict]:
    """Fetch the most recent `limit` intraday candles for a symbol.

    Returns a list of candle dicts with keys:
      timestamp (ISO), open, high, low, close, volume
    """
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        results = []
        for ts_ms, open_, high, low, close, volume in ohlcv:
            results.append({
                "timestamp": datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat(),
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
            })
        return results
    except Exception as e:
        print(f"  [WARN] {timeframe} OHLCV fetch failed for {symbol}: {e}", file=sys.stderr)
        return []


# ---------------------------------------------------------------------------
# Deduplication helpers
# ---------------------------------------------------------------------------

def deduplicate_ohlcv_daily(records: list) -> list:
    """Keep only the latest record per date."""
    seen: dict[str, dict] = {}
    for r in records:
        seen[r["date"]] = r
    return sorted(seen.values(), key=lambda x: x["date"])


def deduplicate_intraday(records: list, max_records: int) -> list:
    """Deduplicate by timestamp, keep chronological order, cap at max_records."""
    seen: dict[str, dict] = {}
    for r in records:
        seen[r["timestamp"]] = r
    sorted_records = sorted(seen.values(), key=lambda x: x["timestamp"])
    return sorted_records[-max_records:]


def deduplicate_tickers(records: list, max_records: int = 10_000) -> list:
    return records[-max_records:]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print(f"Starting data collection — fallback chain: {', '.join(EXCHANGE_FALLBACK)}")
    print(f"Symbols: {', '.join(SYMBOLS)}")
    print(f"Time: {datetime.now(tz=timezone.utc).isoformat()}")
    print()

    exchange = get_exchange()
    errors = 0

    for symbol in SYMBOLS:
        print(f"[{symbol}]")
        symbol_dir = safe_symbol_dir(symbol)

        # --- Ticker snapshot ---
        ticker_path = symbol_dir / "ticker.json"
        record = collect_ticker(exchange, symbol)
        if record:
            records = load_json(ticker_path)
            records.append(record)
            records = deduplicate_tickers(records)
            save_json(ticker_path, records)
            print(f"  ticker  → last={record['last']}, 24h change={record['change_pct_24h']}%")
        else:
            errors += 1

        # --- Daily OHLCV ---
        ohlcv_path = symbol_dir / "ohlcv_daily.json"
        candle = collect_ohlcv_daily(exchange, symbol)
        if candle:
            records = load_json(ohlcv_path)
            records.append(candle)
            records = deduplicate_ohlcv_daily(records)
            save_json(ohlcv_path, records)
            print(f"  daily   → date={candle['date']}, close={candle['close']}")
        else:
            errors += 1

        # --- Intraday OHLCV (1h and 15m) ---
        for timeframe, fetch_limit, max_stored in INTRADAY_TIMEFRAMES:
            intraday_path = symbol_dir / f"ohlcv_{timeframe}.json"
            candles = collect_ohlcv_intraday(exchange, symbol, timeframe, fetch_limit)
            if candles:
                existing = load_json(intraday_path)
                merged = deduplicate_intraday(existing + candles, max_stored)
                save_json(intraday_path, merged)
                latest = candles[-1]
                print(f"  {timeframe:<4}    → {len(merged)} candles stored, latest close={latest['close']} @ {latest['timestamp']}")
            else:
                errors += 1

        print()

    if errors:
        print(f"Completed with {errors} warning(s).", file=sys.stderr)
        sys.exit(1)
    else:
        print("All symbols collected successfully.")


if __name__ == "__main__":
    main()
