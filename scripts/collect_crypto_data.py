"""
Crypto data collection script for GitHub Actions.

Usage:
    python scripts/collect_crypto_data.py

Fetches ticker snapshots and daily OHLCV for configured symbols,
appending results to data/crypto/<SYMBOL>/ticker.json and
data/crypto/<SYMBOL>/ohlcv_daily.json.
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SYMBOLS = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
EXCHANGE_ID = os.environ.get("CCXT_EXCHANGE", "binance")
DATA_ROOT = Path(__file__).resolve().parents[1] / "data" / "crypto"


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

    exchange_cls = getattr(ccxt, EXCHANGE_ID)
    return exchange_cls(
        {
            "apiKey": os.environ.get("CCXT_API_KEY", ""),
            "secret": os.environ.get("CCXT_API_SECRET", ""),
            "enableRateLimit": True,
        }
    )


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
    """Fetch yesterday's completed daily candle and return a record."""
    try:
        # Fetch last 2 candles; use index -2 (yesterday's completed candle)
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe="1d", limit=2)
        if not ohlcv:
            return None
        # Use the most recently *completed* candle (second-to-last or last if only one)
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
        print(f"  [WARN] OHLCV fetch failed for {symbol}: {e}", file=sys.stderr)
        return None


def deduplicate_ohlcv(records: list) -> list:
    """Keep only the latest record per date."""
    seen: dict[str, dict] = {}
    for r in records:
        seen[r["date"]] = r
    return sorted(seen.values(), key=lambda x: x["date"])


def deduplicate_tickers(records: list, max_records: int = 10_000) -> list:
    """Keep last N ticker snapshots (prevent unbounded growth)."""
    return records[-max_records:]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print(f"Starting data collection — exchange: {EXCHANGE_ID}")
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
            records = deduplicate_ohlcv(records)
            save_json(ohlcv_path, records)
            print(f"  ohlcv   → date={candle['date']}, close={candle['close']}")
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
