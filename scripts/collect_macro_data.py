"""
Macro data collection script for GitHub Actions.

Fetches daily OHLCV for macro indicators via yfinance and saves to
data/macro/{symbol}_1d.json in the same format used by collect_crypto_data.py.

Symbols collected:
  ^DXY   — US Dollar Index
  GC=F   — Gold futures (front month)
  ^TNX   — 10-Year Treasury yield (%)
  ^VIX   — CBOE Volatility Index

Usage:
    python scripts/collect_macro_data.py
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# yfinance ticker → output filename stem
MACRO_SYMBOLS = {
    "^DXY": "DXY",
    "GC=F": "GOLD",
    "^TNX": "TNX",
    "^VIX": "VIX",
}

# How many days of history to keep in the JSON files
HISTORY_DAYS = 365

OUTPUT_DIR = Path(__file__).parent.parent / "data" / "macro"


# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------

def fetch_macro_ohlcv(yf_symbol: str, period: str = "1y", interval: str = "1d") -> list[dict]:
    """Fetch OHLCV from yfinance and return as a list of candle dicts."""
    try:
        import yfinance as yf
    except ImportError:
        print("yfinance is not installed. Run: pip install yfinance", file=sys.stderr)
        sys.exit(1)

    ticker = yf.Ticker(yf_symbol)
    df = ticker.history(period=period, interval=interval, auto_adjust=True)

    if df.empty:
        print(f"[macro] No data for {yf_symbol}", file=sys.stderr)
        return []

    candles = []
    for ts, row in df.iterrows():
        # Normalise timezone to UTC
        if hasattr(ts, "tzinfo") and ts.tzinfo is not None:
            ts_utc = ts.astimezone(timezone.utc)
        else:
            ts_utc = ts.replace(tzinfo=timezone.utc)

        candles.append({
            "timestamp": int(ts_utc.timestamp() * 1000),  # ms, consistent with crypto format
            "datetime": ts_utc.isoformat(),
            "open": float(row["Open"]) if not _isnan(row["Open"]) else None,
            "high": float(row["High"]) if not _isnan(row["High"]) else None,
            "low": float(row["Low"]) if not _isnan(row["Low"]) else None,
            "close": float(row["Close"]) if not _isnan(row["Close"]) else None,
            "volume": float(row["Volume"]) if not _isnan(row["Volume"]) else None,
        })

    return candles


def _isnan(v) -> bool:
    try:
        import math
        return math.isnan(float(v))
    except (TypeError, ValueError):
        return v is None


def load_existing(path: Path) -> dict:
    """Load existing JSON file or return empty structure."""
    if path.exists():
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            pass
    return {"symbol": path.stem, "timeframe": "1d", "ohlcv": []}


def merge_candles(existing: list[dict], new: list[dict]) -> list[dict]:
    """Merge candle lists deduped by timestamp, sorted ascending."""
    by_ts = {c["timestamp"]: c for c in existing}
    for c in new:
        by_ts[c["timestamp"]] = c
    return sorted(by_ts.values(), key=lambda c: c["timestamp"])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    now_utc = datetime.now(timezone.utc).isoformat()
    cutoff_ms = (datetime.now(timezone.utc).timestamp() - HISTORY_DAYS * 86_400) * 1000

    errors: list[str] = []

    for yf_symbol, stem in MACRO_SYMBOLS.items():
        print(f"[macro] Fetching {yf_symbol} → {stem}_1d.json")
        try:
            candles = fetch_macro_ohlcv(yf_symbol, period="2y", interval="1d")
        except Exception as exc:
            print(f"[macro] ERROR fetching {yf_symbol}: {exc}", file=sys.stderr)
            errors.append(yf_symbol)
            continue

        if not candles:
            errors.append(yf_symbol)
            continue

        out_path = OUTPUT_DIR / f"{stem}_1d.json"
        existing_data = load_existing(out_path)
        merged = merge_candles(existing_data.get("ohlcv", []), candles)

        # Trim to HISTORY_DAYS
        merged = [c for c in merged if c["timestamp"] >= cutoff_ms]

        payload = {
            "symbol": stem,
            "yfinance_ticker": yf_symbol,
            "timeframe": "1d",
            "fetched_at": now_utc,
            "candle_count": len(merged),
            "ohlcv": merged,
        }

        with open(out_path, "w") as f:
            json.dump(payload, f, indent=2)

        print(f"[macro] Saved {len(merged)} candles → {out_path}")

    if errors:
        print(f"\n[macro] WARNING: Failed to fetch: {', '.join(errors)}", file=sys.stderr)
        sys.exit(1)

    print("\n[macro] Done.")


if __name__ == "__main__":
    main()
