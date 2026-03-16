"""
Fetch BTC/USDT and ETH/USDT hourly OHLCV from Binance via CCXT (public endpoint, no key).
Outputs:
  data/polymarket/btc_1h.csv
  data/polymarket/eth_1h.csv

Usage:
  poetry run python scripts/polymarket/fetch_crypto_prices.py
"""

import csv
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import ccxt
except ImportError as exc:
    raise SystemExit(
        "ccxt not installed. Run: poetry add ccxt\n"
        "Or: pip install ccxt"
    ) from exc

DATA_DIR = Path(__file__).parent.parent.parent / "data" / "polymarket"

# 2024-12-01 → 2025-02-01
START_DT = datetime(2024, 12, 1, tzinfo=timezone.utc)
END_DT = datetime(2025, 2, 1, tzinfo=timezone.utc)
START_MS = int(START_DT.timestamp() * 1000)
END_MS = int(END_DT.timestamp() * 1000)

SYMBOLS = [
    ("BTC/USDT", "btc_1h.csv"),
    ("ETH/USDT", "eth_1h.csv"),
]

# 500 candles × 1h = 500h per batch; loop until we cover the range
BATCH_LIMIT = 500


def fetch_ohlcv(exchange: ccxt.Exchange, symbol: str) -> list[list]:
    """Fetch all 1h OHLCV candles between START_MS and END_MS."""
    all_candles: list[list] = []
    since = START_MS

    while since < END_MS:
        print(f"  fetching {symbol} from {datetime.utcfromtimestamp(since/1000).strftime('%Y-%m-%d %H:%M')} UTC …")
        try:
            candles = exchange.fetch_ohlcv(symbol, timeframe="1h", since=since, limit=BATCH_LIMIT)
        except ccxt.BaseError as exc:
            print(f"  [error] {exc}")
            break

        if not candles:
            break

        # filter to [START_MS, END_MS)
        candles = [c for c in candles if c[0] < END_MS]
        all_candles.extend(candles)

        last_ts = candles[-1][0]
        if last_ts <= since:
            break  # no progress
        since = last_ts + 3_600_000  # advance by 1h in ms

        time.sleep(0.2)  # rate-limit courtesy

    # deduplicate by timestamp
    seen: set[int] = set()
    deduped: list[list] = []
    for c in all_candles:
        if c[0] not in seen:
            seen.add(c[0])
            deduped.append(c)

    deduped.sort(key=lambda c: c[0])
    return deduped


def save_csv(candles: list[list], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "datetime_utc", "open", "high", "low", "close", "volume"])
        for c in candles:
            ts, o, h, l, cl, vol = c
            dt = datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M")
            writer.writerow([ts, dt, o, h, l, cl, vol])
    print(f"  saved {len(candles)} candles → {path}")


def compute_intrabar_vol(candles: list[list]) -> None:
    """Print basic stats about intrabar volatility."""
    import statistics
    vols = [(c[2] - c[3]) / c[1] * 100 for c in candles if c[1] > 0]  # (high-low)/open %
    if vols:
        print(f"    intrabar vol (high-low)/open: mean={statistics.mean(vols):.3f}% "
              f"max={max(vols):.3f}% min={min(vols):.3f}%")


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    exchange = ccxt.binance({"enableRateLimit": True})
    print(f"Exchange: {exchange.id} (public, no key needed)")
    print(f"Period: {START_DT.date()} → {END_DT.date()} (1h candles)\n")

    for symbol, filename in SYMBOLS:
        out_path = DATA_DIR / filename
        if out_path.exists():
            print(f"{symbol}: already cached at {out_path} — skipping")
            continue

        print(f"Fetching {symbol} …")
        candles = fetch_ohlcv(exchange, symbol)
        print(f"  total candles: {len(candles)}")
        compute_intrabar_vol(candles)
        save_csv(candles, out_path)
        print()

    print("Done. Crypto price files:")
    for _, filename in SYMBOLS:
        path = DATA_DIR / filename
        if path.exists():
            lines = path.read_text().count("\n") - 1  # subtract header
            print(f"  {path.name}: {lines} rows")


if __name__ == "__main__":
    main()
