"""
Backfill on-chain metrics into onchain_metrics.db.

Uses CoinGecko public API (free, no key required) to download historical
market_cap, price, and volume data. Derives NVT proxy from the fetched data.

If GLASSNODE_API_KEY is set in the environment, also fetches true MVRV and
NVT from Glassnode.

Usage:
    poetry run python scripts/backfill_onchain.py
    poetry run python scripts/backfill_onchain.py --asset ETH --start 2022-01-01

CoinGecko rate limits:
  - Free tier: ~50 req/min (no API key)
  - Demo tier: ~500 req/min (with x-cg-demo-api-key header)
"""

import argparse
import sys
import time
from datetime import datetime, timezone

# Ensure project root is on the path when run directly
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.data.onchain_store import OnchainMetricStore


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backfill on-chain metrics")
    p.add_argument("--asset", default="BTC", help="Asset symbol (BTC, ETH, SOL) [default: BTC]")
    p.add_argument(
        "--start",
        default="2022-01-01",
        help="Start date YYYY-MM-DD [default: 2022-01-01]",
    )
    p.add_argument(
        "--end",
        default=None,
        help="End date YYYY-MM-DD [default: today]",
    )
    p.add_argument(
        "--incremental",
        action="store_true",
        help="Only fetch data newer than the latest stored row",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    end_date = args.end or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    start_ts_ms = int(
        datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc).timestamp() * 1000
    )
    end_ts_ms = int(
        datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc).timestamp() * 1000
    )

    store = OnchainMetricStore()

    if args.incremental:
        latest = store.get_latest_ts(args.asset, "market_cap")
        if latest is not None and latest > start_ts_ms:
            # Add one day buffer to avoid gaps
            start_ts_ms = latest - 86_400_000
            print(f"[backfill_onchain] Incremental mode: starting from {datetime.fromtimestamp(start_ts_ms / 1000, tz=timezone.utc).date()}")

    start_str = datetime.fromtimestamp(start_ts_ms / 1000, tz=timezone.utc).date()
    end_str = datetime.fromtimestamp(end_ts_ms / 1000, tz=timezone.utc).date()
    print(f"[backfill_onchain] Backfilling {args.asset} from {start_str} to {end_str}")

    # CoinGecko limits market_chart/range to ~90d per request for daily resolution
    # Split into 90-day chunks to avoid hitting pagination limits
    CHUNK_MS = 90 * 86_400_000
    total_rows = 0
    current_start = start_ts_ms

    while current_start < end_ts_ms:
        current_end = min(current_start + CHUNK_MS, end_ts_ms)
        chunk_start = datetime.fromtimestamp(current_start / 1000, tz=timezone.utc).date()
        chunk_end = datetime.fromtimestamp(current_end / 1000, tz=timezone.utc).date()
        print(f"[backfill_onchain]   chunk {chunk_start} → {chunk_end}")

        try:
            rows = store.backfill(args.asset, current_start, current_end)
            total_rows += rows
            print(f"[backfill_onchain]   → {rows} rows written")
        except Exception as exc:
            print(f"[backfill_onchain] ERROR: {exc}", file=sys.stderr)

        current_start = current_end
        # Respect CoinGecko free-tier rate limit between chunks
        if current_start < end_ts_ms:
            time.sleep(2.0)

    print(f"\n[backfill_onchain] Done. Total rows written: {total_rows}")


if __name__ == "__main__":
    main()
