"""
Backfill on-chain metrics into onchain_metrics.db (v2).

Data source: CoinMetrics Community API (free, no key required).
  https://docs.coinmetrics.io/api/v4/

Verified working from this network (2026-09-14):
  - CoinMetrics community API       -> OK
  - blockchain.info charts API      -> BLOCKED (TLS handshake error)
  - Glassnode free endpoint         -> 401 (requires API key)

Metrics pulled (daily, 00:00 UTC):
  CoinMetrics metric   -> stored metric name
  AdrActCnt            -> active_addresses        (daily active addresses)
  AdrBalCnt            -> address_count           (addresses with balance > 0)
  TxCnt                -> tx_count                (daily transaction count)
  FlowInExNtv          -> exchange_inflow_native  (BTC into exchanges, native units)
  FlowOutExNtv         -> exchange_outflow_native (BTC out of exchanges, native units)
  CapMVRVCur           -> mvrv                    (real MVRV ratio)
  CapMrktCurUSD        -> market_cap              (USD market capitalisation)
  PriceUSD             -> price                   (daily USD close, full history)

Notes:
  - The community API pages NEWEST-FIRST; we follow next_page_url until exhausted.
  - Exchange flow values with "flash" status are preliminary estimates; they get
    corrected on re-runs (INSERT OR REPLACE).
  - Trading volume is NOT available on the community tier; CoinGecko free is
    limited to the past 365 days, so `volume`/`nvt_approx` only cover ~1 year.

Usage:
    poetry run python scripts/backfill_onchain_v2.py
    poetry run python scripts/backfill_onchain_v2.py --start 2019-01-01
    poetry run python scripts/backfill_onchain_v2.py --incremental
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone

# Ensure project root is on the path when run directly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import requests

from src.data.onchain_store import OnchainMetricStore

_CM_BASE = "https://community-api.coinmetrics.io/v4/timeseries/asset-metrics"

# CoinMetrics metric -> stored metric name
_CM_METRIC_MAP = {
    "AdrActCnt": "active_addresses",
    "AdrBalCnt": "address_count",
    "TxCnt": "tx_count",
    "FlowInExNtv": "exchange_inflow_native",
    "FlowOutExNtv": "exchange_outflow_native",
    "CapMVRVCur": "mvrv",
    "CapMrktCurUSD": "market_cap",
    "PriceUSD": "price",
}

_CM_ASSET_MAP = {"BTC": "btc", "ETH": "eth", "SOL": "sol"}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backfill on-chain metrics (CoinMetrics community)")
    p.add_argument("--asset", default="BTC", help="Asset symbol [default: BTC]")
    p.add_argument("--start", default="2019-01-01", help="Start date YYYY-MM-DD [default: 2019-01-01]")
    p.add_argument("--end", default=None, help="End date YYYY-MM-DD [default: today]")
    p.add_argument("--incremental", action="store_true", help="Resume from latest stored row")
    p.add_argument("--page-size", type=int, default=10000, help="CoinMetrics page size")
    return p.parse_args()


def _parse_cm_time(t: str) -> int:
    """'2018-01-30T00:00:00.000000000Z' -> epoch ms (UTC)."""
    dt = datetime.strptime(t[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def fetch_coinmetrics(
    asset: str,
    start_ts_ms: int,
    end_ts_ms: int,
    page_size: int = 10000,
) -> list[dict]:
    """
    Fetch all on-chain metrics for asset in [start, end) from CoinMetrics community API.

    Returns records: [{asset, metric, ts_ms, value}, ...]
    """
    cm_asset = _CM_ASSET_MAP.get(asset.upper())
    if not cm_asset:
        print(f"[backfill_onchain_v2] No CoinMetrics mapping for {asset}, skipping")
        return []

    start_str = datetime.fromtimestamp(start_ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = datetime.fromtimestamp(end_ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    url = _CM_BASE
    params = {
        "assets": cm_asset,
        "metrics": ",".join(_CM_METRIC_MAP.keys()),
        "frequency": "1d",
        "page_size": page_size,
        "start_time": start_str,
        "end_time": end_str,
    }

    records: list[dict] = []
    page = 0
    while url:
        resp = requests.get(url, params=params if page == 0 else None, timeout=60)
        resp.raise_for_status()
        payload = resp.json()

        if "error" in payload:
            print(f"[backfill_onchain_v2] API error: {payload['error']}", file=sys.stderr)
            break

        for row in payload.get("data", []):
            ts_ms = _parse_cm_time(row["time"])
            if not (start_ts_ms <= ts_ms < end_ts_ms):
                continue
            for cm_name, stored_name in _CM_METRIC_MAP.items():
                v = row.get(cm_name)
                if v is None:
                    continue
                try:
                    records.append({
                        "asset": asset.upper(),
                        "metric": stored_name,
                        "ts_ms": ts_ms,
                        "value": float(v),
                    })
                except (TypeError, ValueError):
                    continue

        page += 1
        url = payload.get("next_page_url")
        params = None
        if url:
            time.sleep(1.0)  # be polite to the free tier

    return records


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
        # Resume from the latest on-chain metric row (not price, which CoinGecko owns)
        latest = store.get_latest_ts(args.asset, "active_addresses")
        if latest is not None and latest > start_ts_ms:
            start_ts_ms = max(start_ts_ms, latest - 86_400_000)
            print(
                "[backfill_onchain_v2] Incremental: resuming from "
                f"{datetime.fromtimestamp(start_ts_ms / 1000, tz=timezone.utc).date()}"
            )

    start_str = datetime.fromtimestamp(start_ts_ms / 1000, tz=timezone.utc).date()
    end_str = datetime.fromtimestamp(end_ts_ms / 1000, tz=timezone.utc).date()
    print(f"[backfill_onchain_v2] Fetching {args.asset} on-chain metrics {start_str} → {end_str}")

    try:
        records = fetch_coinmetrics(args.asset, start_ts_ms, end_ts_ms, args.page_size)
    except Exception as exc:
        print(f"[backfill_onchain_v2] Fetch failed: {exc}", file=sys.stderr)
        sys.exit(1)

    if not records:
        print("[backfill_onchain_v2] No records returned — nothing to write.")
        sys.exit(1)

    written = store.upsert(records)

    # Summary per metric
    by_metric: dict[str, int] = {}
    for r in records:
        by_metric[r["metric"]] = by_metric.get(r["metric"], 0) + 1
    print(f"[backfill_onchain_v2] Wrote {written} rows:")
    for metric in sorted(by_metric):
        print(f"    {metric:<26} {by_metric[metric]:>6} points")


if __name__ == "__main__":
    main()
