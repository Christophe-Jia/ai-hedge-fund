"""
Polymarket CLOB price tick collector.

Polls the public CLOB /prices-history?fidelity=1 endpoint every 30 seconds
for active geopolitical prediction markets and persists tick-level price
history to a local SQLite database.

No API key required — the fidelity=1 endpoint is public.

Market discovery: scans Gamma API for active markets matching geopolitical
keywords (Iran, Israel, war, nuclear, Russia, Ukraine, etc.) and refreshes
the market list every MARKET_REFRESH_INTERVAL seconds.

Usage:
    poetry run python scripts/collect_polymarket_ticks.py
    poetry run python scripts/collect_polymarket_ticks.py --interval 60 --keywords "Iran,Israel,war"
    poetry run python scripts/collect_polymarket_ticks.py --db data/my_ticks.db

Background:
    nohup poetry run python scripts/collect_polymarket_ticks.py > logs/polymarket_ticks.log 2>&1 &
"""

from __future__ import annotations

import argparse
import os
import signal
import sys
import time
from datetime import datetime, timezone

import requests

# Ensure project root is on path when run directly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.data.polymarket_tick_store import PolymarketTickStore

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CLOB_BASE = "https://clob.polymarket.com"
GAMMA_BASE = "https://gamma-api.polymarket.com"

DEFAULT_KEYWORDS = [
    "iran", "israel", "war", "nuclear", "russia", "ukraine",
    "missile", "attack", "sanctions", "ceasefire", "invasion",
    "conflict", "nato", "military",
]

MARKET_REFRESH_INTERVAL = 3600  # refresh market list every hour (seconds)
REQUEST_TIMEOUT = 10  # seconds per HTTP request
MAX_MARKETS = 50       # cap to avoid hammering the API


# ---------------------------------------------------------------------------
# Market discovery
# ---------------------------------------------------------------------------

def fetch_active_geopolitical_markets(keywords: list[str]) -> list[dict]:
    """
    Query the Gamma API for open (active=true, closed=false) markets whose
    question contains any keyword (case-insensitive local filter).

    Returns list of dicts with keys: token_id, condition_id, question.
    """
    import json as _json

    markets: list[dict] = []
    seen_token_ids: set[str] = set()

    # Paginate through all open markets; filter locally by keyword
    offset = 0
    page_size = 100
    kw_lower = [k.lower() for k in keywords]

    while len(markets) < MAX_MARKETS:
        try:
            resp = requests.get(
                f"{GAMMA_BASE}/markets",
                params={
                    "active": "true",
                    "closed": "false",
                    "limit": page_size,
                    "offset": offset,
                },
                timeout=REQUEST_TIMEOUT,
            )
        except requests.RequestException as e:
            print(f"  [WARN] Gamma API error (offset={offset}): {e}")
            break

        if resp.status_code != 200:
            print(f"  [WARN] Gamma API HTTP {resp.status_code} (offset={offset})")
            break

        data = resp.json()
        items = data if isinstance(data, list) else data.get("data", [])
        if not items:
            break  # no more pages

        matched_this_page = 0
        for m in items:
            question = m.get("question", "")
            if not any(kw in question.lower() for kw in kw_lower):
                continue

            tokens = m.get("clobTokenIds", [])
            if isinstance(tokens, str):
                try:
                    tokens = _json.loads(tokens)
                except Exception:
                    tokens = [tokens]

            condition_id = m.get("conditionId", "")
            for token_id in tokens:
                if token_id and token_id not in seen_token_ids:
                    seen_token_ids.add(token_id)
                    markets.append(
                        {
                            "token_id": token_id,
                            "condition_id": condition_id,
                            "question": question,
                        }
                    )
                    matched_this_page += 1

            if len(markets) >= MAX_MARKETS:
                break

        offset += page_size

        # If page was smaller than page_size, we've reached the last page
        if len(items) < page_size:
            break

    return markets[:MAX_MARKETS]


# ---------------------------------------------------------------------------
# Tick fetching
# ---------------------------------------------------------------------------

def fetch_price_ticks(
    token_id: str, start_ts: int | None = None
) -> list[tuple[int, float]]:
    """
    Fetch fidelity=1 price history for a token from the CLOB API.

    Returns list of (ts_seconds, price) tuples, filtered to ts > start_ts
    if provided.

    The CLOB /prices-history endpoint requires either interval= or startTs+endTs.
    We use interval=max on first fetch (no prior data), then startTs+endTs for
    incremental updates.
    """
    now = int(time.time())
    params: dict = {"market": token_id, "fidelity": "1"}

    if start_ts is None:
        # First fetch — pull all available history
        params["interval"] = "max"
    else:
        # Incremental fetch: request only newer data
        params["startTs"] = str(start_ts)
        params["endTs"] = str(now)

    try:
        resp = requests.get(
            f"{CLOB_BASE}/prices-history",
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
    except requests.RequestException as e:
        print(f"  [WARN] CLOB request error for {token_id[:12]}...: {e}")
        return []

    if resp.status_code == 404:
        # Market may be closed / expired
        return []
    if resp.status_code != 200:
        print(f"  [WARN] CLOB HTTP {resp.status_code} for {token_id[:12]}...")
        return []

    try:
        body = resp.json()
    except Exception:
        return []

    history = body.get("history", [])
    if not history:
        return []

    ticks: list[tuple[int, float]] = []
    for entry in history:
        t = entry.get("t") or entry.get("timestamp")
        p = entry.get("p") or entry.get("price")
        if t is None or p is None:
            continue
        ts = int(t)
        if start_ts is not None and ts <= start_ts:
            continue
        ticks.append((ts, float(p)))

    return ticks


# ---------------------------------------------------------------------------
# Main polling loop
# ---------------------------------------------------------------------------

def run_collector(
    keywords: list[str],
    poll_interval: int,
    db_path: str,
) -> None:
    store = PolymarketTickStore(db_path)

    print(f"\n=== Polymarket Tick Collector ===")
    print(f"  Keywords  : {', '.join(keywords)}")
    print(f"  Interval  : {poll_interval}s")
    print(f"  DB path   : {db_path}")
    print(f"  Started   : {datetime.now(tz=timezone.utc).isoformat()}")
    print(f"  Stop      : Ctrl+C or SIGTERM\n")

    # Graceful shutdown
    _stop = False

    def _handle_signal(signum, frame):
        nonlocal _stop
        _stop = True
        print("\n[collector] Shutdown signal received ...")

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    markets: list[dict] = []
    last_market_refresh = 0
    iteration = 0

    while not _stop:
        now = time.time()

        # Refresh market list periodically
        if now - last_market_refresh > MARKET_REFRESH_INTERVAL or not markets:
            print(f"[{datetime.now(tz=timezone.utc).strftime('%H:%M:%S')}] Refreshing market list ...")
            markets = fetch_active_geopolitical_markets(keywords)
            last_market_refresh = now

            # Upsert market metadata
            for m in markets:
                store.upsert_market(
                    m["token_id"],
                    m.get("condition_id"),
                    m.get("question"),
                    last_seen=int(now),
                )
            print(f"  Tracking {len(markets)} token(s) across {len(set(m['condition_id'] for m in markets))} markets")
            if markets:
                for m in markets[:5]:
                    print(f"    {m['token_id'][:16]}...  {m['question'][:60]}")
                if len(markets) > 5:
                    print(f"    ... and {len(markets) - 5} more")

        iteration += 1
        ts_label = datetime.now(tz=timezone.utc).strftime("%H:%M:%S")
        total_new = 0

        for m in markets:
            if _stop:
                break
            token_id = m["token_id"]
            last_ts = store.get_latest_ts(token_id)
            new_ticks = fetch_price_ticks(token_id, start_ts=last_ts)
            if new_ticks:
                n = store.upsert_ticks(token_id, new_ticks)
                total_new += n

        total_stored = store.get_total_tick_count()
        print(
            f"[{ts_label}] iter={iteration:>5}  new_ticks={total_new:>5}  "
            f"total_stored={total_stored:>10,}  markets={len(markets)}"
        )

        # Sleep with periodic _stop checks
        deadline = time.time() + poll_interval
        while time.time() < deadline and not _stop:
            time.sleep(1)

    print("\n[collector] Shutdown complete.")
    total_stored = store.get_total_tick_count()
    print(f"  Total ticks in DB: {total_stored:,}")
    tracked = store.list_markets()
    print(f"  Tracked markets  : {len(tracked)}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Polymarket CLOB fidelity=1 tick collector"
    )
    parser.add_argument(
        "--keywords",
        default=",".join(DEFAULT_KEYWORDS),
        help="Comma-separated keywords to filter markets (default: geopolitical terms)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=30,
        help="Poll interval in seconds (default: 30)",
    )
    parser.add_argument(
        "--db",
        default=os.path.join(
            os.path.dirname(__file__), "..", "data", "polymarket_ticks.db"
        ),
        help="SQLite database path (default: data/polymarket_ticks.db)",
    )
    args = parser.parse_args()

    keywords = [k.strip().lower() for k in args.keywords.split(",") if k.strip()]
    db_path = os.path.abspath(args.db)

    os.makedirs(os.path.join(os.path.dirname(__file__), "..", "logs"), exist_ok=True)

    run_collector(
        keywords=keywords,
        poll_interval=args.interval,
        db_path=db_path,
    )


if __name__ == "__main__":
    main()
