"""
One-time backfill of Polymarket 1-minute price history for markets that are
still open, using the CLOB startTs-only full-history capability.

Why now: CLOB /prices-history returns NOTHING for closed markets — ticks not
captured before a market closes are lost forever. For still-open markets a
startTs-only request (no endTs; startTs+endTs pairs longer than a few days
are rejected with HTTP 400) fetches full history back to creation at true
1-minute granularity (~13 months). This window shrinks as markets close.

Scope (see reports/polymarket_mining.json):
  - Primary: BTC/ETH deadline price market families under tag_slug=crypto
    ("What price will Bitcoin/Ethereum hit in 2026?", "When will Bitcoin hit
    $150k?", "Bitcoin all time high by ___?", ...).
  - Secondary: open fed-rates markets above a volume floor (the "Fed Decision
    in September?" family closes 2026-09-16 — its history is lost otherwise).

Binary Yes/No markets: only the YES token is fetched. Mining finding
(yes_no_complement): YES+NO mids sum to exactly 1.0 at every timestamp —
the NO series is a perfect mirror and adds no information. Metadata for
both tokens is recorded; the live collector continues to poll both tokens.

Polite rate limiting: sequential requests, REQUEST_SLEEP seconds apart.
Failed markets are logged and skipped.

Usage:
    poetry run python scripts/backfill_polymarket_history.py --dry-run
    poetry run python scripts/backfill_polymarket_history.py
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone

import requests

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from collect_polymarket_ticks import (  # noqa: E402
    CLOB_BASE,
    REQUEST_TIMEOUT,
    fetch_tag_markets,
    log,
    migrate_markets_table,
    now_utc,
    parse_iso,
    upsert_market_meta,
)
from src.data.polymarket_tick_store import PolymarketTickStore  # noqa: E402

BACKFILL_START_TS = 1704067200  # 2024-01-01: earlier than any market creation
REQUEST_SLEEP = 1.5             # seconds between CLOB requests
FED_MIN_VOLUME = 100_000        # USD volume floor for fed-rates markets
BTC_ETH_MIN_VOLUME = 100_000    # USD volume floor (excludes weekly micro-cap range markets)
CHUNK_SIZE = 100_000            # ticks per DB upsert batch


def is_btc_eth_price_market(question: str) -> bool:
    q = question.lower()
    has_asset = "bitcoin" in q or "ethereum" in q
    has_price = "$" in q or "price" in q or "all time high" in q or "ath" in q
    return has_asset and has_price


def select_backfill_markets() -> tuple[list[dict], list[dict]]:
    """Return (btc_eth_markets, fed_markets) — market-level dicts."""
    now = now_utc()

    crypto_tokens = fetch_tag_markets("crypto")
    btc_eth: dict[str, dict] = {}
    for t in crypto_tokens:
        if is_btc_eth_price_market(t["question"]) and t["volume"] >= BTC_ETH_MIN_VOLUME:
            m = btc_eth.setdefault(
                t["condition_id"],
                {
                    "condition_id": t["condition_id"],
                    "question": t["question"],
                    "end_date": t["end_date"],
                    "volume": t["volume"],
                    "tokens": [],
                },
            )
            m["tokens"].append(t["token_id"])
            m["volume"] = max(m["volume"], t["volume"])

    fed_tokens = fetch_tag_markets("fed-rates")
    fed: dict[str, dict] = {}
    for t in fed_tokens:
        if t["volume"] < FED_MIN_VOLUME:
            continue
        m = fed.setdefault(
            t["condition_id"],
            {
                "condition_id": t["condition_id"],
                "question": t["question"],
                "end_date": t["end_date"],
                "volume": t["volume"],
                "tokens": [],
            },
        )
        m["tokens"].append(t["token_id"])
        m["volume"] = max(m["volume"], t["volume"])

    return (
        sorted(btc_eth.values(), key=lambda m: -m["volume"]),
        sorted(fed.values(), key=lambda m: -m["volume"]),
    )


def clob_market_tokens(condition_id: str) -> list[dict] | None:
    """CLOB per-condition lookup: token_id + outcome name, or None on failure."""
    try:
        resp = requests.get(
            f"{CLOB_BASE}/markets/{condition_id}", timeout=REQUEST_TIMEOUT
        )
    except requests.RequestException as e:
        log(f"  [WARN] CLOB /markets error ({condition_id[:14]}...): {e}")
        return None
    if resp.status_code != 200:
        return None
    try:
        body = resp.json()
    except ValueError:
        return None
    return body.get("tokens", []) or None


def fetch_full_history(token_id: str) -> list[tuple[int, float]] | None:
    """startTs-only full-history fetch (backfill-only capability)."""
    try:
        resp = requests.get(
            f"{CLOB_BASE}/prices-history",
            params={"market": token_id, "fidelity": 1, "startTs": BACKFILL_START_TS},
            timeout=300,
        )
    except requests.RequestException as e:
        log(f"  [WARN] prices-history error ({token_id[:14]}...): {e}")
        return None
    if resp.status_code != 200:
        log(f"  [WARN] prices-history HTTP {resp.status_code} ({token_id[:14]}...)")
        return None
    try:
        history = resp.json().get("history", [])
    except ValueError:
        return None
    return [(int(e["t"]), float(e["p"])) for e in history if e.get("t") is not None]


def backfill_market(
    store: PolymarketTickStore,
    meta: sqlite3.Connection,
    market: dict,
) -> dict:
    """Backfill one market. Returns per-market stats."""
    cond = market["condition_id"]
    stats = {
        "question": market["question"],
        "condition_id": cond,
        "end_date": market["end_date"],
        "tokens_fetched": [],
        "n_ticks": 0,
        "coverage": None,
        "error": None,
    }

    tokens = clob_market_tokens(cond)
    time.sleep(REQUEST_SLEEP)
    if not tokens:
        stats["error"] = "clob lookup failed"
        return stats

    # Record metadata for ALL tokens (incl. mirrored NO token)
    now_ts = int(time.time())
    for tok in tokens:
        upsert_market_meta(
            meta, tok["token_id"], cond, market["question"], now_ts, market["end_date"]
        )

    outcomes = {tok.get("outcome") for tok in tokens}
    is_binary = outcomes == {"Yes", "No"}
    # Binary: YES token only (NO is an exact mirror — see mining report).
    to_fetch = (
        [t for t in tokens if t.get("outcome") == "Yes"]
        if is_binary
        else tokens
    )

    all_ts: list[int] = []
    for tok in to_fetch:
        token_id = tok["token_id"]
        history = fetch_full_history(token_id)
        time.sleep(REQUEST_SLEEP)
        if history is None:
            stats["error"] = "history fetch failed"
            continue
        for i in range(0, len(history), CHUNK_SIZE):
            store.upsert_ticks(token_id, history[i : i + CHUNK_SIZE])
        stats["tokens_fetched"].append(
            {"token_id": token_id, "outcome": tok.get("outcome"), "n_ticks": len(history)}
        )
        stats["n_ticks"] += len(history)
        if history:
            all_ts.extend([history[0][0], history[-1][0]])

    if all_ts:
        fmt = lambda ts: datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")  # noqa: E731
        stats["coverage"] = [fmt(min(all_ts)), fmt(max(all_ts))]
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    parser.add_argument("--dry-run", action="store_true", help="List markets, fetch nothing")
    parser.add_argument(
        "--db",
        default=os.path.join(
            os.path.dirname(__file__), "..", "data", "polymarket_ticks.db"
        ),
    )
    args = parser.parse_args()
    db_path = os.path.abspath(args.db)

    log("Selecting backfill markets ...")
    btc_eth, fed = select_backfill_markets()
    log(f"Selected: {len(btc_eth)} BTC/ETH price markets, {len(fed)} fed-rates markets "
        f"(volume >= ${FED_MIN_VOLUME:,})")

    for m in btc_eth:
        log(f"  [btc/eth]  vol={m['volume']:>12,.0f}  {m['question'][:70]}")
    for m in fed[:10]:
        log(f"  [fed]      vol={m['volume']:>12,.0f}  {m['question'][:70]}")
    if len(fed) > 10:
        log(f"  [fed]      ... and {len(fed) - 10} more")

    if args.dry_run:
        log("Dry run complete.")
        return

    store = PolymarketTickStore(db_path)
    meta = sqlite3.connect(db_path, timeout=60)
    migrate_markets_table(meta)

    results: list[dict] = []
    t0 = time.time()
    for i, market in enumerate(btc_eth + fed, 1):
        log(f"[{i:>3}/{len(btc_eth) + len(fed)}] {market['question'][:70]}")
        stats = backfill_market(store, meta, market)
        results.append(stats)
        if stats["error"]:
            log(f"         ERROR: {stats['error']}")
        else:
            log(f"         {stats['n_ticks']:,} ticks  {stats['coverage']}")

    elapsed = time.time() - t0
    ok = [r for r in results if not r["error"]]
    failed = [r for r in results if r["error"]]
    total_ticks = sum(r["n_ticks"] for r in ok)
    log("=" * 70)
    log(f"Backfill complete in {elapsed / 60:.1f} min")
    log(f"  markets ok    : {len(ok)}")
    log(f"  markets failed: {len(failed)}")
    log(f"  total ticks   : {total_ticks:,}")
    log(f"  DB total ticks: {store.get_total_tick_count():,}")
    if failed:
        for r in failed:
            log(f"  FAILED: {r['question'][:60]} ({r['error']})")

    report = {
        "generated_at": now_utc().isoformat(),
        "elapsed_sec": round(elapsed, 1),
        "n_markets_ok": len(ok),
        "n_markets_failed": len(failed),
        "total_ticks_fetched": total_ticks,
        "note": "Binary markets: YES token only (NO is exact 1-YES mirror, mining finding)",
        "markets": results,
    }
    report_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "reports", "polymarket_backfill.json")
    )
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)
    log(f"Report written: {report_path}")
    meta.close()


if __name__ == "__main__":
    main()
