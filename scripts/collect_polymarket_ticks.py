"""
Polymarket CLOB price tick collector.

Polls the public CLOB /prices-history?fidelity=1 endpoint for active
prediction markets and persists tick-level price history to a local
SQLite database. No API key required.

Market discovery (rewritten 2026-09-15 per mining findings, see
reports/polymarket_mining.json):
  - Primary channel: Gamma /events?tag_slug=crypto and tag_slug=fed-rates
    (both active, high-volume families; the old keyword scan missed all
    crypto-price and Fed/macro markets and wasted slots on dead markets).
  - Local dead-market filtering: Gamma active/closed flags are unreliable,
    so markets are kept only if market-level closed==false AND
    endDate > now. Markets are ranked by volume; top MAX_PER_TAG per tag.
  - Small keyword supplement channel (optional, off by default).

Dead-market retirement: a token with no new ticks for DEAD_AFTER_SECONDS
(3 days; active markets tick every ~10 min) is checked against the CLOB
for its resolved outcome, recorded in the markets table, then removed
from the polling list for the remainder of the process lifetime.

Schema (markets table, migrated at runtime — src/ store is unchanged):
  - end_date TEXT  : market endDate (ISO) recorded at discovery
  - outcome  TEXT  : resolved winning outcome, recorded on close
Events studies (probability -> reality) become possible from the DB alone.

Poll cadence: 300s (stored fidelity=1 ticks have ~10-min effective
granularity; 30s polling added nothing).

Usage:
    poetry run python scripts/collect_polymarket_ticks.py
    poetry run python scripts/collect_polymarket_ticks.py --once
    poetry run python scripts/collect_polymarket_ticks.py --db data/my_ticks.db

Background:
    nohup poetry run python scripts/collect_polymarket_ticks.py > logs/polymarket_ticks.log 2>&1 &
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sqlite3
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

# Primary discovery channel: tag-based event families
TAG_SLUGS = ["crypto", "fed-rates"]

# Optional keyword supplement (off by default; --keywords to enable)
DEFAULT_SUPPLEMENT_KEYWORDS: list[str] = []
SUPPLEMENT_MAX_PAGES = 10  # /markets pages per refresh (100 markets/page)

# Sports/entertainment noise to exclude from the supplement channel
EXCLUDE_KEYWORDS = [
    "nba", "nfl", "nhl", "mlb", "fifa", "world cup",
    "mvp", "rookie", "stanley cup", "super bowl",
    "warriors", "lakers", "celtics", "yankees",
    "gta", "oscar", "grammy", "box office",
]

MARKET_REFRESH_INTERVAL = 3600  # refresh market list every hour (seconds)
REQUEST_TIMEOUT = 30            # seconds per HTTP request (history payloads can be large)
MAX_PER_TAG = 40                # top-N markets by volume per tag slug
DEAD_AFTER_SECONDS = 3 * 86400  # no new ticks for 3 days -> retire token
LONG_GAP_SECONDS = 7 * 86400    # gap too long for startTs+endTs pair -> interval=max bootstrap
OUTCOME_CHECK_CAP = 25          # max outcome lookups per refresh cycle

DEFAULT_POLL_INTERVAL = 300     # 5 minutes


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------

def parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None


def now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


def log(msg: str) -> None:
    print(f"[{now_utc().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Markets table migration + direct SQL metadata access
# (runtime ALTER so src/data/polymarket_tick_store.py stays untouched)
# ---------------------------------------------------------------------------

def migrate_markets_table(conn: sqlite3.Connection) -> None:
    """Add end_date / outcome columns to the markets table if missing."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(markets)")}
    if "end_date" not in cols:
        conn.execute("ALTER TABLE markets ADD COLUMN end_date TEXT")
        log("Schema: added markets.end_date column")
    if "outcome" not in cols:
        conn.execute("ALTER TABLE markets ADD COLUMN outcome TEXT")
        log("Schema: added markets.outcome column")
    conn.commit()


def upsert_market_meta(
    conn: sqlite3.Connection,
    token_id: str,
    condition_id: str | None,
    question: str | None,
    last_seen: int,
    end_date: str | None,
) -> None:
    """Upsert metadata; preserves an already-recorded outcome."""
    conn.execute(
        """
        INSERT INTO markets (token_id, condition_id, question, last_seen, end_date)
        VALUES (:token_id, :condition_id, :question, :last_seen, :end_date)
        ON CONFLICT(token_id) DO UPDATE SET
            condition_id = excluded.condition_id,
            question = excluded.question,
            last_seen = excluded.last_seen,
            end_date = COALESCE(excluded.end_date, markets.end_date)
        """,
        {
            "token_id": token_id,
            "condition_id": condition_id,
            "question": question,
            "last_seen": last_seen,
            "end_date": end_date,
        },
    )
    conn.commit()


def set_outcome(
    conn: sqlite3.Connection, condition_id: str, outcome: str
) -> int:
    """Record the resolved outcome on every token row of a condition."""
    cur = conn.execute(
        "UPDATE markets SET outcome = :outcome WHERE condition_id = :cid AND outcome IS NULL",
        {"outcome": outcome, "cid": condition_id},
    )
    conn.commit()
    return cur.rowcount


def pending_outcome_conditions(
    conn: sqlite3.Connection, require_end_date_passed: bool = True
) -> list[tuple[str, str | None]]:
    """Conditions with no recorded outcome (optionally only where endDate passed)."""
    rows = conn.execute(
        """
        SELECT DISTINCT condition_id, end_date FROM markets
        WHERE outcome IS NULL AND condition_id IS NOT NULL AND condition_id != ''
        """
    ).fetchall()
    out: list[tuple[str, str | None]] = []
    now = now_utc()
    for cid, end_date in rows:
        if not require_end_date_passed:
            out.append((cid, end_date))
            continue
        ed = parse_iso(end_date)
        if ed is not None and ed <= now:
            out.append((cid, end_date))
    return out


# ---------------------------------------------------------------------------
# Market discovery
# ---------------------------------------------------------------------------

def _event_markets(ev: dict) -> list[dict]:
    mks = ev.get("markets", [])
    if isinstance(mks, str):
        try:
            mks = json.loads(mks)
        except (ValueError, TypeError):
            mks = []
    return [m for m in (mks or []) if isinstance(m, dict)]


def _market_records(m: dict, tag: str, event_title: str | None, now: datetime) -> list[dict]:
    """Build token-level records for a Gamma market dict, or [] if dead/invalid."""
    if m.get("closed"):
        return []
    ed = parse_iso(m.get("endDate"))
    if ed is None or ed <= now:
        return []  # Gamma flags unreliable: enforce endDate > now locally
    end_iso = m.get("endDate")

    tokens = m.get("clobTokenIds", [])
    if isinstance(tokens, str):
        try:
            tokens = json.loads(tokens)
        except (ValueError, TypeError):
            tokens = []
    if not tokens:
        return []

    condition_id = m.get("conditionId", "")
    question = m.get("question", "")
    try:
        volume = float(m.get("volumeNum") or 0.0)
    except (TypeError, ValueError):
        volume = 0.0

    return [
        {
            "token_id": t,
            "condition_id": condition_id,
            "question": question,
            "end_date": end_iso,
            "volume": volume,
            "tag": tag,
            "event_title": event_title or "",
        }
        for t in tokens
        if t
    ]


def fetch_tag_markets(tag_slug: str) -> list[dict]:
    """All live (endDate > now, closed=false) token records under a tag."""
    now = now_utc()
    records: list[dict] = []
    offset = 0
    while True:
        try:
            resp = requests.get(
                f"{GAMMA_BASE}/events",
                params={
                    "tag_slug": tag_slug,
                    "closed": "false",
                    "limit": 100,
                    "offset": offset,
                },
                timeout=REQUEST_TIMEOUT,
            )
        except requests.RequestException as e:
            log(f"[WARN] Gamma /events error ({tag_slug}, offset={offset}): {e}")
            break
        if resp.status_code != 200:
            log(f"[WARN] Gamma /events HTTP {resp.status_code} ({tag_slug}, offset={offset})")
            break
        events = resp.json()
        if not isinstance(events, list) or not events:
            break
        for ev in events:
            if not isinstance(ev, dict):
                continue
            for m in _event_markets(ev):
                records.extend(_market_records(m, tag_slug, ev.get("title"), now))
        offset += 100
        if len(events) < 100:
            break
    return records


def fetch_keyword_supplement(
    keywords: list[str], exclude: list[str]
) -> list[dict]:
    """Optional supplement: keyword scan over open Gamma /markets pages."""
    if not keywords:
        return []
    now = now_utc()
    kw = [k.lower() for k in keywords]
    ex = [k.lower() for k in exclude]
    records: list[dict] = []
    offset = 0
    for _ in range(SUPPLEMENT_MAX_PAGES):
        try:
            resp = requests.get(
                f"{GAMMA_BASE}/markets",
                params={"active": "true", "closed": "false", "limit": 100, "offset": offset},
                timeout=REQUEST_TIMEOUT,
            )
        except requests.RequestException as e:
            log(f"[WARN] Gamma /markets error (offset={offset}): {e}")
            break
        if resp.status_code != 200:
            break
        items = resp.json()
        if not isinstance(items, list) or not items:
            break
        for m in items:
            q = (m.get("question") or "").lower()
            if not any(k in q for k in kw):
                continue
            if any(x in q for x in ex):
                continue
            records.extend(_market_records(m, "keyword", None, now))
        offset += 100
        if len(items) < 100:
            break
    return records


def discover_markets(keywords: list[str]) -> list[dict]:
    """Tag-primary discovery + optional keyword supplement; deduped by token."""
    by_token: dict[str, dict] = {}
    for tag in TAG_SLUGS:
        tag_records = fetch_tag_markets(tag)
        # Rank markets by volume, keep top MAX_PER_TAG per tag
        tag_records.sort(key=lambda r: r["volume"], reverse=True)
        kept = 0
        seen_conditions: set[str] = set()
        for r in tag_records:
            if r["condition_id"] in seen_conditions:
                by_token.setdefault(r["token_id"], r)  # sibling tokens always kept
                continue
            if kept >= MAX_PER_TAG:
                break
            seen_conditions.add(r["condition_id"])
            kept += 1
            by_token.setdefault(r["token_id"], r)
        log(f"Discovery[{tag}]: {len(tag_records)} live tokens, kept top {len(seen_conditions)} markets")

    supp = fetch_keyword_supplement(keywords, EXCLUDE_KEYWORDS)
    supp_conditions = {r["condition_id"] for r in supp}
    for r in supp:
        if r["condition_id"] not in {by_token[t]["condition_id"] for t in by_token}:
            by_token.setdefault(r["token_id"], r)
    if supp:
        log(f"Discovery[keyword supplement]: {len(supp_conditions)} extra markets")

    return list(by_token.values())


# ---------------------------------------------------------------------------
# Tick fetching
# ---------------------------------------------------------------------------

def fetch_price_ticks(
    token_id: str, start_ts: int | None = None
) -> list[tuple[int, float]]:
    """
    Fetch fidelity=1 price history for a token from the CLOB API.

    Incremental mode uses startTs+endTs pairs (short windows only; long
    pairs are rejected by the API). If the gap since start_ts exceeds
    LONG_GAP_SECONDS, fall back to interval=max (~31-day bootstrap).
    The startTs-only full-history capability is reserved for the
    backfill script, never used here.
    """
    now = int(time.time())
    params: dict = {"market": token_id, "fidelity": "1"}

    if start_ts is None or now - start_ts > LONG_GAP_SECONDS:
        params["interval"] = "max"
    else:
        params["startTs"] = str(start_ts)
        params["endTs"] = str(now)

    try:
        resp = requests.get(
            f"{CLOB_BASE}/prices-history", params=params, timeout=REQUEST_TIMEOUT
        )
    except requests.RequestException as e:
        print(f"  [WARN] CLOB request error for {token_id[:12]}...: {e}", flush=True)
        return []

    if resp.status_code == 404:
        return []  # market closed / gone
    if resp.status_code != 200:
        print(f"  [WARN] CLOB HTTP {resp.status_code} for {token_id[:12]}...", flush=True)
        return []

    try:
        body = resp.json()
    except ValueError:
        return []

    ticks: list[tuple[int, float]] = []
    for entry in body.get("history", []):
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
# Outcome recording
# ---------------------------------------------------------------------------

def check_condition_outcome(condition_id: str) -> str | None:
    """
    Ask the CLOB whether a condition is resolved; return the winning
    outcome name (e.g. 'Yes' / 'No' / candidate name) or None.
    """
    try:
        resp = requests.get(
            f"{CLOB_BASE}/markets/{condition_id}", timeout=REQUEST_TIMEOUT
        )
    except requests.RequestException as e:
        print(f"  [WARN] CLOB /markets lookup error ({condition_id[:12]}...): {e}", flush=True)
        return None
    if resp.status_code != 200:
        return None
    try:
        body = resp.json()
    except ValueError:
        return None
    if not body.get("closed"):
        return None
    for tok in body.get("tokens", []):
        if tok.get("winner"):
            return tok.get("outcome") or "Yes"
    return None


def record_outcome(conn: sqlite3.Connection, condition_id: str) -> str | None:
    outcome = check_condition_outcome(condition_id)
    if outcome:
        n = set_outcome(conn, condition_id, outcome)
        if n:
            log(f"Outcome recorded: {condition_id[:14]}... -> {outcome} ({n} tokens)")
    return outcome


def sweep_outcomes(
    conn: sqlite3.Connection, require_end_date_passed: bool = True
) -> int:
    """Record outcomes for tracked conditions that have closed. Returns n recorded."""
    recorded = 0
    pending = pending_outcome_conditions(conn, require_end_date_passed)
    for cid, _end in pending[:OUTCOME_CHECK_CAP]:
        if record_outcome(conn, cid):
            recorded += 1
        time.sleep(0.5)
    if pending:
        log(f"Outcome sweep: {len(pending)} pending, recorded {recorded} this cycle")
    return recorded


# ---------------------------------------------------------------------------
# Main polling loop
# ---------------------------------------------------------------------------

def run_collector(
    keywords: list[str],
    poll_interval: int,
    db_path: str,
    once: bool = False,
) -> None:
    store = PolymarketTickStore(db_path)
    meta = sqlite3.connect(db_path, timeout=30)
    migrate_markets_table(meta)

    print("\n=== Polymarket Tick Collector ===")
    print(f"  Tags      : {', '.join(TAG_SLUGS)}"
          + (f"  + keywords: {', '.join(keywords)}" if keywords else ""))
    print(f"  Interval  : {poll_interval}s")
    print(f"  DB path   : {db_path}")
    print(f"  Started   : {now_utc().isoformat()}")
    print(f"  Stop      : Ctrl+C or SIGTERM\n")

    _stop = False

    def _handle_signal(signum, frame):
        nonlocal _stop
        _stop = True
        print("\n[collector] Shutdown signal received ...", flush=True)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    markets: list[dict] = []
    last_market_refresh = 0.0
    iteration = 0
    retired: set[str] = set()          # tokens removed from polling this process
    first_poll_ts: dict[str, float] = {}

    def refresh_markets() -> list[dict]:
        found = discover_markets(keywords)
        now_ts = int(time.time())
        for m in found:
            upsert_market_meta(
                meta, m["token_id"], m["condition_id"], m["question"], now_ts, m["end_date"]
            )
        return found

    def retire_token(token_id: str, reason: str) -> None:
        m = next((x for x in markets if x["token_id"] == token_id), None)
        cid = m["condition_id"] if m else None
        if cid:
            record_outcome(meta, cid)  # record result if resolved
        retired.add(token_id)
        q = m["question"][:60] if m else ""
        log(f"Retired token {token_id[:14]}... ({reason})  {q}")

    while not _stop:
        now = time.time()

        if now - last_market_refresh > MARKET_REFRESH_INTERVAL or not markets:
            log("Refreshing market list ...")
            markets = refresh_markets()
            last_market_refresh = now
            n_conditions = len({m["condition_id"] for m in markets})
            log(f"Tracking {len(markets)} token(s) across {n_conditions} markets "
                f"({len(retired)} retired)")
            tags: dict[str, int] = {}
            for m in markets:
                tags[m["tag"]] = tags.get(m["tag"], 0) + 1
            log(f"  by tag: {tags}")
            for m in markets[:8]:
                log(f"    {m['token_id'][:14]}...  [{m['tag']}]  {m['question'][:64]}")
            if len(markets) > 8:
                log(f"    ... and {len(markets) - 8} more")
            sweep_outcomes(meta)  # record results of markets whose endDate passed

        iteration += 1
        total_new = 0
        polled = 0

        for m in markets:
            if _stop:
                break
            token_id = m["token_id"]
            if token_id in retired:
                continue
            polled += 1
            first_poll_ts.setdefault(token_id, now)

            last_ts = store.get_latest_ts(token_id)
            new_ticks = fetch_price_ticks(token_id, start_ts=last_ts)
            if new_ticks:
                store.upsert_ticks(token_id, new_ticks)
                total_new += len(new_ticks)

            # Dead-market fallback (Gamma flags unreliable): no tick progress
            # for DEAD_AFTER_SECONDS -> check outcome, retire from polling.
            latest = store.get_latest_ts(token_id)
            if latest is not None and now - latest > DEAD_AFTER_SECONDS:
                retire_token(token_id, "no new ticks > 3d")
            elif latest is None and now - first_poll_ts[token_id] > DEAD_AFTER_SECONDS:
                retire_token(token_id, "never produced ticks > 3d")

        total_stored = store.get_total_tick_count()
        log(
            f"iter={iteration:>5}  new_ticks={total_new:>6}  "
            f"total_stored={total_stored:>10,}  polled={polled}  retired={len(retired)}"
        )

        if once:
            log("--once: single pass complete, exiting.")
            break

        deadline = time.time() + poll_interval
        while time.time() < deadline and not _stop:
            time.sleep(1)

    print("\n[collector] Shutdown complete.")
    print(f"  Total ticks in DB: {store.get_total_tick_count():,}")
    meta.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Polymarket CLOB fidelity=1 tick collector (tag-based discovery)"
    )
    parser.add_argument(
        "--keywords",
        default=",".join(DEFAULT_SUPPLEMENT_KEYWORDS),
        help="Optional keyword supplement channel, comma-separated (default: off)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_POLL_INTERVAL,
        help=f"Poll interval in seconds (default: {DEFAULT_POLL_INTERVAL})",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one discovery + poll pass and exit",
    )
    parser.add_argument(
        "--sweep-all-outcomes",
        action="store_true",
        help="One-off: record outcomes for ALL tracked conditions missing one, then exit",
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

    if args.sweep_all_outcomes:
        meta = sqlite3.connect(db_path, timeout=30)
        migrate_markets_table(meta)
        pending = pending_outcome_conditions(meta, require_end_date_passed=False)
        log(f"Sweep-all: {len(pending)} tracked conditions without outcome")
        recorded = 0
        for cid, _end in pending:
            if record_outcome(meta, cid):
                recorded += 1
            time.sleep(0.5)
        log(f"Sweep-all complete: {recorded} outcomes recorded")
        meta.close()
        return

    os.makedirs(os.path.join(os.path.dirname(__file__), "..", "logs"), exist_ok=True)

    run_collector(
        keywords=keywords,
        poll_interval=args.interval,
        db_path=db_path,
        once=args.once,
    )


if __name__ == "__main__":
    main()
