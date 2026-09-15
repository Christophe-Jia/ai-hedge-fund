"""
Gate.io BBO / order-book depth snapshot collector (low-frequency poller).

Why this exists: our platform trades daily-frequency signals, but owning a
long, continuous record of order-book microstructure lets us build daily
aggregates (book imbalance, taker-flow ratio, depth regime) that nobody else
has. HFT firms ignore 45s-polling data and retail collectors never survive —
after a few months this DB is a proprietary dataset. This is the flywheel
starter, not a trading signal.

Polls Gate.io public REST endpoints (no API key) once per interval:

  1. Spot   BTC_USDT order book, top-20 levels   /spot/order_book
  2. Perp   BTC_USDT order book, top-20 levels   /futures/usdt/order_book
  3. Spot   BTC_USDT recent trades (incremental) /spot/trades

Gate.io is used because api.ginance.com is blocked on this network while
Gate REST is verified reachable (our funding-rate data also comes from Gate).

Storage: data/bbo_snapshots.db (SQLite, created automatically).

Schema:
    book_snapshots  -- one row per price level per poll cycle
        (market, symbol, recv_ts_ms, exch_ts_ms, level, side, price, size)
        market  : 'spot' | 'perp'
        side    : 'bid' | 'ask'
        level   : 0 = best, 1 = second, ...
        size    : base-asset quantity in BTC.
                  Spot returns BTC directly; perp returns contract counts,
                  converted via quanto multiplier (0.0001 BTC/contract for
                  BTC_USDT) so both markets are directly comparable.
    trades         -- one row per spot trade, deduped by trade_id
        (trade_id PK, ts_ms, recv_ts_ms, symbol, side, price, size)
        side    : TAKER direction ('buy' = aggressive buyer lifted the ask)

Restart safety: trades dedupe via PRIMARY KEY trade_id (Gate IDs increase
monotonically per pair); book snapshots are naturally idempotent (each poll
is a fresh timestamped row).

Usage:
    poetry run python scripts/collect_bbo_snapshots.py            # resident loop, 45s
    poetry run python scripts/collect_bbo_snapshots.py --once     # single cycle (test)
    poetry run python scripts/collect_bbo_snapshots.py --interval 30 --depth 20

Long-running deployment:
    # nohup
    nohup poetry run python scripts/collect_bbo_snapshots.py \\
        > logs/bbo_snapshots.log 2>&1 &

    # macOS launchd: ~/Library/LaunchAgents/com.aihedgefund.bbo.plist
    <?xml version="1.0" encoding="UTF-8"?>
    <!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
      "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
    <plist version="1.0"><dict>
      <key>Label</key><string>com.aihedgefund.bbo</string>
      <key>ProgramArguments</key><array>
        <string>poetry</string><string>run</string><string>python</string>
        <string>scripts/collect_bbo_snapshots.py</string>
      </array>
      <key>WorkingDirectory</key><string>/Users/jeffryjia/Vibe/ai-hedge-fund</string>
      <key>RunAtLoad</key><true/>
      <key>KeepAlive</key><true/>
      <key>StandardOutPath</key><string>logs/bbo_snapshots.log</string>
      <key>StandardErrorPath</key><string>logs/bbo_snapshots.log</string>
    </dict></plist>
    # then: launchctl load ~/Library/LaunchAgents/com.aihedgefund.bbo.plist

Example analytics (the daily-feature prototypes this data feeds):

    -- Last 1h: avg bid depth vs ask depth (spot, top-10 levels, per snapshot)
    WITH per_snap AS (
        SELECT recv_ts_ms, side, SUM(size) AS depth
        FROM book_snapshots
        WHERE market = 'spot' AND level < 10
          AND recv_ts_ms > (CAST(strftime('%s','now') AS INTEGER) - 3600) * 1000
        GROUP BY recv_ts_ms, side)
    SELECT side, AVG(depth) AS avg_depth FROM per_snap GROUP BY side;

    -- Last 1h: taker buy/sell volume ratio
    SELECT SUM(CASE WHEN side = 'buy' THEN size ELSE 0 END) * 1.0
         / NULLIF(SUM(CASE WHEN side = 'sell' THEN size ELSE 0 END), 0)
           AS taker_buy_sell_ratio
    FROM trades
    WHERE ts_ms > (CAST(strftime('%s','now') AS INTEGER) - 3600) * 1000;
"""

from __future__ import annotations

import argparse
import os
import signal
import sqlite3
import sys
import time
from datetime import datetime, timezone

import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GATE_BASE = "https://api.gateio.ws/api/v4"

SPOT_PAIR = "BTC_USDT"
PERP_CONTRACT = "BTC_USDT"

DEFAULT_INTERVAL = 45          # seconds between polls
DEFAULT_DEPTH = 20             # order book levels per side
TRADES_LIMIT = 1000            # max trades per poll (Gate cap; covers busy bursts)
REQUEST_TIMEOUT = 15           # seconds per HTTP request
BACKOFF_INITIAL = 5            # seconds, doubles on consecutive failures ...
BACKOFF_MAX = 300              # ... capped at 5 minutes
STATS_INTERVAL = 3600          # print stats line every hour
FALLBACK_QUANTO = 0.0001       # BTC per perp contract (BTC_USDT) if lookup fails


# ---------------------------------------------------------------------------
# SQLite store
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS book_snapshots (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    market      TEXT    NOT NULL,   -- 'spot' | 'perp'
    symbol      TEXT    NOT NULL,   -- 'BTC_USDT'
    recv_ts_ms  INTEGER NOT NULL,   -- local receive time, epoch ms
    exch_ts_ms  REAL    NOT NULL,   -- exchange timestamp, epoch ms
    level       INTEGER NOT NULL,   -- 0 = best level, 1 = second, ...
    side        TEXT    NOT NULL,   -- 'bid' | 'ask'
    price       REAL    NOT NULL,   -- USDT
    size        REAL    NOT NULL    -- base qty in BTC (perp converted from contracts)
);
CREATE INDEX IF NOT EXISTS idx_book_market_ts ON book_snapshots (market, recv_ts_ms);
CREATE INDEX IF NOT EXISTS idx_book_ts        ON book_snapshots (recv_ts_ms);

CREATE TABLE IF NOT EXISTS trades (
    trade_id    INTEGER PRIMARY KEY,  -- Gate trade id (monotonic per pair)
    ts_ms       INTEGER NOT NULL,     -- exchange trade time, epoch ms
    recv_ts_ms  INTEGER NOT NULL,     -- local receive time, epoch ms
    symbol      TEXT    NOT NULL,
    side        TEXT    NOT NULL,     -- taker direction: 'buy' | 'sell'
    price       REAL    NOT NULL,     -- USDT
    size        REAL    NOT NULL      -- base qty in BTC
);
CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades (ts_ms);
"""


class BboStore:
    """Thin SQLite wrapper. One transaction per poll cycle."""

    def __init__(self, db_path: str):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def insert_book_rows(self, rows: list[tuple]) -> None:
        self.conn.executemany(
            "INSERT INTO book_snapshots"
            " (market, symbol, recv_ts_ms, exch_ts_ms, level, side, price, size)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        self.conn.commit()

    def insert_trades(self, rows: list[tuple]) -> int:
        """INSERT OR IGNORE — trade_id dedupe makes restarts idempotent."""
        cur = self.conn.executemany(
            "INSERT OR IGNORE INTO trades"
            " (trade_id, ts_ms, recv_ts_ms, symbol, side, price, size)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        self.conn.commit()
        return cur.rowcount

    def last_trade_id(self) -> int:
        row = self.conn.execute("SELECT MAX(trade_id) FROM trades").fetchone()
        return row[0] or 0

    def counts(self) -> tuple[int, int]:
        b = self.conn.execute("SELECT COUNT(*) FROM book_snapshots").fetchone()[0]
        t = self.conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        return b, t

    def recent_latency_ms(self, lookback_s: int = 3600) -> float | None:
        """Mean (recv_ts - exch_ts) over recent book rows; None if empty."""
        cutoff = (time.time() - lookback_s) * 1000
        row = self.conn.execute(
            "SELECT AVG(recv_ts_ms - exch_ts_ms) FROM book_snapshots"
            " WHERE recv_ts_ms > ?",
            (cutoff,),
        ).fetchone()
        return row[0]


# ---------------------------------------------------------------------------
# Gate.io REST helpers
# ---------------------------------------------------------------------------

def _get(path: str, params: dict) -> dict | list:
    """GET with timeout; raise RuntimeError on non-200 so caller can back off."""
    resp = requests.get(
        f"{GATE_BASE}{path}", params=params, timeout=REQUEST_TIMEOUT
    )
    if resp.status_code == 429:
        raise RuntimeError(f"HTTP 429 rate limited: {path}")
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code} from {path}")
    return resp.json()


def fetch_spot_book(depth: int) -> tuple[float, list, list]:
    """Returns (exch_ts_ms, bids, asks); each side is [(price, size_btc), ...]
    sorted best-first."""
    d = _get("/spot/order_book", {"currency_pair": SPOT_PAIR, "limit": depth})
    bids = [(float(p), float(s)) for p, s in d["bids"]]   # best (highest) first
    asks = [(float(p), float(s)) for p, s in d["asks"]]   # best (lowest) first
    return float(d["current"]), bids, asks


def fetch_perp_book(depth: int, quanto: float) -> tuple[float, list, list]:
    """Returns (exch_ts_ms, bids, asks); size converted contracts -> BTC."""
    d = _get("/futures/usdt/order_book", {"contract": PERP_CONTRACT, "limit": depth})
    bids = [(float(x["p"]), float(x["s"]) * quanto) for x in d["bids"]]
    asks = [(float(x["p"]), float(x["s"]) * quanto) for x in d["asks"]]
    return float(d["current"]) * 1000.0, bids, asks  # perp ts is in seconds


def fetch_spot_trades(last_trade_id: int) -> list[dict]:
    """Returns new trades with id > last_trade_id, oldest first.

    Note: Gate caps at TRADES_LIMIT per call and returns newest first. During
    extreme bursts a 45s gap can exceed the cap; the oldest trades in that
    gap are dropped. Acceptable for a low-frequency feature poller.
    """
    d = _get("/spot/trades", {"currency_pair": SPOT_PAIR, "limit": TRADES_LIMIT})
    out = [t for t in d if int(t["id"]) > last_trade_id]
    out.reverse()  # oldest first for a clean insert order
    return out


def fetch_quanto_multiplier() -> float:
    """BTC per perp contract, from the contracts endpoint (self-correcting)."""
    try:
        d = _get("/futures/usdt/contracts", {"contract": PERP_CONTRACT})
        return float(d["quanto_multiplier"])
    except Exception:
        return FALLBACK_QUANTO


# ---------------------------------------------------------------------------
# Collector loop
# ---------------------------------------------------------------------------

def run_cycle(
    store: BboStore, depth: int, quanto: float, last_tid: int
) -> tuple[int, int]:
    """One poll cycle. Returns (book_rows_written, new_trades_written)."""
    recv_ms = int(time.time() * 1000)
    book_rows: list[tuple] = []

    for market, fetch in (
        ("spot", lambda: fetch_spot_book(depth)),
        ("perp", lambda: fetch_perp_book(depth, quanto)),
    ):
        exch_ms, bids, asks = fetch()
        for level, (price, size) in enumerate(bids):
            book_rows.append(
                (market, SPOT_PAIR, recv_ms, exch_ms, level, "bid", price, size)
            )
        for level, (price, size) in enumerate(asks):
            book_rows.append(
                (market, SPOT_PAIR, recv_ms, exch_ms, level, "ask", price, size)
            )

    store.insert_book_rows(book_rows)

    trades = fetch_spot_trades(last_tid)
    trade_rows = [
        (
            int(t["id"]),
            int(float(t["create_time_ms"])),
            recv_ms,
            t["currency_pair"],
            t["side"],               # taker direction
            float(t["price"]),
            float(t["amount"]),
        )
        for t in trades
    ]
    n_new = store.insert_trades(trade_rows)
    return len(book_rows), n_new


def run_collector(interval: int, depth: int, db_path: str, once: bool) -> None:
    store = BboStore(db_path)

    print("=== Gate.io BBO Snapshot Collector ===")
    print(f"  Interval : {interval}s (depth={depth})")
    print(f"  DB path  : {db_path}")
    print(f"  Started  : {datetime.now(tz=timezone.utc).isoformat()}")
    print(f"  Stop     : Ctrl+C or SIGTERM\n")

    quanto = fetch_quanto_multiplier()
    print(f"[init] perp quanto multiplier: {quanto} BTC/contract "
          f"(contracts -> BTC conversion)")

    _stop = False

    def _handle_signal(signum, frame):
        nonlocal _stop
        _stop = True
        print("\n[collector] Shutdown signal received ...")

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    last_tid = store.last_trade_id()
    print(f"[init] resuming trades after trade_id={last_tid}")

    # Stats bookkeeping
    stat_book = stat_trades = stat_cycles = 0
    stat_started = time.time()
    backoff = BACKOFF_INITIAL

    while not _stop:
        t0 = time.time()
        try:
            n_book, n_trades = run_cycle(store, depth, quanto, last_tid)
            if n_trades:
                last_tid = store.last_trade_id()
            stat_book += n_book
            stat_trades += n_trades
            stat_cycles += 1
            backoff = BACKOFF_INITIAL  # success resets the backoff
            print(
                f"[{datetime.now(tz=timezone.utc).strftime('%H:%M:%S')}] "
                f"cycle ok: book_rows={n_book} new_trades={n_trades}"
            )
        except Exception as e:
            # Network error / 5xx / 429: back off, do not crash. SQLite state
            # is safe — the failed cycle either committed fully or not at all.
            print(f"[{datetime.now(tz=timezone.utc).strftime('%H:%M:%S')}] "
                  f"[WARN] cycle failed ({e}); retrying in {backoff:.0f}s")
            deadline = time.time() + backoff
            while time.time() < deadline and not _stop:
                time.sleep(1)
            backoff = min(backoff * 2, BACKOFF_MAX)
            if once:
                raise

        if once:
            break

        # Hourly stats
        if time.time() - stat_started >= STATS_INTERVAL:
            lat = store.recent_latency_ms()
            lat_str = f"{lat:.0f}ms" if lat is not None else "n/a"
            print(
                f"[stats] last {STATS_INTERVAL // 60}min: "
                f"cycles={stat_cycles} book_rows={stat_book} "
                f"trades={stat_trades} recent_recv_latency={lat_str}"
            )
            stat_book = stat_trades = stat_cycles = 0
            stat_started = time.time()

        # Sleep out the remainder of the interval, waking on stop signals
        deadline = t0 + interval
        while time.time() < deadline and not _stop:
            time.sleep(1)

    total_book, total_trades = store.counts()
    print("\n[collector] Shutdown complete.")
    print(f"  DB totals: book_snapshot_rows={total_book:,} trades={total_trades:,}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Gate.io BBO/depth snapshot collector (spot+perp book, spot trades)"
    )
    parser.add_argument(
        "--interval", type=int, default=DEFAULT_INTERVAL,
        help=f"Poll interval in seconds (default: {DEFAULT_INTERVAL})",
    )
    parser.add_argument(
        "--depth", type=int, default=DEFAULT_DEPTH,
        help=f"Order book levels per side (default: {DEFAULT_DEPTH})",
    )
    parser.add_argument(
        "--db", default=os.path.join(
            os.path.dirname(__file__), "..", "data", "bbo_snapshots.db"
        ),
        help="SQLite database path (default: data/bbo_snapshots.db)",
    )
    parser.add_argument(
        "--once", action="store_true",
        help="Run a single poll cycle and exit (for testing)",
    )
    args = parser.parse_args()

    db_path = os.path.abspath(args.db)
    run_collector(
        interval=args.interval,
        depth=args.depth,
        db_path=db_path,
        once=args.once,
    )


if __name__ == "__main__":
    main()
