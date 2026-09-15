"""
Gate.io derivatives snapshot collector: open interest, liquidation flow and
long/short ratios (low-frequency poller).

Why this exists: our platform trades daily-frequency signals, but owning a
long, continuous record of derivatives positioning (open interest, forced
liquidation flow, crowd long/short balance) lets us build daily aggregates
that nobody else has. Combined with data/bbo_snapshots.db (spot/perp books +
trades) this forms a proprietary microstructure dataset. This is the flywheel
starter, not a trading signal.

Polls Gate.io public REST endpoints (no API key) once per interval for
BTC_USDT and ETH_USDT perpetuals:

  1. Real-time OI       /futures/usdt/tickers      -> oi_snapshots
     (total_size x quanto_multiplier)
  2. 5m window stats    /futures/usdt/contract_stats -> lssr_snapshots
     (LSR / top-trader LSR /        + liq_window_agg
      user counts / OI / funding)

Endpoint degradation (verified 2026-09-15 on this network):

  * GET /futures/usdt/open_interest now returns HTTP 400
    "MISSING_REQUIRED_HEADER: KEY" -- Gate moved it behind API-key auth.
    Real-time OI is instead taken from tickers.total_size (same basis,
    contracts x quanto_multiplier).
  * GET /futures/usdt/liquidates returns the same auth error, so individual
    liquidation records are NOT currently retrievable without an API key.
    The collector still probes the endpoint once per hour; until it becomes
    reachable the `liquidations` table stays empty and liquidation flow is
    captured as per-5m-window aggregates from contract_stats
    (liq_window_agg: long/short liquidated size/amount/USD per window).
    To enable per-record capture later: create a Gate API key and sign the
    request (see Gate APIv4 auth docs).

Storage: data/deriv_snapshots.db (SQLite, created automatically).

Schema:
    oi_snapshots   -- one row per contract per poll cycle (real-time)
        (id, recv_ts_ms, contract, oi_base, oi_contracts, mark_price,
         funding_rate)
        oi_base      : open interest in base units (BTC / ETH)
        oi_contracts : raw open interest in contracts

    lssr_snapshots  -- one row per contract per 5m window (long/short ratios)
        (contract, window_ts PK, lsr_taker, lsr_account, top_lsr_size,
         top_lsr_account, long_users, short_users, long_taker_size,
         short_taker_size, open_interest, open_interest_usd, mark_price,
         last_funding_rate, recv_ts_ms)
        window_ts    : unix seconds of the 5m window open (Gate "time" field)
        lsr_taker    : long/short taker-volume ratio (<1 = heavier aggressive
                       selling); lsr_account: long/short account ratio;
                       top_lsr_*: same but top traders (by size / by account)

    liq_window_agg  -- aggregate liquidation flow per contract per 5m window
        (contract, window_ts PK, long_liq_size, short_liq_size,
         long_liq_amount, short_liq_amount, long_liq_usd, short_liq_usd,
         long_liq_usd_new, short_liq_usd_new, recv_ts_ms)
        *_liq_size   : contracts liquidated on that side
        *_liq_amount : base units liquidated (BTC / ETH)
        *_liq_usd_new: USD notional (newer calculation basis)
        "long" = long positions were liquidated (sell-side pressure).

    liquidations    -- individual liquidation records, only populated if the
                       /liquidates endpoint ever becomes reachable (see above)
        (contract, ts_ms, side, price, size, leverage, recv_ts_ms,
         PK (contract, ts_ms, side, price, size))
        side : 'long' | 'short' (which side got liquidated)
        size : base units (contracts x quanto_multiplier)

Restart safety: stats windows are upserted by (contract, window_ts) with a
6h lookback, so a restart backfills missed windows; the in-progress window
is refreshed each poll (INSERT OR REPLACE finalises it). Individual
liquidation records dedupe via composite primary key; empty polls are normal.

Usage:
    poetry run python scripts/collect_deriv_snapshots.py            # resident, 300s
    poetry run python scripts/collect_deriv_snapshots.py --once     # single cycle (test)
    poetry run python scripts/collect_deriv_snapshots.py --interval 60

Long-running deployment:
    # nohup
    mkdir -p logs && nohup poetry run python scripts/collect_deriv_snapshots.py \\
        > logs/deriv_snapshots.log 2>&1 &

    # macOS launchd: ~/Library/LaunchAgents/com.aihedgefund.deriv.plist
    <?xml version="1.0" encoding="UTF-8"?>
    <!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
      "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
    <plist version="1.0"><dict>
      <key>Label</key><string>com.aihedgefund.deriv</string>
      <key>ProgramArguments</key><array>
        <string>poetry</string><string>run</string><string>python</string>
        <string>scripts/collect_deriv_snapshots.py</string>
      </array>
      <key>WorkingDirectory</key><string>/Users/jeffryjia/Vibe/ai-hedge-fund</string>
      <key>RunAtLoad</key><true/>
      <key>KeepAlive</key><true/>
      <key>StandardOutPath</key><string>logs/deriv_snapshots.log</string>
      <key>StandardErrorPath</key><string>logs/deriv_snapshots.log</string>
    </dict></plist>
    # then: launchctl load ~/Library/LaunchAgents/com.aihedgefund.deriv.plist

Example analytics (the daily-feature prototypes this data feeds):

    -- Last 1h: OI change, BTC perp (base units)
    SELECT ROUND(100.0 * (a.oi_base - b.oi_base) / b.oi_base, 2) AS oi_pct_1h
    FROM (SELECT oi_base FROM oi_snapshots WHERE contract='BTC_USDT'
           ORDER BY recv_ts_ms DESC LIMIT 1) a,
         (SELECT oi_base FROM oi_snapshots WHERE contract='BTC_USDT'
           AND recv_ts_ms <= (SELECT MAX(recv_ts_ms) - 3600000
                                FROM oi_snapshots WHERE contract='BTC_USDT')
           ORDER BY recv_ts_ms DESC LIMIT 1) b;

    -- Last 24h: liquidated USD notional per contract per side
    SELECT contract,
           ROUND(SUM(long_liq_usd_new), 0)  AS long_liq_usd,
           ROUND(SUM(short_liq_usd_new), 0) AS short_liq_usd
    FROM liq_window_agg
    WHERE window_ts > CAST(strftime('%s','now') AS INTEGER) - 86400
    GROUP BY contract;

    -- Latest crowd positioning: long/short ratios, BTC perp
    SELECT window_ts, lsr_taker, lsr_account, top_lsr_size, top_lsr_account
    FROM lssr_snapshots WHERE contract='BTC_USDT'
    ORDER BY window_ts DESC LIMIT 1;
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

CONTRACTS = ["BTC_USDT", "ETH_USDT"]

DEFAULT_INTERVAL = 300          # seconds between polls (matches 5m stats windows)
REQUEST_TIMEOUT = 15            # seconds per HTTP request
BACKOFF_INITIAL = 5             # seconds, doubles on consecutive failures ...
BACKOFF_MAX = 300               # ... capped at 5 minutes
STATS_INTERVAL = 3600           # print stats line every hour
STATS_LOOKBACK_S = 6 * 3600     # restart backfill window for contract_stats
STATS_LIMIT = 100               # rows per contract_stats call (covers lookback)
FALLBACK_QUANTO = {             # base units per contract if tickers lookup fails
    "BTC_USDT": 0.0001,
    "ETH_USDT": 0.01,
}


# ---------------------------------------------------------------------------
# SQLite store
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS oi_snapshots (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    recv_ts_ms    INTEGER NOT NULL,  -- local receive time, epoch ms
    contract      TEXT    NOT NULL,  -- 'BTC_USDT' | 'ETH_USDT'
    oi_base       REAL    NOT NULL,  -- open interest in base units (BTC/ETH)
    oi_contracts  REAL    NOT NULL,  -- raw open interest in contracts
    mark_price    REAL,              -- USDT
    funding_rate  REAL               -- current funding rate (per interval)
);
CREATE INDEX IF NOT EXISTS idx_oi_contract_ts ON oi_snapshots (contract, recv_ts_ms);

CREATE TABLE IF NOT EXISTS lssr_snapshots (
    contract          TEXT    NOT NULL,
    window_ts         INTEGER NOT NULL, -- unix sec, 5m window open (Gate 'time')
    lsr_taker         REAL,             -- long/short taker-volume ratio
    lsr_account       REAL,             -- long/short account ratio
    top_lsr_size      REAL,             -- top-trader (by size) long/short ratio
    top_lsr_account   REAL,             -- top-trader (by account) ratio
    long_users        INTEGER,
    short_users       INTEGER,
    long_taker_size   REAL,             -- taker volume in contracts
    short_taker_size  REAL,
    open_interest     INTEGER,          -- contracts (window snapshot)
    open_interest_usd REAL,
    mark_price        REAL,
    last_funding_rate REAL,
    recv_ts_ms        INTEGER NOT NULL,
    PRIMARY KEY (contract, window_ts)
);

CREATE TABLE IF NOT EXISTS liq_window_agg (
    contract         TEXT    NOT NULL,
    window_ts        INTEGER NOT NULL,  -- unix sec, 5m window open
    long_liq_size    REAL,              -- contracts liquidated (long side)
    short_liq_size   REAL,
    long_liq_amount  REAL,              -- base units liquidated
    short_liq_amount REAL,
    long_liq_usd     REAL,              -- USD notional liquidated
    short_liq_usd    REAL,
    long_liq_usd_new REAL,              -- USD notional, newer basis
    short_liq_usd_new REAL,
    recv_ts_ms       INTEGER NOT NULL,
    PRIMARY KEY (contract, window_ts)
);

CREATE TABLE IF NOT EXISTS liquidations (
    contract    TEXT    NOT NULL,
    ts_ms       INTEGER NOT NULL,       -- exchange liquidation time, epoch ms
    side        TEXT    NOT NULL,       -- 'long' | 'short' (side liquidated)
    price       REAL    NOT NULL,       -- USDT
    size        REAL    NOT NULL,       -- base units
    leverage    REAL,
    recv_ts_ms  INTEGER NOT NULL,
    PRIMARY KEY (contract, ts_ms, side, price, size)
);
CREATE INDEX IF NOT EXISTS idx_liq_ts ON liquidations (ts_ms);
"""


class DerivStore:
    """Thin SQLite wrapper. One transaction per write batch."""

    def __init__(self, db_path: str):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def insert_oi(self, rows: list[tuple]) -> None:
        self.conn.executemany(
            "INSERT INTO oi_snapshots"
            " (recv_ts_ms, contract, oi_base, oi_contracts, mark_price, funding_rate)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )
        self.conn.commit()

    def upsert_lssr(self, rows: list[tuple]) -> int:
        """INSERT OR REPLACE -- the in-progress 5m window gets finalised."""
        cur = self.conn.executemany(
            "INSERT OR REPLACE INTO lssr_snapshots"
            " (contract, window_ts, lsr_taker, lsr_account, top_lsr_size,"
            "  top_lsr_account, long_users, short_users, long_taker_size,"
            "  short_taker_size, open_interest, open_interest_usd, mark_price,"
            "  last_funding_rate, recv_ts_ms)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        self.conn.commit()
        return cur.rowcount

    def upsert_liq_agg(self, rows: list[tuple]) -> int:
        cur = self.conn.executemany(
            "INSERT OR REPLACE INTO liq_window_agg"
            " (contract, window_ts, long_liq_size, short_liq_size,"
            "  long_liq_amount, short_liq_amount, long_liq_usd, short_liq_usd,"
            "  long_liq_usd_new, short_liq_usd_new, recv_ts_ms)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        self.conn.commit()
        return cur.rowcount

    def insert_liquidations(self, rows: list[tuple]) -> int:
        """INSERT OR IGNORE -- composite PK dedupes restarts/re-polls."""
        cur = self.conn.executemany(
            "INSERT OR IGNORE INTO liquidations"
            " (contract, ts_ms, side, price, size, leverage, recv_ts_ms)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        self.conn.commit()
        return cur.rowcount

    def stats_from_ts(self, contract: str, lookback_s: int) -> int:
        """Fetch windows after the newest stored one; fall back to lookback."""
        row = self.conn.execute(
            "SELECT MAX(window_ts) FROM lssr_snapshots WHERE contract = ?",
            (contract,),
        ).fetchone()
        newest = row[0] or 0
        return max(int(time.time()) - lookback_s, newest)

    def counts(self) -> dict[str, int]:
        out = {}
        for table in ("oi_snapshots", "lssr_snapshots", "liq_window_agg",
                      "liquidations"):
            out[table] = self.conn.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
        return out


# ---------------------------------------------------------------------------
# Gate.io REST helpers
# ---------------------------------------------------------------------------

class AuthRequiredError(RuntimeError):
    """Endpoint requires an API key (degradation path, not a cycle failure)."""


def _get(path: str, params: dict) -> dict | list:
    """GET with timeout; raise RuntimeError on non-200 so caller can back off."""
    resp = requests.get(
        f"{GATE_BASE}{path}", params=params, timeout=REQUEST_TIMEOUT
    )
    if resp.status_code == 429:
        raise RuntimeError(f"HTTP 429 rate limited: {path}")
    if resp.status_code != 200:
        # Gate signals "login required" endpoints with 400 + this label
        try:
            label = resp.json().get("label", "")
        except Exception:
            label = ""
        if "MISSING_REQUIRED_HEADER" in label or resp.status_code == 401:
            raise AuthRequiredError(
                f"{path} requires API-key auth ({label or resp.status_code})"
            )
        raise RuntimeError(f"HTTP {resp.status_code} from {path}")
    return resp.json()


def fetch_ticker(contract: str) -> dict:
    """Real-time perp ticker: has total_size (= OI in contracts), mark price,
    funding rate and the quanto multiplier inline."""
    d = _get("/futures/usdt/tickers", {"contract": contract})
    if isinstance(d, list):  # Gate returns a list even for single contract
        d = d[0]
    return d


def fetch_contract_stats(contract: str, from_ts: int) -> list[dict]:
    """5m-window stats: LSR, top-trader LSR, user counts, liquidation
    aggregates, OI, mark price, funding. Includes the in-progress window."""
    return _get(
        "/futures/usdt/contract_stats",
        {"contract": contract, "interval": "5m", "from": from_ts,
         "limit": STATS_LIMIT},
    )


def fetch_liquidates(contract: str) -> list[dict]:
    """Recent individual liquidation records. Currently raises
    AuthRequiredError on this network (endpoint moved behind API keys);
    kept so the collector picks it up again if Gate reopens it."""
    return _get("/futures/usdt/liquidates", {"contract": contract})


# ---------------------------------------------------------------------------
# Collector loop
# ---------------------------------------------------------------------------

def run_cycle(
    store: DerivStore,
    quantos: dict[str, float],
    liq_state: dict,
) -> dict[str, int]:
    """One poll cycle. Returns per-table write counts.

    Failures of the (auth-blocked) liquidates endpoint are isolated and never
    fail the cycle; OI/stats fetch failures propagate for backoff handling.
    """
    recv_ms = int(time.time() * 1000)
    counts = {"oi": 0, "lssr": 0, "liq_agg": 0, "liq": 0, "liq_endpoint_err": 0}

    # 1. Real-time OI snapshots (tickers: total_size x quanto_multiplier)
    oi_rows = []
    for contract in CONTRACTS:
        t = fetch_ticker(contract)
        quanto = float(t.get("quanto_multiplier") or FALLBACK_QUANTO[contract])
        quantos[contract] = quanto
        oi_rows.append((
            recv_ms,
            contract,
            float(t["total_size"]) * quanto,           # base units
            float(t["total_size"]),                    # contracts
            float(t["mark_price"]),
            float(t["funding_rate"]),
        ))
    store.insert_oi(oi_rows)
    counts["oi"] = len(oi_rows)

    # 2. 5m window stats -> long/short ratios + liquidation aggregates
    for contract in CONTRACTS:
        stats = fetch_contract_stats(
            contract, store.stats_from_ts(contract, STATS_LOOKBACK_S)
        )
        lssr_rows, liq_rows = [], []
        for s in stats:
            lssr_rows.append((
                contract, int(s["time"]),
                s.get("lsr_taker"), s.get("lsr_account"),
                s.get("top_lsr_size"), s.get("top_lsr_account"),
                s.get("long_users"), s.get("short_users"),
                s.get("long_taker_size"), s.get("short_taker_size"),
                s.get("open_interest"), s.get("open_interest_usd"),
                s.get("mark_price"), s.get("last_funding_rate"),
                recv_ms,
            ))
            liq_rows.append((
                contract, int(s["time"]),
                s.get("long_liq_size"), s.get("short_liq_size"),
                s.get("long_liq_amount"), s.get("short_liq_amount"),
                s.get("long_liq_usd"), s.get("short_liq_usd"),
                s.get("long_liq_usd_new"), s.get("short_liq_usd_new"),
                recv_ms,
            ))
        counts["lssr"] += store.upsert_lssr(lssr_rows)
        counts["liq_agg"] += store.upsert_liq_agg(liq_rows)

    # 3. Individual liquidation records (currently auth-blocked on Gate).
    #    Probe while enabled; isolate any failure from the rest of the cycle.
    if not liq_state["disabled"]:
        for contract in CONTRACTS:
            try:
                recs = fetch_liquidates(contract)
                quanto = quantos.get(contract, FALLBACK_QUANTO[contract])
                rows = [(
                    contract,
                    int(r["time"]) * 1000,
                    r["side"],
                    float(r["price"]),
                    float(r["size"]) * quanto,   # contracts -> base units
                    float(r.get("leverage") or 0),
                    recv_ms,
                ) for r in recs]
                counts["liq"] += store.insert_liquidations(rows)
            except AuthRequiredError as e:
                liq_state["disabled"] = True
                print(
                    f"[{datetime.now(tz=timezone.utc).strftime('%H:%M:%S')}] "
                    f"[WARN] liquidates endpoint unavailable ({e}); "
                    f"degrading to contract_stats window aggregates "
                    f"(liq_window_agg). Will re-probe hourly."
                )
                counts["liq_endpoint_err"] += 1
                break
            except Exception as e:
                # Transient error on a best-effort endpoint: note and continue
                print(
                    f"[{datetime.now(tz=timezone.utc).strftime('%H:%M:%S')}] "
                    f"[WARN] liquidates fetch failed for {contract} ({e}); "
                    f"continuing without per-record data"
                )
                counts["liq_endpoint_err"] += 1

    return counts


def run_collector(interval: int, db_path: str, once: bool) -> None:
    store = DerivStore(db_path)

    print("=== Gate.io Derivatives Snapshot Collector ===")
    print(f"  Interval : {interval}s (contracts: {', '.join(CONTRACTS)})")
    print(f"  DB path  : {db_path}")
    print(f"  Started  : {datetime.now(tz=timezone.utc).isoformat()}")
    print(f"  Stop     : Ctrl+C or SIGTERM\n")

    _stop = False

    def _handle_signal(signum, frame):
        nonlocal _stop
        _stop = True
        print("\n[collector] Shutdown signal received ...")

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    quantos: dict[str, float] = {}
    liq_state = {"disabled": False}

    # Stats bookkeeping
    stat = {"oi": 0, "lssr": 0, "liq_agg": 0, "liq": 0, "cycles": 0}
    stat_started = time.time()
    backoff = BACKOFF_INITIAL

    while not _stop:
        t0 = time.time()
        try:
            c = run_cycle(store, quantos, liq_state)
            for k in ("oi", "lssr", "liq_agg", "liq"):
                stat[k] += c[k]
            stat["cycles"] += 1
            backoff = BACKOFF_INITIAL  # success resets the backoff
            print(
                f"[{datetime.now(tz=timezone.utc).strftime('%H:%M:%S')}] "
                f"cycle ok: oi={c['oi']} lssr_windows={c['lssr']} "
                f"liq_agg_windows={c['liq_agg']} liq_records={c['liq']}"
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

        # Hourly stats (+ hourly re-probe of the disabled liquidates endpoint)
        if time.time() - stat_started >= STATS_INTERVAL:
            totals = store.counts()
            print(
                f"[stats] last {STATS_INTERVAL // 60}min: "
                f"cycles={stat['cycles']} oi_rows={stat['oi']} "
                f"lssr_upserts={stat['lssr']} liq_agg_upserts={stat['liq_agg']} "
                f"liq_records={stat['liq']} "
                f"liq_endpoint={'disabled (auth)' if liq_state['disabled'] else 'ok'} "
                f"| DB totals: {totals}"
            )
            if liq_state["disabled"]:
                print("[stats] re-probing liquidates endpoint ...")
                liq_state["disabled"] = False
            stat = {"oi": 0, "lssr": 0, "liq_agg": 0, "liq": 0, "cycles": 0}
            stat_started = time.time()

        # Sleep out the remainder of the interval, waking on stop signals
        deadline = t0 + interval
        while time.time() < deadline and not _stop:
            time.sleep(1)

    print("\n[collector] Shutdown complete.")
    print(f"  DB totals: {store.counts()}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    # Line-buffer stdout so nohup/launchd logs appear in real time
    sys.stdout.reconfigure(line_buffering=True)

    parser = argparse.ArgumentParser(
        description="Gate.io derivatives collector (OI, liquidation flow, "
                    "long/short ratios for BTC/ETH perps)"
    )
    parser.add_argument(
        "--interval", type=int, default=DEFAULT_INTERVAL,
        help=f"Poll interval in seconds (default: {DEFAULT_INTERVAL})",
    )
    parser.add_argument(
        "--db", default=os.path.join(
            os.path.dirname(__file__), "..", "data", "deriv_snapshots.db"
        ),
        help="SQLite database path (default: data/deriv_snapshots.db)",
    )
    parser.add_argument(
        "--once", action="store_true",
        help="Run a single poll cycle and exit (for testing)",
    )
    args = parser.parse_args()

    db_path = os.path.abspath(args.db)
    run_collector(interval=args.interval, db_path=db_path, once=args.once)


if __name__ == "__main__":
    main()
