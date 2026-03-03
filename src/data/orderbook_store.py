"""
SQLite-backed store for tick-by-tick trade data and order book snapshots.

Designed for real-time collection via WebSocket feeds. Mirrors the
_make_engine + raw SQL pattern from historical_store.py.

Database: data/orderbook_trades.db (WAL mode for concurrent reads/writes)

Tables:
  - trades: individual trade executions (deduped by trade_id)
  - order_book_snapshots: periodic L2 order book snapshots (top N levels)
"""

from __future__ import annotations

import os
import time

import sqlalchemy as sa

_DEFAULT_DB_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "orderbook_trades.db"
)

_DDL = """
CREATE TABLE IF NOT EXISTS trades (
    trade_id  TEXT    PRIMARY KEY,
    ts_ms     INTEGER NOT NULL,
    symbol    TEXT    NOT NULL,
    price     REAL    NOT NULL,
    amount    REAL    NOT NULL,
    side      TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_trades_symbol_ts ON trades (symbol, ts_ms);

CREATE TABLE IF NOT EXISTS order_book_snapshots (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_ms   INTEGER NOT NULL,
    symbol  TEXT    NOT NULL,
    side    TEXT    NOT NULL,
    price   REAL    NOT NULL,
    size    REAL    NOT NULL,
    level   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ob_symbol_ts ON order_book_snapshots (symbol, ts_ms)
"""


def _make_engine(db_path: str) -> sa.Engine:
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    engine = sa.create_engine(f"sqlite:///{db_path}", future=True)
    with engine.connect() as conn:
        # Enable WAL mode for concurrent read/write access
        conn.execute(sa.text("PRAGMA journal_mode=WAL"))
        for stmt in _DDL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(sa.text(stmt))
        conn.commit()
    return engine


class OrderBookTradeStore:
    """
    Local SQLite store for real-time trade ticks and order book snapshots.

    Usage:
        store = OrderBookTradeStore()
        store.insert_trades([{
            "trade_id": "12345",
            "ts_ms": 1740700800000,
            "symbol": "BTC/USD",
            "price": 95000.0,
            "amount": 0.1,
            "side": "buy",
        }])
        store.insert_order_book_snapshot(ts_ms, "BTC/USD", bids, asks)
    """

    def __init__(self, db_path: str = _DEFAULT_DB_PATH) -> None:
        self._engine = _make_engine(db_path)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def insert_trades(self, trades: list[dict]) -> int:
        """
        Insert trade records. Silently ignores duplicates (PRIMARY KEY = trade_id).
        Returns the number of new rows inserted.

        Each dict must have: trade_id, ts_ms, symbol, price, amount, side.
        """
        if not trades:
            return 0

        sql = sa.text(
            """
            INSERT OR IGNORE INTO trades (trade_id, ts_ms, symbol, price, amount, side)
            VALUES (:trade_id, :ts_ms, :symbol, :price, :amount, :side)
            """
        )
        with self._engine.begin() as conn:
            result = conn.execute(sql, trades)
        return result.rowcount

    def insert_order_book_snapshot(
        self,
        ts_ms: int,
        symbol: str,
        bids: list[list[float]],
        asks: list[list[float]],
    ) -> int:
        """
        Insert a full L2 order book snapshot.

        bids/asks: list of [price, size] pairs ordered best-first.
        Returns total rows inserted (len(bids) + len(asks)).
        """
        records = []
        for level, (price, size) in enumerate(bids):
            records.append({
                "ts_ms": ts_ms,
                "symbol": symbol,
                "side": "bid",
                "price": float(price),
                "size": float(size),
                "level": level,
            })
        for level, (price, size) in enumerate(asks):
            records.append({
                "ts_ms": ts_ms,
                "symbol": symbol,
                "side": "ask",
                "price": float(price),
                "size": float(size),
                "level": level,
            })

        if not records:
            return 0

        sql = sa.text(
            """
            INSERT INTO order_book_snapshots (ts_ms, symbol, side, price, size, level)
            VALUES (:ts_ms, :symbol, :side, :price, :size, :level)
            """
        )
        with self._engine.begin() as conn:
            conn.execute(sql, records)
        return len(records)

    def get_trade_count(self, symbol: str) -> int:
        """Return total number of trades stored for the given symbol."""
        sql = sa.text("SELECT COUNT(*) FROM trades WHERE symbol = :symbol")
        with self._engine.connect() as conn:
            row = conn.execute(sql, {"symbol": symbol}).fetchone()
        return int(row[0]) if row else 0

    def get_snapshot_count(self, symbol: str) -> int:
        """Return total number of order book snapshot rows for the given symbol."""
        sql = sa.text(
            "SELECT COUNT(*) FROM order_book_snapshots WHERE symbol = :symbol"
        )
        with self._engine.connect() as conn:
            row = conn.execute(sql, {"symbol": symbol}).fetchone()
        return int(row[0]) if row else 0

    def get_latest_trade_ts(self, symbol: str) -> int | None:
        """Return the most recent trade timestamp (ms) or None if empty."""
        sql = sa.text(
            "SELECT MAX(ts_ms) FROM trades WHERE symbol = :symbol"
        )
        with self._engine.connect() as conn:
            row = conn.execute(sql, {"symbol": symbol}).fetchone()
        return int(row[0]) if row and row[0] is not None else None

    def get_recent_trades(
        self,
        symbol: str,
        since_ts_ms: int,
        until_ts_ms: int | None = None,
        limit: int = 10_000,
    ) -> list[dict]:
        """
        Return trades for symbol in [since_ts_ms, until_ts_ms) ordered by ts_ms asc.
        If until_ts_ms is None, returns all trades from since_ts_ms onward.
        """
        if until_ts_ms is not None:
            sql = sa.text(
                """
                SELECT trade_id, ts_ms, symbol, price, amount, side
                FROM trades
                WHERE symbol = :symbol
                  AND ts_ms >= :since
                  AND ts_ms < :until
                ORDER BY ts_ms ASC
                LIMIT :limit
                """
            )
            params: dict = {"symbol": symbol, "since": since_ts_ms, "until": until_ts_ms, "limit": limit}
        else:
            sql = sa.text(
                """
                SELECT trade_id, ts_ms, symbol, price, amount, side
                FROM trades
                WHERE symbol = :symbol
                  AND ts_ms >= :since
                ORDER BY ts_ms ASC
                LIMIT :limit
                """
            )
            params = {"symbol": symbol, "since": since_ts_ms, "limit": limit}

        with self._engine.connect() as conn:
            rows = conn.execute(sql, params).fetchall()

        return [
            {"trade_id": r[0], "ts_ms": int(r[1]), "symbol": r[2],
             "price": float(r[3]), "amount": float(r[4]), "side": r[5]}
            for r in rows
        ]

    def get_latest_order_book_snapshot(
        self,
        symbol: str,
        before_ts_ms: int | None = None,
    ) -> dict:
        """
        Return the most recent order book snapshot as {bids: [...], asks: [...]}.
        Each side is a list of [price, size] pairs sorted best-first.
        Returns {"bids": [], "asks": []} if no snapshot is available.
        """
        if before_ts_ms is not None:
            sql = sa.text(
                "SELECT MAX(ts_ms) FROM order_book_snapshots WHERE symbol = :symbol AND ts_ms < :ts"
            )
            with self._engine.connect() as conn:
                row = conn.execute(sql, {"symbol": symbol, "ts": before_ts_ms}).fetchone()
        else:
            sql = sa.text(
                "SELECT MAX(ts_ms) FROM order_book_snapshots WHERE symbol = :symbol"
            )
            with self._engine.connect() as conn:
                row = conn.execute(sql, {"symbol": symbol}).fetchone()

        if not row or row[0] is None:
            return {"bids": [], "asks": []}

        latest_ts = int(row[0])

        sql = sa.text(
            """
            SELECT side, price, size, level
            FROM order_book_snapshots
            WHERE symbol = :symbol AND ts_ms = :ts
            ORDER BY level ASC
            """
        )
        with self._engine.connect() as conn:
            rows = conn.execute(sql, {"symbol": symbol, "ts": latest_ts}).fetchall()

        bids = [[float(r[1]), float(r[2])] for r in rows if r[0] == "bid"]
        asks = [[float(r[1]), float(r[2])] for r in rows if r[0] == "ask"]
        return {"bids": bids, "asks": asks, "ts_ms": latest_ts}
