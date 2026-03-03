"""
SQLite-backed store for Polymarket price tick data.

Persists fidelity=1 price history from the CLOB /prices-history API.
Each row is a (token_id, ts, price) triple; the composite primary key
prevents duplicates across incremental polling runs.

Database: data/polymarket_ticks.db (WAL mode for concurrent reads/writes)

Table:
  - price_ticks: per-token price ticks from Polymarket CLOB
  - markets:     metadata snapshot for tracked market tokens
"""

from __future__ import annotations

import os

import sqlalchemy as sa

_DEFAULT_DB_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "polymarket_ticks.db"
)

_DDL = """
CREATE TABLE IF NOT EXISTS price_ticks (
    token_id  TEXT    NOT NULL,
    ts        INTEGER NOT NULL,
    price     REAL    NOT NULL,
    PRIMARY KEY (token_id, ts)
);
CREATE INDEX IF NOT EXISTS idx_ticks_token_ts ON price_ticks (token_id, ts);

CREATE TABLE IF NOT EXISTS markets (
    token_id     TEXT    PRIMARY KEY,
    condition_id TEXT,
    question     TEXT,
    last_seen    INTEGER NOT NULL
);
"""


def _make_engine(db_path: str) -> sa.Engine:
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    engine = sa.create_engine(f"sqlite:///{db_path}", future=True)
    with engine.connect() as conn:
        conn.execute(sa.text("PRAGMA journal_mode=WAL"))
        for stmt in _DDL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(sa.text(stmt))
        conn.commit()
    return engine


class PolymarketTickStore:
    """
    Local SQLite store for Polymarket CLOB price ticks.

    Usage:
        store = PolymarketTickStore()
        store.upsert_ticks(token_id, [(ts_seconds, price), ...])
        latest = store.get_latest_ts(token_id)
        store.upsert_market(token_id, condition_id, question)
    """

    def __init__(self, db_path: str = _DEFAULT_DB_PATH) -> None:
        self._engine = _make_engine(db_path)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def upsert_ticks(self, token_id: str, ticks: list[tuple[int, float]]) -> int:
        """
        Insert or replace price tick rows. Silently ignores duplicates via
        PRIMARY KEY (token_id, ts).

        ticks: list of (ts_seconds, price)
        Returns number of rows written.
        """
        if not ticks:
            return 0

        records = [
            {"token_id": token_id, "ts": int(ts), "price": float(price)}
            for ts, price in ticks
        ]

        sql = sa.text(
            """
            INSERT OR REPLACE INTO price_ticks (token_id, ts, price)
            VALUES (:token_id, :ts, :price)
            """
        )
        with self._engine.begin() as conn:
            conn.execute(sql, records)

        return len(records)

    def get_latest_ts(self, token_id: str) -> int | None:
        """Return the most recent stored timestamp (seconds) for a token, or None."""
        sql = sa.text(
            "SELECT MAX(ts) FROM price_ticks WHERE token_id = :token_id"
        )
        with self._engine.connect() as conn:
            row = conn.execute(sql, {"token_id": token_id}).fetchone()
        return int(row[0]) if row and row[0] is not None else None

    def get_tick_count(self, token_id: str) -> int:
        """Return total number of ticks stored for the given token."""
        sql = sa.text(
            "SELECT COUNT(*) FROM price_ticks WHERE token_id = :token_id"
        )
        with self._engine.connect() as conn:
            row = conn.execute(sql, {"token_id": token_id}).fetchone()
        return int(row[0]) if row else 0

    def get_total_tick_count(self) -> int:
        """Return total number of ticks across all tokens."""
        sql = sa.text("SELECT COUNT(*) FROM price_ticks")
        with self._engine.connect() as conn:
            row = conn.execute(sql).fetchone()
        return int(row[0]) if row else 0

    def upsert_market(
        self,
        token_id: str,
        condition_id: str | None,
        question: str | None,
        last_seen: int,
    ) -> None:
        """Upsert market metadata row."""
        sql = sa.text(
            """
            INSERT OR REPLACE INTO markets (token_id, condition_id, question, last_seen)
            VALUES (:token_id, :condition_id, :question, :last_seen)
            """
        )
        with self._engine.begin() as conn:
            conn.execute(
                sql,
                {
                    "token_id": token_id,
                    "condition_id": condition_id,
                    "question": question,
                    "last_seen": last_seen,
                },
            )

    def list_markets(self) -> list[dict]:
        """Return all tracked markets as a list of dicts."""
        sql = sa.text(
            "SELECT token_id, condition_id, question, last_seen FROM markets ORDER BY last_seen DESC"
        )
        with self._engine.connect() as conn:
            rows = conn.execute(sql).fetchall()
        return [
            {
                "token_id": r[0],
                "condition_id": r[1],
                "question": r[2],
                "last_seen": r[3],
            }
            for r in rows
        ]

    def get_ticks(
        self, token_id: str, start_ts: int | None = None, end_ts: int | None = None
    ) -> list[tuple[int, float]]:
        """
        Return (ts, price) tuples for a token, optionally filtered by time range.
        ts is in Unix seconds.  Upper bound end_ts is exclusive (look-ahead safe).
        """
        conditions = ["token_id = :token_id"]
        params: dict = {"token_id": token_id}

        if start_ts is not None:
            conditions.append("ts >= :start_ts")
            params["start_ts"] = start_ts
        if end_ts is not None:
            conditions.append("ts < :end_ts")
            params["end_ts"] = end_ts

        sql = sa.text(
            f"SELECT ts, price FROM price_ticks WHERE {' AND '.join(conditions)} ORDER BY ts ASC"
        )
        with self._engine.connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [(int(r[0]), float(r[1])) for r in rows]
