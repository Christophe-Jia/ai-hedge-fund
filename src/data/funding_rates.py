"""
Funding rate historical store for BTC perpetual futures.

Binance USDT-M perpetuals settle funding every 8 hours (00:00, 08:00, 16:00 UTC).
This module fetches and caches historical funding rates in SQLite, then provides
look-ahead-safe access for backtesting cost calculations.
"""

from __future__ import annotations

import os
import time
from typing import List, Tuple

import sqlalchemy as sa

_DEFAULT_DB_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "btc_history.db"
)

_DDL = """
CREATE TABLE IF NOT EXISTS funding_rates (
    symbol TEXT    NOT NULL,
    ts     INTEGER NOT NULL,
    rate   REAL    NOT NULL,
    PRIMARY KEY (symbol, ts)
);
CREATE INDEX IF NOT EXISTS idx_funding_lookup
    ON funding_rates (symbol, ts);
"""


def _make_engine(db_path: str) -> sa.Engine:
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    engine = sa.create_engine(f"sqlite:///{db_path}", future=True)
    with engine.connect() as conn:
        for stmt in _DDL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(sa.text(stmt))
        conn.commit()
    return engine


def _fetch_ccxt_funding_history(
    symbol: str,
    start_ts_ms: int,
    end_ts_ms: int,
    exchange_id: str = "binance",
) -> list[dict]:
    """Fetch funding rate history from CCXT exchange."""
    try:
        import ccxt
    except ImportError:
        raise ImportError("ccxt is required. Run: poetry add ccxt")

    exchange_cls = getattr(ccxt, exchange_id)
    exchange: ccxt.Exchange = exchange_cls(
        {"enableRateLimit": True, "options": {"defaultType": "future"}}
    )

    all_rates: list[dict] = []
    since = start_ts_ms
    limit = 500
    last_ts = None

    while since < end_ts_ms:
        rates = exchange.fetch_funding_rate_history(
            symbol, since=since, limit=limit
        )
        if not rates:
            break
        filtered = [r for r in rates if r["timestamp"] < end_ts_ms]
        all_rates.extend(filtered)
        newest_ts = rates[-1]["timestamp"]
        if newest_ts >= end_ts_ms:
            break
        if newest_ts == last_ts:
            break
        last_ts = newest_ts
        since = newest_ts + 1
        time.sleep(exchange.rateLimit / 1000)

    return all_rates


class FundingRateStore:
    """
    SQLite-backed store for perpetual futures funding rates.

    Binance settles funding every 8 hours. Rates are stored as decimals
    (e.g., 0.0001 = 0.01%).

    Usage:
        store = FundingRateStore()
        store.fetch_and_store("BTC/USDT:USDT", start_ts_ms, end_ts_ms)
        rates = store.get_rates_in_range("BTC/USDT:USDT", start_ts_ms, end_ts_ms)
        cost = store.get_cumulative_funding_cost(
            "BTC/USDT:USDT",
            position_size_usd=10_000,
            is_long=True,
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
        )
    """

    def __init__(
        self,
        db_path: str = _DEFAULT_DB_PATH,
        exchange_id: str = "binance",
    ) -> None:
        self._engine = _make_engine(db_path)
        self._exchange_id = exchange_id

    def fetch_and_store(
        self,
        symbol: str,
        start_ts_ms: int,
        end_ts_ms: int,
    ) -> int:
        """
        Fetch funding rate history from the exchange and persist to SQLite.
        Returns the number of new records stored.
        """
        raw = _fetch_ccxt_funding_history(
            symbol, start_ts_ms, end_ts_ms, self._exchange_id
        )
        if not raw:
            return 0

        records = [
            {
                "symbol": symbol,
                "ts": int(r["timestamp"]),
                "rate": float(r["fundingRate"]),
            }
            for r in raw
            if r.get("fundingRate") is not None and r.get("timestamp") is not None
        ]

        if not records:
            return 0

        upsert_sql = sa.text(
            """
            INSERT OR REPLACE INTO funding_rates (symbol, ts, rate)
            VALUES (:symbol, :ts, :rate)
            """
        )
        with self._engine.begin() as conn:
            conn.execute(upsert_sql, records)

        return len(records)

    def get_rates_in_range(
        self,
        symbol: str,
        start_ts_ms: int,
        end_ts_ms: int,
    ) -> List[Tuple[int, float]]:
        """
        Return list of (timestamp_ms, rate) for funding events in
        [start_ts_ms, end_ts_ms).  Upper bound is exclusive (look-ahead safe).
        """
        sql = sa.text(
            """
            SELECT ts, rate
            FROM funding_rates
            WHERE symbol = :symbol
              AND ts >= :start_ts
              AND ts < :end_ts
            ORDER BY ts ASC
            """
        )
        with self._engine.connect() as conn:
            rows = conn.execute(
                sql,
                {"symbol": symbol, "start_ts": start_ts_ms, "end_ts": end_ts_ms},
            ).fetchall()

        return [(int(r[0]), float(r[1])) for r in rows]

    def get_cumulative_funding_cost(
        self,
        symbol: str,
        position_size_usd: float,
        is_long: bool,
        start_ts_ms: int,
        end_ts_ms: int,
    ) -> float:
        """
        Compute the total funding cash flow for a position held over the range.

        Sign convention (Binance standard):
          - Positive rate: longs pay shorts → long pays, short receives
          - Negative rate: shorts pay longs → short pays, long receives

        Returns a negative number when you PAY funding (a cost),
        and positive when you RECEIVE funding (a gain).

        Args:
            position_size_usd: Notional value of the position in USD.
            is_long: True for long position, False for short.
            start_ts_ms: Start of holding period (inclusive).
            end_ts_ms: End of holding period (exclusive, look-ahead guard).
        """
        rates = self.get_rates_in_range(symbol, start_ts_ms, end_ts_ms)

        total_cost = 0.0
        for _ts, rate in rates:
            payment = position_size_usd * rate
            if is_long:
                # Long pays when rate > 0, receives when rate < 0
                total_cost -= payment
            else:
                # Short receives when rate > 0, pays when rate < 0
                total_cost += payment

        return total_cost

    def upsert_rates(self, symbol: str, rows: list[tuple[int, float]]) -> int:
        """Insert or replace funding rate rows directly (for seed scripts).

        rows: list of (ts_ms, rate)
        Returns number of rows written.
        """
        if not rows:
            return 0
        records = [{"symbol": symbol, "ts": int(ts), "rate": float(rate)} for ts, rate in rows]
        upsert_sql = sa.text(
            "INSERT OR REPLACE INTO funding_rates (symbol, ts, rate) VALUES (:symbol, :ts, :rate)"
        )
        with self._engine.begin() as conn:
            conn.execute(upsert_sql, records)
        return len(records)

    def get_latest_ts(self, symbol: str) -> int | None:
        """Return the most recent stored funding rate timestamp (ms) or None."""
        sql = sa.text(
            "SELECT MAX(ts) FROM funding_rates WHERE symbol = :symbol"
        )
        with self._engine.connect() as conn:
            row = conn.execute(sql, {"symbol": symbol}).fetchone()
        return int(row[0]) if row and row[0] is not None else None
