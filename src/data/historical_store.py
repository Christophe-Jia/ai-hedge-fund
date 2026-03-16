"""
SQLite-backed OHLCV historical data store with CCXT fallback.

Provides look-ahead-safe access to historical candle data for BTC spot
and perpetual futures. All queries enforce an exclusive upper bound on
timestamp to prevent future data leakage during backtesting.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Literal

import pandas as pd
import sqlalchemy as sa

MarketType = Literal["spot", "perp"]

_DEFAULT_DB_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "btc_history.db"
)

_DDL = """
CREATE TABLE IF NOT EXISTS ohlcv (
    symbol      TEXT    NOT NULL,
    market_type TEXT    NOT NULL,
    timeframe   TEXT    NOT NULL,
    ts          INTEGER NOT NULL,
    open        REAL    NOT NULL,
    high        REAL    NOT NULL,
    low         REAL    NOT NULL,
    close       REAL    NOT NULL,
    volume      REAL    NOT NULL,
    PRIMARY KEY (symbol, market_type, timeframe, ts)
);
CREATE INDEX IF NOT EXISTS idx_ohlcv_lookup
    ON ohlcv (symbol, market_type, timeframe, ts);
"""

# Minimum coverage ratio before triggering a CCXT fetch
_MIN_COVERAGE = 0.95


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


def _expected_bars(timeframe: str, start_ts_ms: int, end_ts_ms: int) -> int:
    """Rough estimate of how many bars should exist in the range."""
    tf_ms = {
        "1m": 60_000,
        "5m": 300_000,
        "15m": 900_000,
        "1h": 3_600_000,
        "4h": 14_400_000,
        "1d": 86_400_000,
    }
    ms_per_bar = tf_ms.get(timeframe, 86_400_000)
    return max(1, (end_ts_ms - start_ts_ms) // ms_per_bar)


def _fetch_ccxt(
    symbol: str,
    market_type: MarketType,
    timeframe: str,
    start_ts_ms: int,
    end_ts_ms: int,
    exchange_id: str = "binance",
) -> list[list]:
    """Paginate through CCXT to fetch all candles in [start_ts_ms, end_ts_ms)."""
    try:
        import ccxt
    except ImportError:
        raise ImportError("ccxt is required. Run: poetry add ccxt")

    exchange_cls = getattr(ccxt, exchange_id)
    kwargs: dict = {"enableRateLimit": True}
    if market_type == "perp":
        kwargs["options"] = {"defaultType": "future"}
    exchange: ccxt.Exchange = exchange_cls(kwargs)

    all_ohlcv: list[list] = []
    since = start_ts_ms
    limit = 500
    last_ts = None

    while since < end_ts_ms:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=limit)
        if not ohlcv:
            break
        # Filter out candles beyond end_ts_ms
        filtered = [c for c in ohlcv if c[0] < end_ts_ms]
        all_ohlcv.extend(filtered)
        newest_ts = ohlcv[-1][0]
        # Stop conditions: reached end, or exchange returned no new data
        if newest_ts >= end_ts_ms:
            break
        if newest_ts == last_ts:
            # No progress — exchange returned same last timestamp, avoid infinite loop
            break
        last_ts = newest_ts
        since = newest_ts + 1
        time.sleep(exchange.rateLimit / 1000)

    return all_ohlcv


class HistoricalOHLCVStore:
    """
    Local SQLite cache of OHLCV candles with CCXT fallback.

    Usage:
        store = HistoricalOHLCVStore()
        df = store.get_ohlcv(
            symbol="BTC/USDT",
            market_type="spot",
            timeframe="1d",
            start_ts_ms=...,
            end_ts_ms=...,   # exclusive upper bound — look-ahead guard
        )
    """

    def __init__(
        self,
        db_path: str = _DEFAULT_DB_PATH,
        exchange_id: str = "binance",
    ) -> None:
        self._engine = _make_engine(db_path)
        self._exchange_id = exchange_id

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_ohlcv(
        self,
        symbol: str,
        market_type: MarketType,
        timeframe: str,
        start_ts_ms: int,
        end_ts_ms: int,
    ) -> pd.DataFrame:
        """
        Return OHLCV DataFrame for the requested range.

        The returned data is strictly bounded by [start_ts_ms, end_ts_ms).
        The look-ahead guard (ts < end_ts_ms) is enforced in both SQLite
        queries and on the final DataFrame.

        Columns: ts (int ms), open, high, low, close, volume
        """
        cached = self._query_sqlite(symbol, market_type, timeframe, start_ts_ms, end_ts_ms)

        expected = _expected_bars(timeframe, start_ts_ms, end_ts_ms)
        coverage = len(cached) / expected if expected > 0 else 0.0

        if coverage < _MIN_COVERAGE:
            raw = _fetch_ccxt(
                symbol, market_type, timeframe, start_ts_ms, end_ts_ms, self._exchange_id
            )
            if raw:
                self._upsert(symbol, market_type, timeframe, raw)
            cached = self._query_sqlite(symbol, market_type, timeframe, start_ts_ms, end_ts_ms)

        # Enforce look-ahead guard unconditionally
        if not cached.empty:
            cached = cached[cached["ts"] < end_ts_ms].copy()

        return cached.reset_index(drop=True)

    def upsert_ohlcv(
        self,
        symbol: str,
        market_type: MarketType,
        timeframe: str,
        rows: list[list],
    ) -> int:
        """
        Upsert raw CCXT candle rows [[ts_ms, open, high, low, close, volume], ...].
        Returns the number of rows written.
        """
        return self._upsert(symbol, market_type, timeframe, rows)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _query_sqlite(
        self,
        symbol: str,
        market_type: str,
        timeframe: str,
        start_ts_ms: int,
        end_ts_ms: int,
    ) -> pd.DataFrame:
        sql = sa.text(
            """
            SELECT ts, open, high, low, close, volume
            FROM ohlcv
            WHERE symbol = :symbol
              AND market_type = :market_type
              AND timeframe = :timeframe
              AND ts >= :start_ts
              AND ts < :end_ts
            ORDER BY ts ASC
            """
        )
        with self._engine.connect() as conn:
            result = conn.execute(
                sql,
                {
                    "symbol": symbol,
                    "market_type": market_type,
                    "timeframe": timeframe,
                    "start_ts": start_ts_ms,
                    "end_ts": end_ts_ms,
                },
            )
            rows = result.fetchall()

        if not rows:
            return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])

        return pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])

    def _upsert(
        self,
        symbol: str,
        market_type: str,
        timeframe: str,
        raw_rows: list[list],
    ) -> int:
        if not raw_rows:
            return 0

        records = [
            {
                "symbol": symbol,
                "market_type": market_type,
                "timeframe": timeframe,
                "ts": int(row[0]),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
            }
            for row in raw_rows
        ]

        upsert_sql = sa.text(
            """
            INSERT OR REPLACE INTO ohlcv
                (symbol, market_type, timeframe, ts, open, high, low, close, volume)
            VALUES
                (:symbol, :market_type, :timeframe, :ts, :open, :high, :low, :close, :volume)
            """
        )

        with self._engine.begin() as conn:
            conn.execute(upsert_sql, records)

        return len(records)

    def get_latest_ts(
        self, symbol: str, market_type: str, timeframe: str
    ) -> int | None:
        """Return the most recent stored timestamp (ms) or None if empty."""
        sql = sa.text(
            """
            SELECT MAX(ts) FROM ohlcv
            WHERE symbol = :symbol AND market_type = :market_type AND timeframe = :timeframe
            """
        )
        with self._engine.connect() as conn:
            row = conn.execute(
                sql,
                {"symbol": symbol, "market_type": market_type, "timeframe": timeframe},
            ).fetchone()
        return int(row[0]) if row and row[0] is not None else None
