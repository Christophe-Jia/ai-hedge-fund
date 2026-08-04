"""
SQLite-backed store for on-chain metrics.

Supported data sources (in priority order):
  1. Glassnode API (requires GLASSNODE_API_KEY env var)
  2. CoinGecko public API (free, no key required — market_cap as MVRV proxy)

Database: data/onchain_metrics.db

Table schema:
  onchain_metrics(asset TEXT, metric TEXT, ts_ms INTEGER, value REAL)
  PRIMARY KEY (asset, metric, ts_ms)

Metrics stored:
  - market_cap         : Total market capitalisation (USD)
  - price              : Daily close price (USD)
  - mvrv_approx        : Approximate MVRV = market_cap / 21M * avg_cost_basis
                         (real MVRV requires Glassnode; this is a proxy via
                          market_cap growth normalisation)
  - nvt_approx         : NVT proxy = market_cap / 90d_avg_volume
  - glassnode_mvrv     : True MVRV from Glassnode (if API key provided)
  - glassnode_nvt      : True NVT from Glassnode (if API key provided)
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone

import sqlalchemy as sa

_DEFAULT_DB_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "onchain_metrics.db"
)

_DDL = """
CREATE TABLE IF NOT EXISTS onchain_metrics (
    asset   TEXT    NOT NULL,
    metric  TEXT    NOT NULL,
    ts_ms   INTEGER NOT NULL,
    value   REAL    NOT NULL,
    PRIMARY KEY (asset, metric, ts_ms)
);
CREATE INDEX IF NOT EXISTS idx_onchain_lookup
    ON onchain_metrics (asset, metric, ts_ms);
"""

_COINGECKO_ID_MAP = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
}

# CoinGecko free tier: ~50 req/min without API key
_COINGECKO_BASE = "https://api.coingecko.com/api/v3"


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


def _fetch_coingecko_market_chart(
    coingecko_id: str,
    start_ts_s: int,
    end_ts_s: int,
) -> dict:
    """
    Fetch market_caps, prices, and total_volumes from CoinGecko /market_chart/range.
    Returns dict with keys: prices, market_caps, total_volumes.
    Each is a list of [timestamp_ms, value].
    """
    try:
        import requests
    except ImportError:
        raise ImportError("requests is required. Run: poetry add requests")

    url = f"{_COINGECKO_BASE}/coins/{coingecko_id}/market_chart/range"
    params = {
        "vs_currency": "usd",
        "from": start_ts_s,
        "to": end_ts_s,
    }
    try:
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code == 429:
            # Rate limited — wait and retry once
            time.sleep(60)
            resp = requests.get(url, params=params, timeout=30)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"[onchain_store] CoinGecko fetch error: {e}")
    return {}


def _fetch_glassnode_metric(
    metric_path: str,
    asset: str,
    start_ts_s: int,
    end_ts_s: int,
    api_key: str,
) -> list[dict]:
    """Fetch a Glassnode metric using the v1 API with authentication."""
    try:
        import requests
    except ImportError:
        raise ImportError("requests is required.")

    url = f"https://api.glassnode.com/v1/metrics/{metric_path}"
    params = {
        "a": asset,
        "i": "24h",
        "s": start_ts_s,
        "u": end_ts_s,
        "api_key": api_key,
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"[onchain_store] Glassnode fetch error for {metric_path}: {e}")
    return []


class OnchainMetricStore:
    """
    SQLite-backed store for on-chain metrics with CoinGecko and Glassnode support.

    Usage:
        store = OnchainMetricStore()
        store.backfill("BTC", start_ts_ms, end_ts_ms)
        df = store.get_metrics("BTC", ["market_cap", "mvrv_approx"], start_ts_ms, end_ts_ms)
    """

    def __init__(self, db_path: str = _DEFAULT_DB_PATH) -> None:
        self._engine = _make_engine(db_path)
        self._glassnode_key = os.environ.get("GLASSNODE_API_KEY", "").strip()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def backfill(
        self,
        asset: str,
        start_ts_ms: int,
        end_ts_ms: int,
    ) -> int:
        """
        Fetch and persist on-chain metrics for the given asset and date range.

        Returns the number of rows written.
        Uses Glassnode if GLASSNODE_API_KEY is set, otherwise CoinGecko.
        """
        total_written = 0

        cg_id = _COINGECKO_ID_MAP.get(asset)
        if cg_id:
            total_written += self._backfill_coingecko(asset, cg_id, start_ts_ms, end_ts_ms)

        if self._glassnode_key:
            total_written += self._backfill_glassnode(asset, start_ts_ms, end_ts_ms)

        return total_written

    def get_metrics(
        self,
        asset: str,
        metrics: list[str],
        start_ts_ms: int,
        end_ts_ms: int,
    ) -> list[dict]:
        """
        Return on-chain metrics as a list of dicts:
          [{"ts_ms": int, "metric": str, "value": float}, ...]

        Ordered by ts_ms ascending.
        """
        placeholders = ", ".join(f":m{i}" for i in range(len(metrics)))
        params: dict = {
            "asset": asset,
            "start_ts": start_ts_ms,
            "end_ts": end_ts_ms,
        }
        for i, m in enumerate(metrics):
            params[f"m{i}"] = m

        sql = sa.text(
            f"""
            SELECT ts_ms, metric, value
            FROM onchain_metrics
            WHERE asset = :asset
              AND metric IN ({placeholders})
              AND ts_ms >= :start_ts
              AND ts_ms < :end_ts
            ORDER BY ts_ms ASC
            """
        )
        with self._engine.connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [{"ts_ms": int(r[0]), "metric": r[1], "value": float(r[2])} for r in rows]

    def get_latest_value(self, asset: str, metric: str, before_ts_ms: int) -> float | None:
        """Return the most recent value of a metric before the given timestamp."""
        sql = sa.text(
            """
            SELECT value FROM onchain_metrics
            WHERE asset = :asset AND metric = :metric AND ts_ms < :ts
            ORDER BY ts_ms DESC LIMIT 1
            """
        )
        with self._engine.connect() as conn:
            row = conn.execute(sql, {"asset": asset, "metric": metric, "ts": before_ts_ms}).fetchone()
        return float(row[0]) if row else None

    def get_latest_ts(self, asset: str, metric: str) -> int | None:
        """Return the most recent stored timestamp (ms) or None."""
        sql = sa.text(
            "SELECT MAX(ts_ms) FROM onchain_metrics WHERE asset = :asset AND metric = :metric"
        )
        with self._engine.connect() as conn:
            row = conn.execute(sql, {"asset": asset, "metric": metric}).fetchone()
        return int(row[0]) if row and row[0] is not None else None

    def has_data(self, asset: str, start_ts_ms: int, end_ts_ms: int) -> bool:
        """Return True if there is at least one row for asset in the given range."""
        sql = sa.text(
            """
            SELECT COUNT(*) FROM onchain_metrics
            WHERE asset = :asset AND ts_ms >= :start_ts AND ts_ms < :end_ts
            """
        )
        with self._engine.connect() as conn:
            row = conn.execute(sql, {"asset": asset, "start_ts": start_ts_ms, "end_ts": end_ts_ms}).fetchone()
        return bool(row and row[0] > 0)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _upsert(self, records: list[dict]) -> int:
        if not records:
            return 0
        sql = sa.text(
            """
            INSERT OR REPLACE INTO onchain_metrics (asset, metric, ts_ms, value)
            VALUES (:asset, :metric, :ts_ms, :value)
            """
        )
        with self._engine.begin() as conn:
            conn.execute(sql, records)
        return len(records)

    def _backfill_coingecko(
        self,
        asset: str,
        coingecko_id: str,
        start_ts_ms: int,
        end_ts_ms: int,
    ) -> int:
        start_ts_s = start_ts_ms // 1000
        end_ts_s = end_ts_ms // 1000

        data = _fetch_coingecko_market_chart(coingecko_id, start_ts_s, end_ts_s)
        if not data:
            return 0

        prices = {int(p[0]): float(p[1]) for p in data.get("prices", [])}
        market_caps = {int(p[0]): float(p[1]) for p in data.get("market_caps", [])}
        volumes = {int(p[0]): float(p[1]) for p in data.get("total_volumes", [])}

        # Align all series on common timestamps
        all_ts = sorted(set(prices) | set(market_caps) | set(volumes))

        records: list[dict] = []
        # Compute 90-day rolling average volume for NVT proxy
        vol_list = [(ts, volumes.get(ts, 0.0)) for ts in sorted(volumes)]

        for ts_ms in all_ts:
            if ts_ms < start_ts_ms or ts_ms >= end_ts_ms:
                continue

            price = prices.get(ts_ms)
            mcap = market_caps.get(ts_ms)
            vol = volumes.get(ts_ms)

            if price is not None:
                records.append({"asset": asset, "metric": "price", "ts_ms": ts_ms, "value": price})
            if mcap is not None:
                records.append({"asset": asset, "metric": "market_cap", "ts_ms": ts_ms, "value": mcap})
            if vol is not None:
                records.append({"asset": asset, "metric": "volume", "ts_ms": ts_ms, "value": vol})

            # NVT proxy: market_cap / 90d_avg_daily_volume
            if mcap is not None and vol_list:
                # Use volumes up to this timestamp, last 90 days
                _90d_ms = 90 * 86_400_000
                recent_vols = [v for (t, v) in vol_list if t <= ts_ms and t >= ts_ms - _90d_ms]
                if recent_vols:
                    avg_vol = sum(recent_vols) / len(recent_vols)
                    if avg_vol > 0:
                        nvt_proxy = mcap / avg_vol
                        records.append({"asset": asset, "metric": "nvt_approx", "ts_ms": ts_ms, "value": nvt_proxy})

        return self._upsert(records)

    def _backfill_glassnode(
        self,
        asset: str,
        start_ts_ms: int,
        end_ts_ms: int,
    ) -> int:
        start_ts_s = start_ts_ms // 1000
        end_ts_s = end_ts_ms // 1000

        records: list[dict] = []

        mvrv_data = _fetch_glassnode_metric(
            "market/mvrv", asset, start_ts_s, end_ts_s, self._glassnode_key
        )
        for row in mvrv_data:
            if row.get("v") is not None:
                records.append({
                    "asset": asset,
                    "metric": "glassnode_mvrv",
                    "ts_ms": int(row["t"]) * 1000,
                    "value": float(row["v"]),
                })

        # Respect Glassnode rate limit between calls
        time.sleep(0.5)

        nvt_data = _fetch_glassnode_metric(
            "indicators/nvt", asset, start_ts_s, end_ts_s, self._glassnode_key
        )
        for row in nvt_data:
            if row.get("v") is not None:
                records.append({
                    "asset": asset,
                    "metric": "glassnode_nvt",
                    "ts_ms": int(row["t"]) * 1000,
                    "value": float(row["v"]),
                })

        return self._upsert(records)
