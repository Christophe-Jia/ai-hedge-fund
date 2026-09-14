"""On-chain fundamentals diverging from price.

Weak signal: bearish when price outruns on-chain usage (rich valuation
metrics, positive price-vs-usage divergence, net exchange inflows),
bullish when usage leads price. Uses only data available STRICTLY BEFORE
`as_of` (no look-ahead).

Data source: OnchainMetricStore (data/onchain_metrics.db). Metric
selection degrades gracefully based on availability:

  valuation : mvrv (true MVRV) -> nvt_approx (proxy)
  usage     : active_addresses -> volume (exchange volume proxy)
  flows     : exchange_inflow_native - exchange_outflow_native (optional)

See src/data/onchain_store.py and scripts/backfill_onchain_v2.py.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone

import pandas as pd

from src.data.onchain_store import OnchainMetricStore

from .base import Signal, SignalOutput, direction_from_score

_MS_PER_DAY = 86_400_000
_STALENESS_MS = 7 * _MS_PER_DAY  # a metric older than this is stale
_SMOOTH_DAYS = 30                # rolling window for usage/flow smoothing

_VALUATION_CANDIDATES = ("mvrv", "nvt_approx")  # preference order
_CANDIDATE_METRICS = (
    "price",
    "volume",
    "nvt_approx",
    "mvrv",
    "active_addresses",
    "exchange_inflow_native",
    "exchange_outflow_native",
)
_OPTIONAL_METRICS = (
    "mvrv",
    "active_addresses",
    "exchange_inflow_native",
    "exchange_outflow_native",
)


def _zscore(s: pd.Series) -> float:
    """Z-score of the last observation vs the series mean/std."""
    if len(s) < 2:
        return 0.0
    std = s.std()
    if std is None or std <= 1e-12:
        return 0.0
    return float((s.iloc[-1] - s.mean()) / std)


def _daily_last(series: pd.Series) -> pd.Series:
    """Collapse (possibly hourly) observations to one value per day."""
    s = series.sort_index()
    s.index = pd.to_datetime(s.index, unit="ms")
    return s.groupby(s.index.floor("D")).last()


class OnchainFundamentalSignal(Signal):
    """On-chain fundamentals (valuation, usage, exchange flows) vs price."""

    name = "onchain_fundamental"
    description = (
        "Bearish when price outruns on-chain usage (rich MVRV/NVT, positive "
        "price-vs-usage divergence, net exchange inflows); bullish when "
        "usage leads price."
    )

    def __init__(
        self,
        store: OnchainMetricStore | None = None,
        asset: str = "BTC",
        lookback_days: int = 180,
        min_history_days: int = 120,
        sigma_cap: float = 2.0,
    ) -> None:
        """
        Args:
            store: metric store; defaults to the shared onchain_metrics.db.
            asset: asset symbol as stored (e.g. "BTC").
            lookback_days: trailing window for z-scores.
            min_history_days: minimum daily observations required, else
                generate() returns None.
            sigma_cap: z-score magnitude mapped to a full +/-1 score.
        """
        self._store = store if store is not None else OnchainMetricStore()
        self._asset = asset
        self._lookback_days = lookback_days
        self._min_history_days = min_history_days
        self._sigma_cap = sigma_cap

    # ------------------------------------------------------------------
    # Signal interface
    # ------------------------------------------------------------------

    def generate(self, as_of: pd.Timestamp) -> SignalOutput | None:
        end_ms = int(pd.Timestamp(as_of).value // 1_000_000)
        start_ms = end_ms - (self._lookback_days + 5) * _MS_PER_DAY

        rows = self._store.get_metrics(self._asset, list(_CANDIDATE_METRICS), start_ms, end_ms)

        daily: dict[str, pd.Series] = {}
        for metric in _CANDIDATE_METRICS:
            points = [
                (r["ts_ms"], r["value"]) for r in rows
                if r["metric"] == metric and r["value"] > 0.0
            ]
            if points:
                # Index stays as raw ts_ms ints; _daily_last converts them.
                s = pd.Series([v for _, v in points], index=[t for t, _ in points])
                daily[metric] = _daily_last(s)

        if "price" not in daily:
            return None

        # --- metric selection with graceful degradation ----------------
        valuation = next(
            (m for m in _VALUATION_CANDIDATES if len(daily.get(m, [])) >= self._min_history_days),
            None,
        )
        if valuation is None:
            return None
        usage = (
            "active_addresses"
            if len(daily.get("active_addresses", [])) >= self._min_history_days
            else "volume"
        )
        if usage not in daily:
            return None
        flows_available = (
            "exchange_inflow_native" in daily and "exchange_outflow_native" in daily
        )

        # --- align on days where all chosen series have observations ---
        columns: dict[str, pd.Series] = {
            "price": daily["price"],
            "valuation": daily[valuation],
            "usage": daily[usage],
        }
        if flows_available:
            columns["netflow"] = (
                daily["exchange_inflow_native"] - daily["exchange_outflow_native"]
            )
        window = pd.concat(columns, axis=1).dropna().tail(self._lookback_days)
        if len(window) < self._min_history_days:
            return None

        # --- component z-scores over the trailing window ---------------
        log_price = window["price"].map(math.log)
        log_usage = window["usage"].map(math.log)
        usage_ma = log_usage.rolling(_SMOOTH_DAYS, min_periods=1).mean()

        valuation_z = _zscore(window["valuation"])
        price_z = _zscore(log_price)
        usage_z = _zscore(usage_ma)
        divergence = price_z - usage_z  # price outrunning usage = bearish

        components = {"valuation_z": valuation_z, "divergence": divergence}
        if flows_available:
            netflow_ma = window["netflow"].rolling(_SMOOTH_DAYS, min_periods=1).mean()
            components["netflow_z"] = _zscore(netflow_ma)  # inflow-heavy = bearish

        raw = -sum(components.values()) / len(components)
        score = max(-1.0, min(1.0, raw / self._sigma_cap))
        coverage = min(1.0, len(window) / self._lookback_days)
        strength = min(1.0, abs(raw) / self._sigma_cap)
        confidence = max(0.0, min(1.0, 0.5 * coverage + 0.5 * strength))

        metadata = {
            "asset": self._asset,
            "as_of": str(as_of),
            **{k: v for k, v in components.items()},
            "raw": raw,
            "valuation_metric": valuation,
            "usage_metric": usage,
            "exchange_flows_used": flows_available,
            "price_z": price_z,
            "usage_z": usage_z,
            "days_used": len(window),
            "window_start": str(window.index[0].date()),
            "window_end": str(window.index[-1].date()),
        }
        return SignalOutput(
            score=score,
            direction=direction_from_score(score),
            confidence=confidence,
            metadata=metadata,
        )

    def data_health(self, now_ms: int | None = None) -> dict:
        """Check availability/freshness of the metrics this signal can use."""
        if now_ms is None:
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        metrics: dict[str, dict] = {}
        present: dict[str, int] = {}
        for metric in _CANDIDATE_METRICS:
            ts = self._store.get_latest_ts(self._asset, metric)
            if ts is not None:
                present[metric] = ts
            metrics[metric] = {
                "latest_ts": (
                    datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()
                    if ts is not None
                    else None
                ),
                "days_stale": (now_ms - ts) / _MS_PER_DAY if ts is not None else None,
            }

        has_valuation = any(m in present for m in _VALUATION_CANDIDATES)
        if "price" not in present or not has_valuation:
            status = "no_data"
        elif any((now_ms - ts) > _STALENESS_MS for ts in present.values()):
            status = "stale"
        elif not all(m in present for m in _OPTIONAL_METRICS):
            status = "degraded"  # proxies in use, core metrics missing
        else:
            status = "ok"

        return {
            "asset": self._asset,
            "status": status,
            "metrics": metrics,
        }
