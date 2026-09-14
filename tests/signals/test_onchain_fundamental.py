"""Tests for OnchainFundamentalSignal (on-chain fundamentals vs price)."""

from __future__ import annotations

import os
import random
from pathlib import Path

import pandas as pd
import pytest

from src.data.historical_store import HistoricalOHLCVStore
from src.data.onchain_store import OnchainMetricStore
from src.signals import OnchainFundamentalSignal, Signal

ASSET = "BTC"
N_DAYS = 200
BASE_TS = pd.Timestamp("2025-01-01", tz="UTC")
AS_OF = BASE_TS + pd.Timedelta(days=N_DAYS)  # day after the last data point


def build_store(
    tmp_path, series: dict[str, list[float]], asset: str = ASSET
) -> OnchainMetricStore:
    """Create a store with daily observations at 12:00 UTC."""
    store = OnchainMetricStore(db_path=str(tmp_path / "onchain_test.db"))
    records = []
    for metric, values in series.items():
        for i, v in enumerate(values):
            ts = BASE_TS + pd.Timedelta(days=i, hours=12)
            records.append({
                "asset": asset,
                "metric": metric,
                "ts_ms": int(ts.value // 1_000_000),
                "value": v,
            })
    store._upsert(records)
    return store


def bearish_series(n: int = N_DAYS) -> dict[str, list[float]]:
    """Price ramps up while usage stays flat -> NVT rich -> bearish."""
    flat_days = n - 60
    price = [100.0] * flat_days + [100.0 * (1.01 ** i) for i in range(1, 61)]
    volume = [1e9] * n
    nvt = [p / v * 1e7 for p, v in zip(price, volume)]  # tracks price
    return {"price": price, "volume": volume, "nvt_approx": nvt}


def bullish_series(n: int = N_DAYS) -> dict[str, list[float]]:
    """Price ramps down while usage ramps up -> NVT cheap -> bullish."""
    flat_days = n - 60
    price = [100.0] * flat_days + [100.0 * (0.99 ** i) for i in range(1, 61)]
    volume = [1e9] * flat_days + [1e9 * (1.01 ** i) for i in range(1, 61)]
    nvt = [p / v * 1e7 for p, v in zip(price, volume)]
    return {"price": price, "volume": volume, "nvt_approx": nvt}


def full_metrics_bearish(n: int = N_DAYS) -> dict[str, list[float]]:
    """Bearish setup with every v2 metric present (mvrv, addresses, flows)."""
    s = bearish_series(n)
    flat_days = n - 60
    s["mvrv"] = [1.0] * flat_days + [1.0 + 0.02 * i for i in range(1, 61)]  # rich valuation
    s["active_addresses"] = [1e6] * n                                      # usage flat
    s["exchange_inflow_native"] = [5_000.0] * n
    s["exchange_outflow_native"] = [5_000.0] * n                           # netflow ~ 0
    return s


@pytest.fixture
def bearish_store(tmp_path):
    return build_store(tmp_path, bearish_series())


@pytest.fixture
def bullish_store(tmp_path):
    return build_store(tmp_path, bullish_series())


@pytest.fixture
def full_store(tmp_path):
    return build_store(tmp_path, full_metrics_bearish())


# ---------------------------------------------------------------------------
# generate()
# ---------------------------------------------------------------------------


class TestGenerate:
    def test_bearish_divergence(self, bearish_store):
        sig = OnchainFundamentalSignal(store=bearish_store)
        out = sig.generate(AS_OF)
        assert out is not None
        assert out.score < 0.0
        assert out.direction == "short"
        assert out.metadata["divergence"] > 0.0  # price outrunning usage
        assert out.metadata["valuation_z"] > 0.0

    def test_bullish_divergence(self, bullish_store):
        sig = OnchainFundamentalSignal(store=bullish_store)
        out = sig.generate(AS_OF)
        assert out is not None
        assert out.score > 0.0
        assert out.direction == "long"
        assert out.metadata["divergence"] < 0.0
        assert out.metadata["valuation_z"] < 0.0

    def test_output_contract(self, bearish_store):
        sig = OnchainFundamentalSignal(store=bearish_store)
        out = sig.generate(AS_OF)
        assert isinstance(out.score, float)
        assert -1.0 <= out.score <= 1.0
        assert 0.0 <= out.confidence <= 1.0
        assert out.direction in ("long", "short", "flat")
        assert out.metadata["days_used"] >= 120

    def test_insufficient_history_returns_none(self, tmp_path):
        series = bearish_series(n=50)
        store = build_store(tmp_path, series)
        as_of = BASE_TS + pd.Timedelta(days=50)
        sig = OnchainFundamentalSignal(store=store)
        assert sig.generate(as_of) is None

    def test_missing_valuation_metric_returns_none(self, tmp_path):
        series = bearish_series()
        del series["nvt_approx"]
        store = build_store(tmp_path, series)
        sig = OnchainFundamentalSignal(store=store)
        assert sig.generate(AS_OF) is None

    def test_missing_price_returns_none(self, tmp_path):
        series = {"mvrv": [1.0] * N_DAYS, "volume": [1e9] * N_DAYS}
        store = build_store(tmp_path, series)
        sig = OnchainFundamentalSignal(store=store)
        assert sig.generate(AS_OF) is None

    def test_empty_store_returns_none(self, tmp_path):
        store = OnchainMetricStore(db_path=str(tmp_path / "empty.db"))
        sig = OnchainFundamentalSignal(store=store)
        assert sig.generate(AS_OF) is None

    def test_as_of_before_data_returns_none(self, bearish_store):
        sig = OnchainFundamentalSignal(store=bearish_store)
        assert sig.generate(pd.Timestamp("2020-01-01", tz="UTC")) is None

    def test_no_look_ahead(self, tmp_path):
        """Data at/after as_of must not influence the output."""
        full = bearish_series()
        # Append a dramatic post-as_of crash that would flip every z-score.
        for i in range(1, 61):
            full["price"].append(100.0 * (1.01 ** 60) * (0.90 ** i))
            full["volume"].append(1e9 * (1.05 ** i))
            full["nvt_approx"].append(full["price"][-1] / full["volume"][-1] * 1e7)

        store_full = build_store(tmp_path / "full", full)
        store_trunc = build_store(tmp_path / "trunc", bearish_series())

        out_full = OnchainFundamentalSignal(store=store_full).generate(AS_OF)
        out_trunc = OnchainFundamentalSignal(store=store_trunc).generate(AS_OF)
        assert out_full is not None and out_trunc is not None
        assert out_full.score == pytest.approx(out_trunc.score)
        assert out_full.confidence == pytest.approx(out_trunc.confidence)
        assert out_full.metadata["days_used"] == out_trunc.metadata["days_used"]

    def test_weak_signal_typical_magnitude(self, tmp_path):
        """Most days the signal should be weak — random-ish walk stays small."""
        random.seed(42)
        price, volume, nvt = [], [], []
        p, v = 100.0, 1e9
        for _ in range(N_DAYS):
            p *= 1 + random.gauss(0, 0.01)
            v *= 1 + random.gauss(0, 0.02)
            price.append(p)
            volume.append(v)
            nvt.append(p / v * 1e7)
        store = build_store(tmp_path, {"price": price, "volume": volume, "nvt_approx": nvt})
        out = OnchainFundamentalSignal(store=store).generate(AS_OF)
        assert out is not None
        assert abs(out.score) < 0.8  # not pinned at the +/-1 rail

    def test_hourly_data_collapses_to_daily(self, tmp_path):
        """Real store data is hourly — extra intraday rows must not break it."""
        series = bearish_series()
        store = OnchainMetricStore(db_path=str(tmp_path / "hourly.db"))
        records = []
        for metric, values in series.items():
            for i, v in enumerate(values):
                for hour in (0, 6, 12, 18):
                    ts = BASE_TS + pd.Timedelta(days=i, hours=hour)
                    records.append({
                        "asset": ASSET,
                        "metric": metric,
                        "ts_ms": int(ts.value // 1_000_000),
                        "value": v,
                    })
        store._upsert(records)
        sig = OnchainFundamentalSignal(store=store)
        out = sig.generate(AS_OF)
        assert out is not None
        assert out.direction == "short"
        # hourly rows collapsed to daily, capped at lookback_days
        assert out.metadata["days_used"] == 180

    def test_is_a_signal(self, bearish_store):
        sig = OnchainFundamentalSignal(store=bearish_store)
        assert isinstance(sig, Signal)


# ---------------------------------------------------------------------------
# Metric selection (v2 metrics with fallback)
# ---------------------------------------------------------------------------


class TestMetricSelection:
    def test_prefers_true_mvrv_over_nvt_proxy(self, full_store):
        out = OnchainFundamentalSignal(store=full_store).generate(AS_OF)
        assert out is not None
        assert out.metadata["valuation_metric"] == "mvrv"
        assert out.metadata["usage_metric"] == "active_addresses"
        assert out.metadata["exchange_flows_used"] is True

    def test_falls_back_to_nvt_proxy(self, bearish_store):
        out = OnchainFundamentalSignal(store=bearish_store).generate(AS_OF)
        assert out is not None
        assert out.metadata["valuation_metric"] == "nvt_approx"
        assert out.metadata["usage_metric"] == "volume"  # no active_addresses
        assert out.metadata["exchange_flows_used"] is False

    def test_mvrv_without_enough_history_falls_back(self, tmp_path):
        """A mvrv series shorter than min_history must not be chosen."""
        store = OnchainMetricStore(db_path=str(tmp_path / "sparse.db"))
        records = []
        for metric, values in bearish_series().items():
            for i, v in enumerate(values):
                ts = BASE_TS + pd.Timedelta(days=i, hours=12)
                records.append({"asset": ASSET, "metric": metric, "ts_ms": int(ts.value // 1_000_000), "value": v})
        for i in range(30):
            ts = BASE_TS + pd.Timedelta(days=N_DAYS - 30 + i, hours=12)
            records.append({"asset": ASSET, "metric": "mvrv", "ts_ms": int(ts.value // 1_000_000), "value": 2.0})
        store._upsert(records)
        out = OnchainFundamentalSignal(store=store).generate(AS_OF)
        assert out is not None
        assert out.metadata["valuation_metric"] == "nvt_approx"

    def test_net_inflow_is_bearish(self, tmp_path):
        """Rising net exchange inflow should push the score more negative."""
        base = full_metrics_bearish()
        inflow_ramp = dict(base)
        inflow_ramp["exchange_inflow_native"] = (
            [5_000.0] * (N_DAYS - 60) + [5_000.0 * (1.02 ** i) for i in range(1, 61)]
        )
        store_flat = build_store(tmp_path / "flat", base)
        store_ramp = build_store(tmp_path / "ramp", inflow_ramp)
        out_flat = OnchainFundamentalSignal(store=store_flat).generate(AS_OF)
        out_ramp = OnchainFundamentalSignal(store=store_ramp).generate(AS_OF)
        assert out_flat is not None and out_ramp is not None
        assert out_ramp.metadata["netflow_z"] > 0.0
        assert out_ramp.score < out_flat.score

    def test_full_metrics_bearish(self, full_store):
        out = OnchainFundamentalSignal(store=full_store).generate(AS_OF)
        assert out is not None
        assert out.score < 0.0
        assert out.direction == "short"
        assert out.metadata["valuation_z"] > 0.0   # mvrv rich
        assert out.metadata["divergence"] > 0.0    # price outrunning addresses


# ---------------------------------------------------------------------------
# data_health()
# ---------------------------------------------------------------------------


class TestDataHealth:
    def test_no_data(self, tmp_path):
        store = OnchainMetricStore(db_path=str(tmp_path / "empty.db"))
        health = OnchainFundamentalSignal(store=store).data_health()
        assert health["status"] == "no_data"
        assert health["metrics"]["mvrv"]["latest_ts"] is None

    def test_no_valuation_metric_is_no_data(self, tmp_path):
        series = {"price": [1.0] * N_DAYS, "volume": [1.0] * N_DAYS}
        store = build_store(tmp_path, series)
        health = OnchainFundamentalSignal(store=store).data_health()
        assert health["status"] == "no_data"

    def test_degraded_when_only_proxies(self, tmp_path):
        """Fresh price/volume/nvt but no v2 metrics -> degraded, not ok."""
        now = pd.Timestamp.now(tz="UTC")
        store = OnchainMetricStore(db_path=str(tmp_path / "proxy.db"))
        records = []
        for metric in ("price", "volume", "nvt_approx"):
            for i in range(150):
                ts = now - pd.Timedelta(days=150 - i)
                records.append({
                    "asset": ASSET, "metric": metric,
                    "ts_ms": int(ts.value // 1_000_000), "value": 1.0 + i,
                })
        store._upsert(records)
        health = OnchainFundamentalSignal(store=store).data_health()
        assert health["status"] == "degraded"

    def test_ok_when_all_metrics_fresh(self, tmp_path):
        now = pd.Timestamp.now(tz="UTC")
        store = OnchainMetricStore(db_path=str(tmp_path / "fresh.db"))
        metrics = (
            "price", "volume", "nvt_approx", "mvrv",
            "active_addresses", "exchange_inflow_native", "exchange_outflow_native",
        )
        records = []
        for metric in metrics:
            for i in range(150):
                ts = now - pd.Timedelta(days=150 - i)
                records.append({
                    "asset": ASSET, "metric": metric,
                    "ts_ms": int(ts.value // 1_000_000), "value": 1.0 + i,
                })
        store._upsert(records)
        health = OnchainFundamentalSignal(store=store).data_health()
        assert health["status"] == "ok"
        assert all(m["days_stale"] < 7 for m in health["metrics"].values())

    def test_stale_when_old(self, tmp_path):
        """data_health accepts now_ms for deterministic testing."""
        now_ms = int(pd.Timestamp("2026-09-14", tz="UTC").value // 1_000_000)
        series = bearish_series()  # ends 2025-07-19 -> months stale
        store = build_store(tmp_path, series)
        health = OnchainFundamentalSignal(store=store).data_health(now_ms=now_ms)
        assert health["status"] == "stale"

    def test_reports_asset_and_all_metrics(self, tmp_path):
        store = OnchainMetricStore(db_path=str(tmp_path / "empty.db"))
        health = OnchainFundamentalSignal(store=store, asset="ETH").data_health()
        assert health["asset"] == "ETH"
        assert "mvrv" in health["metrics"]
        assert "exchange_inflow_native" in health["metrics"]


# ---------------------------------------------------------------------------
# Execution targets (BTC direct, not crypto stocks)
# ---------------------------------------------------------------------------


class TestTargets:
    def test_default_targets_are_btc_perp(self, bearish_store):
        out = OnchainFundamentalSignal(store=bearish_store).generate(AS_OF)
        assert out is not None
        assert out.metadata["targets"] == ["BTC/USDT:USDT"]
        assert out.metadata["instrument"] == "perp"

    def test_custom_targets_respected(self, bearish_store):
        sig = OnchainFundamentalSignal(store=bearish_store, targets=["BTC/USDT"])
        out = sig.generate(AS_OF)
        assert out is not None
        assert out.metadata["targets"] == ["BTC/USDT"]
        assert out.metadata["instrument"] == "perp"

    def test_no_stock_symbols_in_default_targets(self, bearish_store):
        """The signal must NOT point at the crypto-proxy stock basket."""
        out = OnchainFundamentalSignal(store=bearish_store).generate(AS_OF)
        assert out is not None
        for sym in ("COIN", "MSTR", "MARA", "RIOT"):
            assert sym not in out.metadata["targets"]


# ---------------------------------------------------------------------------
# data_health(): market-data freshness (execution instrument bars)
# ---------------------------------------------------------------------------


def build_price_store(tmp_path, last_day: str, n_days: int = 10) -> HistoricalOHLCVStore:
    """BTC/USDT spot 1d store with `n_days` bars ending on `last_day`."""
    store = HistoricalOHLCVStore(
        db_path=str(tmp_path / "btc_prices.db"), allow_fetch=False
    )
    rows = []
    base = pd.Timestamp(last_day, tz="UTC") - pd.Timedelta(days=n_days - 1)
    for i in range(n_days):
        ts = base + pd.Timedelta(days=i)
        rows.append([int(ts.value // 1_000_000), 100.0, 101.0, 99.0, 100.5, 10.0])
    store.upsert_ohlcv("BTC/USDT", "spot", "1d", rows)
    return store


class TestMarketDataHealth:
    NOW_MS = int(pd.Timestamp("2026-09-14", tz="UTC").value // 1_000_000)

    def test_ok_when_bars_fresh(self, tmp_path):
        store = build_store(tmp_path, bearish_series())
        prices = build_price_store(tmp_path / "px", last_day="2026-09-13")
        health = OnchainFundamentalSignal(
            store=store, price_store=prices
        ).data_health(now_ms=self.NOW_MS)
        md = health["market_data"]
        assert md["symbol"] == "BTC/USDT"
        assert md["status"] == "ok"
        assert md["days_stale"] == pytest.approx(1.0)

    def test_stale_when_bars_old(self, tmp_path):
        store = build_store(tmp_path, bearish_series())
        prices = build_price_store(tmp_path / "px", last_day="2026-09-01")
        health = OnchainFundamentalSignal(
            store=store, price_store=prices
        ).data_health(now_ms=self.NOW_MS)
        assert health["market_data"]["status"] == "stale"

    def test_no_data_when_price_store_empty(self, tmp_path):
        store = build_store(tmp_path, bearish_series())
        prices = HistoricalOHLCVStore(
            db_path=str(tmp_path / "px_empty.db"), allow_fetch=False
        )
        health = OnchainFundamentalSignal(
            store=store, price_store=prices
        ).data_health(now_ms=self.NOW_MS)
        assert health["market_data"]["status"] == "no_data"
        assert health["market_data"]["latest_ts"] is None

    def test_market_data_does_not_override_onchain_status(self, tmp_path):
        """On-chain metrics are stale but bars fresh -> overall status still 'stale'."""
        store = build_store(tmp_path, bearish_series())  # ends 2025-07-19
        prices = build_price_store(tmp_path / "px", last_day="2026-09-13")
        health = OnchainFundamentalSignal(
            store=store, price_store=prices
        ).data_health(now_ms=self.NOW_MS)
        assert health["status"] == "stale"
        assert health["market_data"]["status"] == "ok"


# ---------------------------------------------------------------------------
# Integration: real onchain_metrics.db (skipped if absent)
# ---------------------------------------------------------------------------

_REAL_DB = Path(__file__).resolve().parents[2] / "data" / "onchain_metrics.db"


@pytest.mark.skipif(not _REAL_DB.exists(), reason="real onchain db not present")
class TestRealDataSmoke:
    def test_generate_over_recent_dates(self):
        sig = OnchainFundamentalSignal()  # default shared store
        as_of = pd.Timestamp("2026-09-01", tz="UTC")
        out = sig.generate(as_of)
        # Real data covers 2019..2026-09, so this should produce a valid
        # output (or None only if history is somehow insufficient).
        if out is not None:
            assert -1.0 <= out.score <= 1.0
            assert 0.0 <= out.confidence <= 1.0
            assert out.direction in ("long", "short", "flat")
            assert out.metadata["valuation_metric"] == "mvrv"
            assert out.metadata["exchange_flows_used"] is True
            assert out.metadata["targets"] == ["BTC/USDT:USDT"]
            assert out.metadata["instrument"] == "perp"

    def test_data_health_on_real_store(self):
        health = OnchainFundamentalSignal().data_health()
        assert health["status"] in ("ok", "stale", "no_data", "degraded")
        assert health["metrics"]["price"]["latest_ts"] is not None
