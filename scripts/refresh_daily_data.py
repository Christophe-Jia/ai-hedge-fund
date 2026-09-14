"""
Unified daily data refresh + health check for the cross-market platform.

One command to refresh every data source and confirm signal health before
the trading day. Safe for cron/launchd: exit code 0 = all healthy,
1 = at least one source/signal failed.

Data sources refreshed (each independently — one failure never blocks others):
  1. BTC/USDT spot 1d OHLCV   — Binance public mirror data-api.binance.vision
                               (api.binance.com is blocked on this network)
  2. BTC/USDT:USDT perp funding — Gate.io (via CCXT)
  3. Crypto-proxy stocks/ETFs  — Nasdaq public API (COIN/MSTR/MARA/RIOT/...)
  4. On-chain metrics          — CoinMetrics community API (incremental)
     + CoinGecko price/market_cap refresh (rate-limit tolerant)

After refreshing, runs data_health() on every registered signal
(try-imported; signals still in development are skipped with a note).

Usage:
    poetry run python scripts/refresh_daily_data.py

Exit codes:
    0 — every data source and signal healthy (degraded passes with a warning)
    1 — at least one source/signal failed
"""

from __future__ import annotations

import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

# Ensure project root is on the path when run directly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd

from scripts.backfill_onchain_v2 import fetch_coinmetrics
from scripts.backfill_perp_ohlcv import make_exchange

from src.data.funding_rates import FundingRateStore
from src.data.historical_store import HistoricalOHLCVStore
from src.data.nasdaq_store import NasdaqDailyStore
from src.data.onchain_store import OnchainMetricStore

try:
    import ccxt
except ImportError as exc:  # pragma: no cover
    raise SystemExit("ccxt is required. Run: poetry install") from exc

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BTC_SPOT_SYMBOL = "BTC/USDT"
BTC_PERP_SYMBOL = "BTC/USDT:USDT"
BINANCE_MIRROR_HOST = "data-api.binance.vision"
ONCHAIN_ASSET = "BTC"

# Crypto-proxy stocks/ETFs (same lists as scripts/backfill_crypto_stocks.py)
from scripts.backfill_crypto_stocks import CRYPTO_ETFS, CRYPTO_STOCKS

# Staleness thresholds (days) per source — beyond this the source is failed
_BTC_SPOT_MAX_STALE = 3       # trades 7d/week; weekend signal allows 3
_FUNDING_MAX_STALE = 2        # settles every 8h
_STOCK_MAX_STALE = 4          # weekday-only + long weekends
_ONCHAIN_MAX_STALE = 4        # daily metric, ~1d publication lag
_COINGECKO_PRICE_MAX_STALE = 3

# Nasdaq refresh window (incremental — no need to re-pull full history)
_NASDAQ_LOOKBACK_DAYS = 21

_DAY_MS = 86_400_000


# ---------------------------------------------------------------------------
# Result bookkeeping
# ---------------------------------------------------------------------------


@dataclass
class SourceResult:
    name: str
    status: str = "ok"                 # ok | degraded | fail
    rows: int = 0
    last_date: str | None = None
    stale_days: int | None = None
    detail: str = ""
    extra_rows: list = field(default_factory=list)  # sub-rows (per symbol)

    @property
    def failed(self) -> bool:
        return self.status == "fail"


def _today() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC").tz_localize(None).normalize()


def _stale_days(date_str: str | None) -> int | None:
    if not date_str:
        return None
    return int((_today() - pd.Timestamp(date_str).normalize()).days)


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _fetch_retry(fn, *, retries: int = 3, wait_s: float = 5.0, label: str = ""):
    """Call fn() with bounded retries on network errors (never hangs)."""
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except ccxt.NetworkError as exc:
            last_exc = exc
            if attempt < retries:
                print(f"    [WARN] network error ({exc}) — retry {attempt}/{retries - 1} in {wait_s:.0f}s")
                time.sleep(wait_s)
    raise RuntimeError(f"{label}: network error after {retries} attempts: {last_exc}")


# ---------------------------------------------------------------------------
# Refresh steps — each returns a SourceResult, exceptions handled by caller
# ---------------------------------------------------------------------------


def refresh_btc_spot_1d() -> SourceResult:
    """BTC/USDT spot 1d OHLCV via the Binance public market-data mirror."""
    res = SourceResult(name="btc_spot_1d (binance-mirror)")
    ex = make_exchange("binance", hostname=BINANCE_MIRROR_HOST)
    store = HistoricalOHLCVStore(exchange_id="binance", allow_fetch=False)

    last_ts = store.get_latest_ts(BTC_SPOT_SYMBOL, "spot", "1d")
    now_ms = _now_ms()
    since = (last_ts + 1) if last_ts else now_ms - 2 * 365 * _DAY_MS

    last_newest = None
    while since < now_ms:
        candles = _fetch_retry(
            lambda: ex.fetch_ohlcv(BTC_SPOT_SYMBOL, timeframe="1d", since=since, limit=1000),
            label="binance mirror ohlcv",
        )
        candles = [c for c in candles if c[0] < now_ms]  # closed candles only
        if not candles:
            break
        res.rows += store.upsert_ohlcv(BTC_SPOT_SYMBOL, "spot", "1d", candles)
        newest = candles[-1][0]
        if newest == last_newest or newest < since:
            break  # no progress guard
        last_newest = newest
        since = newest + 1
        time.sleep(ex.rateLimit / 1000)

    first, last, n = store.get_coverage(BTC_SPOT_SYMBOL, "spot", "1d")
    res.last_date, res.stale_days = last, _stale_days(last)
    res.detail = f"{n} bars total"
    res.status = "ok" if (res.stale_days is not None and res.stale_days <= _BTC_SPOT_MAX_STALE) else "fail"
    return res


def refresh_perp_funding() -> SourceResult:
    """BTC/USDT:USDT perpetual funding rates via Gate.io."""
    res = SourceResult(name="perp_funding (gate)")
    ex = make_exchange("gate")
    store = FundingRateStore(exchange_id="gate")

    last_ts = store.get_latest_ts(BTC_PERP_SYMBOL)
    now_ms = _now_ms()
    since = (last_ts + 1) if last_ts else now_ms - 180 * _DAY_MS  # gate: 180d lookback

    last_newest = None
    while since < now_ms:
        rates = _fetch_retry(
            lambda: ex.fetch_funding_rate_history(BTC_PERP_SYMBOL, since=since, limit=500),
            label="gate funding",
        )
        rates = [r for r in rates if r.get("timestamp") and r["timestamp"] < now_ms]
        if not rates:
            break
        rows = [(int(r["timestamp"]), float(r["fundingRate"])) for r in rates
                if r.get("fundingRate") is not None]
        if rows:
            res.rows += store.upsert_rates(BTC_PERP_SYMBOL, rows)
        newest = rates[-1]["timestamp"]
        if newest == last_newest or newest < since:
            break
        last_newest = newest
        since = newest + 1
        time.sleep(ex.rateLimit / 1000)

    latest = store.get_latest_ts(BTC_PERP_SYMBOL)
    if latest:
        res.last_date = datetime.fromtimestamp(latest / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
        res.stale_days = max(0, int((_now_ms() - latest) / _DAY_MS))
    res.status = "ok" if (res.stale_days is not None and res.stale_days <= _FUNDING_MAX_STALE) else "fail"
    return res


def refresh_crypto_stocks() -> SourceResult:
    """Crypto-proxy stocks + ETFs via the Nasdaq public API."""
    res = SourceResult(name="crypto_stocks (nasdaq)")
    from_date = _iso_days_ago(_NASDAQ_LOOKBACK_DAYS)
    stock_store = NasdaqDailyStore(assetclass="stocks", politeness_s=1.5)
    etf_store = NasdaqDailyStore(assetclass="etf", politeness_s=1.5)

    failures: list[str] = []
    for sym, store in [(s, stock_store) for s in CRYPTO_STOCKS] + [
        (s, etf_store) for s in CRYPTO_ETFS
    ]:
        try:
            res.rows += store.fetch_and_store(sym, from_date=from_date)
        except Exception as exc:
            failures.append(sym)
            print(f"    [WARN] {sym}: {str(exc)[:100]}")
        time.sleep(1.5)

    # Per-symbol freshness sub-rows
    ok_syms, stale_syms = 0, []
    for sym, store in [(s, stock_store) for s in CRYPTO_STOCKS] + [
        (s, etf_store) for s in CRYPTO_ETFS
    ]:
        try:
            first, last, n = store.get_coverage(sym)
        except Exception:
            first, last, n = None, None, 0
        stale = _stale_days(last)
        sym_ok = n > 0 and stale is not None and stale <= _STOCK_MAX_STALE
        if sym_ok:
            ok_syms += 1
        else:
            stale_syms.append(sym)
        res.extra_rows.append((f"  {sym}", "ok" if sym_ok else "stale", last, stale, f"{n} rows"))

    total = len(CRYPTO_STOCKS) + len(CRYPTO_ETFS)
    if ok_syms == total:
        res.status = "ok"
    elif ok_syms > 0:
        res.status = "degraded"
        res.detail = f"{ok_syms}/{total} fresh; stale/failed: {', '.join(stale_syms)}"
    else:
        res.status = "fail"
        res.detail = "no fresh symbols"
    # Overall last_date = freshest symbol
    dates = [r[2] for r in res.extra_rows if r[2]]
    if dates:
        res.last_date = max(dates)
        res.stale_days = _stale_days(res.last_date)
    return res


def _iso_days_ago(days: int) -> str:
    """UTC date string (YYYY-MM-DD) `days` days before now."""
    dt = datetime.fromtimestamp(datetime.now(timezone.utc).timestamp() - days * 86400, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def refresh_onchain() -> SourceResult:
    """On-chain fundamentals via CoinMetrics community API (incremental),
    plus a CoinGecko price/market_cap refresh (rate-limit tolerant)."""
    res = SourceResult(name="onchain (coinmetrics + coingecko)")
    store = OnchainMetricStore()
    now_ms = _now_ms()
    degraded_notes: list[str] = []

    # --- CoinMetrics incremental (active_addresses as the resume cursor) ---
    try:
        latest = store.get_latest_ts(ONCHAIN_ASSET, "active_addresses")
        # Default start: 2019-01-01 (matches existing CoinMetrics coverage)
        default_start = int(datetime(2019, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
        start_ms = (latest - _DAY_MS) if latest else default_start
        if start_ms < now_ms:
            records = fetch_coinmetrics(ONCHAIN_ASSET, start_ms, now_ms)
            res.rows += store.upsert(records)
    except Exception as exc:
        degraded_notes.append(f"coinmetrics: {str(exc)[:80]}")

    # --- CoinGecko price/market_cap/volume/nvt refresh (429-tolerant) ---
    try:
        latest_px = store.get_latest_ts(ONCHAIN_ASSET, "price")
        start_px = (latest_px - _DAY_MS) if latest_px else now_ms - 80 * _DAY_MS
        if start_px < now_ms:
            n = store.backfill(ONCHAIN_ASSET, start_px, now_ms)
            res.rows += n
            if n == 0:
                degraded_notes.append("coingecko: 0 rows (possibly rate-limited)")
    except Exception as exc:
        degraded_notes.append(f"coingecko: {str(exc)[:80]}")

    # --- Freshness of the metrics that matter ---
    worst_stale = 0
    for metric in ("active_addresses", "mvrv", "exchange_outflow_native"):
        ts = store.get_latest_ts(ONCHAIN_ASSET, metric)
        if ts is None:
            res.status = "fail"
            res.detail = f"missing {metric}"
            return res
        stale = max(0, int((now_ms - ts) / _DAY_MS))
        worst_stale = max(worst_stale, stale)
        res.extra_rows.append((
            f"  {metric}",
            "ok" if stale <= _ONCHAIN_MAX_STALE else "stale",
            datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d"),
            stale,
            "",
        ))
    ts_px = store.get_latest_ts(ONCHAIN_ASSET, "price")
    px_stale = max(0, int((now_ms - ts_px) / _DAY_MS)) if ts_px else 999
    res.extra_rows.append((
        "  price (coingecko)",
        "ok" if px_stale <= _COINGECKO_PRICE_MAX_STALE else "stale",
        datetime.fromtimestamp(ts_px / 1000, tz=timezone.utc).strftime("%Y-%m-%d") if ts_px else None,
        px_stale if ts_px else None,
        "",
    ))

    res.last_date = res.extra_rows[0][2]
    res.stale_days = worst_stale
    if worst_stale > _ONCHAIN_MAX_STALE or px_stale > _COINGECKO_PRICE_MAX_STALE:
        res.status = "degraded" if degraded_notes else "fail"
        if worst_stale > _ONCHAIN_MAX_STALE:
            res.detail = f"coinmetrics data {worst_stale}d stale"
    else:
        res.status = "degraded" if degraded_notes else "ok"
    if degraded_notes:
        res.detail = (res.detail + "; " if res.detail else "") + "; ".join(degraded_notes)
    return res


# ---------------------------------------------------------------------------
# Signal health (try-import — signals in development are skipped, not failed)
# ---------------------------------------------------------------------------


def collect_signal_health() -> list[tuple[str, str, str]]:
    """(signal_name, status, detail) rows; status in ok|fail|skipped."""
    rows: list[tuple[str, str, str]] = []

    # --- weekend_gap ---------------------------------------------------
    try:
        from src.signals.weekend_gap import WeekendGapSignal

        h = WeekendGapSignal().data_health()
        ok = bool(h.get("ok"))
        btc = h.get("btc", {})
        stocks_ok = sum(1 for v in h.get("stocks", {}).values() if v.get("ok"))
        stocks_n = len(h.get("stocks", {}))
        detail = (
            f"btc last={btc.get('last_date', '?')} (stale {btc.get('stale_days', '?')}d); "
            f"stocks {stocks_ok}/{stocks_n} ok"
        )
        rows.append(("weekend_gap", "ok" if ok else "fail", detail))
    except ImportError as exc:
        rows.append(("weekend_gap", "skipped", f"not importable: {exc}"))
    except Exception as exc:
        rows.append(("weekend_gap", "fail", f"error: {str(exc)[:100]}"))

    # --- onchain_fundamental -------------------------------------------
    try:
        from src.signals.onchain_fundamental import OnchainFundamentalSignal

        h = OnchainFundamentalSignal().data_health()
        status = h.get("status", "unknown")
        metrics = h.get("metrics", {})
        latest_bits = "; ".join(
            f"{m}={v.get('latest_ts', '?')}" for m, v in list(metrics.items())[:3]
        )
        rows.append((
            "onchain_fundamental",
            "ok" if status == "ok" else "fail",
            f"status={status}; {latest_bits}",
        ))
    except ImportError as exc:
        rows.append(("onchain_fundamental", "skipped", f"not importable: {exc}"))
    except Exception as exc:
        rows.append(("onchain_fundamental", "fail", f"error: {str(exc)[:100]}"))

    # --- funding_rate (in development by gp-2 — optional) ----------------
    funding_cls = None
    for mod_name in ("src.signals.funding_rate", "src.signals.funding", "src.signals.funding_rates"):
        try:
            module = __import__(mod_name, fromlist=["*"])
            for attr in ("FundingRateSignal", "FundingSignal"):
                if hasattr(module, attr):
                    funding_cls = getattr(module, attr)
                    break
            if funding_cls is not None:
                break
        except ImportError:
            continue
    if funding_cls is None:
        rows.append(("funding_rate", "skipped", "not available yet (in development)"))
    else:
        try:
            h = funding_cls().data_health()
            ok = bool(h.get("ok", h.get("status") == "ok"))
            rows.append(("funding_rate", "ok" if ok else "fail", str(h)[:120]))
        except Exception as exc:
            rows.append(("funding_rate", "fail", f"error: {str(exc)[:100]}"))

    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _run_step(idx: int, total: int, fn, results: list[SourceResult]) -> None:
    name = fn.__doc__.strip().splitlines()[0] if fn.__doc__ else fn.__name__
    print(f"\n[{idx}/{total}] {name}")
    try:
        res = fn()
    except Exception as exc:
        print(f"    [ERROR] step failed: {exc}")
        res = SourceResult(name=name.split(" — ")[0].strip(), status="fail",
                           detail=f"exception: {str(exc)[:120]}")
    print(f"    → status={res.status}, rows=+{res.rows}, last={res.last_date}, stale={res.stale_days}d")
    results.append(res)


def main() -> int:
    started = datetime.now(timezone.utc)
    print(f"=== Daily Data Refresh — {started.isoformat(timespec='seconds')} ===")

    steps = [refresh_btc_spot_1d, refresh_perp_funding, refresh_crypto_stocks, refresh_onchain]
    results: list[SourceResult] = []
    for i, fn in enumerate(steps, 1):
        _run_step(i, len(steps), fn, results)

    # ------------------------------------------------------------------
    # Signal health
    # ------------------------------------------------------------------
    print("\n[5/5] Signal data_health()")
    signal_rows = collect_signal_health()

    # ------------------------------------------------------------------
    # Summary table
    # ------------------------------------------------------------------
    print("\n" + "=" * 100)
    print("HEALTH SUMMARY")
    print("=" * 100)
    header = f"{'Source / Signal':<32} {'Status':<10} {'Last Date':<18} {'Stale(d)':<9} Detail"
    print(header)
    print("-" * 100)
    for r in results:
        print(
            f"{r.name:<32} {r.status:<10} {str(r.last_date or '-'):<18} "
            f"{str(r.stale_days if r.stale_days is not None else '-'):<9} {r.detail[:44]}"
        )
        for sub_name, sub_status, sub_last, sub_stale, sub_detail in r.extra_rows:
            print(
                f"{sub_name:<32} {sub_status:<10} {str(sub_last or '-'):<18} "
                f"{str(sub_stale if sub_stale is not None else '-'):<9} {sub_detail[:44]}"
            )
    print("-" * 100)
    for name, status, detail in signal_rows:
        print(f"{name:<32} {status:<10} {'':<18} {'':<9} {detail[:44]}")

    failed_sources = [r.name for r in results if r.failed]
    degraded_sources = [r.name for r in results if r.status == "degraded"]
    failed_signals = [n for n, s, _ in signal_rows if s == "fail"]
    skipped_signals = [n for n, s, _ in signal_rows if s == "skipped"]

    print("-" * 100)
    overall_ok = not failed_sources and not failed_signals
    verdict = "OK" if overall_ok else "FAILED"
    print(f"Overall: {verdict}")
    if degraded_sources:
        print(f"  degraded (passing): {', '.join(degraded_sources)}")
    if skipped_signals:
        print(f"  signals skipped (in development): {', '.join(skipped_signals)}")
    if failed_sources:
        print(f"  FAILED sources: {', '.join(failed_sources)}")
    if failed_signals:
        print(f"  FAILED signals: {', '.join(failed_signals)}")

    return 0 if overall_ok else 1


if __name__ == "__main__":
    sys.exit(main())
