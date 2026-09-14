"""Signal 2: BTC perp funding rate extreme → crypto-stock LONG signal.

Validated in scripts/backtest_funding_signal.py. The original hypothesis
("high funding → de-leverage → short risk assets") was INVERTED by the
data — user directive: use the sign the data shows.

What the data actually says (3-day MA of daily-average funding,
annualized %, BTC/USDT:USDT perp since 2023-03):

- HIGH funding (> +50% ann., ~99th percentile): overheated leverage is
  momentum CONFIRMATION, not de-leverage. MSTR +8.2% next day (100% win),
  +40.9% at 10 days (100% win); COIN +9.5% at 10 days (100% win).
  Miners do NOT follow (MARA/RIOT -9% to -28% at 10d) → momentum targets
  are MSTR/COIN only.
- LOW funding (< -5% ann., ~1st percentile; the backtest's original -20%
  threshold never triggers): cooling/capitulation → broad rebound repair.
  MSTR +7.0%/10d (83% win), RIOT +11.6%/10d (83% win), MARA +5.5%/10d
  (83% win) over n=6 events → rebound targets are the broad crypto-stock
  basket.
- QQQ correlation -0.04 — independent of the broad market.

Both regimes are LONG signals. Signal semantics: `generate(as_of)` uses
only funding data with ts strictly BEFORE as_of (the 3-day window is
[as_of-3, as_of-1]), which corresponds to the backtest's trigger date
D = as_of-1 with entry at D's close.
"""

from __future__ import annotations

import pandas as pd
import sqlalchemy as sa

from src.data.historical_store import HistoricalOHLCVStore
from src.data.nasdaq_store import NasdaqDailyStore
from src.signals.base import Signal, SignalOutput

FUNDING_SYMBOL = "BTC/USDT:USDT"

DEFAULT_MOMENTUM_TARGETS = ("MSTR", "COIN")
DEFAULT_REBOUND_TARGETS = ("MSTR", "COIN", "MARA", "RIOT")

# Funding accrues every 8h; last record older than this = stale feed.
_FUNDING_MAX_STALE_HOURS = 24
# US equities: Friday data checked on the following Monday is 3 days old.
_STOCK_MAX_STALE_DAYS = 4

# 8h funding rate → annualized percentage (as in the backtest).
_ANN_FACTOR = 3 * 365 * 100


def _to_naive_utc(ts: pd.Timestamp) -> pd.Timestamp:
    """Normalize any Timestamp to tz-naive UTC midnight."""
    ts = pd.Timestamp(ts)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts.normalize()


def _ms(ts: pd.Timestamp) -> int:
    """Epoch milliseconds for a tz-naive UTC Timestamp."""
    return int(ts.value // 1_000_000)


class FundingRateSignal(Signal):
    """Signal: BTC perp funding extreme → long crypto-proxy stocks.

    Direction is LONG in both regimes (validated; see module docstring):
    - "high" (rate_ma > high_threshold): momentum confirmation → MSTR/COIN.
    - "low" (rate_ma < low_threshold): capitulation rebound → broad basket.

    Score is continuous: 0.5 at the threshold, 1.0 at twice the threshold
    distance (e.g. high: 50%→0.5, 100%→1.0; low: -5%→0.5, -10%→1.0).
    """

    name = "funding_rate"
    description = (
        "BTC perp funding extremes signal regime shifts in crypto stocks: "
        "overheated funding confirms momentum (MSTR/COIN), deeply cooled "
        "funding marks capitulation rebounds (broad basket)"
    )

    def __init__(
        self,
        high_threshold: float = 50.0,
        low_threshold: float = -5.0,
        window: int = 3,
        momentum_targets: tuple[str, ...] | list[str] = DEFAULT_MOMENTUM_TARGETS,
        rebound_targets: tuple[str, ...] | list[str] = DEFAULT_REBOUND_TARGETS,
        db_path: str | None = None,
    ) -> None:
        """
        Args:
            high_threshold: 3-day funding MA (annualized %) above which the
                momentum regime fires. Validated at 50 (~99th percentile).
            low_threshold: MA below which the rebound regime fires.
                Validated at -5 (~1st percentile; the original backtest
                default of -20 never triggers in the data).
            window: rolling window (days) for the funding MA.
            momentum_targets: stocks to buy in the high regime (miners are
                excluded — they fall after high funding).
            rebound_targets: stocks to buy in the low regime.
            db_path: SQLite path (defaults to the shared data/btc_history.db).
        """
        self.high_threshold = float(high_threshold)
        self.low_threshold = float(low_threshold)
        self.window = int(window)
        self.momentum_targets = tuple(momentum_targets)
        self.rebound_targets = tuple(rebound_targets)
        store_kwargs = {"db_path": db_path} if db_path else {}
        self._store = HistoricalOHLCVStore(allow_fetch=False, **store_kwargs)
        self._stock_store = NasdaqDailyStore(assetclass="stocks", **store_kwargs)

    # ------------------------------------------------------------------
    # Signal
    # ------------------------------------------------------------------

    def generate(self, as_of: pd.Timestamp) -> SignalOutput | None:
        """Fire long when the funding MA (strictly before as_of) is extreme.

        Returns None when the window is incomplete or funding is in the
        neutral band between the two thresholds.
        """
        as_of = _to_naive_utc(as_of)

        # Window days: the `window` calendar days ending at as_of-1.
        window_days = pd.date_range(
            as_of - pd.Timedelta(days=self.window),
            as_of - pd.Timedelta(days=1),
            freq="D",
        )
        # One extra day back so we can tell whether this is a NEW extreme.
        prev_days = window_days - pd.Timedelta(days=1)

        funding = self._load_funding(
            _ms(window_days[0] - pd.Timedelta(days=1)), _ms(as_of)
        )
        if funding.empty:
            return None

        daily = self._daily_avg_annualized(funding)
        if not all(d in daily.index for d in window_days):
            return None  # incomplete window (data gap or coverage start)

        rate_ma = float(daily.reindex(window_days).mean())

        if rate_ma > self.high_threshold:
            regime = "high"
            targets = self.momentum_targets
            excess = (rate_ma - self.high_threshold) / abs(self.high_threshold)
        elif rate_ma < self.low_threshold:
            regime = "low"
            targets = self.rebound_targets
            excess = (self.low_threshold - rate_ma) / abs(self.low_threshold)
        else:
            return None  # neutral band

        # Was the previous day's window already extreme in this regime?
        is_new = True
        if all(d in daily.index for d in prev_days):
            prev_ma = float(daily.reindex(prev_days).mean())
            prev_extreme = (
                prev_ma > self.high_threshold
                if regime == "high"
                else prev_ma < self.low_threshold
            )
            is_new = not prev_extreme

        score = min(0.5 + 0.5 * excess, 1.0)

        return SignalOutput(
            score=score,
            direction="long",
            confidence=score,
            metadata={
                "regime": regime,
                "targets": list(targets),
                "funding_ma_annualized_pct": round(rate_ma, 2),
                "funding_daily_avg_pct": {
                    d.date().isoformat(): round(float(v), 2)
                    for d, v in daily.reindex(window_days).items()
                },
                "window_days": self.window,
                "high_threshold_pct": self.high_threshold,
                "low_threshold_pct": self.low_threshold,
                "is_new_extreme": is_new,
            },
        )

    # ------------------------------------------------------------------
    # Data health
    # ------------------------------------------------------------------

    def data_health(self) -> dict:
        """Check funding-rate feed freshness and target-stock coverage."""
        now = pd.Timestamp.now(tz="UTC").tz_localize(None)
        health: dict = {"signal": self.name, "ok": False, "funding": {}, "stocks": {}}

        try:
            first, last, rows = self._funding_coverage()
            if rows == 0:
                health["funding"] = {
                    "ok": False,
                    "reason": f"no {FUNDING_SYMBOL} funding records",
                }
            else:
                stale_hours = (now - last).total_seconds() / 3600.0
                health["funding"] = {
                    "ok": stale_hours <= _FUNDING_MAX_STALE_HOURS,
                    "first_date": first.date().isoformat(),
                    "last_date": last.date().isoformat(),
                    "last_event": last.isoformat(),
                    "stale_hours": round(stale_hours, 1),
                    "max_stale_hours": _FUNDING_MAX_STALE_HOURS,
                    "rows": rows,
                }
        except Exception as exc:  # noqa: BLE001 — report, don't crash
            health["funding"] = {"ok": False, "error": str(exc)}

        all_targets = sorted(set(self.momentum_targets) | set(self.rebound_targets))
        for sym in all_targets:
            try:
                first_s, last_s, rows_s = self._stock_store.get_coverage(sym)
                if rows_s == 0:
                    health["stocks"][sym] = {"ok": False, "reason": "no data"}
                else:
                    stale = (now.normalize() - pd.Timestamp(last_s)).days
                    health["stocks"][sym] = {
                        "ok": stale <= _STOCK_MAX_STALE_DAYS,
                        "first_date": first_s,
                        "last_date": last_s,
                        "rows": rows_s,
                        "stale_days": stale,
                    }
            except Exception as exc:  # noqa: BLE001 — report, don't crash
                health["stocks"][sym] = {"ok": False, "error": str(exc)}

        funding_ok = bool(health["funding"].get("ok"))
        any_stock_ok = any(v.get("ok") for v in health["stocks"].values())
        health["ok"] = funding_ok and any_stock_ok
        return health

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _load_funding(self, start_ms: int, end_ms: int) -> pd.DataFrame:
        """Raw funding rows for FUNDING_SYMBOL in [start_ms, end_ms)."""
        sql = sa.text(
            "SELECT ts, rate FROM funding_rates "
            "WHERE symbol = :symbol AND ts >= :start AND ts < :end "
            "ORDER BY ts ASC"
        )
        with self._store._engine.connect() as conn:
            rows = conn.execute(
                sql,
                {"symbol": FUNDING_SYMBOL, "start": start_ms, "end": end_ms},
            ).fetchall()
        if not rows:
            return pd.DataFrame(columns=["ts", "rate"])
        return pd.DataFrame(rows, columns=["ts", "rate"])

    @staticmethod
    def _daily_avg_annualized(funding: pd.DataFrame) -> pd.Series:
        """Mean annualized funding (%) per UTC calendar day."""
        dates = pd.to_datetime(funding["ts"], unit="ms").dt.tz_localize(None).dt.normalize()
        ann = funding["rate"].astype(float) * _ANN_FACTOR
        return ann.groupby(dates).mean()

    def _funding_coverage(self) -> tuple[pd.Timestamp, pd.Timestamp, int]:
        """(first_ts, last_ts, row_count) for the funding feed."""
        sql = sa.text(
            "SELECT MIN(ts), MAX(ts), COUNT(*) FROM funding_rates "
            "WHERE symbol = :symbol"
        )
        with self._store._engine.connect() as conn:
            row = conn.execute(sql, {"symbol": FUNDING_SYMBOL}).fetchone()
        if not row or row[0] is None:
            return pd.Timestamp(0), pd.Timestamp(0), 0
        first = pd.Timestamp(row[0], unit="ms", tz="UTC").tz_localize(None)
        last = pd.Timestamp(row[1], unit="ms", tz="UTC").tz_localize(None)
        return first, last, int(row[2])
