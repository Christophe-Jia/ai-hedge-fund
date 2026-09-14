"""Signal 2: BTC perp funding rate extreme → crypto-stock LONG signal.

Validated in scripts/backtest_funding_signal.py. The original hypothesis
("high funding → de-leverage → short risk assets") was INVERTED by the
data — user directive: use the sign the data shows.

What the data actually says (3-day MA of daily-average funding,
annualized %, BTC/USDT:USDT perp):

- HIGH funding (momentum regime): overheated leverage is momentum
  CONFIRMATION, not de-leverage. Miners do NOT follow (MARA/RIOT fall
  after high funding) → momentum targets are MSTR/COIN only.
- LOW funding (capitulation regime): cooling/capitulation → broad
  rebound repair → rebound targets are the broad crypto-stock basket
  (MSTR/COIN/MARA/RIOT).
- QQQ correlation -0.04 — independent of the broad market.

Both regimes are LONG signals. Signal semantics: `generate(as_of)` uses
only funding data with ts strictly BEFORE as_of (the 3-day window is
[as_of-3, as_of-1]), which corresponds to the backtest's trigger date
D = as_of-1 with entry at D's close.

THRESHOLD MODES (see scripts/backtest_funding_rolling.py for the
validation backtest of this change):

- rolling (default): the high/low triggers are the p99/p1 of the 3-day
  MA distribution over a trailing `window_days` window (default 730d),
  computed strictly point-in-time (reference MA days end at as_of-2).
  Rationale: the absolute thresholds (+50/-5 ann. %) were
  quantile-calibrated on 2023+ data and do NOT transfer across regimes —
  the out-of-sample backtest (reports/outofsample_backtest.json) showed
  +50 was a p99 tail event in 2023-2026 but exceeded 20% of days in the
  2021 bull (window p99 = 140%), and the -5 low trigger bled -19.4% in
  2022. Rolling percentiles adapt the trigger to each period's own
  funding distribution. Fewer than `min_window_days` (default 365d) of
  history → warmup: the signal returns None. Between min and full
  window the signal fires but metadata carries window_partial=True.
- absolute (absolute_mode=True): the original fixed thresholds
  (high_threshold=+50, low_threshold=-5 ann. %), kept for comparison
  backtests.
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

# Rolling-percentile defaults (days / percentiles).
DEFAULT_WINDOW_DAYS = 730
DEFAULT_MIN_WINDOW_DAYS = 365
DEFAULT_HIGH_PERCENTILE = 99.0
DEFAULT_LOW_PERCENTILE = 1.0


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
    - "high" (rate_ma > high threshold): momentum confirmation → MSTR/COIN.
    - "low" (rate_ma < low threshold): capitulation rebound → broad basket.

    Thresholds:
    - rolling mode (default): high = p{high_percentile} and low =
      p{low_percentile} of the 3-day funding-MA distribution over the
      trailing `window_days` days (point-in-time; the current MA value is
      NOT part of the reference distribution). Warmup (< min_window_days
      of history) → generate() returns None; a partial window
      (min_window_days ≤ span < window_days) fires normally and is
      flagged via metadata["window_partial"].
    - absolute mode (absolute_mode=True): fixed +50 / -5 ann. % (the
      legacy behavior, quantile-calibrated on 2023+ data).

    Score is continuous: 0.5 at the threshold, 1.0 at twice the
    threshold distance (e.g. absolute high: 50%→0.5, 100%→1.0; rolling
    thresholds use the same formula relative to the current percentile
    value).
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
        absolute_mode: bool = False,
        window_days: int = DEFAULT_WINDOW_DAYS,
        min_window_days: int = DEFAULT_MIN_WINDOW_DAYS,
        high_percentile: float = DEFAULT_HIGH_PERCENTILE,
        low_percentile: float = DEFAULT_LOW_PERCENTILE,
    ) -> None:
        """
        Args:
            high_threshold: absolute-mode 3-day funding MA (annualized %)
                above which the momentum regime fires (default 50).
            low_threshold: absolute-mode MA below which the rebound regime
                fires (default -5).
            window: rolling window (days) for the funding MA.
            momentum_targets: stocks to buy in the high regime (miners are
                excluded — they fall after high funding).
            rebound_targets: stocks to buy in the low regime.
            db_path: SQLite path (defaults to the shared data/btc_history.db).
            absolute_mode: use the legacy fixed thresholds instead of
                rolling percentiles (for comparison backtests).
            window_days: rolling-percentile lookback window in days
                (default 730 = 2 years).
            min_window_days: minimum history required before the rolling
                mode can fire (default 365 = 1 year; below this the signal
                is in warmup and returns None).
            high_percentile: momentum-regime percentile of the rolling
                3d-MA distribution (default 99.0).
            low_percentile: rebound-regime percentile (default 1.0).
        """
        if not 0.0 < low_percentile < high_percentile < 100.0:
            raise ValueError(
                "percentiles must satisfy 0 < low_percentile < "
                f"high_percentile < 100, got {low_percentile}/{high_percentile}"
            )
        if min_window_days > window_days:
            raise ValueError(
                f"min_window_days ({min_window_days}) cannot exceed "
                f"window_days ({window_days})"
            )
        self.high_threshold = float(high_threshold)
        self.low_threshold = float(low_threshold)
        self.window = int(window)
        self.momentum_targets = tuple(momentum_targets)
        self.rebound_targets = tuple(rebound_targets)
        self.absolute_mode = bool(absolute_mode)
        self.window_days = int(window_days)
        self.min_window_days = int(min_window_days)
        self.high_percentile = float(high_percentile)
        self.low_percentile = float(low_percentile)
        # Diagnostics for the last generate() call (None results carry no
        # metadata, so warmup/neutral/incomplete reasons are exposed here).
        self.last_eval_metadata: dict = {}
        store_kwargs = {"db_path": db_path} if db_path else {}
        self._store = HistoricalOHLCVStore(allow_fetch=False, **store_kwargs)
        self._stock_store = NasdaqDailyStore(assetclass="stocks", **store_kwargs)

    # ------------------------------------------------------------------
    # Signal
    # ------------------------------------------------------------------

    def generate(self, as_of: pd.Timestamp) -> SignalOutput | None:
        """Fire long when the funding MA (strictly before as_of) is extreme.

        Returns None when the 3-day MA window is incomplete, when the
        rolling mode is in warmup (< min_window_days of history), or when
        funding is in the neutral band between the two thresholds. The
        reason is always recorded in ``self.last_eval_metadata``.
        """
        as_of = _to_naive_utc(as_of)
        self.last_eval_metadata = {}

        # Funding load range: the MA window plus one extra day back for
        # the is_new_extreme check; the rolling mode additionally needs
        # the trailing window_days of MA history (each reference MA day d
        # uses [d-window+1, d], all strictly before as_of).
        if self.absolute_mode:
            load_start = as_of - pd.Timedelta(days=self.window + 1)
        else:
            load_start = as_of - pd.Timedelta(
                days=self.window_days + self.window + 1
            )

        funding = self._load_funding(_ms(load_start), _ms(as_of))
        if funding.empty:
            self.last_eval_metadata = {"status": "no_data"}
            return None

        daily = self._daily_avg_annualized(funding)
        ma = self._ma_series(daily)

        # Current evaluation: MA window [as_of-window, as_of-1].
        t_cur = as_of - pd.Timedelta(days=1)
        if t_cur not in ma.index:
            self.last_eval_metadata = {"status": "incomplete_ma_window"}
            return None
        rate_ma = float(ma.loc[t_cur])

        if self.absolute_mode:
            high_thr = self.high_threshold
            low_thr = self.low_threshold
            window_info: dict = {"mode": "absolute"}
        else:
            info = self._rolling_thresholds(ma, daily.index.min(), t_cur)
            if info.get("warmup"):
                self.last_eval_metadata = info
                return None
            high_thr = info.pop("high")
            low_thr = info.pop("low")
            window_info = info

        if rate_ma > high_thr:
            regime = "high"
            targets = self.momentum_targets
            denom = abs(high_thr) if abs(high_thr) > 1e-9 else 1.0
            excess = (rate_ma - high_thr) / denom
        elif rate_ma < low_thr:
            regime = "low"
            targets = self.rebound_targets
            denom = abs(low_thr) if abs(low_thr) > 1e-9 else 1.0
            excess = (low_thr - rate_ma) / denom
        else:
            self.last_eval_metadata = {"status": "neutral"}
            return None

        # Was the previous day's window already extreme in this regime?
        # The previous evaluation's thresholds are recomputed for its own
        # reference window so the dedup is exactly "would generate(as_of-1)
        # have fired in the same regime".
        t_prev = as_of - pd.Timedelta(days=2)
        is_new = True
        if t_prev in ma.index:
            prev_ma = float(ma.loc[t_prev])
            if self.absolute_mode:
                prev_high, prev_low = self.high_threshold, self.low_threshold
            else:
                prev_info = self._rolling_thresholds(
                    ma, daily.index.min(), t_prev
                )
                if prev_info.get("warmup"):
                    prev_high = prev_low = None  # prev day could not have fired
                else:
                    prev_high = prev_info["high"]
                    prev_low = prev_info["low"]
            if prev_high is not None:
                prev_extreme = (
                    prev_ma > prev_high
                    if regime == "high"
                    else prev_ma < prev_low
                )
                is_new = not prev_extreme

        score = min(0.5 + 0.5 * excess, 1.0)

        ma_days = pd.date_range(
            as_of - pd.Timedelta(days=self.window),
            as_of - pd.Timedelta(days=1),
            freq="D",
        )
        metadata = {
            "regime": regime,
            "targets": list(targets),
            "funding_ma_annualized_pct": round(rate_ma, 2),
            "funding_daily_avg_pct": {
                d.date().isoformat(): round(float(v), 2)
                for d, v in daily.reindex(ma_days).items()
            },
            "window_days": self.window,
            "high_threshold_pct": (
                self.high_threshold
                if self.absolute_mode
                else round(high_thr, 2)
            ),
            "low_threshold_pct": (
                self.low_threshold
                if self.absolute_mode
                else round(low_thr, 2)
            ),
            "is_new_extreme": is_new,
        }
        metadata.update(window_info)
        self.last_eval_metadata = {"status": "fired", **window_info}
        return SignalOutput(
            score=score,
            direction="long",
            confidence=score,
            metadata=metadata,
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

    def _ma_series(self, daily: pd.Series) -> pd.Series:
        """3-day MA of daily funding, defined only on calendar-complete
        windows (a missing day inside the window → NaN → dropped)."""
        if daily.empty:
            return pd.Series(dtype=float)
        cal = pd.date_range(daily.index.min(), daily.index.max(), freq="D")
        ma = daily.reindex(cal).rolling(self.window).mean().dropna()
        return ma

    def _rolling_thresholds(
        self, ma: pd.Series, data_first: pd.Timestamp, t: pd.Timestamp
    ) -> dict:
        """Rolling-percentile thresholds for the evaluation whose MA
        window ends on day ``t``.

        The reference distribution is the 3-day MA over the
        ``window_days`` days ENDING THE DAY BEFORE ``t`` (the current MA
        value is excluded), so everything used is strictly before as_of.
        """
        ref_end = t - pd.Timedelta(days=1)
        ref_start = t - pd.Timedelta(days=self.window_days)
        ref = ma[(ma.index >= ref_start) & (ma.index <= ref_end)]
        if ref.empty:
            return {
                "warmup": True,
                "status": "warmup",
                "available_span_days": 0,
                "min_window_days": self.min_window_days,
            }
        # Calendar span of possible reference days, bounded by data start
        # (the earliest MA day is data_first + window - 1).
        first_ma_day = max(
            data_first + pd.Timedelta(days=self.window - 1), ref_start
        )
        span = (ref_end - first_ma_day).days + 1
        if span < self.min_window_days:
            return {
                "warmup": True,
                "status": "warmup",
                "available_span_days": int(span),
                "min_window_days": self.min_window_days,
            }
        high = float(ref.quantile(self.high_percentile / 100.0))
        low = float(ref.quantile(self.low_percentile / 100.0))
        return {
            "high": high,
            "low": low,
            "mode": "rolling",
            "rolling_window_days": self.window_days,
            "min_window_days": self.min_window_days,
            "rolling_window_obs": int(len(ref)),
            "rolling_window_span_days": int(span),
            "window_partial": span < self.window_days,
            "high_percentile": self.high_percentile,
            "low_percentile": self.low_percentile,
            "window_mean_pct": round(float(ref.mean()), 2),
            "window_median_pct": round(float(ref.median()), 2),
        }

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
        return ann.groupby(dates).mean().sort_index()

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
