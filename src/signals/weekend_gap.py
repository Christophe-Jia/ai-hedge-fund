"""Signal 1: weekend BTC gap -> Monday crypto-proxy stock move.

Hypothesis (validated in scripts/backtest_weekend_gap.py):
BTC trades 24/7 while equities trade weekdays only. When BTC makes a
large move over a weekend, crypto-proxy stocks (COIN, MSTR, MARA)
under-react at the Monday open — the Monday gap correlates 0.82 with the
BTC weekend move and mostly does NOT fill (0-20% fill rate).

Signal rule:
- Evaluated on the first trading day after a weekend (`as_of`).
- BTC Friday close -> Sunday close return (BTC trades 7 days/week).
- If |return| >= threshold (default 5%), signal in the direction of the
  BTC move, with magnitude proportional to the move size.

Look-ahead safety: only BTC bars dated on or before the Sunday (i.e.
strictly before `as_of`) are read.
"""

from __future__ import annotations

import pandas as pd

from src.data.historical_store import HistoricalOHLCVStore
from src.data.nasdaq_store import NasdaqDailyStore
from src.signals.base import Signal, SignalOutput

BTC_SYMBOL = "BTC/USDT"
DEFAULT_TARGETS = ("COIN", "MSTR", "MARA")

# BTC daily bars must be at most this stale, otherwise the store cannot
# see the most recent weekend and the signal is unreliable.
_BTC_MAX_STALE_DAYS = 3
# US equities: Friday data checked on the following Monday is 3 days old
# (allow 4 to cover long weekends).
_STOCK_MAX_STALE_DAYS = 4


def _to_naive_utc(ts: pd.Timestamp) -> pd.Timestamp:
    """Normalize any Timestamp to tz-naive UTC midnight.

    BTC daily bars come out of SQLite as tz-naive UTC (ts in ms); stock
    data from NasdaqDailyStore is tz-aware UTC. Everything in this signal
    is compared on the tz-naive UTC date axis.
    """
    ts = pd.Timestamp(ts)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts.normalize()


def _ms(ts: pd.Timestamp) -> int:
    """Epoch milliseconds for a tz-naive UTC Timestamp."""
    return int(ts.value // 1_000_000)


class WeekendGapSignal(Signal):
    """Signal: BTC weekend move > threshold -> trade crypto-proxy stocks on Monday."""

    name = "weekend_gap"
    description = (
        "BTC 24/7 trading creates an information gap over weekends; "
        "crypto stocks under-react at the Monday open"
    )

    def __init__(
        self,
        threshold: float = 5.0,
        targets: tuple[str, ...] | list[str] = DEFAULT_TARGETS,
        max_eval_lag_days: int = 2,
        db_path: str | None = None,
    ) -> None:
        """
        Args:
            threshold: BTC weekend move (%) required to fire the signal.
            targets: crypto-proxy stock symbols to trade.
            max_eval_lag_days: how many days after the Sunday the signal may
                still be evaluated. 2 = Monday, or Tuesday when Monday is a
                market holiday.
            db_path: SQLite path (defaults to the shared data/btc_history.db).
        """
        self.threshold = float(threshold)
        self.targets = tuple(targets)
        self.max_eval_lag_days = int(max_eval_lag_days)
        store_kwargs = {"db_path": db_path} if db_path else {}
        self._btc_store = HistoricalOHLCVStore(allow_fetch=False, **store_kwargs)
        self._stock_store = NasdaqDailyStore(assetclass="stocks", **store_kwargs)

    # ------------------------------------------------------------------
    # Signal
    # ------------------------------------------------------------------

    def generate(self, as_of: pd.Timestamp) -> SignalOutput | None:
        """Check if the most recent completed weekend had a large BTC move.

        Returns None when `as_of` is not within `max_eval_lag_days` of a
        completed weekend, when BTC weekend data is missing, or when the
        weekend move is below the threshold.
        """
        as_of = _to_naive_utc(as_of)

        # Locate the most recent Sunday STRICTLY before as_of.
        # Monday->1, Tuesday->2, ..., Saturday->6, Sunday->7.
        days_back = (as_of.dayofweek + 1) % 7 or 7
        if days_back > self.max_eval_lag_days:
            return None  # the post-weekend window has passed
        sunday = as_of - pd.Timedelta(days=days_back)
        friday = sunday - pd.Timedelta(days=2)

        btc = self._load_btc_daily(friday - pd.Timedelta(days=2), as_of)
        if btc.empty:
            return None  # no BTC data for the weekend window
        window = btc[(btc.index >= friday) & (btc.index <= sunday)]
        fri_rows = window[window.index.dayofweek == 4]
        sun_rows = window[window.index.dayofweek == 6]
        if fri_rows.empty or sun_rows.empty:
            return None  # BTC weekend data incomplete

        fri_close = float(fri_rows["close"].iloc[-1])
        sun_close = float(sun_rows["close"].iloc[-1])
        if fri_close <= 0:
            return None

        weekend_ret = (sun_close / fri_close - 1.0) * 100.0
        if abs(weekend_ret) < self.threshold:
            return None

        # Continuous sizing: threshold move -> 0.5, 2x threshold -> 1.0.
        magnitude = min(abs(weekend_ret) / (2.0 * self.threshold), 1.0)

        return SignalOutput(
            score=magnitude if weekend_ret > 0 else -magnitude,
            direction="long" if weekend_ret > 0 else "short",
            confidence=magnitude,
            metadata={
                "targets": list(self.targets),
                "btc_weekend_return_pct": round(weekend_ret, 2),
                "btc_friday_close": fri_close,
                "btc_sunday_close": sun_close,
                "friday": friday.date().isoformat(),
                "sunday": sunday.date().isoformat(),
                "threshold_pct": self.threshold,
            },
        )

    # ------------------------------------------------------------------
    # Data health
    # ------------------------------------------------------------------

    def data_health(self) -> dict:
        """Check BTC and target-stock daily data availability and freshness."""
        today = pd.Timestamp.now(tz="UTC").tz_localize(None).normalize()
        health: dict = {"signal": self.name, "ok": False, "btc": {}, "stocks": {}}

        try:
            last_ts = self._btc_store.get_latest_ts(BTC_SYMBOL, "spot", "1d")
            if last_ts is None:
                health["btc"] = {"ok": False, "reason": f"no {BTC_SYMBOL} spot 1d data"}
            else:
                last = pd.Timestamp(last_ts, unit="ms", tz="UTC").tz_localize(None)
                stale = (today - last.normalize()).days
                health["btc"] = {
                    "ok": stale <= _BTC_MAX_STALE_DAYS,
                    "last_date": last.date().isoformat(),
                    "stale_days": stale,
                    "max_stale_days": _BTC_MAX_STALE_DAYS,
                }
        except Exception as exc:  # noqa: BLE001 — report, don't crash
            health["btc"] = {"ok": False, "error": str(exc)}

        for sym in self.targets:
            try:
                first, last, rows = self._stock_store.get_coverage(sym)
                if rows == 0:
                    health["stocks"][sym] = {"ok": False, "reason": "no data"}
                else:
                    stale = (today - pd.Timestamp(last)).days
                    health["stocks"][sym] = {
                        "ok": stale <= _STOCK_MAX_STALE_DAYS,
                        "first_date": first,
                        "last_date": last,
                        "rows": rows,
                        "stale_days": stale,
                    }
            except Exception as exc:  # noqa: BLE001 — report, don't crash
                health["stocks"][sym] = {"ok": False, "error": str(exc)}

        btc_ok = bool(health["btc"].get("ok"))
        any_stock_ok = any(v.get("ok") for v in health["stocks"].values())
        health["ok"] = btc_ok and any_stock_ok
        return health

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _load_btc_daily(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """BTC/USDT spot daily bars for [start, end), tz-naive UTC date index."""
        df = self._btc_store.get_ohlcv(BTC_SYMBOL, "spot", "1d", _ms(start), _ms(end))
        if df.empty:
            return df
        df = df.copy()
        df.index = pd.to_datetime(df["ts"], unit="ms").dt.normalize()
        return df.sort_index()
