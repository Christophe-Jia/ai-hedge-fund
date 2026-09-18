"""
US daily price data via Nasdaq's public quote API (ETFs AND stocks).

The endpoint (https://api.nasdaq.com/api/quote/{sym}/historical) serves
true daily OHLCV — no registration, no key — and is reachable from
mainland China corporate networks where Yahoo (429) and Stooq (IP-blocked)
fail. History depth: ~10 years per request window.

Data notes:
- Closes are raw exchange closes (unadjusted). For the supported ETFs there
  have been no splits in the past decade, so prices are split-consistent;
  individual STOCKS can and do split — cross-sectional price momentum is
  robust to this, and **the API DOES serve split-adjusted prices**
  (verified 2026-09-15: AAPL 2020-08-28 = 124.81 = 499.23/4;
  NVDA 2024-06-07 = 120.89 = /10; TSLA 2022-08-24 = 297.10 = /3).
  Therefore a raw close is NOT a historical price level: for market cap or
  any absolute-price feature you MUST apply the split factor — see
  data/fundamentals.db `splits` table (142 splits, 17/17 known splits
  matched, built from EDGAR as-filed share counts + restatement
  corroboration by scripts/fetch_edgar_fundamentals.py).
- Dividends are NOT reinvested: pass `div_yield_annual` to
  `get_close_series()` for an approximate total-return series
  (VOO ~1.3%/yr, QQQ ~0.6%/yr as of 2026).
- Volume strings arrive with thousands separators ("31,414,410"); stock
  prices arrive with a leading "$" ("$326.57").

Usage:
    etf = NasdaqDailyStore()                       # ETFs (assetclass=etf)
    etf.fetch_and_store("QQQ")
    stocks = NasdaqDailyStore(assetclass="stocks")
    stocks.fetch_and_store("AAPL")
    s = stocks.get_close_series("AAPL", "2020-01-01", "2026-09-01")
"""

from __future__ import annotations

import json
import re
import time
import urllib.request
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from .historical_store import HistoricalOHLCVStore

_NASDAQ_URL = (
    "https://api.nasdaq.com/api/quote/{symbol}/historical"
    "?assetclass={assetclass}&fromdate={from_date}&todate={to_date}&limit=9999"
)
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.nasdaq.com",
    "Referer": "https://www.nasdaq.com/",
}

# Default window: Nasdaq only serves ~10 years; request a generous 12y span
# and keep whatever comes back.
_DEFAULT_FROM = "2014-01-01"

# ETF whitelist (assetclass=etf). Stocks accept any valid ticker symbol.
# Sector/asset-class ETFs added 2026-09-18 for the Merrill-clock hypothesis
# (hypotheses/registry.jsonl -> merrill_clock_regime_rotation).
_ETF_SUPPORTED = {
    "QQQ", "VOO", "SPY", "QLD", "TQQQ", "IWM", "DIA", "GLD", "TLT", "GBTC", "BITB",
    "BIL", "DBC", "IEF",
    "XLB", "XLE", "XLF", "XLI", "XLK", "XLP", "XLU", "XLV", "XLY",
}

# Approximate trailing dividend yields (for total-return adjustment only).
_APPROX_DIV_YIELDS: dict[str, float] = {
    "QQQ": 0.006,
    "QLD": 0.005,
    "VOO": 0.013,
    "SPY": 0.013,
    "TQQQ": 0.0,
    "IWM": 0.011,
    "DIA": 0.016,
    "GLD": 0.0,
    "TLT": 0.038,
    # sector / asset-class estimates (2026-09-18, rough trailing yields)
    "BIL": 0.045,
    "DBC": 0.0,
    "IEF": 0.033,
    "XLB": 0.018,
    "XLE": 0.033,
    "XLF": 0.015,
    "XLI": 0.014,
    "XLK": 0.007,
    "XLP": 0.024,
    "XLU": 0.030,
    "XLV": 0.016,
    "XLY": 0.008,
}

_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")


def _num(x) -> float:
    """Parse a Nasdaq numeric string: strips '$', commas, 'N/A' -> raises."""
    return float(str(x).replace("$", "").replace(",", "").strip())


class NasdaqDailyStore:
    """Download-and-cache wrapper around Nasdaq's public history API.

    Args:
        db_path:     SQLite path (defaults to the shared data/btc_history.db).
        assetclass:  "etf" or "stocks" — selects the Nasdaq API asset class
                     and the market_type used in the local store.
        timeout:     HTTP timeout in seconds.
        politeness_s: sleep between consecutive HTTP fetches (Nasdaq
                     throttles rapid-fire requests).
    """

    def __init__(
        self,
        db_path: Optional[str] = None,
        assetclass: str = "etf",
        timeout: float = 30.0,
        politeness_s: float = 1.5,
    ) -> None:
        if assetclass not in ("etf", "stocks"):
            raise ValueError(f"assetclass must be 'etf' or 'stocks', got {assetclass!r}")
        self._assetclass = assetclass
        self._market_type = assetclass  # "etf" | "stocks" in the shared store
        self._store = HistoricalOHLCVStore(
            **({"db_path": db_path} if db_path else {}),
            allow_fetch=False,  # never CCXT-fetch for US symbols
        )
        self._timeout = timeout
        self._politeness_s = politeness_s

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    def fetch_and_store(
        self, symbol: str, from_date: str = _DEFAULT_FROM, to_date: str | None = None
    ) -> int:
        """Download Nasdaq daily history for `symbol` and upsert.

        Returns the number of rows written.
        """
        sym = symbol.upper()
        if self._assetclass == "etf":
            if sym not in _ETF_SUPPORTED:
                raise ValueError(
                    f"Unsupported ETF {symbol!r}. Known: {sorted(_ETF_SUPPORTED)}"
                )
        else:
            if not _TICKER_RE.match(sym):
                raise ValueError(f"Invalid ticker symbol {symbol!r}")
        to_date = to_date or datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

        payload = self._http_get_json(sym, from_date, to_date)
        rows = self._parse_payload(payload)
        if not rows:
            raise RuntimeError(
                f"Nasdaq returned no rows for {sym} "
                f"[{from_date}, {to_date}] (message: "
                f"{(payload or {}).get('message')!r})"
            )
        return self._store.upsert_ohlcv(sym, self._market_type, "1d", rows)

    def _http_get_json(self, sym: str, from_date: str, to_date: str) -> dict | None:
        url = _NASDAQ_URL.format(
            symbol=sym, assetclass=self._assetclass, from_date=from_date, to_date=to_date
        )
        last_err: Exception | None = None
        for attempt in range(3):
            try:
                req = urllib.request.Request(url, headers=_HEADERS)
                with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                    text = resp.read().decode("utf-8", errors="replace")
                payload = json.loads(text)
                if isinstance(payload, dict) and payload.get("data") is not None:
                    return payload
                # 200 with null data (or throttle HTML) — back off and retry
                last_err = RuntimeError(f"non-data payload: {text[:120]!r}")
            except Exception as exc:  # noqa: BLE001 — network errors vary
                last_err = exc
            time.sleep(self._politeness_s * (attempt + 1))
        raise RuntimeError(f"Nasdaq fetch failed for {sym} after retries: {last_err}")

    @staticmethod
    def _parse_payload(payload: dict | None) -> list[list]:
        """Parse Nasdaq historical payload into upsert_ohlcv rows.

        Rows arrive newest-first with MM/DD/YYYY dates, comma-formatted
        numbers, and (for stocks) a leading '$' on prices; returned rows
        are oldest-first.
        """
        if not payload:
            return []
        table = (payload.get("data") or {}).get("tradesTable") or {}
        raw_rows = table.get("rows") or []
        rows: list[list] = []
        for rec in raw_rows:
            try:
                dt = datetime.strptime(rec["date"], "%m/%d/%Y").replace(
                    tzinfo=timezone.utc
                )
                ts = int(dt.timestamp() * 1000)
                o = _num(rec["open"])
                h = _num(rec["high"])
                low = _num(rec["low"])
                c = _num(rec["close"])
            except (KeyError, ValueError, AttributeError):
                continue  # skip rows with bad prices/dates
            # volume may be N/A — keep the row with zero volume
            try:
                v = _num(rec.get("volume", "0")) if rec.get("volume") else 0.0
            except (ValueError, AttributeError):
                v = 0.0
            if o > 0 and c > 0 and h >= low > 0:
                rows.append([ts, o, h, low, c, v])
        rows.sort(key=lambda r: r[0])
        return rows

    # ------------------------------------------------------------------
    # Query (look-ahead safe via the shared store)
    # ------------------------------------------------------------------

    def get_daily(self, symbol: str, start: str, end: str) -> pd.DataFrame:
        """Daily OHLCV DataFrame indexed by UTC date, [start, end)."""
        start_ts = int(
            datetime.fromisoformat(start).replace(tzinfo=timezone.utc).timestamp() * 1000
        )
        end_ts = int(
            datetime.fromisoformat(end).replace(tzinfo=timezone.utc).timestamp() * 1000
        )
        df = self._store.get_ohlcv(symbol.upper(), self._market_type, "1d", start_ts, end_ts)
        if df.empty:
            return df
        out = df.set_index(pd.to_datetime(df["ts"], unit="ms", utc=True))
        return out[["open", "high", "low", "close", "volume"]].sort_index()

    def get_close_series(
        self,
        symbol: str,
        start: str,
        end: str,
        div_yield_annual: float | None = None,
    ) -> pd.Series:
        """Close price series for [start, end).

        Args:
            div_yield_annual: if given, accrues a daily dividend yield so the
                series approximates TOTAL RETURN (Nasdaq closes are
                price-only). Defaults to the built-in estimate for known
                ETFs, 0.0 for stocks; pass a number to override.
        """
        df = self.get_daily(symbol, start, end)
        if df.empty:
            return pd.Series(dtype=float)
        if div_yield_annual is None:
            div_yield_annual = _APPROX_DIV_YIELDS.get(symbol.upper(), 0.0)
        s = df["close"]
        if div_yield_annual > 0:
            # approximate daily accrual on trading days (252/yr)
            daily = (1.0 + div_yield_annual) ** (1.0 / 252.0) - 1.0
            s = s * (1.0 + daily) ** pd.Series(range(len(s)), index=s.index)
        return s

    def get_coverage(self, symbol: str) -> tuple[str | None, str | None, int]:
        """(first_date, last_date, row_count) for debugging/verification."""
        import sqlalchemy as sa

        with self._store._engine.connect() as conn:
            row = conn.execute(
                sa.text(
                    "SELECT MIN(ts), MAX(ts), COUNT(*) FROM ohlcv "
                    "WHERE symbol = :s AND market_type = :m AND timeframe = '1d'"
                ),
                {"s": symbol.upper(), "m": self._market_type},
            ).fetchone()
        if not row or row[0] is None:
            return None, None, 0
        fmt = lambda ts: datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        return fmt(row[0]), fmt(row[1]), int(row[2])


# Backward-compatible alias: the ETF-specialised name used at import sites.
EtfDailyStore = NasdaqDailyStore
