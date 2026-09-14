"""Data layer: fetching, caching, and building 40-year total-return series.

Series construction (newest -> oldest, each segment only covers dates before the
previous source's first date):

    QQQ  : QQQ (real, 1999-03+)  <- ^NDX price + div yield - ER (1985-10+)
    VOO  : VOO (real, 2010-05+)  <- SPY (real, 1993-01+)  <- ^SP500TR - ER
           <- ^GSPC price + div yield - ER (1927+)
    TQQQ : TQQQ (real, 2010-02+) <- synthetic 3x daily NDX price return minus
           2x financing at (13-week T-bill + spread) minus ER (1985-10+)

Real ETF data is total-return (dividends reinvested, net of the fund's own ER);
we additionally subtract dividend withholding tax (see config). Synthetic
segments get their dividend/ER assumptions applied explicitly.

FX: USD/CNY market history (CNY=X); before market data begins, annual official
rates are used (see _HIST_USDCNY).
"""

from __future__ import annotations

import logging
import pickle
from pathlib import Path

import numpy as np
import pandas as pd

from .config import InstrumentSpec, TRADING_DAYS

log = logging.getLogger("dca_backtest.data")

CACHE_DIR = Path(__file__).resolve().parent / "cache"

FETCH_SYMBOLS = [
    "QQQ", "SPY", "VOO", "TQQQ",  # real ETFs (total return via auto-adjusted close)
    "^NDX",                        # Nasdaq-100 price index (1985-10+)
    "^GSPC",                       # S&P 500 price index (1927+)
    "^SP500TR",                    # S&P 500 total return index (~1988+)
    "^IRX",                        # 13-week T-bill yield (%) for TQQQ financing
    "CNY=X",                       # USD/CNY
]

# (source symbol, segment kind) ordered newest-first
CHAINS: dict[str, list[tuple[str, str]]] = {
    "QQQ": [("QQQ", "real"), ("^NDX", "price")],
    "VOO": [("VOO", "real"), ("SPY", "real"), ("^SP500TR", "tr"), ("^GSPC", "price")],
    "TQQQ": [("TQQQ", "real"), ("^NDX", "levered")],
}

# Official USD/CNY annual rates (start-of-year approximation) used before the
# earliest market data. 1994 unification (~8.70) is a real historical devaluation.
_HIST_USDCNY: dict[int, float] = {
    1985: 2.94, 1986: 3.45, 1987: 3.72, 1988: 3.72, 1989: 3.77,
    1990: 4.78, 1991: 5.32, 1992: 5.51, 1993: 5.76, 1994: 8.70,
    1995: 8.32, 1996: 8.30, 1997: 8.29, 1998: 8.28, 1999: 8.28,
    2000: 8.28, 2001: 8.28, 2002: 8.28, 2003: 8.28, 2004: 8.28,
}


def _clean(s: pd.Series) -> pd.Series:
    s = s.astype(float)
    s = s[~s.index.duplicated(keep="last")].sort_index()
    return s.ffill().dropna()


def fetch_raw(refresh: bool = False, max_retries: int = 4, backoff_s: float = 45.0) -> dict[str, pd.Series]:
    """Download (or load cached) raw close series for all sources.

    Retries with backoff on Yahoo rate limits (they are transient, per-IP).
    """
    CACHE_DIR.mkdir(exist_ok=True)
    cache = CACHE_DIR / "raw_prices.pkl"
    if cache.exists() and not refresh:
        log.info("loading cached raw data from %s", cache)
        return pickle.loads(cache.read_bytes())

    import time

    import yfinance as yf  # imported lazily so offline tests don't need it

    data: dict[str, pd.Series] = {}
    for sym in FETCH_SYMBOLS:
        close = None
        for attempt in range(1, max_retries + 1):
            try:
                df = yf.download(sym, period="max", auto_adjust=True, progress=False)
            except Exception as exc:  # noqa: BLE001 - yfinance raises various errors
                log.warning("download failed for %s: %s", sym, exc)
                df = None
            if df is not None and len(df) > 0:
                close = df["Close"]
                if isinstance(close, pd.DataFrame):
                    close = close.iloc[:, 0]
                close = _clean(close)
                break
            log.warning(
                "no data for %s (attempt %d/%d); waiting %.0fs before retry",
                sym, attempt, max_retries, backoff_s * attempt,
            )
            time.sleep(backoff_s * attempt)
        if close is not None and len(close):
            data[sym] = close
            log.info("fetched %s: %s .. %s (%d rows)", sym, close.index[0].date(), close.index[-1].date(), len(close))
        else:
            log.warning("giving up on %s after %d attempts", sym, max_retries)

    missing = [s for s in ("^NDX", "^GSPC", "CNY=X", "^IRX") if s not in data]
    if missing:
        raise RuntimeError(f"required data sources unavailable: {missing}")

    with open(cache, "wb") as fh:
        pickle.dump(data, fh)
    log.info("cached raw data to %s", cache)
    return data


def _segment_level(
    s: pd.Series,
    kind: str,
    spec: InstrumentSpec,
    wht: float,
    rf_pct: pd.Series | None,
) -> pd.Series:
    """Turn a raw source series into a level series with all drags applied.

    kind:
        "real"   - total-return data; subtract withholding tax drag only
        "tr"     - total-return index; subtract ER + withholding tax drag
        "price"  - price index; add net dividends, subtract ER
        "levered"- synthetic levered ETF (3x daily, financing, ER)
    """
    s = _clean(s)
    if kind == "real":
        drag = wht * spec.div_yield_for_tax / TRADING_DAYS
        return s * (1.0 - drag) ** np.arange(len(s), dtype=float)
    if kind == "tr":
        daily_cost = (spec.er_annual + wht * spec.div_yield_for_tax) / TRADING_DAYS
        fac = 1.0 + s.pct_change(fill_method=None).fillna(0.0) - daily_cost
        return fac.cumprod()
    if kind == "price":
        daily_adj = (spec.div_yield_annual * (1.0 - wht) - spec.er_annual) / TRADING_DAYS
        fac = 1.0 + s.pct_change(fill_method=None).fillna(0.0) + daily_adj
        return fac.cumprod()
    if kind == "levered":
        r = s.pct_change(fill_method=None).fillna(0.0)
        rf = (
            rf_pct.reindex(s.index).ffill().bfill().fillna(4.0).clip(lower=0.0)
            / 100.0
            / TRADING_DAYS
        )
        fac = (
            1.0
            + 3.0 * r
            - 2.0 * rf
            - 2.0 * spec.financing_spread / TRADING_DAYS
            - spec.er_annual / TRADING_DAYS
        )
        return fac.cumprod()
    raise ValueError(f"unknown segment kind: {kind}")


def _chain_symbol(
    sym: str,
    raw: dict[str, pd.Series],
    spec: InstrumentSpec,
    wht: float,
    rf_pct: pd.Series | None,
) -> tuple[pd.Series, list[dict]]:
    level: pd.Series | None = None
    meta: list[dict] = []
    for key, kind in CHAINS[sym]:
        src = raw.get(key)
        if src is None or len(src) == 0:
            log.warning("source %s unavailable - skipped in chain for %s", key, sym)
            continue
        seg = _segment_level(src, kind, spec, wht, rf_pct)
        if level is None:
            level = seg
        else:
            older = seg[seg.index < level.index[0]]
            if older.empty:
                continue
            # rescale so the two segments join continuously
            older = older * (level.iloc[0] / older.iloc[-1])
            level = pd.concat([older, level])
        meta.append(
            {
                "symbol": sym,
                "source": key,
                "kind": kind,
                "start": level.index[0],
                "end": level.index[-1],
            }
        )
    if level is None:
        raise RuntimeError(f"no usable data for {sym}")
    return level, meta


def build_growth_levels(
    raw: dict[str, pd.Series],
    instruments: dict[str, InstrumentSpec],
    wht: float,
    symbols: list[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build aligned growth-index levels (arbitrary scale) per symbol.

    Returns (levels_df, provenance_df). Only relative changes matter to the
    engine, so each series starts wherever its data begins.
    """
    rf_pct = raw.get("^IRX")
    if rf_pct is None:
        raise RuntimeError("^IRX (T-bill) data required for synthetic TQQQ financing")

    symbols = symbols or list(instruments)
    levels: dict[str, pd.Series] = {}
    meta_rows: list[dict] = []
    for sym in symbols:
        spec = instruments[sym]
        lvl, meta = _chain_symbol(sym, raw, spec, wht, rf_pct)
        levels[sym] = lvl
        meta_rows.extend(meta)
        log.info("%s series: %s .. %s", sym, lvl.index[0].date(), lvl.index[-1].date())

    df = pd.DataFrame(levels).sort_index()
    provenance = pd.DataFrame(meta_rows)
    return df, provenance


def build_fx(raw: dict[str, pd.Series]) -> pd.Series:
    """USD/CNY series, extended backwards with annual official rates."""
    s = raw.get("CNY=X")
    if s is None or len(s) == 0:
        raise RuntimeError("FX data (CNY=X) unavailable")
    s = _clean(s)
    first_year = s.index[0].year
    if first_year > 1985:
        idx = pd.to_datetime([f"{y}-01-01" for y in range(1985, first_year)])
        vals = [_HIST_USDCNY.get(y, 8.28) for y in range(1985, first_year)]
        s = pd.concat([pd.Series(vals, index=idx, dtype=float), s])
    return s.sort_index()
