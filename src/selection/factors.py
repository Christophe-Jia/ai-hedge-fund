"""Cross-sectional factor scoring for stock selection.

Factors operate on a close-price DataFrame (index=trading date, columns=
ticker) and return per-ticker scores. Higher score = more desirable.

All factor functions are look-ahead safe by construction: they only use
rows strictly before `as_of` (exclusive).
"""

from __future__ import annotations

import pandas as pd


def momentum_12_1(
    closes: pd.DataFrame,
    as_of: pd.Timestamp,
    lookback_bars: int = 252,
    skip_recent_bars: int = 21,
) -> pd.Series:
    """Classic 12-1 momentum: return over the window ending one month ago.

    score = close[t - skip] / close[t - lookback] - 1, where t is the last
    trading day STRICTLY BEFORE `as_of` (exclusive — no same-day signal).

    Tickers with insufficient history (fewer than `lookback_bars` rows
    before as_of) get NaN and are excluded from ranking by the engine.
    """
    hist = closes[closes.index < as_of]
    if len(hist) < lookback_bars + 1:
        return pd.Series(dtype=float)
    recent = hist.iloc[-(skip_recent_bars + 1)] if skip_recent_bars > 0 else hist.iloc[-1]
    base = hist.iloc[-(lookback_bars + 1)] if skip_recent_bars > 0 else hist.iloc[-(lookback_bars)]
    return (recent / base - 1.0).astype(float)


def momentum_n(
    closes: pd.DataFrame,
    as_of: pd.Timestamp,
    lookback_bars: int,
) -> pd.Series:
    """Plain N-bar momentum (no recent-month skip)."""
    hist = closes[closes.index < as_of]
    if len(hist) < lookback_bars + 1:
        return pd.Series(dtype=float)
    return (hist.iloc[-1] / hist.iloc[-(lookback_bars + 1)] - 1.0).astype(float)
