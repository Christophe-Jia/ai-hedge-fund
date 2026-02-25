"""
Unit tests for technical indicator functions in src/agents/technicals.py.

These tests require no network access.  They also document two known bugs:
  - Bug 1: calculate_adx() mutates its input DataFrame (adds ~10 extra columns).
  - Bug 2: calculate_trend_signals() has no guard against < 55 data points.
"""

import math

import numpy as np
import pandas as pd
import pytest

from src.agents.technicals import (
    calculate_adx,
    calculate_bollinger_bands,
    calculate_ema,
    calculate_rsi,
    calculate_trend_signals,
)


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def price_df() -> pd.DataFrame:
    """80-day synthetic OHLCV DataFrame – sufficient for EMA-55 computation."""
    dates = pd.date_range("2023-01-01", periods=80, freq="D")
    np.random.seed(42)
    closes = 100 + np.cumsum(np.random.randn(80))
    return pd.DataFrame(
        {
            "open": closes - 0.5,
            "high": closes + 1.5,
            "low": closes - 1.5,
            "close": closes,
            "volume": 1_000_000,
        },
        index=dates,
    )


@pytest.fixture
def rising_df() -> pd.DataFrame:
    """80-day monotonically rising price series."""
    dates = pd.date_range("2023-01-01", periods=80, freq="D")
    closes = np.linspace(50, 200, 80)
    return pd.DataFrame(
        {
            "open": closes - 0.5,
            "high": closes + 1.0,
            "low": closes - 1.0,
            "close": closes,
            "volume": 1_000_000,
        },
        index=dates,
    )


@pytest.fixture
def falling_df() -> pd.DataFrame:
    """80-day monotonically falling price series."""
    dates = pd.date_range("2023-01-01", periods=80, freq="D")
    closes = np.linspace(200, 50, 80)
    return pd.DataFrame(
        {
            "open": closes + 0.5,
            "high": closes + 1.0,
            "low": closes - 1.0,
            "close": closes,
            "volume": 1_000_000,
        },
        index=dates,
    )


# ---------------------------------------------------------------------------
# calculate_ema
# ---------------------------------------------------------------------------


def test_calculate_ema_basic(price_df):
    """EMA(20) should return a Series with the same length as the input."""
    result = calculate_ema(price_df, 20)

    assert isinstance(result, pd.Series), f"Expected Series, got {type(result)}"
    assert len(result) == len(price_df), (
        f"Length mismatch: expected {len(price_df)}, got {len(result)}"
    )
    # Final value should be within a reasonable range of the close prices
    last_close = price_df["close"].iloc[-1]
    assert abs(result.iloc[-1] - last_close) < 50, "EMA final value should be near recent close"


def test_calculate_ema_different_windows(price_df):
    """Short-window EMA should react more quickly than long-window EMA.

    On an 80-point random walk the values themselves may differ; we just
    assert that EMA(8) and EMA(21) produce valid Series of the right length.
    """
    ema_8 = calculate_ema(price_df, 8)
    ema_21 = calculate_ema(price_df, 21)

    assert len(ema_8) == len(price_df)
    assert len(ema_21) == len(price_df)

    # Short EMA should track price more tightly (smaller lag)
    # Verify the absolute difference from close is smaller for EMA-8 vs EMA-21
    diff_8 = abs(ema_8 - price_df["close"])
    diff_21 = abs(ema_21 - price_df["close"])
    assert diff_8.mean() < diff_21.mean(), (
        "EMA-8 should track close price more tightly than EMA-21"
    )


# ---------------------------------------------------------------------------
# calculate_rsi
# ---------------------------------------------------------------------------


def test_calculate_rsi_range(price_df):
    """RSI values should be within [0, 100], excluding the initial NaN warmup."""
    rsi = calculate_rsi(price_df, period=14)

    valid_rsi = rsi.dropna()
    assert len(valid_rsi) > 0, "Should have non-NaN RSI values"
    assert (valid_rsi >= 0).all(), f"RSI below 0 detected: {valid_rsi[valid_rsi < 0]}"
    assert (valid_rsi <= 100).all(), f"RSI above 100 detected: {valid_rsi[valid_rsi > 100]}"


# ---------------------------------------------------------------------------
# calculate_bollinger_bands
# ---------------------------------------------------------------------------


def test_calculate_bollinger_bands_structure(price_df):
    """Upper band should be above SMA and lower band should be below SMA everywhere they are finite."""
    upper, lower = calculate_bollinger_bands(price_df, window=20)

    assert isinstance(upper, pd.Series), "Upper band should be a Series"
    assert isinstance(lower, pd.Series), "Lower band should be a Series"

    # Compute middle band for comparison
    middle = price_df["close"].rolling(20).mean()

    # Where all three are finite, upper > middle > lower must hold
    mask = upper.notna() & lower.notna() & middle.notna()
    assert mask.any(), "Expected at least some non-NaN Bollinger values"
    assert (upper[mask] > middle[mask]).all(), "Upper band should always exceed middle band"
    assert (middle[mask] > lower[mask]).all(), "Middle band should always exceed lower band"


# ---------------------------------------------------------------------------
# calculate_adx – Bug documentation
# ---------------------------------------------------------------------------


def test_calculate_adx_mutates_input_df_bug(price_df):
    """BUG: calculate_adx() adds extra columns to the caller's DataFrame.

    This test documents the side-effect behaviour: after calling calculate_adx
    with the original (not copied) DataFrame, the caller's DataFrame gains the
    intermediate computation columns injected by the function.
    """
    original_cols = set(price_df.columns)

    # Intentionally pass the *original* df to demonstrate the mutation bug
    _ = calculate_adx(price_df)

    added_cols = set(price_df.columns) - original_cols
    mutation_cols = {"adx", "+di", "-di"}
    assert mutation_cols.issubset(added_cols), (
        f"Bug not reproduced: expected columns {mutation_cols} to be added, "
        f"but only {added_cols} were added."
    )


def test_calculate_adx_returns_valid_values(price_df):
    """ADX values should be in [0, 100] for valid input.

    Uses .copy() to work around the mutation bug documented in
    test_calculate_adx_mutates_input_df_bug.
    """
    result = calculate_adx(price_df.copy(), period=14)

    assert isinstance(result, pd.DataFrame), f"Expected DataFrame, got {type(result)}"
    for col in ("adx", "+di", "-di"):
        assert col in result.columns, f"Missing column '{col}' in ADX result"

    adx_valid = result["adx"].dropna()
    assert len(adx_valid) > 0, "Should have non-NaN ADX values"
    assert (adx_valid >= 0).all(), "ADX should be >= 0"
    assert (adx_valid <= 100).all(), "ADX should be <= 100"


# ---------------------------------------------------------------------------
# calculate_trend_signals
# ---------------------------------------------------------------------------


def test_calculate_trend_signals_bullish(rising_df):
    """Monotonically rising series should yield a bullish signal."""
    # calculate_trend_signals calls calculate_adx internally, which mutates the
    # df – pass a copy so other tests are not affected.
    result = calculate_trend_signals(rising_df.copy())

    assert result["signal"] == "bullish", (
        f"Expected 'bullish' for rising price series, got '{result['signal']}'"
    )
    confidence = result["confidence"]
    assert 0 < confidence <= 1, f"Confidence should be in (0, 1], got {confidence}"


def test_calculate_trend_signals_bearish(falling_df):
    """Monotonically falling series should yield a bearish signal."""
    result = calculate_trend_signals(falling_df.copy())

    assert result["signal"] == "bearish", (
        f"Expected 'bearish' for falling price series, got '{result['signal']}'"
    )


def test_calculate_trend_signals_insufficient_data_warning():
    """BUG: calculate_trend_signals() has no guard for < 55 data points.

    With only 30 rows the EMA-55 computation returns all-NaN values.
    The function currently does *not* raise an error – it silently returns a
    result whose confidence is NaN (trend_strength derived from NaN ADX).
    This test documents that behaviour; it should be updated once a
    protective check is added to the function.
    """
    dates = pd.date_range("2023-01-01", periods=30, freq="D")
    np.random.seed(0)
    closes = 100 + np.cumsum(np.random.randn(30))
    small_df = pd.DataFrame(
        {
            "open": closes - 0.5,
            "high": closes + 1.0,
            "low": closes - 1.0,
            "close": closes,
            "volume": 1_000_000,
        },
        index=dates,
    )

    # The function should not raise — it silently handles short data (the bug)
    result = calculate_trend_signals(small_df.copy())

    # confidence is derived from ADX/100; with only 30 points ADX can be NaN
    # or a very small value.  Either way the function returns *something*.
    assert "signal" in result, "Result should always contain 'signal'"
    assert "confidence" in result, "Result should always contain 'confidence'"

    # Document: no ValueError is raised for insufficient data (the missing guard)
    # If a future fix raises ValueError here, update this test accordingly.
