"""
Tests for src/data/crypto.py - data fetching and conversion utilities.

Network tests are marked with @pytest.mark.network and require
Binance API accessibility. Run them with:
    poetry run pytest tests/test_crypto_data.py -v -m network
"""

import pandas as pd
import pytest

from src.data.crypto import crypto_prices_to_df, get_crypto_prices, get_crypto_ticker
from src.data.models import Price


# ---------------------------------------------------------------------------
# Network tests – require live Binance connection
# ---------------------------------------------------------------------------


@pytest.mark.network
def test_get_sol_ticker_returns_valid_price():
    """get_crypto_ticker should return a dict with the expected fields and a positive last price."""
    result = get_crypto_ticker("SOL/USDT")

    required_fields = {"symbol", "last", "bid", "ask", "volume", "timestamp"}
    assert required_fields.issubset(result.keys()), (
        f"Missing fields: {required_fields - result.keys()}"
    )
    assert result["last"] is not None, "Expected a 'last' price, got None"
    assert result["last"] > 0, f"Expected SOL price > 0, got {result['last']}"


@pytest.mark.network
def test_get_sol_prices_returns_historical_candles():
    """get_crypto_prices should return a non-empty list of Price objects for a valid date range."""
    prices = get_crypto_prices("SOL/USDT", "2024-01-01", "2024-01-07")

    assert isinstance(prices, list), "Expected a list of Price objects"
    assert len(prices) > 0, "Expected at least one candle"

    for price in prices:
        assert isinstance(price, Price), f"Expected Price object, got {type(price)}"
        assert price.open is not None, "Expected non-None open"
        assert price.high is not None, "Expected non-None high"
        assert price.low is not None, "Expected non-None low"
        assert price.close is not None, "Expected non-None close"
        assert price.volume is not None, "Expected non-None volume"
        assert price.time is not None, "Expected non-None time"
        assert price.close > 0, f"Expected close > 0, got {price.close}"


@pytest.mark.network
def test_get_crypto_ticker_unknown_symbol_raises():
    """get_crypto_ticker should raise an exception for an unrecognized symbol."""
    with pytest.raises(Exception):
        get_crypto_ticker("INVALID/XYZ")


# ---------------------------------------------------------------------------
# Unit tests – no network required
# ---------------------------------------------------------------------------


def _make_prices(n: int = 3) -> list[Price]:
    """Helper: build a small list of synthetic Price objects."""
    return [
        Price(
            open=100.0 + i,
            high=102.0 + i,
            low=99.0 + i,
            close=101.0 + i,
            volume=1_000_000,
            time=f"2024-01-0{i + 1}T00:00:00Z",
        )
        for i in range(n)
    ]


def test_crypto_prices_to_df_converts_correctly():
    """crypto_prices_to_df should return a properly structured, UTC-indexed DataFrame."""
    prices = _make_prices(3)
    df = crypto_prices_to_df(prices)

    # Should be a DataFrame
    assert isinstance(df, pd.DataFrame), f"Expected DataFrame, got {type(df)}"

    # Index should be UTC datetime
    assert hasattr(df.index, "tz"), "Index should be timezone-aware"
    assert str(df.index.tz) == "UTC", f"Index timezone should be UTC, got {df.index.tz}"

    # Required columns
    for col in ("open", "close", "high", "low", "volume"):
        assert col in df.columns, f"Missing column '{col}'"

    # Should be sorted ascending
    assert df.index.is_monotonic_increasing, "Index should be sorted ascending by time"

    # Values should be non-zero
    assert (df["close"] > 0).all(), "All close prices should be > 0"
