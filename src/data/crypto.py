"""
Crypto market data fetcher using CCXT.

Provides OHLCV price data, order book snapshots, and ticker info
for BTC/USD and other crypto pairs via any CCXT-supported exchange.
"""

import os
import time
from datetime import datetime, timezone

import pandas as pd

from src.data.models import Price


def get_crypto_prices(
    symbol: str,
    start_date: str,
    end_date: str,
    exchange_id: str = None,
    timeframe: str = "1d",
) -> list[Price]:
    """
    Fetch OHLCV candle data for a crypto symbol via CCXT.

    Args:
        symbol:      CCXT market symbol, e.g. "BTC/USDT"
        start_date:  ISO date string "YYYY-MM-DD"
        end_date:    ISO date string "YYYY-MM-DD"
        exchange_id: CCXT exchange id (default: CCXT_EXCHANGE env var or "binance")
        timeframe:   CCXT timeframe string (default "1d")

    Returns:
        List of Price objects sorted ascending by time.
    """
    try:
        import ccxt
    except ImportError:
        raise ImportError("ccxt is required. Run: poetry add ccxt")

    exchange_id = exchange_id or os.environ.get("CCXT_EXCHANGE", "binance")
    exchange_cls = getattr(ccxt, exchange_id)
    exchange: ccxt.Exchange = exchange_cls(
        {
            "apiKey": os.environ.get("CCXT_API_KEY", ""),
            "secret": os.environ.get("CCXT_API_SECRET", ""),
            "enableRateLimit": True,
        }
    )

    # Convert date strings to millisecond timestamps
    start_ts = int(datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc).timestamp() * 1000)
    end_ts = int(datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc).timestamp() * 1000)

    all_ohlcv: list[list] = []
    since = start_ts
    limit = 500  # Most exchanges cap at 500-1000 candles per request

    while since < end_ts:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=limit)
        if not ohlcv:
            break
        all_ohlcv.extend(ohlcv)
        since = ohlcv[-1][0] + 1  # advance past the last candle
        if len(ohlcv) < limit:
            break
        time.sleep(exchange.rateLimit / 1000)

    prices: list[Price] = []
    for candle in all_ohlcv:
        ts_ms, open_, high, low, close, volume = candle
        if ts_ms > end_ts:
            break
        dt_str = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        prices.append(
            Price(
                open=float(open_),
                high=float(high),
                low=float(low),
                close=float(close),
                volume=int(volume),
                time=dt_str,
            )
        )

    return prices


def crypto_prices_to_df(prices: list[Price]) -> pd.DataFrame:
    """Convert a list of Price objects to a DataFrame indexed by datetime."""
    records = [p.model_dump() for p in prices]
    df = pd.DataFrame(records)
    df["time"] = pd.to_datetime(df["time"], utc=True)
    df = df.set_index("time").sort_index()
    return df


def get_crypto_ticker(symbol: str, exchange_id: str = None) -> dict:
    """
    Fetch the latest ticker (bid/ask/last price) for a symbol.

    Returns a dict with keys: symbol, last, bid, ask, volume, timestamp.
    """
    try:
        import ccxt
    except ImportError:
        raise ImportError("ccxt is required. Run: poetry add ccxt")

    exchange_id = exchange_id or os.environ.get("CCXT_EXCHANGE", "binance")
    exchange_cls = getattr(ccxt, exchange_id)
    exchange: ccxt.Exchange = exchange_cls(
        {
            "apiKey": os.environ.get("CCXT_API_KEY", ""),
            "secret": os.environ.get("CCXT_API_SECRET", ""),
            "enableRateLimit": True,
        }
    )
    ticker = exchange.fetch_ticker(symbol)
    return {
        "symbol": ticker["symbol"],
        "last": ticker.get("last"),
        "bid": ticker.get("bid"),
        "ask": ticker.get("ask"),
        "volume": ticker.get("baseVolume"),
        "timestamp": ticker.get("datetime"),
    }
