"""
StockTwits data collector.

Uses the public StockTwits REST API (no auth required for read endpoints,
optional token for higher rate limits).

Env vars (optional):
  STOCKTWITS_ACCESS_TOKEN
"""

import os
import time

import requests


_BASE_URL = "https://api.stocktwits.com/api/2"


def get_stocktwits_messages(
    symbol: str,
    limit: int = 30,
    filter_type: str = "all",
) -> list[dict]:
    """
    Fetch recent StockTwits messages for a ticker symbol.

    Args:
        symbol:      StockTwits symbol, e.g. "BTC.X" for Bitcoin, "AAPL" for Apple
        limit:       Number of messages to return (max 30 per API call)
        filter_type: "all" | "top" | "trending"

    Returns:
        List of dicts with keys: id, body, sentiment, created_at, user_followers.
    """
    params: dict = {"limit": min(limit, 30)}
    token = os.environ.get("STOCKTWITS_ACCESS_TOKEN")
    if token:
        params["access_token"] = token

    url = f"{_BASE_URL}/streams/symbol/{symbol}.json"
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        print(f"[stocktwits] Error fetching {symbol}: {e}")
        return []

    messages: list[dict] = []
    for msg in data.get("messages", []):
        sentiment = None
        if msg.get("entities", {}).get("sentiment"):
            sentiment = msg["entities"]["sentiment"].get("basic")  # "Bullish" | "Bearish"
        messages.append(
            {
                "id": msg["id"],
                "body": msg["body"],
                "sentiment": sentiment,
                "created_at": msg["created_at"],
                "user_followers": msg.get("user", {}).get("followers", 0),
            }
        )

    return messages


# Common StockTwits symbols
STOCKTWITS_SYMBOLS = {
    "BTC": "BTC.X",
    "ETH": "ETH.X",
    "AAPL": "AAPL",
    "NVDA": "NVDA",
    "TSLA": "TSLA",
}
