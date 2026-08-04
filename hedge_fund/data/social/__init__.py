"""Social media data collectors for sentiment analysis."""

from src.data.social.reddit import get_reddit_posts
from src.data.social.stocktwits import get_stocktwits_messages
from src.data.social.telegram import get_telegram_messages

__all__ = [
    "get_reddit_posts",
    "get_stocktwits_messages",
    "get_telegram_messages",
]
