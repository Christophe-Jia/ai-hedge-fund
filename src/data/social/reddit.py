"""
Reddit data collector using the PRAW library.

Fetches recent posts and comments from finance/crypto subreddits
and returns them as plain dicts for downstream sentiment analysis.

Required env vars:
  REDDIT_CLIENT_ID
  REDDIT_CLIENT_SECRET
  REDDIT_USER_AGENT   (e.g. "ai-hedge-fund/1.0 by YourUsername")
"""

import os
from datetime import datetime, timezone
from typing import Optional


def _get_reddit_client():
    """Build and return an authenticated PRAW Reddit instance."""
    client_id = os.environ.get("REDDIT_CLIENT_ID")
    client_secret = os.environ.get("REDDIT_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise ValueError(
            "Reddit credentials not configured. Set REDDIT_CLIENT_ID and "
            "REDDIT_CLIENT_SECRET environment variables."
        )

    try:
        import praw
    except ImportError:
        raise ImportError("praw is required. Run: poetry add praw")

    return praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=os.environ.get("REDDIT_USER_AGENT", "ai-hedge-fund/1.0"),
        # Read-only mode – no username/password needed
    )


def get_reddit_posts(
    subreddits: list[str],
    query: str = "",
    limit: int = 100,
    sort: str = "new",
) -> list[dict]:
    """
    Fetch recent posts from one or more subreddits.

    Args:
        subreddits: List of subreddit names, e.g. ["Bitcoin", "CryptoCurrency"]
        query:      Optional search query (uses .search() when provided)
        limit:      Max posts to return per subreddit
        sort:       Reddit sort order: "new" | "hot" | "top" | "rising"

    Returns:
        List of dicts with keys: id, subreddit, title, selftext, score,
        num_comments, upvote_ratio, url, created_utc.
    """
    try:
        reddit = _get_reddit_client()
    except ValueError as e:
        print(f"[reddit] {e}")
        return []

    posts: list[dict] = []

    for sub_name in subreddits:
        subreddit = reddit.subreddit(sub_name)
        try:
            if query:
                listing = subreddit.search(query, sort=sort, limit=limit)
            else:
                listing = getattr(subreddit, sort)(limit=limit)

            for submission in listing:
                posts.append(
                    {
                        "id": submission.id,
                        "subreddit": sub_name,
                        "title": submission.title,
                        "selftext": submission.selftext[:2000],  # Truncate long posts
                        "score": submission.score,
                        "num_comments": submission.num_comments,
                        "upvote_ratio": submission.upvote_ratio,
                        "url": submission.url,
                        "created_utc": datetime.fromtimestamp(
                            submission.created_utc, tz=timezone.utc
                        ).isoformat(),
                    }
                )
        except Exception as e:
            # Log but do not crash if a single subreddit fails
            print(f"[reddit] Error fetching r/{sub_name}: {e}")

    return posts


# Default subreddits to watch for BTC / crypto signals
BTC_SUBREDDITS = ["Bitcoin", "CryptoCurrency", "BitcoinMarkets", "CryptoMarkets"]

# Default subreddits for US stock signals
STOCKS_SUBREDDITS = ["wallstreetbets", "investing", "stocks", "StockMarket"]
