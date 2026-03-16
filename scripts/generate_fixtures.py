#!/usr/bin/env python3
"""
Generate test fixtures for integration tests.

Fetches real data from financialdatasets.ai API if FINANCIAL_DATASETS_API_KEY
is set; otherwise generates synthetic data with realistic price continuity.

Usage:
    poetry run python scripts/generate_fixtures.py
    poetry run python scripts/generate_fixtures.py --synthetic  # force synthetic
"""
from __future__ import annotations

import argparse
import json
import math
import os
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURES_ROOT = REPO_ROOT / "tests" / "fixtures" / "api"
PRICES_DIR = FIXTURES_ROOT / "prices"
FM_DIR = FIXTURES_ROOT / "financial_metrics"
NEWS_DIR = FIXTURES_ROOT / "news"
INSIDER_DIR = FIXTURES_ROOT / "insider_trades"

for d in (PRICES_DIR, FM_DIR, NEWS_DIR, INSIDER_DIR):
    d.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Synthetic price generator (geometric Brownian motion)
# ---------------------------------------------------------------------------

def _business_days(start: str, end: str):
    """Yield datetime objects for each business day in [start, end]."""
    s = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    e = datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    cur = s
    while cur <= e:
        if cur.weekday() < 5:  # Mon-Fri
            yield cur
        cur += timedelta(days=1)


def _calendar_days(start: str, end: str):
    """Yield datetime objects for each calendar day in [start, end]."""
    s = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    e = datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    cur = s
    while cur <= e:
        yield cur
        cur += timedelta(days=1)


def generate_synthetic_prices(
    ticker: str,
    start: str,
    end: str,
    seed_price: float = 100.0,
    mu: float = 0.0003,    # daily drift
    sigma: float = 0.015,  # daily volatility
    business_days_only: bool = True,
    seed: int = 42,
) -> list[dict]:
    """
    Generate synthetic OHLCV price data using geometric Brownian motion.
    Returns list of price dicts compatible with the fixture JSON format.
    """
    rng = random.Random(seed)
    prices = []
    price = seed_price

    day_iter = _business_days(start, end) if business_days_only else _calendar_days(start, end)

    for dt in day_iter:
        # GBM step
        ret = mu + sigma * _box_muller(rng)
        open_price = price
        close_price = round(open_price * math.exp(ret), 2)

        # Intraday range (±1-3% from open)
        range_pct = 0.01 + rng.random() * 0.02
        high_price = round(max(open_price, close_price) * (1 + range_pct), 2)
        low_price = round(min(open_price, close_price) * (1 - range_pct), 2)

        volume = int(10_000_000 + rng.random() * 90_000_000)

        prices.append({
            "ticker": ticker,
            "open": open_price,
            "close": close_price,
            "high": high_price,
            "low": low_price,
            "volume": volume,
            "time": dt.strftime("%Y-%m-%dT05:00:00Z"),
            "time_milliseconds": int(dt.timestamp() * 1000),
        })
        price = close_price

    return prices


def _box_muller(rng: random.Random) -> float:
    """Box-Muller transform for standard normal sample."""
    u1, u2 = rng.random(), rng.random()
    return math.sqrt(-2 * math.log(max(u1, 1e-15))) * math.cos(2 * math.pi * u2)


# ---------------------------------------------------------------------------
# Synthetic financial metrics generator
# ---------------------------------------------------------------------------

METRIC_TEMPLATE = {
    "report_period": None,
    "fiscal_period": None,
    "period": "ttm",
    "currency": "USD",
    "market_cap": None,
    "enterprise_value": None,
    "price_to_earnings_ratio": 25.0,
    "price_to_book_ratio": 10.0,
    "price_to_sales_ratio": 6.0,
    "enterprise_value_to_ebitda_ratio": 20.0,
    "enterprise_value_to_revenue_ratio": 7.0,
    "free_cash_flow_yield": 0.04,
    "peg_ratio": 2.5,
    "gross_margin": 0.45,
    "operating_margin": 0.30,
    "net_margin": 0.25,
    "return_on_equity": 1.5,
    "return_on_assets": 0.28,
    "return_on_invested_capital": 0.38,
    "asset_turnover": 1.1,
    "inventory_turnover": 60.0,
    "receivables_turnover": 7.0,
    "days_sales_outstanding": 0.14,
    "operating_cycle": 65.0,
    "working_capital_turnover": 12.0,
    "current_ratio": 1.0,
    "quick_ratio": 0.95,
    "cash_ratio": 0.25,
    "operating_cash_flow_ratio": 0.85,
    "debt_to_equity": 4.0,
    "debt_to_assets": 0.31,
    "interest_coverage": None,
    "revenue_growth": 0.01,
    "earnings_growth": 0.03,
    "book_value_growth": 0.05,
    "earnings_per_share_growth": 0.03,
    "free_cash_flow_growth": 0.05,
    "operating_income_growth": 0.03,
    "ebitda_growth": 0.03,
    "payout_ratio": 0.15,
    "earnings_per_share": 6.0,
    "book_value_per_share": 4.0,
    "free_cash_flow_per_share": 6.0,
}

QUARTERS = [
    ("2023-12-30", "2024-Q1"),
    ("2023-09-30", "2023-Q4"),
    ("2023-07-01", "2023-Q3"),
    ("2023-04-01", "2023-Q2"),
    ("2022-12-31", "2023-Q1"),
    ("2022-09-24", "2022-Q4"),
    ("2022-06-25", "2022-Q3"),
    ("2022-03-26", "2022-Q2"),
    ("2021-12-25", "2022-Q1"),
    ("2021-09-25", "2021-Q4"),
]


def generate_synthetic_financial_metrics(
    ticker: str,
    start: str,
    end: str,
    seed_market_cap: float = 2.0e12,
) -> list[dict]:
    metrics = []
    for rp, fp in QUARTERS:
        m = dict(METRIC_TEMPLATE)
        m["ticker"] = ticker
        m["report_period"] = rp
        m["fiscal_period"] = fp
        m["market_cap"] = seed_market_cap
        m["enterprise_value"] = seed_market_cap * 1.03
        metrics.append(m)
    return metrics


# ---------------------------------------------------------------------------
# Synthetic news generator
# ---------------------------------------------------------------------------

def generate_synthetic_news(
    ticker: str,
    start: str,
    end: str,
) -> list[dict]:
    items = []
    for i, dt in enumerate(_business_days(start, end)):
        if i % 3 == 0:  # every 3 days
            items.append({
                "id": f"{ticker}-news-{i}",
                "ticker": ticker,
                "title": f"{ticker} reports steady performance",
                "author": "Market Analyst",
                "source": "Financial Times",
                "date": dt.strftime("%Y-%m-%dT12:00:00Z"),
                "url": f"https://example.com/{ticker.lower()}-{i}",
                "sentiment": "neutral",
                "text": f"{ticker} continues to show stable fundamentals and market performance.",
            })
    return items


# ---------------------------------------------------------------------------
# Synthetic insider trades generator
# ---------------------------------------------------------------------------

def generate_synthetic_insider_trades(
    ticker: str,
    start: str,
    end: str,
) -> list[dict]:
    items = []
    days = list(_business_days(start, end))
    for i, dt in enumerate(days):
        if i % 15 == 0:  # every 15 business days
            items.append({
                "id": f"{ticker}-insider-{i}",
                "ticker": ticker,
                "issuer": f"{ticker} Inc.",
                "name": "John Smith",
                "title": "Chief Executive Officer",
                "is_board_director": False,
                "transaction_date": dt.strftime("%Y-%m-%d"),
                "transaction_shares": 1000,
                "transaction_price_per_share": 170.0,
                "transaction_acquired_disposed_code": "A",
                "shares_owned_before_transaction": 100000,
                "shares_owned_after_transaction": 101000,
                "security_title": "Common Stock",
                "filing_date": dt.strftime("%Y-%m-%d"),
            })
    return items


# ---------------------------------------------------------------------------
# API-based fetcher (when key is available)
# ---------------------------------------------------------------------------

def fetch_from_api(ticker: str, start: str, end: str, api_key: str) -> list[dict] | None:
    """Attempt to fetch price data from financialdatasets.ai."""
    try:
        import requests
        url = "https://api.financialdatasets.ai/prices/"
        params = {
            "ticker": ticker,
            "interval": "day",
            "interval_multiplier": 1,
            "start_date": start,
            "end_date": end,
        }
        headers = {"X-API-KEY": api_key}
        resp = requests.get(url, params=params, headers=headers, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("prices", [])
        else:
            print(f"  API returned {resp.status_code} for {ticker}", file=sys.stderr)
            return None
    except Exception as e:
        print(f"  API fetch error for {ticker}: {e}", file=sys.stderr)
        return None


# ---------------------------------------------------------------------------
# Fixture writers
# ---------------------------------------------------------------------------

# Ticker-specific seed prices for more realistic values
SEED_PRICES = {
    "AAPL": 180.0,
    "MSFT": 420.0,
    "GOOGL": 140.0,
    "TSLA": 200.0,
    "NVDA": 500.0,
    "SPY": 480.0,
}

SEED_MARKET_CAPS = {
    "AAPL": 2.8e12,
    "MSFT": 3.1e12,
    "GOOGL": 1.9e12,
    "TSLA": 6e11,
    "NVDA": 1.2e12,
    "SPY": None,
}


def write_price_fixture(ticker: str, start: str, end: str, use_api: bool, api_key: str | None):
    safe_ticker = ticker.replace("/", "-")
    outfile = PRICES_DIR / f"{safe_ticker}_{start}_{end}.json"
    if outfile.exists():
        print(f"  [SKIP] {outfile.name} already exists")
        return

    prices = None
    if use_api and api_key:
        print(f"  [API]  Fetching {ticker} prices {start}..{end}")
        raw = fetch_from_api(ticker, start, end, api_key)
        if raw:
            prices = raw

    if prices is None:
        print(f"  [SYNTH] Generating synthetic prices for {ticker} {start}..{end}")
        seed_price = SEED_PRICES.get(ticker, 100.0)
        prices = generate_synthetic_prices(ticker, start, end, seed_price=seed_price)

    payload = {"ticker": ticker, "prices": prices}
    outfile.write_text(json.dumps(payload, indent=2))
    print(f"  [DONE] {outfile.name} ({len(prices)} rows)")


def write_fm_fixture(ticker: str, start: str, end: str):
    safe_ticker = ticker.replace("/", "-")
    outfile = FM_DIR / f"{safe_ticker}_{start}_{end}.json"
    if outfile.exists():
        print(f"  [SKIP] {outfile.name} already exists")
        return
    seed_mc = SEED_MARKET_CAPS.get(ticker, 1e12)
    metrics = generate_synthetic_financial_metrics(ticker, start, end, seed_market_cap=seed_mc or 1e12)
    payload = {"financial_metrics": metrics}
    outfile.write_text(json.dumps(payload, indent=2))
    print(f"  [DONE] {outfile.name} ({len(metrics)} records)")


def write_news_fixture(ticker: str, start: str, end: str):
    safe_ticker = ticker.replace("/", "-")
    outfile = NEWS_DIR / f"{safe_ticker}_{start}_{end}.json"
    if outfile.exists():
        print(f"  [SKIP] {outfile.name} already exists")
        return
    news = generate_synthetic_news(ticker, start, end)
    payload = {"news": news}
    outfile.write_text(json.dumps(payload, indent=2))
    print(f"  [DONE] {outfile.name} ({len(news)} articles)")


def write_insider_fixture(ticker: str, start: str, end: str):
    safe_ticker = ticker.replace("/", "-")
    outfile = INSIDER_DIR / f"{safe_ticker}_{start}_{end}.json"
    if outfile.exists():
        print(f"  [SKIP] {outfile.name} already exists")
        return
    trades = generate_synthetic_insider_trades(ticker, start, end)
    payload = {"insider_trades": trades}
    outfile.write_text(json.dumps(payload, indent=2))
    print(f"  [DONE] {outfile.name} ({len(trades)} trades)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

# Fixtures needed for integration tests
# Each entry: (ticker, start_date, end_date)
PRICE_FIXTURES_NEEDED = [
    # For existing short-range integration tests (AAPL/MSFT/TSLA 2024-03-01..03-08)
    # These already exist in prices/ but need to be in fm/news/insider dirs too
    ("AAPL", "2024-03-01", "2024-03-08"),
    ("MSFT", "2024-03-01", "2024-03-08"),
    ("TSLA", "2024-03-01", "2024-03-08"),
    # Full year fixtures for integration tests that need more data
    ("AAPL", "2024-01-01", "2024-12-31"),
    ("MSFT", "2024-01-01", "2024-12-31"),
    ("TSLA", "2024-01-01", "2024-12-31"),
    ("GOOGL", "2024-01-01", "2024-12-31"),
    # SPY for benchmark
    ("SPY", "2024-03-01", "2024-03-08"),
]

NON_PRICE_FIXTURES_NEEDED = [
    # Existing tests use these tickers/dates
    ("AAPL", "2024-03-01", "2024-12-31"),
    ("MSFT", "2024-03-01", "2024-12-31"),
    ("TSLA", "2024-03-01", "2024-12-31"),
    ("GOOGL", "2024-01-01", "2024-12-31"),
]


def main():
    parser = argparse.ArgumentParser(description="Generate integration test fixtures")
    parser.add_argument("--synthetic", action="store_true", help="Force synthetic data (no API calls)")
    args = parser.parse_args()

    api_key = None if args.synthetic else os.environ.get("FINANCIAL_DATASETS_API_KEY", "")
    use_api = bool(api_key) and not args.synthetic

    print("=== Generating Price Fixtures ===")
    for ticker, start, end in PRICE_FIXTURES_NEEDED:
        write_price_fixture(ticker, start, end, use_api=use_api, api_key=api_key)

    print("\n=== Generating Financial Metrics Fixtures ===")
    for ticker, start, end in NON_PRICE_FIXTURES_NEEDED:
        write_fm_fixture(ticker, start, end)

    print("\n=== Generating News Fixtures ===")
    for ticker, start, end in NON_PRICE_FIXTURES_NEEDED:
        write_news_fixture(ticker, start, end)

    print("\n=== Generating Insider Trades Fixtures ===")
    for ticker, start, end in NON_PRICE_FIXTURES_NEEDED:
        write_insider_fixture(ticker, start, end)

    print("\nAll fixtures generated successfully.")


if __name__ == "__main__":
    main()
