"""
Fetch Polymarket geopolitical market list and hourly price history.

Data sources:
  - Market list:  Gamma API (https://gamma-api.polymarket.com)
  - Price history: CLOB API (https://clob.polymarket.com/prices-history)
    — requires a token_id (from clobTokenIds on the Gamma market record)
    — max window per request: 15 days  →  we chunk and stitch

Outputs:
  data/polymarket/markets_jan2025.json       — market metadata
  data/polymarket/price_history/<id>.json    — hourly YES probability per market

Usage:
  poetry run python scripts/polymarket/fetch_markets.py
"""

import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests

# ── constants ────────────────────────────────────────────────────────────────
GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"

DATA_DIR = Path(__file__).parent.parent.parent / "data" / "polymarket"
PRICE_DIR = DATA_DIR / "price_history"

# Analysis window: Dec 2024 → Feb 2025
START_DT = datetime(2024, 12, 1)
END_DT = datetime(2025, 2, 1)
START_TS = int(START_DT.timestamp())
END_TS = int(END_DT.timestamp())

# Jan 2025 closing markets
MARKET_START_DATE = "2025-01-01"
MARKET_END_DATE = "2025-01-31"

# Geopolitical / political keywords
KEYWORDS = [
    "Iran", "Israel", "Gaza", "Syria", "Lebanon", "Hamas", "Hezbollah",
    "ceasefire", "hostage", "war", "attack", "strike", "conflict",
    "Trump", "Russia", "Ukraine", "China", "Taiwan",
    "nuclear", "missile", "military", "assassination",
]

MIN_VOLUME_USD = 50_000   # minimum market volume to keep

# CLOB prices-history max window = 15 days
WINDOW_DAYS = 15


# ── helpers ──────────────────────────────────────────────────────────────────
def get_json(url: str, params: dict, retries: int = 3, backoff: float = 2.0) -> Any:
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            if attempt < retries - 1:
                time.sleep(backoff)
            else:
                print(f"  [error] {exc}")
                return None


def deduplicate(markets: list[dict]) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for m in markets:
        mid = m.get("conditionId") or m.get("id", "")
        if mid and mid not in seen:
            seen.add(mid)
            out.append(m)
    return out


# ── step 1: fetch market list ─────────────────────────────────────────────────
def fetch_markets() -> list[dict]:
    """Fetch all markets closing in Jan 2025, then filter by keyword."""
    print("=== Step 1: Fetching Polymarket Jan 2025 markets via Gamma API ===")

    all_markets: list[dict] = []
    offset = 0
    while True:
        data = get_json(
            f"{GAMMA_BASE}/markets",
            params={
                "limit": 100,
                "closed": True,
                "start_date_min": MARKET_START_DATE,
                "end_date_max": MARKET_END_DATE,
                "offset": offset,
            },
        )
        if not isinstance(data, list) or not data:
            break
        all_markets.extend(data)
        print(f"  fetched offset={offset}  total so far={len(all_markets)}")
        if len(data) < 100:
            break
        offset += 100
        if offset > 2000:
            break
        time.sleep(0.3)

    all_markets = deduplicate(all_markets)
    print(f"  → {len(all_markets)} unique markets closing in Jan 2025")

    # filter geopolitical
    def is_geo(m: dict) -> bool:
        text = (m.get("question", "") + " " + m.get("description", "")).lower()
        return any(kw.lower() in text for kw in KEYWORDS)

    geo = [m for m in all_markets if is_geo(m)]
    print(f"  → {len(geo)} geopolitical/political markets")

    # filter by volume
    def vol(m: dict) -> float:
        return float(m.get("volumeNum", m.get("volume", 0)) or 0)

    liquid = [m for m in geo if vol(m) >= MIN_VOLUME_USD]
    print(f"  → {len(liquid)} with volume >= ${MIN_VOLUME_USD:,}")

    # sort by volume descending
    liquid.sort(key=vol, reverse=True)
    return liquid


# ── step 2: fetch price history per market ───────────────────────────────────
def fetch_price_history_clob(token_id: str) -> list[dict]:
    """
    Fetch hourly YES price from CLOB API for a single token.
    Chunks into 15-day windows to stay under the API limit.
    Returns list of {t: unix_seconds, p: float}.
    """
    all_ticks: list[dict] = []
    chunk_start = START_DT

    while chunk_start < END_DT:
        chunk_end = min(chunk_start + timedelta(days=WINDOW_DAYS), END_DT)
        data = get_json(
            f"{CLOB_BASE}/prices-history",
            params={
                "market": token_id,
                "startTs": int(chunk_start.timestamp()),
                "endTs": int(chunk_end.timestamp()),
                "fidelity": 60,
            },
        )
        if isinstance(data, dict) and "history" in data:
            all_ticks.extend(data["history"])
        elif isinstance(data, list):
            all_ticks.extend(data)

        chunk_start = chunk_end
        time.sleep(0.25)

    # deduplicate by timestamp
    seen: set[int] = set()
    deduped: list[dict] = []
    for tick in all_ticks:
        t = tick.get("t", tick.get("timestamp"))
        if t and t not in seen:
            seen.add(int(t))
            deduped.append({"t": int(t), "p": float(tick.get("p", tick.get("price", 0)))})

    deduped.sort(key=lambda x: x["t"])
    return deduped


def extract_token_id(market: dict) -> str | None:
    """
    Extract the YES-outcome token ID from a Gamma market record.
    clobTokenIds is a JSON string like '["token1", "token2"]' where token1 = YES.
    """
    raw = market.get("clobTokenIds")
    if raw:
        try:
            ids = json.loads(raw) if isinstance(raw, str) else raw
            if ids:
                return str(ids[0])  # first = YES token
        except (json.JSONDecodeError, TypeError, IndexError):
            pass

    # fallback: tokens array
    for t in market.get("tokens", []):
        outcome = (t.get("outcome") or "").upper()
        if "YES" in outcome or outcome == "1":
            return str(t.get("token_id", ""))

    # last resort: take first token regardless
    tokens = market.get("tokens", [])
    if tokens:
        return str(tokens[0].get("token_id", ""))

    return None


def fetch_all_price_histories(markets: list[dict]) -> None:
    print("\n=== Step 2: Fetching hourly price history per market ===")
    PRICE_DIR.mkdir(parents=True, exist_ok=True)
    success = 0

    for i, m in enumerate(markets):
        condition_id = m.get("conditionId") or m.get("id", "unknown")
        question = m.get("question", "")[:80]
        vol = float(m.get("volumeNum", m.get("volume", 0)) or 0)

        out_path = PRICE_DIR / f"{condition_id}.json"
        if out_path.exists():
            print(f"  [{i+1}/{len(markets)}] cached  {question[:55]}")
            success += 1
            continue

        token_id = extract_token_id(m)
        print(f"  [{i+1}/{len(markets)}] ${vol:>9,.0f}  {question[:55]}")
        if not token_id:
            print("    no token_id found — skipping")
            continue
        print(f"    token={token_id[:20]}…")

        ticks = fetch_price_history_clob(token_id)
        if ticks:
            out_path.write_text(json.dumps(ticks, indent=2))
            print(f"    {len(ticks)} hourly ticks saved → {out_path.name}")
            success += 1
        else:
            print("    no price history returned")

    print(f"\n  Price histories: {success}/{len(markets)} markets")


# ── main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    markets = fetch_markets()

    # save metadata
    out_path = DATA_DIR / "markets_jan2025.json"
    out_path.write_text(json.dumps(markets, indent=2))
    print(f"\nSaved {len(markets)} markets → {out_path}")

    # show top by volume
    def vol(m: dict) -> float:
        return float(m.get("volumeNum", m.get("volume", 0)) or 0)

    print("\nTop 10 by volume:")
    for m in markets[:10]:
        end = m.get("endDateIso", m.get("endDate", ""))[:10]
        print(f"  ${vol(m):>12,.0f}  [{end}]  {m.get('question', '')[:70]}")

    if markets:
        fetch_all_price_histories(markets)
        print("\nDone. Price histories saved to data/polymarket/price_history/")
    else:
        print("\n[!] No markets found.")


if __name__ == "__main__":
    main()
