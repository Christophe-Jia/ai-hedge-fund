"""
Polymarket Signal Agent

Reads geopolitical prediction market prices from PolymarketTickStore and
derives a macro risk signal for crypto (primarily BTC).

Signal logic:
  - Scans tracked markets for keywords indicating geopolitical risk
    (war, conflict, default, ban, sanctions, etc.)
  - For each matching market, high probability (>0.70) → bearish macro risk
  - Tracks 24h probability change velocity (fast rise → worsening risk)
  - Combines into a single bearish/neutral signal with confidence

When no Polymarket data is available (empty DB or no recent ticks) the agent
outputs {"signal": "neutral", "confidence": 0, "data_available": false}.

Note: Polymarket YES token prices represent P(event = YES). A "YES" token for
a geopolitical risk event at 0.80 means the market prices 80% probability the
event occurs — a bearish signal for risk assets.
"""

import json
import time
from datetime import datetime, timezone

from langchain_core.messages import HumanMessage

from src.data.polymarket_tick_store import PolymarketTickStore
from src.graph.state import AgentState, show_agent_reasoning
from src.utils.progress import progress

# Look-back windows (seconds)
_RECENT_WINDOW_S = 3_600      # 1h — "current" probability
_CHANGE_WINDOW_S = 86_400     # 24h — velocity window

# Keywords that flag a market as geopolitical / macro-relevant risk
_RISK_KEYWORDS = [
    "war", "conflict", "attack", "invasion", "nuke", "nuclear",
    "sanctions", "ban", "restrict", "default", "recession",
    "fed rate", "rate hike", "tariff", "crisis", "crash",
    "btc ban", "crypto ban", "sec", "regulation",
]

# Probability thresholds
_HIGH_RISK_THRESHOLD = 0.70
_EXTREME_RISK_THRESHOLD = 0.85
_LOW_RISK_THRESHOLD = 0.15

# Minimum velocity (probability change per day) to trigger a signal boost
_VELOCITY_BOOST_THRESHOLD = 0.10


def polymarket_signal_agent(state: AgentState, agent_id: str = "polymarket_signal_agent") -> dict:
    """
    Polymarket geopolitical risk signal agent.

    Scans all tracked prediction markets for risk keywords, reads their
    current probability, and emits a bearish/neutral signal.

    In backtesting mode (no live Polymarket data), returns neutral +
    data_available=false.
    """
    data = state["data"]
    tickers = data["tickers"]
    end_date = data["end_date"]

    try:
        end_ts_s = int(datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc).timestamp())
    except Exception:
        end_ts_s = int(time.time())

    recent_start_s = end_ts_s - _RECENT_WINDOW_S
    change_start_s = end_ts_s - _CHANGE_WINDOW_S

    store = PolymarketTickStore()
    markets = store.list_markets()

    pm_analysis: dict = {}

    for symbol in tickers:
        progress.update_status(agent_id, symbol, "Scanning Polymarket risk markets")

        if not markets:
            pm_analysis[symbol] = _unavailable(symbol, "No Polymarket markets tracked yet")
            continue

        # Filter for risk-relevant markets with recent data
        risk_markets = _filter_risk_markets(markets, store, end_ts_s)

        if not risk_markets:
            pm_analysis[symbol] = _unavailable(
                symbol, "No recent Polymarket data — collector may be paused"
            )
            continue

        # Compute signal from risk markets
        market_signals = []
        for m in risk_markets:
            sig = _analyse_market(m, store, recent_start_s, change_start_s, end_ts_s)
            if sig:
                market_signals.append(sig)

        if not market_signals:
            pm_analysis[symbol] = {
                "signal": "neutral",
                "confidence": 0,
                "data_available": True,
                "reasoning": {
                    "note": "Risk markets found but all probabilities in neutral range",
                    "top_markets": [],
                },
            }
            continue

        combined = _combine_pm_signals(market_signals)

        pm_analysis[symbol] = {
            "signal": combined["signal"],
            "confidence": combined["confidence"],
            "data_available": True,
            "reasoning": {
                "risk_level": combined.get("risk_level", "moderate"),
                "top_markets": sorted(
                    market_signals, key=lambda x: abs(x.get("probability", 0.5) - 0.5), reverse=True
                )[:5],
                "markets_scanned": len(risk_markets),
            },
        }
        progress.update_status(agent_id, symbol, "Done", analysis=json.dumps(pm_analysis, indent=2))

    message = HumanMessage(content=json.dumps(pm_analysis), name=agent_id)

    if state["metadata"]["show_reasoning"]:
        show_agent_reasoning(pm_analysis, "Polymarket Signal Agent")

    state["data"]["analyst_signals"][agent_id] = pm_analysis
    progress.update_status(agent_id, None, "Done")

    return {"messages": state["messages"] + [message], "data": data}


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------

def _unavailable(symbol: str, reason: str) -> dict:
    return {
        "signal": "neutral",
        "confidence": 0,
        "data_available": False,
        "reasoning": {"note": reason, "top_markets": []},
    }


def _is_risk_market(question: str) -> bool:
    """Return True if the market question contains any risk keyword."""
    if not question:
        return False
    q = question.lower()
    return any(kw in q for kw in _RISK_KEYWORDS)


def _filter_risk_markets(
    markets: list[dict],
    store: PolymarketTickStore,
    before_ts_s: int,
) -> list[dict]:
    """Return markets that (a) match risk keywords and (b) have recent ticks."""
    result = []
    for m in markets:
        if not _is_risk_market(m.get("question", "")):
            continue
        latest = store.get_latest_ts(m["token_id"])
        if latest is None:
            continue
        # Require data within 48h of the query time
        if latest < before_ts_s - 2 * 86_400:
            continue
        result.append(m)
    return result


def _analyse_market(
    market: dict,
    store: PolymarketTickStore,
    recent_start_s: int,
    change_start_s: int,
    end_ts_s: int,
) -> dict | None:
    """Compute probability and velocity for a single market."""
    token_id = market["token_id"]

    # Current probability: average of recent ticks
    recent_ticks = store.get_ticks(token_id, start_ts=recent_start_s, end_ts=end_ts_s)
    if not recent_ticks:
        return None

    current_prob = sum(p for _, p in recent_ticks) / len(recent_ticks)

    # Velocity: probability change vs 24h ago
    old_ticks = store.get_ticks(token_id, start_ts=change_start_s, end_ts=recent_start_s)
    old_prob = sum(p for _, p in old_ticks) / len(old_ticks) if old_ticks else current_prob
    velocity_24h = current_prob - old_prob

    return {
        "token_id": token_id,
        "question": market.get("question", ""),
        "probability": round(current_prob, 4),
        "velocity_24h": round(velocity_24h, 4),
    }


def _combine_pm_signals(market_signals: list[dict]) -> dict:
    """
    Aggregate individual market signals into a portfolio-level signal.

    High-probability risk events → bearish; rapidly rising probability → boost confidence.
    """
    bearish_score = 0.0
    max_conf = 0

    for m in market_signals:
        prob = m.get("probability", 0.5)
        vel = m.get("velocity_24h", 0.0)

        # Base score from current probability
        if prob >= _EXTREME_RISK_THRESHOLD:
            score = 1.0
            conf = 80
        elif prob >= _HIGH_RISK_THRESHOLD:
            score = 0.7
            conf = 55
        elif prob <= _LOW_RISK_THRESHOLD:
            # Very unlikely risk event → slightly bullish (risk-off resolved)
            score = -0.3
            conf = 25
        else:
            score = 0.0
            conf = 0

        # Velocity boost
        if vel >= _VELOCITY_BOOST_THRESHOLD and prob > 0.5:
            conf = min(conf + 15, 90)
            score = min(score + 0.2, 1.0)

        bearish_score += score
        max_conf = max(max_conf, conf)

    if bearish_score <= 0:
        return {"signal": "neutral", "confidence": 0, "risk_level": "low"}

    # Normalise
    avg_score = bearish_score / len(market_signals)

    if avg_score >= 0.60:
        signal = "bearish"
        risk_level = "high"
    elif avg_score >= 0.25:
        signal = "bearish"
        risk_level = "moderate"
    else:
        signal = "neutral"
        risk_level = "low"

    return {"signal": signal, "confidence": max_conf, "risk_level": risk_level}
