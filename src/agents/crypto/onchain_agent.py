"""
On-Chain Metrics Agent for Bitcoin (and other major chains)

Data source priority:
  1. OnchainMetricStore (SQLite) — pre-fetched via backfill_onchain.py
  2. Glassnode API (if GLASSNODE_API_KEY env var is set)
  3. CoinGecko public API (free, no key required)

When no data is available for a metric, the agent explicitly sets
data_available=False so downstream consumers (portfolio manager) can
distinguish "neutral because of data absence" from "neutral because of signal".
"""

import json
import os
from datetime import datetime, timezone

from langchain_core.messages import HumanMessage

from src.data.onchain_store import OnchainMetricStore
from src.graph.state import AgentState, show_agent_reasoning
from src.utils.progress import progress


def onchain_agent(state: AgentState, agent_id: str = "onchain_agent") -> dict:
    """
    On-chain metrics analyst.

    Produces bullish/bearish/neutral signals for BTC-like assets based on
    MVRV (or proxy), NVT (or proxy), and market cap trends.

    When on-chain data is genuinely unavailable the output contains
    ``data_available: false`` so callers can treat the signal as absent
    rather than as a real neutral reading.
    """
    data = state["data"]
    tickers = data["tickers"]
    start_date = data["start_date"]
    end_date = data["end_date"]

    start_ts_ms = int(datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc).timestamp() * 1000)
    end_ts_ms = int(datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc).timestamp() * 1000)

    store = OnchainMetricStore()

    onchain_analysis: dict = {}

    for symbol in tickers:
        asset = symbol.split("/")[0]
        if asset not in ("BTC", "ETH", "SOL"):
            # On-chain metrics only meaningful for major chains
            continue

        progress.update_status(agent_id, symbol, "Fetching on-chain metrics")

        # Trigger a live fetch if we have no local data
        if not store.has_data(asset, start_ts_ms, end_ts_ms):
            progress.update_status(agent_id, symbol, "Backfilling on-chain data")
            try:
                n = store.backfill(asset, start_ts_ms, end_ts_ms)
                if n == 0:
                    progress.update_status(agent_id, symbol, "No on-chain data available")
                    onchain_analysis[symbol] = _unavailable_signal(asset)
                    continue
            except Exception as exc:
                progress.update_status(agent_id, symbol, f"Backfill failed: {exc}")
                onchain_analysis[symbol] = _unavailable_signal(asset, str(exc))
                continue

        # Determine which MVRV metric to use
        glassnode_available = bool(os.environ.get("GLASSNODE_API_KEY", "").strip())
        mvrv_metric = "glassnode_mvrv" if glassnode_available else None
        nvt_metric = "glassnode_nvt" if glassnode_available else "nvt_approx"

        mvrv_value = None
        if mvrv_metric:
            mvrv_value = store.get_latest_value(asset, mvrv_metric, end_ts_ms)

        nvt_value = store.get_latest_value(asset, nvt_metric, end_ts_ms)

        mvrv_signal = _analyse_mvrv(mvrv_value, glassnode_available)
        nvt_signal = _analyse_nvt(nvt_value, glassnode_available)

        signals = [mvrv_signal, nvt_signal]
        combined = _combine_onchain(signals)

        onchain_analysis[symbol] = {
            "signal": combined["signal"],
            "confidence": combined["confidence"],
            "data_available": combined.get("data_available", True),
            "reasoning": {
                "mvrv": mvrv_signal,
                "nvt": nvt_signal,
                "data_source": "glassnode" if glassnode_available else "coingecko_proxy",
                "note": combined.get("note", ""),
            },
        }
        progress.update_status(agent_id, symbol, "Done", analysis=json.dumps(onchain_analysis, indent=2))

    message = HumanMessage(content=json.dumps(onchain_analysis), name=agent_id)

    if state["metadata"]["show_reasoning"]:
        show_agent_reasoning(onchain_analysis, "On-Chain Agent")

    state["data"]["analyst_signals"][agent_id] = onchain_analysis
    progress.update_status(agent_id, None, "Done")

    return {"messages": state["messages"] + [message], "data": data}


# ------------------------------------------------------------------
# Signal helpers
# ------------------------------------------------------------------

def _unavailable_signal(asset: str, reason: str = "No data source available") -> dict:
    return {
        "signal": "neutral",
        "confidence": 0,
        "data_available": False,
        "reasoning": {
            "note": reason,
            "mvrv": {"signal": "neutral", "confidence": 0, "value": None},
            "nvt": {"signal": "neutral", "confidence": 0, "value": None},
        },
    }


def _analyse_mvrv(value: float | None, is_glassnode: bool) -> dict:
    """
    MVRV interpretation:
      > 3.5  → historically overbought (bearish)
      1.0–2.0 → fair value zone (neutral)
      < 1.0  → historically undervalued (bullish)

    When using a CoinGecko proxy the metric is not available,
    so we return a low-confidence neutral.
    """
    if value is None or not is_glassnode:
        return {
            "signal": "neutral",
            "confidence": 0,
            "value": value,
            "note": "MVRV requires Glassnode key" if not is_glassnode else "No MVRV data",
        }

    if value > 3.5:
        signal, conf = "bearish", min(int((value - 3.5) / 3.5 * 100), 90)
    elif value < 1.0:
        signal, conf = "bullish", min(int((1.0 - value) / 1.0 * 100), 90)
    else:
        signal, conf = "neutral", 30
    return {"signal": signal, "confidence": conf, "value": round(value, 3)}


def _analyse_nvt(value: float | None, is_glassnode: bool) -> dict:
    """
    NVT interpretation:
      > 90   → network overvalued vs activity (bearish)
      < 30   → undervalued (bullish)

    The CoinGecko proxy (market_cap / 90d_avg_volume) has different
    absolute ranges, so confidence is halved for proxy readings.
    """
    if value is None:
        return {"signal": "neutral", "confidence": 0, "value": None, "note": "No NVT data"}

    if is_glassnode:
        if value > 90:
            signal, conf = "bearish", min(int((value - 90) / 90 * 100), 80)
        elif value < 30:
            signal, conf = "bullish", min(int((30 - value) / 30 * 100), 80)
        else:
            signal, conf = "neutral", 30
    else:
        # Proxy: typical range 20–200, halve confidence to reflect uncertainty
        if value > 150:
            signal, conf = "bearish", min(int((value - 150) / 150 * 50), 40)
        elif value < 40:
            signal, conf = "bullish", min(int((40 - value) / 40 * 50), 40)
        else:
            signal, conf = "neutral", 15

    note = "" if is_glassnode else "NVT proxy (market_cap/90d_vol) — lower confidence"
    return {"signal": signal, "confidence": conf, "value": round(value, 2), "note": note}


def _combine_onchain(signals: list[dict]) -> dict:
    """Simple majority vote with confidence averaging."""
    valid = [s for s in signals if s.get("confidence", 0) > 0]
    if not valid:
        return {
            "signal": "neutral",
            "confidence": 0,
            "data_available": False,
            "note": "No valid on-chain signals — treat as absent, not neutral",
        }

    score_map = {"bullish": 1, "neutral": 0, "bearish": -1}
    avg_score = sum(score_map[s["signal"]] for s in valid) / len(valid)
    avg_conf = int(sum(s["confidence"] for s in valid) / len(valid))

    if avg_score > 0.3:
        signal = "bullish"
    elif avg_score < -0.3:
        signal = "bearish"
    else:
        signal = "neutral"

    return {"signal": signal, "confidence": avg_conf, "data_available": True}
