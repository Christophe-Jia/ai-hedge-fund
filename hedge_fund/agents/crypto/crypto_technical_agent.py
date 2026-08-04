"""
Crypto Technical Analyst Agent

Applies the same multi-signal technical analysis framework used for stocks
(trend, mean-reversion, momentum, volatility, stat-arb) to crypto OHLCV data.

Data source: HistoricalOHLCVStore (SQLite).  When the local DB has insufficient
coverage the store falls back to CCXT automatically, so the agent never calls
CCXT directly.

Tickers in the agent state are expected to be CCXT symbols, e.g. "BTC/USDT".
"""

import json
from datetime import datetime, timezone

import pandas as pd
from langchain_core.messages import HumanMessage

from src.agents.technicals import (
    calculate_trend_signals,
    calculate_mean_reversion_signals,
    calculate_momentum_signals,
    calculate_volatility_signals,
    calculate_stat_arb_signals,
    weighted_signal_combination,
    normalize_pandas,
)
from src.data.historical_store import HistoricalOHLCVStore
from src.graph.state import AgentState, show_agent_reasoning
from src.utils.progress import progress


def crypto_technical_agent(state: AgentState, agent_id: str = "crypto_technical_agent") -> dict:
    """
    Technical analysis agent for crypto markets.

    Reads tickers from state["data"]["tickers"] (CCXT symbol format, e.g. "BTC/USDT")
    and produces the same signal/confidence/reasoning structure as the stock
    technical_analyst_agent so downstream portfolio/risk agents can consume it
    transparently.

    Uses HistoricalOHLCVStore for data access (SQLite with CCXT fallback).
    """
    data = state["data"]
    start_date = data["start_date"]
    end_date = data["end_date"]
    tickers = data["tickers"]
    exchange_id = data.get("exchange_id", "binance")

    start_ts_ms = int(datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc).timestamp() * 1000)
    end_ts_ms = int(datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc).timestamp() * 1000)

    store = HistoricalOHLCVStore(exchange_id=exchange_id)

    technical_analysis: dict = {}

    strategy_weights = {
        "trend": 0.30,
        "mean_reversion": 0.15,
        "momentum": 0.30,
        "volatility": 0.15,
        "stat_arb": 0.10,
    }

    for symbol in tickers:
        progress.update_status(agent_id, symbol, "Fetching OHLCV data")

        # Determine spot symbol for the store (strip ":USDT" perp suffix if present)
        spot_symbol = symbol.split(":")[0]

        df = store.get_ohlcv(
            symbol=spot_symbol,
            market_type="spot",
            timeframe="1d",
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
        )

        if df.empty:
            progress.update_status(agent_id, symbol, "Failed: No price data")
            continue

        # Rename ts → date and ensure standard column order expected by technicals
        df = df.rename(columns={"ts": "date"})
        df["date"] = pd.to_datetime(df["date"], unit="ms", utc=True)

        if len(df) < 63:
            progress.update_status(agent_id, symbol, "Failed: Insufficient data (need ≥63 candles)")
            continue

        progress.update_status(agent_id, symbol, "Calculating trend signals")
        trend = calculate_trend_signals(df)

        progress.update_status(agent_id, symbol, "Calculating mean reversion")
        mean_rev = calculate_mean_reversion_signals(df)

        progress.update_status(agent_id, symbol, "Calculating momentum")
        momentum = calculate_momentum_signals(df)

        progress.update_status(agent_id, symbol, "Analysing volatility")
        volatility = calculate_volatility_signals(df)

        progress.update_status(agent_id, symbol, "Statistical analysis")
        stat_arb = calculate_stat_arb_signals(df)

        progress.update_status(agent_id, symbol, "Combining signals")
        combined = weighted_signal_combination(
            {
                "trend": trend,
                "mean_reversion": mean_rev,
                "momentum": momentum,
                "volatility": volatility,
                "stat_arb": stat_arb,
            },
            strategy_weights,
        )

        technical_analysis[symbol] = {
            "signal": combined["signal"],
            "confidence": round(combined["confidence"] * 100),
            "reasoning": {
                "trend_following": {
                    "signal": trend["signal"],
                    "confidence": round(trend["confidence"] * 100),
                    "metrics": normalize_pandas(trend["metrics"]),
                },
                "mean_reversion": {
                    "signal": mean_rev["signal"],
                    "confidence": round(mean_rev["confidence"] * 100),
                    "metrics": normalize_pandas(mean_rev["metrics"]),
                },
                "momentum": {
                    "signal": momentum["signal"],
                    "confidence": round(momentum["confidence"] * 100),
                    "metrics": normalize_pandas(momentum["metrics"]),
                },
                "volatility": {
                    "signal": volatility["signal"],
                    "confidence": round(volatility["confidence"] * 100),
                    "metrics": normalize_pandas(volatility["metrics"]),
                },
                "statistical_arbitrage": {
                    "signal": stat_arb["signal"],
                    "confidence": round(stat_arb["confidence"] * 100),
                    "metrics": normalize_pandas(stat_arb["metrics"]),
                },
            },
        }
        progress.update_status(agent_id, symbol, "Done", analysis=json.dumps(technical_analysis, indent=2))

    message = HumanMessage(content=json.dumps(technical_analysis), name=agent_id)

    if state["metadata"]["show_reasoning"]:
        show_agent_reasoning(technical_analysis, "Crypto Technical Analyst")

    state["data"]["analyst_signals"][agent_id] = technical_analysis
    progress.update_status(agent_id, None, "Done")

    return {"messages": state["messages"] + [message], "data": data}
