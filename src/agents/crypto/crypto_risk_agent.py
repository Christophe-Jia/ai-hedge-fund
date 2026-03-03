"""
Crypto Risk Management Agent

Extends the stock risk_manager logic to handle:
  - 24/7 crypto market (no daily close)
  - Higher volatility regimes
  - Dynamic position sizing via ATR-based stops
  - Max position limits as % of portfolio

Data source: HistoricalOHLCVStore (SQLite with CCXT fallback).
"""

import json
import math
from datetime import datetime, timezone

import pandas as pd
from langchain_core.messages import HumanMessage

from src.data.historical_store import HistoricalOHLCVStore
from src.graph.state import AgentState, show_agent_reasoning
from src.utils.progress import progress


def crypto_risk_agent(state: AgentState, agent_id: str = "crypto_risk_agent") -> dict:
    """
    Crypto risk management agent.

    Outputs per-symbol risk metrics and computes a maximum position size
    (as a fraction of total portfolio value) that respects:
      1. Maximum drawdown constraint (default 20%)
      2. Volatility scaling (ATR-based)
      3. Maximum single-asset exposure cap (default 40% of portfolio)
    """
    data = state["data"]
    tickers = data["tickers"]
    start_date = data["start_date"]
    end_date = data["end_date"]
    portfolio = data["portfolio"]
    exchange_id = data.get("exchange_id", "binance")

    start_ts_ms = int(datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc).timestamp() * 1000)
    end_ts_ms = int(datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc).timestamp() * 1000)

    store = HistoricalOHLCVStore(exchange_id=exchange_id)

    # Config (can be overridden via state metadata)
    max_drawdown = float(state["metadata"].get("max_drawdown_pct", 0.20))
    max_position_pct = float(state["metadata"].get("max_position_pct", 0.40))
    atr_risk_pct = float(state["metadata"].get("atr_risk_pct", 0.02))  # Risk 2% of portfolio per ATR unit

    total_portfolio_value = portfolio.get("total_cash", 0.0)
    for pos in portfolio.get("positions", {}).values():
        total_portfolio_value += pos.get("cash", 0.0)

    risk_analysis: dict = {}

    for symbol in tickers:
        progress.update_status(agent_id, symbol, "Fetching price data")

        # Strip perp suffix for spot price lookup
        spot_symbol = symbol.split(":")[0]

        df = store.get_ohlcv(
            symbol=spot_symbol,
            market_type="spot",
            timeframe="1d",
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
        )

        if df.empty:
            risk_analysis[symbol] = _empty_risk(symbol, "No price data")
            continue

        df = df.rename(columns={"ts": "date"})

        if len(df) < 14:
            risk_analysis[symbol] = _empty_risk(symbol, "Insufficient data")
            continue

        current_price = float(df["close"].iloc[-1])

        # --- Volatility metrics ---
        returns = df["close"].pct_change().dropna()
        daily_vol = float(returns.std())
        annualised_vol = daily_vol * math.sqrt(365)

        # ATR (14-period)
        atr = _calc_atr(df, 14)

        # --- ATR-based position sizing ---
        # Position size = (portfolio * atr_risk_pct) / ATR_in_dollars
        if atr > 0:
            atr_position_value = (total_portfolio_value * atr_risk_pct) / atr
        else:
            atr_position_value = 0.0

        # Cap at max_position_pct of portfolio
        max_value = total_portfolio_value * max_position_pct
        position_value = min(atr_position_value, max_value)
        max_quantity = position_value / current_price if current_price > 0 else 0.0

        # --- Max drawdown check ---
        cum_returns = (1 + returns).cumprod()
        rolling_max = cum_returns.cummax()
        drawdowns = (cum_returns - rolling_max) / rolling_max
        current_drawdown = float(drawdowns.iloc[-1])
        max_dd = float(drawdowns.min())

        # Reduce position if currently in large drawdown
        drawdown_scale = max(0.0, 1.0 + current_drawdown / max_drawdown)  # 0 at max_drawdown, 1 at 0 drawdown
        adjusted_quantity = max_quantity * drawdown_scale

        risk_analysis[symbol] = {
            "signal": "neutral",  # Risk agent outputs constraints, not direction
            "confidence": 100,
            "current_price": current_price,
            "max_position_size": round(adjusted_quantity, 8),
            "remaining_position_limit": round(adjusted_quantity * current_price, 2),
            "reasoning": {
                "daily_volatility": round(daily_vol, 4),
                "annualised_volatility": round(annualised_vol, 4),
                "atr_14": round(atr, 4),
                "current_drawdown_pct": round(current_drawdown * 100, 2),
                "max_drawdown_pct": round(max_dd * 100, 2),
                "drawdown_scale": round(drawdown_scale, 3),
                "atr_position_value": round(atr_position_value, 2),
                "max_position_value": round(max_value, 2),
                "total_portfolio_value": round(total_portfolio_value, 2),
            },
        }
        progress.update_status(agent_id, symbol, "Done", analysis=json.dumps(risk_analysis, indent=2))

    message = HumanMessage(content=json.dumps(risk_analysis), name=agent_id)

    if state["metadata"]["show_reasoning"]:
        show_agent_reasoning(risk_analysis, "Crypto Risk Agent")

    state["data"]["analyst_signals"][agent_id] = risk_analysis
    progress.update_status(agent_id, None, "Done")

    return {"messages": state["messages"] + [message], "data": data}


def _calc_atr(df: pd.DataFrame, period: int = 14) -> float:
    """Calculate the most recent ATR value."""
    high_low = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift()).abs()
    low_close = (df["low"] - df["close"].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr_series = tr.rolling(period).mean()
    return float(atr_series.iloc[-1]) if not atr_series.empty else 0.0


def _empty_risk(symbol: str, reason: str) -> dict:
    return {
        "signal": "neutral",
        "confidence": 0,
        "current_price": 0.0,
        "max_position_size": 0.0,
        "remaining_position_limit": 0.0,
        "reasoning": {"error": reason},
    }
