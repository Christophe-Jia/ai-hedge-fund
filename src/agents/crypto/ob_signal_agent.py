"""
Order Book Signal Agent

Computes microstructure signals from real-time tick data collected by
the Gate.io WebSocket collector (scripts/collect_orderbook.py).

Signals derived:
  - bid_ask_imbalance : (bid_volume - ask_volume) / (bid_volume + ask_volume)
                        Positive → buying pressure (bullish)
  - trade_flow        : fraction of volume traded as buys in the window
                        > 0.6 → bullish, < 0.4 → bearish
  - large_order_bias  : net directional bias of trades >= large_order_threshold USD

When no order book data is available (empty DB or no recent data) the agent
outputs {"signal": "neutral", "confidence": 0, "data_available": false} so
the portfolio manager can distinguish absent data from a real neutral reading.
"""

import json
import time
from datetime import datetime, timezone

from langchain_core.messages import HumanMessage

from src.data.orderbook_store import OrderBookTradeStore
from src.graph.state import AgentState, show_agent_reasoning
from src.utils.progress import progress

# Look-back window for microstructure analysis (milliseconds)
_DEFAULT_LOOKBACK_MS = 60 * 60 * 1000  # 1 hour

# Minimum number of trades required to produce a non-neutral signal
_MIN_TRADES = 20

# Order size (USD) considered "large"
_LARGE_ORDER_USD = 50_000


def ob_signal_agent(state: AgentState, agent_id: str = "ob_signal_agent") -> dict:
    """
    Order book microstructure signal agent.

    Reads from OrderBookTradeStore and computes:
      - bid-ask imbalance from the latest L2 snapshot
      - trade flow direction (buy vs sell volume) over the last hour
      - large-order directional bias

    In backtesting mode (no live OB data), returns neutral + data_available=false.
    """
    data = state["data"]
    tickers = data["tickers"]
    end_date = data["end_date"]

    # Convert end_date to ms timestamp; use current time for live mode
    try:
        end_ts_ms = int(datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc).timestamp() * 1000)
    except Exception:
        end_ts_ms = int(time.time() * 1000)

    since_ts_ms = end_ts_ms - _DEFAULT_LOOKBACK_MS

    store = OrderBookTradeStore()

    ob_analysis: dict = {}

    for symbol in tickers:
        progress.update_status(agent_id, symbol, "Reading order book data")

        # Normalise symbol: "BTC/USDT:USDT" → "BTC/USDT"
        ob_symbol = symbol.split(":")[0]

        # Check if we have any recent data
        latest_ts = store.get_latest_trade_ts(ob_symbol)
        if latest_ts is None or latest_ts < since_ts_ms:
            ob_analysis[symbol] = {
                "signal": "neutral",
                "confidence": 0,
                "data_available": False,
                "reasoning": {
                    "note": "No order book data available — accumulating live data",
                    "trade_count": 0,
                },
            }
            continue

        trades = store.get_recent_trades(ob_symbol, since_ts_ms=since_ts_ms, until_ts_ms=end_ts_ms)

        if len(trades) < _MIN_TRADES:
            ob_analysis[symbol] = {
                "signal": "neutral",
                "confidence": 0,
                "data_available": True,
                "reasoning": {
                    "note": f"Insufficient trades ({len(trades)} < {_MIN_TRADES}) in lookback window",
                    "trade_count": len(trades),
                },
            }
            continue

        ob_snapshot = store.get_latest_order_book_snapshot(ob_symbol, before_ts_ms=end_ts_ms)

        # Compute signals
        imbalance_signal = _calc_bid_ask_imbalance(ob_snapshot)
        flow_signal = _calc_trade_flow(trades)
        large_order_signal = _calc_large_order_bias(trades)

        combined = _combine_ob_signals([imbalance_signal, flow_signal, large_order_signal])

        ob_analysis[symbol] = {
            "signal": combined["signal"],
            "confidence": combined["confidence"],
            "data_available": True,
            "reasoning": {
                "bid_ask_imbalance": imbalance_signal,
                "trade_flow": flow_signal,
                "large_order_bias": large_order_signal,
                "trade_count": len(trades),
                "lookback_minutes": _DEFAULT_LOOKBACK_MS // 60_000,
            },
        }
        progress.update_status(agent_id, symbol, "Done", analysis=json.dumps(ob_analysis, indent=2))

    message = HumanMessage(content=json.dumps(ob_analysis), name=agent_id)

    if state["metadata"]["show_reasoning"]:
        show_agent_reasoning(ob_analysis, "OB Signal Agent")

    state["data"]["analyst_signals"][agent_id] = ob_analysis
    progress.update_status(agent_id, None, "Done")

    return {"messages": state["messages"] + [message], "data": data}


# ------------------------------------------------------------------
# Signal computation helpers
# ------------------------------------------------------------------

def _calc_bid_ask_imbalance(ob_snapshot: dict) -> dict:
    """
    Compute imbalance = (bid_vol - ask_vol) / (bid_vol + ask_vol).
    Range: [-1, 1]. Positive means more bid-side depth (bullish pressure).
    """
    bids = ob_snapshot.get("bids", [])
    asks = ob_snapshot.get("asks", [])

    if not bids and not asks:
        return {"signal": "neutral", "confidence": 0, "imbalance": None, "note": "No OB snapshot"}

    bid_vol = sum(float(b[1]) * float(b[0]) for b in bids)  # USD notional
    ask_vol = sum(float(a[1]) * float(a[0]) for a in asks)
    total = bid_vol + ask_vol

    if total == 0:
        return {"signal": "neutral", "confidence": 0, "imbalance": 0.0}

    imbalance = (bid_vol - ask_vol) / total

    if imbalance > 0.20:
        signal, conf = "bullish", min(int(abs(imbalance) * 150), 80)
    elif imbalance < -0.20:
        signal, conf = "bearish", min(int(abs(imbalance) * 150), 80)
    else:
        signal, conf = "neutral", 20

    return {"signal": signal, "confidence": conf, "imbalance": round(imbalance, 4)}


def _calc_trade_flow(trades: list[dict]) -> dict:
    """
    Buy fraction = buy_volume / total_volume over the lookback window.
    > 0.6 → bullish, < 0.4 → bearish.
    """
    buy_vol = sum(t["amount"] * t["price"] for t in trades if t["side"] in ("buy", "Buy"))
    sell_vol = sum(t["amount"] * t["price"] for t in trades if t["side"] in ("sell", "Sell"))
    total = buy_vol + sell_vol

    if total == 0:
        return {"signal": "neutral", "confidence": 0, "buy_fraction": None}

    buy_frac = buy_vol / total

    if buy_frac > 0.60:
        signal, conf = "bullish", min(int((buy_frac - 0.50) * 300), 80)
    elif buy_frac < 0.40:
        signal, conf = "bearish", min(int((0.50 - buy_frac) * 300), 80)
    else:
        signal, conf = "neutral", 20

    return {"signal": signal, "confidence": conf, "buy_fraction": round(buy_frac, 4)}


def _calc_large_order_bias(trades: list[dict], threshold_usd: float = _LARGE_ORDER_USD) -> dict:
    """
    Net bias of large orders (>= threshold_usd notional).
    Large buy dominance → bullish; large sell dominance → bearish.
    """
    large_buy = sum(t["amount"] * t["price"] for t in trades
                    if t["side"] in ("buy", "Buy") and t["amount"] * t["price"] >= threshold_usd)
    large_sell = sum(t["amount"] * t["price"] for t in trades
                     if t["side"] in ("sell", "Sell") and t["amount"] * t["price"] >= threshold_usd)

    total = large_buy + large_sell
    if total == 0:
        return {"signal": "neutral", "confidence": 0, "note": "No large orders in window"}

    bias = (large_buy - large_sell) / total

    if bias > 0.30:
        signal, conf = "bullish", min(int(bias * 100), 70)
    elif bias < -0.30:
        signal, conf = "bearish", min(int(abs(bias) * 100), 70)
    else:
        signal, conf = "neutral", 15

    return {
        "signal": signal,
        "confidence": conf,
        "bias": round(bias, 4),
        "large_buy_usd": round(large_buy, 0),
        "large_sell_usd": round(large_sell, 0),
    }


def _combine_ob_signals(signals: list[dict]) -> dict:
    """Weighted average of OB sub-signals."""
    weights = [0.40, 0.40, 0.20]  # imbalance, flow, large_order
    score_map = {"bullish": 1, "neutral": 0, "bearish": -1}

    weighted_score = 0.0
    weighted_conf = 0.0
    total_weight = 0.0

    for sig, w in zip(signals, weights):
        if sig.get("confidence", 0) > 0:
            weighted_score += score_map[sig["signal"]] * w
            weighted_conf += sig["confidence"] * w
            total_weight += w

    if total_weight == 0:
        return {"signal": "neutral", "confidence": 0}

    avg_score = weighted_score / total_weight
    avg_conf = int(weighted_conf / total_weight)

    if avg_score > 0.25:
        signal = "bullish"
    elif avg_score < -0.25:
        signal = "bearish"
    else:
        signal = "neutral"

    return {"signal": signal, "confidence": avg_conf}
