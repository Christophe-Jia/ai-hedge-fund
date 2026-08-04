"""
Crypto Social Sentiment Agent

Two-layer sentiment pipeline:
  Layer 1 (fast):  FinBERT scores every post/message from Reddit, StockTwits,
                   and Telegram.
  Layer 2 (LLM):  Periodically sends aggregated context to an LLM to identify
                   extreme sentiment events, key narratives, and top/bottom signals.
"""

import json
import os
from datetime import datetime, timezone

from langchain_core.messages import HumanMessage
from langchain_core.prompts import ChatPromptTemplate

from src.data.sentiment_nlp import score_texts_batch, aggregate_sentiment
from src.data.social.reddit import get_reddit_posts, BTC_SUBREDDITS
from src.data.social.stocktwits import get_stocktwits_messages, STOCKTWITS_SYMBOLS
from src.data.social.telegram import get_telegram_messages
from src.graph.state import AgentState, show_agent_reasoning
from src.utils.llm import call_llm
from src.utils.progress import progress


def crypto_sentiment_agent(state: AgentState, agent_id: str = "crypto_sentiment_agent") -> dict:
    """
    Aggregate social media sentiment for each crypto ticker.

    State keys used:
      data.tickers          – list of CCXT symbols, e.g. ["BTC/USDT"]
      metadata.model_name   – LLM model to use for layer-2 analysis
      metadata.model_provider
    """
    data = state["data"]
    tickers = data["tickers"]
    sentiment_analysis: dict = {}

    for symbol in tickers:
        # Normalise symbol to a short name for social media queries, e.g. "BTC/USDT" -> "BTC"
        asset = symbol.split("/")[0]
        progress.update_status(agent_id, symbol, "Collecting social data")

        texts: list[str] = []

        # --- Reddit ---
        try:
            reddit_posts = get_reddit_posts(subreddits=BTC_SUBREDDITS, query=asset, limit=50)
            for p in reddit_posts:
                if p.get("title"):
                    texts.append(p["title"])
                if p.get("selftext"):
                    texts.append(p["selftext"][:300])
        except Exception as e:
            progress.update_status(agent_id, symbol, f"Reddit unavailable: {e}")

        # --- StockTwits ---
        try:
            st_symbol = STOCKTWITS_SYMBOLS.get(asset, f"{asset}.X")
            st_messages = get_stocktwits_messages(st_symbol, limit=30)
            for m in st_messages:
                if m.get("body"):
                    texts.append(m["body"])
        except Exception as e:
            progress.update_status(agent_id, symbol, f"StockTwits unavailable: {e}")

        # --- Telegram ---
        try:
            tg_messages = get_telegram_messages(limit=30)
            for m in tg_messages:
                if m.get("text"):
                    texts.append(m["text"])
        except Exception as e:
            progress.update_status(agent_id, symbol, f"Telegram unavailable: {e}")

        if not texts:
            sentiment_analysis[symbol] = {
                "signal": "neutral",
                "confidence": 0,
                "reasoning": {"error": "No social data available"},
            }
            continue

        # --- Layer 1: FinBERT batch scoring ---
        progress.update_status(agent_id, symbol, "Running FinBERT scoring")
        scored = score_texts_batch(texts)
        agg = aggregate_sentiment(scored)

        # --- Layer 2: LLM deep analysis ---
        progress.update_status(agent_id, symbol, "LLM narrative analysis")
        llm_signal = _llm_sentiment_analysis(state, symbol, agg, texts[:20])

        # Combine layers
        final_signal = _combine_layers(agg, llm_signal)
        sentiment_analysis[symbol] = final_signal
        progress.update_status(agent_id, symbol, "Done", analysis=json.dumps(final_signal, indent=2))

    message = HumanMessage(content=json.dumps(sentiment_analysis), name=agent_id)

    if state["metadata"]["show_reasoning"]:
        show_agent_reasoning(sentiment_analysis, "Crypto Sentiment Agent")

    state["data"]["analyst_signals"][agent_id] = sentiment_analysis
    progress.update_status(agent_id, None, "Done")

    return {"messages": state["messages"] + [message], "data": data}


def _llm_sentiment_analysis(state: AgentState, symbol: str, agg: dict, sample_texts: list[str]) -> dict:
    """Call the configured LLM to produce a structured sentiment signal."""
    sample = "\n".join(f"- {t[:200]}" for t in sample_texts[:10])
    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                "You are an expert crypto market sentiment analyst. "
                "Analyse the provided social media posts and aggregated sentiment stats "
                "for {symbol}. Identify key narratives, extreme sentiment (euphoria or panic), "
                "potential top/bottom signals, and influential voices. "
                "Return a JSON object with keys: signal (bullish/bearish/neutral), "
                "confidence (0-100), key_themes (list of strings), extreme_sentiment (bool), "
                "reasoning (string).",
            ),
            (
                "human",
                "Symbol: {symbol}\n"
                "Aggregated FinBERT stats: {agg}\n\n"
                "Sample posts:\n{sample}",
            ),
        ]
    )

    try:
        result = call_llm(
            prompt=prompt,
            model_name=state["metadata"].get("model_name"),
            model_provider=state["metadata"].get("model_provider"),
            pydantic_model=None,
            agent_name="crypto_sentiment_agent",
            default_factory=lambda: {
                "signal": "neutral",
                "confidence": 50,
                "key_themes": [],
                "extreme_sentiment": False,
                "reasoning": "LLM unavailable",
            },
            symbol=symbol,
            agg=json.dumps(agg),
            sample=sample,
        )
        if isinstance(result, str):
            return json.loads(result)
        return result
    except Exception:
        return {
            "signal": agg["signal"].replace("positive", "bullish").replace("negative", "bearish"),
            "confidence": int(agg["score"] * 100),
            "key_themes": [],
            "extreme_sentiment": False,
            "reasoning": "LLM call failed; using FinBERT result only",
        }


def _combine_layers(finbert_agg: dict, llm_signal: dict) -> dict:
    """Merge FinBERT aggregate and LLM signal into a final output."""
    # Map "positive"/"negative" to "bullish"/"bearish" for finbert
    label_map = {"positive": "bullish", "negative": "bearish", "neutral": "neutral"}
    fb_signal = label_map.get(finbert_agg["signal"], finbert_agg["signal"])
    fb_conf = abs(finbert_agg["score"]) * 100

    llm_s = llm_signal.get("signal", "neutral")
    llm_c = float(llm_signal.get("confidence", 50))

    # Weighted average: LLM gets 60%, FinBERT 40%
    signal_val = {"bullish": 1, "neutral": 0, "bearish": -1}
    combined_score = 0.4 * signal_val.get(fb_signal, 0) + 0.6 * signal_val.get(llm_s, 0)

    if combined_score > 0.15:
        final_signal = "bullish"
    elif combined_score < -0.15:
        final_signal = "bearish"
    else:
        final_signal = "neutral"

    final_conf = int(0.4 * fb_conf + 0.6 * llm_c)

    return {
        "signal": final_signal,
        "confidence": final_conf,
        "reasoning": {
            "finbert": {
                "signal": fb_signal,
                "score": finbert_agg["score"],
                "num_positive": finbert_agg["num_positive"],
                "num_negative": finbert_agg["num_negative"],
                "num_neutral": finbert_agg["num_neutral"],
                "consensus": finbert_agg["consensus"],
            },
            "llm": {
                "signal": llm_s,
                "confidence": llm_c,
                "key_themes": llm_signal.get("key_themes", []),
                "extreme_sentiment": llm_signal.get("extreme_sentiment", False),
                "reasoning": llm_signal.get("reasoning", ""),
            },
        },
    }
