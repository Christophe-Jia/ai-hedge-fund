"""
FinBERT-based sentiment analysis for financial text.

Uses ProsusAI/finbert (a BERT model fine-tuned on financial data) to
score text as positive/negative/neutral with a confidence score.

Falls back to a simple keyword heuristic when transformers is unavailable.

Env vars (optional):
  FINBERT_MODEL  – HuggingFace model ID (default: "ProsusAI/finbert")
"""

import os
from functools import lru_cache
from typing import Literal

SentimentLabel = Literal["positive", "negative", "neutral"]


@lru_cache(maxsize=1)
def _load_pipeline():
    """Load and cache the FinBERT sentiment pipeline."""
    try:
        from transformers import pipeline as hf_pipeline
    except ImportError:
        return None

    model_id = os.environ.get("FINBERT_MODEL", "ProsusAI/finbert")
    return hf_pipeline(
        "text-classification",
        model=model_id,
        tokenizer=model_id,
        top_k=None,  # Return all labels with scores
        truncation=True,
        max_length=512,
    )


def score_sentiment(text: str) -> dict:
    """
    Score the sentiment of a single text string.

    Args:
        text: Financial text (tweet, headline, post body, etc.)

    Returns:
        Dict with keys:
          label:      "positive" | "negative" | "neutral"
          confidence: float 0–1
          scores:     {"positive": float, "negative": float, "neutral": float}
    """
    pipe = _load_pipeline()
    if pipe is not None:
        results = pipe(text[:512])
        # results is a list of [{"label": ..., "score": ...}, ...]
        scores = {r["label"].lower(): r["score"] for r in results[0]}
        label = max(scores, key=scores.get)
        return {"label": label, "confidence": scores[label], "scores": scores}

    # Fallback: keyword heuristic
    return _keyword_sentiment(text)


def score_texts_batch(texts: list[str]) -> list[dict]:
    """
    Score sentiment for a batch of texts.

    Args:
        texts: List of text strings.

    Returns:
        List of sentiment dicts (same structure as score_sentiment).
    """
    pipe = _load_pipeline()
    if pipe is not None:
        truncated = [t[:512] for t in texts]
        batch_results = pipe(truncated)
        output = []
        for results in batch_results:
            scores = {r["label"].lower(): r["score"] for r in results}
            label = max(scores, key=scores.get)
            output.append({"label": label, "confidence": scores[label], "scores": scores})
        return output

    return [_keyword_sentiment(t) for t in texts]


def aggregate_sentiment(scored_items: list[dict], weight_by_confidence: bool = True) -> dict:
    """
    Aggregate a list of scored sentiment dicts into a single market signal.

    Args:
        scored_items:         Output from score_texts_batch or a list of score_sentiment results.
        weight_by_confidence: If True, weight each item by its confidence score.

    Returns:
        Dict with keys:
          signal:       "bullish" | "bearish" | "neutral"
          score:        float in [-1, 1]  (positive = bullish)
          num_positive: int
          num_negative: int
          num_neutral:  int
          consensus:    float 0–1 (how aligned the signals are)
    """
    if not scored_items:
        return {"signal": "neutral", "score": 0.0, "num_positive": 0, "num_negative": 0, "num_neutral": 0, "consensus": 0.0}

    label_to_value = {"positive": 1, "neutral": 0, "negative": -1}
    total_weight = 0.0
    weighted_sum = 0.0
    counts = {"positive": 0, "negative": 0, "neutral": 0}

    for item in scored_items:
        label = item["label"]
        conf = item["confidence"] if weight_by_confidence else 1.0
        val = label_to_value.get(label, 0)
        weighted_sum += val * conf
        total_weight += conf
        counts[label] = counts.get(label, 0) + 1

    score = weighted_sum / total_weight if total_weight > 0 else 0.0

    # Consensus: fraction of items agreeing with the dominant direction
    n = len(scored_items)
    dominant_count = max(counts.values())
    consensus = dominant_count / n if n > 0 else 0.0

    if score > 0.1:
        signal = "bullish"
    elif score < -0.1:
        signal = "bearish"
    else:
        signal = "neutral"

    return {
        "signal": signal,
        "score": round(score, 4),
        "num_positive": counts["positive"],
        "num_negative": counts["negative"],
        "num_neutral": counts["neutral"],
        "consensus": round(consensus, 4),
    }


# ---------------------------------------------------------------------------
# Keyword-based fallback (no model required)
# ---------------------------------------------------------------------------
_BULLISH_WORDS = {
    "moon", "bull", "bullish", "long", "buy", "up", "rally", "surge", "pump",
    "breakout", "all-time high", "ath", "undervalued", "growth", "strong",
}
_BEARISH_WORDS = {
    "bear", "bearish", "short", "sell", "down", "crash", "dump", "correction",
    "overvalued", "weak", "decline", "drop", "loss", "red",
}


def _keyword_sentiment(text: str) -> dict:
    words = set(text.lower().split())
    bull = len(words & _BULLISH_WORDS)
    bear = len(words & _BEARISH_WORDS)
    if bull > bear:
        label, conf = "positive", min(0.5 + 0.1 * (bull - bear), 0.9)
    elif bear > bull:
        label, conf = "negative", min(0.5 + 0.1 * (bear - bull), 0.9)
    else:
        label, conf = "neutral", 0.5
    scores = {"positive": conf if label == "positive" else 0.1, "negative": conf if label == "negative" else 0.1, "neutral": conf if label == "neutral" else 0.1}
    return {"label": label, "confidence": conf, "scores": scores}
