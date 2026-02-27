#!/usr/bin/env python3
"""
Crypto Technical Analysis – offline mode.

Reads local OHLCV data from data/crypto/ and runs the same signal
functions used by the live technical agent, printing a summary table.

Usage:
    poetry run python scripts/run_crypto_analysis.py
    poetry run python scripts/run_crypto_analysis.py --timeframe daily
"""

import argparse
import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Allow imports from repo root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.agents.technicals import (
    calculate_trend_signals,
    calculate_mean_reversion_signals,
    calculate_momentum_signals,
    calculate_volatility_signals,
    calculate_stat_arb_signals,
    weighted_signal_combination,
)

DATA_ROOT = Path(__file__).resolve().parent.parent / "data" / "crypto"

SYMBOLS = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]

STRATEGY_WEIGHTS = {
    "trend": 0.25,
    "mean_reversion": 0.20,
    "momentum": 0.25,
    "volatility": 0.15,
    "stat_arb": 0.15,
}

MIN_ROWS = 63  # calculate_trend_signals needs EMA-55 + some warmup


def load_local_ohlcv(symbol: str, timeframe: str = "1h") -> pd.DataFrame:
    """Load OHLCV data from a local JSON file (no network required)."""
    dir_name = symbol.replace("/", "-")
    path = DATA_ROOT / dir_name / f"ohlcv_{timeframe}.json"
    if not path.exists():
        raise FileNotFoundError(f"Data file not found: {path}")

    with open(path) as f:
        records = json.load(f)

    df = pd.DataFrame(records)
    df["datetime"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.rename(columns={"datetime": "date"})
    df = df.sort_values("date").reset_index(drop=True)

    # Ensure standard column names expected by technicals.py
    required = {"open", "high", "low", "close", "volume"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns {missing} in {path}")

    return df


def run_analysis(symbol: str, timeframe: str) -> dict:
    """Run all technical signal functions on local data for one symbol."""
    df = load_local_ohlcv(symbol, timeframe)

    if len(df) < MIN_ROWS:
        return {
            "symbol": symbol,
            "error": f"Only {len(df)} rows (need ≥{MIN_ROWS})",
        }

    # calculate_adx mutates the input df, so work on a copy
    df_copy = df.copy()

    trend = calculate_trend_signals(df_copy)
    # Reload a fresh copy for each subsequent call (adx mutation side-effect)
    mean_rev = calculate_mean_reversion_signals(df.copy())
    momentum = calculate_momentum_signals(df.copy())
    volatility = calculate_volatility_signals(df.copy())
    stat_arb = calculate_stat_arb_signals(df.copy())

    combined = weighted_signal_combination(
        {
            "trend": trend,
            "mean_reversion": mean_rev,
            "momentum": momentum,
            "volatility": volatility,
            "stat_arb": stat_arb,
        },
        STRATEGY_WEIGHTS,
    )

    rsi_val = mean_rev["metrics"].get("rsi_14", float("nan"))
    adx_val = trend["metrics"].get("adx", float("nan"))

    # Build a short notes string
    notes_parts = []
    if not np.isnan(adx_val):
        notes_parts.append(f"ADX={adx_val:.1f}")
    if not np.isnan(rsi_val):
        notes_parts.append(f"RSI={rsi_val:.1f}")
    notes_parts.append(f"rows={len(df)}")

    return {
        "symbol": symbol,
        "signal": combined["signal"],
        "confidence": round(combined["confidence"] * 100),
        "trend": trend["signal"],
        "mean_rev": mean_rev["signal"],
        "momentum": momentum["signal"],
        "volatility": volatility["signal"],
        "stat_arb": stat_arb["signal"],
        "notes": ", ".join(notes_parts),
        "error": None,
    }


def _color(signal: str) -> str:
    """ANSI colour codes for terminal output."""
    codes = {"bullish": "\033[92m", "bearish": "\033[91m", "neutral": "\033[93m"}
    reset = "\033[0m"
    return codes.get(signal, "") + signal + reset


def print_table(results: list[dict], timeframe: str) -> None:
    header = f"{'Symbol':<12} {'Signal':<10} {'Conf':>5}  {'Trend':<10} {'MeanRev':<10} {'Momentum':<10} {'Volatility':<12} {'StatArb':<10} Notes"
    sep = "-" * len(header)
    print(f"\nCrypto Technical Analysis  [{timeframe} bars]")
    print(sep)
    print(header)
    print(sep)

    for r in results:
        if r.get("error"):
            print(f"{r['symbol']:<12}  ERROR: {r['error']}")
            continue

        print(
            f"{r['symbol']:<12} "
            f"{_color(r['signal']):<20} "
            f"{r['confidence']:>4}%  "
            f"{_color(r['trend']):<20} "
            f"{_color(r['mean_rev']):<20} "
            f"{_color(r['momentum']):<20} "
            f"{_color(r['volatility']):<22} "
            f"{_color(r['stat_arb']):<20} "
            f"{r['notes']}"
        )
    print(sep)


def main() -> None:
    parser = argparse.ArgumentParser(description="Offline crypto technical analysis")
    parser.add_argument(
        "--timeframe",
        default="1h",
        choices=["15m", "1h", "daily"],
        help="OHLCV timeframe to use (default: 1h)",
    )
    args = parser.parse_args()

    results = []
    for symbol in SYMBOLS:
        try:
            result = run_analysis(symbol, args.timeframe)
        except FileNotFoundError as e:
            result = {"symbol": symbol, "error": str(e)}
        except Exception as e:
            result = {"symbol": symbol, "error": f"Unexpected error: {e}"}
        results.append(result)
        status = result.get("signal", "ERROR")
        print(f"[✓] {symbol}  →  {status}")

    print_table(results, args.timeframe)


if __name__ == "__main__":
    main()
