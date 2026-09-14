"""Pluggable strategy interface.

A strategy decides the portfolio's target weights. It is consulted on every
contribution date: new cash is deployed at these weights, and (if rebalancing
is enabled in BacktestConfig) the portfolio is traded back to them.

To write a custom strategy, subclass Strategy and implement target_weights().
The StrategyContext gives you everything you need:

    ctx.date             current trading day
    ctx.values           current market value per symbol, USD (e.g. {"QQQ": 1234.0})
    ctx.nav_usd          total portfolio value, USD
    ctx.cum_contrib_rmb  cumulative contributions, RMB
    ctx.month_index      how many contributions have happened so far (0-based)
    ctx.levels           full growth-index history (DataFrame, read-only);
                         look back with ctx.levels.loc[:ctx.date]
    ctx.fx               USD/CNY series (read-only)

Weights need not sum to 1 (they are normalized). Return {} to stay in cash for
that month. Implement reset() if your strategy keeps internal state - it is
called at the start of every backtest run.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Mapping

import pandas as pd


@dataclass
class StrategyContext:
    date: pd.Timestamp
    values: Mapping[str, float]
    nav_usd: float
    cum_contrib_rmb: float
    month_index: int
    levels: pd.DataFrame
    fx: pd.Series


class Strategy(ABC):
    name: str = "strategy"

    def reset(self) -> None:
        """Hook called before every run; override if you keep state."""

    @abstractmethod
    def target_weights(self, ctx: StrategyContext) -> dict[str, float]:
        """Return target weights, e.g. {"QQQ": 0.6, "VOO": 0.4}."""


class FixedWeights(Strategy):
    """Static allocation, e.g. classic DCA into a fixed mix."""

    def __init__(self, weights: dict[str, float], name: str | None = None):
        total = sum(weights.values())
        if total <= 0:
            raise ValueError("weights must sum to a positive number")
        self.weights = {k: v / total for k, v in weights.items() if v > 0}
        if not self.weights:
            raise ValueError("no positive weights")
        self.name = name or " + ".join(
            f"{k}{round(v * 100)}%" for k, v in self.weights.items()
        )

    def target_weights(self, ctx: StrategyContext) -> dict[str, float]:
        return dict(self.weights)


PRESETS: dict[str, dict[str, float]] = {
    "voo": {"VOO": 1.0},
    "qqq": {"QQQ": 1.0},
    "both": {"QQQ": 0.5, "VOO": 0.5},
    "core-satellite": {"VOO": 0.7, "QQQ": 0.2, "TQQQ": 0.1},
    "tqqq-20": {"VOO": 0.8, "TQQQ": 0.2},
    "tqqq": {"TQQQ": 1.0},
}
