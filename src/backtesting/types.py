from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Mapping, Optional, Sequence, TypedDict, Literal
from enum import Enum

import pandas as pd


class Action(str, Enum):
    BUY = "buy"
    SELL = "sell"
    SHORT = "short"
    COVER = "cover"
    HOLD = "hold"

# Backward-compatible alias
ActionLiteral = Literal["buy", "sell", "short", "cover", "hold"]


class PositionState(TypedDict):
    """Represents per-ticker position state in the portfolio."""

    long: int
    short: int
    long_cost_basis: float
    short_cost_basis: float
    short_margin_used: float


class TickerRealizedGains(TypedDict):
    """Realized PnL per side for a single ticker."""

    long: float
    short: float


class PortfolioSnapshot(TypedDict):
    """Snapshot of portfolio state.

    The structure mirrors the existing dict used by the current Backtester
    to ensure drop-in compatibility during incremental refactors.
    """

    cash: float
    margin_used: float
    margin_requirement: float
    positions: Dict[str, PositionState]
    realized_gains: Dict[str, TickerRealizedGains]


# DataFrame alias for clarity in interfaces
PriceDataFrame = pd.DataFrame


class AgentDecision(TypedDict):
    action: ActionLiteral
    quantity: float


AgentDecisions = Dict[str, AgentDecision]


# Analyst signal payloads can vary by agent; keep as loose dicts
AnalystSignal = Dict[str, Any]
AgentSignals = Dict[str, Dict[str, AnalystSignal]]


class AgentOutput(TypedDict):
    decisions: AgentDecisions
    analyst_signals: AgentSignals


# Use functional style to allow keys with spaces to mirror current code
PortfolioValuePoint = TypedDict(
    "PortfolioValuePoint",
    {
        "Date": datetime,
        "Portfolio Value": float,
        "Long Exposure": float,
        "Short Exposure": float,
        "Gross Exposure": float,
        "Net Exposure": float,
        "Long/Short Ratio": float,
    },
    total=False,
)


class PerformanceMetrics(TypedDict, total=False):
    """Performance metrics computed over the equity curve.

    Keys are aligned with the current implementation in src/backtester.py.
    Values are optional to support progressive calculation over time.
    """

    sharpe_ratio: Optional[float]
    sortino_ratio: Optional[float]
    max_drawdown: Optional[float]
    max_drawdown_date: Optional[str]
    long_short_ratio: Optional[float]
    gross_exposure: Optional[float]
    net_exposure: Optional[float]
    # BTC cost-aware metrics
    total_fees_paid: Optional[float]
    total_funding_paid: Optional[float]
    total_slippage_cost: Optional[float]
    net_pnl_after_costs: Optional[float]
    num_trades: Optional[int]
    win_rate: Optional[float]
    profit_factor: Optional[float]
    calmar_ratio: Optional[float]


class PerpPositionState(TypedDict):
    """State of a single perpetual futures position (isolated margin)."""

    symbol: str
    side: Literal["long", "short"]
    size: float               # in base currency (e.g. BTC)
    entry_price: float        # average entry price in USD
    leverage: float           # e.g. 5.0 for 5x
    initial_margin: float     # USD locked as margin
    unrealized_pnl: float     # mark-to-market PnL in USD
    realized_pnl: float       # cumulative realized PnL in USD
    cumulative_funding: float # net funding payments (negative = paid)
    liquidation_price: float  # price at which position is force-closed
    is_liquidated: bool       # True once the position has been liquidated


class TradeRecord(TypedDict):
    """Record of a single executed trade for cost reporting."""

    timestamp: str        # ISO-8601 datetime string
    symbol: str
    market_type: str      # "spot" or "perp"
    side: str             # "buy" or "sell"
    quantity: float
    price: float          # execution price (after slippage)
    notional: float       # quantity * price
    fee_usd: float
    slippage_usd: float
    total_cost_usd: float


