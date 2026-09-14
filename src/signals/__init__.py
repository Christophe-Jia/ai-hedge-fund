"""Signal framework: weak, independent signals combined into decisions."""

from .base import Signal, SignalOutput, direction_from_score
from .combiner import SignalCombiner
from .funding_rate import FundingRateSignal
from .onchain_fundamental import OnchainFundamentalSignal
from .weekend_gap import WeekendGapSignal

__all__ = [
    "Signal",
    "SignalOutput",
    "SignalCombiner",
    "direction_from_score",
    "FundingRateSignal",
    "OnchainFundamentalSignal",
    "WeekendGapSignal",
]
