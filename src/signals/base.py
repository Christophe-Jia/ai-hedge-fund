"""Signal base class and output contract.

Every trading signal in the platform derives from :class:`Signal` and returns
a :class:`SignalOutput` for a given `as_of` date. Signals are expected to be
WEAK individually (score near 0 most days, strong occasionally) — the power
comes from combining many of them (see :mod:`src.signals.combiner`).

No look-ahead: implementations must only use data available STRICTLY BEFORE
`as_of`. Each signal is completely independent of every other signal.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

import pandas as pd

VALID_DIRECTIONS = ("long", "short", "flat")


@dataclass
class SignalOutput:
    """Output from a signal for a given date.

    Attributes:
        score: -1 (max bearish) to +1 (max bullish).
        direction: "long" | "short" | "flat".
        confidence: 0 to 1, how confident the signal is.
        metadata: extra info (e.g. underlying indicator values).
    """

    score: float
    direction: str
    confidence: float
    metadata: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not -1.0 <= self.score <= 1.0:
            raise ValueError(
                f"score must be in [-1, 1], got {self.score}"
            )
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(
                f"confidence must be in [0, 1], got {self.confidence}"
            )
        if self.direction not in VALID_DIRECTIONS:
            raise ValueError(
                f"direction must be one of {VALID_DIRECTIONS}, got {self.direction!r}"
            )


def direction_from_score(score: float, threshold: float = 0.0) -> str:
    """Map a score to a direction.

    |score| <= threshold -> "flat", otherwise sign of the score decides.
    """
    if score > threshold:
        return "long"
    if score < -threshold:
        return "short"
    return "flat"


class Signal(ABC):
    """Base class for all trading signals."""

    name: str
    description: str

    @abstractmethod
    def generate(self, as_of: pd.Timestamp) -> SignalOutput | None:
        """Generate signal for a given date.

        Must only use data available BEFORE `as_of` (no look-ahead).
        Return None if insufficient data.
        """

    @abstractmethod
    def data_health(self) -> dict:
        """Check data availability. Returns dict with status info."""
