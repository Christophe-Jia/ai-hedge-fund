"""Combine multiple independent signals into one portfolio decision."""

from __future__ import annotations

import pandas as pd

from .base import Signal, SignalOutput, direction_from_score


class SignalCombiner:
    """Combine multiple signals into one portfolio decision.

    Weights are renormalized over the signals that actually returned a
    SignalOutput on a given date, so one signal returning None (insufficient
    data) never blocks the others.
    """

    def __init__(
        self,
        signals: list[Signal],
        weights: dict[str, float] | None = None,
        threshold: float = 0.0,
    ) -> None:
        """
        Args:
            signals: signal instances to combine. Must have unique names.
            weights: per-signal weights keyed by signal name; equal
                weighting when None. Names missing from the dict get
                weight 0.0.
            threshold: |combined score| must exceed this to be non-flat.
        """
        if not signals:
            raise ValueError("signals must be non-empty")
        names = [s.name for s in signals]
        if len(set(names)) != len(names):
            raise ValueError(f"duplicate signal names: {names}")
        if weights is None:
            weights = {n: 1.0 for n in names}
        else:
            unknown = set(weights) - set(names)
            if unknown:
                raise ValueError(f"weights contain unknown signal names: {sorted(unknown)}")

        self._signals = list(signals)
        self._weights = dict(weights)
        self._threshold = threshold

    @property
    def signals(self) -> list[Signal]:
        return list(self._signals)

    def combined_score(self, as_of: pd.Timestamp) -> SignalOutput:
        """Weighted average of all signal scores.

        Signals returning None are skipped and the weights renormalized
        over the contributing signals. If nothing contributes, returns a
        flat, zero-confidence output.
        """
        weight_sum = 0.0
        score_sum = 0.0
        conf_sum = 0.0
        contributing: list[str] = []
        missing: list[str] = []

        for sig in self._signals:
            out = sig.generate(as_of)
            if out is None:
                missing.append(sig.name)
                continue
            w = self._weights.get(sig.name, 0.0)
            weight_sum += w
            score_sum += w * out.score
            conf_sum += w * out.confidence
            contributing.append(sig.name)

        if weight_sum <= 0.0:
            return SignalOutput(
                score=0.0,
                direction="flat",
                confidence=0.0,
                metadata={
                    "contributing": [],
                    "missing": missing,
                    "reason": "no contributing signal with positive weight",
                },
            )

        score = score_sum / weight_sum
        return SignalOutput(
            score=score,
            direction=direction_from_score(score, self._threshold),
            confidence=conf_sum / weight_sum,
            metadata={"contributing": contributing, "missing": missing},
        )

    def signal_correlations(
        self,
        start: pd.Timestamp | str,
        end: pd.Timestamp | str,
        freq: str = "D",
    ) -> pd.DataFrame:
        """Pairwise correlation of signal scores over a period.

        Evaluates every signal on each date in [start, end] (pandas
        date_range with `freq`) and correlates the score series. Dates
        where a signal returned None are excluded from that signal's
        series (NaN in the frame, pairwise-complete observations used).
        """
        dates = pd.date_range(start, end, freq=freq)
        records: list[dict[str, float | None]] = []
        for day in dates:
            row: dict[str, float | None] = {}
            for sig in self._signals:
                out = sig.generate(day)
                row[sig.name] = None if out is None else out.score
            records.append(row)

        frame = pd.DataFrame(records, index=dates, columns=[s.name for s in self._signals])
        return frame.corr()
