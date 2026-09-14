"""Tests for the signal framework (base classes + combiner)."""

from __future__ import annotations

import pandas as pd
import pytest

from src.signals import (
    Signal,
    SignalCombiner,
    SignalOutput,
    direction_from_score,
)


class StubSignal(Signal):
    """Deterministic stub: fixed score/confidence, optional None dates."""

    name = "stub"
    description = "stub signal for testing"

    def __init__(
        self,
        name: str = "stub",
        score: float = 0.0,
        confidence: float = 1.0,
        none_dates: set[pd.Timestamp] | None = None,
    ) -> None:
        self.name = name
        self.score = score
        self.confidence = confidence
        self.none_dates = none_dates or set()

    def generate(self, as_of: pd.Timestamp) -> SignalOutput | None:
        if as_of in self.none_dates:
            return None
        return SignalOutput(
            score=self.score,
            direction=direction_from_score(self.score),
            confidence=self.confidence,
        )

    def data_health(self) -> dict:
        return {"status": "ok", "name": self.name}


class LinearSignal(Signal):
    """Score equals the day-of-period index — perfectly linear over time."""

    name = "linear"
    description = "linear ramp signal"

    def __init__(self, name: str = "linear", slope: float = 1.0) -> None:
        self.name = name
        self.slope = slope

    def generate(self, as_of: pd.Timestamp) -> SignalOutput | None:
        score = max(-1.0, min(1.0, self.slope * (as_of.dayofyear % 10) / 10.0))
        return SignalOutput(score=score, direction=direction_from_score(score), confidence=0.5)

    def data_health(self) -> dict:
        return {"status": "ok"}


# ---------------------------------------------------------------------------
# SignalOutput
# ---------------------------------------------------------------------------


class TestSignalOutput:
    def test_valid_output(self):
        out = SignalOutput(score=0.5, direction="long", confidence=0.8)
        assert out.score == 0.5
        assert out.direction == "long"
        assert out.confidence == 0.8
        assert out.metadata == {}

    def test_metadata_defaults_to_empty_dict(self):
        a = SignalOutput(score=0.0, direction="flat", confidence=0.5)
        b = SignalOutput(score=0.0, direction="flat", confidence=0.5)
        assert a.metadata is not b.metadata  # no shared mutable default

    @pytest.mark.parametrize("score", [-1.5, 1.01, 2.0, -100.0])
    def test_score_out_of_range_raises(self, score):
        with pytest.raises(ValueError, match="score"):
            SignalOutput(score=score, direction="long", confidence=0.5)

    @pytest.mark.parametrize("confidence", [-0.1, 1.5])
    def test_confidence_out_of_range_raises(self, confidence):
        with pytest.raises(ValueError, match="confidence"):
            SignalOutput(score=0.0, direction="flat", confidence=confidence)

    @pytest.mark.parametrize("direction", ["buy", "LONG", "", "hold"])
    def test_invalid_direction_raises(self, direction):
        with pytest.raises(ValueError, match="direction"):
            SignalOutput(score=0.0, direction=direction, confidence=0.5)

    def test_boundary_values_accepted(self):
        out = SignalOutput(score=-1.0, direction="short", confidence=0.0)
        assert out.score == -1.0 and out.confidence == 0.0
        out = SignalOutput(score=1.0, direction="long", confidence=1.0)
        assert out.score == 1.0 and out.confidence == 1.0


# ---------------------------------------------------------------------------
# Signal ABC
# ---------------------------------------------------------------------------


class TestSignalABC:
    def test_cannot_instantiate_abstract_signal(self):
        with pytest.raises(TypeError):
            Signal()  # type: ignore[abstract]

    def test_subclass_must_implement_abstract_methods(self):
        class Incomplete(Signal):
            name = "incomplete"
            description = "missing methods"

        with pytest.raises(TypeError):
            Incomplete()  # type: ignore[abstract]

    def test_subclass_with_stub_works(self):
        sig = StubSignal(score=0.3)
        out = sig.generate(pd.Timestamp("2026-01-15"))
        assert out is not None and out.score == 0.3
        assert sig.data_health()["status"] == "ok"


# ---------------------------------------------------------------------------
# direction_from_score
# ---------------------------------------------------------------------------


class TestDirectionFromScore:
    def test_positive_is_long(self):
        assert direction_from_score(0.4) == "long"

    def test_negative_is_short(self):
        assert direction_from_score(-0.4) == "short"

    def test_zero_is_flat(self):
        assert direction_from_score(0.0) == "flat"

    def test_threshold_deadband(self):
        assert direction_from_score(0.05, threshold=0.1) == "flat"
        assert direction_from_score(0.15, threshold=0.1) == "long"
        assert direction_from_score(-0.15, threshold=0.1) == "short"
        assert direction_from_score(-0.05, threshold=0.1) == "flat"


# ---------------------------------------------------------------------------
# SignalCombiner — construction
# ---------------------------------------------------------------------------


class TestCombinerConstruction:
    def test_empty_signals_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            SignalCombiner([])

    def test_duplicate_names_raise(self):
        with pytest.raises(ValueError, match="duplicate"):
            SignalCombiner([StubSignal(name="dup"), StubSignal(name="dup")])

    def test_unknown_weight_key_raises(self):
        with pytest.raises(ValueError, match="unknown signal names"):
            SignalCombiner([StubSignal(name="a")], weights={"nope": 1.0})

    def test_signals_property_returns_copy(self):
        sigs = [StubSignal(name="a")]
        comb = SignalCombiner(sigs)
        comb.signals.append(StubSignal(name="b"))
        assert len(comb.signals) == 1


# ---------------------------------------------------------------------------
# SignalCombiner — combined_score
# ---------------------------------------------------------------------------


class TestCombinedScore:
    def test_equal_weighting_by_default(self):
        a = StubSignal(name="a", score=0.6, confidence=1.0)
        b = StubSignal(name="b", score=0.2, confidence=1.0)
        comb = SignalCombiner([a, b])
        out = comb.combined_score(pd.Timestamp("2026-01-15"))
        assert out.score == pytest.approx(0.4)
        assert out.direction == "long"
        assert out.confidence == pytest.approx(1.0)
        assert out.metadata["contributing"] == ["a", "b"]
        assert out.metadata["missing"] == []

    def test_custom_weights(self):
        a = StubSignal(name="a", score=1.0, confidence=1.0)
        b = StubSignal(name="b", score=-1.0, confidence=1.0)
        comb = SignalCombiner([a, b], weights={"a": 3.0, "b": 1.0})
        out = comb.combined_score(pd.Timestamp("2026-01-15"))
        assert out.score == pytest.approx(0.5)

    def test_missing_signal_is_skipped_and_weights_renormalized(self):
        as_of = pd.Timestamp("2026-01-15")
        a = StubSignal(name="a", score=0.6, none_dates={as_of})
        b = StubSignal(name="b", score=0.2)
        comb = SignalCombiner([a, b])
        out = comb.combined_score(as_of)
        assert out.score == pytest.approx(0.2)  # only b contributes
        assert out.metadata["missing"] == ["a"]
        assert out.metadata["contributing"] == ["b"]

    def test_missing_signal_with_zero_weight(self):
        """A None signal that carried zero weight shouldn't distort anything."""
        as_of = pd.Timestamp("2026-01-15")
        a = StubSignal(name="a", score=0.9, none_dates={as_of})
        b = StubSignal(name="b", score=0.3)
        comb = SignalCombiner([a, b], weights={"a": 0.0, "b": 1.0})
        out = comb.combined_score(as_of)
        assert out.score == pytest.approx(0.3)

    def test_all_missing_returns_flat_zero(self):
        as_of = pd.Timestamp("2026-01-15")
        comb = SignalCombiner(
            [StubSignal(name="a", none_dates={as_of}), StubSignal(name="b", none_dates={as_of})]
        )
        out = comb.combined_score(as_of)
        assert out.score == 0.0
        assert out.direction == "flat"
        assert out.confidence == 0.0
        assert sorted(out.metadata["missing"]) == ["a", "b"]

    def test_all_zero_weights_returns_flat_zero(self):
        comb = SignalCombiner([StubSignal(name="a", score=0.5)], weights={"a": 0.0})
        out = comb.combined_score(pd.Timestamp("2026-01-15"))
        assert out.score == 0.0
        assert out.direction == "flat"
        assert "reason" in out.metadata

    def test_confidence_is_weighted_average(self):
        a = StubSignal(name="a", score=0.5, confidence=1.0)
        b = StubSignal(name="b", score=0.5, confidence=0.2)
        comb = SignalCombiner([a, b], weights={"a": 3.0, "b": 1.0})
        out = comb.combined_score(pd.Timestamp("2026-01-15"))
        assert out.confidence == pytest.approx((3 * 1.0 + 1 * 0.2) / 4)

    def test_threshold_flips_direction(self):
        comb = SignalCombiner([StubSignal(name="a", score=0.05)], threshold=0.1)
        out = comb.combined_score(pd.Timestamp("2026-01-15"))
        assert out.score == pytest.approx(0.05)
        assert out.direction == "flat"

    def test_short_direction(self):
        comb = SignalCombiner([StubSignal(name="a", score=-0.7)])
        out = comb.combined_score(pd.Timestamp("2026-01-15"))
        assert out.direction == "short"


# ---------------------------------------------------------------------------
# SignalCombiner — signal_correlations
# ---------------------------------------------------------------------------


class TestSignalCorrelations:
    def test_perfectly_correlated_signals(self):
        up1 = LinearSignal(name="up1", slope=1.0)
        up2 = LinearSignal(name="up2", slope=1.0)
        comb = SignalCombiner([up1, up2])
        corr = comb.signal_correlations("2026-01-01", "2026-01-20")
        assert corr.loc["up1", "up2"] == pytest.approx(1.0)
        assert corr.loc["up1", "up1"] == pytest.approx(1.0)

    def test_anticorrelated_signals(self):
        up = LinearSignal(name="up", slope=1.0)
        down = LinearSignal(name="down", slope=-1.0)
        comb = SignalCombiner([up, down])
        corr = comb.signal_correlations("2026-01-01", "2026-01-20")
        assert corr.loc["up", "down"] == pytest.approx(-1.0)

    def test_none_observations_are_ignored(self):
        """NaN entries from missing signals must not corrupt correlations."""
        dates = pd.date_range("2026-01-01", periods=10, freq="D")
        none_dates = set(dates[::2])  # every other day missing
        up = LinearSignal(name="up", slope=1.0)
        gappy = StubSignal(name="gappy", score=0.5, none_dates=none_dates)
        comb = SignalCombiner([up, gappy])
        corr = comb.signal_correlations("2026-01-01", "2026-01-10")
        assert "up" in corr.index and "gappy" in corr.index
        # constant series -> NaN correlation (undefined), but no crash
        assert corr.loc["up", "gappy"] is None or pd.isna(corr.loc["up", "gappy"])

    def test_index_and_columns_are_signal_names(self):
        comb = SignalCombiner([LinearSignal(name="x"), LinearSignal(name="y")])
        corr = comb.signal_correlations("2026-01-01", "2026-01-10")
        assert set(corr.index) == {"x", "y"}
        assert set(corr.columns) == {"x", "y"}
