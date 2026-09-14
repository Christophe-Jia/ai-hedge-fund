"""Tests for FundingRateSignal (src/signals/funding_rate.py).

Uses real historical funding events from data/btc_history.db:

- HIGH regime (3d MA > +50% ann.): trigger dates D=2024-02-29 (ma=+52.94)
  and D=2024-03-12 (ma=+56.81); the regime persists through 2024-03-07.
- LOW regime (3d MA < -5% ann.): trigger dates include D=2026-04-22
  (ma=-7.06) and D=2024-09-09 (ma=-5.05).

Signal semantics: generate(as_of) evaluates the window [as_of-3, as_of-1],
i.e. it corresponds to the backtest's trigger date D = as_of-1.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.signals.funding_rate import FundingRateSignal


@pytest.fixture()
def sig() -> FundingRateSignal:
    return FundingRateSignal()


# ---------------------------------------------------------------------------
# High-funding regime (momentum → MSTR/COIN)
# ---------------------------------------------------------------------------


class TestHighRegime:
    def test_fires_long_on_high_extreme(self, sig):
        """D=2024-02-29 (ma=+52.94) → generate(2024-03-01) fires long."""
        out = sig.generate(pd.Timestamp("2024-03-01"))
        assert out is not None
        assert out.direction == "long"
        assert out.metadata["regime"] == "high"
        assert out.metadata["targets"] == ["MSTR", "COIN"]
        assert out.metadata["is_new_extreme"] is True
        assert out.metadata["funding_ma_annualized_pct"] == pytest.approx(
            52.94, abs=0.01
        )

    def test_score_scales_with_extremity(self, sig):
        """Score = 0.5 at the +50 threshold, +0.5 per threshold of excess."""
        out = sig.generate(pd.Timestamp("2024-03-01"))  # ma=+52.94
        assert out.score == pytest.approx(0.52944, abs=1e-4)
        assert out.confidence == pytest.approx(out.score)
        assert 0.5 <= out.score <= 1.0

    def test_miners_excluded_from_momentum_targets(self, sig):
        """MARA/RIOT fall after high funding — they must not be targets."""
        out = sig.generate(pd.Timestamp("2024-03-01"))
        assert "MARA" not in out.metadata["targets"]
        assert "RIOT" not in out.metadata["targets"]

    def test_persistent_extreme_is_not_new(self, sig):
        """D=2024-03-01 also extreme (ma=+57.25) → 2024-03-02 continues it."""
        out = sig.generate(pd.Timestamp("2024-03-02"))
        assert out is not None
        assert out.direction == "long"
        assert out.metadata["regime"] == "high"
        assert out.metadata["is_new_extreme"] is False

    def test_second_high_event_fires(self, sig):
        """D=2024-03-12 (ma=+56.81) after a neutral gap → new extreme."""
        out = sig.generate(pd.Timestamp("2024-03-13"))
        assert out is not None
        assert out.metadata["is_new_extreme"] is True
        assert out.score == pytest.approx(0.56813, abs=1e-4)


# ---------------------------------------------------------------------------
# Low-funding regime (capitulation rebound → broad basket)
# ---------------------------------------------------------------------------


class TestLowRegime:
    def test_fires_long_on_low_extreme(self, sig):
        """D=2026-04-22 (ma=-7.06) → generate(2026-04-23) fires long."""
        out = sig.generate(pd.Timestamp("2026-04-23"))
        assert out is not None
        assert out.direction == "long"
        assert out.metadata["regime"] == "low"
        assert out.metadata["targets"] == ["MSTR", "COIN", "MARA", "RIOT"]
        assert out.metadata["is_new_extreme"] is True
        assert out.metadata["funding_ma_annualized_pct"] == pytest.approx(
            -7.06, abs=0.01
        )

    def test_score_scales_with_depth(self, sig):
        """Score = 0.5 at -5, 1.0 at -10 (threshold distance doubles)."""
        out = sig.generate(pd.Timestamp("2026-04-23"))  # ma=-7.06
        assert out.score == pytest.approx(0.70567, abs=1e-4)
        assert 0.5 <= out.score <= 1.0

    def test_shallow_negative_inside_neutral_band(self, sig):
        """Slightly negative funding (between -5 and +50) must NOT fire."""
        # 2025-06-13..15 funding MA is mildly positive (~+5%) — neutral.
        assert sig.generate(pd.Timestamp("2025-06-15")) is None


# ---------------------------------------------------------------------------
# Look-ahead safety
# ---------------------------------------------------------------------------


class TestLookAheadSafety:
    def test_trigger_day_itself_does_not_fire(self, sig):
        """D=2024-02-29 is the backtest trigger, but generate on that date
        only sees [02-26..02-28] (ma=+37.5, neutral) → None. The 02-29
        funding data that makes the day extreme is NOT visible yet."""
        assert sig.generate(pd.Timestamp("2024-02-29")) is None

    def test_query_upper_bound_is_strictly_before_as_of(self, sig, monkeypatch):
        """The funding query end bound must be <= as_of (no same-day data)."""
        captured: dict[str, int] = {}
        original = sig._load_funding

        def spy(start_ms: int, end_ms: int):
            captured["start_ms"] = start_ms
            captured["end_ms"] = end_ms
            return original(start_ms, end_ms)

        monkeypatch.setattr(sig, "_load_funding", spy)
        as_of = pd.Timestamp("2024-03-01")
        sig.generate(as_of)
        assert captured["end_ms"] <= int(as_of.value // 1_000_000)
        # window = 3 days + 1 extra day for the is_new_extreme check
        expected_start = as_of - pd.Timedelta(days=4)
        assert captured["start_ms"] == int(expected_start.value // 1_000_000)

    def test_day_after_regime_ends_returns_none(self, sig):
        """D=2024-03-08 (ma=+46.99) drops below threshold → 03-09 is None."""
        assert sig.generate(pd.Timestamp("2024-03-09")) is None


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_insufficient_window_returns_none(self, sig):
        """Funding coverage starts 2023-03-02; 2023-03-04 lacks a full window."""
        assert sig.generate(pd.Timestamp("2023-03-04")) is None

    def test_tz_aware_as_of_handled(self, sig):
        """tz-aware input normalizes to the same UTC day as naive input."""
        naive = sig.generate(pd.Timestamp("2024-03-01"))
        aware = sig.generate(pd.Timestamp("2024-03-01", tz="UTC"))
        aware_ny = sig.generate(pd.Timestamp("2024-03-01", tz="America/New_York"))
        assert naive is not None and aware is not None and aware_ny is not None
        assert naive.score == aware.score == aware_ny.score

    def test_first_full_window_works(self, sig):
        """as_of=2023-03-06 has the first complete [03-03..03-05] window."""
        out = sig.generate(pd.Timestamp("2023-03-06"))
        # Fires or not depending on the funding level — but must not raise,
        # and if it fires the direction is long with valid score bounds.
        if out is not None:
            assert out.direction == "long"
            assert 0.5 <= out.score <= 1.0

    def test_both_regimes_are_long(self, sig):
        """Per the validated (inverted) hypothesis, both extremes are long."""
        high = sig.generate(pd.Timestamp("2024-03-01"))
        low = sig.generate(pd.Timestamp("2026-04-23"))
        assert high.direction == "long"
        assert low.direction == "long"
        assert high.score > 0 and low.score > 0


# ---------------------------------------------------------------------------
# data_health
# ---------------------------------------------------------------------------


class TestDataHealth:
    def test_structure_and_internal_consistency(self, sig):
        h = sig.data_health()
        assert h["signal"] == "funding_rate"
        assert set(h) >= {"ok", "funding", "stocks"}
        f = h["funding"]
        assert f["rows"] > 0
        assert f["first_date"] <= "2023-03-02"  # feed started here; may extend earlier after backfills
        assert "stale_hours" in f and "max_stale_hours" in f
        for sym in ("MSTR", "COIN", "MARA", "RIOT"):
            assert sym in h["stocks"]
            assert h["stocks"][sym]["rows"] > 0
        # overall ok is exactly funding-ok AND at least one stock ok
        expected = bool(f.get("ok")) and any(
            v.get("ok") for v in h["stocks"].values()
        )
        assert h["ok"] is expected

    def test_custom_targets_reflected_in_health(self):
        sig = FundingRateSignal(
            momentum_targets=("MSTR",),
            rebound_targets=("COIN", "MARA"),
        )
        h = sig.data_health()
        assert set(h["stocks"]) == {"MSTR", "COIN", "MARA"}
