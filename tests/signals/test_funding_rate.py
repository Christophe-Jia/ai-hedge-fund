"""Tests for FundingRateSignal (src/signals/funding_rate.py).

The signal has two threshold modes:

- rolling (default, the 2026 fix): high/low triggers are the p99/p1 of
  the 3-day funding-MA distribution over a trailing window (default
  730d, min 365d warmup) — see reports/outofsample_backtest.json for why
  the absolute thresholds do not transfer across funding regimes.
- absolute (absolute_mode=True): the legacy fixed +50/-5 ann. %
  thresholds, kept for comparison backtests.

The legacy-semantics tests below use real historical funding events from
data/btc_history.db (absolute mode):

- HIGH regime (3d MA > +50% ann.): trigger dates D=2024-02-29 (ma=+52.94)
  and D=2024-03-12 (ma=+56.81); the regime persists through 2024-03-07.
- LOW regime (3d MA < -5% ann.): trigger dates include D=2026-04-22
  (ma=-7.06) and D=2024-09-09 (ma=-5.05).

Signal semantics: generate(as_of) evaluates the window [as_of-3, as_of-1],
i.e. it corresponds to the backtest's trigger date D = as_of-1.

Rolling-mode tests use both real events (2024-02/03 high, 2022-11 FTX
low with a partial window) and synthetic injected funding data (exact
quantile / warmup / partial-window semantics).
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.signals.funding_rate import FundingRateSignal


@pytest.fixture()
def sig() -> FundingRateSignal:
    """Legacy absolute-threshold mode (+50 / -5 ann. %)."""
    return FundingRateSignal(absolute_mode=True)


# ---------------------------------------------------------------------------
# High-funding regime (momentum → MSTR/COIN) — legacy absolute mode
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
# Low-funding regime (capitulation rebound → broad basket) — legacy mode
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
# Edge cases — legacy mode
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
            absolute_mode=True,
            momentum_targets=("MSTR",),
            rebound_targets=("COIN", "MARA"),
        )
        h = sig.data_health()
        assert set(h["stocks"]) == {"MSTR", "COIN", "MARA"}


# ---------------------------------------------------------------------------
# Rolling-percentile mode (default)
# ---------------------------------------------------------------------------


class TestRollingMode:
    def test_default_constructor_is_rolling(self):
        sig = FundingRateSignal()
        assert sig.absolute_mode is False
        assert sig.window_days == 730
        assert sig.min_window_days == 365
        assert sig.high_percentile == 99.0
        assert sig.low_percentile == 1.0

    def test_rolling_high_fire_on_real_event(self):
        """as_of=2024-03-01: ma=+52.94 vs rolling p99≈34.45 → high fire.

        The rolling mode actually detects this episode one day EARLIER
        (2024-02-29, ma=+37.5 > p99≈30.5) — the 2021-calibrated absolute
        +50 needed to wait for 52.94.
        """
        sig = FundingRateSignal()
        out = sig.generate(pd.Timestamp("2024-03-01"))
        assert out is not None
        assert out.direction == "long"
        m = out.metadata
        assert m["regime"] == "high"
        assert m["targets"] == ["MSTR", "COIN"]
        assert m["mode"] == "rolling"
        assert m["funding_ma_annualized_pct"] == pytest.approx(52.94, abs=0.01)
        assert m["high_threshold_pct"] == pytest.approx(34.45, abs=0.1)
        assert m["low_threshold_pct"] == pytest.approx(-6.93, abs=0.1)
        assert m["rolling_window_obs"] == 730
        assert m["rolling_window_span_days"] == 730
        assert m["window_partial"] is False
        assert "window_mean_pct" in m and "window_median_pct" in m
        # the episode started at 2024-02-29 → not a new extreme on 03-01
        assert m["is_new_extreme"] is False

    def test_rolling_low_fire_on_real_event(self):
        """as_of=2026-04-23: ma=-7.06 vs rolling p1≈-5.43 → low fire."""
        sig = FundingRateSignal()
        out = sig.generate(pd.Timestamp("2026-04-23"))
        assert out is not None
        m = out.metadata
        assert m["regime"] == "low"
        assert m["targets"] == ["MSTR", "COIN", "MARA", "RIOT"]
        assert m["low_threshold_pct"] == pytest.approx(-5.43, abs=0.1)
        assert m["is_new_extreme"] is True

    def test_rolling_partial_window_ftx(self):
        """2022-11 FTX collapse: only ~676d of history (< 730d window) →
        fires with window_partial=True. The shallow 2022 dips (Apr/May,
        ma ≈ -8..-10) stay in the neutral band because the rolling p1
        (≈ -10..-14) adapts to the regime — the fix for the absolute
        threshold's 2022 bleeding."""
        sig = FundingRateSignal()
        assert sig.generate(pd.Timestamp("2022-04-25")) is None  # ma=-9.15, neutral
        out = sig.generate(pd.Timestamp("2022-11-11"))  # FTX: ma=-33.99
        assert out is not None
        m = out.metadata
        assert m["regime"] == "low"
        assert m["window_partial"] is True
        assert m["rolling_window_obs"] == 676
        assert m["rolling_window_span_days"] == 676
        assert m["is_new_extreme"] is True

    def test_rolling_point_in_time_bounds(self, monkeypatch):
        """Rolling mode: the reference window (up to window_days back) must
        also be loaded strictly before as_of — the SQL upper bound stays
        <= as_of and the load reaches window_days+window+1 days back."""
        sig = FundingRateSignal()
        captured: dict[str, int] = {}
        original = sig._load_funding

        def spy(start_ms: int, end_ms: int):
            captured["start_ms"] = start_ms
            captured["end_ms"] = end_ms
            return original(start_ms, end_ms)

        monkeypatch.setattr(sig, "_load_funding", spy)
        as_of = pd.Timestamp("2024-03-01")
        out = sig.generate(as_of)
        assert out is not None  # fires → the thresholds came from this load
        assert captured["end_ms"] <= int(as_of.value // 1_000_000)
        expected_start = as_of - pd.Timedelta(days=730 + 3 + 1)
        assert captured["start_ms"] == int(expected_start.value // 1_000_000)

    def test_warmup_returns_none(self):
        """Less than min_window_days (365) of history → None, and the
        reason is exposed via last_eval_metadata (None carries no
        metadata by contract)."""
        sig = FundingRateSignal()
        # funding data starts 2021-01-01 → only 148d of history
        assert sig.generate(pd.Timestamp("2021-06-01")) is None
        assert sig.last_eval_metadata.get("status") == "warmup"
        assert sig.last_eval_metadata.get("warmup") is True
        # 364d on the eve of the boundary is still warmup...
        assert sig.generate(pd.Timestamp("2022-01-03")) is None
        assert sig.last_eval_metadata.get("status") == "warmup"
        assert sig.last_eval_metadata.get("available_span_days") == 364
        # ...and 365d (2022-01-04) leaves warmup (neutral band here, but
        # no longer warmup).
        assert sig.generate(pd.Timestamp("2022-01-04")) is None
        assert sig.last_eval_metadata.get("status") == "neutral"

    def test_absolute_mode_has_no_warmup(self):
        """Legacy mode only needs the 3-day MA window — no rolling warmup."""
        sig = FundingRateSignal(absolute_mode=True)
        out = sig.generate(pd.Timestamp("2021-01-10"))
        # window [01-07..01-09] is complete; must not be a warmup None
        assert sig.last_eval_metadata.get("status") != "warmup"
        if out is not None:
            assert out.metadata["mode"] == "absolute"
            assert out.metadata["high_threshold_pct"] == 50.0


# ---------------------------------------------------------------------------
# Rolling mode on synthetic injected funding data (exact semantics)
# ---------------------------------------------------------------------------


def _synthetic_funding_frame(
    base: pd.Timestamp,
    daily_values: dict[int, float],
    n_days: int | None = None,
) -> pd.DataFrame:
    """One funding row per day at 12:00 UTC with the given annualized %.

    daily_values maps day-offset-from-base → annualized percent; days not
    listed default to 10.0 (a quiet regime). n_days defaults to
    max(daily_values)+1 (or 60 for an all-default frame).
    """
    if n_days is None:
        n_days = max(daily_values) + 1 if daily_values else 60
    rows = []
    for i in range(n_days):
        v = daily_values.get(i, 10.0)
        ts = base + pd.Timedelta(days=i, hours=12)
        rows.append({"ts": int(ts.value // 1_000_000), "rate": v / (3 * 365 * 100)})
    return pd.DataFrame(rows)


def _inject(sig: FundingRateSignal, frame: pd.DataFrame, monkeypatch) -> None:
    """Replace the signal's SQL loader with a bounded slice of `frame`."""

    def fake_load(start_ms: int, end_ms: int) -> pd.DataFrame:
        sel = frame[(frame["ts"] >= start_ms) & (frame["ts"] < end_ms)]
        return sel.reset_index(drop=True)

    monkeypatch.setattr(sig, "_load_funding", fake_load)


class TestRollingSynthetic:
    """window_days=100, min_window_days=50 for compact, exact checks.

    Data (base 2023-01-01): days 0..56 at +10% ann., days 57..59 at
    +100% ann. As_of = day 60 → current MA (days 57-59) = 100.

    Reference window = MA days [t-100, t-1] = [day2, day58] (data starts
    day 0 → first MA day is day 2): 55 values of 10, plus MA_57=40 and
    MA_58=70 → 57 obs, span 57 (50 ≤ 57 < 100 → partial window).

    p99 of {10×55, 40, 70} = 40 + 0.44·(70−40) = 53.2 (linear interp),
    p1 = 10.0 exactly. The CURRENT MA (100) is not in the reference set
    — if it were wrongly included, p99 would be ≈83, so the threshold
    assertion also pins the exclusion.
    """

    BASE = pd.Timestamp("2023-01-01")

    @pytest.fixture()
    def spike_sig(self, monkeypatch) -> FundingRateSignal:
        sig = FundingRateSignal(window_days=100, min_window_days=50)
        frame = _synthetic_funding_frame(
            self.BASE, {57: 100.0, 58: 100.0, 59: 100.0}
        )
        _inject(sig, frame, monkeypatch)
        return sig

    def test_partial_window_quantile_thresholds(self, spike_sig):
        out = spike_sig.generate(self.BASE + pd.Timedelta(days=60))
        assert out is not None
        m = out.metadata
        assert m["regime"] == "high"
        assert m["funding_ma_annualized_pct"] == pytest.approx(100.0, abs=0.01)
        assert m["high_threshold_pct"] == pytest.approx(53.2, abs=0.05)
        assert m["low_threshold_pct"] == pytest.approx(10.0, abs=1e-6)
        assert m["rolling_window_obs"] == 57
        assert m["rolling_window_span_days"] == 57
        assert m["window_partial"] is True
        assert m["window_mean_pct"] == pytest.approx(
            (55 * 10 + 40 + 70) / 57, abs=0.05
        )
        # score = 0.5 + 0.5·(100−53.2)/53.2 ≈ 0.940
        assert out.score == pytest.approx(0.9398, abs=0.005)
        # prev day's MA (70) already exceeded ITS rolling p99 (≈23.5) →
        # this continues an episode, not a new extreme
        assert m["is_new_extreme"] is False

    def test_full_window_not_partial(self, monkeypatch):
        """200 days of quiet data + 3 spike days → the 100d reference
        window is fully inside the data → window_partial=False."""
        sig = FundingRateSignal(window_days=100, min_window_days=50)
        frame = _synthetic_funding_frame(
            self.BASE, {197: 100.0, 198: 100.0, 199: 100.0}
        )
        _inject(sig, frame, monkeypatch)
        out = sig.generate(self.BASE + pd.Timedelta(days=200))
        assert out is not None
        m = out.metadata
        assert m["regime"] == "high"
        assert m["window_partial"] is False
        assert m["rolling_window_obs"] == 100
        assert m["rolling_window_span_days"] == 100

    def test_synthetic_warmup(self, monkeypatch):
        """Only 40 days of data (< min_window_days=50) → warmup None."""
        sig = FundingRateSignal(window_days=100, min_window_days=50)
        frame = _synthetic_funding_frame(self.BASE, {37: 100.0, 38: 100.0, 39: 100.0})
        _inject(sig, frame, monkeypatch)
        out = sig.generate(self.BASE + pd.Timedelta(days=40))
        assert out is None
        assert sig.last_eval_metadata.get("status") == "warmup"
        assert sig.last_eval_metadata.get("available_span_days") == 37

    def test_neutral_when_inside_rolling_band(self, monkeypatch):
        """Flat funding (every day +10% ann.) → p1 = p99 = 10 = current
        MA; the strict inequalities (ma > p99 / ma < p1) leave it in the
        neutral band. With a tight reference distribution even a mild
        spike clears p99 — the percentiles, not the absolute +50 level,
        decide."""
        sig = FundingRateSignal(window_days=100, min_window_days=50)
        frame = _synthetic_funding_frame(self.BASE, {})
        _inject(sig, frame, monkeypatch)
        out = sig.generate(self.BASE + pd.Timedelta(days=60))
        assert out is None
        assert sig.last_eval_metadata.get("status") == "neutral"


# ---------------------------------------------------------------------------
# Parameterization / validation
# ---------------------------------------------------------------------------


class TestParameterization:
    def test_percentile_params_reflected(self):
        sig = FundingRateSignal(high_percentile=97.0, low_percentile=3.0)
        out = sig.generate(pd.Timestamp("2024-03-01"))
        assert out is not None
        m = out.metadata
        assert m["high_percentile"] == 97.0
        assert m["low_percentile"] == 3.0
        # p97 of the same reference window is below its p99 (34.45)
        assert m["high_threshold_pct"] < 34.45

    def test_invalid_percentiles_rejected(self):
        with pytest.raises(ValueError):
            FundingRateSignal(high_percentile=95.0, low_percentile=95.0)
        with pytest.raises(ValueError):
            FundingRateSignal(high_percentile=99.0, low_percentile=0.0)
        with pytest.raises(ValueError):
            FundingRateSignal(high_percentile=100.0, low_percentile=1.0)

    def test_invalid_window_params_rejected(self):
        with pytest.raises(ValueError):
            FundingRateSignal(window_days=365, min_window_days=730)
