"""Unit tests for the top-N boundary stability check."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.validation import boundary_stability


def _separated(n_symbols: int = 30, top_n: int = 10, n_rows: int = 3) -> pd.DataFrame:
    """Well-separated scores: no ties anywhere, clean gaps of 1.0."""
    cols = [f"S{i:02d}" for i in range(n_symbols)]
    rows = [np.arange(n_symbols, dtype=float) for _ in range(n_rows)]
    return pd.DataFrame(rows, index=[f"m{i}" for i in range(n_rows)], columns=cols)


def _block_ties(n_symbols: int = 30, top_n: int = 10) -> pd.DataFrame:
    """Two clean leaders then a large tied block straddling the top-N cut.

    This reproduces the GBM pathology: a big group of names carries identical
    scores, so most of the top-N list is decided by sort-tie order.
    """
    cols = [f"S{i:02d}" for i in range(n_symbols)]
    scores = [100.0, 99.0] + [50.0] * (n_symbols - 2)
    return pd.DataFrame([scores], index=["m0"], columns=cols)


def _boundary_only_ties(n_symbols: int = 30, top_n: int = 10) -> pd.DataFrame:
    """Nine clean leaders, then ties that only reach the very last slot."""
    cols = [f"S{i:02d}" for i in range(n_symbols)]
    scores = [float(v) for v in range(100, 100 - (top_n - 1), -1)] + [50.0] * (n_symbols - (top_n - 1))
    return pd.DataFrame([scores], index=["m0"], columns=cols)


def test_separated_scores_are_stable():
    res = boundary_stability(_separated(), top_n=10, n_perturb=10, drop_frac=0.0)
    assert res["verdict"] == "STABLE"
    assert res["mean_flip_rate"] == 0.0
    assert res["max_tie_ratio"] == 0.0
    assert res["has_exact_ties"] is False
    assert res["max_boundary_ties"] == 1


def test_tied_block_straddling_the_cut_is_arbitrary():
    """The GBM pathology: most of the top-N list sits in one tied block."""
    res = boundary_stability(_block_ties(), top_n=10, n_perturb=30, drop_frac=0.0)
    assert res["verdict"] == "ARBITRARY"
    assert res["mean_flip_rate"] > 0.4
    assert res["has_exact_ties"] is True
    assert res["max_boundary_ties"] == 28


def test_tied_block_per_section_metrics():
    res = boundary_stability(_block_ties(), top_n=10, n_perturb=5, drop_frac=0.0)
    section = res["per_section"][0]
    assert section["boundary_score"] == 50.0
    assert section["boundary_ties"] == 28
    assert section["n_symbols"] == 30
    assert section["n_unique_scores"] == 3
    assert section["duplicates_in_top_n"] == 7  # 100, 99 then eight 50s


def test_ties_only_at_the_last_slot_are_stable_but_flagged():
    """Conservative by design: losing 1 of 10 slots is not >20% churn."""
    res = boundary_stability(_boundary_only_ties(), top_n=10, n_perturb=30, drop_frac=0.0)
    assert res["mean_flip_rate"] < 0.2
    assert res["verdict"] == "STABLE"
    assert res["has_exact_ties"] is True
    assert res["max_boundary_ties"] == 21


def test_tie_ratio_and_unique_counts():
    # 30 names, scores 0..9 repeated 3x -> 10 unique
    scores = [float(i % 10) for i in range(30)]
    df = pd.DataFrame([scores], index=["m0"], columns=[f"S{i}" for i in range(30)])
    res = boundary_stability(df, top_n=5, n_perturb=3, drop_frac=0.0)
    assert res["mean_tie_ratio"] == pytest.approx(2.0 / 3.0)
    assert res["per_section"][0]["n_unique_scores"] == 10


def test_series_input_is_single_cross_section():
    s = pd.Series(np.arange(20, dtype=float), index=[f"S{i}" for i in range(20)])
    res = boundary_stability(s, top_n=5, n_perturb=5, drop_frac=0.0)
    assert res["n_cross_sections"] == 1
    assert res["n_evaluated"] == 1
    assert res["verdict"] == "STABLE"


def test_small_cross_sections_are_skipped():
    df = pd.DataFrame([[1.0, 2.0, 3.0]], index=["thin"], columns=["A", "B", "C"])
    res = boundary_stability(df, top_n=5)
    assert res["verdict"] == "INSUFFICIENT"
    assert res["n_skipped"] == 1
    assert res["n_evaluated"] == 0


def test_drop_perturbation_flips_when_top_names_are_dropped():
    # 20 names, dropping 5% (=1 name) has a ~50% chance of hitting the top-10
    df = _separated(n_symbols=20, n_rows=1)
    res = boundary_stability(df, top_n=10, n_perturb=30, drop_frac=0.05, noise_scale=0.0)
    assert 0.0 < res["mean_flip_rate"] < 0.2


def test_deterministic_for_a_fixed_seed():
    a = boundary_stability(_block_ties(), top_n=10, n_perturb=10, drop_frac=0.0, seed=7)
    b = boundary_stability(_block_ties(), top_n=10, n_perturb=10, drop_frac=0.0, seed=7)
    assert a["mean_flip_rate"] == b["mean_flip_rate"]


def test_threshold_is_configurable():
    df = _separated(n_symbols=20, n_rows=1)
    strict = boundary_stability(df, top_n=10, n_perturb=30, drop_frac=0.05, noise_scale=0.0, flip_threshold=0.0)
    lax = boundary_stability(df, top_n=10, n_perturb=30, drop_frac=0.05, noise_scale=0.0, flip_threshold=0.5)
    assert strict["verdict"] == "ARBITRARY"
    assert lax["verdict"] == "STABLE"


def test_nan_scores_ignored():
    df = _separated(n_symbols=30, n_rows=1).copy()
    df.iloc[0, 0] = np.nan
    res = boundary_stability(df, top_n=10, n_perturb=3, drop_frac=0.0)
    assert res["per_section"][0]["n_symbols"] == 29
