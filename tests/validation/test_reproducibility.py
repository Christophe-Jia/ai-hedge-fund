"""Unit tests for the reproducibility probe."""

from __future__ import annotations

import pytest

from src.validation import reproducibility_probe


def test_the_gbm_mismatch_is_non_reproducible():
    """The exact crisis: an --as-of rerun produced 2/10 overlap with the picks."""
    stored = [f"S{i:02d}" for i in range(10)]
    rerun = stored[:2] + [f"N{i:02d}" for i in range(8)]
    res = reproducibility_probe(stored, rerun, top_n=10)
    assert res["verdict"] == "NON_REPRODUCIBLE"
    assert res["overlap_ratio"] == pytest.approx(0.2)
    assert res["n_intersection"] == 2
    assert len(res["missing_from_candidate"]) == 8
    assert res["scope"] == "top_10"


def test_identical_selections_are_reproducible():
    picks = [f"S{i:02d}" for i in range(10)]
    res = reproducibility_probe(picks, picks, top_n=10)
    assert res["verdict"] == "REPRODUCIBLE"
    assert res["overlap_ratio"] == 1.0
    assert res["jaccard"] == 1.0
    assert res["n_value_mismatches"] == 0


def test_top_n_ignores_ranks_below_the_cut():
    base = {f"S{i:02d}": float(100 - i) for i in range(10)}
    cand = dict(base)
    cand["S09"] = 50.0  # only the last name moved; top-3 is untouched
    res = reproducibility_probe(base, cand, top_n=3)
    assert res["verdict"] == "REPRODUCIBLE"
    assert res["overlap_ratio"] == 1.0


def test_score_mismatch_is_flagged():
    base = {"A": 1.0, "B": 2.0, "C": 3.0}
    cand = {"A": 1.0, "B": 2.0, "C": 3.5}
    res = reproducibility_probe(base, cand)
    assert res["verdict"] == "NON_REPRODUCIBLE"
    assert res["overlap_ratio"] == 1.0
    assert res["n_value_mismatches"] == 1
    assert res["value_mismatches"][0]["key"] == "C"
    assert res["value_mismatches"][0]["abs_diff"] == pytest.approx(0.5)
    assert "score mismatches" in res["interpretation"]


def test_mismatch_within_tolerance_is_fine():
    base = {"A": 0.02543, "B": 0.01884}
    cand = {"A": 0.0254300001, "B": 0.01884}
    res = reproducibility_probe(base, cand, tolerance=1e-6)
    assert res["verdict"] == "REPRODUCIBLE"


def test_score_ties_do_not_count_as_reproducible():
    """Two runs that break identical scores differently are not reproducible."""
    base = {"A": 1.0, "B": 1.0, "C": 1.0}
    cand = {"A": 1.0, "B": 1.0, "C": 0.999999}
    res = reproducibility_probe(base, cand, tolerance=1e-9)
    assert res["verdict"] == "NON_REPRODUCIBLE"
    assert res["n_value_mismatches"] == 1


def test_accepts_lists_of_dicts():
    base = [{"symbol": "MSFT", "score": 0.025}, {"symbol": "INTC", "score": 0.019}]
    cand = [{"symbol": "MSFT", "score": 0.025}, {"symbol": "INTC", "score": 0.019}]
    res = reproducibility_probe(base, cand, top_n=2)
    assert res["verdict"] == "REPRODUCIBLE"


def test_pass_line_is_configurable():
    stored = [f"S{i:02d}" for i in range(10)]
    rerun = stored[:8] + ["N1", "N2"]
    assert reproducibility_probe(stored, rerun, top_n=10)["verdict"] == "NON_REPRODUCIBLE"
    assert reproducibility_probe(stored, rerun, top_n=10, pass_line=0.7)["verdict"] == "REPRODUCIBLE"


def test_empty_input_is_insufficient():
    assert reproducibility_probe([], ["A"])["verdict"] == "INSUFFICIENT"
    assert reproducibility_probe(["A"], None)["verdict"] == "INSUFFICIENT"
    assert reproducibility_probe("not-a-list", "also-not")["verdict"] == "INSUFFICIENT"


def test_verification_block_month_counts_model():
    """The real risk_gate/gbm_attribution 'verification_vs_source_report' shape."""
    in_source = {f"m{i}": 1.0 for i in range(71)}
    matching = {f"m{i}": 1.0 for i in range(71)}
    assert reproducibility_probe(in_source, matching)["verdict"] == "REPRODUCIBLE"
    mismatched = {f"m{i}": 1.0 for i in range(2)}
    res = reproducibility_probe(in_source, mismatched)
    assert res["verdict"] == "NON_REPRODUCIBLE"
    assert res["overlap_ratio"] == pytest.approx(2 / 71, abs=1e-4)
