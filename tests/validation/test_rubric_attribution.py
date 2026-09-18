"""Tests for the rubric attribution analysis (exploratory, with honest warnings)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from src.validation.rubric import DIMENSIONS

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "rubric_attribution.py"
_spec = importlib.util.spec_from_file_location("rubric_attribution", SCRIPT)
rubric_attribution = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rubric_attribution)

analyse = rubric_attribution.analyse
EXPLORATORY_WARNING = rubric_attribution.EXPLORATORY_WARNING

_DIMS = [d["id"] for d in DIMENSIONS]


def _rec(name, survived, base_rate, counterparty=1, status=None):
    answers = {d: 3 for d in _DIMS}
    answers["base_rate_anchoring"] = base_rate
    answers["counterparty_arbitrage"] = counterparty
    rec = {
        "hypothesis_id": name,
        "rubric": {"answers": answers},
        "outcome": {"survived": survived},
    }
    if status:
        rec["status"] = status
    return rec


def test_requires_survival_labels_or_is_ignored():
    records = [
        _rec("a", True, 5),
        {"hypothesis_id": "no_label", "rubric": {"answers": {d: 3 for d in _DIMS}}},
    ]
    report = analyse(records)
    assert report["n_decided"] == 1


def test_attribution_reports_warning_and_all_dimensions():
    records = [
        _rec("s1", True, 5),
        _rec("s2", True, 5),
        _rec("f1", False, 1),
        _rec("f2", False, 1),
        _rec("f3", False, 1),
        _rec("f4", False, 3),
    ]
    report = analyse(records)
    assert report["warning"] == EXPLORATORY_WARNING
    assert "探索性分析" in report["warning"]
    assert len(report["dimensions"]) == 10
    ids = {r["id"] for r in report["dimensions"]}
    assert ids == set(_DIMS)
    assert report["n_survived"] == 2


def test_constant_dimension_has_no_correlation():
    records = [
        _rec("s1", True, 5, counterparty=1),
        _rec("s2", True, 5, counterparty=1),
        _rec("f1", False, 1, counterparty=1),
        _rec("f2", False, 1, counterparty=1),
    ]
    report = analyse(records)
    cp = next(r for r in report["dimensions"] if r["id"] == "counterparty_arbitrage")
    assert cp["point_biserial"] is None or abs(cp["point_biserial"]) < 1e-9


def test_correlated_dimension_is_ranked_first():
    records = [
        _rec("s1", True, 5),
        _rec("s2", True, 5),
        _rec("f1", False, 1),
        _rec("f2", False, 1),
        _rec("f3", False, 1),
    ]
    report = analyse(records)
    assert report["best_dimension"] == "base_rate_anchoring"
    assert report["best_abs_point_biserial"] > 0.5


def test_multiplicity_controls_are_reported():
    records = [_rec(f"f{i}", i < 2, 5 if i < 2 else 1) for i in range(10)]
    report = analyse(records)
    assert report["bonferroni_alpha"] == 0.05 / report["n_dimensions"]
    assert report["critical_abs_r_bonferroni"] is not None
    assert "fdr_bh" in report
    assert report["expected_false_positives_at_0.05"] > 0


def test_leave_one_out_is_reported_for_ranked_dimensions():
    records = [_rec(f"r{i}", i < 2, 5 if i < 2 else 1) for i in range(6)]
    report = analyse(records)
    best = next(r for r in report["dimensions"] if r["id"] == "base_rate_anchoring")
    assert best["loo_abs_r_min"] is not None
    assert best["loo_abs_r_max"] >= best["loo_abs_r_min"]


def test_scoring_degradation_is_reported():
    """The honest limit: constant retrospective dimensions collapse out entirely."""
    records = [_rec(f"d{i}", i < 2, 5 if i < 2 else 1) for i in range(8)]
    report = analyse(records)
    deg = report["scoring_degradation"]
    # nine dimensions are constant in this fixture -> only base_rate is analysable
    assert deg["n_dimensions_scored"] == 1
    assert deg["n_binary_or_less_dimensions"] == 1
    assert deg["distinct_score_counts"]["base_rate_anchoring"] == 2
    assert "退化" in deg["warning"]
    assert "前瞻" in deg["warning"]
    assert "prospective" in deg["implication"]

