"""Unit tests for the red-team checklist."""

from __future__ import annotations

from src.validation import RED_TEAM_QUESTIONS, red_team_checklist, render_checklist, unanswered

GBM_LIKE_REPORT = {
    "meta": {
        "generated_at": "2026-09-14T15:10:25+00:00",
        "script": "scripts/xsec_gbm_selection.py",
        "data_range": ["2016-09-12", "2026-09-10"],
        "price_data": "Nasdaq daily OHLCV",
        "universe": {"point_in_time": "data/universe/sp100_YYYY.json"},
    },
    "walk_forward": {"n_test_months": 71},
    "comparison_full_test_window": {"gbm_top10": {"sharpe": 0.963, "total_costs": 12208.0}},
    "monthly_ic": {"mean": 0.0082, "n_months": 70},
    "conventions": {"signal": "month-end close"},
    "verdict_2021_plus": "OUTPERFORM",
}

COMPLETE_REPORT = {
    "meta": {
        "script": "scripts/thing.py",
        "generated_at": "2026-09-15T00:00:00+00:00",
        "data_range": ["2020-01-01", "2026-01-01"],
        "price_data": "Nasdaq",
        "universe": {"point_in_time": "data/universe/x.json"},
    },
    "monthly_ic": {"mean": 0.05, "std": 0.08, "t_stat": 5.2},
    "validation": {"multi_window": {"sign_consistency": 1.0}, "boundary_stability": {"verdict": "STABLE"}},
    "baseline_significance": {"t_stat": 2.5},
    "multiple_comparison": {"n_variants": 3},
    "cost_sensitivity": {"zero_cost_sharpe": 0.5},
    "total_costs_usd": 1234.0,
    "walk_forward": {"first_test_date": "2021-01-01"},
    "reproducibility": {"verdict": "REPRODUCIBLE", "overlap_ratio": 1.0},
    "conventions": {"execution": "next open"},
    "verdict": "PASS",
}


def test_empty_report_fails_everything():
    res = red_team_checklist({})
    assert res["verdict"] == "FAIL"
    assert res["n_answered"] == 0
    assert res["coverage"] == 0.0
    assert len(res["high_severity_unanswered"]) > 0


def test_complete_report_passes():
    res = red_team_checklist(COMPLETE_REPORT)
    assert res["verdict"] == "PASS"
    assert res["n_unanswered"] == 0
    assert res["coverage"] == 1.0


def test_gbm_like_report_fails_on_the_crisis_questions():
    res = red_team_checklist(GBM_LIKE_REPORT)
    assert res["verdict"] == "FAIL"
    missing = set(res["high_severity_unanswered"]) | set(res["medium_severity_unanswered"])
    # the five questions that would have exposed the crisis
    assert {"significance", "window_stability", "score_distribution", "baseline_significance", "reproducibility"} <= missing
    # but provenance / window / costs / walk-forward were actually answered
    answered = {i["id"] for i in res["items"] if i["answered"]}
    assert {"provenance", "data_window", "universe_snapshot", "cost_reporting", "out_of_sample", "verdict"} <= answered


def test_all_mode_requires_every_path():
    report = {"meta": {"script": "x.py"}}  # generated_at missing
    item = next(i for i in red_team_checklist(report)["items"] if i["id"] == "provenance")
    assert item["answered"] is False
    assert "meta.generated_at" in item["missing_paths"]


def test_any_mode_requires_one_path():
    report = {"monthly_ic": {"std": 0.08}}  # t_stat/se absent, std present
    item = next(i for i in red_team_checklist(report)["items"] if i["id"] == "significance")
    assert item["answered"] is True
    assert "monthly_ic.std" in item["matched_paths"]


def test_medium_only_gaps_yield_warn():
    report = dict(COMPLETE_REPORT)
    report.pop("cost_sensitivity")
    res = red_team_checklist(report)
    assert res["verdict"] == "WARN"
    assert res["high_severity_unanswered"] == []
    assert "cost_sensitivity" in res["medium_severity_unanswered"]


def test_questions_have_unique_ids_and_required_metadata():
    ids = [q["id"] for q in RED_TEAM_QUESTIONS]
    assert len(ids) == len(set(ids))
    for q in RED_TEAM_QUESTIONS:
        assert q["paths"] and q["question"] and q["why"]
        assert q["severity"] in {"high", "medium"}
        assert q["mode"] in {"all", "any"}


def test_unanswered_lists_high_severity_first():
    res = unanswered({})
    high = red_team_checklist({})["high_severity_unanswered"]
    assert res[: len(high)] == high


def test_render_checklist_annotates_statuses():
    lines = render_checklist(GBM_LIKE_REPORT)
    text = "\n".join(lines)
    assert "MISSING" in text and "PASS" in text
    assert len(lines) == len(RED_TEAM_QUESTIONS) + 2
