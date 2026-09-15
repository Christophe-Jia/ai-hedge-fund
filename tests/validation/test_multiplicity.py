"""Unit tests for the multiple-comparison correction."""

from __future__ import annotations

import pytest

from src.validation import expected_max_abs_t, multiple_comparisons, required_t


def test_required_t_reference_values():
    assert required_t(1) == pytest.approx(1.959964, abs=1e-5)
    assert required_t(20) == pytest.approx(3.0233, abs=1e-3)
    assert required_t(100) == pytest.approx(3.4808, abs=1e-3)


def test_expected_max_abs_t_reference():
    assert expected_max_abs_t(20) == pytest.approx(2.4477, abs=1e-3)
    assert expected_max_abs_t(100) == pytest.approx(3.0349, abs=1e-3)
    assert expected_max_abs_t(1) == pytest.approx(0.6745, abs=1e-3)


def test_gbm_pre_2024_best_does_not_survive_the_search():
    """The real case: t=1.837 on 38 months, after ~20 directions searched."""
    res = multiple_comparisons(20, 1.837, n=38)
    assert res["verdict"] == "FAILS"
    assert res["observed_p"] == pytest.approx(0.0744, abs=1e-3)
    assert res["bonferroni_p"] == pytest.approx(1.0)  # capped
    assert res["required_t"] == pytest.approx(3.023, abs=1e-3)
    assert res["expected_best_abs_t_under_null"] == pytest.approx(2.448, abs=1e-3)
    assert res["family_wise_survivor"] is False
    assert "search noise" in res["interpretation"]


def test_single_hypothesis_uses_the_plain_bar():
    res = multiple_comparisons(1, 2.5, n=100)
    assert res["required_t"] == pytest.approx(1.96, abs=1e-3)
    assert res["verdict"] == "SURVIVES"


def test_strong_result_survives_a_wide_search():
    res = multiple_comparisons(20, 4.0, n=100)
    assert res["verdict"] == "SURVIVES"
    assert res["bonferroni_p"] < 0.05


def test_family_of_t_stats_runs_bh_fdr():
    res = multiple_comparisons(3, [0.5, 1.2, 2.1], n=50)
    assert res["n_hypotheses"] == 3
    assert res["observed_t"] == pytest.approx(2.1)
    assert res["observed_p"] == pytest.approx(0.0409, abs=1e-3)
    assert res["fdr_bh"] is not None
    assert res["fdr_bh"]["n_rejected"] == 0
    assert res["fdr_bh"]["adjusted_best_p"] == pytest.approx(0.1227, abs=1e-3)
    assert res["verdict"] == "FAILS"


def test_family_accepts_mapping_with_sample_size():
    res = multiple_comparisons(20, {"t_stat": 2.0, "n": 38})
    assert res["observed_p"] == pytest.approx(0.0531, abs=1e-3)
    assert res["df"] == 37


def test_mapping_with_p_value_is_accepted():
    res = multiple_comparisons(10, {"p_value": 0.01})
    assert res["observed_p"] == pytest.approx(0.01)
    assert res["required_t"] == pytest.approx(2.8070, abs=1e-3)


def test_normal_approximation_without_sample_size():
    res = multiple_comparisons(5, 1.96)
    assert res["df"] is None
    assert res["observed_p"] == pytest.approx(0.05, abs=1e-4)


def test_insufficient_inputs():
    res = multiple_comparisons(20, None)
    assert res["verdict"] == "INSUFFICIENT"
    assert res["required_t"] == pytest.approx(3.023, abs=1e-3)
    assert multiple_comparisons(20, [float("nan")])["verdict"] == "INSUFFICIENT"


def test_zero_hypotheses_is_treated_as_one():
    res = multiple_comparisons(0, 2.0, n=100)
    assert res["n_hypotheses"] == 1
