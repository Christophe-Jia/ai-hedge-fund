"""Unit tests for the multi-window robustness check."""

from __future__ import annotations

import numpy as np
import pytest

from src.validation import multi_window


def _result(value):
    return {"metrics": {"sharpe": value}}


def test_all_same_sign_is_stable():
    res = multi_window(lambda w: {"sharpe": w}, [0.9, 0.8, 1.1, 0.7], metric="sharpe")
    assert res["verdict"] == "STABLE"
    assert res["sign_consistency"] == 1.0
    assert res["n_positive"] == 4
    assert res["mean"] == pytest.approx(0.875)
    assert res["sign_flips"] == 0


def test_sign_flip_is_unstable():
    res = multi_window(lambda w: {"sharpe": w}, [0.9, -0.2, 0.5, -0.1], metric="sharpe")
    assert res["verdict"] == "UNSTABLE"
    assert res["sign_consistency"] == 0.5
    assert res["n_positive"] == 2 and res["n_negative"] == 2


def test_majority_rule_uses_majority_not_mean():
    # 3 negative, 2 positive -> consistency 0.6 (below 0.8) even though mean > 0
    vals = [-0.5, -0.4, -0.6, 0.9, 1.0]
    res = multi_window(lambda w: {"sharpe": w}, vals, metric="sharpe")
    assert res["n_negative"] == 3
    assert res["sign_consistency"] == pytest.approx(0.6)
    assert res["verdict"] == "UNSTABLE"


def test_pass_line_is_configurable():
    vals = [-0.5, -0.4, -0.6, 0.9, 1.0]
    assert multi_window(lambda w: w, vals, metric="value", pass_line=0.5)["verdict"] == "STABLE"


def test_dotted_path_and_nested_metrics():
    fn = lambda w: {"metrics": {"sharpe": w, "cagr_pct": w * 10}}  # noqa: E731
    assert multi_window(fn, [1.0, 2.0, 3.0], metric="metrics.sharpe")["mean"] == pytest.approx(2.0)
    assert multi_window(fn, [1.0, 2.0, 3.0], metric="cagr_pct")["mean"] == pytest.approx(20.0)


def test_plain_float_results_and_callable_metric():
    assert multi_window(lambda w: w, [0.1, 0.2], metric="value")["n_evaluated"] == 2
    res = multi_window(lambda w: {"sharpe": w}, [0.1, 0.2, 0.3], metric=lambda r: r["sharpe"] - 1.0)
    assert res["mean"] == pytest.approx(-0.8)


def test_missing_values_are_skipped():
    fn = lambda w: {"sharpe": w} if w is not None else {}  # noqa: E731
    res = multi_window(fn, [0.5, None, 0.7], metric="sharpe")
    assert res["n_windows"] == 3
    assert res["n_evaluated"] == 2
    assert res["verdict"] == "STABLE"


def test_single_window_is_insufficient():
    res = multi_window(lambda w: {"sharpe": w}, [0.5], metric="sharpe")
    assert res["verdict"] == "INSUFFICIENT"


def test_no_windows_is_insufficient():
    res = multi_window(lambda w: {"sharpe": w}, [], metric="sharpe")
    assert res["verdict"] == "INSUFFICIENT"
    assert res["n_evaluated"] == 0


def test_window_rows_are_reported_in_order():
    res = multi_window(lambda w: {"sharpe": w}, ["a", "b"], metric="sharpe")
    assert [r["window"] for r in res["windows"]] == ["a", "b"]


def test_gbm_window_flip_reproduces_unstable():
    """The documented GBM IC window flip (+0.0082 -> -0.0076) -> UNSTABLE."""
    res = multi_window(lambda w: {"ic": w}, [0.0082, -0.0076], metric="ic")
    assert res["verdict"] == "UNSTABLE"
    assert res["sign_consistency"] == 0.5


def test_std_and_range():
    res = multi_window(lambda w: {"sharpe": w}, [1.0, 2.0, 3.0], metric="sharpe")
    assert res["min"] == 1.0 and res["max"] == 3.0 and res["range"] == pytest.approx(2.0)
    assert res["std"] == pytest.approx(float(np.std([1.0, 2.0, 3.0], ddof=1)))
