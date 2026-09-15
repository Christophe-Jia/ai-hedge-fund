"""Unit tests for the parameter-neighbourhood stability check."""

from __future__ import annotations

import pytest

from src.validation import neighborhood_stability


def test_smooth_hill_is_not_overfit():
    values = {1: 1.0, 2: 2.0, 3: 3.0, 4: 4.0, 5: 5.0}
    res = neighborhood_stability(lambda x: {"sharpe": values[x]}, {"x": [1, 2, 3, 4, 5]}, metric="sharpe")
    assert res["verdict"] == "SMOOTH"
    assert res["isolation_ratio"] == pytest.approx(0.25)
    assert res["best_params"] == {"x": 5}
    assert res["n_neighbours"] == 1


def test_isolated_spike_is_overfit():
    values = {4: 0.3, 5: 0.4, 6: 0.9, 7: 0.4, 8: 0.3}
    res = neighborhood_stability(lambda x: {"sharpe": values[x]}, {"x": [4, 5, 6, 7, 8]}, metric="sharpe")
    assert res["verdict"] == "OVERFIT"
    assert res["best_params"] == {"x": 6}
    assert res["neighbour_median"] == pytest.approx(0.4)
    assert res["isolation_ratio"] == pytest.approx(0.5 / 0.6)


def test_two_dimensional_smooth_peak():
    def fn(a, b):
        return {"sharpe": -((a - 3) ** 2) - (b - 3) ** 2}

    res = neighborhood_stability(fn, {"a": [1, 2, 3, 4, 5], "b": [1, 2, 3, 4, 5]}, metric="sharpe")
    assert res["n_points"] == 25
    assert res["best_params"] == {"a": 3, "b": 3}
    assert res["n_neighbours"] == 4
    assert res["verdict"] == "SMOOTH"


def test_two_dimensional_spike():
    def fn(a, b):
        return {"sharpe": 1.0 if (a, b) == (2, 2) else 0.0}

    res = neighborhood_stability(fn, {"a": [1, 2, 3], "b": [1, 2, 3]}, metric="sharpe")
    assert res["verdict"] == "OVERFIT"
    assert res["isolation_ratio"] == pytest.approx(1.0)
    assert res["n_neighbours"] == 4


def test_flat_grid_is_smooth():
    res = neighborhood_stability(lambda x: {"sharpe": 0.5}, {"x": [1, 2, 3]}, metric="sharpe")
    assert res["verdict"] == "SMOOTH"
    assert res["value_range"] == 0.0
    assert res["isolation_ratio"] == 0.0


def test_base_params_are_merged():
    seen = {}

    def fn(top_n, threshold):
        seen[top_n] = threshold
        return {"sharpe": 0.5}

    neighborhood_stability(fn, {"top_n": [5, 10]}, base_params={"threshold": 0.15}, metric="sharpe")
    assert seen == {5: 0.15, 10: 0.15}


def test_grid_ordering_matches_param_grid():
    res = neighborhood_stability(lambda x: {"sharpe": x}, {"x": [3, 1, 2]}, metric="sharpe")
    assert [p["params"]["x"] for p in res["points"]] == [3, 1, 2]
    assert res["best_params"] == {"x": 3}


def test_all_none_is_insufficient():
    res = neighborhood_stability(lambda x: {}, {"x": [1, 2, 3]}, metric="sharpe")
    assert res["verdict"] == "INSUFFICIENT"


def test_threshold_is_configurable():
    values = {1: 1.0, 2: 1.1, 3: 1.3}
    strict = neighborhood_stability(lambda x: {"sharpe": values[x]}, {"x": [1, 2, 3]}, metric="sharpe", overfit_threshold=0.0)
    lax = neighborhood_stability(lambda x: {"sharpe": values[x]}, {"x": [1, 2, 3]}, metric="sharpe", overfit_threshold=0.9)
    assert strict["verdict"] == "OVERFIT"
    assert lax["verdict"] == "SMOOTH"


def test_exit_mechanism_real_grid_is_not_a_spike():
    """Real data from reports/exit_mechanism.json: hysteresis K grid deltas."""
    deltas = {10: 16.7, 12: 146.3, 15: 119.5, 18: 28.7, 20: -154.1, 30: -385.5}
    res = neighborhood_stability(lambda k: {"delta_full_bps": deltas[k]}, {"k": sorted(deltas)}, metric="delta_full_bps")
    assert res["best_params"] == {"k": 12}
    # K10/K15 neighbour median 68.1 vs range 531.8 -> ~0.15, a broad hill
    assert res["verdict"] == "SMOOTH"
    assert res["isolation_ratio"] < 0.3
