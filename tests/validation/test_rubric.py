"""Unit tests for the signal due-diligence rubric."""

from __future__ import annotations

import pytest

from src.validation.rubric import (
    BAND_ALLOW,
    BAND_REJECT,
    BAND_STRENGTHEN,
    BAND_THRESHOLDS,
    DIMENSIONS,
    DIMENSION_WEIGHTS,
    band_for,
    render_rubric_markdown,
    score,
    weakest_dimensions,
)

_DIM_IDS = [d["id"] for d in DIMENSIONS]


def _answers(score_value: int) -> dict[str, int]:
    return {d: score_value for d in _DIM_IDS}


def test_rubric_has_ten_dimensions_with_all_criteria():
    assert len(DIMENSIONS) == 10
    for d in DIMENSIONS:
        assert set(d["criteria"]) == {1, 3, 5}, d["id"]
        assert d["question"].strip()
        assert d["weight"] > 0


def test_uniform_scores_map_to_those_scores():
    assert score(_answers(5)).weighted_score == 5.0
    assert score(_answers(3)).weighted_score == 3.0
    assert score(_answers(1)).weighted_score == 1.0


def test_counterparty_and_preregistration_are_double_weighted():
    assert DIMENSION_WEIGHTS["counterparty_arbitrage"] == 2.0
    assert DIMENSION_WEIGHTS["preregistration"] == 2.0
    assert all(DIMENSION_WEIGHTS[d] == 1.0 for d in _DIM_IDS if d not in {"counterparty_arbitrage", "preregistration"})

    # all 1s except the doubled counterparty at 5: (2*5 + 2*1 + 8*1) / 12
    answers = _answers(1)
    answers["counterparty_arbitrage"] = 5
    res = score(answers)
    assert res.weighted_score == pytest.approx((10 + 2 + 8) / 12)


def test_custom_equal_weights_give_plain_mean():
    res = score(_answers(5), weights={d: 1.0 for d in _DIM_IDS})
    assert res.weighted_score == 5.0


def test_band_boundaries_are_inclusive_lower_bounds():
    assert band_for(3.5) == BAND_ALLOW
    assert band_for(3.4999) == BAND_STRENGTHEN
    assert band_for(2.5) == BAND_STRENGTHEN
    assert band_for(2.4999) == BAND_REJECT
    assert BAND_THRESHOLDS[BAND_ALLOW] == 3.5
    assert BAND_THRESHOLDS[BAND_STRENGTHEN] == 2.5


@pytest.mark.parametrize("bad", [0, 2, 4, 6, -1, 99])
def test_scores_must_be_1_3_or_5(bad):
    answers = _answers(3)
    answers["data_moat"] = bad
    with pytest.raises(ValueError):
        score(answers)


def test_boolean_and_non_int_scores_rejected():
    answers = _answers(3)
    answers["data_moat"] = True
    with pytest.raises(ValueError):
        score(answers)
    answers["data_moat"] = "5"
    with pytest.raises(ValueError):
        score(answers)


def test_unknown_dimension_rejected():
    with pytest.raises(ValueError, match="unknown rubric dimension"):
        score({**_answers(3), "moonshot": 5})


def test_empty_answers_rejected():
    with pytest.raises(ValueError):
        score({})


def test_missing_dimensions_are_reported_but_partial_score_computes():
    answers = _answers(3)
    del answers["data_moat"]
    del answers["executability"]
    res = score(answers)
    assert res.missing_dimensions == ["data_moat", "executability"]
    assert not res.complete
    assert res.n_scored == 8
    assert res.weighted_score == 3.0


def test_all_dimensions_scored_is_complete():
    res = score(_answers(3))
    assert res.complete
    assert res.missing_dimensions == []
    assert res.n_scored == 10


def test_weights_must_be_positive_and_known():
    with pytest.raises(ValueError, match="unknown dimension"):
        score(_answers(3), weights={"counterparty_arbitrage": 2.0, "bogus": 1.0})
    with pytest.raises(ValueError, match="positive"):
        score(_answers(3), weights={"counterparty_arbitrage": 0.0})


def test_as_dict_round_trips_core_fields():
    d = score(_answers(3)).as_dict()
    assert d["weighted_score"] == 3.0
    assert d["band"] == BAND_STRENGTHEN
    assert d["complete"] is True
    assert len(d["answers"]) == 10


def test_weakest_dimensions_lists_lowest_scores():
    answers = _answers(3)
    answers["counterparty_arbitrage"] = 1
    answers["data_moat"] = 1
    answers["executability"] = 1
    res = score(answers)
    weak = weakest_dimensions(res, k=3)
    assert set(weak) == {"counterparty_arbitrage", "data_moat", "executability"}


def test_render_markdown_covers_all_dimensions_and_bands():
    md = render_rubric_markdown()
    for d in DIMENSIONS:
        assert d["id"] in md
        assert d["name"] in md
    assert BAND_ALLOW in md and BAND_REJECT in md and BAND_STRENGTHEN in md
    assert "1 / 3 / 5" in md
