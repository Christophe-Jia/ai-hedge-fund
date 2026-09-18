"""Unit tests for the append-only hypothesis registry and its pre-registration rules."""

from __future__ import annotations

import json

import pytest

from src.validation.registry import (
    DEFAULT_MIN_SAMPLE,
    STATUS_REGISTERED,
    STATUS_REJECTED,
    STATUS_RESOLVED,
    DuplicateHypothesisError,
    PreRegistrationDataError,
    RegistryValidationError,
    append_hypothesis,
    build_record,
    check_no_pre_registration_data,
    get_hypothesis,
    load_registry,
    parse_utc,
    registry_stats,
    sample_sufficiency,
    survival_label,
    utc_now_iso,
)
from src.validation.rubric import DIMENSIONS

_DIM_IDS = [d["id"] for d in DIMENSIONS]
_GOOD_ANSWERS = {d: 3 for d in _DIM_IDS}


def _record(**overrides):
    kwargs = dict(
        hypothesis_id="h1",
        statement="X predicts Y",
        mechanism="X -> flow -> Y",
        trigger_definition="when X > 0, go long Y",
        resolution_criteria="|t| >= 2 and same sign in both halves",
        data_requirements=["prices"],
        rubric_answers=dict(_GOOD_ANSWERS),
        author="tester",
        status=STATUS_REGISTERED,
        registered_at_utc="2026-01-01T00:00:00Z",
    )
    kwargs.update(overrides)
    return build_record(**kwargs)


def test_build_record_scores_the_rubric_and_stamps_times():
    rec = _record()
    assert rec["rubric"]["weighted_score"] == 3.0
    assert rec["rubric"]["band"] == "NEEDS_STRENGTHENING"
    assert rec["registered_at_utc"] == "2026-01-01T00:00:00Z"
    assert rec["evaluation_window_start"] == rec["registered_at_utc"]
    assert "revision" not in rec  # revision is added by append_hypothesis, not build_record


def test_build_record_requires_complete_rubric():
    answers = dict(_GOOD_ANSWERS)
    del answers["data_moat"]
    with pytest.raises(RegistryValidationError, match="missing"):
        _record(rubric_answers=answers)


def test_evaluation_window_may_not_precede_registration():
    with pytest.raises(RegistryValidationError, match="may not precede"):
        _record(evaluation_window_start="2025-12-31T00:00:00Z")


def test_reject_band_requires_an_explicit_user_override():
    reject_answers = {d: 1 for d in _DIM_IDS}
    with pytest.raises(RegistryValidationError, match="user_override"):
        _record(rubric_answers=reject_answers)
    rec = _record(
        rubric_answers=reject_answers,
        user_override={"reason": "user insisted", "by": "user"},
    )
    assert rec["rubric"]["band"] == "REJECT"
    assert rec["user_override"]["reason"] == "user insisted"


def test_evaluation_window_after_registration_is_allowed():
    rec = _record(evaluation_window_start="2026-02-01T00:00:00Z")
    assert rec["evaluation_window_start"] == "2026-02-01T00:00:00Z"


def test_validate_record_rejects_missing_fields():
    rec = _record()
    del rec["statement"]
    with pytest.raises(RegistryValidationError, match="missing required"):
        append_hypothesis("/tmp/unused_registry_should_not_be_written.jsonl", rec)


def test_validate_record_rejects_blank_fields_and_bad_status():
    rec = _record()
    rec["mechanism"] = "   "
    with pytest.raises(RegistryValidationError, match="mechanism"):
        append_hypothesis("/tmp/unused_registry_should_not_be_written.jsonl", rec)
    rec = _record()
    rec["status"] = "bogus"
    with pytest.raises(RegistryValidationError, match="status"):
        append_hypothesis("/tmp/unused_registry_should_not_be_written.jsonl", rec)


def test_append_load_and_dedup(tmp_path):
    path = str(tmp_path / "registry.jsonl")
    append_hypothesis(path, _record())
    assert len(load_registry(path)) == 1
    assert get_hypothesis(path, "h1")["hypothesis_id"] == "h1"

    # exact duplicate id -> refused (append-only, one record per hypothesis)
    with pytest.raises(DuplicateHypothesisError):
        append_hypothesis(path, _record())

    # a different id is fine
    append_hypothesis(path, _record(hypothesis_id="h2"))
    assert len(load_registry(path)) == 2


def test_allow_revision_appends_and_latest_collapses(tmp_path):
    path = str(tmp_path / "registry.jsonl")
    append_hypothesis(path, _record())
    revised = _record(status=STATUS_RESOLVED, outcome={"survived": True, "verdict": "SURVIVED"})
    append_hypothesis(path, revised, allow_revision=True)

    assert len(load_registry(path, latest=False)) == 2
    latest = load_registry(path, latest=True)
    assert len(latest) == 1
    assert latest[0]["status"] == STATUS_RESOLVED
    assert latest[0]["outcome"]["survived"] is True
    assert latest[0]["revision"] == 2


def test_ledger_is_jsonl(tmp_path):
    path = tmp_path / "registry.jsonl"
    append_hypothesis(str(path), _record())
    line = path.read_text(encoding="utf-8").strip()
    assert json.loads(line)["hypothesis_id"] == "h1"


# ---------------------------------------------------------------------------
# pre-registration discipline (the point of the registry)
# ---------------------------------------------------------------------------


def test_pre_registration_data_is_refused():
    rec = _record(registered_at_utc="2026-02-01T00:00:00Z")
    with pytest.raises(PreRegistrationDataError, match="before registration"):
        check_no_pre_registration_data(rec, "2026-01-01T00:00:00Z")


def test_data_at_or_after_registration_is_allowed():
    rec = _record(registered_at_utc="2026-02-01T00:00:00Z")
    check_no_pre_registration_data(rec, "2026-02-01T00:00:00Z")  # exact boundary ok
    check_no_pre_registration_data(rec, "2026-03-01T00:00:00Z")


def test_sample_sufficiency_refuses_conclusions_below_min():
    res = sample_sufficiency(10, n_min=30)
    assert res["sufficient"] is False
    assert res["verdict"] == "INSUFFICIENT_SAMPLE"
    assert "未达评估条件" in res["message"]
    assert sample_sufficiency(30, n_min=30)["sufficient"] is True
    assert DEFAULT_MIN_SAMPLE == 30


def test_survival_label_precedence():
    assert survival_label({"status": STATUS_RESOLVED, "outcome": {}}) == 1
    assert survival_label({"status": STATUS_REJECTED, "outcome": {}}) == 0
    assert survival_label({"status": STATUS_REGISTERED, "outcome": {}}) is None
    assert survival_label({"status": STATUS_REJECTED, "outcome": {"survived": True}}) == 1
    assert survival_label({"status": STATUS_RESOLVED, "outcome": {"verdict": "REJECTED"}}) == 0
    assert survival_label({"status": STATUS_REGISTERED, "outcome": {"verdict": "SURVIVED"}}) == 1


def test_registry_stats_counts_status_band_and_survival(tmp_path):
    path = str(tmp_path / "registry.jsonl")
    append_hypothesis(path, _record(hypothesis_id="a"))
    append_hypothesis(
        path,
        _record(
            hypothesis_id="b",
            status=STATUS_RESOLVED,
            outcome={"survived": True},
        ),
    )
    append_hypothesis(
        path,
        _record(hypothesis_id="c", status=STATUS_REJECTED, outcome={"survived": False}),
    )
    stats = registry_stats(load_registry(path))
    assert stats["n_records"] == 3
    assert stats["n_decided"] == 2
    assert stats["n_survived"] == 1
    assert stats["survival_rate"] == 0.5


def test_timestamp_parsing_accepts_z_and_offsets():
    assert parse_utc("2026-01-01T00:00:00Z") == parse_utc("2026-01-01T00:00:00+00:00")
    assert parse_utc("2026-01-01") == parse_utc("2026-01-01T00:00:00Z")
    with pytest.raises(RegistryValidationError):
        parse_utc("not-a-date")


def test_utc_now_iso_is_parseable():
    assert parse_utc(utc_now_iso())
