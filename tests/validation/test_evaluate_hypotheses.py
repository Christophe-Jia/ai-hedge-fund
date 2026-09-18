"""Tests for the evaluation script: pre-registration refusal and sample sufficiency.

The centrepiece is ``test_uses_only_data_after_registration`` — the mechanical
line against look-ahead / post-hoc window shopping.
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

from src.validation.registry import (
    PreRegistrationDataError,
    append_hypothesis,
    build_record,
)
from src.validation.rubric import DIMENSIONS

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "evaluate_hypotheses.py"

_spec = importlib.util.spec_from_file_location("evaluate_hypotheses", SCRIPT)
evaluate_hypotheses = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(evaluate_hypotheses)

evaluate_request = evaluate_hypotheses.evaluate_request
run = evaluate_hypotheses.run

_ANSWERS = {d["id"]: 3 for d in DIMENSIONS}


def _record(hypothesis_id="pending_signal", registered="2026-02-01T00:00:00Z", **overrides):
    return build_record(
        hypothesis_id=hypothesis_id,
        statement="X predicts Y",
        mechanism="X -> Y",
        trigger_definition="when X > 0",
        resolution_criteria="|t| >= 2",
        data_requirements=["prices"],
        rubric_answers=dict(_ANSWERS),
        registered_at_utc=registered,
        **overrides,
    )


# ---------------------------------------------------------------------------
# the hard rule
# ---------------------------------------------------------------------------


def test_uses_only_data_after_registration():
    """A request over data from before registration must be refused, not scored."""
    rec = _record(registered="2026-02-01T00:00:00Z")
    req = {
        "hypothesis_id": "pending_signal",
        "data_start_utc": "2026-01-15T00:00:00Z",  # BEFORE registration
        "n": 100,
        "metric": "sharpe",
        "value": 1.2,
    }
    with pytest.raises(PreRegistrationDataError):
        evaluate_request(rec, req)


def test_run_reports_pre_registration_rejection_and_exit_code():
    rec = _record(registered="2026-02-01T00:00:00Z")
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        p = str(Path(td) / "registry.jsonl")
        append_hypothesis(p, rec)
        report = run(
            p,
            [
                {
                    "hypothesis_id": "pending_signal",
                    "data_start_utc": "2026-01-15T00:00:00Z",
                    "n": 100,
                }
            ],
        )
    assert report["n_pre_registration_rejected"] == 1
    assert report["results"][0]["status"] == "PRE_REGISTRATION_REJECTED"
    assert report["exit_code"] == 2


def test_boundary_data_equal_to_registration_is_allowed():
    rec = _record(registered="2026-02-01T00:00:00Z")
    res = evaluate_request(
        rec,
        {"hypothesis_id": "pending_signal", "data_start_utc": "2026-02-01T00:00:00Z", "n": 50},
    )
    assert res["status"] == "EVALUATED"


# ---------------------------------------------------------------------------
# sample sufficiency
# ---------------------------------------------------------------------------


def test_insufficient_sample_refuses_a_verdict():
    rec = _record()
    res = evaluate_request(
        rec,
        {
            "hypothesis_id": "pending_signal",
            "data_start_utc": "2026-03-01T00:00:00Z",
            "n": 12,
            "n_min": 30,
            "metric": "sharpe",
            "value": 2.5,  # even a great-looking value must not become a verdict
        },
    )
    assert res["status"] == "INSUFFICIENT_SAMPLE"
    assert res["verdict"] is None
    assert "未达评估条件" in res["message"]


def test_sufficient_sample_returns_a_verdict():
    rec = _record()
    res = evaluate_request(
        rec,
        {"hypothesis_id": "pending_signal", "data_start_utc": "2026-03-01T00:00:00Z", "n": 45, "n_min": 30,
         "metric": "sharpe", "value": 0.9},
    )
    assert res["status"] == "EVALUATED"
    assert res["verdict"] == "EVALUATED"
    assert res["value"] == 0.9


# ---------------------------------------------------------------------------
# misc
# ---------------------------------------------------------------------------


def test_unknown_hypothesis_is_flagged():
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        p = str(Path(td) / "registry.jsonl")
        append_hypothesis(p, _record())
        report = run(p, [{"hypothesis_id": "does_not_exist", "data_start_utc": "2026-03-01T00:00:00Z", "n": 50}])
    assert report["results"][0]["status"] == "UNKNOWN_HYPOTHESIS"
    assert report["exit_code"] == 2


def test_cli_refuses_pre_registration_data(tmp_path):
    registry = tmp_path / "registry.jsonl"
    append_hypothesis(str(registry), _record(registered="2026-02-01T00:00:00Z"))
    spec_path = tmp_path / "requests.json"
    spec_path.write_text(
        json.dumps(
            [
                {
                    "hypothesis_id": "pending_signal",
                    "data_start_utc": "2026-01-01T00:00:00Z",
                    "n": 100,
                }
            ]
        ),
        encoding="utf-8",
    )
    out = tmp_path / "eval.json"
    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--registry",
            str(registry),
            "--spec",
            str(spec_path),
            "--out",
            str(out),
        ],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 2, proc.stderr
    report = json.loads(out.read_text(encoding="utf-8"))
    assert report["n_pre_registration_rejected"] == 1


def test_cli_accepts_post_registration_sufficient_data(tmp_path):
    registry = tmp_path / "registry.jsonl"
    append_hypothesis(str(registry), _record(registered="2026-02-01T00:00:00Z"))
    spec_path = tmp_path / "requests.json"
    spec_path.write_text(
        json.dumps(
            [
                {
                    "hypothesis_id": "pending_signal",
                    "data_start_utc": "2026-03-01T00:00:00Z",
                    "n": 100,
                    "metric": "sharpe",
                    "value": 1.0,
                }
            ]
        ),
        encoding="utf-8",
    )
    out = tmp_path / "eval.json"
    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--registry",
            str(registry),
            "--spec",
            str(spec_path),
            "--out",
            str(out),
        ],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    report = json.loads(out.read_text(encoding="utf-8"))
    assert report["n_evaluated"] == 1
    assert report["exit_code"] == 0
