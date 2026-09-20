"""Safety tests for the registry-revision script (scripts/supersede_hypothesis.py).

The script is the only thing in the repo that appends a second record for an
existing ``hypothesis_id``, i.e. the only way the append-only ledger can be
mutated after a freeze.  So the properties that matter are:

1. a revision MUST NOT move the freeze point (``registered_at_utc`` /
   ``evaluation_window_start``), or the pre-registration window would silently
   re-open;
2. a never-evaluated registration must NOT be given a status that makes
   ``survival_label`` fabricate an evaluation outcome (``rejected`` -> 0,
   ``resolved`` -> 1), because that injects phantom data into the rubric
   attribution and breaks ``n_decided == #records with an outcome``;
3. the retraction must be TERMINAL and documented (``superseded`` cannot be
   quietly revised; the reason is recorded on the line);
4. ``--annotate`` must not accidentally change the lifecycle state (the bug this
   script shipped with on first use).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.supersede_hypothesis import build_revision, main
from src.validation.registry import (
    STATUS_SUPERSEDED,
    TerminalStatusError,
    append_hypothesis,
    build_record,
    get_hypothesis,
    load_registry,
    registry_stats,
    survival_label,
)

ANSWERS = {
    "counterparty_arbitrage": 1,
    "mechanism_stateability": 3,
    "base_rate_anchoring": 3,
    "capacity_cost_reality": 1,
    "regime_dependency_declared": 3,
    "decay_logic_monitoring": 3,
    "data_moat": 1,
    "no_chaos_prediction": 3,
    "executability": 3,
    "preregistration": 5,
}


def _seed_registry(path: Path, *, status: str = "registered") -> None:
    """A two-record ledger: one to revise, and one to point --superseded-by at."""
    for hid in ("alpha", "beta"):
        append_hypothesis(
            str(path),
            build_record(
                hypothesis_id=hid,
                statement=f"{hid} claim",
                mechanism=f"{hid} mechanism",
                trigger_definition=f"{hid} trigger",
                resolution_criteria=f"{hid} criteria",
                data_requirements=[f"{hid} data"],
                rubric_answers=ANSWERS,
                author="test",
                status=status,
                registered_at_utc="2026-01-01T00:00:00Z",
                scoring_note="test seed",
                schema_version=2,
                n_trials_planned=4,
                search_grid={"a": 2, "b": 2},
                n_trials_origin="pre_evaluation",
            ),
        )


# ---------------------------------------------------------------------------
# 2. the fabrication guard
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad_status", ["rejected", "resolved"])
def test_fabricating_status_is_refused_without_explicit_override(tmp_path, capsys, bad_status):
    reg = tmp_path / "registry.jsonl"
    _seed_registry(reg)
    rc = main(
        [
            "--registry", str(reg),
            "--hypothesis-id", "alpha",
            "--status", bad_status,
            "--reason", "should be refused",
        ]
    )
    assert rc == 2
    assert "REFUSED" in capsys.readouterr().err
    assert len(load_registry(str(reg), latest=False)) == 2  # nothing appended


def test_default_supersede_does_not_fabricate_a_survival_label(tmp_path):
    reg = tmp_path / "registry.jsonl"
    _seed_registry(reg)
    rc = main(
        [
            "--registry", str(reg),
            "--hypothesis-id", "alpha",
            "--superseded-by", "beta",
            "--reason", "retracted for testing",
        ]
    )
    assert rc == 0
    rec = get_hypothesis(str(reg), "alpha")
    assert rec["status"] == STATUS_SUPERSEDED
    assert survival_label(rec) is None  # inert: neither 0 nor 1
    assert rec["superseded_by"] == "beta"
    assert rec["supersede_reason"] == "retracted for testing"
    assert rec["superseded_at_utc"]
    assert "retracted for testing" in rec["notes"]


def test_superseded_keeps_the_decided_invariant_and_is_visible_in_stats(tmp_path):
    """n_decided must stay equal to the number of records carrying an outcome."""
    reg = tmp_path / "registry.jsonl"
    _seed_registry(reg)
    before = registry_stats(load_registry(str(reg)))
    main(["--registry", str(reg), "--hypothesis-id", "alpha", "--reason", "retracted"])
    after = registry_stats(load_registry(str(reg)))
    records = load_registry(str(reg))
    assert after["n_decided"] == len([r for r in records if r.get("outcome")])
    assert after["n_decided"] == before["n_decided"]
    assert after["n_records"] == before["n_records"]  # same id, latest revision collapses
    # the retraction is surfaced, but NOT as a data point
    assert after["n_superseded"] == before.get("n_superseded", 0) + 1
    assert after["n_survived"] == before["n_survived"]


def test_superseded_is_terminal_and_cannot_be_revised_away(tmp_path, capsys):
    reg = tmp_path / "registry.jsonl"
    _seed_registry(reg)
    assert main(["--registry", str(reg), "--hypothesis-id", "alpha", "--reason", "retract"]) == 0
    before = get_hypothesis(str(reg), "alpha")
    # a second revision, even an annotation, must be refused
    rc = main(
        [
            "--registry", str(reg),
            "--hypothesis-id", "alpha",
            "--annotate", "--status", "registered",
            "--reason", "try to un-retract",
        ]
    )
    assert rc == 1
    assert "TERMINAL" in capsys.readouterr().err
    after = get_hypothesis(str(reg), "alpha")
    assert after["status"] == STATUS_SUPERSEDED
    assert after["revision"] == before["revision"]  # nothing was appended


# ---------------------------------------------------------------------------
# 1. the freeze point must not move
# ---------------------------------------------------------------------------


def test_revision_preserves_the_freeze_point(tmp_path):
    reg = tmp_path / "registry.jsonl"
    _seed_registry(reg)
    original = get_hypothesis(str(reg), "alpha")
    main(["--registry", str(reg), "--hypothesis-id", "alpha", "--reason", "retract"])
    revised = get_hypothesis(str(reg), "alpha")
    assert revised["registered_at_utc"] == original["registered_at_utc"]
    assert revised["evaluation_window_start"] == original["evaluation_window_start"]
    assert revised["revision"] == 2
    # statement / resolution / rubric / search width must survive verbatim
    assert revised["statement"] == original["statement"]
    assert revised["resolution_criteria"] == original["resolution_criteria"]
    assert revised["rubric"] == original["rubric"]
    assert revised["n_trials_planned"] == original["n_trials_planned"]
    assert revised["search_grid"] == original["search_grid"]


def test_build_revision_keeps_the_registration_timestamp():
    rec = build_record(
        hypothesis_id="x",
        statement="s",
        mechanism="m",
        trigger_definition="t",
        resolution_criteria="r",
        data_requirements=["d"],
        rubric_answers=ANSWERS,
        registered_at_utc="2026-01-01T00:00:00Z",
        scoring_note="n",
    )
    rev = build_revision(rec, status=STATUS_SUPERSEDED, reason="why")
    assert rev["registered_at_utc"] == "2026-01-01T00:00:00Z"
    assert rev["evaluation_window_start"] == "2026-01-01T00:00:00Z"


def test_append_hypothesis_refuses_a_second_revision_on_a_terminal_record():
    """The ledger's own guard, independent of this script."""
    from src.validation.registry import _read_lines  # noqa: PLC2701

    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        reg = Path(tmp) / "r.jsonl"
        _seed_registry(reg)
        assert main(["--registry", str(reg), "--hypothesis-id", "alpha", "--reason", "x"]) == 0
        rec = get_hypothesis(str(reg), "alpha")
        with pytest.raises(TerminalStatusError):
            append_hypothesis(str(reg), rec, allow_revision=True)
        assert len(_read_lines(str(reg))) == 3  # alpha x2 + beta, unchanged


# ---------------------------------------------------------------------------
# 4. --annotate semantics
# ---------------------------------------------------------------------------


def test_annotate_without_status_keeps_the_current_status(tmp_path):
    reg = tmp_path / "registry.jsonl"
    _seed_registry(reg)
    rc = main(
        [
            "--registry", str(reg),
            "--hypothesis-id", "alpha",
            "--annotate",
            "--reason", "note only",
            "--search-grid-evidence", "N=4 = 2x2; scanned params count",
            "--n-trials-basis", "registry:n_trials_planned",
        ]
    )
    assert rc == 0
    rec = get_hypothesis(str(reg), "alpha")
    assert rec["status"] == "registered"  # NOT flipped to superseded
    assert "supersede_reason" not in rec
    assert "superseded_at_utc" not in rec
    assert rec["search_grid_evidence"].startswith("N=4")
    assert rec["n_trials_basis"] == "registry:n_trials_planned"
    assert "note only" in rec["notes"]


def test_annotate_with_explicit_status_is_authoritative(tmp_path):
    """The escape hatch used to restore a status that was wrongly moved."""
    reg = tmp_path / "registry.jsonl"
    _seed_registry(reg, status="proposed")
    rc = main(
        [
            "--registry", str(reg),
            "--hypothesis-id", "alpha",
            "--annotate", "--status", "registered",
            "--reason", "restore",
        ]
    )
    assert rc == 0
    assert get_hypothesis(str(reg), "alpha")["status"] == "registered"


# ---------------------------------------------------------------------------
# referential integrity / input validation
# ---------------------------------------------------------------------------


def test_unknown_superseded_by_is_refused(tmp_path, capsys):
    reg = tmp_path / "registry.jsonl"
    _seed_registry(reg)
    rc = main(
        [
            "--registry", str(reg),
            "--hypothesis-id", "alpha",
            "--reason", "points at nothing",
            "--superseded-by", "does_not_exist",
        ]
    )
    assert rc == 1
    assert "REFUSED" in capsys.readouterr().err
    assert get_hypothesis(str(reg), "alpha")["status"] == "registered"


def test_unknown_hypothesis_id_returns_1(tmp_path, capsys):
    reg = tmp_path / "registry.jsonl"
    _seed_registry(reg)
    assert main(["--registry", str(reg), "--hypothesis-id", "nope", "--reason", "x"]) == 1
    assert "UNKNOWN" in capsys.readouterr().err


def test_annotations_accumulate_and_stay_appended(tmp_path):
    reg = tmp_path / "registry.jsonl"
    _seed_registry(reg)
    main(["--registry", str(reg), "--hypothesis-id", "alpha", "--annotate", "--reason", "one"])
    main(["--registry", str(reg), "--hypothesis-id", "alpha", "--annotate", "--reason", "two"])
    rec = get_hypothesis(str(reg), "alpha")
    assert len(rec.get("status_history") or []) == 0  # annotations are not transitions
    assert rec["revision"] == 3
    assert rec["status"] == "registered"
    assert "one" in rec["notes"] and "two" in rec["notes"]
    # append-only: three raw lines for alpha
    raw = [json.loads(line) for line in reg.read_text().splitlines() if line.strip()]
    assert sum(1 for r in raw if r["hypothesis_id"] == "alpha") == 3
