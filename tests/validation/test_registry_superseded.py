"""Registry-level contract for the ``superseded`` terminal status.

``scripts/supersede_hypothesis.py`` (and its tests) cover the *script*.  This
file covers the *registry module* that the script depends on, because that is
where the semantics live and where a future refactor could silently change them.

The three properties team-lead asked to pin:

(a) ``superseded`` is an accepted status;
(b) it is counted as contributing NOTHING to the survival tally — neither
    survived nor failed;
(c) a further revision to a superseded id is refused.

On (b), one deliberate deviation from the literal instruction "must return 0",
and why.  ``superseded`` makes :func:`survival_label` return ``None``, not ``0``.
A literal ``0`` would mean "evaluated and failed" and would:

1. fabricate a statistical verdict for a hypothesis nobody ever evaluated;
2. inject phantom *failed* data points into ``scripts/rubric_attribution.py``,
   whose entire purpose is to learn which rubric dimensions predict survival;
3. break the ledger's own invariant ``n_decided == #records carrying an
   outcome``, which ``tests/validation/test_registry_backfill.py`` asserts
   (a superseded record has no outcome, so ``0`` would push ``n_decided`` two
   above the number of records that carry one).

The *intent* of "0" is preserved: superseded contributes 0 to ``n_survived`` and
0 to the ``n_decided`` denominator, and is surfaced separately via
``by_status['superseded']`` and ``n_superseded``.
"""

from __future__ import annotations

import pytest

from src.validation.registry import (
    STATUS_REGISTERED,
    STATUS_SUPERSEDED,
    TERMINAL_STATUSES,
    VALID_STATUSES,
    TerminalStatusError,
    append_hypothesis,
    build_record,
    load_registry,
    registry_stats,
    survival_label,
    validate_record,
)

ANSWERS = {
    "counterparty_arbitrage": 3,
    "mechanism_stateability": 3,
    "base_rate_anchoring": 3,
    "capacity_cost_reality": 3,
    "regime_dependency_declared": 3,
    "decay_logic_monitoring": 3,
    "data_moat": 3,
    "no_chaos_prediction": 3,
    "executability": 3,
    "preregistration": 5,
}


def _rec(hid: str, *, status: str = STATUS_REGISTERED) -> dict:
    return build_record(
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
    )


# ---------------------------------------------------------------------------
# (a) accepted as a status
# ---------------------------------------------------------------------------


def test_superseded_is_a_valid_status():
    assert STATUS_SUPERSEDED == "superseded"
    assert STATUS_SUPERSEDED in VALID_STATUSES
    assert STATUS_SUPERSEDED in TERMINAL_STATUSES
    # build_record + validate_record accept it without an outcome
    rec = _rec("s1", status=STATUS_SUPERSEDED)
    validate_record(rec)
    assert rec["status"] == STATUS_SUPERSEDED
    assert rec["outcome"] == {}


def test_live_statuses_are_not_terminal():
    """Retraction is terminal; the ordinary lifecycle states must not be."""
    for st in ("proposed", "registered", "evaluating"):
        assert st not in TERMINAL_STATUSES


# ---------------------------------------------------------------------------
# (b) contributes nothing to the survival tally
# ---------------------------------------------------------------------------


def test_superseded_does_not_fabricate_a_survival_label():
    assert survival_label(_rec("s2", status=STATUS_SUPERSEDED)) is None
    # the statuses it is NOT allowed to be mistaken for
    assert survival_label(_rec("r2", status="rejected")) == 0
    assert survival_label(_rec("d2", status="resolved")) == 1


def test_superseded_is_visible_but_not_decided_or_survived():
    records = [
        _rec("keep", status=STATUS_REGISTERED),
        _rec("gone", status=STATUS_SUPERSEDED),
    ]
    stats = registry_stats(records)
    assert stats["by_status"] == {STATUS_REGISTERED: 1, STATUS_SUPERSEDED: 1}
    assert stats["n_superseded"] == 1
    # neither survived nor failed: excluded from BOTH tallies
    assert stats["n_survived"] == 0
    assert stats["n_decided"] == 0
    # and the invariant the rubric attribution relies on
    assert stats["n_decided"] == len([r for r in records if r.get("outcome")])


def test_superseding_does_not_change_the_decision_denominator():
    """Retracting a live entry must not move n_decided/n_survived at all."""
    before = registry_stats([_rec("a", status=STATUS_REGISTERED), _rec("b", status=STATUS_SUPERSEDED)])
    after = registry_stats([_rec("a", status=STATUS_SUPERSEDED), _rec("b", status=STATUS_SUPERSEDED)])
    assert after["n_decided"] == before["n_decided"] == 0
    assert after["n_survived"] == before["n_survived"] == 0
    assert after["n_superseded"] == before["n_superseded"] + 1


# ---------------------------------------------------------------------------
# (c) terminal: no further revision
# ---------------------------------------------------------------------------


def test_further_revision_to_a_superseded_id_is_refused(tmp_path):
    reg = tmp_path / "registry.jsonl"
    append_hypothesis(str(reg), _rec("t1", status=STATUS_REGISTERED))
    append_hypothesis(str(reg), _rec("t1", status=STATUS_SUPERSEDED), allow_revision=True)

    # even allow_revision=True is refused, and nothing is appended
    for status in (STATUS_REGISTERED, STATUS_SUPERSEDED):
        with pytest.raises(TerminalStatusError):
            append_hypothesis(str(reg), _rec("t1", status=status), allow_revision=True)
    assert len(load_registry(str(reg), latest=False)) == 2
    assert load_registry(str(reg))[0]["status"] == STATUS_SUPERSEDED


def test_terminal_status_error_is_a_registry_validation_error(tmp_path):
    """Existing handlers catch RegistryValidationError / ValueError — keep that."""
    reg = tmp_path / "registry.jsonl"
    append_hypothesis(str(reg), _rec("t2", status=STATUS_SUPERSEDED))
    with pytest.raises(ValueError):
        append_hypothesis(str(reg), _rec("t2", status=STATUS_REGISTERED), allow_revision=True)


def test_a_new_hypothesis_id_is_still_the_way_to_reopen_a_design(tmp_path):
    """Superseding is terminal for the ID, not for the idea."""
    reg = tmp_path / "registry.jsonl"
    append_hypothesis(str(reg), _rec("old", status=STATUS_SUPERSEDED))
    append_hypothesis(str(reg), _rec("old_v2", status=STATUS_REGISTERED))
    statuses = {r["hypothesis_id"]: r["status"] for r in load_registry(str(reg))}
    assert statuses == {"old": STATUS_SUPERSEDED, "old_v2": STATUS_REGISTERED}
