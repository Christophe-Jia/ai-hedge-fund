"""Schema v2 — pre-registered search width (N) on the hypothesis registry.

Invariants only: no "total == N" assertions (the registry is append-only and
grows over time; a magic total is a time bomb).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.validation.registry import (
    SCHEMA_VERSION_CURRENT,
    build_record,
    load_registry,
    search_grid_product,
    validate_record,
)
from src.validation.registry import RegistryValidationError
from src.validation.search_width import (
    BASIS_ACTUAL,
    BASIS_PLANNED,
    BASIS_PLATFORM,
    BASIS_REPORT,
    resolve_n_trials,
)

ROOT = Path(__file__).resolve().parents[2]
REGISTRY = ROOT / "hypotheses" / "registry.jsonl"

_ALL3 = {
    "counterparty_arbitrage": 3,
    "mechanism_stateability": 3,
    "base_rate_anchoring": 3,
    "capacity_cost_reality": 3,
    "regime_dependency_declared": 3,
    "decay_logic_monitoring": 3,
    "data_moat": 3,
    "no_chaos_prediction": 3,
    "executability": 3,
    "preregistration": 3,
}


def _mk(**overrides):
    kwargs = dict(
        hypothesis_id="unit_test_hypothesis",
        statement="A falsifiable claim.",
        mechanism="A -> B -> price.",
        trigger_definition="Mechanical trigger.",
        resolution_criteria="Mechanical criterion.",
        data_requirements=["x"],
        rubric_answers=dict(_ALL3),
    )
    kwargs.update(overrides)
    return build_record(**kwargs)


# ---------------------------------------------------------------------------
# Backward compatibility
# ---------------------------------------------------------------------------

def test_every_committed_record_still_validates():
    """The whole ledger (legacy + prospective) must keep loading and validating.

    This is the real backward-compat invariant: v2 must not retroactively
    reject pre-existing lines that carry no search width.
    """
    records = load_registry(str(REGISTRY))
    assert records, "registry must not be empty"
    for rec in records:
        validate_record(rec)  # raises on any schema violation


def test_legacy_record_needs_no_search_width():
    rec = _mk()
    assert "schema_version" not in rec
    assert "n_trials_planned" not in rec
    validate_record(rec)


# ---------------------------------------------------------------------------
# Enforcement on schema v2
# ---------------------------------------------------------------------------

def test_schema_v2_requires_n_trials_planned():
    with pytest.raises(RegistryValidationError, match="requires n_trials_planned"):
        _mk(schema_version=SCHEMA_VERSION_CURRENT)


def test_schema_v2_record_with_planned_is_accepted():
    rec = _mk(schema_version=SCHEMA_VERSION_CURRENT, n_trials_planned=1)
    assert rec["n_trials_planned"] == 1
    assert rec["schema_version"] == 2


def test_search_grid_must_multiply_to_planned():
    with pytest.raises(RegistryValidationError, match="product 12 != n_trials_planned 8"):
        _mk(
            schema_version=SCHEMA_VERSION_CURRENT,
            n_trials_planned=8,
            search_grid={"thresholds": 3, "targets": 4},
        )


def test_search_grid_product_math():
    assert search_grid_product({"exit_rules": 6, "targets": 3, "thresholds": 7}) == 126
    assert search_grid_product({"only": 1}) == 1
    with pytest.raises(RegistryValidationError):
        search_grid_product({})
    with pytest.raises(RegistryValidationError):
        search_grid_product({"bad": 0})


def test_search_grid_without_planned_raises():
    with pytest.raises(RegistryValidationError, match="requires n_trials_planned"):
        _mk(search_grid={"a": 2, "b": 2})


def test_matching_grid_is_accepted():
    rec = _mk(
        schema_version=SCHEMA_VERSION_CURRENT,
        n_trials_planned=8,
        search_grid={"quadrant": 4, "statistic": 2},
    )
    assert rec["search_grid"] == {"quadrant": 4, "statistic": 2}


def test_actual_differing_from_planned_requires_evidence():
    with pytest.raises(RegistryValidationError, match="requires search_grid_evidence"):
        _mk(
            schema_version=SCHEMA_VERSION_CURRENT,
            n_trials_planned=10,
            n_trials_actual=6,
        )


def test_actual_differing_with_evidence_is_accepted():
    rec = _mk(
        schema_version=SCHEMA_VERSION_CURRENT,
        n_trials_planned=10,
        n_trials_actual=6,
        search_grid_evidence="scripts/backtest_exit_rules.py:74",
    )
    assert rec["n_trials_actual"] == 6


def test_nonpositive_planned_rejected():
    with pytest.raises(RegistryValidationError, match="n_trials_planned must be an int >= 1"):
        _mk(schema_version=SCHEMA_VERSION_CURRENT, n_trials_planned=0)


# ---------------------------------------------------------------------------
# Prospective records carry their declared width (per-record invariant)
# ---------------------------------------------------------------------------

def test_prospective_records_declare_a_consistent_search_width():
    records = {r["hypothesis_id"]: r for r in load_registry(str(REGISTRY))}
    for hid in ("merrill_clock_regime_rotation", "sp500_index_inclusion_effect"):
        rec = records[hid]
        assert rec["schema_version"] == SCHEMA_VERSION_CURRENT
        assert rec["n_trials_planned"] >= 1
        assert rec["search_grid"]
        # the declared decomposition must multiply to the declared N
        assert search_grid_product(rec["search_grid"]) == rec["n_trials_planned"]
        assert rec["n_trials_origin"] == "post_registration_pre_evaluation"
        # no evaluation has happened yet, so no actual N either
        assert "n_trials_actual" not in rec


# ---------------------------------------------------------------------------
# resolve_n_trials priority: actual > planned > report grid > platform
# ---------------------------------------------------------------------------

def test_resolve_prefers_actual_over_planned():
    rec = {"n_trials_actual": 5, "n_trials_planned": 3}
    assert resolve_n_trials(rec) == (5, BASIS_ACTUAL)


def test_resolve_falls_back_to_planned():
    assert resolve_n_trials({"n_trials_planned": 3}) == (3, BASIS_PLANNED)


def test_resolve_uses_report_grid_with_evidence():
    n, basis = resolve_n_trials(
        {}, grid_count_fn=lambda r: (126, "scripts/backtest_exit_rules.py:74")
    )
    assert n == 126
    assert basis == f"{BASIS_REPORT}:scripts/backtest_exit_rules.py:74"


def test_resolve_falls_back_to_platform_constant():
    assert resolve_n_trials({}, platform_n=42) == (42, BASIS_PLATFORM)


def test_resolve_ignores_nonpositive_declarations():
    # a malformed 0 must not be trusted; fall through to the platform constant
    assert resolve_n_trials({"n_trials_planned": 0}, platform_n=7) == (7, BASIS_PLATFORM)
