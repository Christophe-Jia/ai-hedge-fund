"""Unit tests for the deployment gate (AMBER blocks strategies, not reports)."""

from __future__ import annotations

from src.validation import deployment_gate, deployment_gate_from_checks


def _checks(sig="PASS", win="STABLE", bnd="STABLE", **kwargs):
    out = {
        "significance": {"verdict": sig},
        "multi_window": {"verdict": win, "scope": kwargs.pop("scope", "windows")},
        "boundary": {"verdict": bnd, **kwargs},
    }
    return out


def test_all_three_gates_pass_is_live():
    res = deployment_gate(_checks())
    assert res["verdict"] == "LIVE_ALLOWED"
    assert res["passed"] is True
    assert res["failed_gates"] == []


def test_noise_significance_blocks_deployment():
    """The weekend_gap case: p=0.053 -> NOISE -> AMBER -> must not go live."""
    res = deployment_gate(_checks(sig="NOISE"))
    assert res["verdict"] == "DEPLOYMENT_BLOCKED"
    assert res["failed_gates"] == ["significance"]
    assert "NOISE/INSUFFICIENT" in res["reasons"][0]


def test_missing_significance_blocks_deployment():
    """The GBM case: significance incalculable -> must not be treated as licensed."""
    checks = _checks()
    checks.pop("significance")
    res = deployment_gate(checks)
    assert res["verdict"] == "DEPLOYMENT_BLOCKED"
    assert res["failed_gates"] == ["significance"]


def test_unstable_window_blocks_deployment():
    res = deployment_gate(_checks(win="UNSTABLE"))
    assert res["verdict"] == "DEPLOYMENT_BLOCKED"
    assert res["failed_gates"] == ["window_stability"]
    assert "window stability is UNSTABLE" in res["reasons"][0]


def test_variant_scope_window_gate_is_unresolved():
    res = deployment_gate(_checks(win="STABLE", scope="variants"))
    assert res["verdict"] == "DEPLOYMENT_BLOCKED"
    assert res["failed_gates"] == ["window_stability"]
    assert "unresolved" in res["reasons"][0]


def test_tie_determined_boundary_blocks_deployment():
    res = deployment_gate(_checks(bnd="STABLE", max_boundary_ties=3))
    assert res["verdict"] == "DEPLOYMENT_BLOCKED"
    assert res["failed_gates"] == ["boundary_stability"]
    assert "shared by 3 names" in res["reasons"][0]
    # a single shared name is not a tie-determined cut (that one name is the cut)
    assert deployment_gate(_checks(bnd="STABLE", max_boundary_ties=1))["verdict"] == "LIVE_ALLOWED"


def test_arbitrary_boundary_blocks_deployment():
    res = deployment_gate(_checks(bnd="ARBITRARY", mean_flip_rate=0.57))
    assert res["verdict"] == "DEPLOYMENT_BLOCKED"
    assert res["failed_gates"] == ["boundary_stability"]


def test_all_gates_failing_are_all_reported():
    res = deployment_gate(_checks(sig="FAIL", win="UNSTABLE", bnd="ARBITRARY"))
    assert res["verdict"] == "DEPLOYMENT_BLOCKED"
    assert set(res["failed_gates"]) == {"significance", "window_stability", "boundary_stability"}
    assert len(res["reasons"]) == 3


def test_empty_checks_blocks():
    res = deployment_gate({})
    assert res["verdict"] == "DEPLOYMENT_BLOCKED"
    assert len(res["failed_gates"]) == 3


def test_booleans_are_accepted():
    res = deployment_gate({"significance": True, "multi_window": True, "boundary": True})
    assert res["verdict"] == "LIVE_ALLOWED"


def test_scope_is_recorded_in_the_rule():
    res = deployment_gate(_checks(), label="x")
    assert "LIVE requires" in res["rule"]
    assert res["label"] == "x"


def test_from_checks_maps_audit_names():
    checks = {
        "significance": {"verdict": "NOISE"},
        "multi_window": {"verdict": "STABLE", "scope": "windows"},
        "boundary": {"verdict": "STABLE", "has_exact_ties": True, "max_boundary_ties": 3},
        "red_team": {"verdict": "FAIL"},
    }
    res = deployment_gate_from_checks(checks, label="gbm")
    assert res["verdict"] == "DEPLOYMENT_BLOCKED"
    assert set(res["failed_gates"]) == {"significance", "boundary_stability"}


def test_event_check_can_satisfy_the_significance_gate():
    checks = {"events": {"verdict": "PASS"}, "multi_window": {"verdict": "STABLE"}, "boundary": {"verdict": "STABLE"}}
    assert deployment_gate(checks)["verdict"] == "LIVE_ALLOWED"
    assert deployment_gate({"events": {"verdict": "NOISE"}, "multi_window": {"verdict": "STABLE"}, "boundary": {"verdict": "STABLE"}})["verdict"] == "DEPLOYMENT_BLOCKED"
