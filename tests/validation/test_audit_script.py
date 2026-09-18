"""Tests for the retrospective audit script (severity logic + GBM calibration)."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "validate_reports.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("validate_reports", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


vr = _load_module()


def _entry(checks):
    return {"checks": checks}


def test_severity_green_when_everything_passes():
    checks = {
        "significance": {"verdict": "PASS"},
        "internal_consistency": {"verdict": "CONSISTENT"},
        "multi_window": {"verdict": "STABLE"},
        "boundary": {"verdict": "STABLE", "has_exact_ties": False},
        "neighborhood": {"verdict": "SMOOTH"},
        "red_team": {"verdict": "PASS"},
    }
    assert vr._severity(_entry(checks)) == "GREEN"


def test_severity_red_on_implausible_consistency():
    assert vr._severity(_entry({"internal_consistency": {"verdict": "IMPLAUSIBLE"}})) == "RED"


def test_severity_red_on_significant_negative():
    assert vr._severity(_entry({"significance": {"verdict": "FAIL"}})) == "RED"


def test_severity_amber_on_noise_and_missing_significance():
    assert vr._severity(_entry({"significance": {"verdict": "NOISE"}})) == "AMBER"
    assert vr._severity(_entry({"significance": {"verdict": "INSUFFICIENT"}})) == "AMBER"


def test_severity_red_on_arbitrary_boundary_and_overfit_peak():
    assert vr._severity(_entry({"boundary": {"verdict": "ARBITRARY"}})) == "RED"
    assert vr._severity(_entry({"neighborhood": {"verdict": "OVERFIT"}})) == "RED"


def test_window_scope_flip_is_red_variant_scope_flip_is_amber():
    windows = {"multi_window": {"verdict": "UNSTABLE", "scope": "windows"}}
    variants = {"multi_window": {"verdict": "UNSTABLE", "scope": "variants"}}
    assert vr._severity(_entry(windows)) == "RED"
    assert vr._severity(_entry(variants)) == "AMBER"


def test_missing_checklist_fields_are_amber_not_red():
    assert vr._severity(_entry({"red_team": {"verdict": "FAIL"}})) == "AMBER"
    assert vr._severity(_entry({"red_team": {"verdict": "WARN"}})) == "AMBER"


def test_severity_red_on_non_reproducible_result():
    assert vr._severity(_entry({"reproducibility": {"verdict": "NON_REPRODUCIBLE"}})) == "RED"


def test_severity_amber_on_missing_reproducibility_evidence():
    assert vr._severity(_entry({"reproducibility": {"verdict": "INSUFFICIENT"}})) == "AMBER"


def test_severity_handles_event_checks():
    assert vr._severity(_entry({"events": {"verdict": "FAIL"}})) == "RED"
    assert vr._severity(_entry({"events": {"verdict": "NOISE"}})) == "AMBER"
    assert vr._severity(_entry({"multiple_comparisons": {"verdict": "FAILS"}})) == "AMBER"
    assert vr._severity(_entry({"multiple_comparisons": {"verdict": "SURVIVES"}})) == "GREEN"


def test_platform_search_uses_the_search_count():
    entry = vr.audit_gbm_attribution()
    mc = entry["checks"]["multiple_comparisons"]
    assert mc["n_hypotheses"] == vr.PLATFORM_HYPOTHESES_SEARCHED
    assert mc["verdict"] == "FAILS"
    assert mc["required_t"] > 3.0


def test_gbm_attribution_carries_reproduction_evidence():
    entry = vr.audit_gbm_attribution()
    assert entry["checks"]["reproducibility"]["verdict"] == "REPRODUCIBLE"


def test_flagship_has_no_reproduction_evidence():
    entry = vr.audit_gbm_sp100()
    assert entry["checks"]["reproducibility"]["verdict"] == "INSUFFICIENT"
    assert any("reproducibility" in f for f in entry["red_flags"])


def test_event_level_checks_run_on_real_trades():
    entry = vr.audit_exit_rules()
    events = entry["checks"]["events"]
    assert events["n_events"] == 25
    assert events["verdict"] == "NOISE"
    assert events["win_rate_wilson_low"] < 0.5 < events["win_rate_wilson_high"]
    windows = entry["checks"]["event_windows"]
    assert windows["n_events_total"] == 25
    assert entry["checks"]["multiple_comparisons"]["verdict"] == "FAILS"


def test_meta_label_event_level_check():
    entry = vr.audit_meta_label()
    assert entry["checks"]["events"]["n_events"] == 50
    assert entry["checks"]["events"]["verdict"] == "NOISE"


def test_mc_from_winrate_helper():
    res = vr._mc_from_winrate(13, 19, 8, label="x")
    assert res["win_rate"] == pytest.approx(13 / 19)
    assert res["observed_t"] == pytest.approx(1.6059, abs=1e-3)
    assert res["verdict"] == "FAILS"
    assert res["wilson"][0] < 0.5 < res["wilson"][1]


def test_events_check_reports_missing_data():
    res = vr._events_check([{"foo": 1}], "ret_pct", label="x")
    assert res["verdict"] == "INSUFFICIENT"


def test_reproducibility_check_without_block():
    res = vr._reproducibility_check({}, label="x")
    assert res["verdict"] == "INSUFFICIENT"
    assert "--as-of" in res["note"] or "as-of" in res["note"]


def test_every_audit_entry_carries_a_deployment_gate():
    for fn in vr.AUDITS:
        entry = fn()
        gate = entry.get("deployment_gate")
        assert gate is not None, entry["name"]
        assert gate["verdict"] in {"LIVE_ALLOWED", "DEPLOYMENT_BLOCKED"}
        if gate["verdict"] == "DEPLOYMENT_BLOCKED":
            assert gate["failed_gates"]


def test_flagship_strategy_is_not_deployable():
    """Report triage RED/AMBER is not a licence: the GBM flagship is blocked."""
    gate = vr.audit_gbm_sp100()["deployment_gate"]
    assert gate["verdict"] == "DEPLOYMENT_BLOCKED"
    assert "significance" in gate["failed_gates"]
    assert "boundary_stability" in gate["failed_gates"]  # 2026-10 picks: 3 names share the cut score


def test_event_strategy_with_variant_scope_is_blocked():
    """weekend_gap: NOISE significance + variant-scope window spread -> blocked."""
    gate = vr.audit_exit_rules()["deployment_gate"]
    assert gate["verdict"] == "DEPLOYMENT_BLOCKED"
    assert "significance" in gate["failed_gates"]
    assert "window_stability" in gate["failed_gates"]


def test_clean_makes_strict_json():
    cleaned = vr._clean({"a": float("nan"), "b": float("inf"), "c": [float("-inf")], "d": 1.0})
    assert cleaned == {"a": None, "b": None, "c": [None], "d": 1.0}
    assert json.loads(json.dumps(cleaned, allow_nan=False)) == cleaned


def test_num_rejects_non_finite():
    assert vr._num("1.5") == 1.5
    assert vr._num(None) is None
    assert vr._num("abc") is None
    assert vr._num(float("nan")) is None


def test_sig_records_missing_fields():
    res = vr._sig(0.0082, None, 71, "mean monthly IC", missing=["monthly_ic.std"])
    assert res["verdict"] == "INSUFFICIENT"
    assert res["missing_fields"] == ["monthly_ic.std"]
    assert "monthly_ic.std" in res["note"]


def test_mw_scope_defaults_to_windows():
    res = vr._mw({"a": 1.0, "b": -1.0}, "x")
    assert res["scope"] == "windows"
    assert vr._mw({"a": 1.0}, "x", scope="variants")["scope"] == "variants"


# --- calibration: the framework must mark the GBM crisis -------------------


def test_gbm_flagship_audit_is_red():
    entry = vr.audit_gbm_sp100()
    assert entry["verdict"] == "RED"
    ic = entry["checks"]["internal_consistency"]
    assert ic["verdict"] == "IMPLAUSIBLE"
    assert ic["raw_ratio"] > 10.0
    # the flagship report cannot even compute significance: the fields are absent
    assert entry["checks"]["significance"]["verdict"] == "INSUFFICIENT"
    # exact score ties are visible in the shipped picks
    assert entry["checks"]["boundary"]["has_exact_ties"] is True
    flags = " ".join(entry["red_flags"])
    assert "IC-implied IR" in flags
    assert entry["should_have_caught"]


def test_gbm_attribution_audit_is_red_and_noise():
    entry = vr.audit_gbm_attribution()
    assert entry["verdict"] == "RED"
    assert entry["checks"]["significance"]["verdict"] == "NOISE"
    assert entry["checks"]["multi_window"]["verdict"] == "UNSTABLE"


def test_sp500_audit_windows_flip():
    entry = vr.audit_gbm_sp500()
    assert entry["verdict"] == "RED"
    assert entry["checks"]["multi_window"]["verdict"] == "UNSTABLE"
    assert entry["checks"]["multi_window"]["scope"] == "windows"


def test_exit_mechanism_neighbourhood_is_smooth():
    entry = vr.audit_exit_mechanism()
    assert entry["checks"]["neighborhood"]["verdict"] == "SMOOTH"
    assert entry["checks"]["neighborhood"]["best_params"] == {"k": 12}


def test_all_audits_produce_a_verdict_and_jsonable_output():
    for fn in vr.AUDITS:
        entry = fn()
        assert entry["verdict"] in {"RED", "AMBER", "GREEN", "ERROR"}
        json.dumps(vr._clean(entry), allow_nan=False)


# --- v1.2 robustness battery wiring -----------------------------------------


def test_every_audit_carries_a_robustness_check():
    for fn in vr.AUDITS:
        entry = fn()
        rb = entry["checks"].get("robustness")
        assert rb is not None, entry["name"]
        assert rb["verdict"] in {"ROBUST", "FRAGILE", "SINGLE_EVENT_DRIVEN", "INSUFFICIENT"}


def test_exit_rules_robustness_is_fragile_by_few_winners():
    """The red-team weekend_gap finding is now caught automatically (k=2)."""
    entry = vr.audit_exit_rules()
    rb = entry["checks"]["robustness"]
    assert rb["verdict"] == "FRAGILE"
    assert "FRAGILE_BY_FEW_WINNERS" in rb["flags"]
    assert rb["leave_k_best_out"]["n_best_to_sustain"] == 2
    assert entry["verdict"] == "RED"


def test_robustness_fragile_by_few_winners_maps_to_red():
    checks = {"robustness": {"verdict": "FRAGILE", "flags": ["FRAGILE_BY_FEW_WINNERS"]}}
    assert vr._severity(_entry(checks)) == "RED"


def test_robustness_single_event_driven_maps_to_red():
    checks = {"robustness": {"verdict": "SINGLE_EVENT_DRIVEN", "flags": ["SINGLE_EVENT_DRIVEN"]}}
    assert vr._severity(_entry(checks)) == "RED"


def test_generic_robustness_fragility_is_amber():
    checks = {"robustness": {"verdict": "FRAGILE", "flags": ["UNSTABLE_UNDER_RESAMPLING"]}}
    assert vr._severity(_entry(checks)) == "AMBER"
    assert vr._severity(_entry({"robustness": {"verdict": "INSUFFICIENT"}})) == "AMBER"


def test_robustness_calibration_catches_weekend_gap():
    cal = vr._robustness_calibration()
    assert cal["headline"]["weekend_gap_caught"] is True
    assert cal["headline"]["weekend_gap_n_best_to_sustain"] == 2
    assert cal["meta"]["all_expectations_met"] is True


# --- v1.3 statistical-deflation wiring --------------------------------------


def test_every_audit_carries_a_deflation_check():
    for fn in vr.AUDITS:
        entry = fn()
        df = entry["checks"].get("deflation")
        assert df is not None, entry["name"]
        assert df["verdict"] in {"SURVIVES", "FAILS", "INSUFFICIENT"}, entry["name"]


def test_deflation_fails_map_to_red_only_when_edge_claimed():
    fails_edge = {"deflation": {"verdict": "FAILS", "claims_edge": True}}
    fails_negative = {"deflation": {"verdict": "FAILS", "claims_edge": False}}
    assert vr._severity(_entry(fails_edge)) == "RED"
    assert vr._severity(_entry(fails_negative)) == "AMBER"
    assert vr._severity(_entry({"deflation": {"verdict": "INSUFFICIENT"}})) == "AMBER"


def test_gbm_flagship_deflation_is_flagged():
    entry = vr.audit_gbm_sp100()
    df = entry["checks"]["deflation"]
    assert df["verdict"] == "FAILS"
    assert df["dsr"] <= vr.DSR_THRESHOLD
    assert df["claims_edge"] is True
    assert df["n_trials"] == 8  # INNER_CV_GRID A-H
    assert any("deflation" in f for f in entry["red_flags"])


def test_weekend_gap_long_leg_deflation_uses_the_grounded_grid():
    df = vr.audit_exit_rules()["checks"]["deflation"]
    assert df["verdict"] == "FAILS"
    # 6 exit rules x 3 traded targets x 7 thresholds = 126 (registry has no variant count)
    assert df["n_trials"] == 126
    assert df["n"] == 13


def test_volume_confirm_deflation_is_insufficient_not_fabricated():
    df = vr.audit_volume_confirm()["checks"]["deflation"]
    assert df["verdict"] == "INSUFFICIENT"
    assert df["n_trials"] == 8


def test_deflation_cross_check_agrees_with_bonferroni():
    # reproduce main()'s platform anchor: the best stored |t| is 1.84 over n=38
    cross = vr._platform_deflation_cross_check((1.8373, "gbm_attribution pre-2024", 38, 1.8373))
    assert cross["bonferroni_on_t"]["verdict"] == "FAILS"
    assert cross["dsr_family_level"]["verdict"] == "FAILS"
    assert cross["consistent"] is True


def test_deflation_audit_artifact_shape():
    entries = [fn() for fn in vr.AUDITS]
    art = vr._deflation_audit(entries, (1.8373, "gbm_attribution pre-2024", 38, 1.8373))
    assert art["summary"]["counts"]["SURVIVES"] == 0
    assert art["summary"]["counts"]["INSUFFICIENT"] == 1
    assert art["platform_cross_check"]["consistent"] is True


def test_frequency_footgun_is_flagged_heuristically():
    """exit_mechanism.monthly_sharpe is named like a period figure but is annualised."""
    entry = vr.audit_exit_mechanism()
    df = entry["checks"]["deflation"]
    assert df["frequency_converted"] is True
    assert df["suspicious_frequency_naming"] is True
    assert any("frequency" in f for f in entry["red_flags"])


def test_deflation_audit_records_source_discrepancy_and_tail_finding():
    entries = [fn() for fn in vr.AUDITS]
    art = vr._deflation_audit(entries, (1.8373, "gbm_attribution pre-2024", 38, 1.8373))
    sd = art["meta"]["source_discrepancy"]
    assert sd["exact_values"]["N=10"] == 1.538753
    assert sd["circulating_table"]["N=10"] == 1.50
    assert "formula" in sd["decision"]
    tail = art["meta"]["weekend_gap_tail_finding"]
    assert "platykurtic" in tail["measured"]
    assert "exit_mechanism" in art["summary"]["frequency_suspects"]
