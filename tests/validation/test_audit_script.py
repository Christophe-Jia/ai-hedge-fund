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
