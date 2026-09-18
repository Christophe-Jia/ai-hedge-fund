"""Integration checks on the committed historical backfill registry."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from src.validation.registry import load_registry, registry_stats, survival_label
from src.validation.rubric import DIMENSIONS

ROOT = Path(__file__).resolve().parents[2]
REGISTRY = ROOT / "hypotheses" / "registry.jsonl"

_spec = importlib.util.spec_from_file_location("rubric_attribution", ROOT / "scripts" / "rubric_attribution.py")
rubric_attribution = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rubric_attribution)


def test_backfill_registry_exists_and_is_valid_jsonl():
    assert REGISTRY.exists(), "hypotheses/registry.jsonl must be committed"
    records = load_registry(str(REGISTRY))
    assert len(records) == 27


def test_every_backfilled_entry_has_a_complete_rubric():
    records = load_registry(str(REGISTRY))
    for rec in records:
        answers = rec["rubric"]["answers"]
        assert set(answers) == {d["id"] for d in DIMENSIONS}, rec["hypothesis_id"]
        assert all(v in (1, 3, 5) for v in answers.values())
        assert rec["rubric"].get("scoring_note"), rec["hypothesis_id"]
        assert rec["outcome"], rec["hypothesis_id"]


def test_weekend_gap_counterparty_is_scored_low_ex_ante():
    """The canonical failure: the counterparty question was never asked."""
    rec = next(r for r in load_registry(str(REGISTRY)) if r["hypothesis_id"] == "weekend_gap")
    assert rec["rubric"]["answers"]["counterparty_arbitrage"] == 1
    assert rec["outcome"]["survived"] is False


def test_gbm_decay_monitoring_is_scored_low_ex_ante():
    """No downgrade red line existed when GBM shipped — scored 1, not back-filled to a pass."""
    rec = next(r for r in load_registry(str(REGISTRY)) if r["hypothesis_id"] == "gbm_sp100_selection")
    assert rec["rubric"]["answers"]["decay_logic_monitoring"] == 1


def test_reject_band_backfill_entries_record_the_user_override():
    """Every REJECT-band historical entry must carry the recorded override."""
    for rec in load_registry(str(REGISTRY)):
        if rec["rubric"]["band"] == "REJECT":
            assert rec.get("user_override"), rec["hypothesis_id"]


def test_registry_survival_tally_is_two_of_twenty_seven():
    stats = registry_stats(load_registry(str(REGISTRY)))
    assert stats["n_records"] == 27
    assert stats["n_decided"] == 27
    assert stats["n_survived"] == 2
    # every historical entry is resolved or rejected, none left undecided
    assert all(survival_label(r) is not None for r in load_registry(str(REGISTRY)))


def test_attribution_on_real_registry_keeps_the_warning():
    report = rubric_attribution.analyse(load_registry(str(REGISTRY)))
    assert "探索性分析" in report["warning"]
    assert report["n_decided"] == 27
    assert report["n_dimensions"] >= 1
    assert report["best_dimension"] is not None
