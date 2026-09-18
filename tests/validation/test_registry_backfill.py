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

# Frozen golden set: the 27 historical backfill entries. Deliberately enumerated
# rather than counted, because the registry is append-only and gains
# *prospective* entries over time — any "total == N" assertion is a time bomb
# (it broke as soon as two prospective hypotheses were registered).
BACKFILL_IDS = frozenset({
    "dca_leverage_policy", "entry_limit_orders", "entry_tranches", "exit_mechanism_family",
    "exit_rules_family", "fomc_decision_day", "fomc_text_hawkishness", "fundamental_factors",
    "funding_absolute", "funding_rolling", "gbm_sp100_selection", "gbm_sp500_expanded",
    "merged_gap_signal", "meta_labeling_weekend", "momentum_m1_m2", "momentum_regime_gate",
    "mvrv_gate", "onchain_stock_basket", "onchain_trade_btc", "orderbook_leading",
    "overnight_gap_market_close", "overnight_gap_only", "polymarket_midprice_leading",
    "score_weighting", "vix_gate", "volume_ratio_confirmation", "weekend_gap",
})


def _backfill_records(records: list[dict]) -> list[dict]:
    """The historical entries: frozen id set, intersected with what is loaded."""
    return [r for r in records if r["hypothesis_id"] in BACKFILL_IDS]


def test_backfill_registry_exists_and_is_valid_jsonl():
    assert REGISTRY.exists(), "hypotheses/registry.jsonl must be committed"
    records = load_registry(str(REGISTRY))
    present = {r["hypothesis_id"] for r in records}
    missing = BACKFILL_IDS - present
    assert not missing, f"historical backfill entries missing from the registry: {sorted(missing)}"
    # append-only: the registry may hold more than the frozen backfill subset
    assert len(records) >= len(BACKFILL_IDS)


def test_every_backfilled_entry_has_a_complete_rubric():
    records = load_registry(str(REGISTRY))
    # rubric completeness applies to EVERY entry, prospective included:
    # the rubric is mandatory at registration time.
    for rec in records:
        answers = rec["rubric"]["answers"]
        assert set(answers) == {d["id"] for d in DIMENSIONS}, rec["hypothesis_id"]
        assert all(v in (1, 3, 5) for v in answers.values())
        assert rec["rubric"].get("scoring_note"), rec["hypothesis_id"]
    # outcome, however, only exists once an entry has been decided — prospective
    # entries are registered with an empty outcome by design.
    for rec in _backfill_records(records):
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
    records = load_registry(str(REGISTRY))
    stats = registry_stats(records)
    backfill = _backfill_records(records)
    # self-consistent instead of magic totals: decided == entries carrying an outcome
    assert stats["n_decided"] == len([r for r in records if r.get("outcome")])
    # the historical fact we care about lives inside the frozen backfill subset
    assert len(backfill) == 27
    assert sum(1 for r in backfill if (r.get("outcome") or {}).get("survived") is True) == 2
    assert sum(1 for r in backfill if (r.get("outcome") or {}).get("survived") is False) == 25
    # every historical entry is resolved or rejected, none left undecided
    assert all(r["outcome"] for r in backfill)
    assert 0 < len(backfill)


def test_attribution_on_real_registry_keeps_the_warning():
    report = rubric_attribution.analyse(load_registry(str(REGISTRY)))
    assert "探索性分析" in report["warning"]
    # invariant: decided entries are exactly those carrying an outcome
    n_decided = len([r for r in load_registry(str(REGISTRY)) if r.get("outcome")])
    assert report["n_decided"] == n_decided
    assert report["n_dimensions"] >= 1
    assert report["best_dimension"] is not None


def test_attribution_flags_scoring_degradation_on_real_registry():
    """Most retro-scored dimensions only take 1-2 values: the key methodological caveat."""
    report = rubric_attribution.analyse(load_registry(str(REGISTRY)))
    deg = report["scoring_degradation"]
    assert deg["n_binary_or_less_dimensions"] >= 5
    assert "退化" in deg["warning"]
    assert "回溯" in deg["warning"]
    # base_rate is the flagged lead, and it too is fragile (only 2 distinct values)
    assert report["best_dimension"] == "base_rate_anchoring"
    assert deg["distinct_score_counts"]["base_rate_anchoring"] <= 3

