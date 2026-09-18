"""Gap 2: the picks artifact must freeze the whole pool score cross-section.

The 2026-09 GBM crisis was boundary churn (adding one row / 21 names flipped a
month's top-10 to a 2/10 overlap) and could not be re-derived because the runner
persisted only the top-N picks.  These tests pin the fix: every run stores
``pool_scores`` + a stable ``pool_hash`` and, when a prior snapshot for the same
month exists, a ``reproducibility`` verdict.

The ledger keeps ``gbm-asof-boundary-churn`` in ``known_gaps`` until two
comparable snapshots accumulate; this file guards the plumbing that makes that
possible.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "run_monthly_gbm.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("run_monthly_gbm", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


rmg = _load_module()


# --- stable_pool_hash -------------------------------------------------------

def test_pool_hash_is_order_independent():
    a = {"AAA": 1.0, "BBB": 2.0, "CCC": 3.0}
    b = {"CCC": 3.0, "AAA": 1.0, "BBB": 2.0}
    assert rmg.stable_pool_hash(a) == rmg.stable_pool_hash(b)


def test_pool_hash_changes_when_a_score_changes():
    a = {"AAA": 1.0, "BBB": 2.0}
    assert rmg.stable_pool_hash(a) != rmg.stable_pool_hash({"AAA": 1.0, "BBB": 2.1})


def test_pool_hash_is_short_and_stable():
    h = rmg.stable_pool_hash({"AAA": 1.0})
    assert isinstance(h, str) and len(h) == 16
    assert h == rmg.stable_pool_hash({"AAA": 1.0})


# --- compare_to_prior_snapshot ----------------------------------------------

def test_no_prior_snapshot_returns_none(tmp_path: Path):
    assert rmg.compare_to_prior_snapshot(tmp_path / "2021-06.json", {"A": 1.0}, 10, "2021-06") is None


def test_legacy_picks_only_prior_detects_the_2_of_10_mismatch(tmp_path: Path):
    """A pre-`pool_scores` artifact is still comparable at top_n."""
    prior = tmp_path / "2021-06.json"
    prior.write_text(json.dumps({
        "generated_at": "2026-09-14T00:00:00Z",
        "picks": [{"symbol": f"S{i}", "score": 10 - i} for i in range(1, 11)],
    }))
    new_pool = {"S1": 10.0, "S2": 9.0}
    new_pool.update({f"N{i}": 8.0 - i * 0.1 for i in range(1, 9)})

    res = rmg.compare_to_prior_snapshot(prior, new_pool, 10, "2021-06")
    assert res is not None
    assert res["verdict"] == "NON_REPRODUCIBLE"
    assert res["overlap_ratio"] == pytest.approx(0.2)  # the 2/10 crisis
    assert res["prior_source"] == "picks_top_n"
    assert res["prior_generated_at"] == "2026-09-14T00:00:00Z"


def test_pool_scores_prior_is_used_when_present(tmp_path: Path):
    prior = tmp_path / "2026-07.json"
    pool = {f"S{i}": float(i) for i in range(1, 11)}
    prior.write_text(json.dumps({"pool_scores": pool, "pool_hash": "deadbeefdeadbeef"}))
    res = rmg.compare_to_prior_snapshot(prior, pool, 10, "2026-07")
    assert res is not None
    assert res["verdict"] == "REPRODUCIBLE"
    assert res["prior_source"] == "pool_scores"
    assert res["prior_pool_hash"] == "deadbeefdeadbeef"


def test_identical_pool_is_reproducible(tmp_path: Path):
    prior = tmp_path / "2026-08.json"
    pool = {"AAA": 3.0, "BBB": 2.0, "CCC": 1.0}
    prior.write_text(json.dumps({"pool_scores": pool}))
    res = rmg.compare_to_prior_snapshot(prior, dict(pool), 3, "2026-08")
    assert res["verdict"] == "REPRODUCIBLE"
    assert res["overlap_ratio"] == 1.0


def test_corrupt_prior_is_safe(tmp_path: Path):
    prior = tmp_path / "2026-05.json"
    prior.write_text("{not json")
    assert rmg.compare_to_prior_snapshot(prior, {"A": 1.0}, 10, "2026-05") is None
