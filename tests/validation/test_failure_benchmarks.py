"""Meta-test: the failure-benchmark flywheel.

This test is *driven by the ledger*, not hard-coded per case.  For every record
in ``tests/validation/benchmarks/failure_benchmarks.json`` it re-runs the named
framework detector and asserts it emits the expected verdict.  One benchmark =
one assertion; a failing assertion prints the ``failure_mode`` and the detector
so the break is immediately locatable.

It also asserts the honest inverse: each entry in ``known_gaps`` is *still open*
(no detector catches it).  The day a gap is closed, this test fails and thereby
forces the gap to graduate into a real benchmark — that is the flywheel turning
in both directions.

Adding a benchmark is a data edit, not a code edit::

    poetry run python scripts/add_failure_benchmark.py --spec my_benchmark.json
"""

from __future__ import annotations

import json

import pytest

from src.validation.failure_benchmarks import (
    BENCHMARKS_PATH,
    REPORTS_DIR,
    describe_failure,
    iter_benchmarks,
    iter_known_gaps,
    load_benchmark_file,
    run_benchmark,
    validate_spec,
)

LEDGER = load_benchmark_file()
BENCHMARKS = iter_benchmarks()
KNOWN_GAPS = iter_known_gaps()
_BENCHMARK_IDS = [b.get("id", f"benchmark-{i}") for i, b in enumerate(BENCHMARKS)]
_GAP_IDS = [g.get("id", f"gap-{i}") for i, g in enumerate(KNOWN_GAPS)]


# ---------------------------------------------------------------------------
# the core replay: one benchmark = one assertion
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("spec", BENCHMARKS, ids=_BENCHMARK_IDS)
def test_failure_benchmark_is_caught(spec: dict) -> None:
    result = run_benchmark(spec)
    detail = "\n".join(
        [
            "FAILURE BENCHMARK NOT CAUGHT",
            f"  id              = {spec.get('id')}",
            f"  failure_mode    = {spec.get('failure_mode')}",
            f"  detector        = {spec.get('detector')}",
            f"  expected_verdict= {spec.get('expected_verdict')!r}",
            f"  {describe_failure(result, spec)}",
        ]
    )
    assert result["passed"], detail


@pytest.mark.parametrize("spec", KNOWN_GAPS, ids=_GAP_IDS)
def test_known_gap_is_still_open(spec: dict) -> None:
    """A documented hole no detector closes yet.

    If this starts passing, the gap has been *closed* — promote it from
    ``known_gaps`` to ``benchmarks`` (the detector now catches it) so the fix is
    locked in forever.
    """
    result = run_benchmark(spec)
    assert result.get("phase") == "assert", (
        f"known_gap '{spec.get('id')}' does not even run: "
        f"{describe_failure(result, spec)}"
    )
    assert not result["passed"], (
        f"KNOWN GAP CLOSED: '{spec.get('id')}' is now caught by {spec.get('detector_tried')}.\n"
        f"  failure_mode = {spec.get('failure_mode')}\n"
        "  Action: move this record from 'known_gaps' to 'benchmarks' in "
        f"{BENCHMARKS_PATH.relative_to(BENCHMARKS_PATH.parents[3])} so the fix is regression-locked."
    )


# ---------------------------------------------------------------------------
# ledger integrity — the flywheel is only as good as its records
# ---------------------------------------------------------------------------

def test_ledger_exists_and_is_nonempty() -> None:
    assert BENCHMARKS_PATH.exists(), f"failure-benchmark ledger missing: {BENCHMARKS_PATH}"
    assert BENCHMARKS, "the ledger has no benchmarks — the flywheel is not wired up"
    # every record must be JSON-serialisable without NaN (strict ledger)
    json.dumps(load_benchmark_file(), allow_nan=False)


def test_every_benchmark_is_structurally_valid() -> None:
    problems: list[str] = []
    for spec in BENCHMARKS:
        for issue in validate_spec(spec):
            problems.append(f"{spec.get('id', '?')}: {issue}")
    assert not problems, "invalid benchmark records:\n  " + "\n  ".join(problems)


def test_benchmark_ids_are_unique() -> None:
    ids = [b["id"] for b in BENCHMARKS]
    assert len(ids) == len(set(ids)), f"duplicate benchmark ids: {ids}"
    gap_ids = [g["id"] for g in KNOWN_GAPS]
    assert not (set(ids) & set(gap_ids)), "an id appears in both benchmarks and known_gaps"


def test_every_benchmark_names_a_real_report_when_it_reads_one() -> None:
    """Evidence must point at a report that exists (or be explicitly synthetic)."""
    missing: list[str] = []
    for spec in BENCHMARKS + KNOWN_GAPS:
        report = (spec.get("input") or {}).get("report")
        if report and not (REPORTS_DIR / report).exists():
            missing.append(f"{spec['id']} -> reports/{report}")
    assert not missing, "benchmarks reference reports that do not exist:\n  " + "\n  ".join(missing)


def test_every_benchmark_has_a_guardian_test() -> None:
    """Each record names the regression test that holds it after the fix."""
    for spec in BENCHMARKS:
        assert spec.get("regression_test"), f"{spec['id']} has no regression_test"
        assert "test_failure_benchmarks" in spec["regression_test"], (
            f"{spec['id']} regression_test must at least include this meta-test"
        )


def test_docs_list_every_benchmark() -> None:
    """docs/failure_benchmarks.md must show every id (single source, one table)."""
    doc_path = BENCHMARKS_PATH.parents[3] / "docs" / "failure_benchmarks.md"
    assert doc_path.exists(), f"missing {doc_path}"
    doc = doc_path.read_text()
    missing = [b["id"] for b in BENCHMARKS if b["id"] not in doc]
    assert not missing, f"benchmarks missing from {doc_path.name}: {missing}"

