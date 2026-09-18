#!/usr/bin/env python
"""Register (or verify) a failure benchmark — the flywheel's entry point.

The workflow this implements (Bridgewater-style "teach" loop, adapted to a
quant research platform):

    discover a failure mode
      -> write a record naming the framework component that must catch it and
         the verdict it must emit
      -> THIS SCRIPT RUNS THE DETECTOR.  If it does not fire, registration is
         refused: no imagined failure modes, no prose-only entries.
      -> append to tests/validation/benchmarks/failure_benchmarks.json
      -> tests/validation/test_failure_benchmarks.py replays the whole ledger
         forever (regression against catastrophic forgetting)

Usage:
    # from a spec file
    poetry run python scripts/add_failure_benchmark.py --spec /tmp/bench.json

    # interactively
    poetry run python scripts/add_failure_benchmark.py --interactive

    # show what is already locked in / re-verify everything
    poetry run python scripts/add_failure_benchmark.py --list
    poetry run python scripts/add_failure_benchmark.py --check

A spec is one JSON object (same shape as a ledger record); see
docs/failure_benchmarks.md for the fields.  `--spec -` reads stdin.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.validation.failure_benchmarks import (  # noqa: E402
    BENCHMARKS_PATH,
    describe_failure,
    load_benchmark_file,
    render_table,
    run_benchmark,
    validate_spec,
)

META_TEST = "tests/validation/test_failure_benchmarks.py::test_failure_benchmark_is_caught"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _git_user() -> str:
    try:
        out = subprocess.run(
            ["git", "config", "user.name"], cwd=ROOT, capture_output=True, text=True, check=True
        )
        return out.stdout.strip() or "agent"
    except Exception:  # noqa: BLE001
        return "agent"


def _read_spec(args) -> dict:
    if args.spec:
        raw = sys.stdin.read() if args.spec == "-" else Path(args.spec).read_text()
        return json.loads(raw)
    return _interactive_spec()


def _interactive_spec() -> dict:
    print("Register a failure benchmark.  detector + expected_verdict are mandatory.\n")

    def ask(field: str, prompt: str, *, required: bool = True, json_field: bool = False):
        while True:
            value = input(f"  {field} ({prompt}): ").strip()
            if not value and not required:
                return None
            if not value:
                print("    required — cannot be blank")
                continue
            if json_field:
                try:
                    return json.loads(value)
                except json.JSONDecodeError as exc:
                    print(f"    must be valid JSON: {exc}")
                    continue
            return value

    print("Tip: 'input' and 'assertions' are JSON.  input kinds:")
    print("  series            {\"kind\":\"series\",\"report\":\"x.json\",\"path\":\"a.b\",\"value_key\":\"ret\"}")
    print("  mapping           {\"kind\":\"mapping\",\"report\":\"x.json\",\"path\":\"a.b\",\"value_path\":\"mean\"}")
    print("  consistency       {\"kind\":\"consistency\",\"report\":\"x.json\",\"ic_path\":..,\"sharpe_path\":..,\"n_months_path\":..}")
    print("  cross_section_picks {\"kind\":\"cross_section_picks\",\"kwargs\":{\"drop_frac\":0.0}}")
    print("  selections        {\"kind\":\"selections\",\"baseline\":{..},\"candidate\":{..}}\n")

    spec = {
        "id": ask("id", "kebab-case unique id"),
        "title": ask("title", "one line"),
        "failure_mode": ask("failure_mode", "what is the disease (one sentence)"),
        "symptom": ask("symptom", "what you can observe"),
        "detector": ask("detector", "dotted framework path, e.g. src.validation.robustness.leave_k_best_out"),
        "expected_verdict": ask("expected_verdict", "concrete verdict, e.g. FRAGILE_BY_FEW_WINNERS"),
        "input": ask("input", "JSON input spec", json_field=True),
        "assertions": ask("assertions", "JSON field assertions", json_field=True),
        "evidence": ask("evidence", "reports/... path or 'synthetic: <why>'"),
        "discovered_by": ask("discovered_by", "who found it", required=False) or _git_user(),
        "discovered_at": ask("discovered_at", "YYYY-MM-DD", required=False) or _today(),
    }
    return spec


def _detector_referenced_in_tests(detector: str) -> bool:
    """Is the detector function referenced anywhere under tests/?"""
    func = detector.rsplit(".", 1)[-1]
    tests_dir = ROOT / "tests"
    for path in tests_dir.rglob("*.py"):
        try:
            if func in path.read_text():
                return True
        except Exception:  # noqa: BLE001
            continue
    return False


def _write_skeleton(detector: str) -> Path:
    func = detector.rsplit(".", 1)[-1]
    path = ROOT / "tests" / "validation" / f"test_{func}.py"
    if path.exists():
        raise FileExistsError(path)
    module = detector.rsplit(".", 1)[0]
    path.write_text(
        f'''"""Skeleton guard for {detector} (auto-generated by add_failure_benchmark.py).

Add assertions that pin the detector's calibration on synthetic known-answer
inputs.  Regression benchmarks are replayed by
tests/validation/test_failure_benchmarks.py; this file is for detector-level
unit tests (exact thresholds, edge cases, determinism).
"""

from __future__ import annotations

from {module} import {func}  # noqa: F401


def test_{func}_placeholder() -> None:
    # TODO: replace with a known-answer assertion.
    assert callable({func})
'''
    )
    return path


def _save(ledger: dict) -> None:
    BENCHMARKS_PATH.parent.mkdir(parents=True, exist_ok=True)
    BENCHMARKS_PATH.write_text(json.dumps(ledger, ensure_ascii=False, indent=2, allow_nan=False) + "\n")


# ---------------------------------------------------------------------------
# commands
# ---------------------------------------------------------------------------

def cmd_list() -> int:
    ledger = load_benchmark_file()
    benchmarks = ledger.get("benchmarks", [])
    gaps = ledger.get("known_gaps", [])
    print(f"ledger: {BENCHMARKS_PATH}")
    print(f"  {len(benchmarks)} benchmarks, {len(gaps)} known gaps\n")
    for line in render_table(benchmarks):
        print(line)
    if gaps:
        print("\nKnown gaps (documented, no detector yet):")
        for g in gaps:
            print(f"  - {g.get('id')}: {g.get('title')}")
    return 0


def cmd_check() -> int:
    ledger = load_benchmark_file()
    benchmarks = ledger.get("benchmarks", [])
    failures = 0
    for spec in benchmarks:
        result = run_benchmark(spec)
        status = "CAUGHT  " if result["passed"] else "NOT CAUGHT"
        print(f"  [{status}] {spec.get('id')}  ({spec.get('detector')})")
        if not result["passed"]:
            failures += 1
            print("      " + describe_failure(result, spec))
    print(f"\n{len(benchmarks) - failures}/{len(benchmarks)} benchmarks caught")
    return 1 if failures else 0


def cmd_add(args) -> int:
    spec = _read_spec(args)
    if not isinstance(spec, dict):
        print("ERROR: spec must be a JSON object", file=sys.stderr)
        return 2

    # mandatory fields are enforced here — detector + expected_verdict included.
    problems = validate_spec(spec)
    if problems:
        print("ERROR: spec is not admissible:", file=sys.stderr)
        for p in problems:
            print(f"  - {p}", file=sys.stderr)
        return 2

    ledger = load_benchmark_file()
    existing = {b.get("id"): b for b in ledger.get("benchmarks", [])}
    if spec["id"] in existing and not args.update:
        print(
            f"ERROR: benchmark '{spec['id']}' already exists. "
            "Pass --update to replace it (the detector is re-verified either way).",
            file=sys.stderr,
        )
        return 2

    # THE GATE: actually run the detector and require it to fire.
    result = run_benchmark(spec)
    if not result["passed"]:
        print(describe_failure(result, spec), file=sys.stderr)
        if result.get("phase") == "assert":
            print(
                "\nRefusing to register: the detector ran but did not emit the expected verdict.\n"
                "  * If the mode is genuinely new, it belongs in 'known_gaps' until the framework covers it.\n"
                "  * If a detector should cover it, fix/extend the detector first.\n"
                "  * If the expected verdict was wrong, correct 'assertions' and retry.",
                file=sys.stderr,
            )
        else:
            print(
                "\nRefusing to register: the detector spec itself is broken (path/input). "
                "Fix the 'detector' or 'input' field and retry.",
                file=sys.stderr,
            )
        return 1

    # fill provenance / guardian defaults without clobbering explicit values
    spec.setdefault("discovered_at", _today())
    spec.setdefault("discovered_by", _git_user())
    spec.setdefault(
        "regression_test", f"{META_TEST}[{spec['id']}]"
    )

    if spec["id"] in existing:
        ledger["benchmarks"] = [spec if b.get("id") == spec["id"] else b for b in ledger["benchmarks"]]
        action = "updated"
    else:
        ledger.setdefault("benchmarks", []).append(spec)
        action = "registered"
    _save(ledger)

    print(f"OK: {action} benchmark '{spec['id']}' -> {BENCHMARKS_PATH.relative_to(ROOT)}")
    print(f"    detector        : {spec['detector']}")
    print(f"    expected_verdict: {spec['expected_verdict']}")
    print(f"    caught          : {result.get('actual_summary')}")
    print(f"    guarded by      : {spec['regression_test']}")

    # the meta-test is the default guardian; a detector with no unit test gets a hint
    if not _detector_referenced_in_tests(spec["detector"]) and not args.no_skeleton:
        func = spec["detector"].rsplit(".", 1)[-1]
        print(
            f"\nNOTE: no test under tests/ references {func}() yet. "
            "The ledger meta-test still guards this benchmark, but a detector-level "
            "known-answer test is recommended."
        )
        if args.skeleton:
            try:
                path = _write_skeleton(spec["detector"])
                print(f"      wrote skeleton: {path.relative_to(ROOT)} — fill in the assertion.")
            except FileExistsError as exc:
                print(f"      skeleton already exists: {exc}")
        else:
            print("      re-run with --skeleton to generate one, or add the assertion by hand.")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Register a failure benchmark after RUNNING its detector.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--spec", help="path to a JSON spec file, or '-' for stdin")
    parser.add_argument("--interactive", action="store_true", help="prompt for the fields")
    parser.add_argument("--list", action="store_true", help="list the current ledger")
    parser.add_argument("--check", action="store_true", help="re-verify every benchmark (exit 1 on failure)")
    parser.add_argument("--update", action="store_true", help="replace an existing benchmark with the same id")
    parser.add_argument("--skeleton", action="store_true", help="write a detector unit-test skeleton if none exists")
    parser.add_argument("--no-skeleton", action="store_true", help="suppress the missing-detector-test hint")
    args = parser.parse_args(argv)

    if args.list:
        return cmd_list()
    if args.check:
        return cmd_check()
    if args.spec or args.interactive:
        return cmd_add(args)
    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
