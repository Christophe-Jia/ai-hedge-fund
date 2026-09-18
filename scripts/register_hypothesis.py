#!/usr/bin/env python3
"""Register a hypothesis in the append-only registry, with a mandatory rubric score.

Two input modes:

    # from a JSON spec file (a single record object, or {"hypotheses": [...]})
    poetry run python scripts/register_hypothesis.py --spec hypotheses/backfill_specs.json

    # from a JSON spec that is the *only* record
    poetry run python scripts/register_hypothesis.py --spec my_hypothesis.json

    # interactive (prompts every field and every rubric dimension)
    poetry run python scripts/register_hypothesis.py --interactive

Registration is the freeze point: ``registered_at_utc`` (and therefore the
evaluation window) is stamped now unless the spec supplies an explicit
historical timestamp for backfill.  A hypothesis whose rubric is incomplete is
rejected — you may not register a hypothesis you have not scored.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.validation.registry import (  # noqa: E402
    DuplicateHypothesisError,
    RegistryValidationError,
    append_hypothesis,
    build_record,
    load_registry,
    registry_stats,
    utc_now_iso,
)
from src.validation.rubric import BAND_REJECT, DIMENSIONS, score  # noqa: E402

DEFAULT_REGISTRY = "hypotheses/registry.jsonl"


def _record_from_spec(
    spec: dict,
    *,
    author: str | None = None,
    registered_at_utc: str | None = None,
) -> dict:
    """Turn a spec dict into a validated registry record."""
    if "rubric_answers" not in spec and "rubric" in spec:
        # allow a pre-scored rubric blob's answers to be reused
        spec = {**spec, "rubric_answers": spec["rubric"].get("answers", {})}

    missing = [
        k
        for k in (
            "hypothesis_id",
            "statement",
            "mechanism",
            "trigger_definition",
            "resolution_criteria",
            "data_requirements",
            "rubric_answers",
        )
        if k not in spec
    ]
    if missing:
        raise RegistryValidationError(f"spec for hypothesis missing field(s): {missing}")

    reg_at = registered_at_utc or spec.get("registered_at_utc") or utc_now_iso()

    extra = {}
    for key in ("backfill", "notes", "source"):
        if key in spec:
            extra[key] = spec[key]

    # A REJECT-band hypothesis needs an explicit override to be registered.
    # Historical backfill entries were evaluated before the rubric existed, so
    # the override is recorded as such rather than silently granted.
    user_override = spec.get("user_override")
    if user_override is None:
        source_meta = spec.get("source_meta") or {}
        nested_meta = source_meta.get("_meta") if isinstance(source_meta.get("_meta"), dict) else {}
        is_backfill = bool(
            spec.get("backfill") or source_meta.get("backfill") or nested_meta.get("backfill")
        )
        if is_backfill and score(spec["rubric_answers"]).band == BAND_REJECT:
            user_override = {
                "reason": "backfill: evaluated before the rubric existed; registered retrospectively",
                "by": "platform-backfill",
            }

    return build_record(
        hypothesis_id=spec["hypothesis_id"],
        statement=spec["statement"],
        mechanism=spec["mechanism"],
        trigger_definition=spec["trigger_definition"],
        resolution_criteria=spec["resolution_criteria"],
        data_requirements=spec["data_requirements"],
        rubric_answers=spec["rubric_answers"],
        author=author or spec.get("author") or "unknown",
        status=spec.get("status", "registered"),
        registered_at_utc=reg_at,
        evaluation_window_start=spec.get("evaluation_window_start", reg_at),
        linked_reports=spec.get("linked_reports"),
        linked_commits=spec.get("linked_commits"),
        outcome=spec.get("outcome"),
        scoring_note=spec.get("scoring_note"),
        user_override=user_override,
        extra=extra,
    )


def _load_specs(path: str) -> list[dict]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "hypotheses" in data:
        meta = {k: v for k, v in data.items() if k != "hypotheses"}
        specs = []
        for h in data["hypotheses"]:
            if meta and "source_meta" not in h:
                h = {**h, "source_meta": meta}
            specs.append(h)
        return specs
    if isinstance(data, dict):
        return [data]
    raise RegistryValidationError(f"unsupported spec shape in {path}")


def _prompt(label: str, default: str | None = None) -> str:
    suffix = f" [{default}]" if default else ""
    while True:
        value = input(f"{label}{suffix}: ").strip()
        if value:
            return value
        if default is not None:
            return default
        print("  (required — please enter a value)")


def _interactive_spec() -> dict:
    print("=== Hypothesis registration ===")
    spec: dict = {
        "hypothesis_id": _prompt("hypothesis_id"),
        "statement": _prompt("statement (falsifiable claim)"),
        "mechanism": _prompt("mechanism (one causal chain)"),
        "trigger_definition": _prompt("trigger_definition (exact, mechanical)"),
        "resolution_criteria": _prompt("resolution_criteria (mechanically decidable)"),
        "data_requirements": _prompt("data_requirements (comma-separated)").split(","),
    }
    print("\n--- Rubric (score every dimension 1 / 3 / 5) ---")
    answers: dict[str, int] = {}
    for d in DIMENSIONS:
        print(f"\n[{d['id']}] {d['name']} — {d['question']}")
        for s in (5, 3, 1):
            print(f"  {s}: {d['criteria'][s]}")
        while True:
            raw = input("  score (1/3/5): ").strip()
            if raw in {"1", "3", "5"}:
                answers[d["id"]] = int(raw)
                break
            print("  must be exactly 1, 3 or 5")
    spec["rubric_answers"] = answers
    spec["scoring_note"] = input("scoring_note (optional): ").strip() or None

    res = score(answers)
    print(f"\nweighted_score={res.weighted_score:.3f} band={res.band} ({res.label})")
    if res.band == BAND_REJECT:
        print("REJECT-band registration requires an explicit user_override.")
        spec["user_override"] = {
            "reason": _prompt("override reason"),
            "by": _prompt("override by", default="user"),
        }
    return spec


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--registry", default=DEFAULT_REGISTRY, help="append-only JSONL ledger path")
    parser.add_argument("--spec", help="JSON spec file (single record or {'hypotheses': [...]})")
    parser.add_argument("--author", help="override author for all records")
    parser.add_argument("--registered-at-utc", help="override registered_at_utc (backfill)")
    parser.add_argument("--interactive", action="store_true", help="prompt for every field interactively")
    parser.add_argument("--dry-run", action="store_true", help="validate and print, do not append")
    args = parser.parse_args(argv)

    if not args.spec and not args.interactive:
        parser.error("one of --spec or --interactive is required")

    if args.interactive:
        specs = [_interactive_spec()]
    else:
        specs = _load_specs(args.spec)

    n_ok = 0
    for spec in specs:
        record = _record_from_spec(
            spec, author=args.author, registered_at_utc=args.registered_at_utc
        )
        rubric = record["rubric"]
        if args.dry_run:
            print(
                f"[dry-run] {record['hypothesis_id']}: "
                f"score={rubric['weighted_score']:.3f} band={rubric['band']} "
                f"status={record['status']}"
            )
            n_ok += 1
            continue
        try:
            append_hypothesis(args.registry, record)
        except DuplicateHypothesisError as exc:
            print(f"SKIP  {record['hypothesis_id']}: {exc}", file=sys.stderr)
            continue
        print(
            f"OK    {record['hypothesis_id']}: score={rubric['weighted_score']:.3f} "
            f"band={rubric['band']} registered_at={record['registered_at_utc']}"
        )
        n_ok += 1

    stats = registry_stats(load_registry(args.registry))
    print(
        f"\nregistry={args.registry}  records={stats['n_records']}  "
        f"by_status={stats['by_status']}  by_band={stats['by_band']}  "
        f"survived={stats['n_survived']}/{stats['n_decided']}"
    )
    return 0 if n_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
