#!/usr/bin/env python3
"""Append a status/supersede REVISION to an existing hypothesis (append-only, official API).

Why this script exists
----------------------
``scripts/register_hypothesis.py`` refuses a second record with an existing
``hypothesis_id`` (``DuplicateHypothesisError``) and exposes no
``--allow-revision`` flag.  Superseding a registration therefore cannot go
through that entry point, and hand-editing ``registry.jsonl`` is forbidden.  This
script fills the gap using the same validated path the register script uses —
:func:`src.validation.registry.build_record` + :func:`append_hypothesis` with
``allow_revision=True``.  Nothing is written by hand.

What it preserves
-----------------
``registered_at_utc`` and ``evaluation_window_start`` are carried over
UNCHANGED.  A revision must never move the freeze point: resetting
``registered_at_utc`` to "now" would silently discard the pre-registration
window and re-open the exact hole the registry exists to close.

Choosing the void status — read this before using ``--status rejected``
----------------------------------------------------------------------
The default is ``superseded`` (:data:`src.validation.registry.STATUS_SUPERSEDED`)
— a TERMINAL but NON-EVALUATIVE state whose ``survival_label`` is ``None``, so a
documentary retraction enters neither the survived nor the decided tally.
Because it is terminal, ``append_hypothesis`` refuses any further revision on
that id; re-opening a design requires a fresh ``hypothesis_id``.

The other candidates fabricate an evaluation outcome:

======================  =========================  ==============================
``--status``            survival_label             effect
======================  =========================  ==============================
``rejected``            0  ("evaluated, failed")    fabricates a FAILED evaluation
``resolved``            1  ("evaluated, survived")  fabricates a SURVIVED outcome
``superseded``          None (inert)               correct — the default
``proposed``            None (inert)               works, but is a LIVE state, so
                                                   it leaves the line looking
                                                   evaluable
======================  =========================  ==============================

Marking a never-evaluated superseded registration ``rejected`` also breaks the
invariant ``n_decided == #records carrying an outcome`` asserted by
``tests/validation/test_registry_backfill.py``, and injects phantom "failed"
data points into ``scripts/rubric_attribution.py`` — the analysis of which rubric
dimensions predict survival.  So ``rejected``/``resolved`` require
``--allow-misleading-status``.

The reason is recorded on the revision line as ``supersede_reason`` (carried by
the existing free-form ``extra`` mechanism of :func:`build_record` — no parallel
field machinery).  ``--n-trials-basis`` records how the family-level search
width was resolved when it differs from this line's ``search_grid`` product.

Usage::

    poetry run python scripts/supersede_hypothesis.py \
        --hypothesis-id lppls_crypto_bubble_criticality \
        --superseded-by lppls_crypto_bubble_criticality_v2 \
        --reason "F3 damping constant mis-transcribed; corrected before any price data was read" \
        --search-grid-evidence "144 = 4x2x2x3x3; scanned params count, fixed filters do not"
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.validation.registry import (  # noqa: E402
    STATUS_SUPERSEDED,
    VALID_STATUSES,
    RegistryValidationError,
    TerminalStatusError,
    append_hypothesis,
    build_record,
    get_hypothesis,
    utc_now_iso,
)

DEFAULT_REGISTRY = "hypotheses/registry.jsonl"

#: statuses that make survival_label() assert an evaluation outcome that may
#: never have happened
FABRICATING_STATUSES = frozenset({"rejected", "resolved"})


def build_revision(
    record: dict,
    *,
    status: str,
    reason: str,
    superseded_by: str | None = None,
    search_grid_evidence: str | None = None,
    n_trials_basis: str | None = None,
    superseded_at_utc: str | None = None,
    annotate_only: bool = False,
) -> dict:
    """Build a revision of ``record`` that carries the freeze point over unchanged.

    ``annotate_only=True`` attaches a note (and optional search-grid evidence)
    WITHOUT writing supersede markers or a status_history entry — use it to
    record how N was counted on a record that is still live.
    """
    at = superseded_at_utc or utc_now_iso()
    notes = record.get("notes")
    label = "annotation" if annotate_only else "superseded"
    addition = f"[{label} {at}] {reason}"
    if superseded_by:
        addition += f" superseded_by={superseded_by}."
    merged_notes = f"{notes}\n{addition}" if notes else addition

    extra: dict = {}
    if not annotate_only:
        extra["supersede_reason"] = reason
        extra["superseded_at_utc"] = at
    # carry an earlier supersession link forward so the chain stays visible,
    # then let an explicit --superseded-by override it
    if record.get("superseded_by"):
        extra["superseded_by"] = record["superseded_by"]
    if superseded_by:
        extra["superseded_by"] = superseded_by
    if record.get("supersedes"):
        extra["supersedes"] = record["supersedes"]
    if not annotate_only:
        history = list(record.get("status_history") or [])
        history.append({"at_utc": at, "status": status, "reason": reason})
        extra["status_history"] = history
    elif record.get("status_history"):
        extra["status_history"] = record["status_history"]
    if n_trials_basis:
        # how the family-level search width was resolved (may differ from the
        # per-line search_grid product when several lines share one design)
        extra["n_trials_basis"] = n_trials_basis
    elif record.get("n_trials_basis"):
        extra["n_trials_basis"] = record["n_trials_basis"]
    # never write null-valued markers: absence is meaningful in this ledger
    extra = {k: v for k, v in extra.items() if v is not None}

    return build_record(
        hypothesis_id=record["hypothesis_id"],
        statement=record["statement"],
        mechanism=record["mechanism"],
        trigger_definition=record["trigger_definition"],
        resolution_criteria=record["resolution_criteria"],
        data_requirements=record["data_requirements"],
        rubric_answers=record["rubric"]["answers"],
        author=record["author"],
        status=status,
        # FREEZE POINT MUST NOT MOVE
        registered_at_utc=record["registered_at_utc"],
        evaluation_window_start=record["evaluation_window_start"],
        linked_reports=record.get("linked_reports"),
        linked_commits=record.get("linked_commits"),
        outcome=record.get("outcome") or None,
        scoring_note=record["rubric"].get("scoring_note"),
        schema_version=record.get("schema_version"),
        n_trials_planned=record.get("n_trials_planned"),
        search_grid=record.get("search_grid"),
        n_trials_actual=record.get("n_trials_actual"),
        search_grid_evidence=search_grid_evidence or record.get("search_grid_evidence"),
        n_trials_origin=record.get("n_trials_origin"),
        user_override=record.get("user_override"),
        extra={**extra, "notes": merged_notes},
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--registry", default=DEFAULT_REGISTRY)
    p.add_argument("--hypothesis-id", required=True)
    p.add_argument(
        "--status",
        default=None,
        choices=list(VALID_STATUSES),
        help=(
            "new status. Default: keep the record's current status when --annotate, "
            "else 'superseded' (terminal, non-evaluative)."
        ),
    )
    p.add_argument("--reason", required=True, help="why this revision is being appended")
    p.add_argument("--superseded-by", help="hypothesis_id that replaces this one")
    p.add_argument("--search-grid-evidence", help="free-text note recording how N was counted")
    p.add_argument("--n-trials-basis", help="how the family-level search width was resolved")
    p.add_argument(
        "--allow-misleading-status",
        action="store_true",
        help="permit a status that fabricates a survival label (see module docstring)",
    )
    p.add_argument(
        "--annotate",
        action="store_true",
        help="attach the note / search-grid evidence WITHOUT a status change or supersede markers",
    )
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    record = get_hypothesis(args.registry, args.hypothesis_id)
    if record is None:
        print(f"UNKNOWN hypothesis_id '{args.hypothesis_id}' in {args.registry}", file=sys.stderr)
        return 1

    # Resolve the status AFTER loading the record.
    #   --annotate with no --status KEEPS the current status, so a plain
    #   annotation can never move the lifecycle state (this is the exact bug
    #   that flipped a live entry when --annotate defaulted to 'proposed').
    #   An EXPLICIT --status is always authoritative, including under
    #   --annotate, because that is how a wrongly-moved status is restored.
    #   A supersede revision defaults to the terminal, non-evaluative
    #   'superseded'.
    if args.status is None:
        status = record["status"] if args.annotate else STATUS_SUPERSEDED
    else:
        status = args.status

    if status in FABRICATING_STATUSES and not args.allow_misleading_status:
        print(
            f"REFUSED: --status {status} makes survival_label() infer an evaluation "
            "outcome that never happened, and breaks 'n_decided == #records with an "
            "outcome'. Use 'superseded' (the default), or re-run with "
            "--allow-misleading-status if that really is intended (see the module "
            "docstring).",
            file=sys.stderr,
        )
        return 2

    if args.superseded_by:
        if get_hypothesis(args.registry, args.superseded_by) is None:
            print(
                f"REFUSED: --superseded-by '{args.superseded_by}' is not in {args.registry}; "
                "a revision may only point at a registration that exists",
                file=sys.stderr,
            )
            return 1

    try:
        revision = build_revision(
            record,
            status=status,
            reason=args.reason,
            superseded_by=args.superseded_by,
            search_grid_evidence=args.search_grid_evidence,
            n_trials_basis=args.n_trials_basis,
            annotate_only=args.annotate,
        )
    except RegistryValidationError as exc:
        print(f"INVALID revision: {exc}", file=sys.stderr)
        return 1

    if args.dry_run:
        print(json.dumps(revision, ensure_ascii=False, indent=2)[:2500])
        print(f"\n[dry-run] would append revision of {args.hypothesis_id} status={status}")
        return 0

    try:
        written = append_hypothesis(args.registry, revision, allow_revision=True)
    except TerminalStatusError as exc:
        print(f"TERMINAL: {exc}", file=sys.stderr)
        return 1

    print(
        f"OK    {written['hypothesis_id']} revision={written['revision']} "
        f"status={written['status']} registered_at={written['registered_at_utc']} "
        f"(freeze point preserved)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
