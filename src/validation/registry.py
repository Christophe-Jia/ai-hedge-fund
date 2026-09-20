"""Hypothesis registry — the mechanical defence against post-hoc selection.

The platform's costly lesson: 27 hypothesis families were evaluated, the best
reached |t| = 1.84, and *none* survived a search correction.  Part of that cost
was paid because hypotheses were formed, revised and thresholded **after** the
data had been seen.  Nothing in the codebase stopped that.

This module makes pre-registration executable:

* an append-only JSONL ledger (``hypotheses/registry.jsonl``);
* every entry carries a UTC registration timestamp and an ``evaluation_window_
  start`` that may not precede it;
* :func:`check_no_pre_registration_data` raises
  :class:`PreRegistrationDataError` if an evaluation touches any data earlier
  than the registration time — the mechanical line against look-ahead and
  after-the-fact window shopping;
* :func:`sample_sufficiency` refuses to emit a verdict below ``n_min``, so a
  handful of events can never be dressed up as a conclusion.

The ledger is append-only by convention: :func:`append_hypothesis` refuses a
second record with the same ``hypothesis_id`` unless ``allow_revision=True``
(status transitions append a new revision line; :func:`load_registry` collapses
to the latest revision per id).
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from .rubric import BAND_REJECT, DIMENSIONS, RubricResult, score

# ---------------------------------------------------------------------------
# Statuses / constants
# ---------------------------------------------------------------------------

STATUS_PROPOSED = "proposed"
STATUS_REGISTERED = "registered"
STATUS_EVALUATING = "evaluating"
STATUS_RESOLVED = "resolved"
STATUS_REJECTED = "rejected"

#: Retracted by a documentary correction to the operationalisation, before any
#: evaluation happened.  This is a TERMINAL, NON-EVALUATIVE state.
#:
#: Why a sixth status was needed: none of the original five fits "the
#: registration was withdrawn before anything was evaluated".
#: ``resolved`` means SURVIVED (:func:`survival_label`) and ``rejected`` means a
#: statistical verdict was rendered — recording either for a hypothesis nobody
#: evaluated would FABRICATE a survival label and inject a phantom data point
#: into the rubric attribution (which exists to learn which dimensions predict
#: survival).  ``proposed``/``registered`` are live states, so they leave a
#: superseded line looking evaluable.
STATUS_SUPERSEDED = "superseded"

VALID_STATUSES: tuple[str, ...] = (
    STATUS_PROPOSED,
    STATUS_REGISTERED,
    STATUS_EVALUATING,
    STATUS_RESOLVED,
    STATUS_REJECTED,
    STATUS_SUPERSEDED,
)

#: Statuses that may not receive any further revision.
TERMINAL_STATUSES: tuple[str, ...] = (STATUS_SUPERSEDED,)

#: Fields every registry record must carry (``outcome`` may be empty until the
#: hypothesis is resolved).
REQUIRED_FIELDS: tuple[str, ...] = (
    "hypothesis_id",
    "registered_at_utc",
    "author",
    "status",
    "statement",
    "mechanism",
    "trigger_definition",
    "resolution_criteria",
    "evaluation_window_start",
    "data_requirements",
    "rubric",
)

#: Verdict strings that count as "survived" in the attribution analysis.
SURVIVED_VERDICTS: frozenset[str] = frozenset(
    {"SURVIVED", "PASS", "PASSED", "ADOPTED", "INCUMBENT", "RETAINED"}
)

DEFAULT_MIN_SAMPLE = 30

# ---------------------------------------------------------------------------
# Search width (schema v2) — pre-registered N for DSR deflation
# ---------------------------------------------------------------------------
#
# ``evaluation_window_start`` freezes the *time* dimension of a hypothesis;
# search width freezes the *multiplicity* dimension.  Both are the same
# discipline: pick them before you look.  ``n_trials_planned`` is the number of
# trials the author commits to searching; ``search_grid`` is the
# machine-checkable decomposition that must multiply to it.
#
# Backward compatibility: NONE of these fields is in ``REQUIRED_FIELDS`` —
# the 29 pre-existing ledger lines have no search width and must keep loading.
# Enforcement keys off an explicit ``schema_version`` marker, so a record only
# becomes subject to the rule when it declares itself v2.
#
# Bias direction (deliberate — record it so no one can reopen a failed result):
# the variants in a real search grid are highly correlated, so the raw count is
# an OVER-estimate of the number of *independent* trials.  A larger N raises the
# DSR noise ceiling, i.e. the test is too strict, not too lenient.  "Too strict,
# never too lenient" is the safe direction for a deflation gate:
#
#   correlated variants -> raw N over-states independent trials
#                       -> DSR bar too high -> a "fail" is safe, a "pass" is real.
#
# Therefore raw N is the default and MUST NOT be quietly swapped for a smaller
# "effective N" to turn a failure into a pass.  The effective count may only be
# used when it is *reproducible* — the trial return matrix is stored so LdP's
# eigenvalue refinement ``(sum(sqrt(lambda)))**2 / sum(lambda)`` can be
# recomputed — and even then only as an explicit, labelled refinement.  "The
# effective N is smaller so it actually passes" without a stored matrix is just
# post-hoc choice of the pass line again: the exact failure mode this registry
# exists to prevent.

SCHEMA_VERSION_LEGACY = 1
SCHEMA_VERSION_CURRENT = 2

#: Optional search-width fields (never required globally — see module note).
SEARCH_WIDTH_FIELDS: tuple[str, ...] = (
    "schema_version",
    "n_trials_planned",
    "search_grid",
    "n_trials_actual",
    "search_grid_evidence",
    "n_trials_origin",
)

__all__ = [
    "STATUS_PROPOSED",
    "STATUS_REGISTERED",
    "STATUS_EVALUATING",
    "STATUS_RESOLVED",
    "STATUS_REJECTED",
    "STATUS_SUPERSEDED",
    "VALID_STATUSES",
    "TERMINAL_STATUSES",
    "REQUIRED_FIELDS",
    "SURVIVED_VERDICTS",
    "DEFAULT_MIN_SAMPLE",
    "SCHEMA_VERSION_LEGACY",
    "SCHEMA_VERSION_CURRENT",
    "SEARCH_WIDTH_FIELDS",
    "RegistryError",
    "RegistryValidationError",
    "DuplicateHypothesisError",
    "TerminalStatusError",
    "PreRegistrationDataError",
    "utc_now_iso",
    "parse_utc",
    "search_grid_product",
    "build_record",
    "validate_record",
    "append_hypothesis",
    "load_registry",
    "get_hypothesis",
    "check_no_pre_registration_data",
    "sample_sufficiency",
    "survival_label",
    "registry_stats",
]


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class RegistryError(Exception):
    """Base class for registry errors."""


class RegistryValidationError(RegistryError, ValueError):
    """A record is missing required fields or holds an invalid value."""


class DuplicateHypothesisError(RegistryError):
    """A record with this ``hypothesis_id`` is already registered."""


class TerminalStatusError(RegistryValidationError):
    """A revision was attempted on an id whose latest revision is terminal.

    Subclasses :class:`RegistryValidationError` (and therefore ``ValueError``) so
    existing handlers keep working; it is separate because "this retraction is
    final" is a different fact from "this record is malformed".
    """


class PreRegistrationDataError(RegistryError, ValueError):
    """An evaluation tried to use data from before the hypothesis was registered."""


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------

_ACCEPTED_FORMATS: tuple[str, ...] = (
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
)


def utc_now_iso() -> str:
    """Current UTC time as a sortable ``YYYY-MM-DDTHH:MM:SSZ`` string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_utc(value: Any) -> datetime:
    """Parse a registry timestamp into an aware UTC datetime.

    Accepts ``...Z``, ISO-8601 with offset, and date-only forms.  Anything else
    raises :class:`RegistryValidationError`.
    """
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        text = value.strip()
        dt = None
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            for fmt in _ACCEPTED_FORMATS:
                try:
                    dt = datetime.strptime(text, fmt)
                    break
                except ValueError:
                    continue
            if dt is None:
                raise RegistryValidationError(f"unparseable UTC timestamp: {value!r}")
    else:
        raise RegistryValidationError(f"unparseable UTC timestamp: {value!r}")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# Search width validation (schema v2)
# ---------------------------------------------------------------------------


def search_grid_product(search_grid: Mapping[str, Any]) -> int:
    """Product of a ``search_grid``'s leaf values (the implied trial count).

    Raises :class:`RegistryValidationError` for a non-mapping, an empty grid,
    or a non-positive-integer leaf.  This is the machine-checkable half of the
    search-width rule: a decomposition that does not multiply to
    ``n_trials_planned`` is rejected rather than trusted.
    """
    if not isinstance(search_grid, Mapping) or not search_grid:
        raise RegistryValidationError("search_grid must be a non-empty object[str, int]")
    product = 1
    for dim, size in search_grid.items():
        if not isinstance(dim, str) or not dim.strip():
            raise RegistryValidationError(f"search_grid keys must be non-empty strings, got {dim!r}")
        if isinstance(size, bool) or not isinstance(size, int) or size < 1:
            raise RegistryValidationError(
                f"search_grid['{dim}'] must be an int >= 1, got {size!r}"
            )
        product *= size
    return product


def _require_positive_int(field: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise RegistryValidationError(f"{field} must be an int >= 1, got {value!r}")
    return value


def _validate_search_width(record: Mapping[str, Any]) -> None:
    """Validate the optional schema-v2 search-width fields.

    All fields are optional for legacy records.  ``schema_version >= 2``
    triggers the requirement that ``n_trials_planned`` be present; every other
    field is validated only when it appears (so a legacy line still loads).
    """
    sv = record.get("schema_version")
    if sv is not None:
        _require_positive_int("schema_version", sv)

    planned = record.get("n_trials_planned")
    grid = record.get("search_grid")
    actual = record.get("n_trials_actual")
    evidence = record.get("search_grid_evidence")
    origin = record.get("n_trials_origin")

    if sv is not None and sv >= SCHEMA_VERSION_CURRENT and planned is None:
        raise RegistryValidationError(
            f"schema_version {sv} requires n_trials_planned (a single confirmatory "
            "test is N=1, not absent) — declare the search width at registration"
        )

    if planned is not None:
        _require_positive_int("n_trials_planned", planned)

    if grid is not None:
        if planned is None:
            raise RegistryValidationError(
                "search_grid requires n_trials_planned to check the product against"
            )
        product = search_grid_product(grid)
        if product != planned:
            factor = product / planned
            raise RegistryValidationError(
                f"search_grid product {product} != n_trials_planned {planned} "
                f"(factor {factor:.6g}); the decomposition must multiply to N"
            )

    if actual is not None:
        _require_positive_int("n_trials_actual", actual)
        if planned is not None and actual != planned and not evidence:
            raise RegistryValidationError(
                f"n_trials_actual ({actual}) != n_trials_planned ({planned}) requires "
                "search_grid_evidence (otherwise a changed N looks like post-hoc "
                "narrowing of the pass line)"
            )

    if evidence is not None and not isinstance(evidence, str):
        raise RegistryValidationError("search_grid_evidence must be a string when present")

    if origin is not None and not isinstance(origin, str):
        raise RegistryValidationError("n_trials_origin must be a string when present")


# ---------------------------------------------------------------------------
# Record construction / validation
# ---------------------------------------------------------------------------


def build_record(
    *,
    hypothesis_id: str,
    statement: str,
    mechanism: str,
    trigger_definition: str,
    resolution_criteria: str,
    data_requirements: Any,
    rubric_answers: Mapping[str, int],
    author: str = "unknown",
    status: str = STATUS_REGISTERED,
    registered_at_utc: str | None = None,
    evaluation_window_start: str | None = None,
    linked_reports: Iterable[str] | None = None,
    linked_commits: Iterable[str] | None = None,
    outcome: Mapping[str, Any] | None = None,
    scoring_note: str | None = None,
    weights: Mapping[str, float] | None = None,
    schema_version: int | None = None,
    n_trials_planned: int | None = None,
    search_grid: Mapping[str, int] | None = None,
    n_trials_actual: int | None = None,
    search_grid_evidence: str | None = None,
    n_trials_origin: str | None = None,
    user_override: Any = None,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build and validate a registry record, scoring the rubric answers.

    Registration is the moment the hypothesis is frozen: ``registered_at_utc``
    and ``evaluation_window_start`` are set to *now* unless the caller supplies
    an explicit (historical) time for backfill.  The rubric must be **complete**
    — a hypothesis with un-scored dimensions must not be registered.

    A rubric band of ``REJECT`` may only be registered with an explicit
    ``user_override`` (per the rubric contract: spending evaluation budget on a
    rejected hypothesis requires recording that the user forced it).

    Schema v2 (search width): pass ``schema_version=2`` to declare the record
    subject to the pre-registered-N rule, then ``n_trials_planned`` is required
    and ``search_grid`` (if given) must multiply to it.  Legacy records that
    pass neither field are untouched.
    """
    result = score(rubric_answers, weights=weights)
    if result.missing_dimensions:
        raise RegistryValidationError(
            "rubric must be complete to register a hypothesis; missing: "
            + ", ".join(result.missing_dimensions)
        )
    if result.band == BAND_REJECT and user_override is None:
        raise RegistryValidationError(
            "rubric band is REJECT — registration requires an explicit "
            "user_override (e.g. {'reason': '...', 'by': 'user'}); "
            "do not spend evaluation budget on a rejected hypothesis silently"
        )

    registered = registered_at_utc or utc_now_iso()
    window_start = evaluation_window_start or registered
    # evaluation window may not start before registration
    if parse_utc(window_start) < parse_utc(registered):
        raise RegistryValidationError(
            f"evaluation_window_start ({window_start}) may not precede "
            f"registered_at_utc ({registered})"
        )

    rubric_blob = result.as_dict()
    if scoring_note:
        rubric_blob["scoring_note"] = scoring_note

    record: dict[str, Any] = {
        "hypothesis_id": hypothesis_id,
        "registered_at_utc": registered,
        "author": author,
        "status": status,
        "statement": statement,
        "mechanism": mechanism,
        "trigger_definition": trigger_definition,
        "resolution_criteria": resolution_criteria,
        "evaluation_window_start": window_start,
        "data_requirements": data_requirements,
        "rubric": rubric_blob,
        "linked_reports": list(linked_reports or []),
        "linked_commits": list(linked_commits or []),
        "outcome": dict(outcome) if outcome else {},
    }
    if user_override is not None:
        record["user_override"] = user_override
    for key, value in (
        ("schema_version", schema_version),
        ("n_trials_planned", n_trials_planned),
        ("search_grid", dict(search_grid) if search_grid is not None else None),
        ("n_trials_actual", n_trials_actual),
        ("search_grid_evidence", search_grid_evidence),
        ("n_trials_origin", n_trials_origin),
    ):
        if value is not None:
            record[key] = value
    if extra:
        record.update(extra)
    validate_record(record)
    return record


def validate_record(record: Mapping[str, Any]) -> None:
    """Raise :class:`RegistryValidationError` if a record is malformed."""
    if not isinstance(record, Mapping):
        raise RegistryValidationError(f"record must be a mapping, got {type(record).__name__}")

    missing = [f for f in REQUIRED_FIELDS if f not in record]
    if missing:
        raise RegistryValidationError(f"record missing required field(s): {missing}")

    if not str(record["hypothesis_id"]).strip():
        raise RegistryValidationError("hypothesis_id must be a non-empty string")

    status = record["status"]
    if status not in VALID_STATUSES:
        raise RegistryValidationError(
            f"status must be one of {VALID_STATUSES}, got {status!r}"
        )

    for text_field in (
        "statement",
        "mechanism",
        "trigger_definition",
        "resolution_criteria",
    ):
        if not str(record.get(text_field, "")).strip():
            raise RegistryValidationError(f"{text_field} must be a non-empty string")

    registered = parse_utc(record["registered_at_utc"])
    window_start = parse_utc(record["evaluation_window_start"])
    if window_start < registered:
        raise RegistryValidationError(
            "evaluation_window_start may not precede registered_at_utc"
        )

    rubric_blob = record["rubric"]
    if not isinstance(rubric_blob, Mapping):
        raise RegistryValidationError("rubric must be a mapping")
    for key in ("weighted_score", "band", "answers"):
        if key not in rubric_blob:
            raise RegistryValidationError(f"rubric missing '{key}'")
    answers = rubric_blob["answers"]
    if not isinstance(answers, Mapping) or len(answers) != len(DIMENSIONS):
        raise RegistryValidationError(
            f"rubric.answers must score all {len(DIMENSIONS)} dimensions"
        )

    _validate_search_width(record)


# ---------------------------------------------------------------------------
# Ledger I/O
# ---------------------------------------------------------------------------


def _read_lines(path: str) -> list[dict[str, Any]]:
    if not os.path.exists(path):
        return []
    records: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise RegistryValidationError(
                    f"{path}:{lineno} is not valid JSON: {exc}"
                ) from exc
    return records


def load_registry(path: str, *, latest: bool = True) -> list[dict[str, Any]]:
    """Load the ledger.

    With ``latest=True`` (default) only the last revision of each
    ``hypothesis_id`` is returned, in first-registration order.
    """
    records = _read_lines(path)
    if not latest:
        return records
    collapsed: dict[str, dict[str, Any]] = {}
    for rec in records:
        collapsed[rec.get("hypothesis_id")] = rec
    return list(collapsed.values())


def get_hypothesis(path: str, hypothesis_id: str, *, latest: bool = True) -> dict[str, Any] | None:
    for rec in load_registry(path, latest=latest):
        if rec.get("hypothesis_id") == hypothesis_id:
            return rec
    return None


def append_hypothesis(
    path: str,
    record: Mapping[str, Any],
    *,
    allow_revision: bool = False,
) -> dict[str, Any]:
    """Append a validated record to the append-only ledger.

    A second record with an existing ``hypothesis_id`` raises
    :class:`DuplicateHypothesisError` unless ``allow_revision=True`` (used for
    status transitions / outcome backfill of an existing id).

    ``allow_revision=True`` is NOT enough once the id's latest revision is in
    :data:`TERMINAL_STATUSES`: a ``superseded`` line is final and is refused for
    the same reason ``rejected``/``resolved`` outcomes must not be edited — a
    retraction that can be quietly revised is not a retraction.  Appending a
    fresh ``hypothesis_id`` remains the way to re-open a superseded design.
    """
    validate_record(record)
    existing = [
        r for r in _read_lines(path) if r.get("hypothesis_id") == record["hypothesis_id"]
    ]
    if existing and not allow_revision:
        raise DuplicateHypothesisError(
            f"hypothesis_id '{record['hypothesis_id']}' already registered "
            f"(use allow_revision=True for a status/outcome revision)"
        )

    if existing:
        latest = existing[-1]
        latest_status = latest.get("status")
        if latest_status in TERMINAL_STATUSES:
            raise TerminalStatusError(
                f"hypothesis_id '{record['hypothesis_id']}' is in terminal status "
                f"'{latest_status}' (revision {latest.get('revision')}); no further "
                "revision is accepted. Register a new hypothesis_id to re-open the "
                "design, leaving this retraction on the record."
            )

    rec = dict(record)
    if existing:
        rec["revision"] = len(existing) + 1
    rec.setdefault("revision", 1)

    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec, ensure_ascii=False, sort_keys=False) + "\n")
    return rec


# ---------------------------------------------------------------------------
# Discipline: no pre-registration data / sample sufficiency
# ---------------------------------------------------------------------------


def check_no_pre_registration_data(
    record: Mapping[str, Any],
    data_start_utc: Any,
) -> None:
    """Raise if an evaluation window starts before the hypothesis was registered.

    This is the mechanical line against post-hoc window shopping and
    look-ahead: a hypothesis may only ever be tested on data that did not exist
    at registration time.
    """
    registered = parse_utc(record["registered_at_utc"])
    start = parse_utc(data_start_utc)
    if start < registered:
        raise PreRegistrationDataError(
            "evaluation window starts before registration: "
            f"data_start={data_start_utc} < registered_at_utc={record['registered_at_utc']} "
            f"(hypothesis '{record.get('hypothesis_id')}'). "
            "Pre-registration discipline forbids using pre-registration data."
        )


def sample_sufficiency(
    n: int,
    *,
    n_min: int = DEFAULT_MIN_SAMPLE,
) -> dict[str, Any]:
    """Decide whether ``n`` observations are enough to report a verdict.

    Below ``n_min`` the only permitted output is "未达评估条件（需 n≥X）" — no
    conclusion, no direction, no p-value headline.
    """
    n = int(n)
    n_min = int(n_min)
    sufficient = n >= n_min
    return {
        "n": n,
        "n_min": n_min,
        "sufficient": sufficient,
        "verdict": "SUFFICIENT" if sufficient else "INSUFFICIENT_SAMPLE",
        "message": (
            f"样本充足：n={n} ≥ {n_min}"
            if sufficient
            else f"未达评估条件（需 n≥{n_min}，当前 n={n}）—— 不得给出结论"
        ),
    }


# ---------------------------------------------------------------------------
# Attribution helpers
# ---------------------------------------------------------------------------


def survival_label(record: Mapping[str, Any]) -> int | None:
    """Binary survival label for the rubric attribution (None if undetermined).

    Precedence: explicit ``outcome.survived`` bool, then ``outcome.verdict`` in
    :data:`SURVIVED_VERDICTS`, then ``status == resolved`` (adopted) /
    ``status == rejected`` (failed).  Every other status — including the
    terminal-but-non-evaluative :data:`STATUS_SUPERSEDED` — is ``None``.

    ``superseded`` returns ``None`` (NOT 0) on purpose.  It contributes 0 to the
    survived tally and 0 to the decided denominator, i.e. it is neither survived
    nor failed; it is *not a data point*.  Returning a literal 0 would assert
    "evaluated and failed" for a hypothesis nobody evaluated, and would break the
    ledger's own invariant ``n_decided == #records carrying an outcome`` that
    ``tests/validation/test_registry_backfill.py`` asserts.  Superseded entries
    are surfaced instead through ``registry_stats()['by_status']`` and
    ``registry_stats()['n_superseded']``.
    """
    outcome = record.get("outcome") or {}
    if isinstance(outcome.get("survived"), bool):
        return 1 if outcome["survived"] else 0
    verdict = outcome.get("verdict")
    if isinstance(verdict, str):
        return 1 if verdict.upper() in SURVIVED_VERDICTS else 0
    status = record.get("status")
    if status == STATUS_SUPERSEDED:
        return None
    if status == STATUS_RESOLVED:
        return 1
    if status == STATUS_REJECTED:
        return 0
    return None


def registry_stats(records: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Summary counts by status and band, plus the survival tally.

    ``n_superseded`` is bucketed separately so a documentary retraction is
    visible without entering the survived/decided tallies (see
    :func:`survival_label`).
    """
    records = list(records)
    by_status: dict[str, int] = {}
    by_band: dict[str, int] = {}
    survived = 0
    decided = 0
    n_superseded = 0
    for rec in records:
        status = rec.get("status", "?")
        by_status[status] = by_status.get(status, 0) + 1
        if status == STATUS_SUPERSEDED:
            n_superseded += 1
        band = (rec.get("rubric") or {}).get("band", "?")
        by_band[band] = by_band.get(band, 0) + 1
        label = survival_label(rec)
        if label is not None:
            decided += 1
            survived += label
    return {
        "n_records": len(records),
        "by_status": by_status,
        "by_band": by_band,
        "n_decided": decided,
        "n_survived": survived,
        "n_superseded": n_superseded,
        "survival_rate": (survived / decided) if decided else None,
    }
