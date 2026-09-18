"""Resolve a hypothesis's search width (N) for DSR deflation — schema v2.

Why this is separate from ``registry.py``
-----------------------------------------
``registry.py`` is the *freeze* boundary: it records what was declared before
the data was seen.  Resolving N at evaluation time is a *read* concern (it
falls back to report-countable grids and platform constants when the registry
has no declaration), so it lives here rather than in the registry.  This keeps
the registry append-only record honest and gives evaluators one shared,
auditable resolution chain.

Priority (highest first)
------------------------
1. ``n_trials_actual``  — the evaluation ran a known, enumerated grid.
2. ``n_trials_planned`` — the width pre-registered on the record.
3. a report/script-countable grid, supplied by ``grid_count_fn`` and carrying
   ``file:line`` evidence.
4. the platform constant (family-level ``PLATFORM_HYPOTHESES_SEARCHED`` by
   default).

The chosen value and its basis are returned together so every DSR number can
be traced back to a moment in time (written by ``validate_reports.py`` into
``checks.deflation.n_trials_basis``).

Deliberate bias direction (do not "fix" silently)
-------------------------------------------------
Variants are usually correlated, so the raw count **over-states** the number
of independent trials.  That raises the DSR noise ceiling and makes the test
**too strict**: a "fail" is safe and a "pass" is real.  Raw N is therefore the
default and MUST NOT be quietly replaced by a smaller effective N.  A future
``n_effective`` may only be used when the trial return matrix is stored so the
eigenvalues can be recomputed, and only as an explicit, labelled refinement.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Mapping

from .registry import search_grid_product

#: Basis tags returned alongside N (kept stable for report consumers).
BASIS_ACTUAL = "registry:n_trials_actual"
BASIS_PLANNED = "registry:n_trials_planned"
BASIS_REPORT = "report:grid"
BASIS_PLATFORM = "platform:constant"

#: Fallbacks mirroring ``scripts/validate_reports.py`` (which owns the platform
#: constants).  Imported lazily so this util has no script-layer dependency and
#: no import cycle.
DEFAULT_PLATFORM_FAMILY_N = 27
DEFAULT_PLATFORM_VARIANT_N = 60


def _platform_family_n() -> int:
    try:  # pragma: no cover - import shim
        import importlib.util
        import sys

        root = Path(__file__).resolve().parents[2]
        spec = importlib.util.spec_from_file_location(
            "_vr_platform_constants", root / "scripts" / "validate_reports.py"
        )
        if spec is None or spec.loader is None:
            return DEFAULT_PLATFORM_FAMILY_N
        mod = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = mod
        spec.loader.exec_module(mod)
        return int(getattr(mod, "PLATFORM_HYPOTHESES_SEARCHED", DEFAULT_PLATFORM_FAMILY_N))
    except Exception:  # noqa: BLE001 — fall back to the mirrored constant
        return DEFAULT_PLATFORM_FAMILY_N


def resolve_n_trials(
    record: Mapping[str, Any],
    *,
    grid_count_fn: Callable[[Mapping[str, Any]], Any] | None = None,
    platform_n: int | None = None,
) -> tuple[int, str]:
    """Resolve ``(n_trials, basis)`` for one registry record.

    Args:
        record: a registry record (``registered_at_utc`` etc.); search-width
            fields are optional (legacy records fall through).
        grid_count_fn: optional callback that reconstructs N from reports or
            scripts and returns either an ``int``, a ``(n, evidence)`` pair, or
            ``None``.  Supplied by the report layer (which knows the artifacts)
            so this util stays dependency-free.
        platform_n: override for the platform constant fallback.

    Returns:
        ``(n, basis)`` with ``n >= 1`` and one of the ``BASIS_*`` tags.

    Raises:
        ValueError: if no source yields a positive N (never silently return 0).
    """
    actual = record.get("n_trials_actual")
    if isinstance(actual, int) and not isinstance(actual, bool) and actual >= 1:
        return actual, BASIS_ACTUAL

    planned = record.get("n_trials_planned")
    if isinstance(planned, int) and not isinstance(planned, bool) and planned >= 1:
        return planned, BASIS_PLANNED

    if grid_count_fn is not None:
        counted = grid_count_fn(record)
        if counted is not None:
            if isinstance(counted, tuple):
                n, evidence = counted
            else:
                n, evidence = counted, None
            if isinstance(n, int) and not isinstance(n, bool) and n >= 1:
                basis = BASIS_REPORT if not evidence else f"{BASIS_REPORT}:{evidence}"
                return n, basis

    n = platform_n if platform_n is not None else _platform_family_n()
    if not isinstance(n, int) or isinstance(n, bool) or n < 1:
        raise ValueError(f"invalid platform N fallback: {n!r}")
    return n, BASIS_PLATFORM


def planned_grid_product(record: Mapping[str, Any]) -> int | None:
    """Product of the record's ``search_grid``, or None when absent."""
    grid = record.get("search_grid")
    if not grid:
        return None
    return search_grid_product(grid)
