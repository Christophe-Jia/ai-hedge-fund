"""Failure-benchmark flywheel: every discovered failure mode becomes a permanent
regression benchmark.

Motivation (the 2026-09 search campaign): each time we found a way a conclusion
could be wrong, we hand-built a one-off check — GBM score ties -> boundary
stability, GBM window sign flip -> multi_window, weekend_gap's two winners ->
``leave_k_best_out``, the S&P500 single-year lottery -> ``concentration_profile``,
meta-label NOISE -> event significance.  The checks were good; the *entry point*
was missing.  A new failure mode discovered tomorrow would be mentioned in a
report, maybe remembered, and forgotten.  This module turns "we found a new way
to be wrong" into a first-class, executable artefact:

    discover a failure  ->  register a benchmark (detector + expected verdict)
                        ->  the detector is RUN and must actually catch it
                        ->  the meta-test replays the whole ledger forever

The ledger is ``tests/validation/benchmarks/failure_benchmarks.json`` (single
source of truth).  A record is only admissible if a *framework component*
actually flags it — see :func:`run_benchmark`, which is shared by
``scripts/add_failure_benchmark.py`` (registration-time gate) and
``tests/validation/test_failure_benchmarks.py`` (regression replay).  This is
what keeps an *imagined* failure mode out of the ledger: if the detector does
not fire, registration is refused.

A benchmark record::

    {
      "id": "weekend-gap-few-winners",
      "discovered_at": "2026-09-15",
      "discovered_by": "red-team",
      "title": "...",
      "failure_mode": "one short sentence: what is the disease",
      "symptom": "what you can observe when it happens",
      "detector": "src.validation.robustness.leave_k_best_out",
      "expected_verdict": "FRAGILE_BY_FEW_WINNERS",
      "assertions": {"flag": "FRAGILE_BY_FEW_WINNERS", "first_failure_k": 2},
      "input": {"kind": "series", "report": "...", "path": "...", "value_key": "..."},
      "evidence": "reports/...json:...",
      "regression_test": "tests/validation/test_failure_benchmarks.py::..."
    }

``known_gaps`` is the honest inverse ledger: failure modes we have *documented*
but which no framework component catches yet.  The meta-test asserts each gap is
still open, so the day someone closes it the test fails and the gap graduates
into a benchmark.
"""

from __future__ import annotations

import importlib
import json
import math
from pathlib import Path
from typing import Any, Callable, Mapping

ROOT = Path(__file__).resolve().parents[2]
BENCHMARKS_PATH = ROOT / "tests" / "validation" / "benchmarks" / "failure_benchmarks.json"
REPORTS_DIR = ROOT / "reports"

# A record is admitted only when these are present and non-empty.  ``detector``
# and ``expected_verdict`` are the two that cannot be hand-waved: the former must
# be an importable framework callable, the latter a concrete verdict — not
# "something is wrong somewhere".
REQUIRED_FIELDS = (
    "id",
    "title",
    "failure_mode",
    "symptom",
    "detector",
    "expected_verdict",
    "input",
)

# Placeholder verdicts that are refused at registration time.
VAGUE_VERDICTS = {"", "?", "todo", "tbd", "unknown", "n/a", "na", "none"}


# ---------------------------------------------------------------------------
# ledger I/O
# ---------------------------------------------------------------------------

def load_benchmark_file(path: str | Path | None = None) -> dict:
    """Load the ledger; an absent file yields an empty ledger (never crashes)."""
    p = Path(path) if path else BENCHMARKS_PATH
    if not p.exists():
        return {"meta": {}, "benchmarks": [], "known_gaps": []}
    return json.loads(p.read_text())


def iter_benchmarks(path: str | Path | None = None) -> list[dict]:
    return list(load_benchmark_file(path).get("benchmarks", []))


def iter_known_gaps(path: str | Path | None = None) -> list[dict]:
    return list(load_benchmark_file(path).get("known_gaps", []))


def render_table(benchmarks: list[dict] | None = None) -> list[str]:
    """Markdown table of the ledger (used by docs and `--list`)."""
    rows = benchmarks if benchmarks is not None else iter_benchmarks()
    lines = ["| id | title | detector | expected verdict |", "|---|---|---|---|"]
    for b in rows:
        lines.append(
            f"| `{b.get('id', '')}` | {b.get('title', '')} | "
            f"`{_short_detector(b.get('detector', ''))}` | `{b.get('expected_verdict', '')}` |"
        )
    return lines


def _short_detector(dotted: str) -> str:
    parts = dotted.rsplit(".", 1)
    return parts[-1] if len(parts) == 2 else dotted


# ---------------------------------------------------------------------------
# spec validation
# ---------------------------------------------------------------------------

def validate_spec(spec: Mapping[str, Any]) -> list[str]:
    """Structural validation independent of actually running the detector."""
    problems: list[str] = []
    for field in REQUIRED_FIELDS:
        if not spec.get(field):
            problems.append(f"missing required field '{field}'")
    verdict = str(spec.get("expected_verdict", "")).strip()
    if verdict.lower() in VAGUE_VERDICTS:
        problems.append(
            "expected_verdict must name the concrete verdict the detector emits "
            "(e.g. FRAGILE_BY_FEW_WINNERS / ARBITRARY / SINGLE_EVENT_DRIVEN / NOISE); "
            f"got {verdict!r}"
        )
    detector = str(spec.get("detector", ""))
    if detector:
        try:
            resolve_detector(detector)
        except Exception as exc:  # noqa: BLE001 - surfaced to the user
            problems.append(f"detector {detector!r} cannot be resolved: {exc}")
    if not spec.get("assertions"):
        problems.append(
            "assertions must pin the machine-checkable fields (e.g. {\"flag\": \"FRAGILE_BY_FEW_WINNERS\"}); "
            "expected_verdict alone is prose"
        )
    return problems


def resolve_detector(dotted: str) -> Callable[..., Any]:
    """Import ``module.attr`` and return it (must be callable)."""
    if "." not in dotted:
        raise ValueError(f"detector must be a dotted path, got {dotted!r}")
    module_name, _, attr = dotted.rpartition(".")
    module = importlib.import_module(module_name)
    fn = getattr(module, attr, None)
    if fn is None:
        raise LookupError(f"{module_name!r} has no attribute {attr!r}")
    if not callable(fn):
        raise TypeError(f"{dotted!r} is not callable")
    return fn


# ---------------------------------------------------------------------------
# json-path helpers
# ---------------------------------------------------------------------------

def dig(obj: Any, path: str | list | None) -> Any:
    """Lookup over dicts / list indices; None-safe.

    ``path`` is a dotted string (``"a.b.0.c"``) or a list of segments.  Use the
    list form when a key itself contains a dot — e.g. the ``"2.0"`` threshold key
    in ``per_year.market_close."2.0".long_only``.
    """
    if not path:
        return obj
    parts = list(path) if isinstance(path, (list, tuple)) else str(path).split(".")
    cur = obj
    for part in parts:
        if cur is None:
            return None
        if isinstance(cur, Mapping):
            cur = cur.get(part)
        elif isinstance(cur, (list, tuple)):
            try:
                cur = cur[int(part)]
            except (ValueError, IndexError):
                return None
        else:
            return None
    return cur


def _finite(x: Any) -> float | None:
    try:
        f = float(x)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _load_report(name: str) -> Any:
    return json.loads((REPORTS_DIR / name).read_text())


def _series_from_spec(spec: Mapping[str, Any]) -> list[float]:
    if "values" in spec:
        raw = list(spec["values"])
    else:
        report = _load_report(str(spec["report"]))
        blob = dig(report, spec.get("path"))
        key = spec.get("value_key")
        if key is not None:
            if not isinstance(blob, (list, tuple)):
                raise ValueError(f"path {spec.get('path')!r} did not yield a list of records")
            raw = [dig(rec, key) if isinstance(rec, Mapping) else rec for rec in blob]
        elif isinstance(blob, Mapping):
            raw = list(blob.values())
        elif isinstance(blob, (list, tuple)):
            raw = list(blob)
        else:
            raise ValueError(f"path {spec.get('path')!r} did not yield a series")
    out = [_finite(v) for v in raw]
    return [v for v in out if v is not None]


def _mapping_from_spec(spec: Mapping[str, Any]) -> dict[str, float]:
    if "entries" in spec:
        report = _load_report(str(spec["report"]))
        return {str(k): _finite(dig(report, p)) for k, p in spec["entries"].items()}  # type: ignore[return-value]
    report = _load_report(str(spec["report"]))
    blob = dig(report, spec.get("path"))
    if "key_field" in spec and "value_field" in spec:
        if not isinstance(blob, (list, tuple)):
            raise ValueError(f"path {spec.get('path')!r} did not yield a list of records")
        out: dict[str, float] = {}
        for rec in blob:
            if not isinstance(rec, Mapping):
                continue
            out[str(dig(rec, spec["key_field"]))] = _finite(dig(rec, spec["value_field"]))
        return {k: v for k, v in out.items() if v is not None}
    if not isinstance(blob, Mapping):
        raise ValueError(f"path {spec.get('path')!r} did not yield a mapping")
    value_path = spec.get("value_path")
    out = {}
    for k, v in blob.items():
        val = _finite(dig(v, value_path) if value_path else v)
        if val is not None:
            out[str(k)] = val
    return out


def _picks_frame():
    import pandas as pd

    picks_dir = REPORTS_DIR / "gbm_picks"
    rows: dict[str, dict[str, float]] = {}
    for path in sorted(picks_dir.glob("*.json")):
        data = json.loads(path.read_text())
        picks = data.get("picks") or []
        if picks:
            rows[path.stem] = {p["symbol"]: float(p["score"]) for p in picks if "score" in p}
    if not rows:
        raise FileNotFoundError(f"no score cross-sections under {picks_dir}")
    return pd.DataFrame(rows).T


def _consistency_args(spec: Mapping[str, Any]) -> tuple[list, dict]:
    if "ic" in spec:
        args = [spec["ic"], spec["sharpe"], float(spec.get("turnover", 0.0)), int(spec["n_months"])]
        kwargs = dict(spec.get("kwargs", {}))
    else:
        report = _load_report(str(spec["report"]))
        args = [
            dig(report, spec["ic_path"]),
            dig(report, spec["sharpe_path"]),
            float(spec.get("turnover", 0.0)),
            int(dig(report, spec["n_months_path"]) or 0),
        ]
        kwargs = dict(spec.get("kwargs", {}))
        if spec.get("benchmark_path"):
            kwargs["benchmark_sharpe"] = dig(report, spec["benchmark_path"])
    return args, kwargs


def build_call(spec: Mapping[str, Any]) -> tuple[Callable[..., Any], tuple, dict]:
    """Resolve ``spec`` into (detector, args, kwargs) ready to invoke."""
    fn = resolve_detector(str(spec.get("detector") or spec.get("detector_tried")))
    spec_in: Mapping[str, Any] = spec["input"]
    kind = str(spec_in.get("kind", "series"))
    kwargs = dict(spec_in.get("kwargs", {}))

    if kind == "series":
        return fn, (_series_from_spec(spec_in),), kwargs

    if kind == "mapping":
        mapping = _mapping_from_spec(spec_in)
        metric = str(spec_in.get("metric", "v"))
        kwargs["metric"] = metric
        windows = list(mapping)

        def _fn(w, _m=mapping, _k=metric):
            return {_k: _m[w]}

        return fn, (_fn, windows), kwargs

    if kind == "consistency":
        args, extra = _consistency_args(spec_in)
        extra.update(kwargs)
        return fn, tuple(args), extra

    if kind == "cross_section_picks":
        df = _picks_frame()
        top_n = spec_in.get("top_n") or int(df.notna().sum(axis=1).max())
        kwargs.setdefault("n_perturb", 25)
        kwargs.setdefault("drop_frac", 0.0)
        kwargs["top_n"] = int(top_n)
        return fn, (df,), kwargs

    if kind == "selections":
        return fn, (spec_in["baseline"], spec_in["candidate"]), kwargs

    raise ValueError(f"unknown input kind {kind!r}")


# ---------------------------------------------------------------------------
# assertions
# ---------------------------------------------------------------------------

def _lookup(obj: Any, dotted: str) -> Any:
    cur = obj
    for part in dotted.split("."):
        if isinstance(cur, Mapping):
            if part not in cur:
                raise KeyError(part)
            cur = cur[part]
        elif isinstance(cur, (list, tuple)):
            cur = cur[int(part)]
        else:
            raise KeyError(part)
    return cur


_OPS: dict[str, Callable[[Any, Any], bool]] = {
    "==": lambda a, b: a == b,
    "!=": lambda a, b: a != b,
    ">": lambda a, b: a is not None and a > b,
    ">=": lambda a, b: a is not None and a >= b,
    "<": lambda a, b: a is not None and a < b,
    "<=": lambda a, b: a is not None and a <= b,
    "approx": lambda a, b: a is not None and abs(float(a) - float(b)) <= 1e-6,
    "in": lambda a, b: a in b,
    "contains": lambda a, b: b in a,
    "is_true": lambda a, b: bool(a) is True,
    "is_false": lambda a, b: bool(a) is False,
}


def _compare(actual: Any, expected: Any) -> bool:
    if isinstance(expected, Mapping) and "op" in expected:
        op = str(expected["op"])
        if op not in _OPS:
            raise ValueError(f"unknown assertion op {op!r}")
        return _OPS[op](actual, expected.get("value"))
    return actual == expected


def check_assertions(actual: Any, assertions: Mapping[str, Any]) -> tuple[bool, list[dict]]:
    """Return (passed, failures) for a mapping of field -> expected value."""
    failures: list[dict] = []
    for field, expected in assertions.items():
        try:
            got = _lookup(actual, field)
        except (KeyError, IndexError, TypeError):
            failures.append({"field": field, "expected": expected, "actual": "<missing>"})
            continue
        try:
            ok = _compare(got, expected)
        except Exception as exc:  # noqa: BLE001 - bad assertion spec
            failures.append({"field": field, "expected": expected, "actual": f"<error: {exc}>"})
            continue
        if not ok:
            failures.append({"field": field, "expected": expected, "actual": got})
    return (not failures), failures


# ---------------------------------------------------------------------------
# the verifier (shared by script + meta-test)
# ---------------------------------------------------------------------------

def run_benchmark(spec: Mapping[str, Any]) -> dict:
    """Run the detector named by ``spec`` and check its output.

    Returns a dict with ``passed`` plus, on failure, a ``phase`` that says where
    it broke:

      - ``resolve`` — the detector path is wrong (a *user* error to fix);
      - ``input``   — the input spec could not be assembled;
      - ``run``     — the detector raised;
      - ``assert``  — the detector ran but did not output the expected verdict
                      (either the framework does not cover this mode yet, or the
                      expected verdict is wrong).
    """
    detector = spec.get("detector") or spec.get("detector_tried")
    out: dict[str, Any] = {
        "id": spec.get("id"),
        "detector": detector,
        "expected_verdict": spec.get("expected_verdict"),
    }
    if not detector:
        out.update(passed=False, phase="resolve", error="no 'detector' / 'detector_tried' field")
        return out
    try:
        fn = resolve_detector(str(detector))
    except Exception as exc:  # noqa: BLE001
        out.update(passed=False, phase="resolve", error=f"{type(exc).__name__}: {exc}")
        return out

    try:
        fn, args, kwargs = build_call(spec)
    except Exception as exc:  # noqa: BLE001
        out.update(passed=False, phase="input", error=f"{type(exc).__name__}: {exc}")
        return out

    try:
        actual = fn(*args, **kwargs)
    except Exception as exc:  # noqa: BLE001
        out.update(passed=False, phase="run", error=f"{type(exc).__name__}: {exc}")
        return out

    assertions: Mapping[str, Any] = spec.get("assertions") or {}
    passed, failures = check_assertions(actual, assertions)
    summary = {}
    for field in assertions:
        try:
            summary[field] = _lookup(actual, field)
        except Exception:  # noqa: BLE001
            summary[field] = "<missing>"

    out.update(passed=passed, phase="assert", assertion_failures=failures, actual_summary=summary)
    return out


def describe_failure(result: Mapping[str, Any], spec: Mapping[str, Any]) -> str:
    """A single, directly locatable error string."""
    head = (
        f"[{spec.get('id')}] detector={spec.get('detector')} "
        f"expected_verdict={spec.get('expected_verdict')!r}"
    )
    if result.get("passed"):
        return f"{head} -> CAUGHT"
    phase = result.get("phase")
    if phase == "assert":
        detail = "; ".join(
            f"{f['field']}: expected {f['expected']!r}, got {f['actual']!r}"
            for f in result.get("assertion_failures", [])
        )
        return (
            f"{head} -> NOT CAUGHT (assert): {detail}. "
            "Either the detector does not cover this failure mode yet (framework gap), "
            "or expected_verdict is wrong."
        )
    return (
        f"{head} -> COULD NOT RUN ({phase}): {result.get('error')}. "
        "The detector spec (path / input) is wrong; fix it before registering."
    )


__all__ = [
    "BENCHMARKS_PATH",
    "REPORTS_DIR",
    "REQUIRED_FIELDS",
    "VAGUE_VERDICTS",
    "load_benchmark_file",
    "iter_benchmarks",
    "iter_known_gaps",
    "render_table",
    "validate_spec",
    "resolve_detector",
    "build_call",
    "check_assertions",
    "run_benchmark",
    "describe_failure",
    "dig",
]
