"""Deployment gate: AMBER blocks strategies, not reports.

RED/AMBER/GREEN is *report triage* — it classifies documents.  It was read as a
deployment licence, and two live examples show the gap:

  - GBM: significance was incalculable (missing fields) -> AMBER, yet the
    strategy shipped to paper trading because nothing treated AMBER as blocking.
  - weekend_gap: p=0.053 -> NOISE -> AMBER, yet it became playbook v1.0 for real
    money; a "NOISE blocks deployment" rule would have caught it on day one.

So the rule is executable here: a **strategy** (not a report) may go live only if
all three gates pass —

  1. significance == PASS,
  2. window stability == PASS (same strategy, sign-consistent across windows),
  3. the top-N boundary is not tie-determined.

Anything else (NOISE, UNSTABLE, ARBITRARY, unknown) blocks.  Reports may still be
delivered with AMBER for iteration.
"""

from __future__ import annotations

from typing import Any, Mapping

VERDICT_LIVE = "LIVE_ALLOWED"
VERDICT_BLOCKED = "DEPLOYMENT_BLOCKED"

_SIGNIFICANCE_KEYS = ("significance", "events", "event_significance")
_WINDOW_KEYS = ("window_stability", "multi_window", "windows")
_BOUNDARY_KEYS = ("boundary_stability", "boundary")


def deployment_gate(checks: Mapping[str, Any], *, label: str | None = None) -> dict:
    """Evaluate the three live-money gates over validation check results.

    Args:
        checks: mapping of check name -> result (a verdict string, a bool, or a
            check dict).  Names may be either the framework's own
            (``significance`` / ``multi_window`` / ``boundary``) or the audit's
            gate names.  Unknown/missing gates count as *not passed*.
        label: optional strategy label echoed into the result.

    Returns a dict with per-gate detail plus ``verdict`` (LIVE_ALLOWED /
    DEPLOYMENT_BLOCKED) and human-readable ``reasons``.
    """
    gates = {
        "significance": _gate(_first(checks, _SIGNIFICANCE_KEYS), _significance_ok),
        "window_stability": _gate(_first(checks, _WINDOW_KEYS), _window_ok),
        "boundary_stability": _gate(_first(checks, _BOUNDARY_KEYS), _boundary_ok),
    }
    failed = [name for name, g in gates.items() if not g["passed"]]
    return {
        "label": label,
        "gates": gates,
        "failed_gates": failed,
        "passed": not failed,
        "verdict": VERDICT_LIVE if not failed else VERDICT_BLOCKED,
        "reasons": [f"{name}: {gates[name]['reason']}" for name in failed],
        "rule": "LIVE requires significance=PASS AND window_stability=PASS AND boundary not tie-determined",
    }


def deployment_gate_from_checks(checks: Mapping[str, Any], *, label: str | None = None) -> dict:
    """Convenience wrapper mapping audit check names onto `deployment_gate`."""
    mapped: dict[str, Any] = {}
    for key in ("significance", "events", "multi_window", "boundary", "significance_from_t_stat"):
        if key in checks:
            mapped[key] = checks[key]
    return deployment_gate(mapped, label=label)


# ---------------------------------------------------------------------------
# internals
# ---------------------------------------------------------------------------

def _first(checks: Mapping[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in checks and checks[key] is not None:
            return checks[key]
    return None


def _verdict_of(value: Any) -> tuple[str | None, dict]:
    if value is None:
        return None, {}
    if isinstance(value, Mapping):
        return value.get("verdict"), dict(value)
    if isinstance(value, bool):
        return ("PASS" if value else "FAIL"), {}
    return str(value).upper(), {}


def _gate(value: Any, predicate) -> dict:
    verdict, blob = _verdict_of(value)
    ok, reason = predicate(verdict, blob)
    return {"passed": ok, "verdict": verdict, "reason": reason}


def _significance_ok(verdict: str | None, blob: dict) -> tuple[bool, str]:
    if verdict is None:
        return False, "no significance result available (cannot license deployment)"
    if verdict == "PASS":
        return True, "significance PASS"
    return False, f"significance is {verdict} (a NOISE/INSUFFICIENT headline must not go live)"


def _window_ok(verdict: str | None, blob: dict) -> tuple[bool, str]:
    if verdict is None:
        return False, "no window-stability result available"
    if blob.get("scope") == "variants":
        return False, "window gate unresolved: the spread is across variants/ablations, not the same strategy across windows"
    if verdict in {"STABLE", "PASS"}:
        if verdict == "STABLE" and blob.get("sign_consistency") is not None:
            return True, f"sign consistency {blob['sign_consistency']}"
        return True, "window stable"
    return False, f"window stability is {verdict}"


def _boundary_ok(verdict: str | None, blob: dict) -> tuple[bool, str]:
    if verdict is None:
        return False, "no boundary-stability result available"
    if verdict == "ARBITRARY":
        return False, f"top-N boundary churns under perturbation (flip rate {blob.get('mean_flip_rate')})"
    ties = blob.get("max_boundary_ties")
    if isinstance(ties, (int, float)) and ties >= 2:
        # the score at the top-N cut is shared -> the cut is decided by sort order
        return False, f"the top-N cut score is shared by {int(ties)} names (tie-determined boundary)"
    if verdict in {"STABLE", "PASS", "SMOOTH"}:
        return True, "boundary not tie-determined"
    return False, f"boundary stability is {verdict}"
