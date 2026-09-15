"""Reproducibility probe: can the conclusion be re-derived from a second run?

This is the generalisation of the probe that actually caught the GBM crisis:
`run_monthly_gbm.py --as-of 2021-06` re-ran one month and produced a top-10 that
overlapped the stored picks in only **2/10** names.  A conclusion that cannot be
reproduced under another data snapshot or another execution path is not a
conclusion — so any report must carry evidence that this probe was run.

`reproducibility_probe` compares two selections (or two score mappings) and
returns the overlap, the value mismatches and a verdict.
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence

import numpy as np

VERDICT_REPRODUCIBLE = "REPRODUCIBLE"
VERDICT_NON_REPRODUCIBLE = "NON_REPRODUCIBLE"
VERDICT_INSUFFICIENT = "INSUFFICIENT"

_NAME_KEYS = ("symbol", "ticker", "name", "key", "id", "month", "label")
_VALUE_KEYS = ("score", "value", "weight", "pct", "sharpe", "ic", "return")


def reproducibility_probe(
    baseline: Any,
    candidate: Any,
    *,
    top_n: int | None = None,
    tolerance: float = 1e-6,
    pass_line: float = 0.9,
    label: str | None = None,
) -> dict:
    """Compare two selections / score vectors and judge reproducibility.

    Args:
        baseline: the shipped/reference artefact.  A mapping ``{key: score}``, a
            list of ``{symbol/score}`` dicts, or a plain list of names.
        candidate: the same shape, produced by the second run / snapshot.
        top_n: if given, compare only the top-N by score (the selection boundary
            is what matters); otherwise compare the full key sets.
        tolerance: absolute value difference above which a score is a mismatch.
        pass_line: minimum overlap ratio to be called REPRODUCIBLE.

    A 2/10 overlap (the GBM case) yields overlap 0.2 -> NON_REPRODUCIBLE.
    """
    a = _normalise(baseline)
    b = _normalise(candidate)
    if not a or not b:
        return {
            "label": label,
            "verdict": VERDICT_INSUFFICIENT,
            "note": "one or both selections were empty / unparseable",
            "n_baseline": len(a),
            "n_candidate": len(b),
        }

    if top_n:
        keys_a = set(sorted(a, key=lambda k: (-a[k], k))[: int(top_n)])
        keys_b = set(sorted(b, key=lambda k: (-b[k], k))[: int(top_n)])
        scope = f"top_{int(top_n)}"
    else:
        keys_a, keys_b = set(a), set(b)
        scope = "full_set"

    intersection = keys_a & keys_b
    overlap = len(intersection) / max(len(keys_a), len(keys_b), 1)
    jaccard = len(intersection) / max(len(keys_a | keys_b), 1)

    mismatches = []
    for key in sorted(intersection):
        va, vb = float(a[key]), float(b[key])
        if np.isfinite(va) and np.isfinite(vb) and abs(va - vb) > float(tolerance):
            mismatches.append({"key": key, "baseline": va, "candidate": vb, "abs_diff": abs(va - vb)})
    mismatches.sort(key=lambda m: -m["abs_diff"])

    reproducible = overlap >= float(pass_line) and not mismatches
    return {
        "label": label,
        "scope": scope,
        "top_n": int(top_n) if top_n else None,
        "n_baseline": len(a),
        "n_candidate": len(b),
        "n_intersection": len(intersection),
        "overlap_ratio": overlap,
        "jaccard": jaccard,
        "pass_line": float(pass_line),
        "tolerance": float(tolerance),
        "missing_from_candidate": sorted(keys_a - keys_b)[:50],
        "extra_in_candidate": sorted(keys_b - keys_a)[:50],
        "n_value_mismatches": len(mismatches),
        "value_mismatches": mismatches[:20],
        "reproducible": reproducible,
        "verdict": VERDICT_REPRODUCIBLE if reproducible else VERDICT_NON_REPRODUCIBLE,
        "interpretation": (
            f"{scope} overlap {overlap:.2f} ({len(intersection)}/{max(len(keys_a), len(keys_b), 1)})"
            + (f" with {len(mismatches)} score mismatches" if mismatches else "")
        ),
    }


def _normalise(selection: Any) -> dict[str, float]:
    """Coerce the supported shapes into {name: score_float}."""
    out: dict[str, float] = {}
    if selection is None:
        return out
    if isinstance(selection, Mapping):
        for k, v in selection.items():
            out[str(k)] = _as_float(v)
        return out
    if isinstance(selection, (str, bytes)):
        return out
    if isinstance(selection, Sequence) or isinstance(selection, np.ndarray) or hasattr(selection, "tolist"):
        items = selection.tolist() if hasattr(selection, "tolist") else list(selection)
        for item in items:
            if isinstance(item, Mapping):
                name = next((item[k] for k in _NAME_KEYS if k in item), None)
                if name is None:
                    continue
                value = next((item[k] for k in _VALUE_KEYS if k in item), 1.0)
                out[str(name)] = _as_float(value)
            else:
                out[str(item)] = 1.0
    return out


def _as_float(value: Any) -> float:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return 1.0
    return f if np.isfinite(f) else 1.0
