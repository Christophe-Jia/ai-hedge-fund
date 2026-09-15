"""Parameter-neighbourhood stability: does the metric move smoothly?

Motivation (the 2026-09 GBM credibility crisis): an "optimal" configuration that
sits on a spike — good at depth=4, bad at depth=3 and 5 — is far more likely to
be fitted noise than a genuine effect.  An isolated peak should be treated as an
overfitting signal, not as the configuration to ship.
"""

from __future__ import annotations

import itertools
from typing import Any, Callable, Mapping, Sequence

import numpy as np

from ._extract import extract_metric

VERDICT_SMOOTH = "SMOOTH"
VERDICT_OVERFIT = "OVERFIT"
VERDICT_INSUFFICIENT = "INSUFFICIENT"


def neighborhood_stability(
    fn: Callable[..., Any],
    param_grid: Mapping[str, Sequence[Any]],
    *,
    metric: Any = "sharpe",
    overfit_threshold: float = 0.5,
    base_params: Mapping[str, Any] | None = None,
    label: str | None = None,
) -> dict:
    """Scan a cartesian parameter grid and check whether the peak is isolated.

    `fn` is called with each grid point (merged over `base_params`).  The
    isolation ratio is::

        (best_value - median(adjacent neighbours)) / (max_value - min_value)

    where "adjacent" means a grid point that differs from the best point in
    exactly one parameter by exactly one step.  A smooth hill has near-tied
    neighbours (ratio ~0); a spike has much lower neighbours (ratio -> 1).
    Ratios above `overfit_threshold` (default 0.5) are flagged OVERFIT.
    """
    names = list(param_grid.keys())
    axes = [list(param_grid[n]) for n in names]
    points: list[dict] = []
    values: list[float | None] = []

    for combo in itertools.product(*axes) if axes else [()]:
        params = dict(zip(names, combo))
        call = dict(base_params or {})
        call.update(params)
        values.append(extract_metric(fn(**call), metric))
        points.append(params)

    finite_idx = [i for i, v in enumerate(values) if v is not None]
    base = {
        "label": label,
        "metric": metric if isinstance(metric, str) else repr(metric),
        "n_points": len(points),
        "points": [{"params": p, "value": v} for p, v in zip(points, values)],
        "overfit_threshold": float(overfit_threshold),
    }
    if not finite_idx:
        base.update({"verdict": VERDICT_INSUFFICIENT, "note": "no grid point produced a finite metric value"})
        return base

    best_i = max(finite_idx, key=lambda i: values[i])  # type: ignore[arg-type]
    best_value = float(values[best_i])  # type: ignore[arg-type]

    axis_pos = [tuple(axes[a].index(pt[names[a]]) for a in range(len(names))) for pt in points]
    best_pos = axis_pos[best_i]
    neighbour_idx = []
    for i, pos in enumerate(axis_pos):
        if i == best_i or values[i] is None:
            continue
        diffs = [abs(pos[a] - best_pos[a]) for a in range(len(names))]
        if sum(1 for d in diffs if d > 0) == 1 and max(diffs) == 1:
            neighbour_idx.append(i)

    all_vals = np.asarray([values[i] for i in finite_idx], dtype=float)
    value_range = float(all_vals.max() - all_vals.min())
    neighbour_median = float(np.median([values[i] for i in neighbour_idx])) if neighbour_idx else None

    if value_range <= 0.0:
        isolation = 0.0
    elif neighbour_median is None:
        isolation = None
    else:
        isolation = (best_value - neighbour_median) / value_range

    verdict = VERDICT_OVERFIT if (isolation is not None and isolation > overfit_threshold) else VERDICT_SMOOTH

    base.update(
        {
            "best_params": points[best_i],
            "best_value": best_value,
            "neighbour_median": neighbour_median,
            "n_neighbours": len(neighbour_idx),
            "value_range": value_range,
            "isolation_ratio": isolation,
            "verdict": verdict,
        }
    )
    return base
