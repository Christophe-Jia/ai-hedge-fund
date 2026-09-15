"""Multi-window robustness: report a metric *distribution*, not a point estimate.

Motivation (the 2026-09 GBM credibility crisis): the same strategy, re-run with
a different test-window start (2020-10 -> 2021-05), flipped its mean IC from
+0.0082 to -0.0076 and its own verdict from "beats momentum" to "NO EDGE".  A
single point estimate cannot reveal that; the sign-consistency rate across
windows can, and it is cheap to compute.
"""

from __future__ import annotations

from typing import Any, Callable, Sequence

import numpy as np

from ._extract import extract_metric

VERDICT_STABLE = "STABLE"
VERDICT_UNSTABLE = "UNSTABLE"
VERDICT_INSUFFICIENT = "INSUFFICIENT"


def multi_window(
    backtest_fn: Callable[[Any], Any],
    windows: Sequence[Any],
    *,
    metric: Any = "sharpe",
    pass_line: float = 0.8,
    label: str | None = None,
) -> dict:
    """Evaluate one strategy over several test windows and summarise the spread.

    Args:
        backtest_fn: called once per window with the window object; returns any
            result shape accepted by `extract_metric`.
        windows: arbitrary window identifiers handed to `backtest_fn`.
        metric: metric to extract (key, dotted path or callable).
        pass_line: minimum sign-consistency rate to be called STABLE (default 0.8).

    Sign consistency is the share of windows agreeing with the *majority* sign;
    it is therefore always >= 0.5.  A strategy that flips sign across windows
    scores near 0.5 and is flagged UNSTABLE.
    """
    rows: list[dict] = []
    for w in windows:
        value = extract_metric(backtest_fn(w), metric)
        rows.append({"window": _label(w), "value": value})

    finite = [r["value"] for r in rows if r["value"] is not None]
    n = len(finite)
    if n == 0:
        return {
            "label": label,
            "metric": metric if isinstance(metric, str) else repr(metric),
            "n_windows": len(rows),
            "n_evaluated": 0,
            "windows": rows,
            "verdict": VERDICT_INSUFFICIENT,
            "note": "no window produced a finite metric value",
        }

    values = np.asarray(finite, dtype=float)
    mean = float(values.mean())
    std = float(values.std(ddof=1)) if n > 1 else 0.0
    n_pos = int((values > 0).sum())
    n_neg = int((values < 0).sum())
    n_zero = int((values == 0).sum())
    consistency = max(n_pos, n_neg) / n

    if n < 2:
        verdict = VERDICT_INSUFFICIENT
    elif consistency >= pass_line:
        verdict = VERDICT_STABLE
    else:
        verdict = VERDICT_UNSTABLE

    return {
        "label": label,
        "metric": metric if isinstance(metric, str) else repr(metric),
        "n_windows": len(rows),
        "n_evaluated": n,
        "mean": mean,
        "std": std,
        "min": float(values.min()),
        "max": float(values.max()),
        "range": float(values.max() - values.min()),
        "n_positive": n_pos,
        "n_negative": n_neg,
        "n_zero": n_zero,
        "sign_consistency": consistency,
        "sign_flips": _sign_flips(values),
        "pass_line": float(pass_line),
        "verdict": verdict,
        "windows": rows,
    }


def _sign_flips(values: np.ndarray) -> int:
    """Number of adjacent (in the given window order) sign changes."""
    signs = np.sign(values)
    signs = signs[signs != 0]
    if signs.size < 2:
        return 0
    return int((signs[1:] != signs[:-1]).sum())


def _label(w: Any) -> Any:
    if isinstance(w, (str, int, float, bool)) or w is None:
        return w
    if isinstance(w, dict):
        return str(w)
    return str(w)
