"""Statistical significance of a per-period information-coefficient (IC) series.

Motivation (the 2026-09 GBM credibility crisis): the flagship report stored a
Sharpe of 0.963 right next to a mean monthly IC of 0.0082.  With ~71 test months
the standard error of that mean IC is ~0.0095, so the IC was statistically
indistinguishable from zero — yet both numbers sat in the same report for two
days without anyone cross-checking them.  This module makes that cross-check a
one-liner that any report generator can call.
"""

from __future__ import annotations

import math
from typing import Iterable

import numpy as np

# Two-sided 95% normal critical value (normal approximation, no scipy dependency).
Z_95 = 1.959963984540054

VERDICT_PASS = "PASS"
VERDICT_FAIL = "FAIL"
VERDICT_NOISE = "NOISE"
VERDICT_INSUFFICIENT = "INSUFFICIENT"


def significance_from_stats(
    mean: float,
    std: float | None,
    n: int,
    *,
    t_threshold: float = 2.0,
    label: str = "IC",
) -> dict:
    """Significance test from summary statistics (mean, sample std, n).

    `std` is the *sample* standard deviation of the per-period series (ddof=1).
    Returns a JSON-serialisable dict; non-finite numbers are normalised to None
    by the caller if strict JSON is required.
    """
    mean = float(mean)
    n = int(n)
    try:
        std = float(std) if std is not None else float("nan")
    except (TypeError, ValueError):
        std = float("nan")

    if n < 2 or not np.isfinite(mean) or not np.isfinite(std):
        se = t = lo = hi = float("nan")
        verdict = VERDICT_INSUFFICIENT
        note = "need n>=2 and a finite std to test significance"
    elif std == 0.0:
        se = 0.0
        lo = hi = mean
        if mean == 0.0:
            t = 0.0
            verdict = VERDICT_NOISE
            note = "degenerate zero-variance, zero-mean series"
        else:
            # Infinite t: a constant non-zero series is 'perfectly' significant.
            t = float("nan")
            verdict = VERDICT_PASS if mean > 0 else VERDICT_FAIL
            note = "zero-variance series with non-zero mean (t is infinite)"
    else:
        se = std / math.sqrt(n)
        t = mean / se
        lo = mean - Z_95 * se
        hi = mean + Z_95 * se
        if t >= t_threshold:
            verdict = VERDICT_PASS
        elif t <= -t_threshold:
            verdict = VERDICT_FAIL
        else:
            verdict = VERDICT_NOISE
        note = ""

    if verdict == VERDICT_PASS:
        direction = "positive"
    elif verdict == VERDICT_FAIL:
        direction = "negative"
    else:
        direction = "none"

    return {
        "label": label,
        "n": n,
        "mean": _f(mean),
        "std": _f(std),
        "se": _f(se),
        "t_stat": _f(t),
        "ci_low": _f(lo),
        "ci_high": _f(hi),
        "t_threshold": float(t_threshold),
        "verdict": verdict,
        "direction": direction,
        "significant": None if verdict == VERDICT_INSUFFICIENT else verdict in (VERDICT_PASS, VERDICT_FAIL),
        "note": note,
    }


def significance(
    ic_series: Iterable[float],
    *,
    t_threshold: float = 2.0,
    label: str = "IC",
) -> dict:
    """One-sample t-test of a per-period IC series against zero.

    Returns PASS (significantly positive), FAIL (significantly negative) or
    NOISE (cannot be distinguished from zero).  INSUFFICIENT when fewer than
    two finite observations are supplied.

    Reference calibration (the GBM case): mean 0.0082, std 0.080, n 71
    -> se ~= 0.0095, t ~= 0.84 -> NOISE.
    """
    values = np.asarray([v for v in ic_series], dtype=float)
    values = values[np.isfinite(values)]
    n = int(values.size)
    if n == 0:
        return significance_from_stats(float("nan"), None, 0, t_threshold=t_threshold, label=label)
    if n < 2:
        return significance_from_stats(float(values[0]), None, n, t_threshold=t_threshold, label=label)
    return significance_from_stats(float(values.mean()), float(values.std(ddof=1)), n, t_threshold=t_threshold, label=label)


def _f(x: float) -> float | None:
    try:
        x = float(x)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None
