"""Small-sample statistics helpers (standard library only — no scipy).

Event-driven strategies are the hardest case in this repo: weekend_gap fires
2-5 times a year, so the whole backtest holds 13-25 observations.  At that size
the normal approximation is too optimistic and a single trade can dominate the
mean, so the framework reports a Student-t p-value, a **bootstrap** confidence
interval for the mean, and a **Wilson** interval for the win rate.
"""

from __future__ import annotations

import math
import statistics
from typing import Callable, Iterable, Sequence

import numpy as np

_NORMAL = statistics.NormalDist()


def z_two_sided(alpha: float = 0.05) -> float:
    """Normal critical value for a two-sided interval of level 1-alpha."""
    return _NORMAL.inv_cdf(1.0 - alpha / 2.0)


def normal_two_sided_p(t: float) -> float:
    """Two-sided p-value of a standard normal test statistic."""
    if not math.isfinite(float(t)):
        return float("nan")
    return 2.0 * (1.0 - _NORMAL.cdf(abs(float(t))))


def _betacf(a: float, b: float, x: float) -> float:
    """Continued fraction for the incomplete beta function (Lentz's method)."""
    max_iter, eps, fpmin = 300, 3e-16, 1e-300
    qab, qap, qam = a + b, a + 1.0, a - 1.0
    c = 1.0
    d = 1.0 - qab * x / qap
    if abs(d) < fpmin:
        d = fpmin
    d = 1.0 / d
    h = d
    for m in range(1, max_iter + 1):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + aa * d
        if abs(d) < fpmin:
            d = fpmin
        c = 1.0 + aa / c
        if abs(c) < fpmin:
            c = fpmin
        d = 1.0 / d
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + aa * d
        if abs(d) < fpmin:
            d = fpmin
        c = 1.0 + aa / c
        if abs(c) < fpmin:
            c = fpmin
        d = 1.0 / d
        delta = d * c
        h *= delta
        if abs(delta - 1.0) < eps:
            break
    return h


def betainc_reg(a: float, b: float, x: float) -> float:
    """Regularised incomplete beta function I_x(a, b)."""
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    lbeta = math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b)
    if x < (a + 1.0) / (a + b + 2.0):
        front = math.exp(lbeta + a * math.log(x) + b * math.log1p(-x))
        return front * _betacf(a, b, x) / a
    front = math.exp(lbeta + b * math.log1p(-x) + a * math.log(x))
    return 1.0 - front * _betacf(b, a, 1.0 - x) / b


def student_t_sf(t: float, df: int) -> float:
    """Upper-tail probability P(T > t) for Student's t with `df` degrees of freedom."""
    t = float(t)
    df = int(df)
    if df <= 0 or not math.isfinite(t):
        return float("nan")
    if t == 0.0:
        return 0.5
    x = df / (df + t * t)
    half_tail = 0.5 * betainc_reg(df / 2.0, 0.5, x)
    return half_tail if t > 0 else 1.0 - half_tail


def two_sided_t_p(t: float, df: int) -> float:
    """Two-sided p-value of a Student-t statistic. Use instead of the normal
    approximation whenever the sample is small (event strategies)."""
    t = float(t)
    df = int(df)
    if df <= 0 or not math.isfinite(t):
        return float("nan")
    x = df / (df + t * t)
    return betainc_reg(df / 2.0, 0.5, x)


def wilson_interval(successes: float, n: int, *, alpha: float = 0.05) -> tuple[float, float]:
    """Wilson score interval for a binomial proportion.

    Preferred over the normal approximation at small n: it never leaves [0, 1]
    and stays meaningful at 0 or n successes.
    """
    n = int(n)
    if n <= 0:
        return (float("nan"), float("nan"))
    z = z_two_sided(alpha)
    p = float(successes) / n
    denom = 1.0 + z * z / n
    centre = (p + z * z / (2.0 * n)) / denom
    half = z * math.sqrt(p * (1.0 - p) / n + z * z / (4.0 * n * n)) / denom
    return (max(0.0, centre - half), min(1.0, centre + half))


def proportion_z(successes: float, n: int, *, p0: float = 0.5) -> float:
    """z statistic for a win rate against p0 (default: coin flip)."""
    n = int(n)
    if n <= 0 or not 0.0 < p0 < 1.0:
        return float("nan")
    p = float(successes) / n
    se = math.sqrt(p0 * (1.0 - p0) / n)
    if se == 0:
        return float("nan")
    return (p - p0) / se


def bootstrap_ci_mean(
    values: Iterable[float],
    *,
    n_resamples: int = 10_000,
    alpha: float = 0.05,
    seed: int = 42,
    statistic: Callable[[np.ndarray], float] | None = None,
) -> dict:
    """Percentile bootstrap CI for the mean of `values`.

    Resampling the observed sample (rather than assuming normality) is the
    honest way to bound the mean of a 13-25 observation event set.
    """
    arr = np.asarray([v for v in values], dtype=float)
    arr = arr[np.isfinite(arr)]
    n = int(arr.size)
    if n == 0:
        return {"n": 0, "n_resamples": 0, "low": None, "high": None, "mean": None, "p_one_sided_leq_zero": None}
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, n, size=(int(n_resamples), n))
    samples = arr[idx]
    draws = samples.mean(axis=1) if statistic is None else np.asarray([statistic(row) for row in samples], dtype=float)
    lo = float(np.percentile(draws, 100.0 * alpha / 2.0))
    hi = float(np.percentile(draws, 100.0 * (1.0 - alpha / 2.0)))
    return {
        "n": n,
        "n_resamples": int(n_resamples),
        "seed": int(seed),
        "alpha": float(alpha),
        "low": lo,
        "high": hi,
        "mean": float(arr.mean()),
        "median": float(np.median(arr)),
        "p_one_sided_leq_zero": float((draws <= 0.0).mean()),
        "draws_mean": float(draws.mean()),
        "draws_std": float(draws.std(ddof=1)) if draws.size > 1 else 0.0,
    }


def fdr_bh(p_values: Sequence[float], *, alpha: float = 0.05) -> dict:
    """Benjamini-Hochberg FDR control over a family of p-values.

    Returns BH-adjusted p-values (monotone) and how many survive at `alpha`.
    """
    p = np.asarray([float(x) for x in p_values], dtype=float)
    finite = np.isfinite(p)
    n = int(finite.sum())
    if n == 0:
        return {"n": 0, "adjusted": [], "n_rejected": 0, "threshold": None}
    idx = np.argsort(p[finite])
    ordered = p[finite][idx]
    m = ordered.size
    adjusted_sorted = ordered * m / (np.arange(1, m + 1))
    # enforce monotonicity from the largest p downwards
    adjusted_sorted = np.minimum.accumulate(adjusted_sorted[::-1])[::-1]
    adjusted_sorted = np.clip(adjusted_sorted, 0.0, 1.0)
    adjusted = np.empty(m)
    adjusted[idx] = adjusted_sorted
    rejected = int((adjusted_sorted <= alpha).sum())
    return {
        "n": m,
        "alpha": float(alpha),
        "adjusted": adjusted.tolist(),
        "n_rejected": rejected,
        "threshold": float(alpha * (rejected + 1) / m) if rejected < m else float(alpha),
        "min_adjusted": float(adjusted_sorted.min()),
    }
