"""Multiple-comparison correction: "given how many things we tried, is this real?"

The platform has searched 20+ directions by now — funding rate, on-chain
fundamentals, FOMC decisions and text, overnight gap, order-book lead, Polymarket
lead, volume confirmation, limit-entry timing, three risk gates, GBM, ...  The
signals that survived were picked *after seeing the data*, so a headline t of ~2
is not evidence: with 20 independent tries the expected best |t| under the null
is already ~2.4.

This module quantifies that: a Bonferroni bound, a required-t threshold, and a
Benjamini-Hochberg FDR option when a full family of p-values is available.
"""

from __future__ import annotations

import math
import statistics
from typing import Any, Mapping, Sequence

import numpy as np

from .stats import fdr_bh, normal_two_sided_p, two_sided_t_p

_NORMAL = statistics.NormalDist()

VERDICT_SURVIVES = "SURVIVES"
VERDICT_FAILS = "FAILS"
VERDICT_INSUFFICIENT = "INSUFFICIENT"


def expected_max_abs_t(n_hypotheses: int) -> float:
    """Expected maximum |z| across N independent standard normal draws.

    Asymptotically sqrt(2 ln N): ~1.96 for N=1, ~2.44 for N=20, ~2.72 for N=100.
    """
    n = int(n_hypotheses)
    if n <= 1:
        return float(_NORMAL.inv_cdf(0.75))  # E|Z| = 0.6745
    return math.sqrt(2.0 * math.log(n))


def required_t(n_hypotheses: int, *, alpha: float = 0.05) -> float:
    """|t| needed to survive a Bonferroni correction (normal approximation)."""
    n = max(1, int(n_hypotheses))
    return float(_NORMAL.inv_cdf(1.0 - alpha / (2.0 * n)))


def multiple_comparisons(
    n_hypotheses: int,
    observed_best: float | Mapping[str, Any] | Sequence[float],
    *,
    n: int | None = None,
    alpha: float = 0.05,
    label: str | None = None,
) -> dict:
    """Is the best result still significant after searching N hypotheses?

    Args:
        n_hypotheses: how many directions/configs/variants were searched.
        observed_best: the winner.  Either the best t-statistic (float), a dict
            with ``t_stat`` (and optional ``n``) or ``p_value``, or a full list
            of t-statistics (then the max |t| is taken and BH-FDR is computed too).
        n: sample size behind the observed t (df = n-1).  Falls back to the
            normal approximation when unavailable.
        alpha: family-wise / FDR level.

    Verdict is SURVIVES only if the corrected p-value clears `alpha`.
    """
    N = max(1, int(n_hypotheses))
    family: list[float] | None = None

    if observed_best is None:
        return _insufficient(N, alpha, label, "no observed result supplied")

    if isinstance(observed_best, Mapping):
        t_obs = observed_best.get("t_stat")
        if t_obs is None and observed_best.get("p_value") is not None:
            p_obs = float(observed_best["p_value"])
            t_obs = _t_from_p(p_obs)
        else:
            t_obs = float(t_obs) if t_obs is not None else None
            p_obs = two_sided_t_p(t_obs, int(observed_best.get("n", n or 0)) - 1) if observed_best.get("n") or n else normal_two_sided_p(t_obs)
        if n is None and observed_best.get("n") is not None:
            n = int(observed_best["n"])
    elif isinstance(observed_best, (list, tuple, np.ndarray)):
        family = [float(x) for x in observed_best]
        finite = [abs(x) for x in family if np.isfinite(x)]
        if not finite:
            return _insufficient(N, alpha, label, "no finite t-statistic in the family")
        t_obs = max(finite)
        N = max(N, len(family))
        p_obs = two_sided_t_p(t_obs, int(n) - 1) if n else normal_two_sided_p(t_obs)
    else:
        t_obs = float(observed_best)
        p_obs = two_sided_t_p(t_obs, int(n) - 1) if n else normal_two_sided_p(t_obs)

    if t_obs is None or not np.isfinite(t_obs):
        return _insufficient(N, alpha, label, "observed t-statistic is missing or non-finite")

    df = int(n) - 1 if n else 0
    corrected_alpha = alpha / N
    bonferroni_p = min(1.0, p_obs * N)
    req_t = required_t(N, alpha=alpha)
    null_best = expected_max_abs_t(N)
    null_threshold_p = normal_two_sided_p(req_t)

    fdr = None
    if family is not None:
        ps = [two_sided_t_p(x, df) if df > 0 else normal_two_sided_p(x) for x in family]
        bh = fdr_bh(ps, alpha=alpha)
        fdr = {"adjusted_best_p": min(bh["adjusted"]) if bh["adjusted"] else None, "n_rejected": bh["n_rejected"], "n": bh["n"]}

    survivors = bool(np.isfinite(p_obs) and p_obs <= corrected_alpha)
    return {
        "label": label,
        "n_hypotheses": N,
        "observed_t": float(t_obs),
        "observed_p": float(p_obs) if np.isfinite(p_obs) else None,
        "df": df if df > 0 else None,
        "alpha": float(alpha),
        "corrected_alpha": corrected_alpha,
        "bonferroni_p": bonferroni_p if np.isfinite(bonferroni_p) else None,
        "required_t": req_t,
        "expected_best_abs_t_under_null": null_best,
        "null_threshold_p": null_threshold_p,
        "family_wise_survivor": survivors,
        "fdr_bh": fdr,
        "verdict": VERDICT_SURVIVES if survivors else VERDICT_FAILS,
        "interpretation": _interpret(t_obs, req_t, null_best, N, survivors),
    }


def _interpret(t_obs: float, req_t: float, null_best: float, N: int, survivors: bool) -> str:
    if survivors:
        return f"|t|={abs(t_obs):.2f} clears the Bonferroni bar {req_t:.2f} for N={N} -> survives"
    return (
        f"|t|={abs(t_obs):.2f} vs required {req_t:.2f} for N={N}; under the null the best of {N} independent "
        f"tests is already ~{null_best:.2f}, so this is inside the search noise"
    )


def _t_from_p(p: float) -> float:
    """Convert a two-sided normal p-value back to a |t| (approximation)."""
    if not np.isfinite(p) or p <= 0.0:
        return float("inf")
    return float(_NORMAL.inv_cdf(1.0 - p / 2.0))


def _insufficient(N: int, alpha: float, label: str | None, note: str) -> dict:
    return {
        "label": label,
        "n_hypotheses": N,
        "observed_t": None,
        "observed_p": None,
        "alpha": float(alpha),
        "corrected_alpha": alpha / N,
        "required_t": required_t(N, alpha=alpha),
        "expected_best_abs_t_under_null": expected_max_abs_t(N),
        "family_wise_survivor": None,
        "fdr_bh": None,
        "verdict": VERDICT_INSUFFICIENT,
        "note": note,
    }
