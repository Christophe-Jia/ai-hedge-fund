"""Statistical deflation — PSR / DSR / MutIC (Bailey & López de Prado).

Multiplicity here is *not* new: :mod:`src.validation.multiplicity` already asks
"given N tries, is the best |t| still significant?" via a Bonferroni-on-t.  This
module adds the two things that framework cannot express:

  1. **Non-normal returns.**  Our failure modes are heavy-tailed and negatively
     skewed: weekend_gap's conclusion stands on 2 trades, the S&P500 expansion
     on distressed-name lottery tickets.  A t-test treats those observations as
     Gaussian; the **Probabilistic Sharpe Ratio** (PSR) corrects the Sharpe
     denominator for skewness (gamma_3) and kurtosis (gamma_4), so a fat-tailed
     return stream is penalised exactly where the naive test is blind.
  2. **Selection deflation.**  The **Deflated Sharpe Ratio** (DSR) replaces the
     zero benchmark with the *expected maximum* Sharpe of N pure-noise trials
     (an extreme-value-theory approximation).  A DSR <= 0.95 means the champion
     is inside the noise band of the search that produced it.

The threshold ``DSR_THRESHOLD = 0.95`` (``DSR > 0.95`` == "true structural
inefficiency") follows the industry-standard usage, e.g. SOPHIE's *Formulaic
Alpha Mining* reference: "Discard any signal failing to achieve a Deflated
Sharpe Ratio confidence interval > 0.95."

``MutIC`` is the diversity penalty from the same lineage: a candidate alpha that
correlates with what is already in the pool has its information content reduced
(``Adjusted IC = IC_raw - lambda * max_corr``), targeting pairwise pool
correlations below 0.30.

Frequency discipline (the #1 implementation bug in DSR code)
-----------------------------------------------------------
Sharpe and n **must be quoted at the same frequency**.  Either use per-period
statistics (a per-period Sharpe with n = number of periods — what
:func:`deflation_report` does for you) or the annualised Sharpe with n = number
of *years*.  Mixing an annualised SR with a per-period n inflates the PSR by
~sqrt(periods_per_year) and silently defeats the test.  Every function here
documents its frequency and every report echoes the convention used.

Known-answer calibration for :func:`expected_max_sharpe` (sigma=1), verified
three independent ways (closed form, 2e6-draw Monte Carlo, and exact quadrature
of ``E[max] = int x*N*phi(x)*Phi(x)^(N-1) dx`` with scipy.quad, error < 1e-8):

    N       formula    exact E[max]   |gap|
    10       1.5746      1.538753     0.036
    100      2.5306      2.507594     0.023
    1000     3.2551      3.241436     0.014
    10000    3.8607      3.851616     0.009

The EVT approximation is accurate to <0.04 across four orders of magnitude of N.
A circulating table (1.50 / 2.20 / 2.80 / 3.20) does NOT match either the formula
or the exact values; see the warning in :func:`expected_max_sharpe` — do not tune
this module to reproduce it.
"""

from __future__ import annotations

import math
import statistics
from typing import Any, Sequence

import numpy as np

_NORMAL = statistics.NormalDist()

# ---------------------------------------------------------------------------
# judgment lines — every constant carries the reason it was chosen
# ---------------------------------------------------------------------------

# DSR > 0.95 == "true structural inefficiency".  Industry-standard line
# (SOPHIE *Formulaic Alpha Mining*, and the Bailey & López de Prado DSR papers):
# the deflated statistic is a confidence in [0, 1], and only a 95% confidence
# that the champion exceeds the search's noise ceiling is treated as real.
DSR_THRESHOLD = 0.95

# MutIC diversity penalty strength.  Adjusted IC = IC_raw - lambda * max_corr.
# 0.5 halves the credit of a candidate whose single worst pool correlation is
# 1.0 (fully redundant); a candidate orthogonal to the pool (corr=0) is unpenalised.
MUTIC_LAMBDA_DEFAULT = 0.5

# Target ceiling for pairwise correlations inside an alpha pool.  Above this the
# candidates carry overlapping information and the pool stops being diversified.
MUTIC_MAX_CORR = 0.30

# Euler-Mascheroni constant used by the EVT max approximation.
EULER_GAMMA = 0.5772156649

# Raw (Pearson) kurtosis of a normal distribution.  The PSR denominator uses
# ``(gamma_4 - 1) / 4`` which vanishes *only* when gamma_4 = 3, i.e. the
# parameter is RAW kurtosis, not excess kurtosis (excess = gamma_4 - 3).
NORMAL_KURTOSIS = 3.0

# Below this many observations the sample skewness/kurtosis are not
# interpretable and the battery-level rule (see robustness.py) already refuses
# to diagnose: report INSUFFICIENT rather than a fabricated number.
MIN_N_DEFLATION = 5

VERDICT_SURVIVES = "SURVIVES"
VERDICT_FAILS = "FAILS"
VERDICT_INSUFFICIENT = "INSUFFICIENT"


# ---------------------------------------------------------------------------
# primitives
# ---------------------------------------------------------------------------

def probabilistic_sharpe_ratio(
    sr: float,
    n: int,
    skew: float,
    kurtosis: float,
    sr_benchmark: float = 0.0,
) -> float:
    """PSR: P(true SR > sr_benchmark), corrected for skewness and fat tails.

    ``PSR = Phi[ (SR - SR*) * sqrt(n-1) / sqrt(1 - gamma_3*SR + ((gamma_4-1)/4)*SR^2) ]``

    Args:
        sr: observed Sharpe at the **same frequency as n** (per-period SR with
            per-period n, or annualised SR with n in years).
        n: number of observations behind ``sr``.
        skew: sample skewness (gamma_3) of the return series.
        kurtosis: **raw** kurtosis (gamma_4), normal == 3.0.  Do not pass excess
            kurtosis: the ``(gamma_4 - 1)/4`` correction requires the raw form so
            that the denominator reduces to ``sqrt(1 + 0.5*SR^2)`` under normality
            (the well-known asymptotic variance of the Sharpe estimator).
        sr_benchmark: SR* the observed Sharpe is tested against (0.0 = "is there
            any edge?", or the noise ceiling for the deflated variant).

    Returns a probability in [0, 1]; ``nan`` on invalid input or when the
    variance correction is non-positive.
    """
    sr = float(sr)
    n = int(n)
    skew = float(skew)
    kurtosis = float(kurtosis)
    sr_benchmark = float(sr_benchmark)
    if n < 2 or not all(math.isfinite(v) for v in (sr, skew, kurtosis, sr_benchmark)):
        return float("nan")
    denom = 1.0 - skew * sr + ((kurtosis - 1.0) / 4.0) * sr * sr
    if denom <= 0.0:
        return float("nan")
    z = (sr - sr_benchmark) * math.sqrt(n - 1) / math.sqrt(denom)
    return float(_NORMAL.cdf(z))


def expected_max_sharpe(
    n_trials: int,
    sr_std: float,
    *,
    euler_gamma: float = EULER_GAMMA,
) -> float:
    """Expected maximum Sharpe of ``n_trials`` pure-noise strategies (EVT).

    ``E[max SR] ~= sigma_SR * [ (1 - gamma) * Phi^-1(1 - 1/N) + gamma * Phi^-1(1 - 1/(N*e)) ]``

    where ``gamma`` is the Euler-Mascheroni constant and ``sigma_SR`` is the
    cross-sectional standard deviation of the trial Sharpe ratios.  This is the
    noise ceiling the champion must beat: it is what the DSR uses as SR*.

    Args:
        n_trials: number of (effective, independent) trials searched.
        sr_std: standard deviation of the trial Sharpe ratios, in the **same
            frequency units** as the SR being deflated.
        euler_gamma: overridable for testing.

    Calibration (sigma=1): N=10 -> 1.57, 100 -> 2.53, 1000 -> 3.26, 10000 ->
    3.86.  These match the exact expected maximum of N iid standard normals
    (1.539 / 2.508 / 3.241 / 3.852) to better than 0.04.

    ⚠️ SOURCE DISCREPANCY — DO NOT "FIX" THIS FUNCTION TO MATCH A PUBLISHED TABLE
    ---------------------------------------------------------------------------
    The formula above is the Bailey & López de Prado extreme-value approximation.
    A commonly-circulated table (SOPHIE *Formulaic Alpha Mining*, and derivatives)
    quotes ~1.50 / 2.20 / 2.80 / 3.20 for N=10/100/1000/10000.  Those numbers are
    NOT the output of this formula and are not consistent with it under any single
    rescaling of sigma (ratios to the formula value are 0.95 / 0.87 / 0.86 / 0.83,
    not constant).  Independent checks of the true expected maximum of N iid
    standard normals — 2e6-draw Monte Carlo and exact quadrature of
    ``int x*N*phi(x)*Phi(x)^(N-1) dx`` (scipy.quad, err < 1e-8) — give
    1.538753 / 2.507594 / 3.241436 / 3.851616, i.e. the formula here (max error
    0.036 at N=10) and NOT the circulating table.  The table is a loose lower
    bound at best.  This implementation follows the formula + verified exact
    values; changing it to reproduce that table would make it wrong.
    """
    n = int(n_trials)
    sigma = float(sr_std)
    if n < 1:
        raise ValueError("n_trials must be >= 1")
    if not math.isfinite(sigma) or sigma < 0.0:
        raise ValueError("sr_std must be a finite non-negative number")
    if n == 1:
        # A single trial has no selection bias: E[max] == the trial's own mean (0).
        return 0.0
    a = _NORMAL.inv_cdf(1.0 - 1.0 / n)
    b = _NORMAL.inv_cdf(1.0 - 1.0 / (n * math.e))
    return sigma * ((1.0 - euler_gamma) * a + euler_gamma * b)


def deflated_sharpe_ratio(
    sr: float,
    n: int,
    skew: float,
    kurtosis: float,
    n_trials: int,
    sr_std: float,
) -> float:
    """DSR: PSR measured against the expected-max Sharpe of ``n_trials`` trials.

    A DSR ``> DSR_THRESHOLD`` (0.95) is the bar for "true structure"; at or below
    it the champion is inside the noise band of the search that found it.
    Frequency discipline is identical to :func:`probabilistic_sharpe_ratio`.
    """
    sr0 = expected_max_sharpe(n_trials, sr_std)
    return probabilistic_sharpe_ratio(sr, n, skew, kurtosis, sr_benchmark=sr0)


def mutic_adjusted_ic(
    ic_raw: float,
    max_corr_with_pool: float,
    lam: float = MUTIC_LAMBDA_DEFAULT,
) -> float:
    """Diversity-adjusted IC: ``IC_raw - lam * max_corr_with_pool``.

    ``max_corr_with_pool`` is the largest (absolute) correlation between the
    candidate and anything already in the pool.  The pool target is pairwise
    correlation below :data:`MUTIC_MAX_CORR` (0.30); a candidate at or above it
    has most of its marginal information content discounted.
    """
    ic_raw = float(ic_raw)
    corr = float(max_corr_with_pool)
    lam = float(lam)
    if not all(math.isfinite(v) for v in (ic_raw, corr, lam)):
        raise ValueError("ic_raw, max_corr_with_pool and lam must be finite")
    if lam < 0.0:
        raise ValueError("lam must be non-negative")
    return ic_raw - lam * corr


# ---------------------------------------------------------------------------
# series-level report
# ---------------------------------------------------------------------------

def _series_stats(returns: Sequence[float]) -> tuple[int, float, float, float, float]:
    """(n, mean, std(ddof=1), skew, raw kurtosis) from a finite return series."""
    arr = np.asarray([float(v) for v in returns], dtype=float)
    arr = arr[np.isfinite(arr)]
    n = int(arr.size)
    if n < 2:
        return n, float("nan"), float("nan"), float("nan"), float("nan")
    mean = float(arr.mean())
    dev = arr - mean
    m2 = float((dev * dev).mean())
    # A flat series can leave a ~1e-36 rounding residue in m2; treat any
    # variance below a relative floor as zero rather than dividing by it.
    if m2 <= (1e-12 * max(1.0, abs(mean))) ** 2:
        return n, mean, 0.0, float("nan"), float("nan")
    m3 = float((dev**3).mean())
    m4 = float((dev**4).mean())
    std = math.sqrt(float((dev * dev).sum()) / (n - 1))
    skew = m3 / m2**1.5
    kurt = m4 / m2**2  # raw kurtosis (normal == 3)
    return n, mean, std, skew, kurt


def _default_sr_std(sr: float, n: int) -> float:
    """Null sampling std of a single Sharpe estimator (per-period).

    ``sqrt((1 + 0.5*SR^2) / (n - 1))`` is the PSR denominator under iid-normal
    returns, i.e. the dispersion of the Sharpe estimator for a zero-alpha
    strategy.  Used only when the caller cannot supply the realised dispersion of
    the trial cross-section.  NOTE: a search over genuinely different strategies
    is *more* dispersed than this floor, so the default is the most favourable
    (least conservative) reading; pass ``sr_std`` explicitly for a real audit.
    """
    if n < 2:
        return float("nan")
    return math.sqrt((1.0 + 0.5 * sr * sr) / (n - 1))


def deflation_report(
    returns: Sequence[float],
    n_trials: int,
    sr_std: float | None = None,
    label: str = "",
) -> dict:
    """Full PSR/DSR report from a per-period return series.

    SR here is the **per-period Sharpe** (mean/std) and ``n`` is the number of
    periods, so the two are same-frequency by construction; no annualisation is
    applied (and none is needed — PSR/DSR are invariant to a common rescaling of
    SR, n and SR* as long as it is applied consistently).

    ``sr_std`` is the cross-sectional std of the trial Sharpe ratios.  When
    omitted it is estimated with the documented null-sampling floor and the
    result is flagged (``sr_std_is_estimate=True``); the audit must state the
    assumption rather than hide it.

    Missing / too-short series -> ``INSUFFICIENT`` (never fabricated).
    """
    n, mean, std, skew, kurt = _series_stats(returns)
    base: dict[str, Any] = {
        "label": label,
        "n_trials": int(n_trials),
        "frequency": "per-period: SR and n are both per-period (no annualisation)",
        "threshold": DSR_THRESHOLD,
    }
    if n < MIN_N_DEFLATION or not math.isfinite(std) or std <= 0.0:
        base.update(
            {
                "verdict": VERDICT_INSUFFICIENT,
                "n": n,
                "note": (
                    f"need at least {MIN_N_DEFLATION} finite observations with non-zero "
                    f"variance for skewness/kurtosis to be meaningful; got n={n}"
                ),
            }
        )
        return base

    sr = mean / std
    psr = probabilistic_sharpe_ratio(sr, n, skew, kurt, sr_benchmark=0.0)
    est = sr_std is None
    sigma = _default_sr_std(sr, n) if est else float(sr_std)
    sr0 = expected_max_sharpe(n_trials, sigma) if math.isfinite(sigma) else float("nan")
    dsr = probabilistic_sharpe_ratio(sr, n, skew, kurt, sr_benchmark=sr0)
    if not math.isfinite(dsr):
        base.update({"verdict": VERDICT_INSUFFICIENT, "n": n, "note": "DSR not computable (non-finite inputs)"})
        return base

    base.update(
        {
            "n": n,
            "mean": mean,
            "std": std,
            "sr": sr,
            "skew": skew,
            "kurtosis": kurt,
            "kurtosis_excess": kurt - NORMAL_KURTOSIS,  # reported for convenience
            "kurtosis_convention": "raw (normal==3.0); gamma_4 in the PSR denominator",
            "sr_std": sigma,
            "sr_std_is_estimate": est,
            "sr_std_assumption": (
                "estimated as sqrt((1 + 0.5*SR^2)/(n-1)) — the null sampling std of the "
                "Sharpe estimator; a lower bound on the true trial dispersion, so this is "
                "the lenient reading (pass sr_std from the trial cross-section for an audit)"
                if est
                else "supplied by the caller (realised dispersion of the trial cross-section)"
            ),
            "expected_max_sharpe": sr0,
            "psr": psr,
            "dsr": dsr,
            "verdict": VERDICT_SURVIVES if dsr > DSR_THRESHOLD else VERDICT_FAILS,
            "interpretation": (
                f"per-period SR={sr:.3f} over n={n} (skew={skew:.2f}, raw kurt={kurt:.2f}); "
                f"PSR(SR>0)={psr:.3f}; noise ceiling E[max SR] for N={int(n_trials)} trials "
                f"(sr_std={sigma:.3f}) = {sr0:.3f}; DSR={dsr:.3f} "
                f"{'>' if dsr > DSR_THRESHOLD else '<='} {DSR_THRESHOLD} -> "
                f"{'SURVIVES' if dsr > DSR_THRESHOLD else 'inside the search noise'}"
            ),
        }
    )
    return base


def deflation_from_stats(
    sr: float,
    n: int,
    n_trials: int,
    *,
    skew: float = 0.0,
    kurtosis: float = NORMAL_KURTOSIS,
    sr_std: float | None = None,
    label: str = "",
) -> dict:
    """PSR/DSR when only summary statistics are stored (no return series).

    ``sr`` and ``n`` must be same-frequency.  Skewness defaults to 0 and
    kurtosis to :data:`NORMAL_KURTOSIS` (i.e. a Gaussian assumption); when the
    defaults are used the report is flagged ``normal_assumption=True`` so a
    reader can see the correction was switched off, not that it was zero.
    """
    sr = float(sr)
    n = int(n)
    skew = float(skew)
    kurtosis = float(kurtosis)
    assumed_normal = (skew == 0.0) and (kurtosis == NORMAL_KURTOSIS)
    base: dict[str, Any] = {
        "label": label,
        "n": n,
        "sr": sr,
        "skew": skew,
        "kurtosis": kurtosis,
        "kurtosis_convention": "raw (normal==3.0); gamma_4 in the PSR denominator",
        "n_trials": int(n_trials),
        "normal_assumption": assumed_normal,
        "frequency": "caller-supplied: sr and n must be the same frequency",
        "threshold": DSR_THRESHOLD,
    }
    if n < 2 or not math.isfinite(sr):
        base.update({"verdict": VERDICT_INSUFFICIENT, "note": f"n={n} / sr={sr} not usable"})
        return base
    est = sr_std is None
    sigma = _default_sr_std(sr, n) if est else float(sr_std)
    if not math.isfinite(sigma):
        base.update({"verdict": VERDICT_INSUFFICIENT, "note": "sr_std not usable"})
        return base
    psr = probabilistic_sharpe_ratio(sr, n, skew, kurtosis, sr_benchmark=0.0)
    sr0 = expected_max_sharpe(n_trials, sigma)
    dsr = probabilistic_sharpe_ratio(sr, n, skew, kurtosis, sr_benchmark=sr0)
    if not math.isfinite(dsr):
        base.update({"verdict": VERDICT_INSUFFICIENT, "note": "DSR not computable"})
        return base
    base.update(
        {
            "sr_std": sigma,
            "sr_std_is_estimate": est,
            "sr_std_assumption": (
                "estimated as sqrt((1 + 0.5*SR^2)/(n-1)) (null sampling std)"
                if est
                else "caller-supplied"
            ),
            "expected_max_sharpe": sr0,
            "psr": psr,
            "dsr": dsr,
            "verdict": VERDICT_SURVIVES if dsr > DSR_THRESHOLD else VERDICT_FAILS,
            "interpretation": (
                f"SR={sr:.3f} over n={n} (skew={skew:.2f}, kurt={kurtosis:.2f}"
                f"{', normal assumed' if assumed_normal else ''}); PSR(SR>0)={psr:.3f}; "
                f"E[max SR] N={int(n_trials)} (sr_std={sigma:.3f})={sr0:.3f}; DSR={dsr:.3f} -> "
                f"{'SURVIVES' if dsr > DSR_THRESHOLD else 'inside the search noise'}"
            ),
        }
    )
    return base
