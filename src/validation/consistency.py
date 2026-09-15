"""Internal consistency: does the claimed Sharpe match the claimed IC?

Motivation (the 2026-09 GBM credibility crisis): the report claimed a Sharpe of
0.963 alongside a mean monthly IC of 0.0082 over 71 months.  The fundamental law
of active management ties skill (IC) to risk-adjusted excess return::

    expected IR ~= IC * sqrt(breadth)

Using the reported numbers as the breadth, IC 0.0082 over 71 months implies an
expected IR of 0.0082 * sqrt(71) ~= 0.07 — an order of magnitude below the
reported 0.96.  Either the "IC" is not the IC that drove the P&L, or the Sharpe
is not what it appears.  Two numbers that cannot both be true sat in the same
report; this check says so automatically.
"""

from __future__ import annotations

import math

import numpy as np

VERDICT_CONSISTENT = "CONSISTENT"
VERDICT_IMPLAUSIBLE = "IMPLAUSIBLE"
VERDICT_INSUFFICIENT = "INSUFFICIENT"


def internal_consistency(
    ic: float,
    sharpe: float,
    turnover: float = 0.0,
    n_months: int = 1,
    *,
    benchmark_sharpe: float = 0.0,
    max_ratio: float = 3.0,
    high_turnover: float = 4.0,
    label: str | None = None,
) -> dict:
    """Triangulate the reported IC, Sharpe and sample length for plausibility.

    Args:
        ic: mean per-period information coefficient (spearman/pearson rank IC).
        sharpe: headline Sharpe of the strategy.
        turnover: optional annual one-side turnover ratio (context only; it
            widens the gross/net gap but is not part of the IC identity).
        n_months: number of independent periods behind the IC.
        benchmark_sharpe: benchmark Sharpe; the IC identity describes *active*
            return, so the test uses sharpe - benchmark_sharpe when supplied.
        max_ratio: how many times the theoretically implied IR the realised
            active Sharpe may reach before being called IMPLAUSIBLE.
    """
    ic = float(ic)
    sharpe = float(sharpe)
    n_months = int(n_months)
    benchmark_sharpe = float(benchmark_sharpe)

    active_sharpe = sharpe - benchmark_sharpe
    expected_ir = ic * math.sqrt(n_months) if n_months > 0 else float("nan")

    notes: list[str] = []
    if n_months < 2 or not np.isfinite(ic) or not np.isfinite(sharpe) or not np.isfinite(expected_ir):
        verdict = VERDICT_INSUFFICIENT
        ratio = None
        required_ic = None
        notes.append("need ic, sharpe and n_months>=2 to run the identity")
    else:
        required_ic = active_sharpe / math.sqrt(n_months)
        if expected_ir <= 0.0:
            ratio = None
            if active_sharpe > 0.0:
                verdict = VERDICT_IMPLAUSIBLE
                notes.append("positive active Sharpe with no positive-IC basis")
            else:
                verdict = VERDICT_CONSISTENT
                notes.append("no positive skill claimed; nothing to contradict")
        else:
            ratio = active_sharpe / expected_ir
            if ratio > max_ratio:
                verdict = VERDICT_IMPLAUSIBLE
                notes.append(f"active Sharpe {active_sharpe:.3f} is {ratio:.1f}x the IC-implied IR {expected_ir:.4f}")
            else:
                verdict = VERDICT_CONSISTENT

    if turnover and abs(turnover) >= high_turnover:
        notes.append(f"high turnover ({turnover:g}x/yr): net-of-cost Sharpe must be reported separately")

    return {
        "label": label,
        "ic": ic,
        "sharpe": sharpe,
        "benchmark_sharpe": benchmark_sharpe,
        "active_sharpe": active_sharpe,
        "n_months": n_months,
        "breadth_sqrt": math.sqrt(n_months) if n_months > 0 else None,
        "expected_ir": expected_ir if np.isfinite(expected_ir) else None,
        "required_ic": required_ic,
        "ratio": ratio,
        "max_ratio": float(max_ratio),
        "turnover": float(turnover),
        "verdict": verdict,
        "notes": notes,
    }
