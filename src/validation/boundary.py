"""Boundary stability of a top-N selection: is the cut decided by signal or by ties?

Motivation (the 2026-09 GBM credibility crisis): `run_monthly_gbm.py --as-of
2021-06` reported MISMATCH (2/10 overlap) against the stored picks, because five
stocks carried *identical* model scores and the top-10 boundary was therefore
decided by sort-tie order.  Adding one row of data (BAYRY) or 21 extra stocks
flipped a whole month's selection.  When the boundary is arbitrary, the ranking
is not a signal — it is a coin flip.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

VERDICT_STABLE = "STABLE"
VERDICT_ARBITRARY = "ARBITRARY"
VERDICT_INSUFFICIENT = "INSUFFICIENT"


def boundary_stability(
    scores_df: pd.DataFrame | pd.Series,
    top_n: int = 10,
    *,
    n_perturb: int = 20,
    drop_frac: float = 0.01,
    noise_scale: float = 1e-6,
    flip_threshold: float = 0.20,
    seed: int = 42,
    label: str | None = None,
) -> dict:
    """Rank each cross-section, perturb it slightly, and measure top-N churn.

    Args:
        scores_df: index = cross-section (e.g. month), columns = symbols,
            values = scores.  A Series is treated as a single cross-section.
        top_n: selection size (the boundary whose arbitrariness is being probed).
        n_perturb: number of perturbation draws per cross-section.
        drop_frac: fraction of names randomly dropped per draw (0 disables).
        noise_scale: gaussian noise magnitude as a fraction of the cross-section
            std (breaks exact ties without reordering well-separated scores).
        flip_threshold: mean top-N churn above which the boundary is ARBITRARY.

    Reports tie structure (unique values, tied share, names sharing the boundary
    score) plus the mean top-N flip rate under perturbation.  Ties are what make
    the flip rate high; a score vector with clean gaps should survive both
    perturbations untouched.
    """
    if isinstance(scores_df, pd.Series):
        df = scores_df.to_frame().T
    else:
        df = pd.DataFrame(scores_df)

    rng = np.random.default_rng(seed)
    per_section: list[dict] = []
    flips: list[float] = []
    tie_ratios: list[float] = []
    boundary_tie_counts: list[int] = []
    n_skipped = 0

    for idx, row in df.iterrows():
        s = pd.to_numeric(row, errors="coerce").dropna().astype(float)
        if len(s) < top_n:
            n_skipped += 1
            continue

        base = _top_n(s, top_n)
        n_valid = int(len(s))
        n_unique = int(s.nunique())
        tie_ratio = 1.0 - n_unique / n_valid
        boundary_score = float(base.iloc[-1])
        boundary_ties = int((s == boundary_score).sum())
        # duplicates strictly inside the top-N (a tie that already shaped the cut)
        top_scores = base.to_numpy()
        duplicates_in_top = int(len(top_scores) - len(np.unique(top_scores)))

        this_flips: list[float] = []
        for _ in range(int(n_perturb)):
            pert = s
            if drop_frac > 0:
                k = max(1, int(round(drop_frac * n_valid)))
                drop = rng.choice(s.index.to_numpy(), size=min(k, n_valid), replace=False)
                pert = pert.drop(index=drop)
            if noise_scale > 0:
                sd = float(s.std())
                scale = noise_scale * (sd if sd > 0 else 1.0)
                # ndarray is added positionally; the Series index is preserved.
                pert = pert + rng.normal(0.0, scale, size=len(pert))
            if len(pert) < top_n:
                continue
            new = _top_n(pert, top_n)
            overlap = len(set(new.index) & set(base.index))
            this_flips.append(1.0 - overlap / top_n)

        flip_rate = float(np.mean(this_flips)) if this_flips else None
        per_section.append(
            {
                "section": _label(idx),
                "n_symbols": n_valid,
                "n_unique_scores": n_unique,
                "tie_ratio": tie_ratio,
                "boundary_score": boundary_score,
                "boundary_ties": boundary_ties,
                "duplicates_in_top_n": duplicates_in_top,
                "flip_rate": flip_rate,
            }
        )
        tie_ratios.append(tie_ratio)
        boundary_tie_counts.append(boundary_ties)
        if flip_rate is not None:
            flips.append(flip_rate)

    if not per_section:
        return {
            "label": label,
            "top_n": top_n,
            "n_cross_sections": int(len(df)),
            "n_evaluated": 0,
            "n_skipped": n_skipped,
            "verdict": VERDICT_INSUFFICIENT,
            "note": f"no cross-section had at least top_n={top_n} valid scores",
            "per_section": [],
        }

    mean_flip = float(np.mean(flips)) if flips else None
    max_flip = float(np.max(flips)) if flips else None
    if mean_flip is None:
        verdict = VERDICT_INSUFFICIENT
    elif mean_flip > flip_threshold:
        verdict = VERDICT_ARBITRARY
    else:
        verdict = VERDICT_STABLE

    return {
        "label": label,
        "top_n": top_n,
        "n_cross_sections": int(len(df)),
        "n_evaluated": len(per_section),
        "n_skipped": n_skipped,
        "n_perturb": int(n_perturb),
        "drop_frac": float(drop_frac),
        "noise_scale": float(noise_scale),
        "flip_threshold": float(flip_threshold),
        "mean_flip_rate": mean_flip,
        "max_flip_rate": max_flip,
        "mean_tie_ratio": float(np.mean(tie_ratios)),
        "max_tie_ratio": float(np.max(tie_ratios)),
        "mean_boundary_ties": float(np.mean(boundary_tie_counts)),
        "max_boundary_ties": int(np.max(boundary_tie_counts)),
        "sections_with_boundary_ties": int(sum(1 for c in boundary_tie_counts if c >= 2)),
        "has_exact_ties": bool(np.max(tie_ratios) > 0 or np.max(boundary_tie_counts) >= 2),
        "verdict": verdict,
        "per_section": per_section,
    }


def _top_n(s: pd.Series, top_n: int) -> pd.Series:
    # mergesort keeps insertion order for equal scores -> the *arbitrary* part.
    return s.sort_values(ascending=False, kind="mergesort").head(top_n)


def _label(idx: Any) -> Any:
    if isinstance(idx, (str, int, float, bool)) or idx is None:
        return idx
    return str(idx)
