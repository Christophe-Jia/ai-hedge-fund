"""Robustness battery — systematic perturbation tests for a return series.

Significance, windows, boundary and consistency answer *"is the headline number
statistically real?"*.  This module answers the next question: *"is the headline
number real, or is it the arithmetic of a handful of observations?"*

Two manual findings motivate every function here:

  - **weekend_gap**: the red team removed the 2 best weekends by hand and the
    two-sided p jumped from 0.053 to 0.18 — the deployment decision collapsed.
    That is :func:`leave_k_best_out`.
  - **meta-label / GBM**: the "2 events carry 63% of gross return" diagnosis is
    :func:`concentration_profile`.

Both inputs the platform produces are supported and statistically identical
(one observation per period / per event):

  - **event-level** returns: one number per trade (``kind="events"``),
  - **monthly/period** series: one number per month (``kind="period"``).

The battery is deliberately *mechanical*: every knock-out is the same
Student-t test as :func:`src.validation.significance`, with the same ``|t| >= 2``
line, so a conclusion that survives the battery is one that survives the
headline test on every perturbation.  A strategy whose verdict is decided by
its best two observations is not a strategy.

Judgment lines (constants below carry their reasons):

  - ``FRAGILE_BY_FEW_WINNERS``  removing the top ``k<=2`` observations is enough
    to lose significance / flip sign.
  - ``FRAGILE_BY_FEW_LOSERS``   symmetric: the conclusion only exists because a
    few large losses are sitting inside the sample.
  - ``UNSTABLE_UNDER_RESAMPLING``  random dropping of the largest fraction
    (70%) still flips the sign more than 10% of the time.
  - ``FRAGILE_BY_ERA``  dropping one era flips the sign / loses significance.
  - ``SINGLE_EVENT_DRIVEN``  a single observation is > 50% of gross positive
    return ("one trade is the strategy").

RED / AMBER mapping for the audit is documented in ``docs/validation_standard.md``
§1.4 and implemented in ``scripts/validate_reports.py``.
"""

from __future__ import annotations

import math
from typing import Any, Iterable, Mapping, Sequence

import numpy as np

from .significance import significance_from_stats

# ---------------------------------------------------------------------------
# labels
# ---------------------------------------------------------------------------

ROBUST = "ROBUST"
FRAGILE = "FRAGILE"
SINGLE_EVENT_DRIVEN = "SINGLE_EVENT_DRIVEN"
INSUFFICIENT = "INSUFFICIENT"

FLAG_FEW_WINNERS = "FRAGILE_BY_FEW_WINNERS"
FLAG_FEW_LOSERS = "FRAGILE_BY_FEW_LOSERS"
FLAG_UNSTABLE_RESAMPLE = "UNSTABLE_UNDER_RESAMPLING"
FLAG_BY_ERA = "FRAGILE_BY_ERA"

# ---------------------------------------------------------------------------
# judgment lines — every constant carries the reason it was chosen
# ---------------------------------------------------------------------------

# Worst-case inspection window for leave-k-out.  Beyond 5 observations the
# "curve" stops being a fragility test and starts being the sample itself.
LEAVE_K_MAX = 5

# If the conclusion dies at k<=2, the conclusion IS those observations.  At
# k=2 the sample still holds >=80% of a >=10-observation series, so this is a
# genuine "two events decided it" statement (weekend_gap: exactly 2).
FRAGILE_K_MAX = 2

# Random-drop grid.  0.7 is the reference from the framework discussion:
# "a robust conclusion keeps its sign after 70% of the data is thrown away".
DEFAULT_FRACTIONS = (0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7)
N_TRIALS_DEFAULT = 50
SEED_DEFAULT = 42  # fixed so the sweep is reproducible (tested)

# At the largest drop fraction (70%) a sign flip should essentially never
# happen.  10% is a little slack for small-n combinatorics, not a licence.
SIGN_FLIP_RATE_MAX = 0.10

# One observation > half of gross positive return = the strategy is one event.
SINGLE_EVENT_SHARE = 0.50

# Two observations > 60% of gross positive return: the "2 events, 63% of the
# gross" diagnosis (reported, but on its own this is a warning not a verdict).
TOP2_CONCENTRATION_SHARE = 0.60

# Below this many observations every perturbation test is dominated by the
# combinatorics of removal, not by the data — say INSUFFICIENT instead.
MIN_N_BATTERY = 5

DEFAULT_T_THRESHOLD = 2.0


# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------

def _f(x: Any) -> float | None:
    """Finite float or None (keeps the output strictly JSON-serialisable)."""
    if x is None:
        return None
    try:
        x = float(x)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _finite(returns: Iterable[float]) -> np.ndarray:
    arr = np.asarray([float(v) for v in returns], dtype=float)
    return arr[np.isfinite(arr)]


def _stats(values: Any, *, t_threshold: float = DEFAULT_T_THRESHOLD, win_threshold: float = 0.0) -> dict:
    """One Student-t snapshot of a series (same lines as ``significance``).

    Returns a JSON-safe dict; ``affirmative`` is True when the series
    direction is statistically resolved (PASS/FAIL), which is the only state a
    knock-out test can *destroy*.
    """
    arr = np.asarray(values, dtype=float)
    arr = arr[np.isfinite(arr)]
    n = int(arr.size)
    if n == 0:
        return {
            "n": 0, "mean": None, "median": None, "std": None, "se": None,
            "t_stat": None, "p_value": None, "win_rate": None,
            "verdict": INSUFFICIENT, "direction": "none", "sign": 0,
            "affirmative": False,
        }
    mean = float(arr.mean())
    std = float(arr.std(ddof=1)) if n > 1 else None
    sig = significance_from_stats(mean, std, n, t_threshold=t_threshold)
    wins = int((arr > float(win_threshold)).sum())
    return {
        "n": n,
        "mean": _f(mean),
        "median": _f(float(np.median(arr))),
        "std": _f(std),
        "se": sig["se"],
        "t_stat": sig["t_stat"],
        "p_value": sig["p_value"],
        "win_rate": _f(wins / n),
        "verdict": sig["verdict"],
        "direction": sig["direction"],
        "sign": int(np.sign(mean)),
        "affirmative": sig["verdict"] in ("PASS", "FAIL"),
    }


def _retained(mean: float | None, base_mean: float | None) -> float | None:
    if mean is None or base_mean is None or base_mean == 0.0:
        return None
    return _f(mean / base_mean)


# ---------------------------------------------------------------------------
# 1. leave-k-best-out
# ---------------------------------------------------------------------------

def leave_k_best_out(
    returns: Iterable[float],
    k: int | None = None,
    *,
    t_threshold: float = DEFAULT_T_THRESHOLD,
    fragile_k_max: int = FRAGILE_K_MAX,
    kind: str = "events",
    label: str = "",
) -> dict:
    """Remove the k largest observations and re-run the headline test.

    ``k`` defaults to ``min(5, n//4)``.  The curve is reported from k=0 to
    k_max, together with:

      - ``first_failure_k`` — smallest k>=1 whose verdict is no longer the
        baseline affirmative verdict (PASS/FAIL);
      - ``sign_flip_k`` — smallest k>=1 whose mean changes sign;
      - ``n_best_to_sustain`` — the number of best observations the conclusion
        needs (equal to ``first_failure_k`` when it is fragile).

    ``FRAGILE_BY_FEW_WINNERS`` when the baseline is affirmative and it dies at
    ``k <= fragile_k_max`` (default 2): the conclusion is arithmetic of those
    observations, not a property of the strategy.
    """
    arr = _finite(returns)
    n = int(arr.size)
    base = _stats(arr, t_threshold=t_threshold)
    if n == 0:
        return {
            "label": label, "kind": kind, "test": "leave_k_best_out",
            "n": 0, "k_max": 0, "t_threshold": float(t_threshold),
            "baseline": base, "curve": [], "first_failure_k": None,
            "sign_flip_k": None, "n_best_to_sustain": None, "fragile": False,
            "flag": None, "note": "no finite observations",
        }

    k_max = int(k) if k is not None else min(LEAVE_K_MAX, n // 4)
    k_max = max(0, min(k_max, n - 1))  # never leave an empty sample

    asc = np.sort(arr)  # ascending: the k best are the last k
    base_verdict = base["verdict"]
    base_sign = base["sign"]
    base_affirm = base["affirmative"]

    curve: list[dict] = []
    first_failure: int | None = None
    sign_flip: int | None = None
    for kk in range(0, k_max + 1):
        sub = asc[: n - kk] if kk > 0 else arr
        st = _stats(sub, t_threshold=t_threshold)
        row: dict[str, Any] = {
            "k": kk,
            "n": st["n"],
            "mean": st["mean"],
            "std": st["std"],
            "t_stat": st["t_stat"],
            "p_value": st["p_value"],
            "win_rate": st["win_rate"],
            "verdict": st["verdict"],
            "sign": st["sign"],
            "delta_mean": _f(
                (st["mean"] - base["mean"]) if st["mean"] is not None and base["mean"] is not None else None
            ),
            "mean_retained_frac": _retained(st["mean"], base["mean"]),
        }
        if kk > 0:
            row["removed"] = [_f(x) for x in asc[n - kk :][::-1]]
        curve.append(row)
        if kk > 0:
            if sign_flip is None and base_sign != 0 and st["sign"] != 0 and st["sign"] != base_sign:
                sign_flip = kk
            if first_failure is None and base_affirm and st["verdict"] != base_verdict:
                first_failure = kk

    fragile = bool(base_affirm and first_failure is not None and first_failure <= fragile_k_max)
    return {
        "label": label,
        "kind": kind,
        "test": "leave_k_best_out",
        "n": n,
        "k_max": k_max,
        "t_threshold": float(t_threshold),
        "fragile_k_max": int(fragile_k_max),
        "baseline": base,
        "curve": curve,
        "first_failure_k": first_failure,
        "sign_flip_k": sign_flip,
        "n_best_to_sustain": first_failure if fragile else None,
        "fragile": fragile,
        "flag": FLAG_FEW_WINNERS if fragile else None,
        "note": (
            f"the conclusion needs its best {first_failure} observation(s)"
            if fragile
            else "the affirmative verdict survives removing the k best observations"
            if base_affirm
            else "baseline is not affirmative (NOISE/INSUFFICIENT): no claim to destroy"
        ),
    }


# ---------------------------------------------------------------------------
# 2. leave-k-worst-out
# ---------------------------------------------------------------------------

def leave_k_worst_out(
    returns: Iterable[float],
    k: int | None = None,
    *,
    t_threshold: float = DEFAULT_T_THRESHOLD,
    fragile_k_max: int = FRAGILE_K_MAX,
    kind: str = "events",
    label: str = "",
) -> dict:
    """Symmetric knock-out: remove the k *smallest* observations.

    A positive strategy usually gets *stronger* here (removing losses helps).
    The failure mode this catches is the opposite: a conclusion that only exists
    because a few large losses are pulling the mean down — remove them and the
    sign flips.  That is ``FRAGILE_BY_FEW_LOSERS``.
    """
    arr = _finite(returns)
    n = int(arr.size)
    base = _stats(arr, t_threshold=t_threshold)
    if n == 0:
        return {
            "label": label, "kind": kind, "test": "leave_k_worst_out",
            "n": 0, "k_max": 0, "t_threshold": float(t_threshold),
            "baseline": base, "curve": [], "first_failure_k": None,
            "sign_flip_k": None, "n_worst_to_sustain": None, "fragile": False,
            "flag": None, "note": "no finite observations",
        }

    k_max = int(k) if k is not None else min(LEAVE_K_MAX, n // 4)
    k_max = max(0, min(k_max, n - 1))

    asc = np.sort(arr)  # ascending: the k worst are the first k
    base_verdict = base["verdict"]
    base_sign = base["sign"]
    base_affirm = base["affirmative"]

    curve: list[dict] = []
    first_failure: int | None = None
    sign_flip: int | None = None
    for kk in range(0, k_max + 1):
        sub = asc[kk:] if kk > 0 else arr
        st = _stats(sub, t_threshold=t_threshold)
        row: dict[str, Any] = {
            "k": kk,
            "n": st["n"],
            "mean": st["mean"],
            "std": st["std"],
            "t_stat": st["t_stat"],
            "p_value": st["p_value"],
            "win_rate": st["win_rate"],
            "verdict": st["verdict"],
            "sign": st["sign"],
            "delta_mean": _f(
                (st["mean"] - base["mean"]) if st["mean"] is not None and base["mean"] is not None else None
            ),
            "mean_retained_frac": _retained(st["mean"], base["mean"]),
        }
        if kk > 0:
            row["removed"] = [_f(x) for x in asc[:kk]]
        curve.append(row)
        if kk > 0:
            if sign_flip is None and base_sign != 0 and st["sign"] != 0 and st["sign"] != base_sign:
                sign_flip = kk
            if first_failure is None and base_affirm and st["verdict"] != base_verdict:
                first_failure = kk

    fragile = bool(base_affirm and first_failure is not None and first_failure <= fragile_k_max)
    return {
        "label": label,
        "kind": kind,
        "test": "leave_k_worst_out",
        "n": n,
        "k_max": k_max,
        "t_threshold": float(t_threshold),
        "fragile_k_max": int(fragile_k_max),
        "baseline": base,
        "curve": curve,
        "first_failure_k": first_failure,
        "sign_flip_k": sign_flip,
        "n_worst_to_sustain": first_failure if fragile else None,
        "fragile": fragile,
        "flag": FLAG_FEW_LOSERS if fragile else None,
        "note": (
            f"the conclusion needs its {first_failure} worst observation(s) removed to reverse"
            if fragile
            else "no loss-driven reversal within k_max"
        ),
    }


# ---------------------------------------------------------------------------
# 3. random drop-fraction sweep
# ---------------------------------------------------------------------------

def drop_fraction_sweep(
    returns: Iterable[float],
    fractions: Sequence[float] = DEFAULT_FRACTIONS,
    *,
    n_trials: int = N_TRIALS_DEFAULT,
    seed: int = SEED_DEFAULT,
    t_threshold: float = DEFAULT_T_THRESHOLD,
    sign_flip_rate_max: float = SIGN_FLIP_RATE_MAX,
    kind: str = "events",
    label: str = "",
) -> dict:
    """Randomly drop x% of observations and re-run the headline test.

    For every drop fraction this draws ``n_trials`` independent subsets
    (without replacement) and reports the distribution of the mean, the
    t-statistic, the sign-flip rate and the verdict-change rate.  The reference
    line: **a robust conclusion keeps its sign after dropping 70% of the data**
    (``sign_flip_rate`` at the largest fraction <= ``sign_flip_rate_max``).

    Deterministic for a fixed ``seed`` (each fraction gets ``seed + i`` so the
    result is stable and fractions do not consume each other's RNG stream).
    """
    arr = _finite(returns)
    n = int(arr.size)
    base = _stats(arr, t_threshold=t_threshold)
    fracs = [float(f) for f in fractions]
    if n == 0 or not fracs:
        return {
            "label": label, "kind": kind, "test": "drop_fraction_sweep",
            "n": n, "n_trials": int(n_trials), "seed": int(seed),
            "baseline": base, "curve": [], "sign_stable_through": None,
            "flag": None, "note": "no finite observations",
        }

    base_sign = base["sign"]
    base_verdict = base["verdict"]
    base_affirm = base["affirmative"]

    curve: list[dict] = []
    sign_stable_through: float | None = None
    for i, frac in enumerate(fracs):
        n_drop = int(round(frac * n))
        n_keep = n - n_drop
        row: dict[str, Any] = {"fraction": frac, "n_drop": n_drop, "n_keep": n_keep}
        if n_keep < 2:
            row.update({"mean_of_means": None, "sign_flip_rate": None, "note": "keeps <2 observations"})
            curve.append(row)
            continue
        rng = np.random.default_rng(int(seed) + i)
        means: list[float] = []
        ts: list[float] = []
        t_values: list[float] = []
        signs: list[int] = []
        verdicts: list[str] = []
        for _ in range(int(n_trials)):
            idx = rng.choice(n, size=n_keep, replace=False)
            st = _stats(arr[idx], t_threshold=t_threshold)
            if st["mean"] is None:
                continue
            means.append(float(st["mean"]))
            signs.append(int(st["sign"]))
            verdicts.append(str(st["verdict"]))
            if st["t_stat"] is not None:
                ts.append(float(st["t_stat"]))
        if not means:
            row.update({"mean_of_means": None, "sign_flip_rate": None, "note": "no valid trials"})
            curve.append(row)
            continue
        m = np.asarray(means, dtype=float)
        t_arr = np.asarray(ts, dtype=float) if ts else np.asarray([], dtype=float)
        s_arr = np.asarray(signs, dtype=int)
        sign_flip_rate = (
            float(np.mean(s_arr != base_sign)) if base_sign != 0 else None
        )
        verdict_change_rate = float(np.mean(np.asarray(verdicts) != base_verdict))
        row.update(
            {
                "mean_of_means": _f(m.mean()),
                "median_of_means": _f(np.median(m)),
                "mean_min": _f(m.min()),
                "mean_max": _f(m.max()),
                "frac_positive": _f(float((m > 0).mean())),
                "t_median": _f(np.median(t_arr)) if t_arr.size else None,
                "t_p05": _f(np.percentile(t_arr, 5)) if t_arr.size else None,
                "t_p95": _f(np.percentile(t_arr, 95)) if t_arr.size else None,
                "sign_flip_rate": sign_flip_rate,
                "verdict_change_rate": verdict_change_rate,
                "pass_rate": _f(float(np.mean(np.asarray(verdicts) == "PASS"))),
            }
        )
        if sign_flip_rate is not None and sign_flip_rate <= sign_flip_rate_max:
            sign_stable_through = frac
        curve.append(row)

    max_frac_row = curve[-1] if curve else None
    flag = None
    if max_frac_row and max_frac_row.get("sign_flip_rate") is not None:
        if max_frac_row["sign_flip_rate"] > sign_flip_rate_max:
            flag = FLAG_UNSTABLE_RESAMPLE
    return {
        "label": label,
        "kind": kind,
        "test": "drop_fraction_sweep",
        "n": n,
        "n_trials": int(n_trials),
        "seed": int(seed),
        "t_threshold": float(t_threshold),
        "sign_flip_rate_max": float(sign_flip_rate_max),
        "baseline": base,
        "baseline_sign": base_sign,
        "baseline_verdict": base_verdict,
        "baseline_affirmative": base_affirm,
        "curve": curve,
        "sign_stable_through": sign_stable_through,
        "flag": flag,
        "note": (
            f"sign still stable after dropping {sign_stable_through:.0%} of observations"
            if sign_stable_through is not None
            else "sign is not stable under resampling"
        ),
    }


# ---------------------------------------------------------------------------
# 4. leave-one-era-out
# ---------------------------------------------------------------------------

def _normalise_eras(returns: Any, eras: Any) -> tuple[np.ndarray, np.ndarray]:
    """Coerce era input into (values, labels) arrays.

    Accepted forms:
      - ``eras`` is a sequence of labels, one per return (aligned);
      - ``eras`` is a mapping ``{era: [returns]}``;
      - ``eras`` is a sequence of ``(era, [returns])`` pairs.
    """
    if isinstance(eras, Mapping):
        vals: list[float] = []
        labs: list[str] = []
        for lab, xs in eras.items():
            for x in xs:
                vals.append(float(x))
                labs.append(str(lab))
        return _finite(np.asarray(vals, dtype=float)), np.asarray(labs, dtype=object)

    seq = list(eras)
    if seq and all(isinstance(x, (tuple, list)) and len(x) == 2 and not np.isscalar(x[1]) for x in seq):
        vals = []
        labs = []
        for lab, xs in seq:
            for x in xs:
                vals.append(float(x))
                labs.append(str(lab))
        return _finite(np.asarray(vals, dtype=float)), np.asarray(labs, dtype=object)

    arr = _finite(returns)
    labels = np.asarray([str(x) for x in seq], dtype=object)
    if labels.size != arr.size:
        raise ValueError(
            f"eras must align with returns (len {arr.size}) or be a mapping/pairs; got len {labels.size}"
        )
    return arr, labels


def leave_one_era_out(
    returns: Iterable[float],
    eras: Any,
    *,
    t_threshold: float = DEFAULT_T_THRESHOLD,
    kind: str = "events",
    label: str = "",
) -> dict:
    """Remove one regime/era at a time and re-run the headline test.

    This is the complement of an era split: instead of asking "what does each
    era look like?", it asks *"what is left when this era is gone?"* — i.e.
    "if the future has no 2020-22 ZIRP regime, does this still hold?".

    ``FRAGILE_BY_ERA`` when dropping one era flips the sign, or makes an
    affirmative verdict non-affirmative.
    """
    try:
        arr, labels = _normalise_eras(returns, eras)
    except ValueError as exc:
        base = _stats(_finite(returns), t_threshold=t_threshold)
        return {
            "label": label, "kind": kind, "test": "leave_one_era_out",
            "n": int(base["n"]), "baseline": base, "eras": [],
            "worst_era": None, "flag": None, "note": str(exc),
        }
    n = int(arr.size)
    base = _stats(arr, t_threshold=t_threshold)
    if n == 0 or labels.size == 0:
        return {
            "label": label, "kind": kind, "test": "leave_one_era_out",
            "n": 0, "baseline": base, "eras": [], "worst_era": None,
            "flag": None, "note": "no era-labelled observations",
        }

    base_sign = base["sign"]
    base_verdict = base["verdict"]
    base_affirm = base["affirmative"]

    rows: list[dict] = []
    for era in sorted({str(x) for x in labels.tolist()}):
        mask = labels != era
        sub = arr[np.asarray(mask, dtype=bool)]
        st = _stats(sub, t_threshold=t_threshold)
        sign_flip = bool(base_sign != 0 and st["sign"] != 0 and st["sign"] != base_sign)
        loses_significance = bool(base_affirm and st["verdict"] != base_verdict)
        rows.append(
            {
                "era": era,
                "n_excluded": int(n - st["n"]),
                "n_remaining": st["n"],
                "mean": st["mean"],
                "std": st["std"],
                "t_stat": st["t_stat"],
                "p_value": st["p_value"],
                "verdict": st["verdict"],
                "sign": st["sign"],
                "delta_mean": _f(
                    (st["mean"] - base["mean"]) if st["mean"] is not None and base["mean"] is not None else None
                ),
                "delta_t": _f(
                    (st["t_stat"] - base["t_stat"])
                    if st["t_stat"] is not None and base["t_stat"] is not None
                    else None
                ),
                "sign_flip": sign_flip,
                "loses_significance": loses_significance,
            }
        )

    def _dependence(row: dict) -> float:
        dt = abs(row["delta_t"]) if row["delta_t"] is not None else 0.0
        return dt + (10.0 if (row["sign_flip"] or row["loses_significance"]) else 0.0)

    worst = max(rows, key=_dependence) if rows else None
    fragile = any(r["sign_flip"] or r["loses_significance"] for r in rows)
    return {
        "label": label,
        "kind": kind,
        "test": "leave_one_era_out",
        "n": n,
        "n_eras": len(rows),
        "t_threshold": float(t_threshold),
        "baseline": base,
        "eras": rows,
        "worst_era": worst["era"] if worst else None,
        "max_abs_delta_t": max((abs(r["delta_t"]) for r in rows if r["delta_t"] is not None), default=None),
        "fragile": bool(fragile),
        "flag": FLAG_BY_ERA if fragile else None,
        "note": (
            f"conclusion flips when era '{worst['era']}' is removed" if fragile and worst else
            "conclusion holds when any single era is removed"
        ),
    }


# ---------------------------------------------------------------------------
# 5. concentration profile
# ---------------------------------------------------------------------------

def concentration_profile(
    returns: Iterable[float],
    *,
    top_frac: float = 0.05,
    kind: str = "events",
    label: str = "",
) -> dict:
    """How much of the result is a handful of observations?

    Reports top-1/2/3 share of **gross positive return** (the "2 events are 63%
    of gross" metric), the Herfindahl index over positive contributions, the
    largest single contribution, and the return that remains once the top
    ``top_frac`` observations are deleted.

    ``single_event_driven`` when one observation is >= 50% of gross positive
    return.  ``top2_concentrated`` is a WARN (>= 60%) on its own.
    """
    arr = _finite(returns)
    n = int(arr.size)
    if n == 0:
        return {
            "label": label, "kind": kind, "test": "concentration_profile",
            "n": 0, "top1_share": None, "top2_share": None, "top3_share": None,
            "hhi": None, "effective_n": None, "single_event_driven": False,
            "top2_concentrated": False, "flag": None, "note": "no finite observations",
        }

    pos = arr[arr > 0]
    neg = arr[arr < 0]
    gross_positive = float(pos.sum())
    gross_negative = float(neg.sum())
    net_total = float(arr.sum())

    desc = np.sort(arr)[::-1]
    top1 = float(desc[0])
    top2 = float(desc[:2].sum())
    top3 = float(desc[:3].sum())

    def share(x: float) -> float | None:
        return _f(x / gross_positive) if gross_positive > 0 else None

    if gross_positive > 0 and pos.size:
        p = pos / gross_positive
        hhi = float(np.sum(p ** 2))
        effective_n = float(1.0 / hhi) if hhi > 0 else None
    else:
        hhi = None
        effective_n = None

    n_top = max(1, int(math.ceil(float(top_frac) * n)))
    ex_top = np.sort(arr)[: n - n_top] if n_top < n else np.asarray([], dtype=float)
    ex_stats = _stats(ex_top)
    top1_share = share(top1)
    top2_share = share(top2)
    top3_share = share(top3)

    single_event = bool(top1_share is not None and top1_share >= SINGLE_EVENT_SHARE)
    top2_concentrated = bool(top2_share is not None and top2_share >= TOP2_CONCENTRATION_SHARE)

    return {
        "label": label,
        "kind": kind,
        "test": "concentration_profile",
        "n": n,
        "gross_positive": _f(gross_positive),
        "gross_negative": _f(gross_negative),
        "net_total": _f(net_total),
        "top1": _f(top1),
        "top2": _f(top2),
        "top3": _f(top3),
        "top1_share": top1_share,
        "top2_share": top2_share,
        "top3_share": top3_share,
        "hhi": _f(hhi),
        "effective_n": _f(effective_n),
        "max_single_share": top1_share,
        "top_frac_dropped": float(top_frac),
        "n_dropped_top_frac": int(n_top),
        "return_ex_top_frac_sum": _f(ex_stats["mean"] * ex_stats["n"]) if ex_stats["n"] else None,
        "return_ex_top_frac_mean": ex_stats["mean"],
        "return_ex_top_frac_verdict": ex_stats["verdict"],
        "single_event_driven": single_event,
        "top2_concentrated": top2_concentrated,
        "flag": SINGLE_EVENT_DRIVEN if single_event else None,
        "note": (
            "a single observation is >=50% of gross positive return"
            if single_event
            else f"top-2 observations are {top2_share:.0%} of gross positive return"
            if top2_concentrated and top2_share is not None
            else "no single observation dominates the gross return"
        ),
    }


# ---------------------------------------------------------------------------
# 6. the battery
# ---------------------------------------------------------------------------

def robustness_battery(
    returns: Iterable[float],
    eras: Any = None,
    *,
    label: str = "",
    kind: str = "events",
    t_threshold: float = DEFAULT_T_THRESHOLD,
    drop_fractions: Sequence[float] = DEFAULT_FRACTIONS,
    n_trials: int = N_TRIALS_DEFAULT,
    seed: int = SEED_DEFAULT,
) -> dict:
    """Run all six perturbation checks and return one verdict.

    Verdict priority:
      1. ``INSUFFICIENT``          — fewer than ``MIN_N_BATTERY`` observations;
      2. ``SINGLE_EVENT_DRIVEN``   — one observation is >=50% of gross return;
      3. ``FRAGILE``               — any knock-out flag fires;
      4. ``ROBUST``                — survives every perturbation.

    ``flags`` carries the specific diagnosis (``FRAGILE_BY_FEW_WINNERS`` etc.)
    so downstream triage can map it to RED/AMBER.
    """
    arr = _finite(returns)
    n = int(arr.size)
    flags: list[str] = []

    concentration = concentration_profile(arr, kind=kind, label=label)
    leave_best = leave_k_best_out(arr, t_threshold=t_threshold, kind=kind, label=label)
    leave_worst = leave_k_worst_out(arr, t_threshold=t_threshold, kind=kind, label=label)
    sweep = drop_fraction_sweep(
        arr, drop_fractions, n_trials=n_trials, seed=seed,
        t_threshold=t_threshold, kind=kind, label=label,
    )
    era = leave_one_era_out(arr, eras, t_threshold=t_threshold, kind=kind, label=label) if eras is not None else None

    if concentration.get("single_event_driven"):
        flags.append(SINGLE_EVENT_DRIVEN)
    if leave_best.get("flag"):
        flags.append(leave_best["flag"])
    if leave_worst.get("flag"):
        flags.append(leave_worst["flag"])
    if sweep.get("flag"):
        flags.append(sweep["flag"])
    if era and era.get("flag"):
        flags.append(era["flag"])

    if n < MIN_N_BATTERY:
        # below MIN_N every knock-out statistic is combinatorics, not evidence:
        # report no diagnosis rather than a misleading one.
        verdict = INSUFFICIENT
        flags = []
    elif SINGLE_EVENT_DRIVEN in flags:
        verdict = SINGLE_EVENT_DRIVEN
    elif flags:
        verdict = FRAGILE
    else:
        verdict = ROBUST

    base = leave_best["baseline"]
    return {
        "label": label,
        "kind": kind,
        "unit": "per-event return" if kind == "events" else "per-period return",
        "n": n,
        "verdict": verdict,
        "flags": flags,
        "t_threshold": float(t_threshold),
        "min_n": int(MIN_N_BATTERY),
        "baseline": base,
        "leave_k_best_out": leave_best,
        "leave_k_worst_out": leave_worst,
        "drop_fraction_sweep": sweep,
        "leave_one_era_out": era,
        "concentration": concentration,
        "thresholds": {
            "t_threshold": float(t_threshold),
            "fragile_k_max": FRAGILE_K_MAX,
            "leave_k_max": LEAVE_K_MAX,
            "sign_flip_rate_max": SIGN_FLIP_RATE_MAX,
            "single_event_share": SINGLE_EVENT_SHARE,
            "top2_concentration_share": TOP2_CONCENTRATION_SHARE,
            "min_n": MIN_N_BATTERY,
        },
        "summary": _summary(verdict, base, leave_best, concentration, sweep),
    }


def _summary(verdict: str, base: dict, leave_best: dict, concentration: dict, sweep: dict) -> str:
    if verdict == INSUFFICIENT:
        return f"n={base.get('n')} < {MIN_N_BATTERY}: too few observations to perturb-test"
    if verdict == SINGLE_EVENT_DRIVEN:
        s = concentration.get("top1_share")
        return f"a single observation is {s:.0%} of gross positive return" if s is not None else "single-event driven"
    if verdict == FRAGILE:
        parts = []
        if leave_best.get("flag"):
            parts.append(f"best {leave_best['first_failure_k']} observation(s) carry the conclusion")
        if concentration.get("top2_concentrated"):
            s = concentration.get("top2_share")
            parts.append(f"top-2 = {s:.0%} of gross return" if s is not None else "top-2 concentrated")
        if sweep.get("flag"):
            parts.append("sign flips under 70% random drop")
        return "; ".join(parts) or "fails at least one perturbation test"
    return "survives leave-k-out, random-drop and concentration checks"


__all__ = [
    "robustness_battery",
    "leave_k_best_out",
    "leave_k_worst_out",
    "drop_fraction_sweep",
    "leave_one_era_out",
    "concentration_profile",
    "ROBUST",
    "FRAGILE",
    "SINGLE_EVENT_DRIVEN",
    "INSUFFICIENT",
    "FLAG_FEW_WINNERS",
    "FLAG_FEW_LOSERS",
    "FLAG_UNSTABLE_RESAMPLE",
    "FLAG_BY_ERA",
    "FRAGILE_K_MAX",
    "LEAVE_K_MAX",
    "DEFAULT_FRACTIONS",
    "SIGN_FLIP_RATE_MAX",
    "SINGLE_EVENT_SHARE",
    "MIN_N_BATTERY",
]
