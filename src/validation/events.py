"""Event-level significance and time-segment distributions.

The platform's event-driven strategies (weekend_gap, funding-rate, FOMC) do not
have a monthly IC series to test — they have a handful of trades.  A 25-trade
backtest needs a *different* statistic than a 71-month one: a Student-t p-value
(not the normal approximation), a bootstrap CI for the mean, and a Wilson
interval for the win rate.  This module is that entry point.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from .stats import bootstrap_ci_mean, proportion_z, two_sided_t_p, wilson_interval
from .windows import multi_window

VERDICT_PASS = "PASS"
VERDICT_FAIL = "FAIL"
VERDICT_NOISE = "NOISE"
VERDICT_INSUFFICIENT = "INSUFFICIENT"

VERDICT_STABLE = "STABLE"
VERDICT_UNSTABLE = "UNSTABLE"


def event_significance(
    returns: Iterable[float],
    *,
    n_resamples: int = 10_000,
    alpha: float = 0.05,
    seed: int = 42,
    win_threshold: float = 0.0,
    t_threshold: float = 2.0,
    label: str = "event return",
) -> dict:
    """Significance of a per-event return series (one observation per trade).

    Reports, for the same sample:
      - t-statistic and its Student-t two-sided p-value (df = n-1),
      - a percentile **bootstrap** CI for the mean return,
      - the **win rate** with a **Wilson** interval, plus a z-test of the win
        rate against 50%.

    `win_threshold` defines a "win" (default: strictly positive return).
    Verdict uses the same PASS / FAIL / NOISE lines as `significance()`.
    """
    arr = np.asarray([v for v in returns], dtype=float)
    arr = arr[np.isfinite(arr)]
    n = int(arr.size)

    wins = int((arr > float(win_threshold)).sum()) if n else 0
    win_rate = (wins / n) if n else float("nan")
    w_lo, w_hi = wilson_interval(wins, n, alpha=alpha) if n else (float("nan"), float("nan"))

    out: dict[str, Any] = {
        "label": label,
        "n_events": n,
        "n_resamples": int(n_resamples),
        "alpha": float(alpha),
        "win_threshold": float(win_threshold),
        "t_threshold": float(t_threshold),
        "n_wins": wins,
        "win_rate": win_rate,
        "win_rate_wilson_low": w_lo,
        "win_rate_wilson_high": w_hi,
        "win_rate_beats_chance": (w_lo > 0.5) if n else None,
    }

    if n == 0:
        out.update({"verdict": VERDICT_INSUFFICIENT, "mean": None, "t_stat": None, "p_value": None, "note": "no finite observations"})
        return out

    out["mean"] = float(arr.mean())
    out["median"] = float(np.median(arr))
    out["std"] = float(arr.std(ddof=1)) if n > 1 else None
    out["min"] = float(arr.min())
    out["max"] = float(arr.max())
    out["total"] = float(arr.sum())

    if n < 2:
        out.update({"verdict": VERDICT_INSUFFICIENT, "t_stat": None, "p_value": None, "note": "need n>=2 for a t-test"})
        return out

    std = float(arr.std(ddof=1))
    se = std / np.sqrt(n)
    t = float(arr.mean() / se) if se > 0 else float("nan")
    p_two = two_sided_t_p(t, n - 1) if np.isfinite(t) else float("nan")

    boot = bootstrap_ci_mean(arr, n_resamples=n_resamples, alpha=alpha, seed=seed)
    win_z = proportion_z(wins, n)
    win_p = two_sided_t_p(win_z, n - 1) if np.isfinite(win_z) else float("nan")

    if not np.isfinite(t):
        verdict = VERDICT_NOISE
    elif t >= t_threshold:
        verdict = VERDICT_PASS
    elif t <= -t_threshold:
        verdict = VERDICT_FAIL
    else:
        verdict = VERDICT_NOISE

    out.update(
        {
            "df": n - 1,
            "se": float(se),
            "t_stat": t,
            "p_value": p_two,
            "verdict": verdict,
            "bootstrap_mean_low": boot["low"],
            "bootstrap_mean_high": boot["high"],
            "bootstrap_p_one_sided_leq_zero": boot["p_one_sided_leq_zero"],
            "bootstrap_ci_excludes_zero": (
                None if boot["low"] is None or boot["high"] is None else bool(boot["low"] > 0.0 or boot["high"] < 0.0)
            ),
            "win_rate_z": win_z,
            "win_rate_p": win_p,
            "small_sample": n < 30,
            "note": "event-level test (Student-t, df=n-1); 1 observation per trade",
        }
    )
    return out


def event_window_stats(
    events: Any,
    windows: Mapping[str, Sequence[Any]] | Sequence[Any],
    *,
    date_col: str = "date",
    ret_col: str = "return_pct",
    label: str | None = None,
    n_resamples: int = 10_000,
    alpha: float = 0.05,
    seed: int = 42,
) -> dict:
    """Split an event/trade list into time sub-periods and report distributions.

    Unlike `multi_window()` (which re-runs a backtest per window), this segments
    an existing trade list — the natural way to ask "does weekend_gap hold in
    2021-23 vs 2024-26?" without re-running anything.

    `windows` may be a mapping ``{label: (start, end)}`` (inclusive) or a list of
    ``(label, start, end)`` triples / dicts with ``label``/``start``/``end``.
    """
    df = events if isinstance(events, pd.DataFrame) else pd.DataFrame(list(events))
    base: dict[str, Any] = {"label": label, "n_windows": 0, "windows": [], "verdict": VERDICT_INSUFFICIENT}
    if df.empty or date_col not in df.columns or ret_col not in df.columns:
        base["note"] = f"events must provide '{date_col}' and '{ret_col}'"
        return base

    df = df.copy()
    df["_date"] = pd.to_datetime(df[date_col], errors="coerce", utc=True)
    df["_ret"] = pd.to_numeric(df[ret_col], errors="coerce")
    df = df.dropna(subset=["_date"])

    triples = _normalise_windows(windows)
    rows: list[dict] = []
    by_label: dict[str, float] = {}
    for lab, start, end in triples:
        sub = df[(df["_date"] >= start) & (df["_date"] <= end)]
        rets = sub["_ret"].dropna().to_numpy()
        row: dict[str, Any] = {"window": lab, "start": str(pd.Timestamp(start).date()), "end": str(pd.Timestamp(end).date()), "n_events": int(rets.size)}
        if rets.size == 0:
            row.update({"verdict": VERDICT_INSUFFICIENT, "mean": None, "t_stat": None, "win_rate": None})
        else:
            sig = event_significance(rets, n_resamples=n_resamples, alpha=alpha, seed=seed, label=str(lab))
            for key in ("mean", "median", "std", "t_stat", "p_value", "win_rate", "win_rate_wilson_low", "win_rate_wilson_high", "verdict", "bootstrap_mean_low", "bootstrap_mean_high"):
                row[key] = sig.get(key)
            by_label[str(lab)] = float(sig["mean"])
        rows.append(row)

    summary = multi_window(lambda lab: {"v": by_label[lab]}, list(by_label), metric="v", label=f"{label or 'events'}: per-window mean return")
    verdict = VERDICT_INSUFFICIENT if summary.get("verdict") == VERDICT_INSUFFICIENT else summary["verdict"]

    return {
        "label": label,
        "date_col": date_col,
        "return_col": ret_col,
        "n_windows": len(rows),
        "n_windows_with_events": len(by_label),
        "n_events_total": int(df["_ret"].notna().sum()),
        "windows": rows,
        "window_sign_consistency": summary.get("sign_consistency"),
        "window_verdicts": {r["window"]: r.get("verdict") for r in rows},
        "verdict": verdict,
        "note": "segments an existing trade list; per-window sign consistency decides STABLE/UNSTABLE",
    }


def _normalise_windows(windows: Mapping[str, Sequence[Any]] | Sequence[Any]) -> list[tuple[str, pd.Timestamp, pd.Timestamp]]:
    if isinstance(windows, Mapping):
        items = [(str(k), v[0], v[1]) for k, v in windows.items()]
    else:
        items = []
        for i, w in enumerate(windows):
            if isinstance(w, Mapping):
                items.append((str(w.get("label", w.get("window", i))), w["start"], w["end"]))
            else:
                items.append((str(w[0]), w[1], w[2]))
    out = []
    for lab, start, end in items:
        out.append((lab, _ts(start), _ts(end)))
    return out


def _ts(value: Any) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
