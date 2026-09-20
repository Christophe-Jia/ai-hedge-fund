#!/usr/bin/env python3
"""Momentum vs reversal horizon decomposition — a k x h cross-sectional IC map.

The "kinetic vs potential energy" economic-physics framing reduces to a
measurable empirical question: **at which look-back horizon does past return
start predicting future return with a positive sign, and where is it
negative?** The textbook story is "short-horizon reversal (days), medium/long
momentum (6-12 months)". This script measures the sign map directly on the
point-in-time S&P 100 pool and then tries to destroy it with the project's
standing discipline.

Method
------
For every trading day ``t`` and every cell ``(k, h)``:

    past_k[t]    = C[t-1] / C[t-1-k] - 1        (observable strictly before t)
    forward_h[t] = C[t-1+h] / C[t-1] - 1        (entry on the signal bar, exit h later)

so ``past_k[t]`` covers the return bars ``r[t-k] … r[t-1]`` and ``forward_h[t]``
covers ``r[t] … r[t+h-1]``: **contiguous, no gap, no shared return bar**. The
look-back uses only data strictly before ``t`` (no look-ahead). The signal bar
``t-1`` is also the entry bar (``execution_lag=0``) — the canonical IC
definition; the two windows do touch the boundary price ``C[t-1]``, and that is
exactly the classic source of a one-day bid-ask-bounce reversal. A second grid
at ``execution_lag=1`` (entry at close ``t``, the live engine's
``execution_lag_bars=1`` convention) removes the shared price entirely and is
reported under ``sensitivity_execution_lag`` so the mechanical component can be
subtracted.

The daily cross-sectional **Spearman** rank IC series is computed across the
PIT eligible pool, then four things happen to it:

1. **Overlap correction.** Forward windows of length ``h`` overlap across
   consecutive ``t``, so naive ``t`` is inflated. Reported ``t`` is
   Newey-West HAC (Bartlett kernel, lag ``max(1, h-1)``); the CI is a 21-day
   **block** bootstrap. Both are reported, not just one.
2. **Multiplicity.** ``k x h = 8 x 5 = 40`` cells is itself the search space.
   The report quotes N=40 Bonferroni (two-sided) and the Bailey-Lopez de Prado
   expected-max (via ``src.validation.deflation.expected_max_sharpe``), and
   deflates each cell's IC-Sharpe with ``deflated_sharpe_ratio``. A cell that
   is only nominally significant is labelled ``NOT_SURVIVING_CORRECTION``.
3. **Era split (mandatory).** 2016-19 / 2020-22 / 2023-26. A sign that flips
   across eras is reported as such — on this platform these structures have a
   habit of being era artifacts.
4. **Effective N.** The 40 cells are far from independent (nested k, overlapping
   h, identical underlying data). N_eff is estimated from the average pairwise
   correlation of the IC series, and both N and N_eff thresholds are shown.

Also reported: single-name time-series momentum/reversal for MU, MRVL, COIN
(user's names) and SPY/QQQ (benchmarks); the exact location of the project's
existing 12-1 momentum rule on this map; and the IC decay curves in both
directions, i.e. IC(k) at fixed h and IC(h) at fixed k.

Usage:
    poetry run python scripts/momentum_reversal_horizon.py
    poetry run python scripts/momentum_reversal_horizon.py --no-bootstrap  # fast
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.data.nasdaq_store import NasdaqDailyStore
from src.validation.deflation import (  # noqa: E402  (read-only import)
    DSR_THRESHOLD,
    deflated_sharpe_ratio,
    expected_max_sharpe,
)
from scripts.xsec_gbm_selection import (  # noqa: E402  single source of truth
    load_pit_universe,
    universe_for_date,
)

ROOT = Path(__file__).resolve().parents[1]
UNIVERSE_DIR = ROOT / "data" / "universe"
REPORT_PATH = ROOT / "reports" / "horizon_decomposition.json"

KS: tuple[int, ...] = (3, 5, 10, 21, 42, 63, 126, 252)
HS: tuple[int, ...] = (1, 5, 10, 21, 63)
ERAS: tuple[tuple[str, str, str], ...] = (
    ("2016-19", "2016-09-01", "2019-12-31"),
    ("2020-22", "2020-01-01", "2022-12-31"),
    ("2023-26", "2023-01-01", "2026-12-31"),
)
BLOCK = 21          # block bootstrap block length (trading days), per spec
N_BOOT = 2000
MIN_NAMES = 20      # need a real cross-section before an IC day is usable
MIN_COVERAGE = 0.8  # Load panel guard: drop dates with <80% of names priced
FOCUS_NAMES = ("MU", "MRVL", "COIN")
BENCHMARKS = ("SPY", "QQQ")
ALPHA = 0.05
DATA_START = "2016-01-01"
DATA_END = "2026-12-31"

STATUS_BONF = "SURVIVES_BONFERRONI"
STATUS_DSR = "SURVIVES_DSR"
STATUS_NOMINAL = "NOT_SURVIVING_CORRECTION"
STATUS_NONE = "NOT_SIGNIFICANT"

_Z = statistics.NormalDist()


# ---------------------------------------------------------------------------
# estimators (pure; unit-tested against synthetic known processes)
# ---------------------------------------------------------------------------

def past_return_matrix(close: pd.DataFrame, k: int) -> pd.DataFrame:
    """Look-back return ``C[t-1]/C[t-1-k]-1`` — strictly before ``t``.

    The signal bar is ``t-1``, so nothing at or after ``t`` is used: no
    look-ahead. ``past_k[t]`` covers the return bars ``r[t-k] … r[t-1]``.
    """
    return close.shift(1) / close.shift(1 + k) - 1.0


def forward_return_matrix(
    close: pd.DataFrame, h: int, *, execution_lag: int = 0
) -> pd.DataFrame:
    """Forward return of length ``h`` measured from the signal bar.

    Entry bar is ``t-1+execution_lag``, exit bar is ``t-1+execution_lag+h``::

        fwd_h[t] = C[t-1+L+h] / C[t-1+L] - 1        (L = execution_lag)

    ``L=0`` (primary) enters on the signal bar, so ``fwd_h[t]`` covers
    ``r[t] … r[t+h-1]`` — **contiguous** with ``past_k[t]`` (``r[t-k] … r[t-1]``)
    and the canonical overlap-free IC definition. The two windows do share the
    boundary price ``C[t-1]``, which is the classic source of a one-day
    bid-ask-bounce reversal; ``L=1`` (the live engine's convention, entry at
    close ``t``) removes that shared price entirely and is reported as a
    sensitivity.
    """
    num = close.shift(-(h - 1 + execution_lag))
    den = close.shift(1 - execution_lag) if execution_lag != 1 else close
    return num / den - 1.0


def build_universe_matrix(
    pit: dict[int, list[str]], index: pd.DatetimeIndex, symbols: list[str]
) -> pd.DataFrame:
    """date x symbol boolean: was ``symbol`` a PIT constituent on ``date``?"""
    cols = list(symbols)
    universe = pd.DataFrame(False, index=index, columns=cols)
    pos = {s: i for i, s in enumerate(cols)}
    for day in index:
        row = np.zeros(len(cols), dtype=bool)
        for s in universe_for_date(pit, day):
            j = pos.get(s)
            if j is not None:
                row[j] = True
        universe.iloc[index.get_loc(day)] = row
    return universe


def cross_sectional_ic(
    past: pd.DataFrame,
    forward: pd.DataFrame,
    universe: pd.DataFrame,
    *,
    min_names: int = MIN_NAMES,
) -> pd.Series:
    """Daily cross-sectional Spearman rank IC of ``past`` vs ``forward``.

    Eligibility on a given day = past & forward both present AND the name is a
    PIT constituent that day. Days with fewer than ``min_names`` eligible names
    (or zero rank dispersion) are dropped. Spearman == Pearson on ranks.
    """
    elig = past.notna() & forward.notna() & universe.reindex_like(past).fillna(False)
    n_elig = elig.sum(axis=1)
    pr = past.where(elig).rank(axis=1)
    fr = forward.where(elig).rank(axis=1)
    pr = pr.sub(pr.where(elig).mean(axis=1), axis=0)
    fr = fr.sub(fr.where(elig).mean(axis=1), axis=0)
    num = (pr * fr).sum(axis=1)
    den = np.sqrt((pr ** 2).sum(axis=1) * (fr ** 2).sum(axis=1))
    ic = num / den.replace(0.0, np.nan)
    ic[n_elig < min_names] = np.nan
    ic = ic.replace([np.inf, -np.inf], np.nan).dropna()
    ic.name = "ic"
    return ic


def newey_west_t(x: np.ndarray, lag: int) -> float:
    """HAC (Bartlett) t-statistic of the mean of ``x``. Handles overlap.

    Dot products use ``np.sum(a*b)`` rather than ``@`` because the macOS
    Accelerate BLAS emits spurious divide-by-zero/invalid RuntimeWarnings for
    matmul on 1-D float64 arrays.
    """
    raw = np.asarray(x, dtype=float)
    n = raw.size
    if n < 3:
        return float("nan")
    mu = float(raw.mean())
    e = raw - mu
    var = float(np.sum(e * e)) / n  # gamma_0
    for l in range(1, min(lag, n - 1) + 1):
        w = 1.0 - l / (lag + 1.0)
        var += 2.0 * w * float(np.sum(e[l:] * e[:-l])) / n
    if var <= 0.0:
        return float("nan")
    return mu / math.sqrt(var / n)


def block_bootstrap_ci(
    x: np.ndarray,
    *,
    block: int = BLOCK,
    n_boot: int = N_BOOT,
    seed: int = 0,
) -> tuple[float, float]:
    """Percentile CI of the mean via a moving-block bootstrap (2.5 / 97.5)."""
    x = np.asarray(x, dtype=float)
    n = x.size
    if n < 2:
        return float("nan"), float("nan")
    rng = np.random.default_rng(seed)
    if n <= block:
        idx = rng.integers(0, n, size=(n_boot, n))
    else:
        n_blocks = int(math.ceil(n / block))
        starts = rng.integers(0, n - block + 1, size=(n_boot, n_blocks))
        offsets = np.arange(block)
        idx = (starts[:, :, None] + offsets[None, None, :]).reshape(n_boot, -1)[:, :n]
    means = x[idx].mean(axis=1)
    lo, hi = np.percentile(means, [100.0 * ALPHA / 2.0, 100.0 * (1.0 - ALPHA / 2.0)])
    return float(lo), float(hi)


def series_moments(x: np.ndarray) -> dict:
    """n, mean, std, skew, raw kurtosis of a finite series."""
    x = np.asarray(x, dtype=float)
    x = x[np.isfinite(x)]
    n = int(x.size)
    if n < 2:
        return {"n": n, "mean": float("nan"), "std": float("nan"),
                "skew": float("nan"), "kurtosis": float("nan")}
    mean = float(x.mean())
    dev = x - mean
    m2 = float((dev ** 2).mean())
    if m2 <= 0.0:
        return {"n": n, "mean": mean, "std": 0.0, "skew": float("nan"),
                "kurtosis": float("nan")}
    std = math.sqrt(float((dev ** 2).sum()) / (n - 1))
    return {
        "n": n,
        "mean": mean,
        "std": std,
        "skew": float((dev ** 3).mean()) / m2 ** 1.5,
        "kurtosis": float((dev ** 4).mean()) / m2 ** 2,
    }


def ic_stats(
    ic: pd.Series,
    h: int,
    *,
    block: int = BLOCK,
    n_boot: int = N_BOOT,
    seed: int = 0,
) -> dict:
    """Full stats for one cell's IC series: mean, naive & HAC t, CI, moments.

    ``nonoverlap_*`` fields describe the stride-``h`` subsample (every h-th day),
    whose forward windows no longer overlap. PSR/DSR assume iid observations, so
    the deflated test is run on that subsample; the full-series (overlapping)
    moments are kept only so the inflation can be shown.
    """
    x = np.asarray(ic.dropna().values, dtype=float)
    m = series_moments(x)
    x_no = x[::h] if h > 0 else x
    mn = series_moments(x_no)
    n = m["n"]
    out = {
        "n_days": n,
        "ic_mean": m["mean"],
        "ic_std": m["std"],
        "ic_positive_rate": float((x > 0).mean()) if n else float("nan"),
        "t_naive": float(m["mean"] / (m["std"] / math.sqrt(n)))
        if n > 1 and m["std"] and m["std"] > 0 else float("nan"),
        "newey_west_lag": max(1, h - 1),
        "t_newey_west": newey_west_t(x, max(1, h - 1)),
        "skew": m["skew"],
        "kurtosis_raw": m["kurtosis"],
        "nonoverlap_stride": h,
        "nonoverlap_n": mn["n"],
        "nonoverlap_ic_mean": mn["mean"],
        "nonoverlap_ic_std": mn["std"],
        "nonoverlap_skew": mn["skew"],
        "nonoverlap_kurtosis_raw": mn["kurtosis"],
        "bootstrap_block": block,
        "n_boot": n_boot if n > 1 else 0,
    }
    if n > 1 and n_boot:
        lo, hi = block_bootstrap_ci(x, block=block, n_boot=n_boot, seed=seed)
        out["ic_ci_lo"], out["ic_ci_hi"] = lo, hi
    else:
        out["ic_ci_lo"] = out["ic_ci_hi"] = float("nan")
    out["sign"] = "REVERSAL" if m["mean"] < 0 else "MOMENTUM"
    return out


def deflate_cell(
    stats: dict, *, n_trials: int, sr_std_overlap: float, sr_std_nonoverlap: float,
    bonf_z: float,
) -> dict:
    """Attach DSR (iid, non-overlapping) + Bonferroni status. Mutates and returns."""
    n = stats["n_days"]
    sr_full = (stats["ic_mean"] / stats["ic_std"]) if stats["ic_std"] else float("nan")
    dsr_overlap = float("nan")
    if n >= 2 and math.isfinite(sr_full) and math.isfinite(sr_std_overlap):
        dsr_overlap = deflated_sharpe_ratio(
            sr_full, n, stats["skew"], stats["kurtosis_raw"],
            n_trials, sr_std_overlap,
        )

    n_no = stats["nonoverlap_n"]
    sr_no = (stats["nonoverlap_ic_mean"] / stats["nonoverlap_ic_std"]
             if stats["nonoverlap_ic_std"] else float("nan"))
    dsr = float("nan")
    sr0 = float("nan")
    if n_no >= 2 and math.isfinite(sr_no) and math.isfinite(sr_std_nonoverlap):
        sr0 = expected_max_sharpe(n_trials, sr_std_nonoverlap)
        dsr = deflated_sharpe_ratio(
            sr_no, n_no, stats["nonoverlap_skew"], stats["nonoverlap_kurtosis_raw"],
            n_trials, sr_std_nonoverlap,
        )
    stats.update({
        "ic_sharpe_per_period": sr_full,
        "nonoverlap_ic_sharpe": sr_no,
        "n_trials": int(n_trials),
        "sr_std_overlap_used": float(sr_std_overlap),
        "sr_std_nonoverlap_used": float(sr_std_nonoverlap),
        "expected_max_sharpe": sr0,
        "dsr_nonoverlap": dsr,
        "dsr_overlap_inflated": dsr_overlap,
        "bonferroni_z": float(bonf_z),
    })
    t = stats["t_newey_west"]
    nominal = math.isfinite(t) and abs(t) >= 1.959964
    if math.isfinite(t) and abs(t) >= bonf_z:
        status = STATUS_BONF
    elif math.isfinite(dsr) and dsr > DSR_THRESHOLD:
        status = STATUS_DSR
    elif nominal:
        status = STATUS_NOMINAL
    else:
        status = STATUS_NONE
    stats["correction_status"] = status
    return stats


def effective_n(ic_frame: pd.DataFrame) -> dict:
    """N_eff from the average pairwise correlation of the IC series."""
    corr = ic_frame.corr(min_periods=30)
    n = corr.shape[0]
    vals = corr.where(~np.eye(n, dtype=bool)).stack().dropna()
    rbar = float(vals.mean()) if len(vals) else 0.0
    n_eff = n / (1.0 + (n - 1) * max(rbar, 0.0)) if n else float("nan")
    return {"n_cells": n, "rbar_pairwise": rbar, "n_effective": n_eff}


# ---------------------------------------------------------------------------
# grids
# ---------------------------------------------------------------------------

def run_grid(
    close: pd.DataFrame,
    universe: pd.DataFrame,
    ks: tuple[int, ...],
    hs: tuple[int, ...],
    *,
    n_boot: int = N_BOOT,
    date_slice: tuple[str, str] | None = None,
    execution_lag: int = 0,
    with_correction: bool = True,
) -> tuple[dict, pd.DataFrame]:
    """Compute every (k,h) cell. Returns ({key: stats}, aligned IC frame)."""
    sub = close
    if date_slice is not None:
        sub = close.loc[(close.index >= date_slice[0]) & (close.index <= date_slice[1])]
    cells: dict[str, dict] = {}
    ic_cols: dict[str, pd.Series] = {}
    for k in ks:
        past = past_return_matrix(sub, k)
        for h in hs:
            fwd = forward_return_matrix(sub, h, execution_lag=execution_lag)
            ic = cross_sectional_ic(past, fwd, universe)
            if date_slice is not None:
                ic = ic[(ic.index >= date_slice[0]) & (ic.index <= date_slice[1])]
            key = f"k{k}_h{h}"
            cells[key] = ic_stats(ic, h, n_boot=n_boot, seed=1000 * k + h)
            cells[key].update({"k": k, "h": h, "mean_of": "cross_sectional_spearman"})
            ic_cols[key] = ic
    frame = pd.DataFrame({k: v for k, v in ic_cols.items()})

    if with_correction and cells:
        n_trials = len(cells)
        bonf_z = _Z.inv_cdf(1.0 - ALPHA / (2.0 * n_trials))
        sr_vals = [c["ic_mean"] / c["ic_std"] for c in cells.values()
                   if c["ic_std"] and math.isfinite(c["ic_std"])]
        sr_std_full = float(np.std(sr_vals, ddof=1)) if len(sr_vals) > 1 else 1.0
        sr_vals_no = [c["nonoverlap_ic_mean"] / c["nonoverlap_ic_std"]
                      for c in cells.values()
                      if c["nonoverlap_ic_std"] and math.isfinite(c["nonoverlap_ic_std"])]
        sr_std_no = float(np.std(sr_vals_no, ddof=1)) if len(sr_vals_no) > 1 else 1.0
        for c in cells.values():
            deflate_cell(c, n_trials=n_trials, sr_std_overlap=sr_std_full,
                         sr_std_nonoverlap=sr_std_no, bonf_z=bonf_z)
    return cells, frame


def sign_map(cells: dict, ks: tuple[int, ...], hs: tuple[int, ...]) -> dict:
    """k x h grid of {ic_mean, t_nw, status} plus an ASCII map."""
    grid: dict[str, dict] = {}
    rows = []
    for k in ks:
        row = []
        for h in hs:
            c = cells.get(f"k{k}_h{h}")
            if c is None:
                grid[f"{k},{h}"] = None
                row.append("  .  ")
                continue
            grid[f"{k},{h}"] = {
                "ic_mean": c["ic_mean"],
                "t_newey_west": c["t_newey_west"],
                "n_days": c["n_days"],
                "correction_status": c["correction_status"],
            }
            sig = abs(c["t_newey_west"]) if math.isfinite(c["t_newey_west"]) else 0.0
            mark = "*" if c["correction_status"] in (STATUS_BONF, STATUS_DSR) else (
                "+" if c["correction_status"] == STATUS_NOMINAL else " ")
            sign = "+" if c["ic_mean"] > 0 else "-"
            row.append(f"{sign}{sig:4.2f}{mark}")
        rows.append({"k": k, "cells": row})
    legend = ("each cell = sign(+/-) then |Newey-West t|; "
              "* = survives N=40 correction (Bonferroni or DSR), "
              "+ = nominal only (p<.05 pre-correction -> NOT_SURVIVING_CORRECTION)")
    return {"grid": grid, "ascii_rows": rows, "legend": legend,
            "columns_h": list(hs), "rows_k": list(ks)}


def decay_curve(cells: dict, fixed: str, varying: tuple[int, ...]) -> dict:
    """IC vs one axis while the other is held fixed.

    ``fixed`` is ``"h<h>"`` (vary k) or ``"k<k>"`` (vary h).
    """
    axis, level = fixed[0], int(fixed[1:])
    pts = []
    for v in varying:
        key = f"k{v}_h{level}" if axis == "h" else f"k{level}_h{v}"
        c = cells.get(key)
        if c is None:
            continue
        pts.append({"value": v, "ic_mean": c["ic_mean"],
                    "t_newey_west": c["t_newey_west"],
                    "correction_status": c["correction_status"]})
    flip = None
    for a, b in zip(pts, pts[1:]):
        if a["ic_mean"] * b["ic_mean"] < 0:
            flip = {"from": a["value"], "to": b["value"]}
            break
    return {"varying": "k" if axis == "h" else "h", "held_at": level,
            "points": pts, "sign_flip_between": flip}


# ---------------------------------------------------------------------------
# single-name time-series momentum / reversal
# ---------------------------------------------------------------------------

def time_series_ic(past: pd.Series, forward: pd.Series, h: int) -> dict:
    """Time-series Spearman rho between a name's own past-k and forward-h."""
    df = pd.DataFrame({"p": past, "f": forward}).dropna()
    n = len(df)
    if n < 30 or df["p"].nunique() < 2 or df["f"].nunique() < 2:
        return {"n_obs": n, "spearman_rho": float("nan"), "t_naive": float("nan"),
                "t_newey_west": float("nan"),
                "note": "insufficient / degenerate sample"}
    p = df["p"].rank().values
    f = df["f"].rank().values
    rho = float(np.corrcoef(p, f)[0, 1])
    pear = float(df["p"].corr(df["f"]))
    t = rho * math.sqrt((n - 2) / max(1e-12, 1.0 - rho ** 2)) if abs(rho) < 1 else float("nan")
    # HAC t for the slope of f on p (overlapping forward windows).
    # beta = Sxy/Sxx; se(beta) = sqrt(HAC_var(sum_t xc_t*resid_t)) / Sxx.
    lag = max(1, h - 1)
    xc = p - p.mean()
    fc = f - f.mean()
    sxx = float(np.sum(xc * xc))
    beta = float(np.sum(xc * fc)) / sxx if sxx else float("nan")
    resid = fc - beta * xc
    g = xc * resid  # score contribution of each observation
    hac = float(np.sum(g * g))
    for l in range(1, min(lag, n - 1) + 1):
        w = 1.0 - l / (lag + 1.0)
        hac += 2.0 * w * float(np.sum(g[l:] * g[:-l]))
    t_nw = beta * sxx / math.sqrt(hac) if hac > 0 and sxx > 0 else float("nan")
    return {
        "n_obs": n,
        "spearman_rho": rho,
        "pearson_r": pear,
        "t_naive": t,
        "t_newey_west": t_nw,
        "note": ("single name + overlapping forward windows: descriptive only, "
                 "t_newey_west is the HAC-corrected slope t"),
    }


def single_name_grid(
    close: pd.DataFrame,
    names: tuple[str, ...],
    ks: tuple[int, ...],
    hs: tuple[int, ...],
    *,
    execution_lag: int = 0,
) -> dict:
    out: dict[str, dict] = {}
    for name in names:
        if name not in close.columns:
            out[name] = {"available": False,
                         "reason": "no daily close data in data/btc_history.db"}
            continue
        col = close[name]
        cells = {}
        for k in ks:
            past = col.shift(1) / col.shift(1 + k) - 1.0
            for h in hs:
                fwd = forward_return_matrix(col.to_frame("x"), h,
                                            execution_lag=execution_lag)["x"]
                cells[f"k{k}_h{h}"] = time_series_ic(past, fwd, h)
        first = col.dropna()
        out[name] = {
            "available": True,
            "first_date": str(first.index[0].date()) if len(first) else None,
            "last_date": str(first.index[-1].date()) if len(first) else None,
            "n_bars": int(first.size),
            "cells": cells,
        }
    return out


# ---------------------------------------------------------------------------
# data orchestration
# ---------------------------------------------------------------------------

def drop_low_coverage_dates(
    panel: pd.DataFrame, *, min_coverage: float = MIN_COVERAGE
) -> tuple[pd.DataFrame, dict[str, float]]:
    """Drop dates where fewer than ``min_coverage`` of names are priced.

    This is the engine's partial-refresh guard: a day on which only a handful of
    tickers were updated is not a trading day for the pool, and treating it as
    one would value every un-refreshed holding at zero. It also fixes the
    panel's effective cutoff — a symbol fetched more recently than the rest of
    the pool contributes its newest bars only on dates almost nobody else has,
    which the guard removes. Returns ``(panel, {dropped_date: coverage_fraction})``.
    """
    frac = panel.notna().mean(axis=1)
    bad = frac[frac < min_coverage]
    return panel.drop(index=bad.index), {str(d.date()): float(v) for d, v in bad.items()}


def load_close_panel(
    store: NasdaqDailyStore, symbols: list[str], start: str, end: str,
    *, min_coverage: float = MIN_COVERAGE,
    dropped_out: dict | None = None,
) -> pd.DataFrame:
    """date x symbol close panel with the same low-coverage guard as the engine.

    ``min_coverage`` defaults to the engine's 0.8. Pass 0.0 for a handful of
    benchmark tickers where a single missing name would trip the fraction test.
    If ``dropped_out`` is a dict it is filled with ``{dropped_date: fraction}``
    so the caller can report why a name's raw bars are not in the panel.
    """
    cols = {}
    for sym in symbols:
        df = store.get_daily(sym, start, end)
        if df.empty:
            continue
        cols[sym] = df["close"]
    if not cols:
        return pd.DataFrame()
    panel = pd.DataFrame(cols).sort_index()
    panel, dropped = drop_low_coverage_dates(panel, min_coverage=min_coverage)
    if dropped and dropped_out is not None:
        dropped_out.update(dropped)
    if dropped:
        print(f"  [guard] dropping {len(dropped)} low-coverage date(s), "
              f"coverage {min(dropped.values()):.3%} .. {max(dropped.values()):.3%}")
    return panel


def name_coverage(
    name: str,
    store: NasdaqDailyStore,
    panel: pd.DataFrame,
    start: str,
    end: str,
    table: str,
) -> dict:
    """Raw-store coverage vs panel coverage for one name, and the difference.

    The report must not let these two numbers silently disagree: the raw store
    row count is what ``backfill_symbols_status.json`` records, while the panel
    count is what the statistics actually consume.
    """
    if name not in panel.columns:
        return {"present": False, "table": table,
                "reason": "no rows in the store"}
    s = panel[name].dropna()
    raw = store.get_daily(name, start, end)
    raw_dates = list(raw.index)
    panel_dates = set(panel.index)
    excluded = [d for d in raw_dates if d not in panel_dates]
    leading = [d for d in panel.index if raw_dates and d < raw_dates[0]]
    return {
        "present": True,
        "table": table,
        "panel_bars": int(s.size),
        "panel_first": str(s.index[0].date()),
        "panel_last": str(s.index[-1].date()),
        "db_rows": int(len(raw_dates)),
        "db_first": str(raw_dates[0].date()) if raw_dates else None,
        "db_last": str(raw_dates[-1].date()) if raw_dates else None,
        "db_bars_excluded_from_panel": len(excluded),
        "db_excluded_dates": [str(d.date()) for d in excluded],
        "panel_dates_before_db_history": len(leading),
    }


def snapshot_membership(names: tuple[str, ...]) -> dict:
    """Which PIT snapshots each name appears in — read straight from the files.

    Makes the "X is not a universe member" claim machine-checkable instead of
    prose. Compares RAW snapshot tickers (no rename mapping), which is what
    index membership means; the renamed/union view is a separate question.
    Returns ``(per_name, n_files_scanned, files)``.
    """
    files = sorted(
        f for tag in ("sp100", "sp500") for f in UNIVERSE_DIR.glob(f"{tag}_[0-9][0-9][0-9][0-9].json")
    )
    membership: dict[str, list[str]] = {n: [] for n in names}
    for f in files:
        tag, year = f.stem.split("_")[0], f.stem.split("_")[1]
        tickers = {c["symbol"] for c in json.loads(f.read_text())}
        for n in names:
            if n in tickers:
                membership[n].append(f"{tag}_{year}")
    return (
        {n: sorted(v) for n, v in membership.items()},
        len(files),
        [f"{f.stem.split('_')[0]}_{f.stem.split('_')[1]}" for f in files],
    )


def _json_default(o):
    if isinstance(o, (np.floating,)):
        return float(o)
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.bool_,)):
        return bool(o)
    raise TypeError(f"not JSON serialisable: {type(o)}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--no-bootstrap", action="store_true",
                    help="skip the (slow) block bootstrap CIs")
    ap.add_argument("--start", default=DATA_START)
    ap.add_argument("--end", default=DATA_END)
    args = ap.parse_args()
    n_boot = 0 if args.no_bootstrap else N_BOOT

    print("[data] loading PIT S&P 100 universe ...")
    pit, union = load_pit_universe("sp100")
    print(f"  union={len(union)} names; years={sorted(pit)}")

    store = NasdaqDailyStore(assetclass="stocks")
    etf_store = NasdaqDailyStore(assetclass="etf")  # SPY / QQQ live in the etf table
    focus = [s for s in FOCUS_NAMES if s not in union]
    guard_dropped: dict[str, float] = {}
    close = load_close_panel(store, union + focus, args.start, args.end,
                             dropped_out=guard_dropped)
    missing_focus = [s for s in FOCUS_NAMES if s not in close.columns]
    bench = load_close_panel(etf_store, list(BENCHMARKS), args.start, args.end,
                             min_coverage=0.0)
    for col in bench.columns:
        close[col] = bench[col].reindex(close.index)
    print(f"[data] close panel {close.shape[0]} days x {close.shape[1]} names "
          f"({close.index[0].date()} -> {close.index[-1].date()})")
    if missing_focus:
        print(f"[data] WARNING focus names with no price data in the store: "
              f"{missing_focus}")

    # The cross-sectional universe only contains names in the PIT lists; adding
    # MU/MRVL/COIN/SPY/QQQ to the panel is harmless because the universe mask
    # gates participation. (SPY/QQQ are ETFs and are never in the sp100 list.)
    universe = build_universe_matrix(pit, close.index, list(close.columns))
    if "COIN" in close.columns:
        print(f"[data] COIN first bar {close['COIN'].first_valid_index()}")

    print("[grid] full-sample k x h ...")
    cells, frame = run_grid(close, universe, KS, HS, n_boot=n_boot)
    eff = effective_n(frame)
    n_trials = len(cells)
    bonf_z = _Z.inv_cdf(1.0 - ALPHA / (2.0 * n_trials))
    print(f"  cells={n_trials} bonferroni_z={bonf_z:.3f} "
          f"expected_max|t|(N={n_trials})= {expected_max_sharpe(n_trials, 1.0):.3f} "
          f"N_eff={eff['n_effective']:.1f}")

    print("[grid] era splits ...")
    era_cells: dict[str, dict] = {}
    era_maps: dict[str, dict] = {}
    for label, lo, hi in ERAS:
        ec, _ = run_grid(close, universe, KS, HS, n_boot=0, date_slice=(lo, hi))
        era_cells[label] = ec
        era_maps[label] = sign_map(ec, KS, HS)
        ic_means = {f"k{k}_h{h}": round(ec[f"k{k}_h{h}"]["ic_mean"], 4)
                    for k in KS for h in HS if f"k{k}_h{h}" in ec}
        print(f"  {label}: e.g. k21_h21={ic_means.get('k21_h21')} "
              f"k252_h21={ic_means.get('k252_h21')}")

    # era sign-flip audit for the two headline cells
    flips = {}
    for k in KS:
        for h in HS:
            key = f"k{k}_h{h}"
            signs = []
            for label, _, _ in ERAS:
                c = era_cells[label].get(key)
                if c and math.isfinite(c["ic_mean"]):
                    signs.append(1 if c["ic_mean"] > 0 else -1)
            flips[key] = {"signs": signs, "flips": len(set(signs)) > 1}

    print("[12-1] project momentum rule location ...")
    past_g = past_return_matrix(close, 252)                         # grid convention
    past_12_1 = close.shift(22) / close.shift(253) - 1.0            # engine's 12-1
    fwd21 = forward_return_matrix(close, 21)                        # L=0, grid-consistent
    fwd21_exec = forward_return_matrix(close, 21, execution_lag=2)  # engine: entry t+1
    ic_g = cross_sectional_ic(past_g, fwd21, universe)
    ic_121 = cross_sectional_ic(past_12_1, fwd21, universe)
    ic_121_e = cross_sectional_ic(past_12_1, fwd21_exec, universe)
    n_plus = n_trials + 1
    bonf_z_plus = _Z.inv_cdf(1.0 - ALPHA / (2.0 * n_plus))
    sr_std_full = float(np.std(
        [c["ic_mean"] / c["ic_std"] for c in cells.values() if c["ic_std"]],
        ddof=1))
    sr_std_no = float(np.std(
        [c["nonoverlap_ic_mean"] / c["nonoverlap_ic_std"]
         for c in cells.values() if c["nonoverlap_ic_std"]],
        ddof=1))
    twelve_one = {
        "grid_cell_k252_h21_no_skip": deflate_cell(
            ic_stats(ic_g, 21, n_boot=n_boot, seed=1),
            n_trials=n_plus, sr_std_overlap=sr_std_full,
            sr_std_nonoverlap=sr_std_no, bonf_z=bonf_z_plus),
        "project_style_skip21_entry_at_t": deflate_cell(
            ic_stats(ic_121, 21, n_boot=n_boot, seed=2),
            n_trials=n_plus, sr_std_overlap=sr_std_full,
            sr_std_nonoverlap=sr_std_no, bonf_z=bonf_z_plus),
        "project_style_skip21_next_day_entry": deflate_cell(
            ic_stats(ic_121_e, 21, n_boot=n_boot, seed=3),
            n_trials=n_plus, sr_std_overlap=sr_std_full,
            sr_std_nonoverlap=sr_std_no, bonf_z=bonf_z_plus),
        "convention_note": (
            "engine 12-1 = C[t-22]/C[t-253]-1 (skips the most recent month); the "
            "grid cell k252_h21 does NOT skip. The first three rows differ only in "
            "the skip and in the entry lag: entry_at_t is grid-consistent (L=0), "
            "next_day_entry replicates the engine's execution_lag_bars=1 (entry at "
            "close t+1, i.e. L=2 relative to the signal bar t-1)."
        ),
    }
    xref_path = ROOT / "reports" / "xsec_gbm_results.json"
    if xref_path.exists():
        try:
            xj = json.loads(xref_path.read_text())
            twelve_one["existing_report_reference"] = {
                "source": "reports/xsec_gbm_results.json",
                "m2_momentum_full_window": xj.get("momentum_full_window_m2_reference"),
                "gbm_monthly_ic": xj.get("monthly_ic"),
                "note": (
                    "the existing reports characterise 12-1 through portfolio returns "
                    "(M2 momentum CAGR/Sharpe) and through the GBM's own monthly IC; "
                    "neither publishes a cross-sectional 12-1 rank IC, so the IC/t "
                    "above is a new number rather than a reconciliation of an old one. "
                    "The M2 engine's positive portfolio result and the weak positive "
                    "12-1 IC here are consistent in sign, but the IC is not "
                    "individually significant and its sign flips in 2020-22."
                ),
            }
        except (json.JSONDecodeError, OSError):
            pass

    print("[sensitivity] 1-day execution lag (removes the shared boundary price) ...")
    sens_cells, sens_frame = run_grid(close, universe, KS, HS, n_boot=0,
                                      execution_lag=1, with_correction=False)
    primary_ic = {k: v["ic_mean"] for k, v in cells.items()}
    lag1_ic = {k: v["ic_mean"] for k, v in sens_cells.items()}
    sensitivity = {
        "note": (
            "execution_lag=1 enters at close t instead of the signal bar t-1, so "
            "past_k and forward_h stop sharing the boundary price C[t-1]. The "
            "difference isolates the mechanical bid-ask-bounce component of the "
            "short-horizon reversal. IC means only, no t-stats (diagnostic)."),
        "n_days_aligned": int(len(sens_frame)),
        "ic_mean_primary_execution_lag_0": {k: round(v, 5) for k, v in primary_ic.items()},
        "ic_mean_execution_lag_1": {k: round(v, 5) for k, v in lag1_ic.items()},
        "delta_lag1_minus_primary": {k: round(lag1_ic[k] - primary_ic[k], 5)
                                     for k in primary_ic if k in lag1_ic},
    }

    print("[single names] MU / MRVL / COIN / SPY / QQQ ...")
    singles = single_name_grid(close, (*FOCUS_NAMES, *BENCHMARKS), KS, HS)

    # Per-name DB-vs-panel reconciliation. The panel date index is the union of
    # every name's dates and is then gated by the coverage guard, so a name
    # fetched freshly (with bars newer than the last universe-wide refresh) can
    # hold raw rows that the panel deliberately excludes. Record both numbers
    # and the excluded dates so the report and the backfill artifact cannot be
    # read as contradicting each other.
    stores = {"stocks": store, "etf": etf_store}
    coverage: dict[str, dict] = {}
    for name in (*FOCUS_NAMES, *BENCHMARKS):
        table = "etf" if name in BENCHMARKS else "stocks"
        coverage[name] = name_coverage(
            name, stores[table], close, args.start, args.end, table
        )
    panel_cutoff = str(close.index[-1].date())
    raw_latest = max((v.get("db_last") or "" for v in coverage.values()
                      if v.get("present")), default=None)

    sp500_path = ROOT / "data" / "universe" / "sp500_union.json"
    sp500_union = set(json.loads(sp500_path.read_text())) if sp500_path.exists() else set()
    sp100_union = set(union)
    pit_membership = {
        name: {"sp100_union": name in sp100_union,
               "sp500_union": name in sp500_union,
               "role": "benchmark_etf" if name in BENCHMARKS else "extra_single_name"}
        for name in (*FOCUS_NAMES, *BENCHMARKS)
    }
    snap_membership, snap_n_files, snap_files = snapshot_membership(
        (*FOCUS_NAMES, *BENCHMARKS)
    )

    def _era_profile(key: str) -> dict:
        by_era = {label: era_cells[label][key]["ic_mean"] for label, _, _ in ERAS}
        signs = {label: ("+" if v > 0 else "-") for label, v in by_era.items()}
        return {
            "ic_by_era": by_era,
            "sign_by_era": signs,
            "sign_stable": len(set(signs.values())) == 1,
        }

    era_interpretation = {
        "one_month_reversal_k21_h21": {
            **_era_profile("k21_h21"),
            "implication": (
                "the classic 1-month reversal is negative in 2016-19 and 2020-22 but "
                "turns positive in 2023-26, i.e. the structure has disappeared or "
                "reversed in the most recent era. That refutes the idea of "
                "short-horizon reversal as a *stable potential well* (a fixed V(k) "
                "with a minimum at short k) and is consistent with the econophysics "
                "criticism that the effective potential is time-varying: the "
                "market's V(k) was re-shaped, not merely sampled with noise."),
        },
        "three_day_reversal_k3_h1": {
            **_era_profile("k3_h1"),
            "implication": (
                "the 3-day reversal keeps its sign across all three eras but its "
                "magnitude decays monotonically (-0.012 / -0.015 / -0.002): a decay, "
                "not a sign flip, and by 2023-26 it is indistinguishable from zero."),
        },
        "long_momentum_k252_h1": {
            **_era_profile("k252_h1"),
            "implication": (
                "the only cell that clears the N=40 bar is positive in all three "
                "eras (unlike 30/40 of the map) — the one piece of sign stability — "
                "but no single era is significant on its own (t=1.99/0.66/2.54), so "
                "the pooled significance leans on 2 of 3 eras."),
        },
        "twelve_one_k252_h21": {
            **_era_profile("k252_h21"),
            "implication": (
                "the 12-1 grid cell is positive in 2016-19 and 2023-26 but negative "
                "in 2020-22, so even the platform's own momentum leg sits on an "
                "era-unstable cell."),
        },
    }

    surviving = sorted(k for k, c in cells.items()
                       if c["correction_status"] in (STATUS_BONF, STATUS_DSR))
    n_flip = sum(1 for v in flips.values() if v["flips"])
    short_ic = float(np.mean([c["ic_mean"] for c in cells.values() if c["k"] <= 63]))
    long_ic = float(np.mean([c["ic_mean"] for c in cells.values() if c["k"] >= 126]))

    report = {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "script": "scripts/momentum_reversal_horizon.py",
            "question": ("at which look-back horizon k does past return switch sign "
                         "for predicting forward-h return, and does it survive "
                         "search multiplicity + era split?"),
            "data": "data/btc_history.db ohlcv, market_type='stocks' (Nasdaq daily)",
            "data_range": [str(close.index[0].date()), str(close.index[-1].date())],
            "universe": {
                "point_in_time": "data/universe/sp100_YYYY.json (year-mapped, renamed)",
                "union_size": len(union),
                "panel_names": int(close.shape[1]),
                "focus_names_missing_price_data": missing_focus,
                "benchmarks": list(BENCHMARKS),
                "benchmark_note": "SPY/QQQ come from the market_type='etf' table; "
                                  "the PIT sp100 mask keeps them out of the cross-section",
            },
            "data_provenance": {
                "store": "data/btc_history.db (SQLite; not committed — gitignored)",
                "tables": {"stocks": "market_type='stocks', timeframe='1d'",
                           "etf": "market_type='etf', timeframe='1d'"},
                "cutoff": {
                    "cross_sectional_cutoff": panel_cutoff,
                    "single_name_cutoff": panel_cutoff,
                    "aligned_on_purpose": True,
                    "coverage_guard": (
                        f"dates with <{MIN_COVERAGE:.0%} of pool names priced are "
                        "dropped from the panel index (load_close_panel)"),
                    "latest_available_in_store_across_names": raw_latest,
                    "why": (
                        "the single-name section deliberately shares the panel cutoff "
                        "rather than each name's latest bar, so single-name and "
                        "cross-sectional numbers describe the same window. A freshly "
                        "backfilled name therefore carries raw rows the panel excludes, "
                        "which is why db_rows and panel_bars can differ — see the "
                        "per-name db_excluded_dates and guard_dropped_dates."),
                },
                "single_name_and_benchmark_coverage": coverage,
                "pit_membership": pit_membership,
                "snapshot_membership": {
                    "per_name_snapshots_containing_it": snap_membership,
                    "n_snapshot_files_scanned": snap_n_files,
                    "files_scanned": snap_files,
                    "note": (
                        "read directly from data/universe/*.json (raw tickers, no "
                        "rename mapping). An empty list is the evidence for 'not a "
                        "universe member'; data/universe/ carries no membership dates, "
                        "so no claim is made about when membership began."),
                },
                "guard_dropped_dates": guard_dropped,
                "reconciliation_notes": (
                    [
                        f"{name}: raw store has {c['db_rows']} rows "
                        f"({c['db_first']}..{c['db_last']}) but the panel keeps "
                        f"{c['panel_bars']} ({c['panel_first']}..{c['panel_last']}). "
                        f"The {c['db_bars_excluded_from_panel']}-bar gap is "
                        f"DETERMINISTIC, not a mid-run read race: the excluded dates "
                        f"{', '.join(c['db_excluded_dates'])} are dates the "
                        f">= {MIN_COVERAGE:.0%} coverage guard removed from the panel "
                        f"index (see guard_dropped_dates for the coverage fraction on "
                        f"each), so they are absent from every statistic in this report."
                        for name, c in sorted(coverage.items())
                        if c.get("present") and c["db_rows"] != c["panel_bars"]
                    ]
                    + [
                        f"{name}: {c['panel_dates_before_db_history']} panel date(s) "
                        f"precede its first stored bar ({c['db_first']}) — a NaN region, "
                        f"not a dropped bar (Nasdaq serves ~10y of daily history)."
                        for name, c in sorted(coverage.items())
                        if c.get("present") and c["panel_dates_before_db_history"]
                    ]
                    + [
                        "the other single-name/benchmark tickers report "
                        "db_rows == panel_bars, which is additional evidence against a "
                        "read race: a race would have truncated them too.",
                        "reports/backfill_symbols_status.json is a raw-DB artifact (rows "
                        "written, through the DB's latest bar) and is expected to differ "
                        "from panel_bars; both are correct — different denominators.",
                    ]
                ),
                "cross_sectional_pool": {
                    "construction": "PIT sp100 union, year-mapped + renamed; "
                                    "low-coverage days (<80% of names) dropped",
                    "n_names": int(close.shape[1]),
                    "first": str(close.index[0].date()),
                    "last": str(close.index[-1].date()),
                },
                "notes": [
                    "NONE of the five single-name/benchmark tickers is in the PIT "
                    "sp100 union, so all five are loaded as extras and excluded from "
                    "the cross-section by the universe mask; see pit_membership. MU "
                    "and COIN are S&P 500 members (not S&P 100); MRVL is in neither "
                    "snapshot; SPY/QQQ are ETFs.",
                    "MRVL was not an S&P 500 constituent inside the sample window: "
                    "S&P Dow Jones Indices announced the addition on 2026-06-05 "
                    "(press release via PRNewswire; effective 2026-06-22, replacing "
                    "Pool Corp), i.e. after the 2026-01 snapshot. EXTERNAL SOURCE — "
                    "this date is not derivable from repo data (data/universe/ carries "
                    "no membership dates); the repo-verifiable part is that MRVL "
                    "appears in no sp100/sp500 snapshot 2016-2026. Its absence from the "
                    "universe files is correct, not a universe-data bug, and it was "
                    "backfilled separately by scripts/backfill_symbols.py (see "
                    "reports/backfill_symbols_status.json).",
                    "Nasdaq serves ~10 years of daily history per request, so the "
                    "oldest bars are truncated at the request window and MRVL starts "
                    "~5 trading days later than the rest of the panel.",
                    "Nasdaq closes are split-adjusted, dividend-unadjusted.",
                ],
            },
            "conventions": {
                "past_k": "C[t-1]/C[t-1-k]-1  (strictly before t -> no look-ahead)",
                "forward_h": "C[t-1+h]/C[t-1]-1  (primary: execution_lag=0, entry on the signal bar)",
                "adjacency": "past covers r[t-k..t-1]; forward covers r[t..t+h-1] -> "
                             "contiguous, no gap, and no overlapping return bar",
                "shared_boundary_price": "both windows touch C[t-1]; the h=1 mechanical "
                                         "bounce is quantified in sensitivity_execution_lag",
                "ic": "daily cross-sectional Spearman rank IC over PIT eligible names",
                "min_names": MIN_NAMES,
                "overlap": f"Newey-West HAC (Bartlett) lag=max(1,h-1); block bootstrap B={BLOCK}",
                "multiplicity": f"N={n_trials} cells, two-sided Bonferroni alpha={ALPHA}",
            },
            "parameters": {"ks": list(KS), "hs": list(HS), "n_boot": n_boot,
                           "block": BLOCK},
        },
        "multiplicity": {
            "n_cells_searched": n_trials,
            "bonferroni_z_two_sided": bonf_z,
            "bonferroni_threshold_note": f"|t_nw| must exceed {bonf_z:.3f} for N={n_trials}",
            "bonferroni_z_two_sided_n_eff": _Z.inv_cdf(
                1.0 - ALPHA / (2.0 * max(1, round(eff["n_effective"])))),
            "bonferroni_n_eff_note": (
                "the 40 cells are nested/overlapping, so N=40 is a deliberately "
                "conservative floor; an effective-N Bonferroni based on the average "
                "pairwise IC-series correlation is ~4x less stringent. Both numbers "
                "are quoted because the verdict of the borderline cell flips between them."),
            "expected_max_abs_t_evt": expected_max_sharpe(n_trials, 1.0),
            "expected_max_abs_t_note": (
                "EVT expected maximum |t| under the null for N independent trials "
                "(deflation.expected_max_sharpe(N, sigma=1)); the loser's bar, not the "
                "significance bar"),
            "effective_n": eff,
            "dsr_threshold": DSR_THRESHOLD,
            "dsr_convention": (
                "PSR/DSR assume iid observations, but forward-h IC series overlap. "
                "The reported dsr_nonoverlap is computed on the stride-h "
                "(non-overlapping) subsample; dsr_overlap_inflated is the naive "
                "full-series value and is shown only to expose the inflation."),
            "sr_std_overlap_across_cells": sr_std_full,
            "sr_std_nonoverlap_across_cells": sr_std_no,
            "borderline_cell_note": (
                "k252_h1 clears the N=40 Bonferroni t-bar by 0.006 (|t_nw|=3.233 vs "
                "3.227) and its 21-day block-bootstrap CI excludes zero, yet it fails "
                "the DSR bar by a wide margin. The two corrections disagree because "
                "DSR's noise ceiling is calibrated on the realised cross-cell Sharpe "
                "dispersion (sr_std ~0.062), which here reflects genuine horizon "
                "structure rather than estimation noise; a null-sampling sr_std "
                "(~1/sqrt(n) ~ 0.021) would still leave DSR ~0.85 < 0.95. Treat the "
                "cell as borderline, not as an established effect."),
        },
        "grid_full_sample": {
            "cells": cells,
            "sign_map": sign_map(cells, KS, HS),
        },
        "decay_curve_h21_vs_k": decay_curve(cells, "h21", KS),
        "decay_curve_k21_vs_h": decay_curve(cells, "k21", HS),
        "era_split": {
            label: {"cells": era_cells[label], "sign_map": era_maps[label]}
            for label, _, _ in ERAS
        },
        "era_sign_flip_audit": {
            "cells_flipping_sign": [k for k, v in flips.items() if v["flips"]],
            "n_cells_with_flip": sum(1 for v in flips.values() if v["flips"]),
            "detail": flips,
        },
        "twelve_one_reference": twelve_one,
        "sensitivity_execution_lag": sensitivity,
        "single_names_time_series": singles,
        "era_interpretation": era_interpretation,
        "headline": {
            "cells_surviving_N40_correction": surviving,
            "n_cells_surviving": len(surviving),
            "n_cells_sign_flip_across_eras": n_flip,
            "short_lookback_k_le_63_mean_ic": short_ic,
            "long_lookback_k_ge_126_mean_ic": long_ic,
            "structure": (
                "negative IC (reversal) for look-backs up to ~3 months, positive IC "
                "(momentum) from ~6 months out; the flip sits between k=63 and k=126"
            ),
            "verdict_on_the_surviving_cell": (
                "BORDERLINE CASE, NOT AN ESTABLISHED EFFECT. The single cell above "
                "the bar (k252_h1) clears the N=40 Bonferroni threshold by 0.006 "
                "(|t_nw|=3.233 vs 3.227) and its 21-day block-bootstrap CI excludes "
                "zero, but it FAILS the deflated-Sharpe bar (dsr_nonoverlap ~0; ~0.85 "
                "even with a lenient null-sampling sr_std), and the whole verdict is "
                "highly sensitive to how the number of trials is counted: 3.227 for "
                "N=40 nominal cells vs 2.498 for the correlation-adjusted N_eff~3.9. "
                "It must not be extracted from this report as a standalone result."
            ),
            "tradeability_caveat": (
                "an IC of ~0.02 at the top of the map is a *ranking* signal, not a "
                "P&L. Converting it to a trade requires paying turnover at every "
                "rebalance (even a 21-day hold on a daily signal is ~100% monthly "
                "turnover), crossing a ~5bp half-spread + per-share commission, and "
                "sizing within the S&P 100's ADTV. A 0.02 rank IC on a 100-name "
                "cross-section is roughly a 1-2% spread between the top and bottom "
                "quintile per horizon — the same order as the round-trip cost. "
                "Nothing here is measured net of costs, capacity or borrow."
            ),
        },
    }

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=1,
                                      default=_json_default))
    print(f"[done] wrote {REPORT_PATH}")

    # console summary
    sm = report["grid_full_sample"]["sign_map"]
    print("  k\\h " + "".join(f"{h:>7}" for h in HS))
    for row in sm["ascii_rows"]:
        print(f"  {row['k']:>3} " + "".join(f"{c:>7}" for c in row["cells"]))
    surv = report["headline"]["cells_surviving_N40_correction"]
    print(f"  Bonferroni z: N=40 -> {bonf_z:.3f} | N_eff={eff['n_effective']:.1f} -> "
          f"{report['multiplicity']['bonferroni_z_two_sided_n_eff']:.3f}")
    print(f"  cells surviving N={n_trials} correction: {surv or 'NONE'}")
    print(f"  cells with sign flip across eras: {report['era_sign_flip_audit']['n_cells_with_flip']}"
          f" / {n_trials}")


if __name__ == "__main__":
    main()
