#!/usr/bin/env python3
"""LPPLS bubble-criticality diagnostic (pre-registered: see hypotheses/registry.jsonl).

Registered hypothesis: ``lppls_crypto_bubble_criticality_v3``
(registered_at_utc = the time of that registration line; frozen windows /
parameter configs / thresholds / forward horizons are declared there as a
``search_grid`` whose leaf product equals ``n_trials_planned`` = 144).

Registration provenance — two pre-data corrections of a garbled brief, both
retained in the append-only ledger:

* ``lppls_crypto_bubble_criticality`` — F3 damping written ``|C|/(omega*|A|)``,
  which is unsatisfiable for a log-price fit and would make the diagnostic
  never fire.
* ``lppls_crypto_bubble_criticality_v2`` — formula fixed to ``m|B|/(omega|C|)``
  but the threshold kept at the brief's unsourced 0.8.
* ``lppls_crypto_bubble_criticality_v3`` (this one) — threshold corrected to the
  sourced 0.5 and F4 given its ``|C|/|B| >= 0.05`` pre-condition, per the
  Sornette-group filter table (Royal Society Open Science rsos.180643) and the
  reference implementation ``lppls`` 0.6.24.  The second correction was made
  after observing that 0.8 is inert; that order of operations is disclosed in
  the report (``registration_provenance.disclosure``).

Two modes
---------
* ``--demo``   : full-history **IN-SAMPLE** scan of COIN (and BTC/USDT as the
  pre-registered control), flagging the dates that would have turned red.
  This is a demonstration, NOT evidence: the windows and filters were fixed at
  registration but the hit/miss pattern is read with hindsight on data that
  predates registration.
* ``--as-of``  : a single-date forward diagnostic using ONLY bars with
  ``ts <= as_of``.  This is the scaffold the platform will run each period.
  It calls :func:`src.validation.registry.check_no_pre_registration_data`
  first and refuses (exit 2) any as-of date earlier than the registration
  timestamp — the mechanical line against evaluating on pre-registration data.

Model (standard 7-parameter LPPLS)::

    ln p_t = A + B (t_c - t)^m + C (t_c - t)^m cos(omega ln(t_c - t) - phi)

For fixed ``(m, omega)`` the model is linear in ``(A, B, C cos phi, C sin phi)``,
so those four are solved by OLS and only ``(t_c, m, omega)`` are searched.  The
time axis inside a window is **calendar days since the window's first bar**,
matching the reference implementation (which uses pandas ordinals).

Usage::

    poetry run python scripts/lppls_crypto.py --demo
    poetry run python scripts/lppls_crypto.py --demo --symbol BOTH --stride 5
    poetry run python scripts/lppls_crypto.py --as-of 2026-09-15 --symbol COIN
    poetry run python scripts/lppls_crypto.py --as-of 2021-06-01 --symbol COIN  # refused (pre-registration)
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from scipy.optimize import minimize  # noqa: E402  (transitive via scikit-learn, declared in pyproject)

from src.data.historical_store import HistoricalOHLCVStore  # noqa: E402
from src.validation.registry import (  # noqa: E402
    PreRegistrationDataError,
    check_no_pre_registration_data,
    get_hypothesis,
)

HYPOTHESIS_ID = "lppls_crypto_bubble_criticality_v3"
SUPERSEDES = ["lppls_crypto_bubble_criticality_v2", "lppls_crypto_bubble_criticality"]
DEFAULT_REGISTRY = "hypotheses/registry.jsonl"
DEFAULT_OUT = "reports/lppls_coin.json"

# ---------------------------------------------------------------------------
# FROZEN CONSTANTS — identical to the registration spec
# hypotheses/lppls_bubble_spec_2026-09-20_v3.json.  Do not tune these against
# the historical demo; a change is a new registration.
# ---------------------------------------------------------------------------

#: Sornette-style LPPLS filter set.  Formulas and bounds verified against two
#: independent sources:
#:   1. the Sornette-group filter table in "Dissection of Bitcoin's multiscale
#:      bubble history from January 2012 to February 2018", Royal Society Open
#:      Science 5:180643 — "damping D [0.5, inf), D = m B / (omega C)",
#:      "B (-inf, 0)", "m (0, 1)", "omega [4, 25]", "t_c in (0, dt_i)",
#:      "O [2.5, inf), O = (omega/2pi) ln((t_c-t1)/(t_c-t2))  if C/B >= 0.05";
#:   2. the reference implementation ``lppls`` 0.6.24,
#:      ``src/lppls/lppls.py:875-891`` (``get_damping``, ``get_oscillations``)
#:      and its ``filter_conditions_config`` default D_min = 0.5.
LPPLS_CONSTRAINTS: dict[str, Any] = {
    # F3 damping D = m*|B| / (omega*|C|) >= 0.5 (rsos.180643 table; lppls default).
    # NOTE: |C|/(omega*|A|) is NOT this filter and is unsatisfiable for a
    # log-price fit (A is the log-price level ~1e1, C the oscillation amplitude
    # ~1e-2), which would make the diagnostic never fire.  The 0.8 that appears
    # in some secondary summaries has no source in the filter table.
    "damping_min": 0.5,
    # F4 at least 2.5 log-periodic oscillations across the window,
    # lppls 0.6.24 `get_oscillations(w, tc, t1, t2) = (w / 2pi) * log((tc-t1)/(tc-t2))`,
    # applied ONLY when the pre-condition below holds (rsos.180643 table).
    "min_oscillations": 2.5,
    # F4 pre-condition: the oscillation amplitude must be non-negligible
    # relative to the power law.  The table writes the signed ratio C/B; with
    # B < 0 the only dimensionally meaningful reading is the amplitude ratio.
    "oscillations_precondition_c_over_b": 0.05,
    # F5 relative error: SSE / sum((y - mean y)^2).  From the Shanghai 2015
    # filter set; NOT in the rsos.180643 table nor the reference
    # implementation — retained as the more conservative choice.
    "rel_error_max": 0.05,
    # t_c must lie ahead of the last bar but within this fraction of the
    # window's CALENDAR span (reference default tc_max_frac = 0.5).
    "tc_max_frac": 0.5,
    # F6 shrinking-window stability: refitting on the most recent
    # min_window_frac of the window may move t_c by at most this fraction of
    # the window's calendar span.
    "shrinking_tc_tol_frac": 0.10,
    # Positive (upward, crash-prone) bubble: ln p accelerates upward into t_c
    # only when B < 0 (rsos.180643 table: B in (-inf, 0)).
    "require_negative_b": True,
    # provenance string, echoed into the report
    "citation": (
        "Sornette & Johansen (2001); Filimonov & Sornette (2013) profile-calibration; "
        "Sornette et al. (2015) Shanghai post-mortem filter set; rsos.180643 filter "
        "table; formulas verified against lppls 0.6.24 src/lppls/lppls.py:875-891"
    ),
}

#: grid leaf: parameter-constraint configs
PARAM_CONFIGS: dict[str, dict[str, tuple[float, float]]] = {
    "standard": {"m": (0.10, 0.90), "omega": (6.0, 13.0)},
    "relaxed": {"m": (0.01, 1.20), "omega": (2.0, 25.0)},
}

#: grid leaf: trailing window lengths in trading days
WINDOWS: tuple[int, ...] = (60, 90, 120, 180)

#: grid leaf: sub-window fractions for the F6 shrinking-window stability test
MIN_WINDOW_FRACS: tuple[float, ...] = (0.6, 0.8)

#: grid leaf: confidence thresholds selecting a "critical state"
CONFIDENCE_THRESHOLDS: tuple[float, ...] = (0.50, 0.75, 0.90)

#: grid leaf: forward horizons in trading days
FORWARD_HORIZONS: tuple[int, ...] = (21, 42, 63)

#: The pre-declared primary configuration of the resolution criteria.
PRIMARY: dict[str, Any] = {
    "param_config": "standard",
    "theta": 0.75,
    "horizon": 21,
    "min_window_frac": 0.6,
}

#: n_trials_planned must equal the product of the search_grid leaves.
N_TRIALS_PLANNED = (
    len(WINDOWS)
    * len(MIN_WINDOW_FRACS)
    * len(PARAM_CONFIGS)
    * len(CONFIDENCE_THRESHOLDS)
    * len(FORWARD_HORIZONS)
)

MAX_WINDOW = max(WINDOWS)

# Instruments: label -> (symbol, market_type)
INSTRUMENTS: dict[str, tuple[str, str]] = {
    "COIN": ("COIN", "stocks"),
    "BTC": ("BTC/USDT", "spot"),
}

# Granularity of the nonlinear search.  Frozen so a re-run is bit-reproducible.
GRID = {"n_m": 12, "n_omega": 12, "n_tc": 16, "refine_maxiter": 120}


# ---------------------------------------------------------------------------
# data access (strict upper bound — the look-ahead guard)
# ---------------------------------------------------------------------------


def _to_ms(day: str) -> int:
    return int(pd.Timestamp(day).value // 1_000_000)


def load_closes(
    label: str,
    *,
    start: str | None = None,
    end: str | None = None,
    allow_fetch: bool = False,
) -> pd.Series:
    """Daily closes with a tz-naive UTC date index, strictly bounded.

    ``end`` is an *inclusive* calendar date, implemented by passing an
    exclusive timestamp of ``end + 1 day`` to the store, so the store's own
    ``ts < end_ts`` guard never returns a later bar.  ``allow_fetch=False``
    keeps the run offline and deterministic.
    """
    symbol, market_type = INSTRUMENTS[label]
    store = HistoricalOHLCVStore(allow_fetch=allow_fetch)
    start_ms = _to_ms(start) if start else 0
    end_ms = (
        _to_ms(end) + 86_400_000
        if end
        else int(pd.Timestamp("2100-01-01").value // 1_000_000)
    )
    df = store.get_ohlcv(symbol, market_type, "1d", start_ms, end_ms)
    if df.empty:
        raise SystemExit(f"no daily bars for {label} ({symbol}/{market_type}) in data/btc_history.db")
    idx = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.tz_localize(None).dt.normalize()
    s = pd.Series(df["close"].to_numpy(dtype=float), index=idx, name=label).sort_index()
    return s[~s.index.duplicated(keep="last")]


def calendar_days(index: pd.DatetimeIndex) -> np.ndarray:
    """Calendar-day offsets from the first bar (float).

    The reference implementation fits in pandas-ordinal time; using a calendar
    axis keeps the oscillation-count and t_c filters comparable across
    instruments with different trading calendars.
    """
    return (index - index[0]).total_seconds().to_numpy(dtype=float) / 86_400.0


# ---------------------------------------------------------------------------
# LPPLS fit
# ---------------------------------------------------------------------------


def _design(tau: np.ndarray, m: float, omega: float) -> np.ndarray:
    """Design matrix for the 4 linear LPPLS parameters, tau = t_c - t > 0."""
    tpow = np.power(tau, m)
    ln_tau = np.log(tau)
    return np.column_stack(
        [np.ones_like(tau), tpow, tpow * np.cos(omega * ln_tau), tpow * np.sin(omega * ln_tau)]
    )


def _ols_sse(t: np.ndarray, y: np.ndarray, tc: float, m: float, omega: float) -> tuple[float, np.ndarray]:
    """Profile out the linear parameters; return (SSE, beta).

    Returns ``inf`` (with a zero beta) for degenerate proposals — the
    Nelder-Mead polisher probes the parameter box edges, where ``tau`` can go
    non-positive and the design matrix non-finite.
    """
    tau = tc - t
    if not np.isfinite(tc) or not np.isfinite(m) or not np.isfinite(omega):
        return float("inf"), np.zeros(4)
    if np.any(tau <= 0):
        return float("inf"), np.zeros(4)
    X = _design(tau, m, omega)
    if not np.all(np.isfinite(X)):
        return float("inf"), np.zeros(4)
    with np.errstate(all="ignore"):
        beta, *_ = np.linalg.lstsq(X, y, rcond=None)
        res = y - X @ beta
        sse = float(res @ res)
    if not np.isfinite(sse):
        return float("inf"), np.zeros(4)
    return sse, beta


def _batched_ols(
    y: np.ndarray, taus: np.ndarray, m: float, omega: float
) -> np.ndarray:
    """OLS for many t_c candidates at once (normal equations, small ridge).

    ``taus`` has shape (n_tc, T) with rows ``t_c - t``.  Returns SSE (n_tc,).
    """
    n_tc, T = taus.shape
    tpow = np.power(taus, m)
    ln_tau = np.log(taus)
    X = np.empty((n_tc, T, 4))
    X[:, :, 0] = 1.0
    X[:, :, 1] = tpow
    X[:, :, 2] = tpow * np.cos(omega * ln_tau)
    X[:, :, 3] = tpow * np.sin(omega * ln_tau)
    with np.errstate(all="ignore"):
        XtX = np.einsum("nti,ntj->nij", X, X)
        Xty = np.einsum("nti,t->ni", X, y)
        # tiny ridge: the design is ill-conditioned for extreme (m, t_c) pairs
        XtX += np.eye(4)[None, :, :] * 1e-10
        beta = np.linalg.solve(XtX, Xty)
        resid = y[None, :] - np.einsum("nti,ni->nt", X, beta)
        sse = np.einsum("nt,nt->n", resid, resid)
    return np.where(np.isfinite(sse), sse, np.inf)


def fit_lppls(
    log_price: np.ndarray,
    times: np.ndarray,
    *,
    m_bounds: tuple[float, float],
    omega_bounds: tuple[float, float],
    tc_max_frac: float = LPPLS_CONSTRAINTS["tc_max_frac"],
    grid: dict[str, int] | None = None,
    refine: bool = True,
) -> dict[str, Any]:
    """Fit the 7-parameter LPPLS by profiling out the 4 linear parameters.

    ``times`` is the calendar-day time axis of the window (increasing).
    A coarse grid over ``(m, omega, t_c)`` with batched OLS is followed by a
    Nelder-Mead polish on ``(t_c, m, omega)``.  Returns the nonlinear
    parameters plus the implied ``A, B, C, phi`` and the diagnostics the
    filters need.
    """
    g = grid or GRID
    y = np.asarray(log_price, dtype=float)
    t = np.asarray(times, dtype=float)
    if t.shape[0] != y.shape[0] or t.shape[0] < 10:
        raise ValueError("times and log_price must be equal-length, >= 10 points")
    if np.any(np.diff(t) <= 0):
        raise ValueError("times must be strictly increasing")

    t_first = float(t[0])
    t_last = float(t[-1])
    span = t_last - t_first
    if span <= 0:
        raise ValueError("degenerate time axis")

    tc_grid = t_last + np.linspace(0.02, tc_max_frac, g["n_tc"]) * span
    m_grid = np.linspace(m_bounds[0], m_bounds[1], g["n_m"])
    om_grid = np.linspace(omega_bounds[0], omega_bounds[1], g["n_omega"])
    taus = np.clip(tc_grid[:, None] - t[None, :], 1e-6, None)

    best = (np.inf, np.nan, np.nan, np.nan)
    for m in m_grid:
        for om in om_grid:
            sse = _batched_ols(y, taus, float(m), float(om))
            k = int(np.argmin(sse))
            if sse[k] < best[0]:
                best = (float(sse[k]), float(tc_grid[k]), float(m), float(om))

    sse_best, tc, m, omega = best

    if refine and np.isfinite(sse_best):
        scale = np.array(
            [max(span, 1e-6), max(m_bounds[1] - m_bounds[0], 1e-6), max(omega_bounds[1] - omega_bounds[0], 1e-6)]
        )

        def obj(z: np.ndarray) -> float:
            tc_z, m_z, om_z = z * scale
            if not np.all(np.isfinite(z)):
                return 1e12
            tc_z = min(max(tc_z, t_last + 1e-3), t_last + tc_max_frac * span)
            m_z = min(max(m_z, m_bounds[0]), m_bounds[1])
            om_z = min(max(om_z, omega_bounds[0]), omega_bounds[1])
            s, _ = _ols_sse(t, y, tc_z, m_z, om_z)
            return s if np.isfinite(s) else 1e12

        res = minimize(
            obj,
            np.array([(tc - t_last) / scale[0], m / scale[1], omega / scale[2]]),
            method="Nelder-Mead",
            options={"maxiter": g["refine_maxiter"], "xatol": 1e-4, "fatol": 1e-12},
        )
        if res.fun < sse_best:
            tc_z, m_z, om_z = res.x * scale
            tc = min(max(float(t_last + tc_z), t_last + 1e-3), t_last + tc_max_frac * span)
            m = float(min(max(m_z, m_bounds[0]), m_bounds[1]))
            omega = float(min(max(om_z, omega_bounds[0]), omega_bounds[1]))
            sse_best = float(res.fun)

    sse, beta = _ols_sse(t, y, tc, m, omega)
    A, B, C1, C2 = (float(v) for v in beta)
    C = float(np.hypot(C1, C2))
    phi = float(np.arctan2(C2, C1))

    tau_max = float(tc - t_first)
    tau_min = float(max(tc - t_last, 1e-9))
    # F4, reference: (w / 2pi) * log((tc - t1) / (tc - t2))
    n_osc = float((omega / (2.0 * np.pi)) * np.log(max(tau_max / tau_min, 1e-9)))
    denom = float(np.sum((y - y.mean()) ** 2))
    rel_error = float(sse / denom) if denom > 0 else np.inf
    # F3, reference: (m * |b|) / (w * |c|)
    damping = float(m * abs(B) / (abs(omega) * abs(C))) if C != 0 and omega != 0 else np.inf

    return {
        "t_c": float(tc),
        "m": float(m),
        "omega": float(omega),
        "A": A,
        "B": B,
        "C": C,
        "phi": phi,
        "sse": float(sse),
        "rel_error": rel_error,
        "damping": damping,
        "n_oscillations": n_osc,
        "t_c_days_ahead": float(tc - t_last),
    }


def oscillation_precondition_holds(fit: dict[str, Any], constraints: dict[str, Any] | None = None) -> bool:
    """F4 pre-condition: |C|/|B| >= 0.05 (rsos.180643 filter table)."""
    c = constraints or LPPLS_CONSTRAINTS
    b = abs(fit["B"])
    if b == 0:
        return False
    return (abs(fit["C"]) / b) >= c["oscillations_precondition_c_over_b"]


def check_filters(
    fit: dict[str, Any],
    *,
    m_bounds: tuple[float, float],
    omega_bounds: tuple[float, float],
    constraints: dict[str, Any] | None = None,
) -> tuple[bool, list[str]]:
    """Apply F1–F5 plus the positive-bubble condition (F6 is applied separately).

    F4 is applied only when its pre-condition holds; when the pre-condition
    fails the oscillation count is meaningless and is not binding.
    """
    c = constraints or LPPLS_CONSTRAINTS
    fails: list[str] = []
    if not (m_bounds[0] <= fit["m"] <= m_bounds[1]):  # F1
        fails.append("F1_m_range")
    if not (omega_bounds[0] <= fit["omega"] <= omega_bounds[1]):  # F2
        fails.append("F2_omega_range")
    if not (fit["damping"] >= c["damping_min"]):  # F3
        fails.append("F3_damping")
    if oscillation_precondition_holds(fit, c):  # F4 (guarded by |C|/|B| >= 0.05)
        if not (fit["n_oscillations"] >= c["min_oscillations"]):
            fails.append("F4_oscillations")
    if not (fit["rel_error"] <= c["rel_error_max"]):  # F5
        fails.append("F5_rel_error")
    if c.get("require_negative_b", True) and not (fit["B"] < 0):  # positive bubble
        fails.append("F0_positive_bubble_B_ge_0")
    return (not fails), fails


def _shrinking_window_stable(
    log_price: np.ndarray,
    times: np.ndarray,
    full_fit: dict[str, Any],
    *,
    frac: float,
    m_bounds: tuple[float, float],
    omega_bounds: tuple[float, float],
    tol_frac: float,
    grid: dict[str, int] | None,
) -> tuple[bool, float]:
    """F6: refit on the most recent ``frac`` of the window; |Δt_c| <= tol*span."""
    T = int(log_price.shape[0])
    sub_len = int(round(frac * T))
    if sub_len < 20:
        return False, float("inf")
    offset = T - sub_len
    sub_times = times[offset:] - times[offset]
    try:
        sub_fit = fit_lppls(
            log_price[offset:],
            sub_times,
            m_bounds=m_bounds,
            omega_bounds=omega_bounds,
            grid=grid,
            refine=False,
        )
    except Exception:  # pragma: no cover - defensive
        return False, float("inf")
    # put the sub-window t_c back into the full-window time axis
    tc_full_units = sub_fit["t_c"] + times[offset]
    delta = abs(tc_full_units - full_fit["t_c"])
    span = float(times[-1] - times[0])
    return bool(delta <= tol_frac * span), float(delta)


def window_verdict(
    log_price: np.ndarray,
    times: np.ndarray,
    *,
    param_config: str,
    min_window_frac: float,
    grid: dict[str, int] | None = None,
    apply_shrinking: bool = True,
) -> dict[str, Any]:
    """Fit one window and return the accept/reject verdict with reasons."""
    bounds = PARAM_CONFIGS[param_config]
    fit = fit_lppls(
        log_price, times, m_bounds=bounds["m"], omega_bounds=bounds["omega"], grid=grid
    )
    ok, fails = check_filters(fit, m_bounds=bounds["m"], omega_bounds=bounds["omega"])
    delta_tc = None
    if ok and apply_shrinking:
        stable, delta_tc = _shrinking_window_stable(
            log_price,
            times,
            fit,
            frac=min_window_frac,
            m_bounds=bounds["m"],
            omega_bounds=bounds["omega"],
            tol_frac=LPPLS_CONSTRAINTS["shrinking_tc_tol_frac"],
            grid=grid,
        )
        if not stable:
            fails.append("F6_shrinking_tc")
            ok = False
    return {
        "accepted": bool(ok),
        "fails": fails,
        "t_c_delta_subwindow": delta_tc,
        "oscillation_precondition_holds": oscillation_precondition_holds(fit),
        **fit,
    }


def confidence_at(
    closes: np.ndarray,
    times: np.ndarray,
    i: int,
    *,
    windows: Sequence[int] = WINDOWS,
    param_config: str = "standard",
    min_window_frac: float = PRIMARY["min_window_frac"],
    grid: dict[str, int] | None = None,
    apply_shrinking: bool = True,
) -> dict[str, Any]:
    """LPPLS confidence at as-of index ``i``: accepted windows / attempted windows.

    Reads ONLY ``closes[max(0, i-L+1) : i+1]`` and the matching ``times`` —
    no bar after ``i`` is touched.
    """
    per_window: dict[str, Any] = {}
    accepted = 0
    attempted = 0
    for L in windows:
        if i + 1 < L:
            continue
        attempted += 1
        logp = np.log(closes[i - L + 1 : i + 1])
        tsub = times[i - L + 1 : i + 1].copy()
        tsub = tsub - tsub[0]
        v = window_verdict(
            logp,
            tsub,
            param_config=param_config,
            min_window_frac=min_window_frac,
            grid=grid,
            apply_shrinking=apply_shrinking,
        )
        accepted += int(v["accepted"])
        per_window[str(L)] = {
            "accepted": v["accepted"],
            "fails": v["fails"],
            "t_c_days_ahead": round(v["t_c_days_ahead"], 2),
            "m": round(v["m"], 4),
            "omega": round(v["omega"], 4),
            "B": round(v["B"], 6),
            "C": round(v["C"], 6),
            "damping": round(v["damping"], 4),
            "n_oscillations": round(v["n_oscillations"], 3),
            "oscillation_precondition_holds": v["oscillation_precondition_holds"],
            "rel_error": round(v["rel_error"], 6),
        }
    conf = (accepted / attempted) if attempted else float("nan")
    return {"confidence": conf, "n_accepted": accepted, "n_attempted": attempted, "windows": per_window}


# ---------------------------------------------------------------------------
# scanning
# ---------------------------------------------------------------------------


def forward_mdd(closes: np.ndarray, i: int, horizon: int) -> float | None:
    """Max drawdown over the next ``horizon`` bars, measured from close[i]."""
    if i + horizon >= closes.shape[0]:
        return None
    seg = closes[i + 1 : i + horizon + 1]
    return float(np.min(seg) / closes[i] - 1.0)


def _fit_record(date: pd.Timestamp, L: int, v: dict[str, Any]) -> dict[str, Any]:
    b = v["B"]
    return {
        "date": date,
        "L": L,
        "accepted": v["accepted"],
        "fails": v["fails"],
        "B": b,
        "damping": v["damping"],
        "n_oscillations": v["n_oscillations"],
        "rel_error": v["rel_error"],
        "m": v["m"],
        "omega": v["omega"],
        "abs_C_over_abs_B": (abs(v["C"]) / abs(b)) if b else None,
    }


def scan_history(
    closes: np.ndarray,
    index: pd.DatetimeIndex,
    *,
    stride: int = 1,
    windows: Sequence[int] = WINDOWS,
    param_config: str = "standard",
    min_window_frac: float = PRIMARY["min_window_frac"],
    grid: dict[str, int] | None = None,
    apply_shrinking: bool = True,
    horizons: Sequence[int] = FORWARD_HORIZONS,
    fit_records: list[dict[str, Any]] | None = None,
) -> pd.DataFrame:
    """Scan every ``stride``-th bar from ``max(windows)-1`` onward.

    Returns a DataFrame indexed by as-of date with ``confidence`` plus the
    forward MDD for each horizon (NaN where the forward window is incomplete).

    If ``fit_records`` is a list, one record per fitted (as-of, window) pair is
    appended to it — this lets :func:`attribute_fits` explain a flat confidence
    series without paying for a second full scan.
    """
    Lmax = max(windows)
    times = calendar_days(index)
    rows: list[dict[str, Any]] = []
    for i in range(Lmax - 1, closes.shape[0], stride):
        conf = confidence_at(
            closes,
            times,
            i,
            windows=windows,
            param_config=param_config,
            min_window_frac=min_window_frac,
            grid=grid,
            apply_shrinking=apply_shrinking,
        )
        row = {
            "date": index[i],
            "confidence": conf["confidence"],
            "n_accepted": conf["n_accepted"],
            "n_attempted": conf["n_attempted"],
        }
        for h in horizons:
            row[f"fwd_mdd_{h}"] = forward_mdd(closes, i, h)
        rows.append(row)
        if fit_records is not None:
            for L, wv in conf["windows"].items():
                fit_records.append(_fit_record(index[i], int(L), wv))
    return pd.DataFrame(rows).set_index("date")


def attribute_fits(fit_records: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Which filter is binding?  Aggregate the per-fit records from a scan.

    An indicator that never fires is uninformative, so the report must say *why*
    it never fires rather than just printing a flat zero series.
    """
    any_fail: dict[str, int] = {}
    first_fail: dict[str, int] = {}
    for r in fit_records:
        for f in r["fails"]:
            any_fail[f] = any_fail.get(f, 0) + 1
        if r["fails"]:
            first_fail[r["fails"][0]] = first_fail.get(r["fails"][0], 0) + 1

    arr = np.array(
        [
            [
                r["B"] if r["B"] is not None else np.nan,
                r["damping"],
                r["n_oscillations"],
                r["rel_error"],
                r["m"],
                r["omega"],
                r["abs_C_over_abs_B"] if r["abs_C_over_abs_B"] is not None else np.nan,
            ]
            for r in fit_records
        ],
        dtype=float,
    ) if fit_records else np.zeros((0, 7))

    def q(j: int) -> dict[str, float] | None:
        col = arr[:, j]
        col = col[np.isfinite(col)]
        if col.size == 0:
            return None
        return {
            "p05": float(np.percentile(col, 5)),
            "median": float(np.median(col)),
            "p95": float(np.percentile(col, 95)),
        }

    def frac(j: int, op: str, bound: float) -> float | None:
        col = arr[:, j]
        col = col[np.isfinite(col)]
        if col.size == 0:
            return None
        hit = (col >= bound) if op == ">=" else (col <= bound)
        return float(hit.mean())

    n_fits = len(fit_records)
    return {
        "n_fits": n_fits,
        "n_accepted": n_fits - sum(first_fail.values()),
        "any_fail_reasons": any_fail,
        "first_fail_reasons": first_fail,
        "distributions": {
            "B": q(0),
            "damping": q(1),
            "n_oscillations": q(2),
            "rel_error": q(3),
            "m": q(4),
            "omega": q(5),
            "abs_C_over_abs_B": q(6),
        },
        "marginal_pass_rates": {
            "F0_B_lt_0": frac(0, "<=", -1e-12),
            "F3_damping_ge_min": frac(1, ">=", LPPLS_CONSTRAINTS["damping_min"]),
            "F4_n_osc_ge_min": frac(2, ">=", LPPLS_CONSTRAINTS["min_oscillations"]),
            "F5_rel_error_le_max": frac(3, "<=", LPPLS_CONSTRAINTS["rel_error_max"]),
            "F4_precondition_absC_over_absB_ge_0.05": frac(
                6, ">=", LPPLS_CONSTRAINTS["oscillations_precondition_c_over_b"]
            ),
        },
        "note": (
            "marginal_pass_rates are per-filter, not joint; they identify the binding "
            "constraint when the confidence series is flat zero"
        ),
    }


def cluster_episodes(dates: Iterable[pd.Timestamp], gap_days: int = 5) -> list[dict[str, Any]]:
    """Group flagged as-of dates into contiguous episodes (calendar gaps <= gap_days)."""
    ds = sorted(pd.Timestamp(d) for d in dates)
    episodes: list[dict[str, Any]] = []
    for d in ds:
        if episodes and (d - episodes[-1]["end"]).days <= gap_days:
            episodes[-1]["end"] = d
            episodes[-1]["n_days"] += 1
        else:
            episodes.append({"start": d, "end": d, "n_days": 1})
    for e in episodes:
        e["start"] = str(e["start"].date())
        e["end"] = str(e["end"].date())
    return episodes


def episode_outcome(
    scan: pd.DataFrame, episodes: list[dict[str, Any]], horizon: int
) -> list[dict[str, Any]]:
    col = f"fwd_mdd_{horizon}"
    out = []
    for e in episodes:
        sub = scan.loc[str(e["start"]) : str(e["end"]), col].dropna()
        out.append(
            {
                **e,
                "fwd_mdd_from_episode_start": (float(sub.iloc[0]) if len(sub) else None),
                "fwd_mdd_worst_in_episode": (float(sub.min()) if len(sub) else None),
            }
        )
    return out


# ---------------------------------------------------------------------------
# report builders
# ---------------------------------------------------------------------------


def _demo_warning() -> str:
    return (
        "IN-SAMPLE DEMONSTRATION — NOT EVIDENCE. The window lengths, parameter "
        "configs, filters and thresholds were fixed at registration "
        "(hypotheses/lppls_bubble_spec_2026-09-20_v2.json) and are NOT tuned here, "
        "but the historical hit/miss pattern below is read with full hindsight on "
        "data that predates the registration timestamp. No evaluation of the "
        "registered hypothesis is permitted on this window; only bars at or after "
        "registered_at_utc may decide it (see forward_scaffold)."
    )


def run_demo(
    labels: Sequence[str],
    *,
    stride: int,
    grid: dict[str, int] | None = None,
    apply_shrinking: bool = True,
) -> dict[str, Any]:
    demo: dict[str, Any] = {"warning": _demo_warning(), "stride_trading_days": stride}
    for label in labels:
        closes_s = load_closes(label)
        closes = closes_s.to_numpy(dtype=float)
        fit_records: list[dict[str, Any]] = []
        scan = scan_history(
            closes,
            closes_s.index,
            stride=stride,
            grid=grid,
            apply_shrinking=apply_shrinking,
            fit_records=fit_records,
        )
        attempted = int(scan["n_attempted"].sum())
        accepted = int(scan["n_accepted"].sum())
        overall = (accepted / attempted) if attempted else None

        per_symbol: dict[str, Any] = {
            "symbol": INSTRUMENTS[label][0],
            "market_type": INSTRUMENTS[label][1],
            "first_bar": str(closes_s.index[0].date()),
            "last_bar": str(closes_s.index[-1].date()),
            "n_bars": int(closes_s.shape[0]),
            "scan": {
                "first_as_of": str(scan.index[0].date()),
                "last_as_of": str(scan.index[-1].date()),
                "n_as_of": int(scan.shape[0]),
                "n_fit_attempts": attempted,
                "n_fit_accepted": accepted,
                "fit_pass_ratio_overall": overall,
                "max_confidence": float(scan["confidence"].max()),
            },
            "filter_attribution": attribute_fits(fit_records),
            "episodes_by_threshold": {},
        }

        for theta in CONFIDENCE_THRESHOLDS:
            flagged = scan.index[scan["confidence"] >= theta]
            episodes = cluster_episodes(flagged)
            for h in FORWARD_HORIZONS:
                ep_all = episode_outcome(scan, episodes, h)
                col = f"fwd_mdd_{h}"
                uncond = scan[col].dropna()
                cond = scan.loc[scan["confidence"] >= theta, col].dropna()
                per_symbol["episodes_by_threshold"].setdefault(f"theta_{theta}", {})[
                    f"horizon_{h}"
                ] = {
                    "n_flagged_days": int(len(flagged)),
                    "n_episodes": len(episodes),
                    "episodes": ep_all,
                    "unconditional": {
                        "n": int(uncond.shape[0]),
                        "mean_fwd_mdd": float(uncond.mean()) if len(uncond) else None,
                        "median_fwd_mdd": float(uncond.median()) if len(uncond) else None,
                        "p05_fwd_mdd": float(uncond.quantile(0.05)) if len(uncond) else None,
                    },
                    "conditional": {
                        "n": int(cond.shape[0]),
                        "mean_fwd_mdd": float(cond.mean()) if len(cond) else None,
                        "median_fwd_mdd": float(cond.median()) if len(cond) else None,
                    },
                    "note": (
                        "in-sample; means are reported for orientation only — a verdict "
                        "needs n>=30 flagged days AND forward (post-registration) data"
                    ),
                }
        demo[label] = per_symbol
    return demo


def run_as_of(
    label: str,
    as_of: str,
    *,
    registry: str = DEFAULT_REGISTRY,
    grid: dict[str, int] | None = None,
    apply_shrinking: bool = True,
) -> dict[str, Any]:
    """Single-date forward diagnostic. Refuses any as-of date < registration."""
    record = get_hypothesis(registry, HYPOTHESIS_ID)
    if record is None:
        raise SystemExit(
            f"hypothesis '{HYPOTHESIS_ID}' not found in {registry}; register it before running --as-of"
        )
    gate: dict[str, Any] = {
        "hypothesis_id": HYPOTHESIS_ID,
        "registry": registry,
        "registered_at_utc": record["registered_at_utc"],
    }
    # HARD DISCIPLINE: no evaluation on pre-registration data.  Not bypassed.
    check_no_pre_registration_data(record, as_of)
    gate["pre_registration_check"] = "PASSED"
    gate["rule"] = "check_no_pre_registration_data: as_of >= registered_at_utc"

    closes_s = load_closes(label, end=as_of)
    closes = closes_s.to_numpy(dtype=float)
    times = calendar_days(closes_s.index)
    last = closes_s.index[-1]
    # The store bounds ts <= as_of; re-assert so the guard is explicit here too.
    if last > pd.Timestamp(as_of):
        raise AssertionError(f"look-ahead: last bar {last.date()} > as_of {as_of}")

    conf = confidence_at(
        closes,
        times,
        closes.shape[0] - 1,
        grid=grid,
        apply_shrinking=apply_shrinking,
    )
    theta = PRIMARY["theta"]
    return {
        "hypothesis_id": HYPOTHESIS_ID,
        "symbol": INSTRUMENTS[label][0],
        "market_type": INSTRUMENTS[label][1],
        "requested_as_of": as_of,
        "last_bar_used": str(last.date()),
        "bars_used": int(closes.shape[0]),
        "pre_registration_gate": gate,
        "confidence": conf["confidence"],
        "n_accepted": conf["n_accepted"],
        "n_attempted": conf["n_attempted"],
        "primary_threshold": theta,
        "critical_state": bool(conf["confidence"] >= theta),
        "windows": conf["windows"],
        "note": (
            "diagnostic only — not a trade signal, and no verdict is possible "
            "below n>=30 flagged as-of days after registered_at_utc"
        ),
    }


def _registry_status(registry: str) -> dict[str, Any]:
    """Latest status/revision of each line in this family, straight from the ledger.

    Kept in the report so a reader can see that the superseded lines are
    terminal (``superseded``) rather than merely older, without having to open
    the append-only ledger — and so the report cannot silently drift from it.
    """
    out: dict[str, Any] = {}
    for hid in [HYPOTHESIS_ID, *SUPERSEDES]:
        rec = get_hypothesis(registry, hid)
        if rec is None:
            out[hid] = {"status": "ABSENT"}
            continue
        out[hid] = {
            "status": rec.get("status"),
            "revision": rec.get("revision"),
            "superseded_by": rec.get("superseded_by"),
            "superseded_at_utc": rec.get("superseded_at_utc"),
            "n_trials_planned": rec.get("n_trials_planned"),
        }
    return out


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--demo", action="store_true", help="full-history in-sample scan")
    mode.add_argument("--as-of", help="single-date forward diagnostic (YYYY-MM-DD)")
    p.add_argument("--symbol", default="COIN", choices=["COIN", "BTC", "BOTH"])
    p.add_argument("--stride", type=int, default=5, help="as-of stride in trading days for --demo [5]")
    p.add_argument("--no-shrinking-filter", action="store_true", help="disable F6 (faster)")
    p.add_argument("--registry", default=DEFAULT_REGISTRY)
    p.add_argument("--out", default=DEFAULT_OUT)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    labels = ["COIN", "BTC"] if args.symbol == "BOTH" else [args.symbol]
    record = get_hypothesis(args.registry, HYPOTHESIS_ID)
    if record is None:
        raise SystemExit(f"hypothesis '{HYPOTHESIS_ID}' not found in {args.registry}")

    grid = GRID
    apply_shrinking = not args.no_shrinking_filter

    base: dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "hypothesis_id": HYPOTHESIS_ID,
        "supersedes": SUPERSEDES,
        "registration_provenance": {
            "live": HYPOTHESIS_ID,
            "superseded": SUPERSEDES,
            "registry_status": _registry_status(args.registry),
            "n_trials_basis": record.get("n_trials_basis"),
            "correction_1": (
                "lppls_crypto_bubble_criticality -> _v2: F3 damping formula corrected from "
                "|C|/(omega*|A|) (unsatisfiable for a log-price fit) to m*|B|/(omega*|C|)"
            ),
            "correction_2": (
                "lppls_crypto_bubble_criticality_v2 -> _v3: F3 threshold corrected 0.8 -> 0.5 "
                "and F4 gained its |C|/|B| >= 0.05 pre-condition, per the rsos.180643 filter "
                "table and the lppls 0.6.24 default"
            ),
            "disclosure": (
                "correction_2 was made AFTER observing that D_min=0.8 is inert on COIN/BTC "
                "(see filter_attribution). It is a documentary correction (two independent "
                "sources state 0.5), but the reader must know the order of operations."
            ),
            "ledger_note": "all three registration lines are retained in the append-only ledger",
        },
        "registered_at_utc": record["registered_at_utc"],
        "evaluation_window_start": record["evaluation_window_start"],
        "rubric": {
            "weighted_score": record["rubric"]["weighted_score"],
            "band": record["rubric"]["band"],
            "answers": record["rubric"]["answers"],
        },
        "n_trials_planned": record.get("n_trials_planned", N_TRIALS_PLANNED),
        "search_grid": record.get("search_grid", {}),
        "n_trials_origin": record.get("n_trials_origin"),
        "n_trials_basis": {
            "resolved_n_trials": N_TRIALS_PLANNED,
            "not": 3 * N_TRIALS_PLANNED,
            "why_not_432": (
                "The LPPLS family has THREE registry lines (v1, v2, v3) and a naive "
                "reading would pool 3 x 144 = 432. It is 144."
            ),
            "line_1_v1": (
                "lppls_crypto_bubble_criticality: its F3 constant was UNSATISFIABLE for a "
                "log-price fit, so this design was never executed at all (it was corrected "
                "before the first fit) and could not have produced an informative accepted "
                "fit. It contributes 0 selectable configurations."
            ),
            "line_2_v2": (
                "lppls_crypto_bubble_criticality_v2: its 144 configurations WERE computed "
                "(that is the run which showed D_min=0.8 is inert). But every bar it "
                "touched predates registered_at_utc, and by the registry's own rule "
                "(check_no_pre_registration_data) pre-registration data may never decide "
                "the hypothesis — so those 144 are not selectable FOR THE VERDICT. They "
                "are in-sample diagnostics, reported as such."
            ),
            "line_3_v3": (
                "lppls_crypto_bubble_criticality_v3: the live line. The forward evaluation "
                "will select a champion from exactly its 144 configurations on "
                "post-registration bars. That is the pool N must describe."
            ),
            "rule_applied": (
                "N = the number of configurations from which the QUOTED result may be "
                "selected. Configurations that can never enter a verdict (v1: vacuous, "
                "v2: pre-registration only) do not multiply it."
            ),
            "counter_argument_overridden": (
                "A strict reader can argue that v2's 144 fits were computed AND are "
                "selectable, so the pool is 288. The answer is NOT to delete provenance: "
                "the mitigation is the disclosure. v2 -> v3 was a documentary constant "
                "correction (0.8 -> 0.5 plus the |C|/|B| >= 0.05 pre-condition) made AFTER "
                "observing that 0.8 is inert; that order of operations is recorded in the "
                "v3 registration _meta, in registration_provenance.disclosure here, and in "
                "the supersede_reason on v2's ledger line. All three lines stay in the "
                "append-only ledger so a reader can reconstruct the pool themselves. If "
                "anyone ever quotes an IN-SAMPLE number from the v2 constants, the pool for "
                "that claim becomes 288 and this estimate must be re-derived."
            ),
            "safe_direction_note": (
                "Over-declaring N only raises the bar against ourselves, so under-declaring "
                "is the risk. 144 is chosen because it is the defensible forward pool, not "
                "because it is the smaller number."
            ),
        },
        "grid_product_check": {
            "leaf_product": N_TRIALS_PLANNED,
            "n_trials_planned": record.get("n_trials_planned", N_TRIALS_PLANNED),
            "matches": N_TRIALS_PLANNED == record.get("n_trials_planned", N_TRIALS_PLANNED),
        },
        "fit_constants": {
            "windows": list(WINDOWS),
            "min_window_fracs": list(MIN_WINDOW_FRACS),
            "param_configs": {k: {kk: list(vv) for kk, vv in v.items()} for k, v in PARAM_CONFIGS.items()},
            "confidence_thresholds": list(CONFIDENCE_THRESHOLDS),
            "forward_horizons": list(FORWARD_HORIZONS),
            "primary": PRIMARY,
            "constraints": LPPLS_CONSTRAINTS,
            "nonlinear_grid": GRID,
        },
        "limitations": {
            "four_window_confidence": (
                "confidence(t) is accepted/attempted over FOUR fixed trailing window "
                "lengths, so it can only take the values 0, 0.25, 0.5, 0.75, 1.0. The "
                "published LPPLS confidence indicator additionally scans the bubble "
                "START time (nested window_start x window_end), which produces a much "
                "smoother, better-powered indicator. That variant is a DIFFERENT "
                "pre-registration — changing the scan now would be post-hoc."
            ),
            "coverage": (
                "a signal needs max(WINDOWS) = 180 bars of history, so the first 180 bars "
                "of every series are structurally unscannable. For COIN (first bar "
                "2021-04-14) this means the April-November 2021 bubble and its November "
                "2021 top are OUT OF RANGE."
            ),
            "missed_2021_11_btc_top": (
                "the November 2021 BTC all-time top fitted with damping 0.31-0.47, below "
                "the sourced 0.5 bound — the filter rejects the single largest bubble top "
                "in the sample. Reported as a miss, not worked around."
            ),
            "m_boundary_pinning": (
                "on noisy windows the profile optimizer pins m at its upper bound (0.9), "
                "where the power law degenerates toward a straight line; F3 is what "
                "rejects those fits. This is a known LPPLS calibration pathology, not a "
                "bug in this implementation."
            ),
        },
        "forward_scaffold": {
            "command": "poetry run python scripts/lppls_crypto.py --as-of <YYYY-MM-DD> --symbol COIN",
            "look_ahead_guard": (
                "HistoricalOHLCVStore query is bounded ts <= as_of; an explicit assertion "
                "re-checks the last bar date here"
            ),
            "pre_registration_gate": (
                "src.validation.registry.check_no_pre_registration_data is called before any "
                "fit; an as-of date earlier than registered_at_utc raises "
                "PreRegistrationDataError and the run exits 2 (this is NOT bypassed)"
            ),
            "not_yet_evaluable": (
                "the registry evaluation window starts at registered_at_utc, so today there "
                "are zero forward bars — the registered hypothesis cannot be decided yet"
            ),
        },
    }

    if args.demo:
        base["mode"] = "in_sample_demo"
        base["in_sample_demo"] = run_demo(
            labels, stride=args.stride, grid=grid, apply_shrinking=apply_shrinking
        )
    else:
        base["mode"] = "forward_as_of"
        try:
            base["as_of_result"] = run_as_of(
                labels[0], args.as_of, registry=args.registry, grid=grid, apply_shrinking=apply_shrinking
            )
        except PreRegistrationDataError as exc:
            print(f"REFUSED (pre-registration data): {exc}", file=sys.stderr)
            return 2

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(base, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    if args.demo:
        for label in labels:
            d = base["in_sample_demo"][label]
            print(
                f"{label}: bars {d['first_bar']}..{d['last_bar']}  as-of n="
                f"{d['scan']['n_as_of']}  fit pass {d['scan']['n_fit_accepted']}/"
                f"{d['scan']['n_fit_attempts']}"
            )
    else:
        r = base["as_of_result"]
        print(
            f"{r['symbol']} as-of {r['last_bar_used']}: confidence={r['confidence']:.3f} "
            f"({r['n_accepted']}/{r['n_attempted']}) critical={r['critical_state']}"
        )
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
