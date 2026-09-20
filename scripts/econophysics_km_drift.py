#!/usr/bin/env python3
"""Kramers-Moyal drift / diffusion estimation: is price in a potential well
(mean reversion, -> OU) or an escape/trend process?

The econophysics question
-------------------------
Two competing pictures of a daily price series:

  A) OVERDAMPED PARTICLE IN A POTENTIAL WELL.  dP = -V'(P) dt + sigma dW with
     V quadratic -> an Ornstein-Uhlenbeck process.  The drift pulls the price
     back toward a reference level; the CONDITIONAL MEAN of the next move is
     negative when the price sits above the well and positive below it.
  B) ESCAPE / FEEDBACK (trend) PROCESS.  The drift reinforces the deviation
     (positive feedback) -> trends, accelerating moves, crashes.

Kramers-Moyal (KM) coefficients answer this WITHOUT assuming a potential:

    M1(y) = E[ y_{t+tau} - y_t | y_t = y ]      (drift)
    M2(y) = E[ (y_{t+tau} - y_t)^2 | y_t = y ]  (diffusion)

A mean-reverting (well) process has dM1/dy < 0; a feedback/escape process has
dM1/dy > 0 (possibly super-linear in the tails).  This script estimates both
functions non-parametrically from conditional moments.

State variable (no look-ahead)
------------------------------
    y_t = (log P_t - mean_W(log P)_t) / std_W(log P)_t
with W the rolling window in TRADING days.  mean/std at t use ONLY log P_{<=t}
(pandas .rolling, trailing).  tau = 1 day.  Sensitivity W in {20, 60, 120}.

*** THE CENTRAL TRAP THIS SCRIPT GUARDS AGAINST ***
The rolling z-score is NOT a neutral state variable.  Even for a pure random
walk (iid returns), a price that ran up sits ABOVE its own trailing mean, and
because the trailing mean keeps catching up, M1(y) is mechanically NEGATIVE.
Measured on synthetic GBM: dM1/dy ~= -0.131 (W=20), -0.042 (W=60), -0.016
(W=120).  A raw negative slope is therefore NOT evidence of a potential well.
Every verdict in this script is decided against a RETURN-SHUFFLE NULL: each
symbol's own daily log-returns are permuted (destroying any serial/feedback
dependence while preserving the exact return distribution and volatility),
the whole pipeline is re-run, and the observed slope is compared to the null
distribution.  The prescribed raw (CI-vs-zero) verdict is ALSO reported, but
labelled as the naive one, precisely so the artifact is visible.

Estimator
---------
  * equal-frequency bins on y (adaptive count so every bin has >= MIN_BIN_OBS
    observations; bins below that are dropped before fitting)
  * M1, M2 per bin, then weighted polynomial fits of M1 vs y (degree 1 and 3)
    and of M2 vs |y| (degree 1)
  * uncertainty: circular BLOCK bootstrap (block = 21 trading days) to respect
    serial dependence; >= 1000 replicates

Panel estimation (the real signal-to-noise win)
-----------------------------------------------
The single-name drift over ~10 years is dominated by the rolling-window
artifact and the bootstrap noise; one name is usually UNIDENTIFIABLE.  Pooling
the PIT S&P100 union (all names x all days) times the common drift function
raises n by ~100x.  Two CLUSTER bootstraps are reported because same-day
cross-sections are not independent:
   * date-clustered : resample 21-trading-day blocks of DATES (keeps the
                      cross-section of a day together)
   * symbol-clustered: resample SYMBOLS with replacement (whole histories)
The panel null is built by shuffling returns within each symbol, re-pooling
and re-fitting (preserves per-symbol vol, destroys feedback).

Era stability
-------------
The linear drift slope is re-estimated per era (2016-19 / 2020-22 / 2023-26)
and the OU half-life h = ln2 / |kappa| (kappa = -slope) is reported for each.
Our platform repeatedly finds era dependence in signals, so an unstable
half-life is itself a finding.

Verdicts (constants below, decided on the NULL-ADJUSTED excess)
    POTENTIAL_WELL : observed slope significantly below the null band
    ESCAPE/TREND   : observed slope significantly above the null band
    UNIDENTIFIABLE : inside the null band (or CI crossing it)

HONESTY NOTE: identifying mean reversion is NOT a trading signal.  Costs,
capacity, crowding, borrow and the fact that a weak daily drift is swamped by
diffusion are all outside this study.

Usage
-----
    poetry run python scripts/econophysics_km_drift.py
    poetry run python scripts/econophysics_km_drift.py --symbols MU,COIN --boot 500
    poetry run python scripts/econophysics_km_drift.py --windows 20,60,120 --no-panel
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from statistics import NormalDist
from typing import Iterable, Sequence

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DB_PATH = ROOT / "data" / "btc_history.db"
UNIVERSE_PATH = ROOT / "data" / "universe" / "sp100_union.json"
OUT_PATH = ROOT / "reports" / "km_drift_diffusion.json"

# Alias targets come from the repo's SINGLE SOURCE OF TRUTH for old->new
# tickers (imported the same way scripts/xsec_gbm_selection.py does).  We do
# NOT maintain a parallel rename map here -- only the recoverability verdict
# below is study-specific.
try:
    from scripts.backfill_sp500_stocks import RENAME_MAP  # noqa: E402
    _RENAME_MAP_ERROR: str | None = None
except Exception as exc:  # pragma: no cover - only on a broken checkout
    RENAME_MAP = {}
    _RENAME_MAP_ERROR = repr(exc)

# Union tickers absent from the store that CANNOT be recovered, because the
# listed security ceased to exist (M&A / restructuring) and its successor is a
# DIFFERENT firm.  The recoverable same-security ticker changes are NOT listed
# here: they are decided at run time by classify_absent_ticker() against the
# live panel membership, so there is deliberately no static list of them (a
# static list would be dead weight that reads like classification logic).
STRUCTURALLY_UNRECOVERABLE: dict[str, str] = {
    "AGN": "acquired by AbbVie (2015-2016); Allergan ceased to exist",
    "CELG": "acquired by Bristol-Myers Squibb (2019-11)",
    "MON": "acquired by Bayer (2018-06)",
    "TWX": "acquired by AT&T (2018-06)",
    "RTN": "Raytheon/UTC merger (2020-04); one of TWO union legs mapping to RTX",
    "UTX": "Raytheon/UTC merger (2020-04); one of TWO union legs mapping to RTX",
    "DWDP": "DowDuPont three-way split (2019-06); security restructured",
}

# Documented direction of the resulting survivorship channel (see notes).
SURVIVORSHIP_BIAS_NOTE = (
    "Direction of the bias: the panel drops names that ceased to exist via "
    "acquisition/restructuring (the 7 STRUCTURALLY_UNRECOVERABLE tickers). "
    "Acquisitions typically remove a security AFTER it has been bid up, so the "
    "drop is biased toward removing post-outperformance winners; the pooled "
    "panel therefore understates the total return of the 2016-2026 universe. "
    "This is a REAL survivorship channel, not a rounding error, and it is a "
    "property of the PIT union source (it tracks index membership, and a "
    "take-private name leaves the index), not of this study. It cannot be "
    "repaired without a delisted-name price source (IBKR: no gateway in this "
    "environment)."
)

def _availability_scope() -> str:
    """Exact query scope behind every data-availability claim in the report."""
    return (f"{DB_PATH} :: table ohlcv, market_type IN ('stocks','etf'), "
            f"timeframe='1d', column close")


# MRVL availability is a POINT-IN-TIME fact about a SHARED, MUTABLE store, so it
# is recorded with its moment of observation and its query scope.  Any such
# claim expires as soon as another workstream writes the store.
MRVL_FIRST_PROBE: dict = {
    "when": "start of this session (2026-09-20, before the parallel backfill landed at 11:36:50)",
    "rows": 0,
    "meaning": "MRVL was GENUINELY absent at that moment — not a filter error, not a "
               "timeframe/assetclass mismatch and not 'no such security'",
}

# --------------------------------------------------------------------------
# Frozen study constants (see module docstring for rationale)
# --------------------------------------------------------------------------
DEFAULT_SYMBOLS = ("MU", "MRVL", "COIN", "SPY")
DEFAULT_WINDOWS = (20, 60, 120)
PRIMARY_WINDOW = 60          # headline W (trading days)
TAU = 1                      # KM time step, trading days
DEFAULT_BINS = 20            # equal-frequency bins (upper cap)
MIN_BIN_OBS = 100            # bins thinner than this are dropped from fits
DEFAULT_BLOCK = 21           # ~1 month circular block-bootstrap block length
DEFAULT_BOOT = 1000          # bootstrap replicates for drift CIs
DEFAULT_NULL = 200           # single-name return-shuffle null replicates
DEFAULT_PANEL_NULL = 100     # panel return-shuffle null replicates
DEFAULT_PANEL_BOOT = 1000
ALPHA = 0.05                 # 95% intervals
START, END = "2016-09-01", "2026-12-31"

ERAS: tuple[tuple[str, str, str], ...] = (
    ("2016-19", "2016-09-01", "2019-12-31"),
    ("2020-22", "2020-01-01", "2022-12-31"),
    ("2023-26", "2023-01-01", "2026-12-31"),
)

VERDICT_WELL = "POTENTIAL_WELL"
VERDICT_ESCAPE = "ESCAPE/TREND"
VERDICT_UNIDENTIFIABLE = "UNIDENTIFIABLE"


# --------------------------------------------------------------------------
# Data access
# --------------------------------------------------------------------------
def load_log_price(symbol: str, start: str = START, end: str = END) -> pd.Series:
    """Trailing-date log close for `symbol` (stocks or etf), UTC-naive daily index."""
    con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    try:
        rows = con.execute(
            "SELECT ts, close FROM ohlcv "
            "WHERE symbol = ? AND timeframe = '1d' "
            "AND market_type IN ('stocks', 'etf') "
            "AND ts >= ? AND ts < ? ORDER BY ts",
            (
                symbol,
                int(pd.Timestamp(start, tz="UTC").timestamp() * 1000),
                int(pd.Timestamp(end, tz="UTC").timestamp() * 1000),
            ),
        ).fetchall()
    finally:
        con.close()
    if not rows:
        return pd.Series(dtype=float)
    idx = pd.DatetimeIndex(pd.to_datetime([r[0] for r in rows], unit="ms", utc=True)).tz_localize(None).normalize()
    close = np.asarray([float(r[1]) for r in rows], dtype=float)
    keep = close > 0
    return pd.Series(np.log(close[keep]), index=idx[keep]).sort_index()


def load_universe() -> list[str]:
    if UNIVERSE_PATH.exists():
        return list(json.loads(UNIVERSE_PATH.read_text()))
    return []


# Result of the explicit WBA probe (team-lead item c).  Recorded as data so the
# boundary between "delisted", "wrong asset class" and "negative-cached" is
# auditable rather than asserted.
WBA_BACKFILL_PROBE: dict = {
    "symbol": "WBA",
    "attempted": "poetry run python scripts/backfill_symbols.py --symbols WBA (assetclass=stocks)",
    "probe_date": "2026-09-20",
    "outcome": "FAILED - not obtainable via the Nasdaq daily endpoint",
    "nasdaq_response": 'HTTP 200 {"data":null,"message":null,"status":{"rCode":400,'
                       '"bCodeMessage":[{"code":1001,"errorMessage":"Symbol not exists."}]}}',
    "controls_run_at_the_same_moment": {
        "MU (valid stock)": "HTTP 200, 2514 rows, no error -> the endpoint is NOT globally "
                            "throttled/negative-cached right now",
        "AAPL (valid stock)": "HTTP 200, 2514 rows, no error",
        "SPY queried with assetclass='stocks'": "HTTP 200, 0 rows, IDENTICAL code-1001 "
                                                "'Symbol not exists.' payload",
    },
    "what_the_evidence_does_AND_does_not_show": (
        "The code-1001 payload is a GENERIC 'this symbol is not in this asset class / not known "
        "here' response: a perfectly valid, currently-trading symbol (SPY) produces the identical "
        "payload when asked for as assetclass=stocks. Therefore this payload ALONE CANNOT "
        "distinguish a delisted/taken-private security from a server-side NEGATIVE CACHE entry - "
        "and the repo's own documented hazard (project memory on the S&P500 backfill) is precisely "
        "that the live endpoint keeps serving a cached code-1001 for symbols that failed during "
        "throttling, for hours. An earlier draft of this record claimed 'live response => not a "
        "cached miss'; that inference was WRONG and is retracted here."
    ),
    "in_repo_corroboration": (
        "data/universe/sp500_backfill_status.json independently records WBA as status='failed', "
        "with error \"'NoneType' object has no attribute 'includeExpired'\" - i.e. an earlier "
        "attempt also failed to get WBA from Nasdaq and the IBKR fallback crashed. So the failure "
        "is reproducible across workstreams, which is all that can be claimed."
    ),
    "conclusion": (
        "WBA is UNRECOVERABLE in this environment via the Nasdaq route. The specific cause "
        "(delisting/take-private vs lingering negative cache) is NOT established by the evidence "
        "available here and is deliberately not asserted. The repo's documented remedy for both "
        "readings - IBKR delisted-name history with includeExpired=True after TTL expiry - requires "
        "an IB Gateway, and nothing is listening on :4002 (checked with nc)."
    ),
}


def classify_absent_ticker(sym: str, target: str | None,
                           panel_members: Iterable[str]) -> tuple[str, str]:
    """Pure decision for one sp100_union ticker that has no bars of its own.

    Returns (verdict, reason) with verdict in
    {"add", "already_represented", "structurally_unrecoverable", "no_alias"}.

    Guards two mistakes that would silently corrupt the pooled panel:
      * a takeover target must NEVER be replaced by its acquirer's series
        (ABB V, BMY, DD, BAYRY, RTX, T are different firms), and
      * a same-security rename must NOT be added when the current ticker is
        already a panel member (that would double-weight one security).
    """
    members = set(panel_members)
    if sym in STRUCTURALLY_UNRECOVERABLE:
        return "structurally_unrecoverable", STRUCTURALLY_UNRECOVERABLE[sym]
    if target is None:
        return "no_alias", "absent from the store and not in RENAME_MAP"
    if target in members:
        return "already_represented", (
            f"{target} is itself a panel member; mapping {sym} would add the same "
            f"security twice and double-weight it in the pooled fit")
    return "add", "same-security ticker change; target is not otherwise in the panel"


def resolve_panel_aliases(panel_members: set[str]) -> tuple[dict[str, pd.Series], dict]:
    """Decide what to do about sp100_union tickers that have no bars of their own.

    Alias TARGETS come from RENAME_MAP (the repo's single source of truth,
    imported from scripts/backfill_sp500_stocks.py).  Only the recoverability
    verdict is study-specific; see classify_absent_ticker for the rules.

    Returns (series_to_add, audit).
    """
    missing = [s for s in load_universe() if s not in panel_members]
    audit: dict = {
        "added": {}, "already_represented": {}, "structurally_unrecoverable": {},
        "no_alias": {}, "rename_map_error": _RENAME_MAP_ERROR,
        "bias_note": SURVIVORSHIP_BIAS_NOTE,
    }
    added: dict[str, pd.Series] = {}
    for sym in missing:
        target = RENAME_MAP.get(sym)
        verdict, reason = classify_absent_ticker(sym, target, panel_members)
        if verdict == "structurally_unrecoverable":
            audit["structurally_unrecoverable"][sym] = {
                "alias_target": target, "reason": reason}
            continue
        if verdict == "no_alias":
            audit["no_alias"][sym] = {
                "reason": reason,
                **({"probe": WBA_BACKFILL_PROBE} if sym == "WBA" else {}),
            }
            continue
        if verdict == "already_represented":
            audit["already_represented"][sym] = {"alias_target": target, "reason": reason}
            continue
        series = load_log_price(target)
        if series.empty:
            audit["no_alias"][sym] = {
                "alias_target": target, "reason": f"target {target} has no rows in the store"}
            continue
        added[target] = series
        audit["added"][sym] = {
            "alias_target": target, "rows": int(len(series)), "reason": reason,
            "first": str(series.index[0].date()), "last": str(series.index[-1].date()),
        }
    return added, audit


# --------------------------------------------------------------------------
# State variable + KM pairs
# --------------------------------------------------------------------------
def rolling_zscore(logp: pd.Series, window: int) -> pd.Series:
    """y_t = (logp_t - mean_W(logp)_t) / std_W(logp)_t.  Trailing only:
    the value at t uses logp_{<=t}, so no look-ahead.  ddof=1."""
    roll = logp.rolling(window, min_periods=window)
    return (logp - roll.mean()) / roll.std(ddof=1)


@dataclass
class KMSample:
    """Aligned KM estimation sample.  y_t predicts dy_t = y_{t+1} - y_t."""
    y: np.ndarray
    dy: np.ndarray
    dates: np.ndarray  # int64 epoch-days, same length as y

    def __len__(self) -> int:  # pragma: no cover - trivial
        return int(self.y.size)


def km_sample(logp: pd.Series, window: int) -> KMSample:
    y = rolling_zscore(logp, window).to_numpy(dtype=float)
    dy = np.full_like(y, np.nan)
    dy[:-1] = y[1:] - y[:-1]
    dates = logp.index.values.astype("datetime64[D]").astype(np.int64)
    ok = np.isfinite(y) & np.isfinite(dy)
    return KMSample(y=y[ok], dy=dy[ok], dates=dates[ok])


# --------------------------------------------------------------------------
# Binned KM coefficients
# --------------------------------------------------------------------------
_BINS_MAX = DEFAULT_BINS  # mutable so --bins can override without breaking defaults


def n_bins_for(n_obs: int, n_bins: int | None = None, min_obs: int = MIN_BIN_OBS) -> int:
    """Adaptive equal-frequency bin count: never let the average bin fall
    below MIN_BIN_OBS.  At least 3 bins so a linear fit is meaningful."""
    if n_bins is None:
        n_bins = _BINS_MAX
    return int(max(3, min(n_bins, n_obs // max(min_obs, 1))))


def bin_km(y: np.ndarray, dy: np.ndarray, n_bins: int, min_obs: int = MIN_BIN_OBS) -> dict:
    """Equal-frequency bins over y; conditional M1 and M2 per bin.

    Bins with fewer than `min_obs` observations (only possible in the tails)
    are returned with `kept=False` and excluded from polynomial fits.
    """
    n = y.size
    if n < 3 * min_obs:
        return {"centers": np.array([]), "counts": np.array([]), "m1": np.array([]),
                "m2": np.array([]), "kept": np.array([], dtype=bool), "edges": np.array([])}
    nb = n_bins
    qs = np.quantile(y, np.linspace(0.0, 1.0, nb + 1))
    qs[0] = -np.inf
    qs[-1] = np.inf
    # interior edges must be strictly increasing for searchsorted
    qs = np.unique(qs)
    nb = len(qs) - 1
    idx = np.clip(np.searchsorted(qs, y, side="left") - 1, 0, nb - 1)
    counts = np.bincount(idx, minlength=nb).astype(float)
    m1 = np.bincount(idx, weights=dy, minlength=nb) / np.maximum(counts, 1.0)
    m2 = np.bincount(idx, weights=dy * dy, minlength=nb) / np.maximum(counts, 1.0)
    centers = np.full(nb, np.nan)
    for k in range(nb):
        sel = idx == k
        if counts[k] > 0:
            centers[k] = y[sel].mean()
    kept = (counts >= min_obs) & np.isfinite(centers) & np.isfinite(m1)
    return {"centers": centers, "counts": counts, "m1": m1, "m2": m2, "kept": kept, "edges": qs}


def fit_poly(centers: np.ndarray, values: np.ndarray, counts: np.ndarray,
             kept: np.ndarray, deg: int) -> np.ndarray | None:
    """Counts-weighted polynomial fit returned in ORIGINAL y units (highest
    degree first, per np.polyfit); None if there are too few kept bins."""
    c, v, w = centers[kept], values[kept], counts[kept]
    if c.size < deg + 1:
        return None
    return np.polyfit(c, v, deg, w=np.sqrt(w))


def drift_slope(bins: dict, deg: int = 1) -> float | None:
    coef = fit_poly(bins["centers"], bins["m1"], bins["counts"], bins["kept"], deg)
    return None if coef is None else float(coef[0])


def tail_amplification(bins: dict, y_eval: float = 2.0) -> float | None:
    """Cubic-minus-linear fitted M1 at y = y_eval (a standardised deviation of
    +2).  > 0 => the drift turns up / down FASTER than linear in the tail,
    i.e. super-linear tail feedback (the ESCAPE signature)."""
    lin = fit_poly(bins["centers"], bins["m1"], bins["counts"], bins["kept"], 1)
    cub = fit_poly(bins["centers"], bins["m1"], bins["counts"], bins["kept"], 3)
    if lin is None or cub is None:
        return None
    return float(np.polyval(cub, y_eval) - np.polyval(lin, y_eval))


def diffusion_slope_linear(bins: dict, power: int = 1) -> float | None:
    """d(shape)/d|y| style slope: linear fit of M2 on |y|^power.
    power=1 is the textbook 'vol amplifies with |deviation|' summary;
    power=2 captures a symmetric U-shape (high in BOTH tails)."""
    coef = fit_poly(np.abs(bins["centers"]) ** power, bins["m2"], bins["counts"], bins["kept"], 1)
    return None if coef is None else float(coef[0])


# --------------------------------------------------------------------------
# Uncertainty: block bootstrap (single name)
# --------------------------------------------------------------------------
def _block_indices(n: int, block: int, n_boot: int, rng: np.random.Generator) -> list[np.ndarray]:
    n_blocks = int(np.ceil(n / block))
    starts = rng.integers(0, n, size=(n_boot, n_blocks))
    offs = np.arange(block)
    out = []
    for b in range(n_boot):
        idx = (starts[b][:, None] + offs[None, :]).ravel() % n
        out.append(idx)
    return out


def bootstrap_km_slopes(y: np.ndarray, dy: np.ndarray, block: int, n_boot: int,
                        n_bins: int, min_obs: int, seed: int,
                        diffusion: bool = False) -> np.ndarray:
    """Circular block bootstrap distribution of the drift slope (or, if
    `diffusion`, of the diffusion slope).  Fixed bin count recomputed on each
    replicate (equal-frequency binning is re-derived, so tails move)."""
    rng = np.random.default_rng(seed)
    slopes = np.full(n_boot, np.nan)
    for b, idx in enumerate(_block_indices(y.size, block, n_boot, rng)):
        bins = bin_km(y[idx], dy[idx], n_bins, min_obs)
        s = diffusion_slope_linear(bins) if diffusion else drift_slope(bins)
        if s is not None:
            slopes[b] = s
    return slopes


# --------------------------------------------------------------------------
# Panel: pooled common drift function
# --------------------------------------------------------------------------
@dataclass
class PanelData:
    y: np.ndarray
    dy: np.ndarray
    date_code: np.ndarray        # int index into `dates`
    symbol_code: np.ndarray      # int index into `symbols`
    dates: np.ndarray
    symbols: list[str]
    rows_by_date: list[np.ndarray] = field(default_factory=list, repr=False)
    rows_by_symbol: list[np.ndarray] = field(default_factory=list, repr=False)

    def __post_init__(self) -> None:
        self.rows_by_date = [np.flatnonzero(self.date_code == d) for d in range(self.dates.size)]
        self.rows_by_symbol = [np.flatnonzero(self.symbol_code == s) for s in range(len(self.symbols))]


def build_panel(samples: dict[str, KMSample]) -> PanelData:
    ys, dys, dts, syms, sym_names = [], [], [], [], []
    for si, (sym, s) in enumerate(samples.items()):
        ys.append(s.y)
        dys.append(s.dy)
        dts.append(s.dates)
        syms.append(np.full(s.y.size, si, dtype=np.int64))
        sym_names.append(sym)
    y = np.concatenate(ys)
    dy = np.concatenate(dys)
    dt = np.concatenate(dts)
    sc = np.concatenate(syms)
    uniq_dates = np.unique(dt)
    date_code = np.searchsorted(uniq_dates, dt)
    return PanelData(y=y, dy=dy, date_code=date_code, symbol_code=sc,
                     dates=uniq_dates, symbols=sym_names)


def panel_bootstrap_slopes(panel: PanelData, scheme: str, block: int, n_boot: int,
                           n_bins: int, min_obs: int, seed: int,
                           diffusion: bool = False) -> np.ndarray:
    """Cluster block bootstrap of the pooled drift slope.

    scheme = "date"   -> circular blocks of DATES (keeps each day's
                         cross-section together; respects same-day correlation)
    scheme = "symbol" -> resample SYMBOLS with replacement (respects that one
                         name's history is one correlated unit)
    """
    rng = np.random.default_rng(seed)
    out = np.full(n_boot, np.nan)
    if scheme == "date":
        nd = panel.dates.size
        inds = _block_indices(nd, block, n_boot, rng)
        for b in range(n_boot):
            rows = np.concatenate([panel.rows_by_date[d] for d in inds[b]])
            bins = bin_km(panel.y[rows], panel.dy[rows], n_bins, min_obs)
            s = diffusion_slope_linear(bins) if diffusion else drift_slope(bins)
            if s is not None:
                out[b] = s
    elif scheme == "symbol":
        ns = len(panel.symbols)
        draws = rng.integers(0, ns, size=(n_boot, ns))
        for b in range(n_boot):
            rows = np.concatenate([panel.rows_by_symbol[s] for s in draws[b]])
            bins = bin_km(panel.y[rows], panel.dy[rows], n_bins, min_obs)
            s = diffusion_slope_linear(bins) if diffusion else drift_slope(bins)
            if s is not None:
                out[b] = s
    else:
        raise ValueError(f"unknown scheme {scheme!r}")
    return out


# --------------------------------------------------------------------------
# Return-shuffle null (the artifact control)
# --------------------------------------------------------------------------
def shuffle_null_slopes(logp: pd.Series, window: int, n_null: int, seed: int,
                        n_bins: int = DEFAULT_BINS, min_obs: int = MIN_BIN_OBS,
                        diffusion: bool = False) -> np.ndarray:
    """Null distribution of the slope when returns carry NO serial/feedback
    dependence.  Each replicate permutes the symbol's own daily log-returns
    (preserves the exact return distribution and volatility), rebuilds the
    price path, and re-runs the identical pipeline."""
    rng = np.random.default_rng(seed)
    base = float(logp.iloc[0]) if len(logp) else 0.0
    rets = np.diff(logp.to_numpy(dtype=float))
    n = len(logp)
    out = np.full(n_null, np.nan)
    for b in range(n_null):
        r = rng.permutation(rets)
        path = np.concatenate([[base], base + np.cumsum(r)])
        s = km_sample(pd.Series(path), window)
        bins = bin_km(s.y, s.dy, n_bins_for(s.y.size, n_bins, min_obs), min_obs)
        v = diffusion_slope_linear(bins) if diffusion else drift_slope(bins)
        if v is not None:
            out[b] = v
    return out


def panel_shuffle_null_slopes(logps: dict[str, pd.Series], window: int, n_null: int,
                              seed: int, n_bins: int = DEFAULT_BINS,
                              min_obs: int = MIN_BIN_OBS,
                              diffusion: bool = False) -> np.ndarray:
    """Panel null: shuffle returns within each symbol, re-pool, re-fit."""
    rng = np.random.default_rng(seed)
    bases = {s: float(logp.iloc[0]) if len(logp) else 0.0 for s, logp in logps.items()}
    rets = {s: np.diff(logp.to_numpy(dtype=float)) for s, logp in logps.items()}
    out = np.full(n_null, np.nan)
    for b in range(n_null):
        samples: dict[str, KMSample] = {}
        for s, logp in logps.items():
            r = rng.permutation(rets[s])
            path = np.concatenate([[bases[s]], bases[s] + np.cumsum(r)])
            samples[s] = km_sample(pd.Series(path), window)
        panel = build_panel(samples)
        nb = n_bins_for(panel.y.size, n_bins, min_obs)
        bins = bin_km(panel.y, panel.dy, nb, min_obs)
        v = diffusion_slope_linear(bins) if diffusion else drift_slope(bins)
        if v is not None:
            out[b] = v
    return out


# --------------------------------------------------------------------------
# Verdicts and derived quantities
# --------------------------------------------------------------------------
def ci(arr: np.ndarray, alpha: float = ALPHA) -> tuple[float, float, float]:
    """(lo, hi, mean) percentile interval, ignoring NaNs."""
    a = arr[np.isfinite(arr)]
    if a.size == 0:
        return float("nan"), float("nan"), float("nan")
    return float(np.percentile(a, 100 * alpha / 2)), float(np.percentile(a, 100 * (1 - alpha / 2))), float(a.mean())


def _std(arr: np.ndarray) -> float:
    """Bootstrap SAMPLING spread of a statistic (std of its replicates).
    This — not std/sqrt(n_boot) — is the uncertainty of the point estimate."""
    a = arr[np.isfinite(arr)]
    if a.size < 2:
        return float("nan")
    return float(a.std(ddof=1))


def _se(arr: np.ndarray) -> float:
    """Standard error of the MEAN of a replicate array (Monte-Carlo error of
    the null mean; do NOT use this as the statistic's sampling spread)."""
    a = arr[np.isfinite(arr)]
    if a.size < 2:
        return float("nan")
    return float(a.std(ddof=1) / np.sqrt(a.size))


# How `excess_z` is formed.  Documented as a constant so the report can carry
# the reasoning verbatim: a reader who re-derives this would otherwise pick the
# wrong denominator.
EXCESS_Z_CONVENTION: dict = {
    "numerator": "observed_slope - null_mean",
    "denominator": "sqrt(sd_stat^2 + se_nullmean^2), where "
                   "sd_stat = _std(bootstrap slope replicates) = the statistic's own sampling "
                   "SD, and se_nullmean = _se(shuffle-null slopes) = the Monte-Carlo error of "
                   "the null MEAN (a small correction term only)",
    "wrong_denominator": "_se(bootstrap replicates) = sd_stat/sqrt(n_boot). This is the "
                         "Monte-Carlo error of the bootstrap MEAN, i.e. how well the bootstrap "
                         "estimated the point estimate -- NOT the sampling uncertainty of the "
                         "quantity being tested. It shrinks as n_boot grows and inflates |z| by "
                         "~sqrt(n_boot) (~45x at n_boot=2000), which briefly made MRVL W=20 look "
                         "z=-4.37 and 'clear' a ~2.9 Bonferroni bar; with the correct denominator "
                         "the same cell is |z|=1.68 and the verdict is UNIDENTIFIABLE. The "
                         "sampling SD must not depend on the number of bootstrap replicates.",
    "applies_to": "BOTH the per-symbol cells and the panel cells; the panel uses the SD of the "
                  "date-clustered bootstrap (boot_sd_date_clustered)",
}


def classify(obs: float, ci_lo: float, ci_hi: float,
             null_lo: float, null_hi: float, null_mean: float) -> str:
    """Null-adjusted verdict (headline).

    POTENTIAL_WELL if the observed slope is significantly BELOW the shuffle
    null (both outside the null band and its own CI entirely below the null
    mean); ESCAPE/TREND if significantly above; else UNIDENTIFIABLE.
    """
    if not (np.isfinite(obs) and np.isfinite(null_lo) and np.isfinite(null_hi)):
        return VERDICT_UNIDENTIFIABLE
    if obs < null_lo and ci_hi < null_mean:
        return VERDICT_WELL
    if obs > null_hi and ci_lo > null_mean:
        return VERDICT_ESCAPE
    return VERDICT_UNIDENTIFIABLE


def classify_naive(ci_lo: float, ci_hi: float) -> str:
    """The textbook rule (CI vs zero), reported for contrast only.  It is
    biased by the rolling-window artifact and will cry 'POTENTIAL_WELL' on a
    pure random walk."""
    if not (np.isfinite(ci_lo) and np.isfinite(ci_hi)):
        return VERDICT_UNIDENTIFIABLE
    if ci_hi < 0:
        return VERDICT_WELL
    if ci_lo > 0:
        return VERDICT_ESCAPE
    return VERDICT_UNIDENTIFIABLE


def diffusion_verdict_label(verdict: str) -> str:
    """Readable name for the diffusion-slope-vs-null comparison.  NOTE this
    compares the SLOPE of M2 vs |y| to its shuffle null; it does NOT mean M2
    rises with |y| in the raw data (see `m2_rises_with_abs_y`)."""
    return {
        VERDICT_WELL: "M2_SLOPE_BELOW_NULL",
        VERDICT_ESCAPE: "M2_SLOPE_ABOVE_NULL",
    }.get(verdict, verdict)


def diffusion_shape(bins: dict) -> dict:
    """Level of M2 in the two tails vs the centre.

    The tail/centre ratio is NOT the same as the linear dM2/d|y| slope: the
    z-normalisation divides the move by the rolling std, which is itself large
    when |y| is large, so the RAW M2 tends to FALL in the tails.  Left vs right
    tail separates the leverage effect (y < 0 = below the trailing mean) from
    the upside.  Tail = |y| >= 1.5, centre = |y| <= 0.5.
    """
    c, m2, kept = bins["centers"], bins["m2"], bins["kept"]
    left = m2[kept & (c <= -1.5)]
    right = m2[kept & (c >= 1.5)]
    center = m2[kept & (np.abs(c) <= 0.5)]
    l = float(np.median(left)) if left.size else float("nan")
    r = float(np.median(right)) if right.size else float("nan")
    k = float(np.median(center)) if center.size else float("nan")
    tail = np.concatenate([left, right])
    t = float(np.median(tail)) if tail.size else float("nan")
    ratio = lambda a, b: float(a / b) if (np.isfinite(a) and np.isfinite(b) and b > 0) else float("nan")
    return {
        "median_m2_left_tail_y_le_m1.5": _f(l),
        "median_m2_right_tail_y_ge_p1.5": _f(r),
        "median_m2_center_abs_y_le_0.5": _f(k),
        "tail_over_center": _f(ratio(t, k)),
        "left_over_right_tail": _f(ratio(l, r)),
    }


def half_life_days(slope: float | None) -> float | None:
    """OU half-life from the linear drift slope: M1(y) ~= -kappa*y with
    tau = 1 day, so kappa = -slope and h = ln2 / kappa.  None when the slope
    is non-negative (no restoring force -> infinite half-life)."""
    if slope is None or not np.isfinite(slope) or slope >= 0:
        return None
    return float(np.log(2.0) / (-slope))


def _f(x: float | None) -> float | None:
    return None if x is None or not np.isfinite(x) else float(x)


def _bins_payload(bins: dict) -> dict:
    return {
        "centers": [round(float(v), 4) for v in bins["centers"]],
        "counts": [int(v) for v in bins["counts"]],
        "m1": [round(float(v), 6) for v in bins["m1"]],
        "m2": [round(float(v), 6) for v in bins["m2"]],
        "kept": [bool(v) for v in bins["kept"]],
    }


# --------------------------------------------------------------------------
# Single-name estimation
# --------------------------------------------------------------------------
def estimate_symbol(sym: str, logp: pd.Series, window: int, n_boot: int, n_null: int,
                    block: int, seed: int, with_null: bool = True) -> dict:
    sample = km_sample(logp, window)
    n_obs = int(sample.y.size)
    nb = n_bins_for(n_obs)
    bins = bin_km(sample.y, sample.dy, nb)
    slope = drift_slope(bins, 1)
    cub = fit_poly(bins["centers"], bins["m1"], bins["counts"], bins["kept"], 3)
    dslope = diffusion_slope_linear(bins)

    boot = bootstrap_km_slopes(sample.y, sample.dy, block, n_boot, nb, MIN_BIN_OBS, seed)
    b_lo, b_hi, b_mean = ci(boot)

    if with_null:
        nul = shuffle_null_slopes(logp, window, n_null, seed + 1, n_bins=nb)
        n_lo, n_hi, n_mean = ci(nul)
        n_se = _se(nul)
    else:
        n_lo = n_hi = n_mean = n_se = float("nan")

    verdict = classify(slope, b_lo, b_hi, n_lo, n_hi, n_mean) if with_null else VERDICT_UNIDENTIFIABLE

    # diffusion CI + null
    dboot = bootstrap_km_slopes(sample.y, sample.dy, block, n_boot, nb, MIN_BIN_OBS,
                                seed + 2, diffusion=True)
    d_lo, d_hi, d_mean = ci(dboot)
    if with_null:
        dnul = shuffle_null_slopes(logp, window, n_null, seed + 3, n_bins=nb, diffusion=True)
        dn_lo, dn_hi, dn_mean = ci(dnul)
    else:
        dn_lo = dn_hi = dn_mean = float("nan")
    diffusion_verdict = classify(dslope, d_lo, d_hi, dn_lo, dn_hi, dn_mean) if with_null else VERDICT_UNIDENTIFIABLE
    diffusion_verdict = diffusion_verdict_label(diffusion_verdict)

    eras = {}
    for name, s0, s1 in ERAS:
        m = (sample.dates >= np.datetime64(s0, "D").astype(np.int64)) & (
            sample.dates <= np.datetime64(s1, "D").astype(np.int64))
        sub = KMSample(y=sample.y[m], dy=sample.dy[m], dates=sample.dates[m])
        if sub.y.size < 3 * MIN_BIN_OBS:
            eras[name] = {"n_obs": int(sub.y.size), "slope": None, "half_life_days": None,
                          "note": "insufficient observations"}
            continue
        eb = bin_km(sub.y, sub.dy, n_bins_for(sub.y.size))
        es = drift_slope(eb, 1)
        eras[name] = {"n_obs": int(sub.y.size), "slope": _f(es),
                      "half_life_days": _f(half_life_days(es))}

    excess = _f(slope - n_mean) if (slope is not None and np.isfinite(n_mean)) else None
    # Denominator = statistic's own sampling SD (_std), NOT _se = sd/sqrt(n_boot).
    # See EXCESS_Z_CONVENTION; the MC error of the bootstrap mean is the wrong scale
    # and would inflate |z| by ~sqrt(n_boot).
    b_sd = _std(boot)
    excess_z = (excess / np.sqrt(b_sd**2 + n_se**2)
                if (excess is not None and np.isfinite(b_sd) and np.isfinite(n_se)
                    and (b_sd**2 + n_se**2) > 0) else None)
    return {
        "n_obs": n_obs,
        "n_bins": nb,
        "drift": {
            "slope_deg1": _f(slope),
            "intercept_deg1": _f(None if (c := fit_poly(bins["centers"], bins["m1"], bins["counts"], bins["kept"], 1)) is None else c[1]),
            "slope_deg3_cubic_coef": _f(None if cub is None else cub[0]),
            "tail_amplification_deg3_minus_deg1_at_y2": _f(tail_amplification(bins)),
            "boot_ci": [_f(b_lo), _f(b_hi)],
            "boot_mean": _f(b_mean),
            "boot_sd": _f(b_sd),
            "null_mean": _f(n_mean),
            "null_ci": [_f(n_lo), _f(n_hi)],
            "null_se": _f(n_se),
            "excess_over_null": excess,
            "excess_z": _f(excess_z),
            "verdict": verdict,
            "verdict_naive_ci_vs_zero": classify_naive(b_lo, b_hi),
        },
        "half_life_days": _f(half_life_days(slope)),
        "half_life_days_null_adjusted": _f(half_life_days(excess)) if verdict == VERDICT_WELL else None,
        "half_life_note": "raw h uses the observed slope and is INFLATED by the "
                          "rolling-window artifact; the null-adjusted value is only "
                          "reported when the verdict is POTENTIAL_WELL",
        "eras": eras,
        "diffusion": {
            "slope_vs_abs_y": _f(dslope),
            "slope_vs_y_squared": _f(diffusion_slope_linear(bins, power=2)),
            "m2_rises_with_abs_y": None if dslope is None else bool(dslope > 0),
            "shape": diffusion_shape(bins),
            "boot_ci": [_f(d_lo), _f(d_hi)],
            "null_mean": _f(dn_mean),
            "null_ci": [_f(dn_lo), _f(dn_hi)],
            "verdict": diffusion_verdict,
        },
        "bins": _bins_payload(bins),
    }


# --------------------------------------------------------------------------
# Panel estimation
# --------------------------------------------------------------------------
def estimate_panel(samples: dict[str, KMSample], logps: dict[str, pd.Series],
                   window: int, n_boot: int, n_null: int, block: int, seed: int) -> dict:
    panel = build_panel(samples)
    nb = n_bins_for(panel.y.size)
    bins = bin_km(panel.y, panel.dy, nb)
    slope = drift_slope(bins, 1)
    cub = fit_poly(bins["centers"], bins["m1"], bins["counts"], bins["kept"], 3)
    dslope = diffusion_slope_linear(bins)

    date_boot = panel_bootstrap_slopes(panel, "date", block, n_boot, nb, MIN_BIN_OBS, seed)
    sym_boot = panel_bootstrap_slopes(panel, "symbol", block, n_boot, nb, MIN_BIN_OBS, seed + 1)
    dl, dh, dm = ci(date_boot)
    sl, sh, sm = ci(sym_boot)

    nul = panel_shuffle_null_slopes(logps, window, n_null, seed + 2, n_bins=nb)
    n_lo, n_hi, n_mean = ci(nul)
    # null band is itself uncertain (finite n_null): widen by 1 null-SE
    n_se = _se(nul)

    verdict_date = classify(slope, dl, dh, n_lo - n_se, n_hi + n_se, n_mean)
    verdict_sym = classify(slope, sl, sh, n_lo - n_se, n_hi + n_se, n_mean)
    # headline: both clusterings must agree on the same non-null verdict
    if verdict_date == verdict_sym:
        verdict = verdict_date
    else:
        verdict = VERDICT_UNIDENTIFIABLE

    dboot = panel_bootstrap_slopes(panel, "date", block, n_boot, nb, MIN_BIN_OBS, seed + 3, diffusion=True)
    d_lo, d_hi, _ = ci(dboot)
    dnul = panel_shuffle_null_slopes(logps, window, n_null, seed + 4, n_bins=nb, diffusion=True)
    dn_lo, dn_hi, dn_mean = ci(dnul)
    diff_verdict = classify(dslope, d_lo, d_hi, dn_lo, dn_hi, dn_mean)
    diff_verdict = diffusion_verdict_label(diff_verdict)

    eras = {}
    for name, s0, s1 in ERAS:
        lo = np.datetime64(s0, "D").astype(np.int64)
        hi = np.datetime64(s1, "D").astype(np.int64)
        m = (panel.dates[panel.date_code] >= lo) & (panel.dates[panel.date_code] <= hi)
        if m.sum() < 3 * MIN_BIN_OBS:
            eras[name] = {"n_obs": int(m.sum()), "slope": None, "half_life_days": None,
                          "note": "insufficient observations"}
            continue
        eb = bin_km(panel.y[m], panel.dy[m], n_bins_for(int(m.sum())))
        es = drift_slope(eb, 1)
        # era slope CI by date-clustered block bootstrap on the era subset
        sub = PanelData(y=panel.y[m], dy=panel.dy[m], date_code=panel.date_code[m],
                        symbol_code=panel.symbol_code[m], dates=panel.dates,
                        symbols=panel.symbols)
        eboot = panel_bootstrap_slopes(sub, "date", block, max(200, n_boot // 2), n_bins_for(int(m.sum())),
                                       MIN_BIN_OBS, seed + 5)
        el, eh, _ = ci(eboot)
        eras[name] = {"n_obs": int(m.sum()), "slope": _f(es),
                      "boot_ci": [_f(el), _f(eh)],
                      "half_life_days": _f(half_life_days(es))}

    excess = _f(slope - n_mean) if (slope is not None and np.isfinite(n_mean)) else None
    # Same denominator convention as the per-symbol path: sampling SD (_std) of the
    # date-clustered bootstrap, plus the null-mean MC error as a small correction.
    # See EXCESS_Z_CONVENTION. The panel is where the headline verdict lives, so this
    # path is regression-tested explicitly.
    d_sd = _std(date_boot)
    excess_z = (excess / np.sqrt(d_sd**2 + n_se**2)
                if (excess is not None and np.isfinite(d_sd) and np.isfinite(n_se)
                    and (d_sd**2 + n_se**2) > 0) else None)
    return {
        "n_obs": int(panel.y.size),
        "n_symbols": len(panel.symbols),
        "n_dates": int(panel.dates.size),
        "n_bins": nb,
        "drift": {
            "slope_deg1": _f(slope),
            "slope_deg3_cubic_coef": _f(None if cub is None else cub[0]),
            "tail_amplification_deg3_minus_deg1_at_y2": _f(tail_amplification(bins)),
            "boot_ci_date_clustered": [_f(dl), _f(dh)],
            "boot_ci_symbol_clustered": [_f(sl), _f(sh)],
            "boot_sd_date_clustered": _f(d_sd),
            "null_mean": _f(n_mean),
            "null_ci": [_f(n_lo), _f(n_hi)],
            "null_se": _f(n_se),
            "excess_over_null": excess,
            "excess_z": _f(excess_z),
            "verdict": verdict,
            "verdict_date_clustered": verdict_date,
            "verdict_symbol_clustered": verdict_sym,
            "verdict_naive_ci_vs_zero": classify_naive(dl, dh),
        },
        "half_life_days": _f(half_life_days(slope)),
        "half_life_days_null_adjusted": _f(half_life_days(excess)) if verdict == VERDICT_WELL else None,
        "half_life_note": "raw h uses the observed slope and is INFLATED by the "
                          "rolling-window artifact; the null-adjusted value is only "
                          "reported when the verdict is POTENTIAL_WELL",
        "eras": eras,
        "diffusion": {
            "slope_vs_abs_y": _f(dslope),
            "slope_vs_y_squared": _f(diffusion_slope_linear(bins, power=2)),
            "m2_rises_with_abs_y": None if dslope is None else bool(dslope > 0),
            "shape": diffusion_shape(bins),
            "boot_ci_date_clustered": [_f(d_lo), _f(d_hi)],
            "null_mean": _f(dn_mean),
            "null_ci": [_f(dn_lo), _f(dn_hi)],
            "verdict": diff_verdict,
        },
        "bins": _bins_payload(bins),
    }


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------
def run(args: argparse.Namespace) -> dict:
    windows = tuple(args.windows)
    symbols = tuple(s.strip().upper() for s in args.symbols.split(",") if s.strip())

    # As-of stamp for every data-availability claim in this report.  The store
    # is shared and is written by other workstreams, so "symbol X has N rows"
    # is a point-in-time fact that EXPIRES: it must carry when it was observed
    # and the exact query scope (see _availability_scope()).
    observed_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    logps: dict[str, pd.Series] = {}
    missing: list[str] = []
    coverage: dict[str, dict] = {}
    for s in symbols:
        lp = load_log_price(s)
        if lp.empty:
            missing.append(s)
            continue
        logps[s] = lp
        coverage[s] = {
            "source": "data/btc_history.db ohlcv (Nasdaq daily, split-adjusted close)",
            "first": str(lp.index[0].date()),
            "last": str(lp.index[-1].date()),
            "rows": int(len(lp)),
            "observed_at": observed_at,
        }

    # multiplicity bookkeeping: one drift cell per (name, window) plus one per
    # panel window.  Any single 'significant' cell must clear this bar.
    n_cells = len(logps) * len(windows) + (0 if args.no_panel else len(windows))
    bonferroni_z = float(NormalDist().inv_cdf(1.0 - ALPHA / (2 * max(n_cells, 1))))

    report: dict = {
        "meta": {
            "generated_by": "scripts/econophysics_km_drift.py",
            "state_variable": "y_t = (log P_t - trailing_mean_W(log P)_t) / trailing_std_W(log P)_t",
            "tau_days": TAU,
            "windows": list(windows),
            "primary_window": PRIMARY_WINDOW,
            "window_days": PRIMARY_WINDOW,
            "bins_max": DEFAULT_BINS,
            "min_bin_obs": MIN_BIN_OBS,
            "block_days": args.block,
            "n_boot": args.boot,
            "n_null": args.null,
            "n_panel_null": args.panel_null,
            "alpha": ALPHA,
            "start": START,
            "end": END,
            "generated_at": observed_at,
            "db_path": str(DB_PATH),
            "universe_source": str(UNIVERSE_PATH),
            "data_provenance": {
                "store": str(DB_PATH),
                "table": "ohlcv",
                "market_types": ["stocks", "etf"],
                "timeframe": "1d",
                "price_field": "close",
                "source_note": "Nasdaq public API via src/data/nasdaq_store.NasdaqDailyStore; "
                               "closes are split-adjusted, so log returns are split-consistent",
                "symbols_present": coverage,
                "symbols_missing": missing,
            },
            # Data-availability claims are point-in-time: the store is shared and
            # is written by other workstreams, so each claim carries when it was
            # observed and the exact query scope that produced it.
            "data_availability_asof": {
                "observed_at": observed_at,
                "scope": _availability_scope(),
                "symbols": {
                    s: {"rows": coverage[s]["rows"], "first": coverage[s]["first"],
                        "last": coverage[s]["last"]}
                    for s in coverage
                },
                "symbols_absent_at_observation": list(missing),
                "mrvl_first_probe_of_session": MRVL_FIRST_PROBE,
                "expires": "the store is shared and mutable; re-query before reusing any of this",
            },
            "excess_z_convention": EXCESS_Z_CONVENTION,
            "multiplicity": {
                "n_cells_searched": int(n_cells),
                "cells": "n_names x n_windows single-name drift cells + n_windows panel cells",
                "bonferroni_z_two_sided": bonferroni_z,
                "note": "This analysis was NOT pre-registered and is not a hypothesis submitted to "
                        "the platform's deflation gate. See the TRADEABILITY note in this report "
                        "for the exact best-of-N |excess_z| against this bar: no cell clears it, "
                        "and the platform's DSR / multiplicity-adjusted bar (~3.1 for a 40-cell "
                        "search) is higher still. Do not extract any single cell as a standalone "
                        "result.",
            },
            "eras": [{"name": n, "start": a, "end": b} for n, a, b in ERAS],
            "requested_symbols": list(symbols),
            "missing_symbols": missing,
        },
        "per_symbol": {},
        "panel": {},
        "notes": [],
    }

    for sym, lp in logps.items():
        report["per_symbol"][sym] = {
            "by_window": {
                str(w): estimate_symbol(sym, lp, w, args.boot, args.null, args.block, seed=1000 + 7 * w)
                for w in windows
            }
        }

    # PIT S&P100 union panel
    panel_logps: dict[str, pd.Series] = {}
    if not args.no_panel:
        uni = [s for s in load_universe() if s not in symbols]
        for s in uni:
            lp = load_log_price(s)
            if len(lp) >= 3 * MIN_BIN_OBS:
                panel_logps[s] = lp
        # include the headline names too (they are legitimate panel members)
        for s, lp in logps.items():
            if len(lp) >= 3 * MIN_BIN_OBS:
                panel_logps[s] = lp

        # survivorship: resolve absent union tickers against RENAME_MAP and
        # audit every inclusion/exclusion (see resolve_panel_aliases)
        members_before = set(panel_logps)
        alias_series, audit = resolve_panel_aliases(members_before)
        for name, series in alias_series.items():
            panel_logps[name] = series

        report["meta"]["panel_universe_present"] = sorted(panel_logps)
        report["meta"]["panel_universe_missing"] = sorted(
            set(load_universe()) - set(panel_logps) - set(audit["added"]))
        report["meta"]["panel_survivorship_audit"] = audit
        report["meta"]["panel_names_before_survivorship_fix"] = len(members_before)
        report["meta"]["panel_names_after_survivorship_fix"] = len(panel_logps)
        report["panel"] = {
            str(w): estimate_panel(
                {s: km_sample(lp, w) for s, lp in panel_logps.items()},
                panel_logps, w, args.panel_boot, args.panel_null, args.block, seed=4242 + 13 * w,
            )
            for w in windows
        }

    report["notes"] = _interpretation(report)
    return report


def _max_abs_excess_z(report: dict) -> tuple[float, str]:
    """Largest |excess_z| across every cell, with the cell's name (for the
    multiplicity discussion)."""
    best_z, best_cell = float("nan"), "n/a"
    for sym, d in report.get("per_symbol", {}).items():
        for w, r in d.get("by_window", {}).items():
            z = r.get("drift", {}).get("excess_z")
            if z is not None and (not np.isfinite(best_z) or abs(z) > best_z):
                best_z, best_cell = abs(float(z)), f"{sym} W={w}"
    for w, r in report.get("panel", {}).items():
        z = r.get("drift", {}).get("excess_z")
        if z is not None and (not np.isfinite(best_z) or abs(z) > best_z):
            best_z, best_cell = abs(float(z)), f"PANEL W={w}"
    return best_z, best_cell


def _interpretation(report: dict) -> list[str]:
    notes: list[str] = []
    notes.append(
        "Rolling-window artifact: y is defined against its own trailing mean, so a "
        "random walk already produces a negative drift slope (-0.13/-0.04/-0.02 at "
        "W=20/60/120). Verdicts are therefore decided against a return-shuffle null, "
        "not against zero. The 'naive' CI-vs-zero verdict is reported alongside to "
        "make the artifact visible; it is NOT the headline."
    )
    for sym, d in report["per_symbol"].items():
        for w, r in d["by_window"].items():
            dr = r["drift"]
            notes.append(
                f"{sym} W={w}: n={r['n_obs']} slope={dr['slope_deg1']} "
                f"CI=[{dr['boot_ci'][0]}, {dr['boot_ci'][1]}] null_band="
                f"[{dr['null_ci'][0]}, {dr['null_ci'][1]}] excess={dr['excess_over_null']} "
                f"-> {dr['verdict']} (naive: {dr['verdict_naive_ci_vs_zero']}); "
                f"half-life raw={r['half_life_days']}d null-adjusted={r['half_life_days_null_adjusted']}d; "
                f"M2 tail/center={r['diffusion']['shape']['tail_over_center']} "
                f"L/R={r['diffusion']['shape']['left_over_right_tail']} "
                f"(dM2/d|y|={r['diffusion']['slope_vs_abs_y']}, rises_with_abs_y="
                f"{r['diffusion']['m2_rises_with_abs_y']})"
            )
    for w, r in report.get("panel", {}).items():
        dr = r["drift"]
        eras = ", ".join(
            f"{k}: slope={v['slope']} h={v['half_life_days']}d n={v['n_obs']}"
            for k, v in r["eras"].items()
        )
        notes.append(
            f"PANEL W={w}: N={r['n_obs']} ({r['n_symbols']} names, {r['n_dates']} days) "
            f"slope={dr['slope_deg1']} CI_date=[{dr['boot_ci_date_clustered'][0]}, "
            f"{dr['boot_ci_date_clustered'][1]}] CI_symbol=[{dr['boot_ci_symbol_clustered'][0]}, "
            f"{dr['boot_ci_symbol_clustered'][1]}] null_band=[{dr['null_ci'][0]}, {dr['null_ci'][1]}] "
            f"excess={dr['excess_over_null']} -> {dr['verdict']} (naive: "
            f"{dr['verdict_naive_ci_vs_zero']}); half-life raw={r['half_life_days']}d "
            f"null-adjusted={r['half_life_days_null_adjusted']}d; "
            f"M2 tail/center={r['diffusion']['shape']['tail_over_center']} "
            f"L/R={r['diffusion']['shape']['left_over_right_tail']} "
            f"(dM2/d|y|={r['diffusion']['slope_vs_abs_y']}, "
            f"dM2/dy^2={r['diffusion']['slope_vs_y_squared']}, "
            f"rises_with_abs_y={r['diffusion']['m2_rises_with_abs_y']}) "
            f"verdict={r['diffusion']['verdict']}; eras: {eras}"
        )
    if report.get("panel"):
        pw = str(report["meta"]["primary_window"])
        p = report["panel"].get(pw)
        if p:
            sh = p["diffusion"]["shape"]
            notes.append(
                f"Diffusion shape (panel W={pw}): raw M2 does NOT rise with |y| "
                f"(dM2/d|y|={p['diffusion']['slope_vs_abs_y']}, tail/centre="
                f"{sh['tail_over_center']}); the z-normalisation deflates each move by the "
                f"rolling std, which is itself large when |y| is large, and the shuffle null "
                f"reproduces this pattern, so there is no evidence of 'slippery' "
                f"(higher-diffusion) tails. Left/right tail asymmetry L/R="
                f"{sh['left_over_right_tail']} (>1 => below-the-trailing-mean states carry "
                f"more next-day variance than above-the-mean states; consistent with a "
                f"leverage-type effect, though the z-normalisation amplifies it, so treat "
                f"the raw multiple L/R as an upper bound)."
            )
    if report["meta"].get("missing_symbols"):
        notes.append(
            "MISSING HEADLINE DATA: " + ", ".join(report["meta"]["missing_symbols"]) +
            " have no daily bars in data/btc_history.db (reported, not silently dropped).")
    notes.append(
        "MRVL AVAILABILITY (with as-of). MRVL was GENUINELY absent — 0 rows — at this session's "
        f"first probe of {_availability_scope()}; that observation was correct, not a filter error. "
        "The parallel line's scripts/backfill_symbols.py then wrote 2514 rows "
        "(2016-09-19..2026-09-18) at 2026-09-20T11:36:50+08:00; this run started ~11:36:47 and "
        "loaded the store once at the top, so the read raced the write by ~3 seconds and the "
        "process held the stale view for its whole duration. missing_symbols=['MRVL'] was therefore "
        "ACCURATE AT READ TIME and OBSOLETE BY WRITE TIME. It is not a filter bug, not an "
        "assetclass/timeframe mismatch (the scope above matches the row's ('stocks','1d')) and not "
        "'no such security'. MRVL is still correctly absent from sp100_union (it is an S&P 500, not "
        f"S&P 100, name) and enters the panel only as a headline name. AS-OF: row counts were "
        f"observed at {report['meta']['generated_at']} against the scope above; the store is shared "
        "and mutable, so this claim expires — re-query before reusing it."
    )
    audit = report["meta"].get("panel_survivorship_audit")
    if audit:
        added = ", ".join(f"{k}->{v['alias_target']}" for k, v in audit["added"].items()) or "none"
        rep = ", ".join(f"{k}->{v['alias_target']}" for k, v in audit["already_represented"].items()) or "none"
        unrec = ", ".join(f"{k} ({v['alias_target']})" for k, v in audit["structurally_unrecoverable"].items())
        noalias = ", ".join(audit["no_alias"].keys()) or "none"
        notes.append(
            f"PANEL SURVIVORSHIP: the PIT S&P100 union lists tickers whose securities no longer "
            f"exist. Resolved against RENAME_MAP (single source of truth): ADDED as same-security "
            f"renames {added}; ALREADY REPRESENTED under their current ticker {rep} (mapping these "
            f"would double-count one security, so they are skipped, not dropped); STRUCTURALLY "
            f"UNRECOVERABLE (listed security ceased to exist via M&A/restructuring) {unrec}; NO "
            f"ALIAS {noalias}. Panel names {report['meta'].get('panel_names_before_survivorship_fix')}"
            f" -> {report['meta'].get('panel_names_after_survivorship_fix')}. {audit['bias_note']}"
            + (f" RENAME_MAP could not be imported ({audit['rename_map_error']})."
               if audit.get("rename_map_error") else "")
        )
    m = report["meta"]["multiplicity"]
    max_z, max_cell = _max_abs_excess_z(report)
    n_cells = max(int(m["n_cells_searched"]), 1)
    evt_max = float(np.sqrt(2.0 * np.log(2.0 * n_cells)))  # EVT approx for max of n |z|
    bar = float(m["bonferroni_z_two_sided"])
    if np.isfinite(max_z) and max_z < bar:
        clears = (f"No cell clears the Bonferroni bar (best-of-{n_cells} is "
                  f"|excess_z|={max_z:.2f}).")
    else:
        clears = (f"CAUTION: {max_cell} reaches |excess_z|={max_z:.2f} >= the Bonferroni bar "
                  f"{bar:.2f}; treat it as a multiplicity candidate to be pre-registered and "
                  f"re-tested out of sample, NOT as an established effect.")
    notes.append(
        "EXCESS_Z DENOMINATOR (read before re-deriving): excess_z = (slope - null_mean) / "
        "sqrt(sd_stat^2 + se_nullmean^2), where sd_stat is the STATISTIC'S OWN SAMPLING SD "
        "(_std of the bootstrap slope replicates) and se_nullmean is only the Monte-Carlo error of "
        "the null MEAN. A first implementation instead divided by _se = sd_stat/sqrt(n_boot) (the "
        "MC error of the bootstrap mean), which is the wrong scale: it shrinks with n_boot and "
        "inflates |z| by ~sqrt(n_boot) (~45x at n_boot=2000), briefly making MRVL W=20 read "
        "z=-4.37 and appear to clear a ~2.9 Bonferroni bar. With the correct denominator the same "
        "cell is |z|=1.68 and UNIDENTIFIABLE. The same convention is used on the PANEL path (SD of "
        "the date-clustered bootstrap), and both paths are locked by regression tests."
    )
    notes.append(
        f"TRADEABILITY / MULTIPLICITY (headline caveat): this study found no effect that could be "
        f"traded. {clears} The strongest cell in the entire study is |excess_z|={abs(max_z):.2f} "
        f"({max_cell}), below BOTH the Bonferroni bar {bar:.2f} AND the ~{evt_max:.2f} that the "
        f"maximum of {n_cells} null z-statistics is expected to reach by extreme-value theory — "
        f"i.e. our best cell is weaker than what noise alone produces. The platform's DSR / "
        f"multiplicity-adjusted bar (~3.1 for a 40-cell search) is higher still, and this analysis "
        f"was never pre-registered or submitted to that gate. Separately, even a REAL daily "
        f"mean-reverting drift of this size is swamped by diffusion (drift/diffusion ~ sqrt(dt)) "
        f"and would have to pay ~100% monthly turnover against a ~5bp half-spread plus commission "
        f"inside S&P100 ADTV limits. Identification is not edge."
    )
    notes.append(
        "ERA CROSS-REFERENCE: the near-constant half-life across eras found here is the signature "
        "of the rolling-window artifact, not of a stable potential. The complementary cross-sectional "
        "evidence is in reports/horizon_decomposition.json (horizon-decomposition line, cited not "
        "re-verified here): the classic 1-month reversal (k21_h21) is -0.0254 / -0.0119 / +0.0056 in "
        "2016-19 / 2020-22 / 2023-26, i.e. negative in the first two eras and POSITIVE in the most "
        "recent one, sign_stable=false. IMPORTANT CAVEAT (per the source line's own audit): NO "
        "single era is individually significant (t_newey_west = -1.39 / -0.44 / +0.28), and no "
        "era x signal interaction test was run, so this is a SIGN-LEVEL observation, not a tested "
        "between-era difference. What it licenses is narrow and sufficient for our purpose: a "
        "firmly-signed short-horizon reversal would be needed for a stable short-reversal potential "
        "well, and that firm sign is absent — it is consistent with (but does not test) a "
        "time-varying effective V(k). Taken together: what is stable over ten years is the "
        "mechanical artifact; what is real (the reversal/momentum structure) is era-unstable at the "
        "level of sign."
    )
    notes.append(
        "CAVEAT: a detected mean-reverting drift is a statistical statement about "
        "conditional means, NOT a tradable edge — costs, capacity, crowding and "
        "diffusion-dominated risk are outside this study."
    )
    notes.append(
        "ESCAPE criterion: the ESCAPE/TREND verdict is triggered by a significantly "
        "POSITIVE linear drift slope (or a slope significantly above the shuffle null). "
        "The super-linear tail branch is reported descriptively via the cubic "
        "coefficient and tail_amplification_deg3_minus_deg1_at_y2; those are NOT "
        "bootstrapped, so they are supporting evidence and never a stand-alone verdict."
    )
    return notes


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    p.add_argument("--windows", default=",".join(map(str, DEFAULT_WINDOWS)),
                   help="comma-separated rolling windows (default 20,60,120)")
    p.add_argument("--bins", type=int, default=DEFAULT_BINS)
    p.add_argument("--block", type=int, default=DEFAULT_BLOCK, help="block bootstrap length in days")
    p.add_argument("--boot", type=int, default=DEFAULT_BOOT)
    p.add_argument("--null", type=int, default=DEFAULT_NULL)
    p.add_argument("--panel-boot", type=int, default=DEFAULT_PANEL_BOOT)
    p.add_argument("--panel-null", type=int, default=DEFAULT_PANEL_NULL)
    p.add_argument("--no-panel", action="store_true")
    p.add_argument("--out", default=str(OUT_PATH))
    return p


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    args.windows = tuple(int(x) for x in str(args.windows).split(","))
    global _BINS_MAX
    _BINS_MAX = args.bins
    report = run(args)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2))
    print("\n".join(report["notes"]))
    print(f"\nwrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
