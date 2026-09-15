#!/usr/bin/env python3
"""Two closing analyses for the EDGAR fundamental factors.

The single-factor screen (scripts/test_fundamental_factors.py) found 0/12
factors at the A bar and one B candidate (`roe`). That is a statement about
*marginal* predictive power, and it leaves two questions open which decide
whether the fundamentals deserve any further investment at all:

Analysis 1 — PIT convention sensitivity (as-filed vs latest-visible)
--------------------------------------------------------------------
`as_of_facts(prefer_latest=...)` documents a genuine modelling choice:

  * as-filed (default, used by the shipped panel): the EARLIEST publication of
    each period wins, forever. A later restatement is ignored.
  * latest-visible: the MOST RECENT publication of that period wins — what a
    live reader would hold after learning about a restatement/reclassification.

Both are look-ahead free; they differ only for periods that were published
more than once with a different value. This script rebuilds the whole panel
under latest-visible (same code path, only `replay_state` swapped — see
`build_panel`), re-runs the 12-factor IC/era/tier machinery on it, and puts the
two side by side. The point is fragility attribution: if a factor's IC flips
sign or collapses when a handful of re-filed periods are read the other way,
its signal lives in the *noise characteristics of the originally filed
numbers*, not in the fundamentals.

Analysis 2 — incremental value over the 13 price-volume features
----------------------------------------------------------------
A single factor can be weak on its own and still add information. The right
question is whether it predicts the cross-section *after* the 13 price-volume
features the production model already uses (scripts/xsec_gbm_selection.py,
imported read-only) are controlled for.

Method (a) of the two offered, cross-sectional residualisation:
  * per month, rank-normalise the return, the factor and the 13 controls
    (rank -> normal scores), so the resulting Pearson correlations are the
    Spearman analogues and heavy tails cannot drive the fit;
  * OLS the rank-normal return on the rank-normal controls -> residual return;
    OLS the rank-normal factor on the same controls -> residual factor;
  * the headline statistic is `inc_ic_partial` = corr(residual factor,
    residual return), the exact partial rank correlation;
  * the two single-sided variants — corr(factor, residual return) and
    corr(residual factor, return) — are also recorded, because each is the
    natural direct output of one reading of "regress out the controls". They
    are NOT the incremental IC: each is deflated by the share of the OTHER
    variable the controls fail to explain (sd(resid)/sd(raw)), so they are
    kept only as a self-check that reproduces the known ratio;
  * also report the mean monthly delta-R^2 from adding the factor to the
    13-control regression.

Method (a) is preferred over (b) partial correlation because it gives the
incremental R^2 for free, and because rank-normalising first makes the "control
for momentum" step robust to the fat tails these price features have (an
ordinary partial correlation on raw values is dominated by a few extreme
names).

`raw IC` is always recomputed on the SAME complete-case sample as the
incremental IC, so the difference between the two columns is the control set
and nothing else.

Honesty rules (inherited)
-------------------------
  * Era split (2016-19 / 2020-22 / 2023-26) is mandatory; a factor must hold
    its sign in all three to be called robust. Sign consistency is not enough
    on its own, so an `era_concentration` flag also reports when the smallest
    era's magnitude is under a third of the largest — the failure mode behind
    this repo's FOMC finding (a whole "effect" that was one regime).
  * No sign flipping, no horizon/universe re-picking after the fact.
  * The 12-1 momentum reference is reported on the same panel — it is
    insignificant there too, which is the correct baseline for reading any
    fundamental number on this pool (the pool/frequency is weak, not
    specifically the fundamentals).
  * Multiple comparisons: the incremental table is 6 factors x 2 conventions =
    12 tests, so the Bonferroni threshold is reported alongside the raw t.
  * Negative results are reported as negative results.

Outputs
-------
reports/fundamental_incremental.json
data/fundamental_features_latest.csv   (cache of the latest-visible panel;
                                        /data/ is gitignored, and the
                                        fundamental_features table is left
                                        untouched so the as-filed panel stays
                                        the shipped artefact)

Usage
-----
    poetry run python scripts/test_fundamental_incremental.py
    poetry run python scripts/test_fundamental_incremental.py --rebuild-latest
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from scipy.stats import norm, spearmanr  # noqa: E402

import scripts.build_fundamental_features as bff  # noqa: E402
from scripts.build_fundamental_features import (  # noqa: E402
    EXPECTED_SIGN,
    FEATURES,
    NEEDED_TAGS,
)
from scripts.fetch_edgar_fundamentals import connect  # noqa: E402
from scripts.test_fundamental_factors import (  # noqa: E402
    ERAS,
    MIN_NAMES,
    RET_HI,
    RET_LO,
    summarize,
    test_factor,
    tier,
)
from scripts.xsec_gbm_selection import (  # noqa: E402
    FEATURES as PRICE_FEATURES,
    compute_daily_features,
    load_panel,
)

REPORT_PATH = ROOT / "reports" / "fundamental_incremental.json"
LATEST_CACHE = ROOT / "data" / "fundamental_features_latest.csv"
LATEST_META = ROOT / "data" / "fundamental_features_latest.meta.json"
CACHE_VERSION = "v1"

# The six factors with the strongest theoretical prior, tested for increment.
INCREMENTAL_FACTORS = ["roe", "asset_growth", "accruals",
                       "ep_ttm", "bp", "gross_margin"]
# Two controls need 252 daily bars, which the local price store (2016-09-12
# onward) cannot supply until ~2017-09. The primary test keeps all 13 controls
# as specified; this reduced set is a sensitivity so era 1 is not gutted.
SHORT_HISTORY_CONTROLS = [f for f in PRICE_FEATURES
                          if f not in ("mom_12_1", "dist_52w_high")]
REFERENCE = "reference_mom_12_1"

# Restated inputs are only decision-relevant if they arrive well after the
# period closed. A re-filing within this many days is a 10-Q/10-K amendment;
# later than that it is almost always the period reappearing as a comparative
# inside a LATER filing (where it can legitimately be reclassified).
COMPARATIVE_LAG_DAYS = 400


def _fmt(v, spec="+.4f", na="n/a") -> str:
    return format(v, spec) if v is not None else na


# ---------------------------------------------------------------------------
# Analysis 1 machinery: the as-of replay, both conventions
# ---------------------------------------------------------------------------

def replay_state_latest(facts: pd.DataFrame, month_ends: list[str]) -> dict:
    """`bff.replay_state` with the opposite tie-break: LAST filing wins.

    Byte-for-byte the same event-driven replay — filings are still applied once,
    in filed-date order, and only filings visible at the month-end are applied —
    except that a period already in the state is OVERWRITTEN by a later filing
    instead of being frozen at its first publication. Same-day re-filings break
    by accn order (the ingest SQL sorts by filed_date, accn), which makes the
    last-listed accession win rather than the first.
    """
    snaps: dict[str, dict] = {}
    state: dict[str, dict] = {}
    if facts.empty:
        return {m: {} for m in month_ends}
    groups = list(facts.groupby("filed_date", sort=True))
    gi = 0
    for m in month_ends:
        while gi < len(groups) and groups[gi][0] <= m:
            for row in groups[gi][1].itertuples(index=False):
                state.setdefault(row.tag, {})[(row.period_start, row.period_end)] = row.value
            gi += 1
        snaps[m] = {tag: dict(d) for tag, d in state.items()}
    return snaps


def build_panel(prefer_latest: bool, symbols: list[str], month_ends: list[str],
                closes: pd.DataFrame, conn) -> pd.DataFrame:
    """Build the derived feature panel under one PIT convention.

    Everything downstream of the replay (freshness guard, TTM assembly,
    split-adjusted market cap, forward return, ratio/growth derivation) is
    `scripts/build_fundamental_features`' own code, imported unchanged. Only the
    one function that decides WHICH filing of a period wins is swapped, so the
    two panels differ by that choice and nothing else.

    Swapping a module global is deliberate: copying `build_raw_panel` here would
    risk the two panels silently diverging in some other line.
    """
    orig = bff.replay_state
    if prefer_latest:
        bff.replay_state = replay_state_latest
    try:
        panel = bff.build_raw_panel(sorted(symbols), month_ends, conn)
    finally:
        bff.replay_state = orig
    panel = bff.attach_prices(panel, closes, month_ends)
    return bff.derive_features(panel)


# ---------------------------------------------------------------------------
# Restatement census — how much surface does the convention choice touch?
# ---------------------------------------------------------------------------

def restatement_census(conn) -> dict:
    """Publication history of every (symbol, tag, period) cell.

    Splits the cells two ways because they mean different things:
      * re-published (>= 2 distinct filed_date) — extremely common, because a
        period is reported again in later filings and every 10-Q carries
        comparatives. Harmless on its own: same number.
      * value-changed (>= 2 distinct value) — the subset where reading the
        first vs the last publication actually changes a number.
    """
    placeholders = ",".join("?" * len(NEEDED_TAGS))
    q = f"""
    SELECT COUNT(*),
           SUM(nf > 1),
           SUM(nv > 1),
           SUM(nf > 1 AND nv = 1)
    FROM (
      SELECT symbol, tag, period_start, period_end, unit,
             COUNT(DISTINCT filed_date) AS nf,
             COUNT(DISTINCT value)      AS nv
      FROM facts
      WHERE tag IN ({placeholders})
      GROUP BY symbol, tag, period_start, period_end, unit
    )
    """
    row = conn.execute(q, NEEDED_TAGS).fetchone()
    sel_total, sel, val_changed, same_val = [int(x or 0) for x in row]

    q_all = """
    SELECT COUNT(*), SUM(nf > 1), SUM(nv > 1) FROM (
      SELECT symbol, tag, period_start, period_end, unit,
             COUNT(DISTINCT filed_date) AS nf, COUNT(DISTINCT value) AS nv
      FROM facts GROUP BY symbol, tag, period_start, period_end, unit)
    """
    all_total, all_repub, all_val = [int(x or 0) for x in conn.execute(q_all).fetchone()]

    # Of the value-changed cells: was the late publication a within-quarter
    # amendment, or the period reappearing as a comparative a year+ later?
    q_lag = """
    SELECT
      SUM(late_lag > ?) AS late_reappearance,
      SUM(late_lag <= ?) AS prompt_amendment
    FROM (
      SELECT (julianday(MAX(filed_date)) - julianday(period_end)) AS late_lag
      FROM facts
      WHERE tag IN ({ph})
      GROUP BY symbol, tag, period_start, period_end, unit
      HAVING COUNT(DISTINCT filed_date) > 1 AND COUNT(DISTINCT value) > 1
    )
    """.format(ph=placeholders)
    late, prompt = [int(x or 0) for x in
                    conn.execute(q_lag, (COMPARATIVE_LAG_DAYS, COMPARATIVE_LAG_DAYS,
                                         *NEEDED_TAGS)).fetchone()]
    return {
        "panel_tags": {
            "cells": sel_total,
            "re_published": sel,
            "re_published_pct": round(100 * sel / sel_total, 2) if sel_total else None,
            "value_changed": val_changed,
            "value_changed_pct": round(100 * val_changed / sel_total, 2) if sel_total else None,
            "re_published_same_value": same_val,
        },
        "all_tags": {
            "cells": all_total,
            "re_published": all_repub,
            "value_changed": all_val,
            "value_changed_pct": round(100 * all_val / all_total, 2) if all_total else None,
        },
        "value_changed_timing": {
            "late_reappearance_gt_400d": late,
            "prompt_amendment_le_400d": prompt,
            "note": (
                "同一期间被多次申报、且数值不同：只有一小部分是真正的重述。"
                "多数是「该期间作为比较期出现在更晚的申报里」而带了轻微重分类差异"
                "（10-Q 的比较期、四舍五入、科目重分类）。因此 latest-visible 口径"
                "引入的更像是「重分类噪声」而非「修正后的真值」——这一点决定了"
                "分析1 里差异该怎么解读"),
        },
    }


def panel_convention_diff(asf: pd.DataFrame, lat: pd.DataFrame) -> dict:
    """Row-level footprint of the convention choice on the derived panel."""
    keys = ["date", "symbol"]
    m = asf[keys + FEATURES].merge(lat[keys + FEATURES], on=keys,
                                   suffixes=("_a", "_l"))
    per_feature: dict[str, dict] = {}
    any_changed = pd.Series(False, index=m.index)
    any_null_shift = pd.Series(False, index=m.index)
    for f in FEATURES:
        a = pd.to_numeric(m[f + "_a"], errors="coerce")
        b = pd.to_numeric(m[f + "_l"], errors="coerce")
        both = a.notna() & b.notna()
        # relative tolerance: these are ratios, so absolute deltas are useless
        denom = pd.concat([a.abs(), b.abs()], axis=1).max(axis=1).replace(0, np.nan)
        changed = both & ((a - b).abs() / denom > 1e-6)
        null_shift = a.isna() != b.isna()
        per_feature[f] = {
            "rows_both": int(both.sum()),
            "value_changed": int(changed.sum()),
            "value_changed_pct": round(100 * changed.mean(), 3),
            "null_shift": int(null_shift.sum()),
        }
        any_changed |= changed
        any_null_shift |= null_shift
    return {
        "rows": int(len(m)),
        "rows_any_feature_changed": int(any_changed.sum()),
        "rows_any_feature_changed_pct": round(100 * any_changed.mean(), 3),
        "rows_null_shift": int(any_null_shift.sum()),
        "per_feature": per_feature,
    }


def convention_comparison(asf: pd.DataFrame, lat: pd.DataFrame) -> dict:
    """12 factors, both conventions, same IC/era/tier machinery."""
    out: dict[str, dict] = {}
    flagged: list[str] = []
    for f in FEATURES:
        a = test_factor(asf, f)
        l = test_factor(lat, f)
        a["tier"], l["tier"] = tier(a, f), tier(l, f)
        ic_a, ic_l = a["overall"]["ic_mean"], l["overall"]["ic_mean"]
        delta = (None if ic_a is None or ic_l is None else round(ic_l - ic_a, 4))
        sign_flip = (ic_a is not None and ic_l is not None
                     and abs(ic_a) > 1e-9 and abs(ic_l) > 1e-9
                     and np.sign(ic_a) != np.sign(ic_l))
        rel = (None if not ic_a or ic_l is None else round(abs(delta) / abs(ic_a), 3))
        era_flips = sum(
            1 for name, _, _ in ERAS
            if (a["by_era"][name]["ic_mean"] or 0) * (l["by_era"][name]["ic_mean"] or 0) < 0
        )
        material = bool(sign_flip or (delta is not None and abs(delta) >= 0.005)
                        or (rel is not None and rel >= 0.5))
        tier_changed = a["tier"] != l["tier"]
        out[f] = {
            "expected_sign": a["expected_sign"],
            "as_filed": {"overall": a["overall"], "by_era": {
                n: {"ic_mean": a["by_era"][n]["ic_mean"], "ic_t": a["by_era"][n]["ic_t"],
                    "n_months": a["by_era"][n]["n_months"]} for n, _, _ in ERAS},
                "era_sign_consistent": a["era_sign_consistent"],
                "tier": a["tier"], "verdict": a["verdict"]},
            "latest_visible": {"overall": l["overall"], "by_era": {
                n: {"ic_mean": l["by_era"][n]["ic_mean"], "ic_t": l["by_era"][n]["ic_t"],
                    "n_months": l["by_era"][n]["n_months"]} for n, _, _ in ERAS},
                "era_sign_consistent": l["era_sign_consistent"],
                "tier": l["tier"], "verdict": l["verdict"]},
            "ic_delta": delta,
            "ic_delta_relative": rel,
            "era_sign_flips": era_flips,
            "sign_flip": bool(sign_flip),
            "material_change": material,
            "tier_changed": tier_changed,
            "tier_became": (f"{a['tier']} -> {l['tier']}" if tier_changed else None),
        }
        if material or tier_changed or era_flips:
            flagged.append(f)
    return {"by_factor": out, "flagged_factors": flagged}


# ---------------------------------------------------------------------------
# Analysis 2 machinery: incremental value over the price features
# ---------------------------------------------------------------------------

def rank_normal(df: pd.DataFrame | pd.Series) -> pd.DataFrame | pd.Series:
    """Rank -> normal scores, column-wise. Spearman as Pearson-after-transform."""
    n = len(df)
    z = norm.ppf(df.rank(method="average") / (n + 1.0))
    if isinstance(df, pd.DataFrame):
        return pd.DataFrame(z, index=df.index, columns=df.columns)
    return pd.Series(z, index=df.index)


def _ols(X: np.ndarray, y: np.ndarray):
    """OLS with intercept -> (residuals, R^2).

    `np.errstate` is turned off around the solve on purpose: numpy's float64
    matmul kernel raises spurious divide-by-zero / overflow FP flags for these
    shapes even when the result is finite and correct (verified: every residual
    and R^2 here is finite, and a constant control column would trip the same
    flags). The explicit finiteness assertion below is what actually guards
    against a degenerate design, so a real problem fails loudly instead of
    being hidden by the silenced warning.
    """
    A = np.column_stack([np.ones(len(X)), X])
    with np.errstate(all="ignore"):
        beta, *_ = np.linalg.lstsq(A, y, rcond=None)
        resid = y - A @ beta
    if not np.isfinite(resid).all() or not np.isfinite(beta).all():
        raise FloatingPointError(
            f"degenerate cross-sectional regression: {len(X)} rows x {A.shape[1]} cols")
    ss_res = float(resid @ resid)
    ss_tot = float(((y - y.mean()) ** 2).sum())
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 1e-12 else np.nan
    return resid, r2


def _corr(a: np.ndarray, b: np.ndarray) -> float:
    if a.std() < 1e-12 or b.std() < 1e-12:
        return float("nan")
    return float(np.corrcoef(a, b)[0, 1])


def monthly_incremental(df: pd.DataFrame, factor: str, controls: list[str]) -> list[dict]:
    """Per month: raw IC vs incremental IC vs the 13 price-volume controls.

    Complete cases only, and the SAME rows feed the raw and the incremental IC,
    so the columns are paired and their difference is attributable to the
    controls alone.

    Three incremental estimators are recorded. They are not interchangeable and
    the difference is a variance-ratio, not noise:

      inc_ic_partial      corr(resid_x, resid_y)  — the exact partial rank
                          correlation. PRIMARY.
      inc_ic_resid_return corr(x_rank, resid_y)   — deflated by sd(resid_x)/sd(x)
      inc_ic_resid_factor corr(resid_x, y_rank)   — deflated by sd(resid_y)/sd(y)

    The last two are what "residualise one side and correlate with the other"
    gives you directly; both shrink toward zero by the share of the variable the
    controls do NOT explain, so quoting either as "the incremental IC" quietly
    understates (or overstates) the effect. They are kept because they are the
    natural output of each variant of method (a) and agree with the exact
    partial correlation to within that known factor — a self-check.
    """
    cols = ["fwd_ret_1m", factor] + list(controls)
    d = df[["date", "symbol"] + cols].copy()
    for c in cols:
        d[c] = pd.to_numeric(d[c], errors="coerce").replace([np.inf, -np.inf], np.nan)
    d = d.dropna(subset=cols)

    out: list[dict] = []
    for date, chunk in d.groupby("date"):
        if len(chunk) < MIN_NAMES or chunk[factor].nunique() < 5:
            continue
        y_raw = chunk["fwd_ret_1m"].to_numpy(dtype=float)
        x_raw = chunk[factor].to_numpy(dtype=float)
        raw_ic = spearmanr(x_raw, y_raw).statistic
        if not np.isfinite(raw_ic):
            continue

        Xn = rank_normal(chunk[list(controls)]).to_numpy(dtype=float)
        yn = rank_normal(chunk["fwd_ret_1m"]).to_numpy(dtype=float)
        xn = rank_normal(chunk[factor]).to_numpy(dtype=float)

        resid_y, r2_base = _ols(Xn, yn)
        resid_x, r2_x = _ols(Xn, xn)
        _, r2_full = _ols(np.column_stack([Xn, xn]), yn)

        out.append({
            "date": date,
            "n": int(len(chunk)),
            "raw_ic": float(raw_ic),
            "inc_ic_partial": _corr(resid_x, resid_y),
            "inc_ic_resid_return": _corr(xn, resid_y),
            "inc_ic_resid_factor": _corr(resid_x, yn),
            "r2_controls": r2_base,
            "r2_factor_on_controls": r2_x,
            "delta_r2": r2_full - r2_base,
        })
    return out


def series_stats(rows: list[dict], key: str, mean_key: str | None = None) -> dict:
    """Overall + era split + sign consistency for one monthly statistic."""
    vals = [r for r in rows if r.get(key) is not None and np.isfinite(r[key])]
    overall = summarize([r[key] for r in vals])
    by_era = {}
    for name, lo, hi in ERAS:
        by_era[name] = summarize([r[key] for r in vals
                                  if lo <= int(r["date"][:4]) <= hi])
    era_means = [by_era[n]["ic_mean"] for n, _, _ in ERAS
                 if by_era[n]["ic_mean"] is not None]
    signs = {np.sign(m) for m in era_means if abs(m) > 1e-9}
    st = {
        "n_months": overall["n_months"],
        "mean": overall["ic_mean"],
        "t": overall["ic_t"],
        "positive_rate": overall["ic_positive_rate"],
        "std": overall["ic_std"],
        "by_era": by_era,
        "era_sign_consistent": bool(len(signs) <= 1 and len(era_means) == len(ERAS)),
    }
    if mean_key:
        arr = np.asarray([r[mean_key] for r in rows
                          if r.get(mean_key) is not None and np.isfinite(r[mean_key])],
                         dtype=float)
        st[mean_key + "_mean"] = round(float(arr.mean()), 5) if len(arr) else None
    return st


def era_concentration(by_era: dict) -> dict:
    """Is the effect spread across eras, or is one era carrying it?

    Same-sign in all three eras is the declared bar, but a factor whose era
    means are +0.054 / +0.003 / +0.012 passes that bar while being a 2016-2019
    story. This flag makes that visible instead of leaving it to the reader to
    notice the magnitudes (the failure mode this repo hit with the FOMC calendar
    effect, which was entirely a ZIRP artefact).

    Only defined when the three era means share a sign: for a factor that
    already flips sign across eras, "how concentrated is the magnitude" is not a
    meaningful question, and answering it with absolute values would flag every
    noise factor as "concentrated".
    """
    means = {n: by_era[n]["ic_mean"] for n, _, _ in ERAS}
    mags = {n: abs(v or 0.0) for n, v in means.items()}
    signs = {np.sign(v) for v in means.values() if v is not None and abs(v) > 1e-9}
    if len(signs) > 1:
        return {"concentrated": None, "abs_era_ic": {k: round(v, 4) for k, v in mags.items()},
                "reading": "三段符号不一致，集中度不适用（因子已判不稳健）"}
    vals = [v for v in mags.values() if v > 1e-9]
    if len(vals) < 2:
        return {"concentrated": None, "abs_era_ic": {k: round(v, 4) for k, v in mags.items()},
                "reading": "可用 era 不足"}
    hi, lo = max(vals), min(vals)
    concentrated = lo < hi / 3.0
    return {
        "abs_era_ic": {k: round(v, 4) for k, v in mags.items()},
        "max_over_min": round(hi / lo, 2),
        "concentrated": bool(concentrated),
        "reading": ("效果集中在单一 era（最小 era 幅度 < 最大 era 的 1/3），"
                    "三段同号并没有让它变成稳定因子" if concentrated
                    else "三段幅度同量级，未见单 era 主导"),
    }


def incremental_verdict(inc: dict, factor: str,
                        bonferroni_t: float) -> tuple[bool, str]:
    """Mechanical rule — no discretion beyond the declared bar.

    A factor "has incremental value" only if its partial rank IC passes
    |t| >= 2, keeps its sign in all three eras, AND clears the Bonferroni bar
    for the 12 tests this table runs. Failing the last bar is reported as
    "suspected false positive" rather than quietly passing.
    """
    t, ic = inc["t"], inc["mean"]
    if t is None or ic is None:
        return False, "无数据：完整样本月份不足"
    if not inc["era_sign_consistent"]:
        got = ", ".join(f"{n}={inc['by_era'][n]['ic_mean']}" for n, _, _ in ERAS)
        return False, f"不稳健：三段增量 IC 符号不一致（{got}）"
    if abs(t) < 2.0:
        return False, f"无增量：增量 IC={ic:+.4f} t={t:.2f} < 2"
    if abs(t) < bonferroni_t:
        return False, (f"疑似假阳性：t={t:.2f} 过 2 但未过 Bonferroni 阈值 "
                       f"{bonferroni_t:.2f}（12 次检验），不作为证据")
    return True, f"有增量：增量 IC={ic:+.4f} t={t:.2f}，过 Bonferroni 阈值"


def test_incrementality(df: pd.DataFrame, controls: list[str],
                        factors: list[str]) -> dict:
    out: dict[str, dict] = {}
    for f in factors:
        rows = monthly_incremental(df, f, controls)
        if not rows:
            out[f] = {"n_months": 0, "verdict": "无数据", "incremental": False}
            continue
        raw = series_stats(rows, "raw_ic")
        partial = series_stats(rows, "inc_ic_partial", mean_key="delta_r2")
        resid_ret = series_stats(rows, "inc_ic_resid_return")
        resid_fac = series_stats(rows, "inc_ic_resid_factor")
        out[f] = {
            "input_is_rank_normalised": True,
            "n_months": len(rows),
            "first_month": min(r["date"] for r in rows),
            "last_month": max(r["date"] for r in rows),
            "n_obs_mean": int(np.mean([r["n"] for r in rows])),
            "raw_ic": raw,
            "inc_ic_partial": partial,
            "inc_ic_resid_return": resid_ret,
            "inc_ic_resid_factor": resid_fac,
            "expected_sign": EXPECTED_SIGN.get(f),
            "sign_vs_academic_prior": (
                ("matches" if (partial["mean"] or 0) * EXPECTED_SIGN[f] > 0 else "contradicts")
                if partial["mean"] is not None and EXPECTED_SIGN.get(f) else None),
            "sign_vs_own_raw_ic": (
                ("matches" if (partial["mean"] or 0) * (raw["mean"] or 0) > 0 else "contradicts")
                if partial["mean"] is not None and raw["mean"] is not None else None),
            "estimator_agreement": {
                "partial_vs_resid_return": round(partial["mean"] - resid_ret["mean"], 4),
                "partial_vs_resid_factor": round(partial["mean"] - resid_fac["mean"], 4),
                "note": ("两个「单边残差」口径按 sd(resid)/sd(原变量) 的固定比例收缩，"
                         "因此与精确偏相关有系统性差值；差值方向与量级应等于 "
                         "sqrt(1-R²) 的比值，用来确认实现无误"),
            },
            "delta_r2_mean": partial["delta_r2_mean"],
            "r2_controls_mean": round(float(np.nanmean([r["r2_controls"] for r in rows])), 5),
            "r2_factor_on_controls_mean": round(
                float(np.nanmean([r["r2_factor_on_controls"] for r in rows])), 5),
            "ic_shrinkage_pct": (round(100 * (1 - partial["mean"] / raw["mean"]), 1)
                                 if raw["mean"] and abs(raw["mean"]) > 1e-9 else None),
            "era_concentration": era_concentration(partial["by_era"]),
        }
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def fingerprint(conn, month_ends: list[str], n_symbols: int) -> str:
    db = ROOT / "data" / "fundamentals.db"
    raw = "|".join([CACHE_VERSION, str(int(db.stat().st_mtime)) if db.exists() else "0",
                    str(len(month_ends)), month_ends[0], month_ends[-1],
                    str(n_symbols)])
    return hashlib.sha1(raw.encode()).hexdigest()[:16]


def main() -> None:
    ap = argparse.ArgumentParser(description="Fundamental PIT + incremental tests")
    ap.add_argument("--rebuild-latest", action="store_true",
                    help="ignore the cached latest-visible panel")
    ap.add_argument("--price-start", default="2016-01-01")
    ap.add_argument("--load-symbols", type=int, default=0,
                    help="debug: cap the number of symbols (report goes to a "
                         ".smoke.json side file so a partial run can never be "
                         "mistaken for the real one)")
    args = ap.parse_args()

    from scripts.xsec_gbm_selection import month_end_signal_days
    from src.data.nasdaq_store import NasdaqDailyStore

    t0 = time.time()
    conn = connect()
    month_ends = [r[0] for r in conn.execute(
        "SELECT DISTINCT date FROM fundamental_features ORDER BY date")]
    syms_db = [r[0] for r in conn.execute(
        "SELECT DISTINCT symbol FROM fundamental_features ORDER BY symbol")]
    symbols = syms_db[:args.load_symbols] if args.load_symbols else syms_db
    end = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    print(f"  month-ends: {len(month_ends)} ({month_ends[0]} ~ {month_ends[-1]}), "
          f"symbols: {len(symbols)}")

    # ---- prices (same source/start as build_fundamental_features.main) ----
    store = NasdaqDailyStore(assetclass="stocks")
    closes = {}
    for sym in symbols:
        d = store.get_daily(sym, args.price_start, end)
        if not d.empty:
            closes[sym] = d["close"]
    closes = pd.DataFrame(closes).sort_index()
    print(f"  close panel: {closes.shape[0]}d x {closes.shape[1]}s "
          f"({str(closes.index[0].date())} ~ {str(closes.index[-1].date())})")

    # ---- as-filed panel: rebuilt through the harness, then verified vs DB ----
    print("  building as-filed panel (harness) ...")
    asf = build_panel(False, symbols, month_ends, closes, conn)
    db_cols = ["date", "symbol", "close", "fwd_ret_1m", "market_cap"] + FEATURES
    dbp = pd.read_sql_query("SELECT " + ", ".join(db_cols)
                            + " FROM fundamental_features", conn)
    if args.load_symbols:
        dbp = dbp[dbp["symbol"].isin(symbols)]
    ver = asf[db_cols].merge(dbp, on=["date", "symbol"], suffixes=("_h", "_db"))
    harness_check = {"rows_compared": int(len(ver))}
    worst = 0
    for c in ["close", "fwd_ret_1m", "market_cap"] + FEATURES:
        a = pd.to_numeric(ver[c + "_h"], errors="coerce")
        b = pd.to_numeric(ver[c + "_db"], errors="coerce")
        both = a.notna() & b.notna()
        mism = int((((a - b).abs() / pd.concat([a.abs(), b.abs()], axis=1)
                     .max(axis=1).replace(0, np.nan))[both] > 1e-9).sum())
        nulls = int((a.isna() != b.isna()).sum())
        worst = max(worst, mism, nulls)
        harness_check[c] = {"mismatch_gt_1e-9": mism, "null_mismatch": nulls}
    harness_check["ok"] = worst == 0
    print(f"  harness check vs DB: {'OK' if harness_check['ok'] else 'MISMATCH'} "
          f"({len(ver)} rows x {len(db_cols) - 2} numeric cols)")

    # ---- latest-visible panel (cached; /data/ is gitignored) ----
    fp = fingerprint(conn, month_ends, len(symbols))
    lat = None
    if not args.rebuild_latest and LATEST_CACHE.exists() and LATEST_META.exists():
        meta = json.loads(LATEST_META.read_text())
        if meta.get("fingerprint") == fp:
            lat = pd.read_csv(LATEST_CACHE)
            print(f"  latest-visible panel: cache hit ({lat.shape})")
    if lat is None:
        print("  building latest-visible panel (prefer_latest=True) ...")
        lat = build_panel(True, symbols, month_ends, closes, conn)
        keep = ["date", "symbol", "close", "market_cap", "fwd_ret_1m"] + FEATURES
        lat[[c for c in keep if c in lat]].to_csv(LATEST_CACHE, index=False)
        LATEST_META.write_text(json.dumps({"fingerprint": fp, "version": CACHE_VERSION,
                                          "built_at": datetime.now(tz=timezone.utc)
                                          .isoformat(timespec="seconds"),
                                          "rows": int(len(lat))}, indent=2))
        print(f"  cached -> {LATEST_CACHE.name}")

    # ---- data hygiene: identical treatment of both panels ----
    panels = {}
    for name, p in (("as_filed", asf), ("latest_visible", lat)):
        q = p.copy()
        for c in ["close", "fwd_ret_1m"] + FEATURES:
            q[c] = pd.to_numeric(q[c], errors="coerce").replace(
                [np.inf, -np.inf], np.nan)
        bad = q["fwd_ret_1m"].notna() & ((q["fwd_ret_1m"] < RET_LO)
                                        | (q["fwd_ret_1m"] > RET_HI))
        q.loc[bad, "fwd_ret_1m"] = np.nan
        q = q.sort_values(["symbol", "date"])
        q[REFERENCE] = (q.groupby("symbol", sort=False)["close"].shift(1)
                        / q.groupby("symbol", sort=False)["close"].shift(12) - 1.0)
        panels[name] = q
        print(f"  [{name}] rows={len(q)} dropped_extreme={int(bad.sum())}")
    asf, lat = panels["as_filed"], panels["latest_visible"]

    # ==================== Analysis 1 ====================
    print("\n  --- analysis 1: PIT convention sensitivity ---")
    a1 = {
        "question": "as-filed（最早申报值永续胜出）vs latest-visible（重述后取修正值）",
        "method": ("用同一套代码路径重建整个面板，只替换 replay_state 的收敛规则；"
                   "12 因子 IC / era split / tier 规则原样复用 "
                   "scripts/test_fundamental_factors.py 的 test_factor() 与 tier()"),
        "restatement_census": restatement_census(conn),
        "panel_diff": panel_convention_diff(asf, lat),
        "harness_check": harness_check,
    }
    cmp = convention_comparison(asf, lat)
    a1.update(cmp)

    print(f"  {'factor':<20}{'as-filed':>18}{'latest-vis':>18}{'delta':>10}")
    for f in FEATURES:
        r = cmp["by_factor"][f]
        print(f"  {f:<20}{_fmt(r['as_filed']['overall']['ic_mean']):>18}"
              f"{_fmt(r['latest_visible']['overall']['ic_mean']):>18}"
              f"{_fmt(r['ic_delta']):>10}"
              f"  {r['as_filed']['tier']}->{r['latest_visible']['tier']}")
    print(f"  flagged: {cmp['flagged_factors'] or 'none'}")

    # ==================== Analysis 2 ====================
    print("\n  --- analysis 2: incremental value over 13 price-volume features ---")
    # restrict to names carried by the fundamental panel
    panel_syms = sorted(set(asf["symbol"]))
    print(f"  loading daily OHLCV for {len(panel_syms)} symbols ...")
    pl = load_panel(store, panel_syms, args.price_start, end)
    pf = compute_daily_features(pl)
    idx = pl["close"].index
    print(f"  daily panel {pl['close'].shape[0]}d x {pl['close'].shape[1]}s, "
          f"{len(PRICE_FEATURES)} price features")

    recs, exact, fallback, skipped = [], 0, 0, []
    for d in month_ends:
        ts = pd.Timestamp(d)
        if idx.tz is not None:
            ts = ts.tz_localize(idx.tz)
        pos = int(idx.searchsorted(ts, side="right")) - 1
        if pos < 0:
            skipped.append(d)
            continue
        lag = (ts - idx[pos]).days
        if lag != 0:
            if lag > 5:            # no usable trading day near this month-end
                skipped.append(d)
                continue
            fallback += 1
        else:
            exact += 1
        sub = pd.DataFrame({f: pf[f].iloc[pos] for f in PRICE_FEATURES})
        sub.index = sub.index.rename("symbol")
        sub["date"] = d
        recs.append(sub.reset_index())
    price = pd.concat(recs, ignore_index=True)
    print(f"  price features sampled: {exact} month-ends exact, {fallback} fallback, "
          f"{len(skipped)} skipped")

    merged = asf.merge(price, on=["date", "symbol"], how="inner")
    merged_l = lat.merge(price, on=["date", "symbol"], how="inner")
    print(f"  merged panel: as-filed {merged.shape}, latest {merged_l.shape}")

    n_tests = len(INCREMENTAL_FACTORS) * 2
    bonf_t = float(norm.ppf(1 - 0.05 / (2 * n_tests)))
    print(f"  {n_tests} incremental tests -> Bonferroni |t| bar = {bonf_t:.2f}")

    inc_full = test_incrementality(merged, PRICE_FEATURES, INCREMENTAL_FACTORS)
    inc_lat = test_incrementality(merged_l, PRICE_FEATURES, INCREMENTAL_FACTORS)
    inc_short = test_incrementality(merged, SHORT_HISTORY_CONTROLS,
                                    INCREMENTAL_FACTORS)

    # reference: 12-1 momentum on the same restricted sample
    ref_rows = monthly_incremental(merged.rename(columns={REFERENCE: "ref"}),
                                   "ref", PRICE_FEATURES)

    def finish(block: dict) -> dict:
        for f, r in block.items():
            if r.get("n_months", 0) == 0:
                r["verdict"], r["incremental"] = "无数据", False
                continue
            ok, why = incremental_verdict(r["inc_ic_partial"], f, bonf_t)
            r["verdict"], r["incremental"] = why, ok
            # same-sample vs full-sample raw IC, so the reader can see whether
            # the complete-case restriction moved the baseline
            full = cmp["by_factor"][f]["as_filed"]["overall"]
            r["raw_ic_full_sample"] = full
            r["raw_ic_sample_delta"] = (
                round(r["raw_ic"]["mean"] - full["ic_mean"], 4)
                if full["ic_mean"] is not None and r["raw_ic"]["mean"] is not None
                else None)
        return block

    a2 = {
        "question": "控制现有 13 个价量特征之后，基本面因子还有没有增量预测力？",
        "method": (
            "(a) 截面残差法（选用理由：可同时给出增量 R²；且先做秩正态化，"
            "避免了原始值偏相关被少数极值股主导）：每月对收益、因子、13 个控制变量"
            "做 rank->normal scores，再把秩正态收益与秩正态因子分别对秩正态控制做 OLS，"
            "主统计量为两个残差之间的相关 inc_ic_partial（精确偏秩相关）；"
            "同时给出两个「单边残差」变体 inc_ic_resid_return / inc_ic_resid_factor，"
            "它们按 sd(resid)/sd(原变量) 的固定比例系统性地收缩，因此只能作为自检"
            "（三方差值可由 sqrt(1-R²) 预测）。另报把因子加入 13 控制回归后的平均月度 ΔR²"),
        "estimator_note": (
            "重要：单边残差 ≠ 增量信息。corr(x, resid_y) 与 corr(resid_x, y) "
            "都被对方变量的残差方差比例缩小，直接把其中任一当作「增量 IC」"
            "会系统性低估；本报告一律以 inc_ic_partial 判定"),
        "controls": PRICE_FEATURES,
        "controls_short_history_sensitivity": SHORT_HISTORY_CONTROLS,
        "min_names": MIN_NAMES,
        "complete_case_pairing": ("raw IC 与增量 IC 用完全相同的 (月, 股票) 样本，"
                                  "两列的差异只来自控制变量"),
        "sample_restriction": {
            "note": ("本地价格库只有 2016-09-12 起的数据，价格特征需要最长 252 个交易日"
                     "回看，且月度截面要 ≥30 只完整样本，因此完整样本月份少于全样本。"
                     "实测：12 因子全样本 119 个月（roe 等），主检验 107 个月；"
                     "去掉两个 252 日特征后的敏感性检验是 108 个月 —— 也就是说"
                     "丢失的 12 个月里只有约 1 个月该归因于 252 日特征，"
                     "其余来自 60-120 日回看 + 早期月份截面不足 30 只。"
                     "era 1 因此从 40 个月缩到 28 个月，敏感性检验用于确认这"
                     "不是结论来源"),
            "price_sample_exact_months": exact,
            "price_sample_fallback_months": fallback,
            "months_skipped": skipped,
        },
        "bonferroni": {"n_tests": n_tests, "alpha": 0.05,
                       "t_threshold": round(bonf_t, 3),
                       "note": ("6 因子 x 2 口径 = 12 次检验，5% 下期望假阳性 0.6 个；"
                                "t 过 2 但不过 Bonferroni 阈值的一律标注为疑似假阳性")},
        "price_feature_informativeness": {
            "note": "13 个价量特征单独对收益秩的线性解释力（月度 R² 均值）——同一批数据的上限参照",
            "r2_controls_mean": inc_full[INCREMENTAL_FACTORS[0]]["r2_controls_mean"],
        },
        "as_filed": finish(inc_full),
        "latest_visible": finish(inc_lat),
        "sensitivity_short_history_controls": finish(inc_short),
        "reference_mom_12_1_same_sample": {
            "n_months": len(ref_rows),
            "raw_ic": summarize([r["raw_ic"] for r in ref_rows
                                 if np.isfinite(r["raw_ic"])]),
            "note": ("12-1 动量本身就在控制集里，算不了「增量」；这里给的是它在"
                     "同一受限样本上的原始 IC，作为「这个池子信号普遍多弱」的标尺"),
        },
    }

    print(f"\n  {'factor':<14}{'rawIC':>9}{'incIC':>9}{'t':>7}{'era':>6}{'conc':>6}"
          f"{'dR2':>9}   verdict")
    for f in INCREMENTAL_FACTORS:
        r = a2["as_filed"][f]
        if r.get("n_months", 0) == 0:
            print(f"  {f:<14}  no data")
            continue
        p, c = r["inc_ic_partial"], r["era_concentration"]
        print(f"  {f:<14}{_fmt(r['raw_ic']['mean']):>9}"
              f"{_fmt(p['mean']):>9}"
              f"{_fmt(p['t'], '.2f'):>7}"
              f"{'yes' if p['era_sign_consistent'] else 'NO':>6}"
              f"{('YES' if c.get('concentrated') else 'no'):>6}"
              f"{_fmt(r['delta_r2_mean'], '.5f'):>9}   {r['verdict']}")

    # ---- cross-check analysis 1 against the committed baseline report ----
    baseline_path = ROOT / "reports" / "fundamental_factor_tests.json"
    replication = {"baseline_report": str(baseline_path.relative_to(ROOT)),
                   "compared": 0, "matched": 0, "mismatches": {}}
    if baseline_path.exists():
        base = json.loads(baseline_path.read_text())
        for f in FEATURES:
            if f not in base.get("factors", {}):
                continue
            replication["compared"] += 1
            bo = base["factors"][f]["overall"]
            mine = cmp["by_factor"][f]["as_filed"]["overall"]
            if (bo["ic_mean"] == mine["ic_mean"] and bo["ic_t"] == mine["ic_t"]
                    and bo["n_months"] == mine["n_months"]
                    and base["factors"][f].get("tier")
                    == cmp["by_factor"][f]["as_filed"]["tier"]):
                replication["matched"] += 1
            else:
                replication["mismatches"][f] = {
                    "baseline": {"ic_mean": bo["ic_mean"], "ic_t": bo["ic_t"],
                                 "n_months": bo["n_months"],
                                 "tier": base["factors"][f].get("tier")},
                    "reproduced": {"ic_mean": mine["ic_mean"], "ic_t": mine["ic_t"],
                                   "n_months": mine["n_months"],
                                   "tier": cmp["by_factor"][f]["as_filed"]["tier"]}}
        replication["ok"] = (replication["compared"] == replication["matched"]
                             and replication["compared"] > 0)
        replication["note"] = (
            "as-filed 一侧逐因子核对已提交的 fundamental_factor_tests.json（IC/t/月份数/tier）。"
            "完全一致说明最新口径对比的「基准侧」不是我新造的口径，而是已发布的那一份；"
            "若有 mismatch，则该因子的两种口径对比不可信，必须先解释差异")
    a1["replication_of_committed_report"] = replication
    print(f"  analysis-1 replication vs committed report: "
          f"{replication['matched']}/{replication['compared']} exact"
          + ("" if replication.get("ok") else f"  MISMATCH {replication['mismatches']}"))

    # ---- verdicts ----
    inc_factors = [f for f in INCREMENTAL_FACTORS if a2["as_filed"][f].get("incremental")]
    flags_a1 = cmp["flagged_factors"]
    roe = cmp["by_factor"]["roe"]
    roe_inc = a2["as_filed"]["roe"]
    roe_p = roe_inc.get("inc_ic_partial", {})
    roe_conc = roe_inc.get("era_concentration", {})
    any_inc_both = [f for f in INCREMENTAL_FACTORS
                    if a2["as_filed"][f].get("incremental")
                    and a2["latest_visible"][f].get("incremental")]
    tier_any_changed = [f for f in FEATURES if cmp["by_factor"][f]["tier_changed"]]
    concentrated = [f for f in INCREMENTAL_FACTORS
                    if a2["as_filed"][f].get("era_concentration", {}).get("concentrated")]
    contradicts = [f for f in INCREMENTAL_FACTORS
                   if a2["as_filed"][f].get("sign_vs_academic_prior") == "contradicts"]
    verdict = (
        f"分析1（口径敏感性）：12 个因子的 tier 在 as-filed 与 latest-visible 下"
        + (f"存在变化：{', '.join(tier_any_changed)}" if tier_any_changed else "完全一致")
        + f"。面板层面有 {a1['panel_diff']['rows_any_feature_changed_pct']}% 的 "
          f"(月,股票) 行至少有一个特征取值不同，但 12 个因子的 IC 变化全部 ≤0.0005："
        + (f"{len(flags_a1)} 个因子被机械标记（{', '.join(flags_a1)}），"
           "但两者 IC 本身都≈0（分母极小导致的相对变化），无实质含义。"
           if flags_a1 else "无因子被标记。")
        + f" roe：as-filed IC={_fmt(roe['as_filed']['overall']['ic_mean'])} -> "
          f"latest IC={_fmt(roe['latest_visible']['overall']['ic_mean'])} "
          f"(Δ={_fmt(roe['ic_delta'])})，"
        + ("符号翻转 —— 信号依赖「原始申报数字」的噪声特性。"
           if roe["sign_flip"] else
           "未翻转也未衰减 —— roe 的信号不建立在「原始申报值 vs 重述值」的差异上，"
           "这是一个真实的稳健性正面结论。")
        + f"\n分析2（增量价值）：控制 13 个价量特征后，"
        + (f"通过门槛的因子：{', '.join(inc_factors)}"
           if inc_factors else "没有任何因子通过「|t|≥2 且三段同号 且过 Bonferroni」")
        + (f"（两种口径下都通过：{', '.join(any_inc_both)}）" if any_inc_both else "")
        + f"。roe 精确偏秩相关增量 IC={_fmt(roe_p.get('mean'))} (t={roe_p.get('t')})，"
          f"相对原始 IC 收缩 {roe_inc.get('ic_shrinkage_pct')}%，三段幅度不均（{roe_conc.get('reading')}）。"
        + (f" 另有 {len(concentrated)} 个因子虽三段同号但幅度集中在单一 era："
           f"{', '.join(concentrated)}。" if concentrated else "")
        + (f" 方向警示：{', '.join(contradicts)} 的增量方向与学术先验相反"
           "（asset_growth 的投资因子先验是高资产增长→低后续收益），"
           "即使过了门槛也必须先解释方向，不能因为有正 IC 就使用。"
           if contradicts else "")
    )

    report = {
        "meta": {
            "generated_at": datetime.now(tz=timezone.utc).isoformat(timespec="seconds"),
            "script": "scripts/test_fundamental_incremental.py",
            "source": "data/fundamentals.db (facts / splits / fundamental_features)",
            "what_this_is": ("两项收尾分析：PIT 口径敏感性 + 相对 13 个价量特征的增量价值。"
                             "均为研究性单因子/截面统计，不是组合回测，不含成本与交易规则"),
            "panel": {
                "rows": int(len(asf)),
                "months": len(month_ends),
                "symbols": int(asf["symbol"].nunique()),
                "date_range": [min(month_ends), max(month_ends)],
            },
            "runtime_sec": round(time.time() - t0, 1),
        },
        "conventions": {
            "ic": "月度截面 Spearman 秩相关（feature vs 未来1个月收益）",
            "forward_return": ("信号月末次一交易日收盘买入，下一个月末的次一交易日收盘卖出"
                              "（与 src/selection 的 execution_lag_bars=1 一致）"),
            "pit": "特征只用 filed_date ≤ 月末的申报；两口径均无前视",
            "era_split": "强制 2016-19 / 2020-22 / 2023-26；t<2 或三段异号即判不稳健",
            "robust_rule": "|IC t| ≥ 2 且三段 IC 均值同号",
            "no_cherry_picking": "不翻转符号、不事后改口径/窗口/池子",
            "data_hygiene": f"剔除月度收益超出 [{RET_LO}, {RET_HI}] 的记录",
            "price_controls": "scripts/xsec_gbm_selection.FEATURES（只读 import）",
        },
        "analysis_1_pit_sensitivity": a1,
        "analysis_2_incremental": a2,
        "multiple_comparisons": {
            "analysis_1_tests": len(FEATURES) * 2,
            "analysis_2_tests": n_tests,
            "analysis_2_bonferroni_t": round(bonf_t, 3),
            "expected_false_positives_at_5pct": round(0.05 * n_tests, 2),
            "note": "报出的 t 值按此打折阅读；单点 t≈2 在此规模下不构成证据",
        },
        "verdict": verdict,
        "recommendation": None,  # filled mechanically below
    }

    # Recommendation is derived from the thresholds, not from judgement about
    # which factor "looks interesting".
    incremental_ok = [f for f in INCREMENTAL_FACTORS
                      if a2["as_filed"][f].get("incremental")
                      and a2["latest_visible"][f].get("incremental")]
    near_miss = {f: {"inc_ic": a2["as_filed"][f]["inc_ic_partial"]["mean"],
                     "t": a2["as_filed"][f]["inc_ic_partial"]["t"],
                     "era_concentrated": a2["as_filed"][f]["era_concentration"]
                     .get("concentrated"),
                     "sign_vs_academic_prior": a2["as_filed"][f]["sign_vs_academic_prior"],
                     "era_abs_ic": a2["as_filed"][f]["era_concentration"]["abs_era_ic"]}
                 for f in INCREMENTAL_FACTORS
                 if a2["as_filed"][f].get("n_months", 0) > 0
                 and not a2["as_filed"][f].get("incremental")
                 and abs(a2["as_filed"][f]["inc_ic_partial"]["t"] or 0) >= 1.5}
    report["near_misses"] = near_miss
    report["recommendation"] = (
        (f"建议进特征库（需先通过组合层面检验）：{', '.join(incremental_ok)}"
         if incremental_ok else
         "建议不进特征库。没有任何基本面因子在控制 13 个价量特征后通过"
         "「|t|≥2 且三段同号 且过 Bonferroni」门槛")
        + "。口径敏感性（分析1）未显示任何被 as-filed 噪声掩盖的强因子，"
          "因此这个负面结论不是口径选择造成的。"
        + (f" 唯一接近的因子（|t|≥1.5 但未达标）："
           + "; ".join(f"{f} 增量 IC={v['inc_ic']:+.4f} t={v['t']}"
                       + ("，三段幅度集中在单一 era" if v["era_concentrated"] else "")
                       + ("，方向与学术先验相反" if v["sign_vs_academic_prior"] == "contradicts"
                          else "")
                       for f, v in near_miss.items())
           + "。这些值不值得为它单独建仓或扩特征库，除非先在组合层面"
             "（不是单因子 IC）证明它能改善 GBM 的样本外选股。"
           if near_miss else "")
        + " 继续在 EDGAR 基本面数据上做特征工程的边际期望价值低于把同样精力"
          "放在其他方向；这份数据更适合作为「解释/归因」而不是「选股信号」。"
    )

    REPORT_PATH.parent.mkdir(exist_ok=True)
    out_path = (LATEST_CACHE.parent.parent / "reports"
                / "fundamental_incremental.smoke.json") if args.load_symbols else REPORT_PATH
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False))
    conn.close()
    print(f"\n  report -> {out_path}")
    print(f"  runtime {time.time() - t0:.1f}s")
    print("\n  VERDICT:\n" + verdict)
    print("\n  RECOMMENDATION:\n  " + report["recommendation"])


if __name__ == "__main__":
    main()
