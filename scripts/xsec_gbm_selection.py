#!/usr/bin/env python3
"""Cross-sectional GBM (LightGBM) monthly stock selection on the S&P 100.

The ML counterpart to the M1/M2 momentum rule: instead of ranking stocks by
12-1 momentum, a gradient-boosted tree ranks them every month from 13
point-in-time price/volume features. Everything else (universe, execution,
costs, metrics) is IDENTICAL to the momentum engine so the comparison is
apples-to-apples.

Data conventions (matching src/selection/engine.py exactly):
  - Universe: point-in-time S&P 100 constituents per year (Wikipedia
    revision snapshots, data/universe/sp100_YYYY.json), old tickers mapped
    to current ones. Delisted/missing names are skipped and recorded.
  - Signal: last trading day of the month; features use data STRICTLY
    BEFORE the signal day (same convention as factors.momentum_12_1).
  - Execution: close of the next trading day after the signal
    (execution_lag_bars=1 in SelectionBacktest).
  - Costs: IBKR Pro (per-share commission + 5 bps half-spread) via
    IbkrCostModel. Prices are split-adjusted, dividend-unadjusted (QQQ
    benchmark is total-return approximated with a 0.6%/yr accrual).

ML discipline (anti-overfit):
  - Label: forward 21-trading-day return minus the same-month cross-
    sectional pool mean ("who beats whom", not the market).
  - Features: z-scored per month across the eligible pool, clipped at ±3.
  - Purged walk-forward: at test month M the model trains ONLY on feature
    months <= M-2 (their 21d label windows close before M's opens; +1
    month safety gap). Expanding window, monthly retrain.
  - Hyperparameters: locked after an INNER walk-forward CV restricted to
    2017-2020 (--mode tune). No random K-fold anywhere. The defaults in
    this script ARE the locked parameters; do not retune on test results.

Four-way comparison over the identical window (and a 2021+ sub-window):
  1. GBM top-N equal weight     3. QQQ buy & hold
  2. 12-1 momentum top-N        4. full-pool equal weight
Plus a momentum run over the full 2017-10+ window as the M2 reference.

Usage:
    poetry run python scripts/xsec_gbm_selection.py                # locked run
    poetry run python scripts/xsec_gbm_selection.py --mode tune    # inner CV
    poetry run python scripts/xsec_gbm_selection.py --top-n 5      # variations
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd

from src.data.nasdaq_store import NasdaqDailyStore
from src.selection import IbkrCostModel, SelectionBacktest, SelectionConfig
from src.selection.factors import momentum_12_1

ROOT = Path(__file__).resolve().parents[1]
UNIVERSE_DIR = ROOT / "data" / "universe"
REPORT_PATH = ROOT / "reports" / "xsec_gbm_results.json"

RF_ANNUAL = 0.0434  # same risk-free as src.backtesting metrics

# Old ticker -> current ticker (price data lives under the new symbol).
# WBA (went private 2025) and MON->BAYRY (Nasdaq serves no data) have no
# price data: they are skipped and recorded in the report.
RENAME = {
    "FB": "META", "PCLN": "BKNG", "UTX": "RTX", "RTN": "RTX",
    "BK": "BNY", "DWDP": "DD", "TWX": "T", "CELG": "BMY",
    "MON": "BAYRY", "AGN": "ABBV",
}

# The 13 cross-sectional features (see module docstring / report).
FEATURES = [
    "mom_12_1", "mom_6m", "mom_3m", "mom_1m",
    "vol_60d", "vol_ratio", "mdd_120d",
    "dist_52w_high", "dist_ma20",
    "log_dollar_vol_20d", "dollar_vol_chg",
    "vol_ratio_5_60", "atr_ratio",
]

# ---------------------------------------------------------------------------
# LOCKED hyperparameters — winner of the inner 2017-2020 walk-forward CV
# (config "G_rank_depth4": mean monthly IC +0.0099, top10 excess +2.31%/mo
# over 15 inner test months; see reports/xsec_gbm_results.json ->
# "inner_cv"). Label = monthly cross-sectional RANK of the 21d forward
# return (robust to outlier months). Do NOT retune on post-2020 results.
# ---------------------------------------------------------------------------
LOCKED_PARAMS = {
    "max_depth": 4,
    "num_leaves": 12,
    "learning_rate": 0.05,
    "n_estimators": 300,          # hard cap; early stopping only inside train
    "min_child_samples": 40,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 1,
    "lambda_l1": 0.1,
    "lambda_l2": 1.0,
    "label_transform": "rank",
}

INNER_CV_GRID = [
    {"name": "A_depth3_lr05", "max_depth": 3, "num_leaves": 8, "learning_rate": 0.05, "min_child_samples": 40},
    {"name": "B_depth4_lr05", "max_depth": 4, "num_leaves": 12, "learning_rate": 0.05, "min_child_samples": 40},
    {"name": "C_depth3_lr03", "max_depth": 3, "num_leaves": 8, "learning_rate": 0.03, "min_child_samples": 40},
    {"name": "D_depth2_lr05", "max_depth": 2, "num_leaves": 6, "learning_rate": 0.05, "min_child_samples": 40},
    {"name": "E_depth3_mc20", "max_depth": 3, "num_leaves": 8, "learning_rate": 0.05, "min_child_samples": 20},
    {"name": "F_rank_depth3", "max_depth": 3, "num_leaves": 8, "learning_rate": 0.05, "min_child_samples": 40, "label_transform": "rank"},
    {"name": "G_rank_depth4", "max_depth": 4, "num_leaves": 12, "learning_rate": 0.05, "min_child_samples": 40, "label_transform": "rank"},
    {"name": "H_huber_depth3", "max_depth": 3, "num_leaves": 8, "learning_rate": 0.05, "min_child_samples": 40, "objective": "huber"},
]
GRID_COMMON = {
    "n_estimators": 300,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 1,
    "lambda_l1": 0.1,
    "lambda_l2": 1.0,
}

# Frozen record of the inner 2017-2020 walk-forward CV (15 inner test months
# from 2019-07, first-inner-test chosen by >=24 trainable months). The rank-
# label transform was the clear winner: all plain-L2 configs had NEGATIVE
# mean IC (they overfit the 24-month training window); rank labels fixed it.
INNER_CV_RESULTS = {
    "inner_window": "2017-09 ~ 2020-12 (inner test from 2019-07, 15 months)",
    "results": [
        {"config": "A_depth3_lr05", "mean_monthly_ic": -0.0778, "ic_positive_rate": 0.333, "mean_top10_monthly_excess_pct": 1.471},
        {"config": "B_depth4_lr05", "mean_monthly_ic": -0.0547, "ic_positive_rate": 0.400, "mean_top10_monthly_excess_pct": 1.343},
        {"config": "C_depth3_lr03", "mean_monthly_ic": -0.0638, "ic_positive_rate": 0.400, "mean_top10_monthly_excess_pct": 2.205},
        {"config": "D_depth2_lr05", "mean_monthly_ic": -0.0618, "ic_positive_rate": 0.400, "mean_top10_monthly_excess_pct": 0.970},
        {"config": "E_depth3_mc20", "mean_monthly_ic": -0.0586, "ic_positive_rate": 0.333, "mean_top10_monthly_excess_pct": 1.993},
        {"config": "F_rank_depth3", "mean_monthly_ic": -0.0026, "ic_positive_rate": 0.467, "mean_top10_monthly_excess_pct": 2.681},
        {"config": "G_rank_depth4", "mean_monthly_ic": 0.0099, "ic_positive_rate": 0.400, "mean_top10_monthly_excess_pct": 2.314},
        {"config": "H_huber_depth3", "mean_monthly_ic": -0.0778, "ic_positive_rate": 0.333, "mean_top10_monthly_excess_pct": 1.471},
    ],
    "winner": "G_rank_depth4 (locked as LOCKED_PARAMS)",
}


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_pit_universe() -> tuple[dict[int, list[str]], list[str]]:
    """(year -> renamed symbols, sorted union of all symbols)."""
    pit: dict[int, list[str]] = {}
    for f in sorted(UNIVERSE_DIR.glob("sp100_*.json")):
        if f.name in ("sp100_union.json", "sp100.json"):
            continue
        year = int(f.stem.split("_")[1])
        syms = sorted({RENAME.get(c["symbol"], c["symbol"]) for c in json.loads(f.read_text())})
        pit[year] = syms
    union = sorted(set().union(*pit.values()))
    return pit, union


def load_panel(store: NasdaqDailyStore, symbols: list[str],
               start: str, end: str) -> dict[str, pd.DataFrame]:
    """Daily close/high/low/volume panels (date x symbol).

    Data-hygiene guard: drops dates where <80% of symbols have prices
    (e.g. a partial-refresh day where only a subset of tickers was
    updated — treating those as trading days would falsely value every
    non-updated holding at zero). Uses coverage counts only, no prices.
    """
    frames = {"close": {}, "high": {}, "low": {}, "volume": {}}
    for sym in symbols:
        df = store.get_daily(sym, start, end)
        if df.empty:
            continue
        frames["close"][sym] = df["close"]
        frames["high"][sym] = df["high"]
        frames["low"][sym] = df["low"]
        frames["volume"][sym] = df["volume"]
    panel = {k: pd.DataFrame(v).sort_index() for k, v in frames.items()}
    frac = panel["close"].notna().mean(axis=1)
    bad = frac[frac < 0.8].index
    if len(bad):
        print(f"  [guard] dropping {len(bad)} low-coverage date(s): "
              f"{[str(b.date()) for b in bad]}")
        panel = {k: v.drop(index=bad) for k, v in panel.items()}
    return panel


# ---------------------------------------------------------------------------
# Features (daily, vectorised; sampled at month-end signal dates later)
# ---------------------------------------------------------------------------

def compute_daily_features(panel: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    C, H, L, V = panel["close"], panel["high"], panel["low"], panel["volume"]
    ret = C.pct_change(fill_method=None)
    F: dict[str, pd.DataFrame] = {}

    # returns / momentum
    F["mom_12_1"] = C.shift(21) / C.shift(252) - 1.0     # matches factors.py
    F["mom_6m"] = C / C.shift(126) - 1.0
    F["mom_3m"] = C / C.shift(63) - 1.0
    F["mom_1m"] = C / C.shift(21) - 1.0                  # short-term reversal

    # risk
    F["vol_60d"] = ret.rolling(60).std() * np.sqrt(252.0)
    F["vol_ratio"] = ret.rolling(22).std() / ret.rolling(120).std()
    dd = C / C.rolling(120).max() - 1.0
    F["mdd_120d"] = dd.rolling(120).min()

    # position
    F["dist_52w_high"] = C / C.rolling(252).max() - 1.0
    F["dist_ma20"] = C / C.rolling(20).mean() - 1.0

    # liquidity / volume-price
    dv = C * V
    dv20 = dv.rolling(20).mean()
    F["log_dollar_vol_20d"] = np.log1p(dv20)
    F["dollar_vol_chg"] = dv20 / dv20.shift(60) - 1.0
    F["vol_ratio_5_60"] = V.rolling(5).mean() / V.rolling(60).mean()

    prev_c = C.shift(1)
    tr = np.maximum.reduce(
        [(H - L).values, (H - prev_c).abs().values, (L - prev_c).abs().values]
    )
    tr = pd.DataFrame(tr, index=C.index, columns=C.columns)
    F["atr_ratio"] = tr.rolling(20).mean() / C

    assert set(F.keys()) == set(FEATURES)
    return F


# ---------------------------------------------------------------------------
# Dataset: (signal month, symbol) rows with z-scored features + labels
# ---------------------------------------------------------------------------

def universe_for_date(pit: dict[int, list[str]], day: pd.Timestamp) -> list[str]:
    """Same fallback logic as SelectionBacktest._select."""
    uni = pit.get(day.year)
    if uni is None:
        years = sorted(pit.keys())
        nearby = [y for y in years if y <= day.year]
        uni = pit[nearby[-1]] if nearby else pit[years[0]]
    return uni


def month_end_signal_days(index: pd.DatetimeIndex, start, end) -> list[pd.Timestamp]:
    """Replicates SelectionBacktest's signal-date mapping (last trading day
    of each month within [start, end], inclusive)."""
    tz = index.tz

    def to_ts(x) -> pd.Timestamp:
        ts = pd.Timestamp(x)
        return ts.tz_localize(tz) if ts.tzinfo is None else ts.tz_convert(tz)

    start_ts, end_ts = to_ts(start), to_ts(end)
    days: list[pd.Timestamp] = []
    seen: set = set()
    for sd in pd.date_range(start_ts, end_ts, freq="ME"):
        pos = index.searchsorted(sd, side="right") - 1
        if pos >= 0 and start_ts <= index[pos] <= end_ts and index[pos] not in seen:
            days.append(index[pos])
            seen.add(index[pos])
    return days


def build_dataset(
    panel: dict[str, pd.DataFrame],
    feats: dict[str, pd.DataFrame],
    pit: dict[int, list[str]],
    signal_days: list[pd.Timestamp],
    min_history_bars: int = 260,
    label_bars: int = 21,
) -> pd.DataFrame:
    """One row per (signal date, eligible symbol): z-scored features +
    forward-21d return label (demeaned across the pool)."""
    C = panel["close"]
    index = C.index
    rows: list[dict] = []

    for T in signal_days:
        uni = universe_for_date(pit, T)
        hist = C.loc[index < T]
        counts = hist.notna().sum()
        eligible = [s for s in uni if s in C.columns and counts.get(s, 0) >= min_history_bars]
        if not eligible:
            continue

        # feature day: last trading day strictly before the signal day
        p = index.searchsorted(T) - 1
        if p < 0:
            continue
        P = index[p]

        # execution day and label end
        e = index.searchsorted(T) + 1
        if e >= len(index):
            continue  # no execution day (data ends at signal)
        E = index[e]
        e21 = e + label_bars

        # raw features at P, z-scored across the eligible pool (clip +-3)
        zfeat: dict[str, pd.Series] = {}
        for f in FEATURES:
            s = feats[f].loc[P, eligible].astype(float)
            mu, sd = s.mean(), s.std(ddof=0)
            z = (s - mu) / sd if sd and np.isfinite(sd) and sd > 1e-12 else s * 0.0
            zfeat[f] = z.clip(-3.0, 3.0)

        # label: forward return off the execution close, demeaned
        if e21 < len(index):
            E21 = index[e21]
            fwd = C.loc[E21, eligible].astype(float) / C.loc[E, eligible].astype(float) - 1.0
            label = fwd - fwd.mean()
        else:
            fwd = pd.Series(np.nan, index=eligible)
            label = fwd

        for sym in eligible:
            row = {"date": T, "period": T.year * 12 + T.month - 1, "symbol": sym}
            row["fwd_ret"] = fwd.get(sym, np.nan)
            for f in FEATURES:
                row[f] = zfeat[f].get(sym, np.nan)
            rows.append(row)

    df = pd.DataFrame(rows)
    df[FEATURES + ["fwd_ret"]] = df[FEATURES + ["fwd_ret"]].astype(float)
    return df


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------

def fit_predict(train: pd.DataFrame, test: pd.DataFrame, params: dict, seed: int = 42):
    """Fit LightGBM on `train` (early stopping on the last 12 train months),
    return (predictions aligned to test rows, fitted model or None)."""
    import lightgbm as lgb

    labeled = train[train["fwd_ret"].notna() & train[FEATURES].notna().any(axis=1)]
    if len(labeled) < 200:
        return np.full(len(test), np.nan), None

    y = labeled["fwd_ret"]
    if params.get("label_transform") == "rank":
        # cross-sectional rank (per month) in [-0.5, 0.5]: robust to outlier
        # months (e.g. COVID) and targets the ordering we actually trade on
        y = labeled.groupby("period")["fwd_ret"].rank(pct=True) - 0.5

    valid_cut = labeled["period"].max() - 12
    fit_rows = labeled[labeled["period"] < valid_cut]
    valid_rows = labeled[labeled["period"] >= valid_cut]
    use_es = len(fit_rows) >= 200 and len(valid_rows) >= 200
    if not use_es:
        fit_rows = labeled

    kw = dict(
        random_state=seed,
        n_jobs=4,
        verbose=-1,
        deterministic=True,
        force_row_wise=True,
    )
    model = lgb.LGBMRegressor(**{**params, **kw})
    if use_es:
        model.fit(
            fit_rows[FEATURES], y.loc[fit_rows.index],
            eval_set=[(valid_rows[FEATURES], y.loc[valid_rows.index])],
            callbacks=[lgb.early_stopping(50, verbose=False)],
        )
    else:
        model.fit(fit_rows[FEATURES], y.loc[fit_rows.index])
    if len(test) == 0:
        return np.array([]), model
    preds = model.predict(test[FEATURES])
    return preds, model


def walk_forward(
    data: pd.DataFrame,
    first_test_period: int,
    params: dict,
    embargo: int = 0,
    seed: int = 42,
    verbose: bool = True,
) -> dict:
    """Purged expanding walk-forward. At test period M, trains on rows with
    period <= M-2-embargo (labels fully realised before M's label window)."""
    periods = sorted(data["period"].unique())
    test_periods = [p for p in periods if p >= first_test_period]
    scores_by_date: dict[pd.Timestamp, pd.Series] = {}
    ics: list[dict] = []
    importances: list[np.ndarray] = []

    for M in test_periods:
        test = data[data["period"] == M]
        T = test["date"].iloc[0]
        train = data[data["period"] <= M - 2 - embargo]
        preds, model = fit_predict(train, test, params, seed=seed)
        s = pd.Series(preds, index=test["symbol"].values, name="score")
        s = s[~s.index.duplicated(keep="last")]
        scores_by_date[T] = s

        if model is not None:
            imp = model.booster_.feature_importance(importance_type="gain")
            importances.append(imp / imp.sum() if imp.sum() > 0 else imp * 0.0)

        # informational IC (only when the label is realised in-sample of the data)
        if test["fwd_ret"].notna().sum() >= 20:
            from scipy.stats import spearmanr
            mask = test["fwd_ret"].notna() & ~np.isnan(preds)
            if mask.sum() >= 20:
                ic = spearmanr(preds[mask], test.loc[mask, "fwd_ret"]).statistic
                top10 = s.sort_values(ascending=False).head(10).index
                pool_ret = test.set_index("symbol")["fwd_ret"]
                top10_ex = pool_ret.reindex(top10).mean()
                ics.append({"date": T, "ic": float(ic), "top10_ex": float(top10_ex)})
        if verbose:
            print(f"    test {T.date()}  train_months={train['period'].nunique()}  "
                  f"n_test={len(test)}  preds={int((~s.isna()).sum())}", flush=True)

    imp_share = np.mean(np.array(importances), axis=0) if importances else np.zeros(len(FEATURES))
    return {"scores": scores_by_date, "ics": ics,
            "feature_importance": dict(zip(FEATURES, imp_share.round(4).tolist()))}


def first_testable_period(data: pd.DataFrame, min_train_months: int) -> tuple[int, int]:
    """First test period that has >= min_train_months of trainable (labelled)
    feature months under the purge rule train_period <= M-2."""
    labeled_periods = sorted(data[data["fwd_ret"].notna()]["period"].unique())
    by_period = {p: i for i, p in enumerate(labeled_periods)}
    for M in sorted(data["period"].unique()):
        n_train = sum(1 for p in labeled_periods if p <= M - 2)
        if n_train >= min_train_months:
            return M, n_train
    raise RuntimeError("not enough training months for the requested min-train-months")


# ---------------------------------------------------------------------------
# Strategies & metrics (identical engine for all four)
# ---------------------------------------------------------------------------

def run_engine(closes: pd.DataFrame, universe, factor, start, end,
               top_n: int, cost_model: IbkrCostModel) -> "SelectionBacktest":
    cfg = SelectionConfig(
        universe=universe,
        start_date=start,
        end_date=end,
        rebalance_freq="ME",
        top_n=top_n,
        execution_lag_bars=1,
        min_history_bars=260,
        cost_model=cost_model,
    )
    return SelectionBacktest(closes, cfg, factor=factor).run()


def constant_factor(_: pd.DataFrame, __: pd.Timestamp) -> pd.Series:
    """Placeholder — with top_n >= pool size the engine never calls the
    factor (returns all eligible names, i.e. full-pool equal weight)."""
    return pd.Series(dtype=float)


def lookup_factor(scores_by_date: dict[pd.Timestamp, pd.Series]):
    def factor(_: pd.DataFrame, as_of: pd.Timestamp) -> pd.Series:
        return scores_by_date.get(as_of, pd.Series(dtype=float))
    return factor


def perf_stats(eq: pd.Series, rf_annual: float = RF_ANNUAL) -> dict:
    eq = eq.dropna()
    if len(eq) < 3:
        return {}
    r = eq.pct_change().dropna()
    total = eq.iloc[-1] / eq.iloc[0] - 1.0
    days = max((eq.index[-1] - eq.index[0]).days, 1)
    cagr = (1.0 + total) ** (365.25 / days) - 1.0
    vol = float(r.std() * np.sqrt(252))
    sharpe = float((r - rf_annual / 252).mean() / r.std() * np.sqrt(252)) if r.std() > 0 else 0.0
    dd = float((eq / eq.cummax() - 1.0).min())
    return {
        "total_return_pct": round(total * 100, 2),
        "cagr_pct": round(cagr * 100, 2),
        "ann_vol_pct": round(vol * 100, 2),
        "sharpe": round(sharpe, 3),
        "max_dd_pct": round(dd * 100, 2),
        "calmar": round(cagr / abs(dd), 3) if dd < 0 else None,
    }


def yearly_returns(eq: pd.Series) -> dict[str, float]:
    ye = eq.resample("YE").last()
    ye.iloc[-1] = eq.iloc[-1]  # guard partial last year
    prev = eq.iloc[0]
    out = {}
    for ts, v in ye.items():
        out[str(ts.year)] = round((v / prev - 1.0) * 100, 2)
        prev = v
    return out


def strategy_report(result, equity: pd.Series) -> dict:
    stats = perf_stats(equity)
    n_reb = len(result.holdings)
    avg_turnover = float(np.mean(result.turnover_notional)) if result.turnover_notional else 0.0
    avg_equity = float(equity.mean())
    stats["total_costs"] = round(result.total_costs, 0)
    stats["avg_one_side_turnover"] = round(avg_turnover, 0)
    stats["cost_bps_per_rebalance"] = round(
        result.total_costs / (avg_equity * n_reb) * 10_000, 2) if n_reb else None
    stats["n_rebalances"] = n_reb
    return stats


def jsonable(o):
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating,)):
        return float(o)
    if isinstance(o, pd.Timestamp):
        return str(o.date())
    raise TypeError(repr(o))


# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------

def mode_tune(args, panel, feats, pit, closes) -> None:
    """Inner walk-forward CV on 2017-2020 ONLY -> pick + print the winner."""
    index = closes.index
    signals = month_end_signal_days(index, index[0], "2020-12-31")
    data = build_dataset(panel, feats, pit, signals)
    labeled = data[data["fwd_ret"].notna()]
    print(f"  inner-CV dataset: {len(data)} rows, "
          f"{data['period'].nunique()} months, labeled {len(labeled)}")

    # inner test starts once >= 24 trainable months exist
    first_inner, n_train = first_testable_period(data, min_train_months=24)
    print(f"  first inner test period: {first_inner} (train months: {n_train})")

    results = []
    for cfg in INNER_CV_GRID:
        params = {**GRID_COMMON, **{k: v for k, v in cfg.items() if k != "name"}}
        t0 = time.time()
        wf = walk_forward(data, first_inner, params, embargo=args.embargo,
                          seed=args.seed, verbose=False)
        ics = wf["ics"]
        mean_ic = float(np.mean([x["ic"] for x in ics]))
        hit = float(np.mean([x["ic"] > 0 for x in ics]))
        top10 = float(np.mean([x["top10_ex"] for x in ics]))
        results.append({
            "config": cfg["name"], "params": params,
            "mean_monthly_ic": round(mean_ic, 4),
            "ic_positive_rate": round(hit, 3),
            "mean_top10_monthly_excess_pct": round(top10 * 100, 3),
            "n_test_months": len(ics),
            "seconds": round(time.time() - t0, 1),
        })
        print(f"    {cfg['name']:<14} IC={mean_ic:+.4f}  hit={hit:.0%}  "
              f"top10_ex={top10*100:+.3f}%/mo  ({len(ics)} months, {time.time()-t0:.0f}s)")

    best = max(results, key=lambda r: r["mean_monthly_ic"])
    print("\n  INNER CV WINNER:", best["config"], "->", best["params"])
    print("  Paste these into LOCKED_PARAMS and run the default mode.")
    print("  (tune mode prints only — it does not overwrite the results report)")


def mode_run(args, panel, feats, pit, closes, store_etf) -> None:
    index = closes.index
    data_end = str(index[-1].date())
    signals_all = month_end_signal_days(index, index[0], data_end)
    data = build_dataset(panel, feats, pit, signals_all)
    print(f"  dataset: {len(data)} rows, {data['period'].nunique()} months, "
          f"{data['symbol'].nunique()} symbols")

    first_period, n_train = first_testable_period(data, args.min_train_months)
    if args.first_test:
        first_period = int(pd.Timestamp(args.first_test).to_period("M").ordinal)
    first_test_date = data[data["period"] == first_period]["date"].iloc[0]
    print(f"  walk-forward: first test {first_test_date.date()} "
          f"(min {args.min_train_months} train months), expanding, embargo={args.embargo}")

    print(f"  training {len([p for p in data['period'].unique() if p >= first_period])} test months ...")
    t0 = time.time()
    wf = walk_forward(data, first_period, LOCKED_PARAMS, embargo=args.embargo,
                      seed=args.seed, verbose=True)
    print(f"  walk-forward done in {time.time()-t0:.0f}s")

    # ---- strategies over the identical window ---------------------------
    start = str(first_test_date.date())
    cm = IbkrCostModel() if not args.no_costs else IbkrCostModel(
        commission_per_share=0, min_commission_per_order=0, spread_bps=0)

    gbm_res = run_engine(closes, pit, lookup_factor(wf["scores"]), start, data_end,
                         args.top_n, cm)
    mom_res = run_engine(closes, pit, momentum_12_1, start, data_end, args.top_n, cm)
    eqw_res = run_engine(closes, pit, constant_factor, start, data_end, 10_000, cm)
    # M2 reference: momentum from the earliest possible month (2017-10 exec)
    mom_full_res = run_engine(closes, pit, momentum_12_1, "2017-09-01", data_end,
                              args.top_n, cm)

    # QQQ buy & hold over the same window (total-return approx)
    qqq = store_etf.get_close_series("QQQ", start, data_end)
    qqq = qqq[qqq.index >= gbm_res.equity.index[0]]

    # ---- comparison -----------------------------------------------------
    strategies = {
        "gbm_top10": (gbm_res, gbm_res.equity),
        "momentum_top10": (mom_res, mom_res.equity),
        "equal_weight_pool": (eqw_res, eqw_res.equity),
    }
    window_stats = {name: strategy_report(res, eq) for name, (res, eq) in strategies.items()}
    window_stats["qqq_buy_hold"] = perf_stats(qqq)

    since2021 = {}
    for name, (res, eq) in strategies.items():
        since2021[name] = perf_stats(eq[eq.index >= "2021-01-01"])
    qqq21 = qqq[qqq.index >= "2021-01-01"]
    since2021["qqq_buy_hold"] = perf_stats(qqq21)

    # ---- diagnostics ----------------------------------------------------
    # holdings overlap GBM vs momentum (aligned by execution date)
    mom_hold = {d: set(s) for d, s in mom_res.holdings}
    overlaps, overlap_detail = [], []
    for d, syms in gbm_res.holdings:
        if d in mom_hold:
            ov = len(set(syms) & mom_hold[d]) / max(len(syms), 1)
            overlaps.append(ov)
            overlap_detail.append({"date": str(d.date()), "pct": round(ov * 100, 1)})

    ics = wf["ics"]
    ic_by_year = {}
    for x in ics:
        y = str(x["date"].year)
        ic_by_year.setdefault(y, []).append(x["ic"])
    ic_by_year = {y: round(float(np.mean(v)), 4) for y, v in sorted(ic_by_year.items())}

    yearly = {}
    for name, (res, eq) in strategies.items():
        yearly[name] = yearly_returns(eq)
    yearly["qqq_buy_hold"] = yearly_returns(qqq)

    # skipped / missing universe names (recorded, per task convention)
    pit_union = sorted(set().union(*pit.values()))
    missing = [s for s in pit_union if s not in closes.columns]

    # ---- verdict --------------------------------------------------------
    g, m, q = since2021["gbm_top10"], since2021["momentum_top10"], since2021["qqq_buy_hold"]
    beats_mom = (g["sharpe"] > m["sharpe"]) or (g["total_return_pct"] > m["total_return_pct"])
    beats_qqq = (g["sharpe"] > q["sharpe"]) or (g["total_return_pct"] > q["total_return_pct"])
    if beats_mom and beats_qqq:
        verdict = ("OUTPERFORM: 样本外(2021+) GBM 同时打败动量规则与 QQQ —— 值得接入信号平台")
    elif beats_qqq:
        verdict = ("NO EDGE OVER MOMENTUM: 样本外(2021+) GBM 打败 QQQ 但未打败动量规则 —— "
                   "ML 无增量，动量仍是更好的规则")
    else:
        verdict = ("NEGATIVE: 样本外(2021+) GBM 既未打败动量也未打败 QQQ —— ML 路线关闭")

    # ---- print ----------------------------------------------------------
    print("\n" + "=" * 78)
    print(f"  横截面 GBM 选股  |  Top{args.top_n} 等权 · 月度 · IBKR 成本 · purged walk-forward")
    print(f"  对比窗口: {start} ~ {data_end}   (样本外 2021+ 单列)")
    print("=" * 78)
    for label, stats in (("全测试窗口", window_stats), ("2021-01 起", since2021)):
        print(f"\n  [{label}]  {'':<18}{'总收益%':>9}{'CAGR%':>8}{'Sharpe':>8}{'回撤%':>8}{'成本$':>8}")
        for name, s in stats.items():
            cost = s.get("total_costs", "")
            cost = f"{cost:,.0f}" if cost != "" else ""
            print(f"    {name:<20}{s['total_return_pct']:>9.1f}{s['cagr_pct']:>8.1f}"
                  f"{s['sharpe']:>8.2f}{s['max_dd_pct']:>8.1f}{cost:>8}")
    print(f"\n  动量 M2 参考 (2017-10 起全程): "
          f"{perf_stats(mom_full_res.equity)}")
    print(f"\n  月度 IC 均值: {np.mean([x['ic'] for x in ics]):+.4f}  "
          f"IC>0 占比: {np.mean([x['ic'] > 0 for x in ics]):.0%}")
    print(f"  与动量持仓重合度均值: {np.mean(overlaps)*100:.0f}%")
    fi = sorted(wf["feature_importance"].items(), key=lambda kv: -kv[1])
    print(f"  特征重要度 top5: {fi[:5]}")
    print(f"\n  结论: {verdict}")

    # ---- report ---------------------------------------------------------
    report = {
        "meta": {
            "generated_at": datetime.now(tz=timezone.utc).isoformat(),
            "script": "scripts/xsec_gbm_selection.py",
            "data_range": [str(index[0].date()), data_end],
            "price_data": "Nasdaq daily OHLCV, split-adjusted, dividend-unadjusted (QQQ TR approx +0.6%/yr)",
            "universe": {
                "point_in_time": "data/universe/sp100_YYYY.json (Wikipedia Jan snapshots)",
                "rename_map": RENAME,
                "union_size": len(pit_union),
                "missing_price_data_skipped": missing,
                "late_starters": {s: str(closes[s].dropna().index[0].date())
                                  for s in closes.columns
                                  if closes[s].dropna().index[0] > pd.Timestamp("2017-01-01", tz=index.tz)},
                "min_history_bars": 260,
            },
            "conventions": {
                "signal": "月末最后交易日收盘产生信号，特征仅用严格早于信号日的数据",
                "execution": "信号日次一交易日收盘成交 (execution_lag_bars=1)",
                "label": "未来21个交易日收益 − 当月截面均值 (monthly sampling, 无重叠)",
                "feature_scaling": "每月截面 z-score, clip ±3",
                "purge": "测试月 M 只用 period ≤ M−2 的样本训练（标签窗口全部结束且留1个月隔离带）",
                "walk_forward": "expanding window, 每月重训, 无随机K折",
                "costs": "IBKR Pro: $0.005/股 min $1 + 5bps 半价差 (IbkrCostModel)" if not args.no_costs else "无成本(理想化)",
                "hyperparams_locked": "LOCKED_PARAMS 由 2017-2020 内部 walk-forward CV 选出，未用测试窗表现调参",
                "yearly_returns_note": "2020 年为 11-12 月两个月（首个测试月 2020-10）；2026 年为 1 月~9 月 10 日（部分年度）",
            },
        },
        "hyperparams": LOCKED_PARAMS,
        "inner_cv_2017_2020": INNER_CV_RESULTS,
        "walk_forward": {
            "first_test_date": str(first_test_date.date()),
            "min_train_months": args.min_train_months,
            "embargo_extra_months": args.embargo,
            "n_test_months": len(wf["scores"]),
            "seed": args.seed,
        },
        "comparison_full_test_window": window_stats,
        "comparison_since_2021": since2021,
        "momentum_full_window_m2_reference": {
            "start": str(mom_full_res.holdings[0][0].date()) if mom_full_res.holdings else None,
            **strategy_report(mom_full_res, mom_full_res.equity),
        },
        "yearly_returns_pct": yearly,
        "monthly_ic": {
            "mean": round(float(np.mean([x["ic"] for x in ics])), 4),
            "positive_rate": round(float(np.mean([x["ic"] > 0 for x in ics])), 3),
            "n_months": len(ics),
            "by_year": ic_by_year,
            "mean_top10_monthly_excess_pct": round(
                float(np.mean([x["top10_ex"] for x in ics])) * 100, 3),
        },
        "overlap_with_momentum": {
            "avg_pct": round(float(np.mean(overlaps)) * 100, 1),
            "per_month": overlap_detail,
        },
        "feature_importance_gain_share": wf["feature_importance"],
        "gbm_holdings": [{"date": str(d.date()), "symbols": syms} for d, syms in gbm_res.holdings],
        "verdict_2021_plus": verdict,
    }
    REPORT_PATH.parent.mkdir(exist_ok=True)
    REPORT_PATH.write_text(json.dumps(report, indent=2, default=jsonable))
    print(f"\n  report -> {REPORT_PATH}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    p = argparse.ArgumentParser(description="Cross-sectional GBM selection on S&P 100")
    p.add_argument("--mode", choices=["run", "tune"], default="run",
                   help="run: locked params full walk-forward; tune: inner 2017-2020 CV")
    p.add_argument("--top-n", type=int, default=10)
    p.add_argument("--min-train-months", type=int, default=36)
    p.add_argument("--embargo", type=int, default=0,
                   help="extra purge months beyond the standard 2-month rule")
    p.add_argument("--first-test", type=str, default=None,
                   help="override first test month (YYYY-MM)")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--no-costs", action="store_true")
    p.add_argument("--data-start", type=str, default="2016-01-01")
    args = p.parse_args()

    pit, union = load_pit_universe()
    print(f"  PIT universe: {len(pit)} 年度名单, union {len(union)} 只")

    stocks = NasdaqDailyStore(assetclass="stocks")
    data_end = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    panel = load_panel(stocks, union, args.data_start, data_end)
    closes = panel["close"]
    print(f"  price panel: {closes.shape[0]} days x {closes.shape[1]} symbols "
          f"({str(closes.index[0].date())} ~ {str(closes.index[-1].date())})")

    feats = compute_daily_features(panel)
    print(f"  features: {len(FEATURES)} 个 (每日滚动, 月末采样截面 z-score)")

    if args.mode == "tune":
        mode_tune(args, panel, feats, pit, closes)
    else:
        mode_run(args, panel, feats, pit, closes, NasdaqDailyStore(assetclass="etf"))


if __name__ == "__main__":
    main()
