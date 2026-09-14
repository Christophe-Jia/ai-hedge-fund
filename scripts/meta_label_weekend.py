#!/usr/bin/env python3
"""Meta-labeling feasibility experiment for the weekend_gap signal.

Question: can a simple ML classifier learn to SEPARATE good from bad
weekend-gap events, when the primary signal threshold is relaxed from 5%
to 3% (long side only)?

Design (frozen BEFORE any model was run — zero tuning discipline:
hyperparameters and the feature list below were fixed a priori and are not
adjusted based on results):

  Event set    every weekend 2019-01..2026-09 where BTC Friday close ->
               Sunday close return >= +3% (upside only; the SHORT side was
               shown to lose money unconditionally and is NOT learned).
               50 events at 3%+, 22 at 5%+.

  Label        Monday open (first stock trading day after the Sunday)
               equal-weight long COIN/MSTR/MARA — legs without data that
               day (COIN before 2021-04-14 IPO) are skipped — exit at the
               T+2 trading-day close. Net of 5 bps per side (10 bps round
               trip, same half-spread convention as exit_rules_backtest).
               Label = 1 when net return > 0.

  Features (9, fixed a priori, each with a financial rationale):
    1. weekend_ret_pct   BTC Fri->Sun close return, continuous
                         (signal strength; bigger gap = stronger impetus)
    2. btc_rv30_ann_pct  30d realized vol through Friday, annualized
                         (vol regime: continuation vs blow-off)
    3. btc_ret5d_pct     BTC 5-day return through Friday
                         (already chasing -> crowded / late-cycle)
    4. funding_3d_ann_pct  perp funding 3-day MA at Sunday, annualized
                         (overheated leverage = fragile rally)
    5. mvrv_z180         MVRV z-score over 180d window at Sunday
                         (on-chain valuation stretch)
    6. vix_fri           VIX Friday close
                         (equity fear regime; crypto proxies are high-beta)
    7. days_since_prev   calendar days since the previous 3%+ up event
                         (event clustering: same-news-chain events decay)
    8. dist_30d_high_pct Friday close vs 30d rolling max close
                         (room to run before resistance)
    9. sat_share         share of the weekend move realized by Saturday
                         close: (sat/fri-1)/(sun/fri-1)
                         (slow digestion -> more informed, less noise chase)

  Missing-data policy (funding starts 2021-01, MVRV starts 2019-01):
    median imputation FIT INSIDE each CV fold (sklearn Pipeline), so no
    fold statistics leak into another fold.

  Models (fixed a priori, no tuning):
    logreg  SimpleImputer(median) + StandardScaler + LogisticRegression(C=1)
    rf      SimpleImputer(median) + RandomForestClassifier(
            n_estimators=300, max_depth=3, min_samples_leaf=2, seed=42)

  Validation:
    LOO-CV  leave-one-out out-of-fold probability for every event
            (standard for n < 100)
    time-split  first half of events (chronological) trains, second half
            tests — cross-regime stability check
    Strategy comparison (event-level, net of costs):
            always-take-3%  |  5%-threshold rule  |  ML filter (LR / RF,
            each at prob > 0.5 and > 0.6 entry gates)

Success bar (decided in advance): ML-filtered return/Sharpe must beat BOTH
baselines to conclude "ML adds value". Honest negative results are the
point of this experiment.

Usage:
    poetry run python scripts/meta_label_weekend.py
"""

from __future__ import annotations

import json
import math
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, roc_auc_score
from sklearn.model_selection import LeaveOneOut, cross_val_predict
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from src.data.fred_store import FredSeries

# ---------------------------------------------------------------------------
# Frozen configuration
# ---------------------------------------------------------------------------

DB_PATH = "data/btc_history.db"
ONCHAIN_DB = "data/onchain_metrics.db"
OUT_PATH = "reports/meta_label_results.json"

RELAXED_THR_PCT = 3.0     # primary screen (events)
STRICT_THR_PCT = 5.0      # original-signal baseline subset
TARGETS = ("COIN", "MSTR", "MARA")
COST_BPS_PER_SIDE = 5.0   # 10 bps round trip on notional
EVENT_START = "2019-01-01"
EVENT_END = "2026-09-30"

FEATURES = (
    "weekend_ret_pct",
    "btc_rv30_ann_pct",
    "btc_ret5d_pct",
    "funding_3d_ann_pct",
    "mvrv_z180",
    "vix_fri",
    "days_since_prev",
    "dist_30d_high_pct",
    "sat_share",
)

SEED = 42

def make_logreg() -> Pipeline:
    return Pipeline([
        ("imp", SimpleImputer(strategy="median")),
        ("sc", StandardScaler()),
        ("clf", LogisticRegression(C=1.0, max_iter=1000, random_state=SEED)),
    ])

def make_rf() -> Pipeline:
    return Pipeline([
        ("imp", SimpleImputer(strategy="median")),
        ("clf", RandomForestClassifier(
            n_estimators=300, max_depth=3, min_samples_leaf=2,
            random_state=SEED, n_jobs=-1)),
    ])

MODELS = {"logreg": make_logreg, "rf": make_rf}
ENTRY_GATES = (0.5, 0.6)

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_btc_daily() -> pd.Series:
    con = sqlite3.connect(DB_PATH)
    df = pd.read_sql(
        "SELECT ts, close FROM ohlcv "
        "WHERE symbol='BTC/USDT' AND market_type='spot' AND timeframe='1d'",
        con,
    )
    con.close()
    df["date"] = pd.to_datetime(df["ts"], unit="ms").dt.normalize()
    return df.set_index("date")["close"].sort_index()


def load_stock_daily() -> dict[str, pd.DataFrame]:
    con = sqlite3.connect(DB_PATH)
    out = {}
    for sym in TARGETS:
        df = pd.read_sql(
            "SELECT ts, open, close FROM ohlcv "
            "WHERE symbol=:s AND market_type='stocks' AND timeframe='1d'",
            con, params={"s": sym},
        )
        df["date"] = pd.to_datetime(df["ts"], unit="ms").dt.normalize()
        out[sym] = df.set_index("date").sort_index()
    con.close()
    return out


def load_funding() -> pd.Series:
    """BTC perp funding rate per 8h epoch, index = UTC timestamp."""
    con = sqlite3.connect(DB_PATH)
    df = pd.read_sql(
        "SELECT ts, rate FROM funding_rates WHERE symbol='BTC/USDT:USDT'",
        con,
    )
    con.close()
    df["date"] = pd.to_datetime(df["ts"], unit="ms")
    return df.set_index("date")["rate"].sort_index()


def load_mvrv() -> pd.Series:
    con = sqlite3.connect(ONCHAIN_DB)
    df = pd.read_sql(
        "SELECT ts_ms, value FROM onchain_metrics "
        "WHERE asset='BTC' AND metric='mvrv'",
        con,
    )
    con.close()
    df["date"] = pd.to_datetime(df["ts_ms"], unit="ms").dt.normalize()
    return df.set_index("date")["value"].sort_index()


# ---------------------------------------------------------------------------
# Event construction
# ---------------------------------------------------------------------------

def find_events(btc: pd.Series) -> list[dict]:
    """All weekends with BTC Fri-close -> Sun-close return >= +3%."""
    events = []
    sundays = [d for d in btc.index if d.dayofweek == 6]
    for sun in sundays:
        fri = sun - pd.Timedelta(days=2)
        sat = sun - pd.Timedelta(days=1)
        if fri < pd.Timestamp(EVENT_START) or sun > pd.Timestamp(EVENT_END):
            continue
        if fri not in btc.index:
            continue
        ret = btc[sun] / btc[fri] - 1.0
        if ret * 100.0 < RELAXED_THR_PCT:
            continue
        events.append({
            "friday": fri,
            "saturday": sat,
            "sunday": sun,
            "weekend_ret_pct": round(ret * 100.0, 3),
            "sat_bar_exists": sat in btc.index,
        })
    return events


def build_trade_label(
    event: dict,
    stocks: dict[str, pd.DataFrame],
    calendar: list[pd.Timestamp],
    idx: dict[pd.Timestamp, int],
) -> dict:
    """Entry-day open -> T+2 close, equal weight, net of 10 bps round trip."""
    sunday = event["sunday"]
    later = [d for d in calendar if d > sunday]
    if not later:
        return {"tradable": False, "reason": "no trading day after weekend"}
    entry_day = later[0]
    i0 = idx[entry_day]
    i_exit = min(i0 + 2, len(calendar) - 1)
    # exit must be strictly after entry (T+2), else end-of-data
    if i_exit <= i0:
        return {"tradable": False, "reason": "end_of_data"}

    legs, leg_rets = [], []
    for sym in TARGETS:
        df = stocks[sym]
        if entry_day not in df.index:
            legs.append({"symbol": sym, "skipped": True})
            continue
        entry_px = float(df.loc[entry_day, "open"])
        if not entry_px > 0:
            legs.append({"symbol": sym, "skipped": True})
            continue
        # walk to a traded bar at/before the exit day
        i = i_exit
        while calendar[i] not in df.index and i > i0:
            i -= 1
        exit_px = float(df.loc[calendar[i], "close"])
        gross = exit_px / entry_px - 1.0
        net = gross - 2.0 * COST_BPS_PER_SIDE / 10_000.0
        leg_rets.append(net)
        legs.append({
            "symbol": sym,
            "entry_date": entry_day.date().isoformat(),
            "entry_price": round(entry_px, 4),
            "exit_date": calendar[i].date().isoformat(),
            "exit_price": round(exit_px, 4),
            "ret_net_pct": round(net * 100.0, 3),
        })
    if not leg_rets:
        return {"tradable": False, "reason": "no tradable legs", "legs": legs}
    ret = float(np.mean(leg_rets))
    return {
        "tradable": True,
        "entry_day": entry_day,
        "n_legs": len(leg_rets),
        "ret_net_pct": round(ret * 100.0, 3),
        "label": int(ret > 0),
        "legs": legs,
    }


# ---------------------------------------------------------------------------
# Feature construction (all look-ahead safe: data through Sunday only)
# ---------------------------------------------------------------------------

def build_features(
    events: list[dict],
    btc: pd.Series,
    funding: pd.Series,
    mvrv: pd.Series,
    vix: pd.Series,
) -> None:
    btc_ret = btc.pct_change()
    roll_max30 = btc.rolling(30, min_periods=10).max()

    # MVRV z-score over a trailing 180d window (min 60 obs)
    m_mean = mvrv.rolling(180, min_periods=60).mean()
    m_std = mvrv.rolling(180, min_periods=60).std()
    mvrv_z = (mvrv - m_mean) / m_std

    for k, ev in enumerate(events):
        fri, sat, sun = ev["friday"], ev["saturday"], ev["sunday"]

        # 1. weekend return (continuous)
        f1 = ev["weekend_ret_pct"]

        # 2. 30d realized vol through Friday, annualized (crypto: 365d/yr)
        hist = btc_ret.loc[:fri].iloc[-30:]
        f2 = float(hist.std() * math.sqrt(365.0) * 100.0) if len(hist) >= 15 else np.nan

        # 3. BTC 5-day return through Friday
        h5 = btc.loc[:fri].iloc[-6:]
        f3 = float(h5.iloc[-1] / h5.iloc[0] - 1.0) * 100.0 if len(h5) >= 6 else np.nan

        # 4. funding 3-day MA (last 9 epochs of 8h) through Sunday, annualized
        fr = funding.loc[:sun].iloc[-9:]
        f4 = float(fr.mean() * 3.0 * 365.0 * 100.0) if len(fr) >= 9 else np.nan

        # 5. MVRV z-score at the last observation <= Sunday
        mz = mvrv_z.loc[:sun]
        f5 = float(mz.iloc[-1]) if len(mz) else np.nan

        # 6. VIX at the last close <= Friday
        vv = vix.loc[:fri]
        f6 = float(vv.iloc[-1]) if len(vv) else np.nan

        # 7. days since the previous 3%+ up event
        f7 = (
            (ev["sunday"] - events[k - 1]["sunday"]).days
            if k > 0 else np.nan
        )

        # 8. Friday close distance to the 30d rolling high
        rm = roll_max30.loc[:fri]
        f8 = float(btc[fri] / rm.iloc[-1] - 1.0) * 100.0 if len(rm) else np.nan

        # 9. share of the weekend move realized by Saturday close
        if ev["sat_bar_exists"] and ev["weekend_ret_pct"] > 0:
            sat_ret = btc[sat] / btc[fri] - 1.0
            f9 = float(sat_ret / (btc[sun] / btc[fri] - 1.0))
        else:
            f9 = np.nan

        ev["features"] = {
            "weekend_ret_pct": round(f1, 3),
            "btc_rv30_ann_pct": None if pd.isna(f2) else round(f2, 2),
            "btc_ret5d_pct": None if pd.isna(f3) else round(f3, 3),
            "funding_3d_ann_pct": None if pd.isna(f4) else round(f4, 3),
            "mvrv_z180": None if pd.isna(f5) else round(f5, 3),
            "vix_fri": None if pd.isna(f6) else round(f6, 2),
            "days_since_prev": None if pd.isna(f7) else float(f7),
            "dist_30d_high_pct": None if pd.isna(f8) else round(f8, 3),
            "sat_share": None if pd.isna(f9) else round(f9, 3),
        }


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------

def event_metrics(rets: list[float], dates: list[pd.Timestamp], label: str) -> dict:
    """Event-level performance stats (per-trade returns in %)."""
    if not rets:
        return {"name": label, "n_trades": 0}
    s = pd.Series(rets, dtype=float)
    years = max((max(dates) - min(dates)).days / 365.25, 1e-9)
    per_year = len(s) / years
    std = float(s.std(ddof=1)) if len(s) > 1 else 0.0
    tstat = float(s.mean() / (std / math.sqrt(len(s)))) if std > 0 and len(s) > 1 else None
    sharpe = float(s.mean() / std * math.sqrt(per_year)) if std > 0 and len(s) > 1 else None
    return {
        "name": label,
        "n_trades": len(s),
        "win_rate_pct": round(100.0 * (s > 0).mean(), 1),
        "avg_ret_pct": round(float(s.mean()), 3),
        "median_ret_pct": round(float(s.median()), 3),
        "total_ret_pct_sum": round(float(s.sum()), 2),
        "std_pct": round(std, 3),
        "t_stat": None if tstat is None else round(tstat, 2),
        "sharpe_annualized": None if sharpe is None else round(sharpe, 2),
        "trades_per_year": round(per_year, 2),
        "worst_ret_pct": round(float(s.min()), 2),
        "best_ret_pct": round(float(s.max()), 2),
    }


def strategy_table(
    events: list[dict],
    probs: dict[str, np.ndarray] | None,
) -> list[dict]:
    """ML-filtered strategies vs the two baselines."""
    rets_all, dates_all = [], []
    rets_5, dates_5 = [], []
    for ev in events:
        rets_all.append(ev["ret_net_pct"])
        dates_all.append(ev["entry_day"])
        if ev["weekend_ret_pct"] >= STRICT_THR_PCT:
            rets_5.append(ev["ret_net_pct"])
            dates_5.append(ev["entry_day"])

    rows = [event_metrics(rets_all, dates_all, "always_take_3pct"),
            event_metrics(rets_5, dates_5, "rule_5pct_threshold")]

    if probs is not None:
        for mname, p in probs.items():
            for gate in ENTRY_GATES:
                sel = [(ev, pi) for ev, pi in zip(events, p) if pi > gate]
                rows.append(event_metrics(
                    [ev["ret_net_pct"] for ev, _ in sel],
                    [ev["entry_day"] for ev, _ in sel],
                    f"ml_{mname}_p{str(gate).replace('.', '')}",
                ))
    return rows


def main() -> None:
    print("=" * 96)
    print("  weekend_gap meta-labeling 试探实验（3% 放宽口径，LOO-CV，零调参）")
    print("=" * 96)

    # ---- data ----
    btc = load_btc_daily()
    stocks = load_stock_daily()
    funding = load_funding()
    mvrv = load_mvrv()
    vix = FredSeries().get("VIXCLS")
    vix.index = pd.to_datetime(vix.index).normalize()

    calendar = sorted({d for df in stocks.values() for d in df.index})
    idx = {d: i for i, d in enumerate(calendar)}

    # ---- events + labels ----
    events = find_events(btc)
    dropped = []
    kept = []
    for ev in events:
        tr = build_trade_label(ev, stocks, calendar, idx)
        if tr["tradable"]:
            ev.update(tr)
            kept.append(ev)
        else:
            dropped.append({"sunday": ev["sunday"].date().isoformat(),
                            "reason": tr["reason"]})
    events = kept
    build_features(events, btc, funding, mvrv, vix)

    n = len(events)
    n_pos = sum(ev["label"] for ev in events)
    print(f"\n事件: {n} 个 3%+ 上涨周末 ({events[0]['sunday'].date()} ~ "
          f"{events[-1]['sunday'].date()}), 正类 {n_pos} ({100.0*n_pos/n:.1f}%)"
          + (f", 剔除 {len(dropped)}" if dropped else ""))

    # ---- design matrices ----
    X = pd.DataFrame(
        [[ev["features"][f] for f in FEATURES] for ev in events],
        columns=FEATURES, dtype=float,
    )
    y = np.array([ev["label"] for ev in events])

    nan_share = X.isna().mean()
    if (nan_share > 0).any():
        print("特征缺失率（fold 内中位数填补）:")
        for f, v in nan_share[nan_share > 0].items():
            print(f"    {f:20s} {v*100:.0f}%")

    # ---- LOO-CV ----
    print("\n留一交叉验证 (LOO-CV):")
    loo_probs = {}
    loo_scores = {}
    for mname, mk in MODELS.items():
        p = cross_val_predict(mk(), X, y, cv=LeaveOneOut(),
                              method="predict_proba")[:, 1]
        pred = (p > 0.5).astype(int)
        auc = roc_auc_score(y, p)
        acc = accuracy_score(y, pred)
        loo_probs[mname] = p
        loo_scores[mname] = {"auc": round(auc, 3), "accuracy": round(acc, 3)}
        print(f"    {mname:7s} AUC {auc:.3f}  acc {acc:.3f}")
        if len(np.unique(y)) < 2:
            loo_scores[mname]["note"] = "degenerate single-class"

    # ---- time split (first half trains, second half tests) ----
    half = n // 2
    X_tr, y_tr = X.iloc[:half], y[:half]
    X_te, y_te = X.iloc[half:], y[half:]
    print(f"\n时间切分: 训练 {events[0]['sunday'].date()}~{events[half-1]['sunday'].date()}"
          f" ({half}) -> 测试 {events[half]['sunday'].date()}~{events[-1]['sunday'].date()} ({n-half})")
    ts_scores = {"train_n": half, "test_n": n - half}
    for mname, mk in MODELS.items():
        m = mk().fit(X_tr, y_tr)
        p = m.predict_proba(X_te)[:, 1]
        rec = {"test_auc": None, "test_accuracy": None}
        if len(np.unique(y_te)) == 2:
            rec["test_auc"] = round(roc_auc_score(y_te, p), 3)
        rec["test_accuracy"] = round(accuracy_score(y_te, (p > 0.5).astype(int)), 3)
        ts_scores[mname] = rec
        print(f"    {mname:7s} AUC {rec['test_auc']}  acc {rec['test_accuracy']}")

    # ---- strategy comparison (LOO probabilities = honest OOF) ----
    print("\n策略对比（事件级，净成本后）:")
    rows = strategy_table(events, loo_probs)
    hdr = (f"  {'策略':22s} {'笔数':>4s} {'胜率%':>6s} {'均笔%':>7s} {'中位%':>7s} "
           f"{'累计%':>8s} {'t值':>6s} {'Sharpe':>7s} {'最差%':>7s}")
    print(hdr)
    for r in rows:
        if r["n_trades"] == 0:
            print(f"  {r['name']:22s}    0")
            continue
        print(f"  {r['name']:22s} {r['n_trades']:>4d} {r['win_rate_pct']:>6.1f} "
              f"{r['avg_ret_pct']:>+7.3f} {r['median_ret_pct']:>+7.3f} "
              f"{r['total_ret_pct_sum']:>+8.1f} "
              f"{('%+.2f' % r['t_stat']) if r['t_stat'] is not None else '   -':>6s} "
              f"{('%.2f' % r['sharpe_annualized']) if r['sharpe_annualized'] is not None else '-':>7s} "
              f"{r['worst_ret_pct']:>+7.2f}")

    # ---- feature importance (in-sample diagnostic only) ----
    imp = {}
    lr_full = make_logreg().fit(X, y)
    coefs = lr_full.named_steps["clf"].coef_[0]
    imp["logreg_std_coefs"] = {
        f: round(float(c), 3) for f, c in zip(FEATURES, coefs)}
    rf_full = make_rf().fit(X, y)
    imp["rf_importances"] = {
        f: round(float(v), 3)
        for f, v in zip(FEATURES, rf_full.named_steps["clf"].feature_importances_)}
    print("\n特征重要度（全样本拟合，仅诊断）:")
    for f, c in sorted(zip(FEATURES, coefs), key=lambda t: -abs(t[1])):
        print(f"    logreg {f:20s} {c:+.3f}   rf {imp['rf_importances'][f]:.3f}")

    # ---- univariate AUC per feature (diagnostic only, not used for trading) ----
    uni = {}
    for f in FEATURES:
        m = X[f].notna()
        if m.sum() >= 10 and len(np.unique(y[m.values])) == 2:
            uni[f] = round(roc_auc_score(y[m.values], X.loc[m, f]), 3)
    print("\n单特征 AUC（方向不定，仅诊断）:")
    for f, a in sorted(uni.items(), key=lambda t: -max(t[1], 1 - t[1])):
        print(f"    {f:20s} {a:.3f}")

    # ---- report ----
    events_out = []
    for ev, plr, prf in zip(events, loo_probs["logreg"], loo_probs["rf"]):
        events_out.append({
            "friday": ev["friday"].date().isoformat(),
            "sunday": ev["sunday"].date().isoformat(),
            "entry_date": ev["entry_day"].date().isoformat(),
            "weekend_ret_pct": ev["weekend_ret_pct"],
            "in_5pct_rule": bool(ev["weekend_ret_pct"] >= STRICT_THR_PCT),
            "n_legs": ev["n_legs"],
            "ret_net_pct": ev["ret_net_pct"],
            "label": ev["label"],
            "loo_prob_logreg": round(float(plr), 3),
            "loo_prob_rf": round(float(prf), 3),
            "features": ev["features"],
            "legs": ev["legs"],
        })

    report = {
        "config": {
            "experiment": "meta-labeling feasibility, weekend_gap relaxed to 3% (long only)",
            "event_set": f"BTC Fri close -> Sun close ret >= +{RELAXED_THR_PCT}% "
                         f"({EVENT_START}..{EVENT_END})",
            "label": "equal-weight long COIN/MSTR/MARA at Monday open -> "
                     "T+2 trading-day close, net of 5bps/side (10bps round trip); 1 = ret > 0",
            "features": list(FEATURES) + ["(frozen a priori, see script docstring for rationale)"],
            "models": {
                "logreg": "median impute + standardize + LogisticRegression(C=1)",
                "rf": "median impute + RandomForest(300 trees, depth 3, leaf>=2)",
            },
            "validation": "LOO-CV primary; chronological first-half/second-half split secondary",
            "zero_tuning": "features, hyperparameters, and entry gates (0.5/0.6) fixed before any run",
            "data_notes": {
                "funding_coverage": "2021-01-01 onward; earlier events imputed (fold-median)",
                "mvrv_coverage": "2019-01-01 onward; z180 needs 60d so early-2019 imputed",
                "vix": "FRED VIXCLS, Friday close",
                "dropped_events": dropped,
            },
        },
        "n_events": n,
        "n_positive": int(n_pos),
        "positive_rate": round(n_pos / n, 3),
        "loo_cv": loo_scores,
        "time_split": ts_scores,
        "strategies": rows,
        "feature_importance_insample": imp,
        "univariate_auc": uni,
        "events": events_out,
    }

    out = Path(OUT_PATH)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    print(f"\nJSON 已写入: {out}")


if __name__ == "__main__":
    main()
