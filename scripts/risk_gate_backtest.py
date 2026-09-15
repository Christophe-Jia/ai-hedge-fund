#!/usr/bin/env python3
"""Layer-3 risk gates: exposure switches on the GBM+momentum 50/50 blend.

The GBM portfolio is currently always 100% invested (layer 1 = stock picking,
layer 2 = entry/exit execution). This script tests layer 3 — mechanisms that
decide the OVERALL equity exposure (or blend tilt) for the coming month.
Three candidates, one unified validation protocol:

  Gate 1  VIX percentile gate (FRED VIXCLS daily)
          VIX in its rolling 3y percentile > threshold -> next month's blend
          exposure cut (rest in cash at rf). Variants: pct 70/80/90 x
          exposure 50%/75%.
  Gate 2  Momentum-regime tilt (evidence: reports/gbm_attribution.json —
          GBM's underperformance = momentum style cycle, beta=-0.48 R2=0.61)
          Cross-sectional momentum factor's trailing-6m return z > +thr ->
          tilt blend to momentum 70 / GBM 30; z < -thr -> GBM 70 / momentum
          30 (GBM is defensive in momentum crashes); else 50/50.
          Variants: thr 0.8/1.0/1.5.
  Gate 3  Onchain valuation gate (BTC MVRV, data/onchain_metrics.db, 2019+)
          MVRV 180d z > thr (crypto valuations overheated -> contagion risk
          for crypto-adjacent equities) -> blend exposure cut.
          Variants: z 1.0/1.5/2.0 x exposure 70%/50%.

Baseline: ungated 50/50 monthly blend over the full test window
(2020-10 signal -> 2020-11 first exec month), same convention as
gbm_attribution.json -> gbm_mom_cormove_and_blend (monthly rebalanced,
cost-free blend of the two ENGINE equity curves which already include IBKR
costs; gated-off capital earns rf=4.34%/yr).

Anti-look-ahead timing: every gate decides at month-end signal day T_k using
only data available strictly before T_k; the decision applies to the exec
window E_k -> E_{k+1} (E = next trading day after T). The momentum factor
return for signal month T_j is only realised ~E_{j+1}, so gate 2 uses factor
months with j <= k-2 (one extra month of lag).

Metrics per variant vs baseline: monthly Sharpe, max drawdown (monthly
compounding), annualised return (the cost of the gate), trigger frequency,
calendar-2022 performance, two half-window era splits, and parameter
neighbourhood consistency.

PASS line (fixed a priori): full-window Sharpe improvement >= +0.05 AND max-
drawdown improvement >= 3pp AND both half-windows directionally improved.
Otherwise the honest answer is "no gate — stay 100% equities".

Usage:
    poetry run python scripts/risk_gate_backtest.py
"""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Load the locked selection script READ-ONLY (same pattern as
# scripts/gbm_attribution.py) — reuse its loaders / features / engine.
_SPEC = importlib.util.spec_from_file_location(
    "xsec_gbm_selection", ROOT / "scripts" / "xsec_gbm_selection.py")
xgbm = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(xgbm)

import numpy as np
import pandas as pd

from src.data.fred_store import FredSeries
from src.data.nasdaq_store import NasdaqDailyStore
from src.data.onchain_store import OnchainMetricStore
from src.selection import IbkrCostModel
from src.selection.factors import momentum_12_1

REPORT_OUT = ROOT / "reports" / "risk_gate.json"
SOURCE_REPORT = ROOT / "reports" / "xsec_gbm_results.json"
RF_ANNUAL = 0.0434
RF_M = RF_ANNUAL / 12.0
TOP_N = 10
SEED = 42
MIN_TRAIN_MONTHS = 36
DAY_MS = 86_400_000

VIX_WINDOW_DAYS = 756          # rolling 3y of trading days
MVRV_LOOKBACK_DAYS = 180       # same as src/signals/onchain_fundamental.py
MOM_FACTOR_LAG = 2             # factor months usable at decision: j <= k-2
MOM_TRAILING = 6               # trailing months for the factor z

# gate variants
VIX_VARIANTS = [(70.0, 0.50), (70.0, 0.75), (80.0, 0.50), (80.0, 0.75),
                (90.0, 0.50), (90.0, 0.75)]        # (pct threshold, exposure when triggered)
MOM_VARIANTS = [0.8, 1.0, 1.5]                     # |z| threshold, tilt 70/30
MVRV_VARIANTS = [(1.0, 0.70), (1.0, 0.50), (1.5, 0.70), (1.5, 0.50),
                 (2.0, 0.70), (2.0, 0.50)]         # (z threshold, exposure when triggered)

PASS_DSHARPE = 0.05
PASS_DMDD_PP = 3.0


def jsonable(o):
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating,)):
        return float(o)
    if isinstance(o, pd.Timestamp):
        return str(o.date())
    raise TypeError(repr(o))


def r4(x):
    return None if x is None or not np.isfinite(x) else round(float(x), 4)


# ---------------------------------------------------------------------------
# 0. Rebuild the production pipeline (identical to gbm_attribution.py)
# ---------------------------------------------------------------------------

def rebuild():
    pit, union = xgbm.load_pit_universe()
    stocks = NasdaqDailyStore(assetclass="stocks")
    data_end = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    panel = xgbm.load_panel(stocks, union, "2016-01-01", data_end)
    closes = panel["close"]
    feats = xgbm.compute_daily_features(panel)
    signals = xgbm.month_end_signal_days(closes.index, closes.index[0], data_end)
    data = xgbm.build_dataset(panel, feats, pit, signals)
    return pit, closes, data, data_end


def momentum_factor_series(data: pd.DataFrame) -> pd.Series:
    """Monthly cross-sectional momentum factor return per signal date
    (top10 by 12-1 mom minus bottom10, pool-relative fwd returns) —
    identical to gbm_attribution.momentum_regime's mom_spread."""
    rows = {}
    for T, g in data[data["fwd_ret"].notna()].groupby("date"):
        if len(g) < 40:
            continue
        mom = g.sort_values("mom_12_1", ascending=False)
        rows[T] = mom["fwd_ret"].head(10).mean() - mom["fwd_ret"].tail(10).mean()
    return pd.Series(rows).sort_index()


# ---------------------------------------------------------------------------
# Gate decision inputs (all strictly pre-signal information)
# ---------------------------------------------------------------------------

def vix_percentile(vix: pd.Series, T: pd.Timestamp, window_days: int = VIX_WINDOW_DAYS) -> float | None:
    """Percentile (0-100) of the latest VIX close <= T within the trailing
    3y daily window ending at that observation."""
    tz = vix.index.tz
    T = pd.Timestamp(T)
    T = T.tz_localize(tz) if T.tzinfo is None else T.tz_convert(tz)
    hist = vix[vix.index <= T]
    if len(hist) < window_days // 2:
        return None
    win = hist.tail(window_days)
    v = win.iloc[-1]
    return float((win <= v).mean() * 100.0)


def mvrv_z(mvrv: pd.Series, T: pd.Timestamp, lookback: int = MVRV_LOOKBACK_DAYS) -> float | None:
    """z-score of the latest MVRV daily value dated strictly before T vs the
    trailing `lookback` daily values (same statistic as
    src/signals/onchain_fundamental.py)."""
    tz = mvrv.index.tz
    T = pd.Timestamp(T)
    T = T.tz_localize(tz) if T.tzinfo is None else T.tz_convert(tz)
    hist = mvrv[mvrv.index < T]          # strictly before the signal day
    if len(hist) < lookback:
        return None
    win = hist.tail(lookback).astype(float)
    sd = win.std()
    if not np.isfinite(sd) or sd <= 1e-12:
        return None
    return float((win.iloc[-1] - win.mean()) / sd)


def mom_factor_z(factors: pd.Series, k: int) -> float | None:
    """z of the momentum factor's trailing-6m mean return at decision month k.

    `factors` is indexed by signal-month position (0-based, aligned with the
    signal/exec lists). Factor month j is realised ~E_{j+1} > T_k for j >= k-1,
    so only months 0..k-2 are usable. z standardises the trailing-6m mean
    against the expanding history of trailing-6m means computable from the
    same usable data."""
    avail = factors.iloc[:max(k - MOM_FACTOR_LAG + 1, 0)].dropna()
    if len(avail) < MOM_TRAILING + 6:      # need 6m trailing + >=6 historical 6m means
        return None
    roll = avail.rolling(MOM_TRAILING).mean().dropna()
    if len(roll) < 6 or roll.std() <= 1e-12:
        return None
    return float((roll.iloc[-1] - roll.mean()) / roll.std())


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def monthly_metrics(r: pd.Series) -> dict:
    r = r.dropna()
    n = len(r)
    if n < 3:
        return {}
    eq = (1.0 + r).cumprod()
    ann = float(eq.iloc[-1] ** (12.0 / n) - 1.0)
    sd = float(r.std())
    sharpe = float((r - RF_M).mean() / sd * np.sqrt(12.0)) if sd > 0 else 0.0
    mdd = float((eq / eq.cummax() - 1.0).min())
    return {"n_months": n, "ann_ret_pct": round(ann * 100, 2),
            "sharpe": round(sharpe, 4), "max_dd_pct": round(mdd * 100, 2)}


def year_return(r: pd.Series, year: int) -> float | None:
    ry = r[r.index.year == year]
    if len(ry) < 3:
        return None
    return float((1.0 + ry).prod() - 1.0)


def evaluate_variant(monthly: pd.Series, baseline: pd.Series,
                     active: pd.Series) -> dict:
    """monthly = gated blend returns; active = bool series (gate triggered)."""
    n = len(monthly)
    half = n // 2
    full = monthly_metrics(monthly)
    h1 = monthly_metrics(monthly.iloc[:half])
    h2 = monthly_metrics(monthly.iloc[half:])
    base = monthly_metrics(baseline)
    b1 = monthly_metrics(baseline.iloc[:half])
    b2 = monthly_metrics(baseline.iloc[half:])
    y22 = year_return(monthly, 2022)
    y22b = year_return(baseline, 2022)
    d = {
        "full": full,
        "half1": h1, "half2": h2,
        "vs_baseline": {
            "d_sharpe": r4(full["sharpe"] - base["sharpe"]),
            "d_mdd_pp": r4(base["max_dd_pct"] - full["max_dd_pct"]),  # + = shallower drawdown
            "return_cost_pp": r4(base["ann_ret_pct"] - full["ann_ret_pct"]),
            "half1_d_sharpe": r4(h1["sharpe"] - b1["sharpe"]) if h1 and b1 else None,
            "half2_d_sharpe": r4(h2["sharpe"] - b2["sharpe"]) if h2 and b2 else None,
            "half1_d_mdd_pp": r4(b1["max_dd_pct"] - h1["max_dd_pct"]) if h1 and b1 else None,
            "half2_d_mdd_pp": r4(b2["max_dd_pct"] - h2["max_dd_pct"]) if h2 and b2 else None,
        },
        "ret_2022_pct": r4(y22 * 100) if y22 is not None else None,
        "baseline_2022_pct": r4(y22b * 100) if y22b is not None else None,
        "triggers": {
            "n_active": int(active.sum()), "n_months": n,
            "pct": round(float(active.mean() * 100), 1),
            "n_active_2022": int(active[active.index.year == 2022].sum()),
            "active_months": [str(t.date()) for t in active[active].index],
        },
    }
    vb = d["vs_baseline"]
    d["passes"] = bool(
        vb["d_sharpe"] is not None and vb["d_sharpe"] >= PASS_DSHARPE
        and vb["d_mdd_pp"] is not None and vb["d_mdd_pp"] >= PASS_DMDD_PP
        and vb["half1_d_sharpe"] is not None and vb["half1_d_sharpe"] > 0
        and vb["half2_d_sharpe"] is not None and vb["half2_d_sharpe"] > 0
        and vb["half1_d_mdd_pp"] is not None and vb["half1_d_mdd_pp"] > 0
        and vb["half2_d_mdd_pp"] is not None and vb["half2_d_mdd_pp"] > 0
    )
    return d


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("== rebuilding production pipeline (LOCKED_PARAMS, seed 42) ==", flush=True)
    pit, closes, data, data_end = rebuild()
    first_period, _ = xgbm.first_testable_period(data, MIN_TRAIN_MONTHS)
    first_test_date = data[data["period"] == first_period]["date"].iloc[0]
    print(f"  first test month: {first_test_date.date()}", flush=True)

    print("== walk-forward (identical to production) ==", flush=True)
    wf = xgbm.walk_forward(data, first_period, xgbm.LOCKED_PARAMS,
                           seed=SEED, verbose=False)

    start = str(first_test_date.date())
    cm = IbkrCostModel()
    gbm_res = xgbm.run_engine(closes, pit, xgbm.lookup_factor(wf["scores"]),
                              start, data_end, TOP_N, cm)
    mom_res = xgbm.run_engine(closes, pit, momentum_12_1, start, data_end, TOP_N, cm)

    # ---- verification against the stored report --------------------------
    src = json.loads(SOURCE_REPORT.read_text())
    src_hold = {h["date"]: set(h["symbols"]) for h in src["gbm_holdings"]}
    new_hold = {str(d.date()): set(s) for d, s in gbm_res.holdings}
    mism = [k for k in src_hold if k in new_hold and src_hold[k] != new_hold[k]]
    verification = {
        "holdings_months_matching_source": len(src_hold) - len(mism),
        "holdings_months_in_source": len(src_hold),
        "mismatched_months": mism[:5],
        "gbm_total_return_pct": r4(xgbm.perf_stats(gbm_res.equity)["total_return_pct"]),
        "source_gbm_total_return_pct": src["comparison_full_test_window"]["gbm_top10"]["total_return_pct"],
    }
    print(f"  verification: {verification}", flush=True)

    # ---- monthly exec-window returns (both engines, identical windows) ---
    gbm_hold = {d: set(s) for d, s in gbm_res.holdings}
    assert set(gbm_hold) == {d for d, _ in mom_res.holdings}
    exec_dates = sorted(gbm_hold)
    eq_g, eq_m = gbm_res.equity, mom_res.equity
    index = closes.index

    # signal date for each exec date (exec = next trading day after signal)
    sig_of_exec = {}
    exec_of_sig = {}
    for T in sorted(set(data["date"])):
        e = index.searchsorted(T) + 1
        if e < len(index):
            exec_of_sig[T] = index[e]
            sig_of_exec[index[e]] = T

    rows = []
    for i, d in enumerate(exec_dates):
        d2 = exec_dates[i + 1] if i + 1 < len(exec_dates) else eq_g.index[-1]
        if d in eq_g.index and d2 in eq_g.index and d2 > d:
            rows.append({"exec_date": d, "signal_date": sig_of_exec.get(d),
                         "gbm": eq_g[d2] / eq_g[d] - 1.0,
                         "mom": eq_m[d2] / eq_m[d] - 1.0})
    xw = pd.DataFrame(rows).set_index("exec_date")
    assert xw["signal_date"].notna().all()
    n = len(xw)
    print(f"  exec-window months: {n} "
          f"({str(xw.index[0].date())} .. {str(xw.index[-1].date())})", flush=True)

    baseline = (0.5 * xw["gbm"] + 0.5 * xw["mom"]).rename("blend5050")
    base_stats = monthly_metrics(baseline)
    base_2022 = year_return(baseline, 2022)
    print(f"  baseline 50/50: Sharpe {base_stats['sharpe']}  "
          f"MDD {base_stats['max_dd_pct']}%  ann {base_stats['ann_ret_pct']}%  "
          f"2022 {r4(base_2022*100) if base_2022 is not None else None}%", flush=True)

    # ---- gate inputs ------------------------------------------------------
    vix = FredSeries().get("VIXCLS")
    print(f"  VIX: {len(vix)} obs ({str(vix.index[0].date())} .. {str(vix.index[-1].date())})",
          flush=True)

    store = OnchainMetricStore()
    end_ms = int(pd.Timestamp(data_end, tz=timezone.utc).value // 1_000_000)
    mv_rows = store.get_metrics("BTC", ["mvrv"], 0, end_ms)
    mvrv = pd.Series(
        {pd.Timestamp(r["ts_ms"], unit="ms", tz="UTC"): r["value"] for r in mv_rows}
    ).sort_index()
    # collapse to one value per day (store may hold intraday points)
    mvrv = mvrv.groupby(mvrv.index.floor("D")).last()
    print(f"  MVRV: {len(mvrv)} daily obs ({str(mvrv.index[0].date())} .. "
          f"{str(mvrv.index[-1].date())})", flush=True)

    factors = momentum_factor_series(data)
    # align factor series to the decision-month positions (indexed by signal
    # position among exec months, NOT by the full 2017+ history)
    sig_list = list(xw["signal_date"])
    fpos = pd.Series(
        {i: factors.get(T, np.nan) for i, T in enumerate(sig_list)}, dtype=float)
    print(f"  momentum factor: {factors.notna().sum()} months "
          f"({str(factors.index[0].date())} .. {str(factors.index[-1].date())})",
          flush=True)

    # precompute gate inputs per decision month
    vix_pct = pd.Series([vix_percentile(vix, T) for T in sig_list], index=xw.index)
    mv_z = pd.Series([mvrv_z(mvrv, T) for T in sig_list], index=xw.index)
    mf_z = pd.Series([mom_factor_z(fpos, k) for k in range(n)], index=xw.index)
    print(f"  gate inputs available: VIX {int(vix_pct.notna().sum())}/{n}, "
          f"MVRV {int(mv_z.notna().sum())}/{n}, momFactor {int(mf_z.notna().sum())}/{n}",
          flush=True)

    # ---- Gate 1: VIX percentile ------------------------------------------
    gate1 = {}
    for thr, expo in VIX_VARIANTS:
        active = vix_pct.notna() & (vix_pct > thr)
        monthly = expo * baseline + (1.0 - expo) * RF_M
        monthly[~active] = baseline[~active]
        gate1[f"pct{thr:g}_expo{expo:g}"] = evaluate_variant(monthly, baseline, active)

    # ---- Gate 2: momentum-regime tilt -------------------------------------
    gate2 = {}
    for thr in MOM_VARIANTS:
        w_g = pd.Series(0.5, index=xw.index)
        w_m = pd.Series(0.5, index=xw.index)
        up = mf_z.notna() & (mf_z > thr)
        dn = mf_z.notna() & (mf_z < -thr)
        w_g[up], w_m[up] = 0.3, 0.7     # momentum running hot -> favour momentum
        w_g[dn], w_m[dn] = 0.7, 0.3     # momentum crashing -> GBM defensive
        active = up | dn
        monthly = w_g * xw["gbm"] + w_m * xw["mom"]
        gate2[f"z{thr:g}"] = evaluate_variant(monthly, baseline, active)

    # ---- Gate 3: MVRV valuation -------------------------------------------
    gate3 = {}
    for thr, expo in MVRV_VARIANTS:
        active = mv_z.notna() & (mv_z > thr)
        monthly = expo * baseline + (1.0 - expo) * RF_M
        monthly[~active] = baseline[~active]
        gate3[f"z{thr:g}_expo{expo:g}"] = evaluate_variant(monthly, baseline, active)

    # ---- neighbourhood consistency per gate -------------------------------
    def neighbourhood(variants: dict, key_order: list) -> dict:
        out = {}
        ds = [variants[k]["vs_baseline"]["d_sharpe"] for k in key_order]
        dm = [variants[k]["vs_baseline"]["d_mdd_pp"] for k in key_order]
        out["d_sharpe_by_variant"] = {k: variants[k]["vs_baseline"]["d_sharpe"]
                                      for k in key_order}
        out["d_mdd_pp_by_variant"] = {k: variants[k]["vs_baseline"]["d_mdd_pp"]
                                      for k in key_order}
        out["n_positive_dsharpe"] = int(sum(1 for v in ds if v is not None and v > 0))
        out["n_variants"] = len(key_order)
        return out

    nb1 = neighbourhood(gate1, list(gate1.keys()))
    nb2 = neighbourhood(gate2, list(gate2.keys()))
    nb3 = neighbourhood(gate3, list(gate3.keys()))

    # ---- verdicts ----------------------------------------------------------
    def gate_verdict(variants: dict, nb: dict) -> dict:
        passing = [k for k, v in variants.items() if v["passes"]]
        any_pos_dsharpe = nb["n_positive_dsharpe"] >= nb["n_variants"] // 2
        if passing and any_pos_dsharpe:
            v = "PASS (真增量): 有变体过线且参数邻域方向一致"
        elif passing:
            v = "ISOLATED PASS: 有变体过线但邻域不一致（视为过拟合嫌疑）"
        elif any_pos_dsharpe:
            v = "FAIL: 无变体过线（Sharpe/MDD 改善不足或半窗不一致），但方向上有弱信号"
        else:
            v = "FAIL: 全变体无改善 —— 100% 常态敞口即答案"
        return {"verdict": v, "passing_variants": passing}

    verdicts = {
        "gate1_vix": gate_verdict(gate1, nb1),
        "gate2_momregime": gate_verdict(gate2, nb2),
        "gate3_mvrv": gate_verdict(gate3, nb3),
    }

    # ---- console summary ----------------------------------------------------
    def line(name, ev):
        vb = ev["vs_baseline"]
        tr = ev["triggers"]
        print(f"    {name:<22} Sharpe {ev['full']['sharpe']:>7.3f} (d {vb['d_sharpe']:+.3f})  "
              f"MDD {ev['full']['max_dd_pct']:>6.1f}% (d {vb['d_mdd_pp']:+.1f}pp)  "
              f"retcost {vb['return_cost_pp']:+.2f}pp/yr  "
              f"trig {tr['n_active']}/{tr['n_months']}  "
              f"2022 {ev['ret_2022_pct'] if ev['ret_2022_pct'] is not None else '—'}%"
              f" vs base {r4(base_2022*100) if base_2022 is not None else '—'}%"
              f"  {'PASS' if ev['passes'] else ''}")

    print("\n" + "=" * 100)
    print(f"  风控开关 vs 50/50 基准  |  {str(xw.index[0].date())}..{str(xw.index[-1].date())}"
          f"  |  基准 Sharpe {base_stats['sharpe']}  MDD {base_stats['max_dd_pct']}%  "
          f"ann {base_stats['ann_ret_pct']}%")
    print("=" * 100)
    print("  [Gate 1: VIX 分位门]")
    for k, v in gate1.items():
        line(k, v)
    print("  [Gate 2: 动量 regime 倾斜]")
    for k, v in gate2.items():
        line(k, v)
    print("  [Gate 3: MVRV 估值门]")
    for k, v in gate3.items():
        line(k, v)
    for g, v in verdicts.items():
        print(f"  {g}: {v['verdict']}  passing={v['passing_variants']}")

    # ---- report -------------------------------------------------------------
    report = {
        "meta": {
            "generated_at": datetime.now(tz=timezone.utc).isoformat(),
            "script": "scripts/risk_gate_backtest.py",
            "question": "层3风控开关：VIX 分位 / 动量 regime / MVRV 估值，谁配决定 GBM 组合的整体敞口？",
            "pipeline": "完全复用 scripts/xsec_gbm_selection.py (importlib 只读): "
                        "同 LOCKED_PARAMS / seed 42 / purge / IBKR 成本 / 引擎",
            "baseline": "无开关 50/50 月度混合（引擎月度收益的等权均值，月再平衡，混合本身未另计成本；"
                        "开关减仓部分按 rf=4.34%/年 计息）",
            "data_end": data_end,
            "window": [str(xw.index[0].date()), str(xw.index[-1].date())],
            "n_months": n,
            "timing": "决策=月末信号日 T_k 收盘，仅用严格早于 T_k 的数据；作用于 E_k→E_{k+1} 执行窗。"
                      "动量因子收益实现于 ~E_{j+1}，故 gate2 因子月取 j ≤ k−2（额外滞后1个月）",
            "pass_line": f"ΔSharpe ≥ +{PASS_DSHARPE} 且 ΔMDD ≥ {PASS_DMDD_PP}pp 且两个半窗方向一致",
            "era_split": f"两个等长半窗: 1-{n//2}, {n//2+1}-{n}",
            "gate_inputs_available": {
                "vix_pct": int(vix_pct.notna().sum()),
                "mvrv_z": int(mv_z.notna().sum()),
                "mom_factor_z": int(mf_z.notna().sum()),
            },
        },
        "verification_vs_source_report": verification,
        "baseline_5050": {**base_stats,
                          "ret_2022_pct": r4(base_2022 * 100) if base_2022 is not None else None},
        "gate_inputs": {
            "vix_pctile_by_month": {str(t.date()): r4(v) for t, v in vix_pct.dropna().items()},
            "mvrv_z_by_month": {str(t.date()): r4(v) for t, v in mv_z.dropna().items()},
            "mom_factor_z_by_month": {str(t.date()): r4(v) for t, v in mf_z.dropna().items()},
        },
        "gate1_vix_percentile": {
            "def": f"VIX 滚动3年(756交易日)分位 > 阈值 → 下月敞口降至设定值，其余现金(rf)",
            "variants": gate1, "neighbourhood": nb1,
        },
        "gate2_momentum_regime": {
            "def": "动量因子(截面 top10−bottom10 月度收益)过去6个月均值 z：z>+thr → 动量70/GBM30；"
                   "z<−thr → GBM70/动量30；否则 50/50（不降总敞口，只倾斜）",
            "variants": gate2, "neighbourhood": nb2,
        },
        "gate3_mvrv_valuation": {
            "def": "BTC MVRV 180d z > 阈值（加密估值过热，crypto-adjacent 传染风险）→ 下月敞口降至设定值",
            "variants": gate3, "neighbourhood": nb3,
            "note": "MVRV 数据 2019-01 起，覆盖整个测试窗（2020-11 起），无短样本问题",
        },
        "verdicts": verdicts,
    }
    REPORT_OUT.parent.mkdir(exist_ok=True)
    REPORT_OUT.write_text(json.dumps(report, indent=2, default=jsonable))
    print(f"\n  report -> {REPORT_OUT}")


if __name__ == "__main__":
    main()
