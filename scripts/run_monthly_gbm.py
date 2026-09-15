#!/usr/bin/env python3
"""Monthly GBM stock-selection runner — the paper-trading production loop.

Runs the locked cross-sectional GBM (scripts/xsec_gbm_selection.py, the model
that passed OOS validation in reports/xsec_gbm_results.json) once a month and
persists the picks, so a genuine out-of-sample track record accumulates from
here on. The model, features, purge rule and seed are imported UNMODIFIED
from the validation script — this runner only adds the monthly I/O.

Reuse note: xsec_gbm_selection.py is a script, not a package member, but the
repo root is on sys.path so the `scripts.*` namespace package imports it
directly (same approach as scripts/refresh_daily_data.py). Less invasive than
importlib.util.spec_from_file_location and survives renames of the file's
location; if `scripts` ever becomes a real package or moves, switch to
spec_from_file_location in the import block below.

Selection discipline (identical to the backtest walk-forward):
  - Target month YYYY-MM -> signal at the LAST trading day of the PREVIOUS
    month; features are sampled strictly before the signal day (the feature
    day P is the trading day before the signal day).
  - Training set: all feature months with period <= signal_month - 2 (purged;
    at a true month-end run this equals every fully-labelled month).
  - Hyperparameters: LOCKED_PARAMS (G_rank_depth4), seed 42, deterministic.
  - Universe / eligibility / per-month cross-sectional z-scoring: same as
    build_dataset in xsec_gbm_selection.py.

Modes (auto-selected):
  - asof  — the previous month's last trading day exists in the data (run at
            true month end, or reproducing a historical month). The test
            cross-section comes from the shared build_dataset, so `--as-of
            <historical month>` reproduces the backtest's gbm_holdings
            exactly (verified automatically against the results report).
  - live  — the signal month is still in progress (early run, e.g. the first
            2026-10 picks generated mid-September). Feature day = latest
            COMPLETED trading day in the panel (today's partial bar is never
            used). Caveat: the official month-end run would carry ~2 more
            weeks of data; the first month only, afterwards run at month end.

每月操作流程 (paper trading SOP):
  1. 月末最后交易日收盘后（ET ~16:30，当日 Nasdaq 数据可取）:
         poetry run python scripts/run_monthly_gbm.py --refresh --as-of 2026-11
     （--as-of 填下月；--refresh 增量拉取全部 universe 最新日线）
     -> reports/gbm_picks/2026-11.json + log.jsonl 追加一行
  2. 次月首个交易日收盘后（当日 OHLC 已入库）补录入场基准:
         poetry run python scripts/run_monthly_gbm.py --settle 2026-11
     以当月首个交易日的开盘价作为 paper trading 入场价，写回 JSON 的
     settle 字段（幂等；重复运行会跳过，--force 才覆盖）。
  3. 复现验证（可选，自动）: --as-of 任一回测覆盖内的历史月份，脚本自动
     与 reports/xsec_gbm_results.json 的 gbm_holdings 对比并打印
     MATCH / MISMATCH。

首个 live 月特例：2026-10 的选股于 2026-09-15 提前生成（数据截至
feature_date，比正式月末运行少约两周），2026-09 的选股用 --as-of 复现
（与回测 2026-09-01 持仓一致）后同样 settle，paper 记录自 2026-09 起连续。

Usage:
    poetry run python scripts/run_monthly_gbm.py                          # 下月
    poetry run python scripts/run_monthly_gbm.py --as-of 2026-05          # 复现
    poetry run python scripts/run_monthly_gbm.py --refresh --as-of 2026-10
    poetry run python scripts/run_monthly_gbm.py --settle 2026-09
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd

from scripts.xsec_gbm_selection import (
    FEATURES,
    LOCKED_PARAMS,
    build_dataset,
    compute_daily_features,
    fit_predict,
    jsonable,
    load_panel,
    load_pit_universe,
    month_end_signal_days,
    universe_for_date,
)
from src.data.nasdaq_store import NasdaqDailyStore

PICKS_DIR = ROOT / "reports" / "gbm_picks"
LOG_PATH = PICKS_DIR / "log.jsonl"
RESULTS_PATH = ROOT / "reports" / "xsec_gbm_results.json"
DATA_START = "2016-01-01"          # same as the backtest's --data-start default
REFRESH_LOOKBACK_DAYS = 21         # incremental Nasdaq refresh window
SEED = 42


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def period_of(p: pd.Period) -> int:
    """Same period encoding as build_dataset: year*12 + month - 1."""
    return p.year * 12 + p.month - 1


def period_to_month(p: int) -> str:
    y, m = divmod(p, 12)
    return f"{y:04d}-{m + 1:02d}"


def now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat(timespec="seconds")


def refresh_universe(union: list[str]) -> None:
    """Incrementally refresh daily bars for the whole PIT union (Nasdaq API)."""
    store = NasdaqDailyStore(assetclass="stocks", politeness_s=1.2)
    from_date = (
        datetime.now(tz=timezone.utc) - timedelta(days=REFRESH_LOOKBACK_DAYS)
    ).strftime("%Y-%m-%d")
    ok, failed = 0, []
    for i, sym in enumerate(union, 1):
        try:
            store.fetch_and_store(sym, from_date=from_date)
            ok += 1
        except Exception:
            failed.append(sym)
        if i % 25 == 0:
            print(f"    refresh {i}/{len(union)} ...", flush=True)
    print(f"  refresh: {ok}/{len(union)} ok"
          + (f"; failed (no data / fetch error): {failed}" if failed else ""))


def live_cross_section(
    feats: dict[str, pd.DataFrame],
    closes: pd.DataFrame,
    pit: dict[int, list[str]],
    P: pd.Timestamp,
    period: int,
) -> pd.DataFrame:
    """z-scored feature cross-section at feature day P — replicates
    build_dataset's per-month eligibility + z-scoring exactly (used when the
    signal month is still in progress and build_dataset has no row for it)."""
    uni = universe_for_date(pit, P)
    index = closes.index
    hist = closes.loc[index <= P]
    counts = hist.notna().sum()
    eligible = [s for s in uni if s in closes.columns and counts.get(s, 0) >= 260]

    zfeat: dict[str, pd.Series] = {}
    for f in FEATURES:
        s = feats[f].loc[P, eligible].astype(float)
        mu, sd = s.mean(), s.std(ddof=0)
        z = (s - mu) / sd if sd and np.isfinite(sd) and sd > 1e-12 else s * 0.0
        zfeat[f] = z.clip(-3.0, 3.0)

    test = pd.DataFrame({"symbol": eligible, "date": P, "period": period})
    for f in FEATURES:
        test[f] = zfeat[f].reindex(eligible).values.astype(float)
    return test


# ---------------------------------------------------------------------------
# Produce picks for a target month
# ---------------------------------------------------------------------------

def produce_picks(month: str, top_n: int, refresh: bool) -> dict:
    target = pd.Period(month, freq="M")
    prev = target - 1
    M = period_of(prev)  # period of the signal month (prev month's month-end)

    pit, union = load_pit_universe()
    if refresh:
        print(f"  refreshing {len(union)} universe symbols (Nasdaq, "
              f"lookback {REFRESH_LOOKBACK_DAYS}d) ...")
        refresh_universe(union)

    store = NasdaqDailyStore(assetclass="stocks")
    data_end = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    panel = load_panel(store, union, DATA_START, data_end)
    closes = panel["close"]
    index = closes.index
    print(f"  panel: {index.shape[0]} days x {closes.shape[1]} symbols "
          f"({index[0].date()} ~ {index[-1].date()})")

    feats = compute_daily_features(panel)

    # --- resolve the signal day of the previous month --------------------
    prev_start = prev.to_timestamp(how="start").tz_localize("UTC")
    prev_end_cal = prev.to_timestamp(how="end").tz_localize("UTC")
    in_prev = index[(index >= prev_start) & (index <= prev_end_cal)]
    today = pd.Timestamp.now(tz="UTC").normalize()
    # previous month is "complete" when either the calendar has moved past it,
    # or the panel already extends beyond its last in-data day (data reached
    # the target month, so the month-end signal day exists in the panel)
    prev_complete = (today > prev_end_cal) or (
        len(in_prev) > 0 and index[-1] > in_prev[-1])

    if prev_complete:
        if len(in_prev) == 0:
            raise RuntimeError(
                f"previous month {prev} has no data in the store — refresh first")
        T = in_prev[-1]
        if (prev_end_cal - T.normalize()).days > 7:
            raise RuntimeError(
                f"last trading day of {prev} in data is {T.date()} "
                f"(>7d before month end) — data looks stale, run with --refresh")
        P = index[index < T][-1]
        mode = "asof"
        signal_date: pd.Timestamp | None = T
    else:
        cand = index[index < today]  # never use today's (possibly partial) bar
        if cand.empty:
            raise RuntimeError("no completed trading day before today in the panel")
        P = cand[-1]
        mode = "live"
        signal_date = None

    # --- dataset + training rows (shared build, identical to backtest) ---
    signals = month_end_signal_days(index, index[0], index[-1])
    data = build_dataset(panel, feats, pit, signals)
    train = data[data["period"] <= M - 2]
    n_labeled = int(train["fwd_ret"].notna().sum())
    print(f"  dataset: {len(data)} rows / {data['period'].nunique()} months; "
          f"train <= {period_to_month(M - 2)}: {train['period'].nunique()} months, "
          f"{n_labeled} labeled rows")

    # --- test cross-section ---------------------------------------------
    if mode == "asof":
        test = data[data["period"] == M]
        if test.empty:
            # execution day not yet in data (e.g. run the morning after month
            # end) — rebuild the cross-section at the same feature day P
            test = live_cross_section(feats, closes, pit, P, M)
    else:
        test = live_cross_section(feats, closes, pit, P, M)
    print(f"  mode={mode}  signal_day="
          f"{str(signal_date.date()) if signal_date is not None else 'pending'}"
          f"  feature_day={P.date()}  pool={len(test)}")

    # --- fit + rank -------------------------------------------------------
    preds, model = fit_predict(train, test, LOCKED_PARAMS, seed=SEED)
    if model is None or np.isnan(preds).all():
        raise RuntimeError("model fit failed (insufficient labelled training rows)")
    s = pd.Series(preds, index=test["symbol"].values, name="score")
    s = s[~s.index.duplicated(keep="last")]
    top = s.sort_values(ascending=False).head(top_n)

    # --- feature importance (this run's fitted model) ---------------------
    imp = model.booster_.feature_importance(importance_type="gain")
    imp_share = imp / imp.sum() if imp.sum() > 0 else imp * 0.0
    top5 = sorted(zip(FEATURES, imp_share.tolist()), key=lambda kv: -kv[1])[:5]
    top5_names = [f for f, _ in top5]

    test_idx = test.set_index("symbol")
    picks = []
    for rank, (sym, score) in enumerate(top.items(), 1):
        snap = {}
        for f in top5_names:
            raw = feats[f].loc[P, sym]
            snap[f] = {
                "raw": round(float(raw), 4) if pd.notna(raw) else None,
                "z": round(float(test_idx.loc[sym, f]), 3)
                     if pd.notna(test_idx.loc[sym, f]) else None,
            }
        picks.append({"rank": rank, "symbol": sym,
                      "score": round(float(score), 5), "features": snap})

    notes = []
    if mode == "live":
        notes.append(
            f"提前运行：信号日（{prev} 月末最后交易日）尚未到来，特征日为最新完整交易日 "
            f"{P.date()}。正式月末运行会多出至月末的数据（首月 2026-10 如此，之后按 SOP 月末跑）。")
    else:
        notes.append(
            f"复现/月末运行：信号日 {signal_date.date()}，特征日 {P.date()}，"
            f"与回测 walk-forward 完全同规则同时点。")

    doc = {
        "month": str(target),
        "mode": mode,
        "generated_at": now_iso(),
        "signal_date": str(signal_date.date()) if signal_date is not None else None,
        "signal_date_note": None if signal_date is not None
            else f"last trading day of {prev} (pending at generation time)",
        "feature_date": str(P.date()),
        "data_panel": [str(index[0].date()), str(index[-1].date())],
        "training": {
            "rule": "expanding window, train feature months <= signal_month - 2 "
                    "(purged; identical to backtest walk_forward)",
            "months": [period_to_month(int(train["period"].min())),
                       period_to_month(int(train["period"].max()))],
            "n_months": int(train["period"].nunique()),
            "n_rows": int(len(train)),
            "n_labeled": n_labeled,
            "hyperparams": LOCKED_PARAMS,
            "seed": SEED,
        },
        "pool_size": int(len(test)),
        "top_features": [{"feature": f, "gain_share": round(v, 4)} for f, v in top5],
        "picks": picks,
        "settle": None,
        "notes": notes,
    }

    # --- persist ----------------------------------------------------------
    PICKS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = PICKS_DIR / f"{target}.json"
    out_path.write_text(json.dumps(doc, indent=2, default=jsonable))
    update_log(doc)
    print(f"  picks -> {out_path}  (+ log.jsonl)")

    # --- human-readable table ---------------------------------------------
    print(f"\n  === GBM picks {target} (paper trading, top {top_n}) ===")
    print(f"  {'rank':>4}  {'symbol':<7}{'score':>9}  " +
          "  ".join(f"{f[:11]:>11}" for f in top5_names) + "   (z-scores)")
    for pick in picks:
        zs = "  ".join(
            f"{(pick['features'][f]['z'] if pick['features'][f]['z'] is not None else float('nan')):>11.2f}"
            for f in top5_names)
        print(f"  {pick['rank']:>4}  {pick['symbol']:<7}{pick['score']:>9.4f}  {zs}")

    verify_against_report(doc)
    return doc


def verify_against_report(doc: dict) -> None:
    """asof mode: compare with the backtest's gbm_holdings for that month."""
    if doc["mode"] != "asof" or not RESULTS_PATH.exists():
        return
    rep = json.loads(RESULTS_PATH.read_text())
    entries = [h for h in rep.get("gbm_holdings", [])
               if h["date"][:7] == doc["month"]]
    if not entries:
        print("  verify: no backtest holdings entry for this month (nothing to compare)")
        return
    expected = sorted(entries[0]["symbols"])
    got = sorted(p["symbol"] for p in doc["picks"])
    if expected == got:
        print(f"  verify: MATCH — identical to backtest gbm_holdings "
              f"@ {entries[0]['date']} ({len(got)} symbols)")
    else:
        print(f"  verify: MISMATCH vs backtest gbm_holdings @ {entries[0]['date']}\n"
              f"    backtest only: {sorted(set(expected) - set(got))}\n"
              f"    runner  only: {sorted(set(got) - set(expected))}")


# ---------------------------------------------------------------------------
# log.jsonl (one line per month, dedup-safe)
# ---------------------------------------------------------------------------

def update_log(doc: dict) -> None:
    lines: list[dict] = []
    if LOG_PATH.exists():
        for line in LOG_PATH.read_text().splitlines():
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if rec.get("month") != doc["month"]:
                lines.append(rec)
    lines.append({
        "month": doc["month"],
        "generated_at": doc["generated_at"],
        "mode": doc["mode"],
        "feature_date": doc["feature_date"],
        "picks": [p["symbol"] for p in doc["picks"]],
        "scores": {p["symbol"]: p["score"] for p in doc["picks"]},
        "settled": doc.get("settle") is not None,
        "settle_entry_date": (doc.get("settle") or {}).get("entry_date"),
    })
    lines.sort(key=lambda r: r["month"])
    LOG_PATH.write_text(
        "\n".join(json.dumps(r, default=jsonable) for r in lines) + "\n")


# ---------------------------------------------------------------------------
# --settle: record the entry benchmark (first trading day OPEN of the month)
# ---------------------------------------------------------------------------

def cmd_settle(month: str, force: bool = False) -> None:
    path = PICKS_DIR / f"{month}.json"
    if not path.exists():
        raise SystemExit(f"no picks file for {month} — run --as-of {month} first")
    doc = json.loads(path.read_text())

    store = NasdaqDailyStore(assetclass="stocks")
    target = pd.Period(month, freq="M")
    m_start = target.to_timestamp(how="start").tz_localize("UTC")
    m_end_excl = (target + 1).to_timestamp(how="start").tz_localize("UTC")
    ref = store.get_daily("AAPL", m_start.strftime("%Y-%m-%d"),
                          m_end_excl.strftime("%Y-%m-%d"))
    if ref.empty:
        raise SystemExit("no AAPL data for the month yet — settle after the "
                         "first trading day's data is in the store")
    entry_day = ref.index[0]

    existing = doc.get("settle")
    if existing and existing.get("entry_date") == str(entry_day.date()) and not force:
        print(f"  {month} already settled @ {existing['entry_date']} — skip "
              f"(use --force to re-record)")
        return

    prices: dict[str, dict | None] = {}
    missing: list[str] = []
    end_s = (entry_day + pd.Timedelta(days=3)).strftime("%Y-%m-%d")
    for pick in doc["picks"]:
        sym = pick["symbol"]
        df = store.get_daily(sym, str(entry_day.date()), end_s)
        row = df.loc[df.index == entry_day]
        if row.empty:
            prices[sym] = None
            missing.append(sym)
        else:
            prices[sym] = {"open": round(float(row["open"].iloc[0]), 4),
                           "close": round(float(row["close"].iloc[0]), 4)}

    doc["settle"] = {
        "entry_date": str(entry_day.date()),
        "basis": "当月首个交易日开盘价（paper trading 入场基准；回测引擎口径为次一日收盘，此处以可实际成交的开盘价为准）",
        "prices": prices,
        "recorded_at": now_iso(),
    }
    if missing:
        doc["settle"]["missing"] = missing
    path.write_text(json.dumps(doc, indent=2, default=jsonable))
    update_log(doc)

    print(f"\n  === settle {month} — entry @ {entry_day.date()} OPEN ===")
    for pick in doc["picks"]:
        px = prices[pick["symbol"]]
        o = f"{px['open']:>10.2f}" if px else "        n/a"
        print(f"  {pick['rank']:>4}  {pick['symbol']:<7} open={o}")
    if missing:
        print(f"  WARNING: no price data on entry day for: {missing}")
    print(f"  settled -> {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    p = argparse.ArgumentParser(
        description="Monthly GBM picks runner (paper trading)")
    p.add_argument("--as-of", metavar="YYYY-MM",
                   help="为目标月生成选股（默认：下个月）")
    p.add_argument("--settle", metavar="YYYY-MM",
                   help="补录目标月首个交易日开盘入场价到对应 JSON")
    p.add_argument("--refresh", action="store_true",
                   help="生成前增量刷新全部 universe 日线（月末 SOP 步骤 1）")
    p.add_argument("--top-n", type=int, default=10)
    p.add_argument("--force", action="store_true",
                   help="settle 时覆盖已存在的补录")
    args = p.parse_args()

    month_re = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

    if args.settle:
        if not month_re.match(args.settle):
            raise SystemExit("--settle expects YYYY-MM")
        cmd_settle(args.settle, force=args.force)
        return

    if args.as_of:
        month = args.as_of
        if not month_re.match(month):
            raise SystemExit("--as-of expects YYYY-MM")
    else:
        month = str(pd.Period.now("M") + 1)  # 默认：下个月

    produce_picks(month, top_n=args.top_n, refresh=args.refresh)


if __name__ == "__main__":
    main()
