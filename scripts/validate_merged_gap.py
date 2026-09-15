#!/usr/bin/env python3
"""Robustness validation of the MERGED BTC-gap signal (weekend + overnight).

Candidate under test (configuration INHERITED from prior research —
scripts/backtest_overnight_gap.py, commit 28c51c7 — NOT re-picked here):
  merged = weekend_gap events on their fire days
         + overnight events (market_close definition: prev TRADING day
           16:00 ET -> today 9:30 ET; Monday's window spans the whole
           weekend, 65.5h — weekend semantics on Mondays by construction)
           on all other days
  overnight threshold 5%, long_only, T+2 close exit,
  sizing score = min(|ret|/(2*thr), 1) = |ret|/10 capped at 1,
  IBKR costs (5bps half-spread), equal weight COIN/MSTR/MARA.

Motivation: the candidate was selected as the best of 32 cells
(2 definitions x 4 thresholds x 2 exits x 2 modes) and event frequency is
decaying (17.4/yr front half -> 6.3/yr back half). This script stress-tests
it before it enters the playbook. EVERY cell of every scan is reported.

Pre-registered judgment criteria (decided BEFORE running):
  A threshold band : Sharpe > 0.4 at thresholds 4.5 / 5.0 / 5.5
                     (full window, T+2, long_only, 5bps)
  B era exclusion  : excluding 2020-2022 the signal stays positive
                     (avg event ret > 0 AND t-stat > 0 AND total PnL > 0 —
                     event-level stats are path-independent; equity Sharpe
                     over the full window is diluted by flat years and is
                     reported but NOT the criterion)
  C cost stress    : t-stat > 1.5 at 15bps slippage (baseline config)
  D concentration  : leave-one-out stock subsets (no MSTR / no COIN /
                     no MARA) all keep avg event ret > 0
  verdict: 0 broken -> PASS; 1 broken -> CONDITIONAL PASS; >=2 -> FAIL.

Output: stdout summary + reports/merged_gap_validation.json.

Usage:
    poetry run python scripts/validate_merged_gap.py
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(Path(__file__).resolve().parent))

# Reuse data loading / event construction / portfolio sim from the existing
# experiment. The module file is NOT modified; the only runtime tweak is
# registering a t_plus_3 exit so run_portfolio can handle the exit scan.
import backtest_overnight_gap as base  # noqa: E402
from src.selection.costs import IbkrCostModel  # noqa: E402

base.EXIT_N["t_plus_3"] = 3  # runtime dict entry only

FULL_WINDOW = ("2019-01-01", "2026-09-11")
POST_2023_WINDOW = ("2023-01-01", "2026-09-11")
BASE_THR = 5.0
THRESHOLDS = (4.0, 4.5, 5.0, 5.5, 6.0)
EXITS = ("t_plus_1", "t_plus_2", "t_plus_3")
SLIPPAGE_BPS = (5.0, 15.0, 25.0)
ERAS = {
    "excl_2020": {2020},
    "excl_2020_2021": {2020, 2021},
    "excl_2020_2022": {2020, 2021, 2022},
}
SUBSETS = {
    "mstr_only": ("MSTR",),
    "coin_mstr": ("COIN", "MSTR"),      # == no_mara
    "all_three": ("COIN", "MSTR", "MARA"),
    # leave-one-out, for criterion D
    "no_mstr": ("COIN", "MARA"),
    "no_coin": ("MSTR", "MARA"),
    "no_mara": ("COIN", "MSTR"),
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def extend_exits_t3(
    events: list[dict], calendar: list[pd.Timestamp],
    bars: dict[str, pd.DataFrame],
) -> None:
    """Add exit_price_3 / exit_i_3 to every leg (mirrors simulate_legs)."""
    last_i = len(calendar) - 1
    for ev in events:
        day_i = ev["entry_i"]
        for leg in ev["legs"]:
            df = bars[leg["symbol"]]
            target_i = min(day_i + 3, last_i)
            j = target_i
            while calendar[j] not in df.index and j > day_i:
                j -= 1
            leg["exit_i_3"] = j
            leg["exit_price_3"] = float(df.loc[calendar[j]]["close"])


def merged_events(
    thr: float,
    weekend_events: list[dict],
    fire_days: set[int],
    overnight_by_thr: dict[float, list[dict]],
) -> list[dict]:
    ov = [e for e in overnight_by_thr[thr] if e["entry_i"] not in fire_days]
    out = weekend_events + ov
    out.sort(key=lambda e: e["entry_i"])
    return out


def filter_symbols(events: list[dict], symbols: tuple[str, ...]) -> list[dict]:
    out = []
    for ev in events:
        legs = [lg for lg in ev["legs"] if lg["symbol"] in symbols]
        if not legs:
            continue
        e2 = dict(ev)
        e2["legs"] = legs
        out.append(e2)
    return out


def exclude_years(events: list[dict], years: set[int]) -> list[dict]:
    return [e for e in events if int(e["entry_date"][:4]) not in years]


def summarize(r: dict) -> dict:
    m = r["metrics"]
    pnl = sum(d["pnl_usd"] for d in r["detail"])
    return {
        "n_events": r["n_events"],
        "events_per_year": r.get("events_per_year", 0.0),
        "win_rate_event_pct": r["win_rate_event_pct"],
        "avg_event_ret_pct": r["avg_event_ret_pct"],
        "median_event_ret_pct": r.get("median_event_ret_pct", 0.0),
        "t_stat": r["t_stat"],
        "worst_event_ret_pct": r.get("worst_event_ret_pct", 0.0),
        "total_pnl_usd": round(pnl, 2),
        "total_return_pct": m["total_return_pct"],
        "cagr_pct": m["cagr_pct"],
        "sharpe": m["sharpe"],
        "max_drawdown_pct": m["max_drawdown_pct"],
    }


def run(events, window, mode, exit_rule, cost, bars, calendar, closes_ff):
    return base.run_portfolio(
        events, window, mode, exit_rule, bars, calendar, closes_ff, cost,
    )


def monthly_aggregation(detail: list[dict], initial: float) -> dict:
    """Aggregate per-event PnL by calendar month; check concentration.

    Months without trades count as 0 return (the strategy is flat then) —
    that is the honest denominator for a 22-events/yr signal.
    """
    by_month: dict[str, float] = {}
    for d in detail:
        key = d["entry_date"][:7]
        by_month[key] = by_month.get(key, 0.0) + d["pnl_usd"]
    if not by_month:
        return {}
    months_sorted = sorted(by_month)
    idx = pd.period_range(
        pd.Period(months_sorted[0], "M"), pd.Period(months_sorted[-1], "M"),
        freq="M",
    )
    series = pd.Series([by_month.get(str(m), 0.0) for m in idx])
    rets = series / initial
    sd = float(rets.std(ddof=1))
    sharpe = float(rets.mean() / sd * math.sqrt(12)) if sd > 0 else 0.0
    total = float(series.sum())
    pos_sorted = series[series > 0].sort_values(ascending=False)
    imax, imin = int(series.values.argmax()), int(series.values.argmin())
    top5 = pos_sorted.head(5)
    return {
        "n_months_in_span": len(series),
        "n_months_with_trades": int((series != 0).sum()),
        "monthly_mean_ret_pct_of_initial": round(float(rets.mean()) * 100, 4),
        "monthly_sharpe_annualized_incl_flat_months": round(sharpe, 3),
        "total_pnl_usd": round(total, 2),
        "best_month": {
            "month": str(idx[imax]), "pnl_usd": round(float(series.iloc[imax]), 2),
        },
        "worst_month": {
            "month": str(idx[imin]), "pnl_usd": round(float(series.iloc[imin]), 2),
        },
        "top5_positive_months_share_of_total_pnl_pct": (
            round(float(top5.sum()) / total * 100, 1) if total > 0 else None
        ),
        "top5_months": [
            {"month": str(idx[int(i)]), "pnl_usd": round(float(v), 2)}
            for i, v in top5.items()
        ],
        "note": "monthly ret = month PnL / initial capital; flat months "
                "included in the Sharpe denominator; top5 share can exceed "
                "100% when negative months offset",
    }


def per_symbol_stats(detail: list[dict]) -> dict:
    out: dict[str, dict] = {}
    for d in detail:
        for lg in d["legs"]:
            rec = out.setdefault(
                lg["symbol"], {"n_legs": 0, "wins": 0, "ret_sum": 0.0},
            )
            rec["n_legs"] += 1
            rec["wins"] += 1 if lg["ret_pct"] > 0 else 0
            rec["ret_sum"] += lg["ret_pct"]
    return {
        sym: {
            "n_legs": rec["n_legs"],
            "avg_ret_pct": round(rec["ret_sum"] / rec["n_legs"], 3),
            "win_rate_pct": round(100.0 * rec["wins"] / rec["n_legs"], 2),
        }
        for sym, rec in sorted(out.items())
    }


def frequency_per_year(events: list[dict]) -> dict:
    out: dict[str, dict] = {}
    for e in events:
        yr = e["entry_date"][:4]
        rec = out.setdefault(yr, {"n": 0, "weekend": 0, "overnight": 0})
        rec["n"] += 1
        rec["weekend" if e.get("weekend") else "overnight"] += 1
    return dict(sorted(out.items()))


def brief(s: dict) -> str:
    return (f"n={s['n_events']:>3d} {s['events_per_year']:>5.1f}/yr "
            f"avg {s['avg_event_ret_pct']:>+6.3f}% t={s['t_stat']:>5.2f} "
            f"pnl {s['total_pnl_usd']:>+10.0f}$ ret {s['total_return_pct']:>+7.1f}% "
            f"Sharpe {s['sharpe']:>5.3f} MDD {s['max_drawdown_pct']:>6.1f}%")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    print("=" * 112)
    print("  合并缺口信号（weekend + overnight）稳健性验证 — 进 playbook 前的最后一道关")
    print(f"  基准: market_close 定义 / 隔夜阈值 {BASE_THR:.0f}% / long_only / T+2 / IBKR 5bps")
    print("  判定标准（事先注册）: A 阈值带 4.5-5.5 Sharpe>0.4 | B 剔除 2020-22 仍为正")
    print("                       C 15bps 下 t>1.5        | D 单标的剔除后仍为正")
    print("=" * 112)

    # ---- data (all reused from backtest_overnight_gap) --------------------
    bars = base.load_bars()
    calendar = sorted({d for df in bars.values() for d in df.index})
    calendar = [d for d in calendar if d >= pd.Timestamp("2019-01-01")]
    closes_ff = {
        sym: df["close"].reindex(pd.DatetimeIndex(calendar)).ffill()
        for sym, df in bars.items()
    }
    times, closes, btc_cov = base.load_btc_1h()
    ret_a, _, skipped_days = base.build_day_returns(calendar, times, closes)
    print(f"交易日历 {len(calendar)} 天 | BTC 1h {btc_cov['n_bars']} bars | "
          f"market_close 可用天数 {len(ret_a)} (跳过 {skipped_days['market_close']})")

    weekend_events, fire_days = base.collect_weekend_events(calendar, bars)
    overnight_by_thr = {
        thr: base.build_events(ret_a, thr, calendar, bars) for thr in THRESHOLDS
    }
    extend_exits_t3(weekend_events, calendar, bars)
    for thr in THRESHOLDS:
        extend_exits_t3(overnight_by_thr[thr], calendar, bars)

    merged_by_thr = {
        thr: merged_events(thr, weekend_events, fire_days, overnight_by_thr)
        for thr in THRESHOLDS
    }
    cost5 = IbkrCostModel()  # default 5bps

    ctx = dict(bars=bars, calendar=calendar, closes_ff=closes_ff)

    # ---- baseline reproduction -------------------------------------------
    base_run = run(
        merged_by_thr[BASE_THR], FULL_WINDOW, "long_only", "t_plus_2",
        cost5, **ctx,
    )
    baseline = summarize(base_run)
    print(f"\n  [基准复现] 合并信号 @{BASE_THR:.0f}% T+2 long_only 全窗口:")
    print(f"    {brief(baseline)}")
    print(f"    (参照 28c51c7 报告: 171 事件 22.3/年 +201.3% Sharpe 0.64 — 应一致)")

    # ---- 1. threshold neighborhood ---------------------------------------
    print("\n  [1] 阈值邻域 (T+2 long_only 全窗口, 5bps)")
    threshold_scan = {}
    for thr in THRESHOLDS:
        s = summarize(run(merged_by_thr[thr], FULL_WINDOW, "long_only",
                          "t_plus_2", cost5, **ctx))
        threshold_scan[str(thr)] = s
        print(f"    {thr:.1f}%: {brief(s)}")

    # ---- exit scan --------------------------------------------------------
    print("\n  [1b] 出场敏感性 (基准阈值 5%, long_only 全窗口, 5bps)")
    exit_scan = {}
    for exit_rule in EXITS:
        s = summarize(run(merged_by_thr[BASE_THR], FULL_WINDOW, "long_only",
                          exit_rule, cost5, **ctx))
        exit_scan[exit_rule] = s
        print(f"    {exit_rule}: {brief(s)}")

    # ---- 2. era exclusion -------------------------------------------------
    print("\n  [2] 时代剔除 (事件级统计 path-independent; 权益曲线跨全窗口, 平坦年稀释 Sharpe)")
    era_exclusion = {}
    for name, years in ERAS.items():
        ev = exclude_years(merged_by_thr[BASE_THR], years)
        s = summarize(run(ev, FULL_WINDOW, "long_only", "t_plus_2", cost5, **ctx))
        era_exclusion[name] = s
        print(f"    {name}: {brief(s)}")
    s = summarize(run(merged_by_thr[BASE_THR], POST_2023_WINDOW, "long_only",
                      "t_plus_2", cost5, **ctx))
    era_exclusion["post_2023_01"] = s
    print(f"    post_2023_01: {brief(s)}")

    # ---- 3. cost stress ---------------------------------------------------
    print("\n  [3] 成本压力 (spread/slippage bps, 基准配置)")
    cost_stress = {}
    for bps in SLIPPAGE_BPS:
        cost = IbkrCostModel(spread_bps=bps)
        s = summarize(run(merged_by_thr[BASE_THR], FULL_WINDOW, "long_only",
                          "t_plus_2", cost, **ctx))
        cost_stress[f"{bps:.0f}bps"] = s
        print(f"    {bps:>3.0f}bps: {brief(s)}")

    # ---- 4. underlying subsets -------------------------------------------
    print("\n  [4] 标的子集 (基准配置)")
    subset_scan = {}
    for name, symbols in SUBSETS.items():
        ev = filter_symbols(merged_by_thr[BASE_THR], symbols)
        s = summarize(run(ev, FULL_WINDOW, "long_only", "t_plus_2", cost5, **ctx))
        subset_scan[name] = {**s, "symbols": list(symbols)}
        print(f"    {name:10s} {'+'.join(symbols):>12s}: {brief(s)}")

    per_symbol = per_symbol_stats(base_run["detail"])
    print(f"    分标的 leg 统计: " + " | ".join(
        f"{sym} n={r['n_legs']} avg {r['avg_ret_pct']:+.3f}% win {r['win_rate_pct']}%"
        for sym, r in per_symbol.items()))

    # ---- 5. monthly aggregation ------------------------------------------
    monthly = monthly_aggregation(base_run["detail"], base.INITIAL)
    print("\n  [5] 月度聚合 (月收益 = 月PnL/初始资金, 含无交易月)")
    print(f"    跨度 {monthly['n_months_in_span']} 个月 (有交易 {monthly['n_months_with_trades']}) | "
          f"月度 Sharpe(年化) {monthly['monthly_sharpe_annualized_incl_flat_months']} | "
          f"最佳月 {monthly['best_month']['month']} {monthly['best_month']['pnl_usd']:+.0f}$ | "
          f"最差月 {monthly['worst_month']['month']} {monthly['worst_month']['pnl_usd']:+.0f}$")
    print(f"    Top5 正月占总 PnL {monthly['top5_positive_months_share_of_total_pnl_pct']}%")

    freq = frequency_per_year(merged_by_thr[BASE_THR])
    print(f"\n  事件频率分年 (衰减诊断): " + " ".join(
        f"{yr}:{r['n']}" for yr, r in freq.items()))

    # ---- references: weekend-only / overnight-only ------------------------
    ov_base = [e for e in overnight_by_thr[BASE_THR] if e["entry_i"] not in fire_days]
    references = {}
    for name, ev in (("weekend_gap_original", weekend_events),
                     ("overnight_only", ov_base)):
        references[name] = {
            "full": summarize(run(ev, FULL_WINDOW, "long_only", "t_plus_2",
                                  cost5, **ctx)),
            "excl_2020_2022": summarize(run(
                exclude_years(ev, ERAS["excl_2020_2022"]), FULL_WINDOW,
                "long_only", "t_plus_2", cost5, **ctx)),
            "post_2023_01": summarize(run(ev, POST_2023_WINDOW, "long_only",
                                          "t_plus_2", cost5, **ctx)),
        }
        r = references[name]
        print(f"\n  [参照] {name}:")
        print(f"    full          : {brief(r['full'])}")
        print(f"    excl_2020_2022: {brief(r['excl_2020_2022'])}")
        print(f"    post_2023_01  : {brief(r['post_2023_01'])}")

    # ---- verdict (pre-registered) ----------------------------------------
    band = {str(t): threshold_scan[str(t)]["sharpe"] for t in (4.5, 5.0, 5.5)}
    ex22 = era_exclusion["excl_2020_2022"]
    loo = {k: subset_scan[k]["avg_event_ret_pct"] for k in ("no_mstr", "no_coin", "no_mara")}
    criteria = {
        "A_threshold_band": {
            "definition": "Sharpe > 0.4 at thresholds 4.5/5.0/5.5 (full, T+2, long_only)",
            "sharpe_by_threshold": band,
            "passed": all(v > 0.4 for v in band.values()),
        },
        "B_era_exclusion": {
            "definition": "excl 2020-22: avg_event_ret > 0 AND t_stat > 0 AND total_pnl > 0",
            "avg_event_ret_pct": ex22["avg_event_ret_pct"],
            "t_stat": ex22["t_stat"],
            "total_pnl_usd": ex22["total_pnl_usd"],
            "passed": (ex22["avg_event_ret_pct"] > 0 and ex22["t_stat"] > 0
                       and ex22["total_pnl_usd"] > 0),
        },
        "C_cost_stress": {
            "definition": "t_stat > 1.5 at 15bps slippage (baseline config)",
            "t_stat_15bps": cost_stress["15bps"]["t_stat"],
            "passed": cost_stress["15bps"]["t_stat"] > 1.5,
        },
        "D_concentration": {
            "definition": "leave-one-out subsets (no_mstr/no_coin/no_mara) all avg_event_ret > 0",
            "avg_event_ret_pct_by_subset": loo,
            "passed": all(v > 0 for v in loo.values()),
        },
    }
    n_broken = sum(1 for c in criteria.values() if not c["passed"])
    verdict = "通过" if n_broken == 0 else ("有条件通过" if n_broken == 1 else "不通过")
    print("\n" + "=" * 112)
    for k, c in criteria.items():
        print(f"  {k}: {'PASS' if c['passed'] else 'BROKEN'}  ({c['definition']})")
    print(f"  判定: {verdict} (broken {n_broken}/4)")

    # ---- JSON -------------------------------------------------------------
    report = {
        "purpose": "robustness validation of the merged BTC-gap signal "
                   "(weekend_gap + overnight market_close) before playbook entry",
        "baseline_config": {
            "inherited_from": "scripts/backtest_overnight_gap.py @ 28c51c7",
            "definition": "market_close (prev TRADING day 16:00 ET -> today 9:30 ET; "
                          "Monday window spans the weekend = weekend semantics)",
            "threshold_pct": BASE_THR,
            "mode": "long_only",
            "exit": "t_plus_2 close",
            "sizing": "score = sign*min(|ret|/(2*thr),1); notional = |score|*equity; gross cap 2x",
            "targets": list(base.TARGETS),
            "cost_model": "IBKR $0.005/share min $1 + 5bps half-spread",
            "initial_capital": base.INITIAL,
            "window": list(FULL_WINDOW),
            "multiple_comparison_context": "candidate was best of 32 cells "
                                           "(2 def x 4 thr x 2 exit x 2 mode); this "
                                           "validation is the pre-registered guard",
        },
        "pre_registered_criteria": criteria,
        "verdict": {
            "n_criteria_broken": n_broken,
            "verdict": verdict,
            "rule": "0 broken -> 通过; 1 -> 有条件通过; >=2 -> 不通过",
        },
        "baseline": baseline,
        "threshold_scan": threshold_scan,
        "exit_scan": exit_scan,
        "era_exclusion": {
            **era_exclusion,
            "note": "excl_* runs keep the FULL window equity path (flat in "
                    "excluded years -> Sharpe diluted); event-level stats "
                    "(avg/t/pnl) are path-independent and are the criterion",
        },
        "cost_stress": cost_stress,
        "subset_scan": subset_scan,
        "per_symbol_leg_stats": per_symbol,
        "monthly_aggregation": monthly,
        "event_frequency_per_year": freq,
        "references": references,
        "baseline_events": [
            {
                "date": d["entry_date"],
                "direction": d["direction"],
                "btc_ret_pct": d["btc_overnight_ret_pct"],
                "ret_pct": d["ret_pct"],
                "pnl_usd": d["pnl_usd"],
            }
            for d in base_run["detail"]
        ],
    }
    out_path = Path("reports/merged_gap_validation.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    print(f"\nJSON 已写入: {out_path}")


if __name__ == "__main__":
    main()
