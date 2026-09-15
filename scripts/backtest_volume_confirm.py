#!/usr/bin/env python3
"""Volume-confirmation analysis for weekend_gap / overnight_gap event legs.

Research question: weekend_gap (BTC weekend |move| >= 5% -> Monday-open long
COIN/MSTR/MARA, T+2 close exit) is the flagship signal. Does the event-day
STOCK volume carry additional information — do high-volume-confirmed events
outperform?

Metrics per leg (event = entry day):
  event_day_ratio = event-day volume / mean(volume of the prior 20 trading
                    days of the same symbol, STRICTLY before the event day)
  prior_day_ratio = same but for the previous trading day (Friday for the
                    weekend_gap Monday entries) — the only variant that is
                    KNOWN BEFORE the entry open, hence the only executable
                    entry filter; event_day_ratio is only known at the close
                    (look-ahead if used as an entry filter — stated plainly)

Buckets (standard a-priori thresholds, not tuned):
  high  ratio > 1.5
  mid   0.7 <= ratio <= 1.5
  low   ratio < 0.7

Event sets:
  A  weekend_gap long-only T+2 trades, from
     reports/exit_rules_backtest.json -> trades_full_window.long_only.t_plus_2
     (13 events, 2019-2026; leg returns recomputed GROSS from entry/exit
     prices — the stored ret_pct is net of IBKR costs)
  B  overnight BTC gap (market_close definition) events, from
     reports/overnight_gap_backtest.json -> events_detail.market_close,
     long_only, thresholds 2/3/4/5% — 5% is the headline (92 long events,
     matching the weekend_gap threshold); the other thresholds are reported
     as robustness (nested samples, not independent)

Per-bucket stats: n_legs, n_events, mean/median T+2 leg return, win rate,
mean within-event excess (leg return minus the mean return of the same
event's ratio-covered legs — controls for the common BTC shock / market day).
Also: Spearman correlation (ratio vs return), a descriptive "drop low /
keep high only" filter simulation (event-day AND prior-day variants), and a
per-symbol ratio sanity check (volume bucket must not silently become a
symbol proxy).

Honesty notes baked into the report:
  - set A is 38 legs across 13 events -> purely descriptive, no significance
    claims; set B at 5% is ~250 legs -> still descriptive, better powered
  - open-auction volume is NOT separable from daily volume — intraday data
    would be needed (noted as a limitation, per task spec)
  - volume/price data is the shared data/btc_history.db (Nasdaq daily,
    split-adjusted; MSTR 2024-08 10:1 verified continuous in price AND
    volume)

Output: stdout summary + reports/volume_confirm.json.

Usage:
    poetry run python scripts/backtest_volume_confirm.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.data.nasdaq_store import NasdaqDailyStore

ROOT = Path(__file__).resolve().parent.parent
TARGETS = ("MSTR", "COIN", "MARA")
VOL_LOOKBACK = 20          # trading days, strictly before the measured day
HIGH, LOW = 1.5, 0.7       # a-priori bucket edges
LOAD_START, LOAD_END = "2016-09-01", "2026-09-15"
OVERNIGHT_THRESHOLDS = ("2.0", "3.0", "4.0", "5.0")
HEADLINE_THRESHOLD = "5.0"
BUCKETS = ("high", "mid", "low")


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------


def load_volume() -> dict[str, pd.Series]:
    """Daily volume per target, tz-naive normalized date index."""
    store = NasdaqDailyStore(assetclass="stocks")
    out: dict[str, pd.Series] = {}
    for sym in TARGETS:
        df = store.get_daily(sym, LOAD_START, LOAD_END)
        if df.empty:
            raise RuntimeError(f"no stock data for {sym}")
        s = df["volume"].astype(float).copy()
        s.index = pd.DatetimeIndex(df.index).tz_localize(None).normalize()
        out[sym] = s
    return out


def vol_ratio(s: pd.Series, date: pd.Timestamp, lag: int = 0) -> float | None:
    """volume at (date - `lag` trading days) / mean of the VOL_LOOKBACK days
    strictly before that day. None if the day is missing, has no/zero
    volume, or fewer than VOL_LOOKBACK prior days exist."""
    if date not in s.index:
        return None
    j = int(s.index.get_loc(date)) - lag
    if j < VOL_LOOKBACK:
        return None
    v = float(s.iloc[j])
    window = s.iloc[j - VOL_LOOKBACK : j]
    m = float(window.mean())
    if not (v > 0 and m > 0):
        return None
    return v / m


def bucket_of(ratio: float) -> str:
    if ratio > HIGH:
        return "high"
    if ratio < LOW:
        return "low"
    return "mid"


# ---------------------------------------------------------------------------
# Event sets -> normalized leg records
# ---------------------------------------------------------------------------


def weekend_legs(volume: dict[str, pd.Series]) -> tuple[list[dict], int]:
    """Set A: weekend_gap long-only T+2 legs, gross returns."""
    data = json.loads((ROOT / "reports/exit_rules_backtest.json").read_text())
    events = data["trades_full_window"]["long_only"]["t_plus_2"]
    legs: list[dict] = []
    n_missing_ratio = 0
    for ev in events:
        date = pd.Timestamp(ev["entry_date"])
        for lg in ev["legs"]:
            if lg.get("skipped"):
                continue
            r_ed = vol_ratio(volume[lg["symbol"]], date, lag=0)
            r_pd = vol_ratio(volume[lg["symbol"]], date, lag=1)
            if r_ed is None or r_pd is None:
                n_missing_ratio += 1
                continue
            legs.append({
                "event_date": ev["entry_date"],
                "symbol": lg["symbol"],
                "btc_ret_pct": ev["btc_weekend_return_pct"],
                "ret_pct": round(
                    (lg["exit_price"] / lg["entry_price"] - 1.0) * 100.0, 3
                ),
                "ratio_event_day": round(r_ed, 3),
                "ratio_prior_day": round(r_pd, 3),
                "bucket_event_day": bucket_of(r_ed),
                "bucket_prior_day": bucket_of(r_pd),
            })
    return legs, n_missing_ratio


def overnight_legs(
    volume: dict[str, pd.Series], threshold: str,
    weekend_dates: set[str],
) -> tuple[list[dict], int]:
    """Set B: overnight gap (market_close) long-only legs at `threshold`."""
    data = json.loads((ROOT / "reports/overnight_gap_backtest.json").read_text())
    events = data["events_detail"]["market_close"][threshold]
    legs: list[dict] = []
    n_missing_ratio = 0
    for ev in events:
        if ev["direction"] != "long":
            continue  # flagship signal is long_only
        date = pd.Timestamp(ev["date"])
        for lg in ev["legs"]:
            r_ed = vol_ratio(volume[lg["sym"]], date, lag=0)
            r_pd = vol_ratio(volume[lg["sym"]], date, lag=1)
            if r_ed is None or r_pd is None:
                n_missing_ratio += 1
                continue
            legs.append({
                "event_date": ev["date"],
                "symbol": lg["sym"],
                "btc_ret_pct": ev["btc_ret_pct"],
                "ret_pct": lg["ret_t2_pct"],   # already direction-signed (long)
                "ratio_event_day": round(r_ed, 3),
                "ratio_prior_day": round(r_pd, 3),
                "bucket_event_day": bucket_of(r_ed),
                "bucket_prior_day": bucket_of(r_pd),
                "weekend_gap_day": ev["date"] in weekend_dates,
            })
    return legs, n_missing_ratio


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------


def add_within_event_excess(legs: list[dict], ratio_key: str, bucket_key: str) -> None:
    """excess = leg ret - mean ret of the same event's ratio-covered legs.

    Only defined for events with >= 2 covered legs; controls for the common
    BTC shock / market day."""
    by_event: dict[str, list[dict]] = {}
    for lg in legs:
        by_event.setdefault(lg["event_date"], []).append(lg)
    for ev_legs in by_event.values():
        if len(ev_legs) < 2:
            for lg in ev_legs:
                lg[f"excess_{ratio_key}"] = None
            continue
        mean_ret = sum(lg["ret_pct"] for lg in ev_legs) / len(ev_legs)
        for lg in ev_legs:
            lg[f"excess_{ratio_key}"] = round(lg["ret_pct"] - mean_ret, 3)
    _ = bucket_key  # buckets already assigned


def bucket_stats(legs: list[dict], ratio_key: str, bucket_key: str) -> dict:
    add_within_event_excess(legs, ratio_key, bucket_key)
    out: dict = {}
    for name in BUCKETS:
        sub = [lg for lg in legs if lg[bucket_key] == name]
        rets = [lg["ret_pct"] for lg in sub]
        excess = [lg[f"excess_{ratio_key}"] for lg in sub
                  if lg[f"excess_{ratio_key}"] is not None]
        out[name] = {
            "n_legs": len(sub),
            "n_events": len({lg["event_date"] for lg in sub}),
            "mean_ratio": round(
                sum(lg[ratio_key] for lg in sub) / len(sub), 3
            ) if sub else None,
            "mean_ret_pct": round(sum(rets) / len(rets), 3) if rets else None,
            "median_ret_pct": round(
                float(pd.Series(rets).median()), 3
            ) if rets else None,
            "win_rate_pct": round(
                100.0 * sum(1 for r in rets if r > 0) / len(rets), 2
            ) if rets else None,
            "mean_excess_pct": round(
                sum(excess) / len(excess), 3
            ) if excess else None,
        }
    return out


def spearman(legs: list[dict], ratio_key: str) -> float | None:
    if len(legs) < 5:
        return None
    df = pd.DataFrame(
        {"r": [lg[ratio_key] for lg in legs], "ret": [lg["ret_pct"] for lg in legs]}
    )
    if df["r"].std() == 0 or df["ret"].std() == 0:
        return None
    return round(float(df["r"].rank().corr(df["ret"].rank())), 3)


def filter_sim(legs: list[dict], ratio_key: str, bucket_key: str) -> dict:
    """Descriptive filter simulation: event return = equal-weight mean of the
    event's ratio-covered legs (baseline) vs only legs with ratio >= LOW
    (drop_low) / only ratio > HIGH (high_only). Events left with no legs are
    dropped from the filtered variant."""
    by_event: dict[str, list[dict]] = {}
    for lg in legs:
        by_event.setdefault(lg["event_date"], []).append(lg)

    def event_rets(keep_fn) -> tuple[list[float], int]:
        rets, dropped = [], 0
        for ev_legs in by_event.values():
            kept = [lg["ret_pct"] for lg in ev_legs if keep_fn(lg)]
            if kept:
                rets.append(sum(kept) / len(kept))
            else:
                dropped += 1
        return rets, dropped

    def stats(rets: list[float], dropped: int) -> dict:
        return {
            "n_events": len(rets),
            "n_events_dropped": dropped,
            "mean_event_ret_pct": round(sum(rets) / len(rets), 3) if rets else None,
            "median_event_ret_pct": round(
                float(pd.Series(rets).median()), 3
            ) if rets else None,
            "win_rate_pct": round(
                100.0 * sum(1 for r in rets if r > 0) / len(rets), 2
            ) if rets else None,
        }

    base, _ = event_rets(lambda lg: True)
    drop_low, d1 = event_rets(lambda lg: lg[ratio_key] >= LOW)
    high_only, d2 = event_rets(lambda lg: lg[ratio_key] > HIGH)
    return {
        "baseline_all_legs": stats(base, 0),
        "drop_low_volume_legs": stats(drop_low, d1),
        "high_volume_legs_only": stats(high_only, d2),
        "note": (
            "event_day variant is NOT executable at the entry open (ratio only "
            "known at the close — look-ahead); prior_day variant is known "
            "before the open"
        ),
    }


def per_symbol_sanity(legs: list[dict]) -> dict:
    out: dict = {}
    for sym in TARGETS:
        sub = [lg for lg in legs if lg["symbol"] == sym]
        if not sub:
            continue
        out[sym] = {
            "n_legs": len(sub),
            "mean_ratio_event_day": round(
                sum(lg["ratio_event_day"] for lg in sub) / len(sub), 3
            ),
            "pct_in_high_bucket": round(
                100.0 * sum(1 for lg in sub if lg["bucket_event_day"] == "high")
                / len(sub), 1
            ),
            "mean_ret_pct": round(
                sum(lg["ret_pct"] for lg in sub) / len(sub), 3
            ),
        }
    return out


def summarize(title: str, legs: list[dict], n_missing: int) -> dict:
    ed_stats = bucket_stats(legs, "ratio_event_day", "bucket_event_day")
    pd_stats = bucket_stats(legs, "ratio_prior_day", "bucket_prior_day")
    result = {
        "n_events": len({lg["event_date"] for lg in legs}),
        "n_legs": len(legs),
        "n_legs_skipped_no_ratio": n_missing,
        "bucket_stats_event_day": ed_stats,
        "bucket_stats_prior_day": pd_stats,
        "spearman_ratio_vs_ret": {
            "event_day": spearman(legs, "ratio_event_day"),
            "prior_day": spearman(legs, "ratio_prior_day"),
        },
        "filter_sim_event_day": filter_sim(
            legs, "ratio_event_day", "bucket_event_day"
        ),
        "filter_sim_prior_day": filter_sim(
            legs, "ratio_prior_day", "bucket_prior_day"
        ),
        "per_symbol_sanity": per_symbol_sanity(legs),
        "detail": legs,
    }
    print(f"\n  {title}: {result['n_events']} 事件 / {result['n_legs']} 腿 "
          f"(缺量比跳过 {n_missing})")
    for variant, key in (("事件日量比", "bucket_stats_event_day"),
                         ("前一日量比", "bucket_stats_prior_day")):
        print(f"    {variant}:")
        for b in BUCKETS:
            s = result[key][b]
            print(f"      {b:4s}: n腿 {s['n_legs']:>3d} ({s['n_events']:>3d}事件) "
                  f"均量比 {s['mean_ratio']} | T+2 均值 {s['mean_ret_pct']}% "
                  f"中位 {s['median_ret_pct']}% 胜率 {s['win_rate_pct']}% | "
                  f"事件内超额 {s['mean_excess_pct']}%")
        rho = result["spearman_ratio_vs_ret"][
            "event_day" if key.endswith("event_day") else "prior_day"
        ]
        print(f"      Spearman(量比, T+2收益) = {rho}")
    fs = result["filter_sim_event_day"]
    print(f"    过滤模拟(事件日, 描述性): 基线 {fs['baseline_all_legs']['mean_event_ret_pct']}%"
          f"/胜率{fs['baseline_all_legs']['win_rate_pct']}% | "
          f"去低量 {fs['drop_low_volume_legs']['mean_event_ret_pct']}%"
          f"/胜率{fs['drop_low_volume_legs']['win_rate_pct']}% "
          f"(弃{fs['drop_low_volume_legs']['n_events_dropped']}事件) | "
          f"只高量 {fs['high_volume_legs_only']['mean_event_ret_pct']}%"
          f"/胜率{fs['high_volume_legs_only']['win_rate_pct']}%")
    fs = result["filter_sim_prior_day"]
    print(f"    过滤模拟(前一日, 可执行): 基线 {fs['baseline_all_legs']['mean_event_ret_pct']}%"
          f"/胜率{fs['baseline_all_legs']['win_rate_pct']}% | "
          f"去低量 {fs['drop_low_volume_legs']['mean_event_ret_pct']}%"
          f"/胜率{fs['drop_low_volume_legs']['win_rate_pct']}% "
          f"(弃{fs['drop_low_volume_legs']['n_events_dropped']}事件) | "
          f"只高量 {fs['high_volume_legs_only']['mean_event_ret_pct']}%"
          f"/胜率{fs['high_volume_legs_only']['win_rate_pct']}%")
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    print("=" * 110)
    print("  weekend_gap / overnight_gap 成交量确认分析 — 事件日量比分桶 (T+2)")
    print(f"  量比 = 当日量 / 前{VOL_LOOKBACK}日均量 (严格用事件日之前数据); "
          f"桶: 高 >{HIGH} / 中 {LOW}-{HIGH} / 低 <{LOW}")
    print("=" * 110)

    volume = load_volume()

    report: dict = {
        "config": {
            "question": "事件日股票成交量是否包含 weekend_gap/overnight_gap "
                        "T+2 收益的增量信息",
            "volume_ratio": {
                "event_day": f"event-day volume / mean of prior "
                             f"{VOL_LOOKBACK} trading days (strictly before)",
                "prior_day": "same, measured on the previous trading day — "
                             "known before the entry open (executable filter)",
                "lookahead_warning": "event_day ratio is only known at the "
                                     "close; using it as an ENTRY filter is "
                                     "look-ahead. Reported as description of "
                                     "the confirmation hypothesis, and as an "
                                     "exit/sizing-relevant feature only.",
            },
            "buckets": {"high": f"> {HIGH}", "mid": f"{LOW} - {HIGH}",
                        "low": f"< {LOW}",
                        "note": "a-priori conventional thresholds, not tuned"},
            "returns": "gross open -> T+2 close, per leg (no costs); "
                       "set B ret_t2_pct as stored (direction-signed, long only)",
            "excess": "leg ret - mean ret of the same event's ratio-covered "
                      "legs (>= 2 legs required)",
            "open_volume_limitation": "日线数据无法分离开盘时段成交量——"
                                      "「开盘量比」不可算，需分钟级数据（未做）",
            "data": "data/btc_history.db ohlcv market_type='stocks' "
                    "(Nasdaq daily, split-adjusted; MSTR 2024-08 10:1 "
                    "verified continuous in price and volume)",
            "honesty": [
                "set A (13 events / 38 legs) is purely descriptive — no "
                "significance claims",
                "set B thresholds are nested samples (5% ⊂ 4% ⊂ 3% ⊂ 2%), "
                "not independent confirmations",
                "no threshold was tuned on these results",
                "low bucket (<0.7) at the 5% set is only 18 legs, 9/18 in "
                "2019, none in 2025-26 — 'low-volume event' has essentially "
                "ceased to exist in the modern sample",
                "MARA has both systematically higher ratios (mean 2.17 vs "
                "~1.5) and higher returns — the high bucket is partly a "
                "symbol proxy; within-event excess controls for this only "
                "partially",
            ],
        },
        "weekend_gap_set": {},
        "overnight_set": {},
    }

    # --- set A: weekend_gap -------------------------------------------------
    wg_legs, wg_missing = weekend_legs(volume)
    report["weekend_gap_set"] = summarize(
        "集合A: weekend_gap (13事件, T+2, long_only)", wg_legs, wg_missing
    )
    weekend_dates = {lg["event_date"] for lg in wg_legs}

    # --- set B: overnight gap, long only, all thresholds --------------------
    report["overnight_set"] = {}
    for thr in OVERNIGHT_THRESHOLDS:
        legs, missing = overnight_legs(volume, thr, weekend_dates)
        report["overnight_set"][thr] = summarize(
            f"集合B: overnight(market_close, {thr}%, long_only)", legs, missing
        )
        n_overlap = sum(1 for lg in legs if lg.get("weekend_gap_day"))
        report["overnight_set"][thr]["n_weekend_gap_overlap_legs"] = n_overlap

    # --- conclusion (filled from the headline numbers; wording kept honest) -
    b5 = report["overnight_set"][HEADLINE_THRESHOLD]["bucket_stats_event_day"]
    b5p = report["overnight_set"][HEADLINE_THRESHOLD]["bucket_stats_prior_day"]
    fs_ed = report["overnight_set"][HEADLINE_THRESHOLD]["filter_sim_event_day"]
    fs_pd = report["overnight_set"][HEADLINE_THRESHOLD]["filter_sim_prior_day"]

    def delta_hi_lo(stats: dict) -> float | None:
        hi, lo = stats["high"]["mean_ret_pct"], stats["low"]["mean_ret_pct"]
        return round(hi - lo, 3) if hi is not None and lo is not None else None

    report["conclusion"] = {
        "headline": f"overnight(market_close 5%, long_only): 事件日量比 "
                    f"high桶均值 {b5['high']['mean_ret_pct']}% vs low桶 "
                    f"{b5['low']['mean_ret_pct']}% (差 {delta_hi_lo(b5)}pp); "
                    f"前一日量比 high {b5p['high']['mean_ret_pct']}% vs low "
                    f"{b5p['low']['mean_ret_pct']}% (差 {delta_hi_lo(b5p)}pp)",
        "filter_effect_event_day": {
            "baseline": fs_ed["baseline_all_legs"]["mean_event_ret_pct"],
            "drop_low": fs_ed["drop_low_volume_legs"]["mean_event_ret_pct"],
            "high_only": fs_ed["high_volume_legs_only"]["mean_event_ret_pct"],
        },
        "filter_effect_prior_day": {
            "baseline": fs_pd["baseline_all_legs"]["mean_event_ret_pct"],
            "drop_low": fs_pd["drop_low_volume_legs"]["mean_event_ret_pct"],
            "high_only": fs_pd["high_volume_legs_only"]["mean_event_ret_pct"],
        },
        "verdict": (
            "事件日量比与 T+2 收益在描述性层面有一致的单调正向关系：集合B(5%, "
            "234腿) 高/中/低桶均值 +3.82%/+1.86%/-1.95%，事件内超额同向，"
            "Spearman≈0.14，且 2/3/4% 阈值方向全部一致；集合A(38腿) 高桶 "
            "6.83% vs 中桶 2.97% 方向一致，但事件内超额反向、且无低量桶"
            "（weekend_gap 事件日量比最低 0.75，低量事件在周末事件里不存在），"
            "n 太小仅算'不矛盾'。但两个硬约束使'高量确认'无法落地为入场规则："
            "(1) 事件日量比收盘才可知——作为入场过滤是前视；可执行的'前一日"
            "量比'在所有阈值上均无信息（平坦~反向，5% 处 Spearman -0.07，"
            "按其过滤后收益均下降），即量价配合是同日并发的、不可预判；"
            "(2) 低量桶在 5% 事件集仅 18 腿、9/18 集中在 2019、2025-26 已"
            "基本不存在，'低量降权'在当代样本无对象。一句话结论：量比有"
            "一致的描述性方向（高量事件延续更好），但无可执行入场增量信息；"
            "若要用，只能放出场/日内侧（例：事件日收盘量比<0.7 且持仓浮亏 → "
            "提前至 T+1 出场——未回测，仅为雏形建议）。注意 MARA 量比与收益"
            "均系统性偏高，高量桶部分是 symbol 代理。"
        ),
    }

    out_path = ROOT / "reports/volume_confirm.json"
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    print(f"\nJSON 已写入: {out_path}")


if __name__ == "__main__":
    main()
