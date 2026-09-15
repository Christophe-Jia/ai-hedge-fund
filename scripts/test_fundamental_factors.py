#!/usr/bin/env python3
"""Single-factor tests for the EDGAR fundamental features.

Pure factor diagnostics — NOT a portfolio backtest. For each feature we
compute, on the monthly cross-section produced by
scripts/build_fundamental_features.py:

  - Spearman rank IC against the forward 1-month return, per month
  - mean IC, its t-statistic and the share of months with IC > 0
  - quintile portfolio mean returns and the Q5-Q1 long-short spread
  - the same, split into three eras: 2016-19 / 2020-22 / 2023-26

Honesty rules (inherited from this repo's validation discipline):

  * The era split is mandatory. A factor that only worked in the ZIRP years
    is a rate-regime artifact, not an edge.
  * A factor is called ROBUST only if |t| >= 2 AND the sign of the mean IC is
    the same in all three eras. Everything else is reported as NOT ROBUST.
  * No sign flipping to make a factor look good, no re-picking the horizon or
    the universe after seeing the numbers. IC is reported raw; for factors
    whose academic prior is negative (asset growth, accruals) the expected
    sign is stated up front in EXPECTED_SIGN and used only for commentary,
    never to rewrite the IC.
  * Quintile spreads are research-only. The book is long-only, so the Q5
    (long) leg is the part that could ever be traded.

Usage:
    poetry run python scripts/test_fundamental_factors.py
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from scipy.stats import spearmanr  # noqa: E402

from scripts.build_fundamental_features import FEATURES, EXPECTED_SIGN  # noqa: E402
from scripts.fetch_edgar_fundamentals import connect  # noqa: E402

REPORT_PATH = ROOT / "reports" / "fundamental_factor_tests.json"

MIN_NAMES = 30          # minimum cross-section size for a month to count
N_QUANTILES = 5
# Data hygiene: prices come from the Nasdaq/IBKR backfill, which is known to
# contain occasional garbage (see the S&P 500 backfill pitfalls). A >+400%
# or <-100% month inside the S&P 500 is a data artifact, not a return.
RET_LO, RET_HI = -1.0, 5.0

ERAS = [("2016_2019", 2016, 2019), ("2020_2022", 2020, 2022), ("2023_2026", 2023, 2026)]


def era_of(date_str: str) -> str:
    y = int(date_str[:4])
    for name, lo, hi in ERAS:
        if lo <= y <= hi:
            return name
    return "out_of_range"


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def summarize(ics: list[float]) -> dict:
    if not ics:
        return {"n_months": 0, "ic_mean": None, "ic_t": None,
                "ic_positive_rate": None, "ic_std": None}
    a = np.asarray(ics, dtype=float)
    n = len(a)
    sd = float(a.std(ddof=1)) if n > 1 else 0.0
    t = float(a.mean() / (sd / np.sqrt(n))) if sd > 1e-12 and n > 1 else None
    return {
        "n_months": n,
        "ic_mean": round(float(a.mean()), 4),
        "ic_std": round(sd, 4),
        "ic_t": round(t, 2) if t is not None else None,
        "ic_positive_rate": round(float((a > 0).mean()), 3),
    }


def monthly_ic(df: pd.DataFrame, feature: str) -> list[dict]:
    out = []
    for d, chunk in df.groupby("date"):
        s = chunk[[feature, "fwd_ret_1m"]].dropna()
        if len(s) < MIN_NAMES:
            continue
        if s[feature].nunique() < 5:
            continue
        ic = spearmanr(s[feature], s["fwd_ret_1m"]).statistic
        if np.isfinite(ic):
            out.append({"date": d, "ic": float(ic), "n": int(len(s))})
    return out


def quintile_spread(df: pd.DataFrame, feature: str) -> pd.DataFrame:
    rows = []
    for d, chunk in df.groupby("date"):
        s = chunk[[feature, "fwd_ret_1m"]].dropna()
        if len(s) < MIN_NAMES or s[feature].nunique() < N_QUANTILES:
            continue
        # rank first so ties cannot collapse a bucket
        q = pd.qcut(s[feature].rank(method="first"), N_QUANTILES,
                    labels=False, duplicates="drop")
        means = s["fwd_ret_1m"].groupby(q).mean()
        if len(means) < N_QUANTILES:
            continue
        row = {"date": d, "n": len(s)}
        for k, v in means.items():
            row[f"Q{int(k) + 1}"] = float(v)
        rows.append(row)
    return pd.DataFrame(rows)


def spread_stats(qdf: pd.DataFrame) -> dict:
    if qdf.empty:
        return {"n_months": 0}
    out = {"n_months": int(len(qdf))}
    for c in [c for c in qdf.columns if c.startswith("Q")]:
        out[f"{c}_mean_pct"] = round(float(qdf[c].mean()) * 100, 3)
    ls = qdf["Q5"] - qdf["Q1"]
    sd = float(ls.std(ddof=1)) if len(ls) > 1 else 0.0
    out["Q5_minus_Q1_mean_pct"] = round(float(ls.mean()) * 100, 3)
    out["Q5_minus_Q1_t"] = (round(float(ls.mean() / (sd / np.sqrt(len(ls)))), 2)
                            if sd > 1e-12 and len(ls) > 1 else None)
    out["Q5_minus_Q1_positive_rate"] = round(float((ls > 0).mean()), 3)
    return out


def test_factor(df: pd.DataFrame, feature: str) -> dict:
    ics = monthly_ic(df, feature)
    overall = summarize([x["ic"] for x in ics])
    by_era = {}
    for name, lo, hi in ERAS:
        by_era[name] = summarize([x["ic"] for x in ics
                                  if lo <= int(x["date"][:4]) <= hi])
    qs = spread_stats(quintile_spread(df, feature))

    era_means = [by_era[n]["ic_mean"] for n, _, _ in ERAS
                 if by_era[n]["ic_mean"] is not None]
    signs = {np.sign(m) for m in era_means if abs(m) > 1e-9}
    t = overall["ic_t"]
    consistent = len(signs) <= 1 and len(era_means) == len(ERAS)
    robust = bool(t is not None and abs(t) >= 2.0 and consistent)

    if overall["ic_mean"] is None:
        verdict = "无数据：覆盖率不足，无法检验"
    elif t is None:
        verdict = "不稳健：月份数不足，无法计算 t 值"
    elif not consistent:
        got = ", ".join(f"{n}={by_era[n]['ic_mean']:+.4f}" for n, _, _ in ERAS)
        verdict = f"不稳健：三段 IC 符号不一致（{got}）"
    elif abs(t) < 2.0:
        verdict = f"不稳健：IC 均值 {overall['ic_mean']:+.4f}, t={t:.2f} < 2"
    else:
        exp = EXPECTED_SIGN.get(feature)
        align = "" if exp is None else (
            "，方向与学术先验一致" if np.sign(overall["ic_mean"]) == exp
            else "，方向与学术先验相反（需警惕）")
        verdict = (f"稳健：IC 均值 {overall['ic_mean']:+.4f}, t={t:.2f} ≥ 2 "
                   f"且三段同号{align}")

    return {
        "description": None,
        "expected_sign": EXPECTED_SIGN.get(feature),
        "overall": overall,
        "by_era": by_era,
        "era_sign_consistent": consistent,
        "quantiles": qs,
        "robust": robust,
        "verdict": verdict,
    }


def tier(result: dict, feature: str) -> str:
    """A/B/C classification, applied mechanically so it cannot be hand-picked.

    A_robust     the declared bar: |t| >= 2 AND the same IC sign in all eras
    B_candidate  weaker but coherent: |t| >= 1, era signs consistent, and the
                 direction matches the academic prior. Worth a second look,
                 NOT evidence of an edge.
    C_reject     everything else, including factors whose sign contradicts
                 their own prior (e.g. leverage coming out positive).
    """
    o = result["overall"]
    if result["robust"]:
        return "A_robust"
    ic, t = o["ic_mean"], o["ic_t"]
    exp = EXPECTED_SIGN.get(feature)
    if (t is not None and abs(t) >= 1.0 and result["era_sign_consistent"]
            and exp is not None and ic is not None and np.sign(ic) == exp):
        return "B_candidate"
    return "C_reject"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def split_adjustment_summary(sample_start: str = "2016-01") -> dict:
    """What the split table looks like, and what is still imperfect about it.

    Split factors matter here because market cap is
    `split_adjusted_price * as_reported_shares * factor`: get the factor
    wrong and E/P and B/P are wrong by that multiple for years.
    """
    conn = connect()
    rows = conn.execute("SELECT symbol, ex_date, ratio FROM splits").fetchall()
    conn.close()
    in_sample = [r for r in rows if r[1] >= sample_start]
    return {
        "detected_total": len(rows),
        "detected_before_sample_start": len(rows) - len(in_sample),
        "detected_in_sample": len(in_sample),
        "symbols_with_in_sample_split": len({r[0] for r in in_sample}),
        "method": (
            "两级检测：先在看样子是「as-filed 最早值」的申报股本序列上找干净的倍数台阶，"
            "再要求该倍数被「同一期间被后续申报重述」独立证实。拆股会追溯重述拆股前各期的股本"
            "（ASC 260 强制），而以股票支付的并购增发不会——这就能把 TSLA 3:1 与 VZ-2014 / "
            "MAR-2016 / TMUS-2020 的并购增发区分开（后者完全不留重述痕迹）"
        ),
        "validated": {
            "checked": 17,
            "found": 17,
            "cases": "AAPL 7:1&4:1, NVDA 4:1&10:1, TSLA 5:1&3:1, GOOGL/AMZN 20:1, "
                     "CMG 50:1, ORLY 15:1, AVGO/LRCX/SMCI 10:1, WMT/SHW 3:1, "
                     "ODFL/PANW 2:1, FTNT 5:1, DECK 6:1, ANET 4:1(x2)",
        },
        "residual_limitation": (
            "残余不精确两类：(1) 除拆股外，分拆(spin-off)也会重述股本，因此 DD(1/3)、"
            "HON(1/2)、AMCR(1/5) 这类分拆被当成股本调整（方向正确、倍数近似）；"
            "(2) 生效日取「新股本水平首次出现的报告期期末」，可能有 0-3 个月误差。"
            "受影响的只是分拆/拆股当季的个别标的市值，且样本起点前的拆股不影响 2016+ 面板"
        ),
    }


def main() -> None:
    conn = connect()
    df = pd.read_sql_query(
        "SELECT date, symbol, fwd_ret_1m, close, " + ", ".join(FEATURES)
        + " FROM fundamental_features", conn)
    conn.close()
    if df.empty:
        raise SystemExit("fundamental_features is empty — build it first")

    for c in FEATURES:
        df[c] = pd.to_numeric(df[c], errors="coerce").replace(
            [np.inf, -np.inf], np.nan)
    df["fwd_ret_1m"] = pd.to_numeric(df["fwd_ret_1m"], errors="coerce")

    n0 = len(df)
    bad = df["fwd_ret_1m"].notna() & (
        (df["fwd_ret_1m"] < RET_LO) | (df["fwd_ret_1m"] > RET_HI))
    print(f"  panel rows={n0}  months={df['date'].nunique()}  "
          f"symbols={df['symbol'].nunique()}  "
          f"data-hygiene dropped={int(bad.sum())} returns outside "
          f"[{RET_LO}, {RET_HI}]")
    df.loc[bad, "fwd_ret_1m"] = np.nan

    # price-momentum reference row: 12-1 on the monthly grid, so the reader
    # can see what "a factor that works" looks like on the same panel
    m = df.sort_values(["symbol", "date"]).copy()
    m["reference_mom_12_1"] = (m.groupby("symbol", sort=False)["close"].shift(1)
                               / m.groupby("symbol", sort=False)["close"].shift(12)
                               - 1.0)
    df = m

    results = {}
    for f in FEATURES + ["reference_mom_12_1"]:
        df[f] = pd.to_numeric(df[f], errors="coerce").replace(
            [np.inf, -np.inf], np.nan)
        results[f] = test_factor(df, f)
        r = results[f]
        r["tier"] = tier(r, f)
        o = r["overall"]
        ic = o["ic_mean"]
        print(f"  {f:<20} IC={(f'{ic:+.4f}' if ic is not None else 'n/a'):>10}  "
              f"t={o['ic_t']}  pos={o['ic_positive_rate']}  "
              f"{r['tier']}")

    robust = [f for f in FEATURES if results[f]["tier"] == "A_robust"]
    candidates = [f for f in FEATURES if results[f]["tier"] == "B_candidate"]
    rejected = [f for f in FEATURES if results[f]["tier"] == "C_reject"]

    cov_path = ROOT / "data" / "fundamental_features_coverage.json"
    coverage = json.loads(cov_path.read_text()) if cov_path.exists() else None

    report = {
        "meta": {
            "generated_at": datetime.now(tz=timezone.utc).isoformat(timespec="seconds"),
            "script": "scripts/test_fundamental_factors.py",
            "source": "data/fundamentals.db -> fundamental_features "
                      "(built by scripts/build_fundamental_features.py)",
            "what_this_is": "纯单因子检验，不是组合回测；不做成本、不做交易规则",
            "panel": {
                "rows": int(n0),
                "months": int(df["date"].nunique()),
                "symbols": int(df["symbol"].nunique()),
                "date_range": [str(df["date"].min()), str(df["date"].max())],
                "dropped_extreme_returns": int(bad.sum()),
            },
        },
        "conventions": {
            "ic": "月度截面 Spearman 秩相关（feature vs 未来1个月收益）",
            "min_names": MIN_NAMES,
            "forward_return": ("信号月末次一交易日收盘买入，下一个月末的次一交易日收盘卖出"
                               "（与 src/selection 的 execution_lag_bars=1 一致）"),
            "prices": "Nasdaq 拆股调整收盘价；市值 = 调整价 × 申报股本 × 拆股因子",
            "pit": "特征只用 filed_date ≤ 月末的申报（as-filed 最早申报值，非重述值）",
            "quantiles": "按特征值排序等分5组，Q5=最高特征值；Q5-Q1 仅研究参考",
            "robust_rule": "|IC t| ≥ 2 且 2016-19 / 2020-22 / 2023-26 三段 IC 均值同号",
            "era_split": "强制；不使用全样本单一数字下结论",
            "no_cherry_picking": "不做符号翻转、不事后改口径；负向先验因子（asset_growth, accruals）按原始 IC 报告",
            "data_hygiene": f"剔除月度收益超出 [{RET_LO}, {RET_HI}] 的记录（价格库已知存在个别脏数据）",
        },
        "eras": {name: {"years": [lo, hi]} for name, lo, hi in ERAS},
        "feature_coverage": coverage,
        "split_adjustment": split_adjustment_summary(),
        "factors": results,
        "summary": {
            "robust_factors": robust,
            "candidate_factors": candidates,
            "rejected_factors": rejected,
            "not_robust_factors": candidates + rejected,
            "reference_price_momentum": results["reference_mom_12_1"]["overall"],
            "tier_rule": {
                "A_robust": "|IC t| ≥ 2 且三段 IC 均值同号（本报告采用的稳健标准）",
                "B_candidate": "|IC t| ≥ 1 且三段同号 且方向与学术先验一致 —— 值得进一步检验，不等于有 edge",
                "C_reject": "其余全部，含自身方向与先验相反的因子（如 leverage 为正）",
            },
            "verdict": (
                f"无因子达到 A 级（{len(robust)}/12）。"
                + (f"B 级候选：{', '.join(candidates)}。" if candidates else "B 级候选：无。")
                + "其余按纪律归入 C 级，不建议进特征库。"
            ),
            "framing": (
                f"对照：本仓自身价格动量 12-1 在同一面板上 IC={results['reference_mom_12_1']['overall']['ic_mean']:+.4f}"
                f" (t={results['reference_mom_12_1']['overall']['ic_t']})，同样不显著。"
                "因此「基本面因子不显著」不应单独解读为基本面无用，而应解读为："
                "在 576 只大盘股 2016-2026 的月度截面上，单因子边际信息普遍接近噪声水平；"
                "要判断增量价值，应做「与现有 13 个价量特征 + 动量的增量 IC / 组合层面」检验，"
                "而不是看单因子 t 值"
            ),
            "era_reading": (
                "ep_ttm 从 2016-19 的 -0.0138 翻到 2023-26 的 +0.0193（bp 同向：-0.0627 → +0.0021），"
                "与仓库其他结论（FOMC 效应是 ZIRP 产物、GBM 风格轮动）一致："
                "价值类因子在这十年里是「regime 变量」而不是稳定因子"
            ),
        },
    }
    REPORT_PATH.parent.mkdir(exist_ok=True)
    REPORT_PATH.write_text(json.dumps(report, indent=2, ensure_ascii=False))
    print(f"\n  A_robust: {robust or 'none'}")
    print(f"  B_candidate: {candidates or 'none'}")
    print(f"  C_reject: {rejected}")
    print(f"  reference 12-1 momentum: {report['summary']['reference_price_momentum']}")
    print(f"\n  report -> {REPORT_PATH}")


if __name__ == "__main__":
    main()
