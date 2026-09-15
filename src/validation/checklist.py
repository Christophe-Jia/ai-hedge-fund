"""Red-team checklist: the questions every strategy report must answer.

Motivation (the 2026-09 GBM credibility crisis): the flagship report was
technically rigorous about *cheating* (no look-ahead, locked hyperparameters,
full cost model) and therefore felt trustworthy — but it never answered the
questions that would have exposed it as *noise*.  This checklist turns those
questions into machine-checkable report fields, so a report cannot be "done"
without at least declaring them.
"""

from __future__ import annotations

from typing import Any

from ._extract import dig

# Each item: what must be answered, where it lives in the report JSON, how much
# it matters, and the concrete failure it exists to prevent.
RED_TEAM_QUESTIONS: list[dict] = [
    {
        "id": "provenance",
        "question": "脚本与生成时间是什么？（精确到脚本路径 + UTC 时间戳）",
        "paths": ["meta.script", "meta.generated_at"],
        "mode": "all",
        "severity": "high",
        "why": "没有生成时间就无法判断报告是否早于后来的数据修正。GBM 危机里补 21 只股票(含 BAYRY 一行)就翻转整月选股，没有版本戳就无法复现'同一策略'。",
    },
    {
        "id": "data_window",
        "question": "数据窗口（起止日期）是什么？",
        "paths": ["meta.data_range", "meta.data_end", "meta.window", "window.range", "config.start", "config.range"],
        "mode": "any",
        "severity": "high",
        "why": "换测试窗起点(2020-10 → 2021-05)会把同一策略的 IC 从 +0.0082 翻到 -0.0076；窗口本身必须写进报告。",
    },
    {
        "id": "price_source",
        "question": "价格数据源与调整口径是什么（复权/分红/时区）？",
        "paths": ["meta.price_data", "meta.price_source", "meta.panel", "conventions.price_note", "conventions.data_note"],
        "mode": "any",
        "severity": "medium",
        "why": "Nasdaq 与 IBKR 的口径差异、外国同名 ticker 污染、负缓存都会静默改变结论。",
    },
    {
        "id": "universe_snapshot",
        "question": "股票池是否为 point-in-time 快照，是否有 survivorship 说明？",
        "paths": ["meta.universe.point_in_time", "meta.universe.snapshot", "meta.universe.pit", "coverage_and_survivorship"],
        "mode": "any",
        "severity": "high",
        "why": "用今天的成分股回测十年前会系统性高估；池子覆盖面必须显式声明(GBM 曾漏掉 161/736 个符号)。",
    },
    {
        "id": "significance",
        "question": "头号指标的 t 值 / 标准误 / 置信区间是多少（不只是均值）？",
        "paths": ["monthly_ic.t_stat", "monthly_ic.std", "monthly_ic.se", "significance.t_stat", "ic_significance.t_stat", "validation.significance"],
        "mode": "any",
        "severity": "high",
        "why": "GBM 报告只有'月均 IC 0.0082'没有 SE；真实 SE≈0.0095，t≈0.84 —— 与零无法区分。均值旁边必须并列标准误。",
    },
    {
        "id": "window_stability",
        "question": "换测试窗后的指标分布与符号一致率是多少？",
        "paths": ["validation.multi_window", "window_stability", "stability.sign_consistency", "multi_window.sign_consistency", "sign_consistency"],
        "mode": "any",
        "severity": "high",
        "why": "只给一个窗口(或只做窗口切片)会掩盖符号翻转；必须报告跨窗符号一致率(≥80% 才算稳)。",
    },
    {
        "id": "score_distribution",
        "question": "分数分布如何：唯一值数量、并列比例、top-N 边界并列数？",
        "paths": ["validation.boundary_stability", "boundary_stability", "score_distribution", "tie_ratio", "selection_boundary"],
        "mode": "any",
        "severity": "high",
        "why": "GBM 模型分数大量并列(5 只完全相同)，top-10 边界由排序平局决定 → 名单可任意翻转。不报分数分布就永远查不出来。",
    },
    {
        "id": "baseline_significance",
        "question": "与最简单基线的差异是否显著（t 值 / 置信区间）？",
        "paths": ["baseline_significance", "vs_baseline.t_stat", "benchmark_significance", "significance_vs_momentum", "relative_significance"],
        "mode": "any",
        "severity": "high",
        "why": "GBM 只在全窗 Sharpe 上'打败'动量，月度超额 t≈1.84(<2)，且 2024+ 累计超额 -29% —— 缺显著性检验就会把噪声当 alpha。",
    },
    {
        "id": "multiple_comparison",
        "question": "一共试了多少个配置/变体？是否做了多重比较修正？",
        "paths": ["multiple_comparison", "n_configs_tested", "multiple_comparison_warning", "false_discovery_rate", "bonferroni"],
        "mode": "any",
        "severity": "medium",
        "why": "试 15 个变体挑最好的，等于自带选择偏差；最优点必须先过邻域平滑检验。",
    },
    {
        "id": "cost_sensitivity",
        "question": "成本敏感性如何（零成本 vs 高成本、成本拖累 bps/年）？",
        "paths": ["cost_sensitivity", "cost_scenarios", "zero_cost", "cost_sensitivity_bps", "decomposition.cost_saved_bps_yr"],
        "mode": "any",
        "severity": "medium",
        "why": "高换手策略的净收益可能全被成本吃掉；必须给出成本敏感性而不只是单一成本假设。",
    },
    {
        "id": "cost_reporting",
        "question": "是否报告了实际成本（总额 / bps per rebalance / 成本模型）？",
        "paths": ["comparison_full_test_window.gbm_top10.total_costs", "total_costs_usd", "config.cost_model", "cost_bps_per_rebalance", "costs"],
        "mode": "any",
        "severity": "medium",
        "why": "没有成本的回测收益不可交易。",
    },
    {
        "id": "out_of_sample",
        "question": "是否存在真正的样本外验证（walk-forward / holdout / 时间切分）？",
        "paths": ["walk_forward", "out_of_sample", "holdout", "windows.out_of_sample", "time_split", "loo_cv"],
        "mode": "any",
        "severity": "medium",
        "why": "样本内最优不等于可交易；必须声明样本外口径。",
    },
    {
        "id": "conventions",
        "question": "交易约定是否显式写明（信号时点、执行时点、做空/成本/平仓规则）？",
        "paths": ["conventions", "config.conventions", "config.execution", "config.rules"],
        "mode": "any",
        "severity": "medium",
        "why": "执行窗差一根 K 线就能翻转结论(execution_lag_bars 是真实的工程陷阱)。",
    },
    {
        "id": "verdict",
        "question": "报告是否给出明确的、带方向的结论并在结论里引用上述检验？",
        "paths": ["verdict_2021_plus", "verdict", "findings", "conclusion.verdict", "summary"],
        "mode": "any",
        "severity": "medium",
        "why": "结论必须由检验支撑；GBM 报告曾一边写 OUTPERFORM、一边 IC 与零无异。",
    },
]

VERDICT_PASS = "PASS"
VERDICT_WARN = "WARN"
VERDICT_FAIL = "FAIL"


def red_team_checklist(report: dict, *, questions: list[dict] | None = None) -> dict:
    """Check which mandatory red-team questions a report answers.

    Presence is judged structurally (does the report JSON contain the field?), so
    it is a necessary but not sufficient condition: a field can be present and
    still be wrong.  The point is that *absence* is unambiguous and must block.
    """
    items = questions if questions is not None else RED_TEAM_QUESTIONS
    results: list[dict] = []
    high_missing: list[str] = []
    medium_missing: list[str] = []

    for q in items:
        present_paths = [p for p in q["paths"] if dig(report, p) is not None]
        if q.get("mode", "any") == "all":
            answered = len(present_paths) == len(q["paths"])
        else:
            answered = len(present_paths) > 0
        entry = {
            "id": q["id"],
            "question": q["question"],
            "severity": q["severity"],
            "answered": answered,
            "matched_paths": present_paths,
            "missing_paths": [p for p in q["paths"] if p not in present_paths],
            "why": q["why"],
        }
        results.append(entry)
        if not answered:
            (high_missing if q["severity"] == "high" else medium_missing).append(q["id"])

    n_items = len(results)
    n_answered = sum(1 for r in results if r["answered"])
    if high_missing:
        verdict = VERDICT_FAIL
    elif medium_missing:
        verdict = VERDICT_WARN
    else:
        verdict = VERDICT_PASS

    return {
        "n_questions": n_items,
        "n_answered": n_answered,
        "n_unanswered": n_items - n_answered,
        "coverage": n_answered / n_items if n_items else 0.0,
        "high_severity_unanswered": high_missing,
        "medium_severity_unanswered": medium_missing,
        "verdict": verdict,
        "items": results,
    }


def render_checklist(report: dict | None = None) -> list[str]:
    """Human-readable checklist lines (markdown), optionally annotated per report."""
    lines = ["| # | 必答问题 | 严重度 | 状态 |", "|---|---|---|---|"]
    checked = None
    if isinstance(report, dict):
        checked = {r["id"]: r for r in red_team_checklist(report)["items"]}
    for i, q in enumerate(RED_TEAM_QUESTIONS, start=1):
        status = "—"
        if checked is not None:
            status = "PASS" if checked[q["id"]]["answered"] else "MISSING"
        lines.append(f"| {i} | {q['question']} | {q['severity']} | {status} |")
    return lines


def unanswered(report: dict) -> list[Any]:
    """Convenience: ids of unanswered questions (high severity first)."""
    res = red_team_checklist(report)
    return list(res["high_severity_unanswered"]) + list(res["medium_severity_unanswered"])
