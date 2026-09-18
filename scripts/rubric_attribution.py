#!/usr/bin/env python3
"""Rubric attribution — which due-diligence dimensions actually track survival?

This is the rubric-search counterpart: with the 27 historical hypotheses
backfilled into ``hypotheses/registry.jsonl``, correlate each rubric dimension's
ex-ante score with whether the hypothesis ultimately survived
(:func:`src.validation.registry.survival_label`).

**This is exploratory, not a conclusion.**  n ≈ 27 binary outcomes, 10
dimensions: under the null the largest of 10 correlations is expected to be
substantial by chance alone, so any "winner" here is a lead for a future rubric
weight iteration, never a validated finding.  The report states this explicitly
and applies a Bonferroni threshold and BH-FDR on top.

Output: ``reports/rubric_attribution.json``.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.validation.registry import load_registry, survival_label  # noqa: E402
from src.validation.rubric import DIMENSIONS  # noqa: E402
from src.validation.stats import fdr_bh, two_sided_t_p  # noqa: E402

DEFAULT_REGISTRY = "hypotheses/registry.jsonl"
DEFAULT_OUT = "reports/rubric_attribution.json"

EXPLORATORY_WARNING = (
    "探索性分析，不是结论：n≈27 个二元结果、10 个维度，在空假设下 10 个相关系数中"
    "最大值本就可观。任何在此列出的『最有区分度维度』只是给未来 rubric 权重迭代的初始"
    "线索，不得作为已确立的证据引用；须先扩样并在独立假设集上复现。"
)


def _pearson(xs: list[float], ys: list[float]) -> float:
    n = len(xs)
    if n < 3:
        return float("nan")
    mx = sum(xs) / n
    my = sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    syy = sum((y - my) ** 2 for y in ys)
    if sxx == 0 or syy == 0:
        return float("nan")
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    return sxy / math.sqrt(sxx * syy)


def _ranks(values: list[float]) -> list[float]:
    """Average ranks (1-based) with ties handled."""
    order = sorted(range(len(values)), key=lambda i: values[i])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and values[order[j + 1]] == values[order[i]]:
            j += 1
        avg = (i + 1 + j + 1) / 2.0
        for k in range(i, j + 1):
            ranks[order[k]] = avg
        i = j + 1
    return ranks


def _spearman(xs: list[float], ys: list[float]) -> float:
    return _pearson(_ranks(xs), _ranks(ys))


def _critical_r(alpha: float, df: int) -> float | None:
    """Two-sided critical |r| from the normal approximation to the t-test."""
    if df <= 0:
        return None
    z = 1.959963984540054 if abs(alpha - 0.05) < 1e-9 else _z_for(alpha)
    return z / math.sqrt(z * z + df)


def _z_for(alpha: float) -> float:
    from statistics import NormalDist

    return NormalDist().inv_cdf(1.0 - alpha / 2.0)


def analyse(records: list[dict]) -> dict:
    usable = []
    for rec in records:
        label = survival_label(rec)
        answers = (rec.get("rubric") or {}).get("answers") or {}
        if label is None or not answers:
            continue
        usable.append((rec, label, answers))

    y = [float(label) for _, label, _ in usable]
    n = len(usable)
    df = n - 2 if n > 2 else 0

    p_values: list[float] = []
    rows: list[dict] = []
    for d in DIMENSIONS:
        dim_id = d["id"]
        pairs = [(float(a[dim_id]), label) for _, label, a in usable if dim_id in a]
        if len(pairs) < 3:
            rows.append({"id": dim_id, "name": d["name"], "weight": d["weight"], "n_used": len(pairs),
                         "point_biserial": None, "spearman": None, "p_value": None, "note": "insufficient scored entries"})
            continue
        xs = [p[0] for p in pairs]
        ys = [p[1] for p in pairs]
        pb = _pearson(xs, ys)
        sp = _spearman(xs, ys)
        if math.isnan(pb) or df <= 0:
            p_val = None
            t_stat = None
        elif abs(pb) >= 1.0:
            # perfectly separating dimension: t -> inf, p -> 0
            t_stat = math.inf
            p_val = 0.0
        else:
            t_stat = pb * math.sqrt(df / (1 - pb * pb))
            p_val = two_sided_t_p(t_stat, df)
        p_values.append(p_val if p_val is not None else float("nan"))
        surv = [s for s, lab in pairs if lab == 1]
        fail = [s for s, lab in pairs if lab == 0]
        # Leave-one-out fragility: with n≈27 and few survivors, a single record
        # can create a whole correlation.  Report the range of |r| when dropping
        # one record at a time so a fragile lead cannot be read as stable.
        loo = []
        for k in range(len(pairs)):
            sub = pairs[:k] + pairs[k + 1 :]
            if len(sub) < 3:
                continue
            r = _pearson([q[0] for q in sub], [q[1] for q in sub])
            if not math.isnan(r):
                loo.append(abs(r))
        rows.append(
            {
                "id": dim_id,
                "name": d["name"],
                "weight": d["weight"],
                "n_used": len(pairs),
                "n_survived": len(surv),
                "n_failed": len(fail),
                "n_distinct_scores": len(set(xs)),
                "mean_score_survived": (sum(surv) / len(surv)) if surv else None,
                "mean_score_failed": (sum(fail) / len(fail)) if fail else None,
                "point_biserial": pb if not math.isnan(pb) else None,
                "spearman": sp if not math.isnan(sp) else None,
                "t_stat": t_stat,
                "p_value": p_val,
                "loo_abs_r_min": min(loo) if loo else None,
                "loo_abs_r_max": max(loo) if loo else None,
                "direction": ("higher score -> survives" if (pb or 0) > 0 else "higher score -> fails") if not math.isnan(pb) else None,
            }
        )

    # multiplicity
    finite = [p for p in p_values if p is not None and not math.isnan(p)]
    alpha_family = 0.05
    n_dims = len(finite)
    bonf_alpha = alpha_family / n_dims if n_dims else None
    fdr = fdr_bh(finite, alpha=alpha_family) if finite else {"n": 0, "adjusted": [], "n_rejected": 0}

    crit_r_uncorrected = _critical_r(alpha_family, df) if df else None
    crit_r_bonf = _critical_r(bonf_alpha, df) if (df and bonf_alpha) else None

    ranked = sorted(
        [r for r in rows if r.get("point_biserial") is not None],
        key=lambda r: abs(r["point_biserial"]),
        reverse=True,
    )
    for i, r in enumerate(ranked, start=1):
        r["abs_rank"] = i

    best = ranked[0] if ranked else None
    if best:
        loo_lo, loo_hi = best.get("loo_abs_r_min"), best.get("loo_abs_r_max")
        if crit_r_bonf is None:
            crit_text = "Bonferroni 临界 |r| 不可用（无有效维度）"
        elif abs(best["point_biserial"]) >= crit_r_bonf:
            crit_text = f"Bonferroni 临界 |r| ≈ {crit_r_bonf:.3f}；达到校正门槛"
        else:
            crit_text = f"Bonferroni 临界 |r| ≈ {crit_r_bonf:.3f}；未达到校正门槛，不能视为证据"
        loo_text = ""
        if loo_lo is not None and loo_hi is not None:
            stable = bool(loo_lo and crit_r_bonf and loo_lo >= crit_r_bonf)
            loo_text = (
                f"。留一法 |r| 区间 [{loo_lo:.3f}, {loo_hi:.3f}] —— "
                + ("对单条记录稳健" if stable else "极易被单条记录推翻（存活样本太少）")
            )
        best_interpretation = (
            f"最高 |point-biserial| = {abs(best['point_biserial']):.3f}（维度 {best['id']}），"
            + crit_text
            + loo_text
        )
    else:
        best_interpretation = "无可分析的维度"
    n_survived = int(sum(y))

    # --- scoring degradation (the honest limit of a retrospective registry) ---
    # The rubric allows 1/3/5, but a backfilled hypothesis written up before the
    # rubric existed usually collapses to one or two values per dimension, so the
    # point-biserial is effectively correlating a binary score with a binary
    # outcome.  Report it explicitly; it is the strongest argument for scoring
    # NEW hypotheses prospectively rather than mining the historical ones.
    scored_dims = [r for r in rows if r.get("point_biserial") is not None]
    distinct = [r["n_distinct_scores"] for r in scored_dims if r.get("n_distinct_scores") is not None]
    n_dims_scored = len(distinct)
    n_single_value = sum(1 for d in distinct if d <= 1)
    n_binary_or_less = sum(1 for d in distinct if d <= 2)
    degradation = {
        "n_dimensions_scored": n_dims_scored,
        "n_single_value_dimensions": n_single_value,
        "n_binary_or_less_dimensions": n_binary_or_less,
        "distinct_score_counts": {
            r["id"]: r["n_distinct_scores"] for r in scored_dims
        },
        "warning": (
            f"回溯打分退化：{n_dims_scored} 个可分析维度中有 {n_binary_or_less} 个只有 ≤2 个取值"
            f"（{n_single_value} 个只有单一取值）—— 名义上 1/3/5 的量表退化为近似二元，"
            "point-biserial 因此与『维度粒度』脱钩，无法区分『这一维重要』与『这一维样本里恰好有取值差异』。"
            "更根本的是：这些分数是知道结果之后回溯打的，事后知识污染无法完全剔除（已按 ex-ante 视角尽量约束）。"
            "因此本次 attribution 只作线索；真正的收益从下一个前瞻登记的假设开始。"
        ),
        "implication": "prefer prospective scoring of new registrations; a retrospective registry can only produce leads",
    }
    return {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "n_records": len(records),
        "n_decided": n,
        "n_survived": n_survived,
        "n_failed": n - n_survived,
        "n_dimensions": n_dims,
        "df": df,
        "alpha_family": alpha_family,
        "bonferroni_alpha": bonf_alpha,
        "critical_abs_r_uncorrected": crit_r_uncorrected,
        "critical_abs_r_bonferroni": crit_r_bonf,
        "expected_false_positives_at_0.05": alpha_family * n_dims,
        "fdr_bh": fdr,
        "scoring_degradation": degradation,
        "dimensions": rows,
        "ranking_by_abs_point_biserial": [r["id"] for r in ranked],
        "best_dimension": best["id"] if best else None,
        "best_abs_point_biserial": best["point_biserial"] if best else None,
        "best_interpretation": best_interpretation,
        "warning": EXPLORATORY_WARNING,
        "disclaimer": "This is an exploratory signal for rubric-weight iteration, not a validated conclusion. "
                      "The historical registry is small (n≈27) and the 'survived' label mixes genuinely "
                      "deployable edges with retained incumbents/construction tweaks.",
        "method": "point-biserial (Pearson with a binary outcome) and Spearman rank correlation per dimension; "
                  "two-sided Student-t p-value from the correlation; Bonferroni and Benjamini-Hochberg corrections "
                  "over the ten dimensions.",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--registry", default=DEFAULT_REGISTRY)
    parser.add_argument("--out", default=DEFAULT_OUT)
    args = parser.parse_args(argv)

    records = load_registry(args.registry)
    report = analyse(records)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        f"records={report['n_records']} decided={report['n_decided']} "
        f"survived={report['n_survived']} dims={report['n_dimensions']}"
    )
    for r in sorted(report["dimensions"], key=lambda d: -(abs(d["point_biserial"]) if d["point_biserial"] is not None else -1)):
        if r.get("point_biserial") is None:
            print(f"  {r['id']:28s}  (no variance / insufficient)")
            continue
        print(
            f"  {r['id']:28s} r_pb={r['point_biserial']:+.3f} "
            f"rho={r['spearman']:+.3f} p={r['p_value']:.3f}"
        )
    print(f"\nBEST: {report['best_interpretation']}")
    print(f"\nWARNING: {EXPLORATORY_WARNING}")
    print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
