"""Signal due-diligence rubric — the *mechanism-side* gate the framework was missing.

Why this exists (2026-09 platform post-mortem)
----------------------------------------------
``src/validation/`` already asks "did you measure this correctly?" — significance,
window stability, boundary stability, reproducibility, the deployment gate.  It
never asks "is this thing *worth believing* on mechanistic grounds?".  The cost
of that blind spot was visible: 27 hypothesis families were backtested with
rigorous, well-instrumented pipelines, and the best of them reached |t| = 1.84 —
below even the single-test line, let alone the search-corrected one.

The weekend_gap case is the canonical failure.  Its red-team checklist *passed*
(PIT-clean data, reproducibility probe run, cost model present) yet the whole
premise — "why isn't this gap traded away?" — was never asked.  The answer, found
only after real money was contemplated, was that the move is priced in by the
open and we were taking the leftovers.

This module scores a hypothesis on ten *mechanism* dimensions **before** an
evaluation budget is spent.  Inspiration: rubric-search (Echo/UniPat) — reward
process quality rather than outcome correctness, because fat tails reward lucky
guesses; and let the ranking data (``rubric_attribution.py``) eventually tell us
which dimensions actually predict survival.

Scoring
-------
Each dimension is scored 1 / 3 / 5 (no 2s or 4s — the middle must be a real
commitment, not a hedge).  The weighted mean over the answered dimensions gives
the band::

    >= 3.5            ALLOW_EVALUATION     spend the evaluation budget
    2.5 .. < 3.5      NEEDS_STRENGTHENING  fix the named weak dimensions first
    < 2.5             REJECT               do not evaluate (unless user override)

``counterparty_arbitrage`` and ``preregistration`` carry double weight by
default: they are the two questions whose absence killed the most budget.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

# ---------------------------------------------------------------------------
# Bands
# ---------------------------------------------------------------------------

BAND_ALLOW = "ALLOW_EVALUATION"
BAND_STRENGTHEN = "NEEDS_STRENGTHENING"
BAND_REJECT = "REJECT"

#: ``{band: inclusive lower bound}`` — the highest band whose bound is met wins.
BAND_THRESHOLDS: dict[str, float] = {
    BAND_ALLOW: 3.5,
    BAND_STRENGTHEN: 2.5,
}

BAND_LABELS_ZH: dict[str, str] = {
    BAND_ALLOW: "允许进入评估预算",
    BAND_STRENGTHEN: "需补强特定维度",
    BAND_REJECT: "拒绝（除非用户明确要求强行评估，需在登记簿记录 user_override）",
}

#: Scores a dimension is allowed to take.
VALID_SCORES: tuple[int, ...] = (1, 3, 5)

# ---------------------------------------------------------------------------
# Dimensions (all text lives here, never inline in logic)
# ---------------------------------------------------------------------------

DIMENSIONS: list[dict[str, Any]] = [
    {
        "id": "counterparty_arbitrage",
        "name": "对手方与套利壁垒",
        "name_en": "Counterparty & arbitrage barrier",
        "question": "谁在另一边？为什么这个价差/错价不被套利掉？为什么现在还存在？",
        "weight": 2.0,
        "criteria": {
            5: "能明确指出对手方（如被动对冲者、被迫再平衡的结构性资金、受监管/受托限制无法套利的机构），并说清错价为何持续、为何不会被迅速套利掉；有结构性/行为性溢价的证据。",
            3: "能说出大致的对手方，但对『为何不被套利』只有一般性论述（风险大、流动性差），没有具体壁垒或证据。",
            1: "从未问过对手方是谁；默认 edge 源于『市场没发现』或『统计显著』，没有解释它为何持续存在。",
        },
    },
    {
        "id": "mechanism_stateability",
        "name": "机制可陈述性",
        "name_en": "Mechanism stateability",
        "question": "能否一句话说清因果链（不靠『统计上显著』来替代机制）？",
        "weight": 1.0,
        "criteria": {
            5: "一句话即可陈述完整因果链（A→B→价格），且不依赖『统计显著』作为替代；能指出链条中哪个环节会失效。",
            3: "有因果故事但存在跳跃或未验证的中介环节，需要数据确认才能成立。",
            1: "只有『因子 X 与收益 Y 相关 / 回测好』；没有机制，或机制是看结果之后补编的故事。",
        },
    },
    {
        "id": "base_rate_anchoring",
        "name": "基率锚定",
        "name_en": "Base-rate anchoring",
        "question": "无条件基率是多少？宣称的 edge 是与正确的零假设比吗？",
        "weight": 1.0,
        "criteria": {
            5: "明确给出无条件基率（事件频率 × 无条件收益），并说明 edge 相对正确零假设的增量；已扣除基率。",
            3: "提到基率但未量化，或比较的零假设并非正确基准（与 0 比而非与买入持有 / 动量基线比）。",
            1: "只看条件收益率，从未问『不触发时会发生什么』；把市场 beta / 无条件漂移当成 alpha。",
        },
    },
    {
        "id": "capacity_cost_reality",
        "name": "容量与成本现实",
        "name_en": "Capacity & cost reality",
        "question": "在真实成本/税/滑点/你的资金规模下是否还活着？",
        "weight": 1.0,
        "criteria": {
            5: "用真实成本/税/滑点/借券费/涨跌幅限制以及自己实际资金规模做过净额检验，且容量远大于资金量。",
            3: "做了成本敏感性但参数乐观（只用零成本或单一成本），或容量未被评估。",
            1: "收益是毛收益，没有成本/税/滑点假设，或假设明显不可实现（如按开盘无滑点成交）。",
        },
    },
    {
        "id": "regime_dependency_declared",
        "name": "regime 依赖声明",
        "name_en": "Regime dependency declared",
        "question": "应该在什么市场环境下有效，这个声明是事前写的吗？",
        "weight": 1.0,
        "criteria": {
            5: "事前（注册时）写明有效/失效的市场环境与机械判据，并在多个 regime 子样本中检验过。",
            3: "事前未写，事后描述了 regime 依赖；或声明模糊、无法机械判定。",
            1: "从未声明 regime 依赖，默认策略在所有环境下有效——通常等于对某一段行情过拟合。",
        },
    },
    {
        "id": "decay_logic_monitoring",
        "name": "衰减逻辑与监控",
        "name_en": "Decay logic & monitoring",
        "question": "什么会侵蚀这个 edge？有没有对应的监控指标？",
        "weight": 1.0,
        "criteria": {
            5: "明确指出什么会侵蚀 edge（拥挤/制度变化/数据修正/竞争者），并有可部署的监控指标与降级红线。",
            3: "能说出衰减来源但没有监控指标，或只有定性描述。",
            1: "假设 edge 永久有效；没有任何衰减假设或监控安排。",
        },
    },
    {
        "id": "data_moat",
        "name": "数据护城河",
        "name_en": "Data moat",
        "question": "数据是独占/新鲜的，还是人人都有的（拥挤风险）？",
        "weight": 1.0,
        "criteria": {
            5: "数据独占、需许可证/自采/低延迟，他人难以复制；有清晰的数据护城河。",
            3: "数据半独占（付费可购）或有时效性优势，但并非不可复制。",
            1: "数据人人都有（公开价格/财报/免费 API），信号一旦公开就会被拥挤掉。",
        },
    },
    {
        "id": "no_chaos_prediction",
        "name": "是否需要预测不可预测之物",
        "name_en": "Avoids predicting chaos",
        "question": "是否依赖预测混沌资产的方向？",
        "weight": 1.0,
        "criteria": {
            5: "不依赖预测方向；赚的是结构性/机械性/风险溢价的钱（收租、提供流动性、承接再平衡流）。",
            3: "需要方向判断但有限制（只赌均值回归、带保护、条件触发），并非裸赌方向。",
            1: "本质是预测混沌资产（单标的短期方向）的涨跌；edge 等价于『猜得准』。",
        },
    },
    {
        "id": "executability",
        "name": "可执行性",
        "name_en": "Executability",
        "question": "标的真实存在、下单可行、无借券/涨跌幅限制障碍？",
        "weight": 1.0,
        "criteria": {
            5: "标的真实存在、可交易，无借券/涨跌幅/流动性障碍；下单时点与数据可得时点一致。",
            3: "可交易但有摩擦（需要特定券商、部分标的受限、容量/时段受限）。",
            1: "标的不存在/不可交易，或依赖无法成交的价格，或信号收盘后才可得却假设盘中行动（前视）。",
        },
    },
    {
        "id": "preregistration",
        "name": "事前注册状态",
        "name_en": "Pre-registration status",
        "question": "假设在看数据之前就写下来了吗？",
        "weight": 2.0,
        "criteria": {
            5: "假设、触发条件、成功判据在查看相关数据之前已写入登记簿（带 UTC 时间戳与 commit），事后未修改。",
            3: "事前写了部分条件，但关键判据或阈值是看过数据之后补上的。",
            1: "完全事后提出；由已经看到的回测结果倒推假设、窗口、阈值（典型的多重比较 / HARKing）。",
        },
    },
]

#: Default weights — the single knob ``rubric search`` will iterate on.  Kept as
#: a module constant (not buried in logic) so it can be overridden per call.
DIMENSION_WEIGHTS: dict[str, float] = {d["id"]: float(d["weight"]) for d in DIMENSIONS}

_DIMENSION_BY_ID: dict[str, dict[str, Any]] = {d["id"]: d for d in DIMENSIONS}

# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------


@dataclass
class RubricResult:
    """Outcome of scoring one hypothesis against the rubric."""

    weighted_score: float
    band: str
    answers: dict[str, int]
    weights: dict[str, float]
    missing_dimensions: list[str] = field(default_factory=list)
    contributions: dict[str, float] = field(default_factory=dict)
    detail: list[dict[str, Any]] = field(default_factory=list)

    @property
    def label(self) -> str:
        return BAND_LABELS_ZH[self.band]

    @property
    def n_scored(self) -> int:
        return len(self.answers)

    @property
    def complete(self) -> bool:
        return not self.missing_dimensions

    def as_dict(self) -> dict[str, Any]:
        return {
            "weighted_score": self.weighted_score,
            "band": self.band,
            "band_label": self.label,
            "n_scored": self.n_scored,
            "n_dimensions": len(DIMENSIONS),
            "complete": self.complete,
            "missing_dimensions": list(self.missing_dimensions),
            "answers": dict(self.answers),
            "weights": dict(self.weights),
            "contributions": dict(self.contributions),
            "detail": list(self.detail),
        }


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------


def _coerce_score(dim_id: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(
            f"dimension '{dim_id}' must be scored with an int in {VALID_SCORES}, got {value!r}"
        )
    if value not in VALID_SCORES:
        raise ValueError(
            f"dimension '{dim_id}' score must be one of {VALID_SCORES}, got {value}"
        )
    return value


def _validate_weights(weights: Mapping[str, float]) -> dict[str, float]:
    known = set(_DIMENSION_BY_ID)
    unknown = set(weights) - known
    if unknown:
        raise ValueError(f"unknown dimension(s) in weights: {sorted(unknown)}")
    out: dict[str, float] = {}
    for did, w in weights.items():
        w = float(w)
        if w <= 0:
            raise ValueError(f"weight for '{did}' must be positive, got {w}")
        out[did] = w
    return out


def score(
    answers: Mapping[str, int],
    *,
    weights: Mapping[str, float] | None = None,
) -> RubricResult:
    """Score a hypothesis against the ten-dimension rubric.

    Args:
        answers: ``{dimension_id: 1|3|5}``.  Unknown ids raise; values must be
            exactly 1, 3 or 5.  Missing dimensions are tolerated here (so a
            partial review is still comparable) but reported in
            ``missing_dimensions`` and block registration (see registry.py).
        weights: optional weight overrides.  Defaults to ``DIMENSION_WEIGHTS``;
            exactly-equal weights give the plain mean.

    Returns:
        :class:`RubricResult` with the weighted mean over the *answered*
        dimensions, its band, per-dimension contributions and the missing list.
    """
    w = _validate_weights(weights) if weights is not None else dict(DIMENSION_WEIGHTS)
    # any dimension absent from a custom weight map defaults to weight 1.0
    for did in _DIMENSION_BY_ID:
        w.setdefault(did, 1.0)

    if not answers:
        raise ValueError("no answers supplied — cannot score an empty rubric")

    parsed: dict[str, int] = {}
    for dim_id, value in answers.items():
        if dim_id not in _DIMENSION_BY_ID:
            raise ValueError(
                f"unknown rubric dimension '{dim_id}' (valid: {sorted(_DIMENSION_BY_ID)})"
            )
        parsed[dim_id] = _coerce_score(dim_id, value)

    missing = [d["id"] for d in DIMENSIONS if d["id"] not in parsed]
    total_w = sum(w[did] for did in parsed)
    weighted = sum(parsed[did] * w[did] for did in parsed) / total_w

    contributions = {did: parsed[did] * w[did] / total_w for did in parsed if did in parsed}
    detail = [
        {
            "id": d["id"],
            "name": d["name"],
            "question": d["question"],
            "score": parsed[d["id"]],
            "weight": w[d["id"]],
            "weighted_contribution": contributions[d["id"]],
            "scored": True,
        }
        for d in DIMENSIONS
        if d["id"] in parsed
    ] + [
        {
            "id": d["id"],
            "name": d["name"],
            "question": d["question"],
            "score": None,
            "weight": w[d["id"]],
            "weighted_contribution": None,
            "scored": False,
        }
        for d in DIMENSIONS
        if d["id"] not in parsed
    ]

    return RubricResult(
        weighted_score=round(weighted, 6),
        band=band_for(weighted),
        answers=parsed,
        weights={did: w[did] for did in _DIMENSION_BY_ID},
        missing_dimensions=missing,
        contributions=contributions,
        detail=detail,
    )


def band_for(weighted_score: float) -> str:
    """Map a weighted score to its band (boundaries are inclusive lower bounds)."""
    if weighted_score >= BAND_THRESHOLDS[BAND_ALLOW]:
        return BAND_ALLOW
    if weighted_score >= BAND_THRESHOLDS[BAND_STRENGTHEN]:
        return BAND_STRENGTHEN
    return BAND_REJECT


def weakest_dimensions(result: RubricResult, k: int = 3) -> list[str]:
    """Ids of the ``k`` lowest-scoring answered dimensions (strengthening targets)."""
    scored = [(d["id"], d["score"]) for d in result.detail if d["scored"]]
    scored.sort(key=lambda t: (t[1], t[0]))
    return [did for did, _ in scored[:k]]


# ---------------------------------------------------------------------------
# Rendering / docs
# ---------------------------------------------------------------------------


def render_rubric_markdown() -> str:
    """Auto-generate the rubric documentation from the constants above.

    This is the single source of truth for ``docs/signal_rubric.md`` — edit the
    constants, regenerate, never hand-edit the table.
    """
    lines: list[str] = []
    lines.append("# 信号尽调 Rubric（Signal Due-Diligence Rubric）")
    lines.append("")
    lines.append(
        "> **本文件由 `src/validation/rubric.py` 的常量自动生成（`python -m src.validation.rubric`）。"
        "请勿手工编辑；改判据请改常量后重新生成。**"
    )
    lines.append("")
    lines.append(
        "结果侧验证（`checklist.py` / `gate.py` / `significance.py` …）问的是"
        "「你按规矩测量了吗」；本 rubric 问的是「这件事在机制上值得信吗」。"
        "两者互补：先过 rubric，再花评估预算。"
    )
    lines.append("")
    lines.append("## 评分与分档")
    lines.append("")
    lines.append("每维只允许 **1 / 3 / 5** 分（不允许 2/4 分——中间必须是一个明确承诺，不是和稀泥）。")
    lines.append("加权总分 = 各维分数×权重 之和 / 权重之和（对已作答维度）。")
    lines.append("")
    lines.append("| 分档 | 条件 | 含义 |")
    lines.append("|---|---|---|")
    lines.append(
        f"| `{BAND_ALLOW}` | 加权总分 ≥ {BAND_THRESHOLDS[BAND_ALLOW]} | {BAND_LABELS_ZH[BAND_ALLOW]} |"
    )
    lines.append(
        f"| `{BAND_STRENGTHEN}` | {BAND_THRESHOLDS[BAND_STRENGTHEN]} ≤ 加权总分 < {BAND_THRESHOLDS[BAND_ALLOW]} | {BAND_LABELS_ZH[BAND_STRENGTHEN]} |"
    )
    lines.append(
        f"| `{BAND_REJECT}` | 加权总分 < {BAND_THRESHOLDS[BAND_STRENGTHEN]} | {BAND_LABELS_ZH[BAND_REJECT]} |"
    )
    lines.append("")
    lines.append("## 权重")
    lines.append("")
    lines.append("权重写在 `rubric.py::DIMENSION_WEIGHTS` 常量里，供未来 rubric search 迭代。默认权重：")
    lines.append("")
    lines.append("| # | 维度 | 权重 |")
    lines.append("|---|---|---|")
    for i, d in enumerate(DIMENSIONS, start=1):
        lines.append(f"| {i} | {d['name']}（`{d['id']}`） | {DIMENSION_WEIGHTS[d['id']]:g} |")
    lines.append("")
    lines.append(
        "**对手方与套利壁垒**、**事前注册状态** 默认加倍权重 —— 这两个问题的缺席烧掉了我们最多的评估预算。"
    )
    lines.append("")
    lines.append("## 十维判据")
    lines.append("")
    for i, d in enumerate(DIMENSIONS, start=1):
        lines.append(f"### {i}. {d['name']} — `{d['id']}`（权重 {DIMENSION_WEIGHTS[d['id']]:g}）")
        lines.append("")
        lines.append(f"**问题**：{d['question']}")
        lines.append("")
        lines.append("| 分值 | 判据 |")
        lines.append("|---|---|")
        for s in (5, 3, 1):
            lines.append(f"| **{s}** | {d['criteria'][s]} |")
        lines.append("")
    lines.append("## 用法")
    lines.append("")
    lines.append("```python")
    lines.append("from src.validation.rubric import score")
    lines.append("")
    lines.append("res = score({")
    lines.append('    "counterparty_arbitrage": 1,')
    lines.append('    "mechanism_stateability": 3,')
    lines.append("    ...  # 10 维全部作答")
    lines.append("})")
    lines.append('assert res.band in {"ALLOW_EVALUATION", "NEEDS_STRENGTHENING", "REJECT"}')
    lines.append("```")
    lines.append("")
    lines.append(
        "注册入口（强制十维齐全）见 `scripts/register_hypothesis.py`；"
        "历史条目与存活相关性分析见 `scripts/rubric_attribution.py`。"
    )
    lines.append("")
    return "\n".join(lines)


def _main() -> None:  # pragma: no cover - CLI convenience
    print(render_rubric_markdown())


if __name__ == "__main__":  # pragma: no cover
    _main()
