# 策略验证规范（Validation Standard）

**v1.0 — 2026-09-15** | 起因：GBM 选股线的可信度危机。既有的 purged walk-forward / 超参锁定 / 成本模型防住了**作弊**，却没防住**噪音**：Sharpe 0.963 与月均 IC 0.0082（SE≈0.0095，t≈0.84）在同一份报告里躺了两天；换个测试窗 IC 就翻号；`--as-of 2021-06` 因分数并列报出 2/10 MISMATCH。

> **核心原则：一份策略报告，如果没有回答下面 6 个检查 + 14 条红队问题，就等于没写完，不具有决策资格。**

---

## 1. 每条规则防的是什么失败

| # | 检查 | 判定线 | 防哪种失败（GBM 教训） |
|---|---|---|---|
| 1 | **显著性** `significance(ic_series)` | 需 \|t\| ≥ 2；否则 PASS/FAIL/**NOISE** | GBM 只报「月均 IC 0.0082」不报 SE。真实 SE≈0.0095 → t≈0.84，**与零无法区分**却被写成 OUTPERFORM。 |
| 2 | **多窗口** `multi_window(fn, windows)` | 跨窗**符号一致率 ≥ 80%**，否则 UNSTABLE | 同一策略换测试窗起点（2020-10 → 2021-05）IC 从 +0.0082 翻到 **-0.0076**，脚本结论从「打败动量」变成「NO EDGE」。点估计永远看不出这个。 |
| 3 | **参数邻域** `neighborhood_stability(fn, grid)` | 孤立峰值（isolation ratio > 0.5）判 **OVERFIT** | 「最优」配置若在 depth=4 好、depth=3/5 差，就是拟合噪声；只有平坦山丘才可能是真效应。 |
| 4 | **边界稳定性** `boundary_stability(scores_df, top_n)` | 扰动后 top-N **翻转率 > 20%** 判 ARBITRARY；并强制报告**并列比例** | GBM 模型分数大量并列（5 只完全相同），top-10 边界由排序平局决定 → 2/10 重合；补 21 只股票 + 1 行数据（BAYRY）就翻转整月选股。 |
| 5 | **内部一致性** `internal_consistency(ic, sharpe, turnover, n_months)` | 实际 Sharpe 不得超出 IC 隐含 IR 的 **3 倍**（IR ≈ IC × √breadth） | IC 0.008 + 71 月 → 期望 IR≈0.07，而实际 Sharpe 0.963 高一个数量级（对基准调整后仍有 3.9 倍）。两个数字不可能同时为真。 |
| 6 | **红队清单** `red_team_checklist(report)` | 14 条必答问题（见 §3），高严重度缺一即 FAIL | 报告结构里没有「显著性 / 换窗 / 分数分布 / 基线对比」这四个位置，所以没人会去填，也没人会去看。 |

---

## 2. 任何报告必须包含的字段

```jsonc
{
  "meta": { "script": "...", "generated_at": "...", "data_range": ["...", "..."],
            "price_data": "...", "universe": { "point_in_time": "data/universe/xxx_YYYY.json" } },

  "monthly_ic": { "mean": 0.0082, "std": 0.080, "se": 0.0095, "t_stat": 0.84, "n_months": 71 },

  "validation": {
    "significance":        { "verdict": "NOISE", "ci": [-0.011, 0.027] },
    "multi_window":        { "sign_consistency": 0.57, "windows": { "...": 0.0 } },
    "boundary_stability":  { "tie_ratio": 0.5, "boundary_ties": 3, "flip_rate": 0.0 },
    "neighborhood":        { "isolation_ratio": 0.15, "verdict": "SMOOTH" },
    "internal_consistency":{ "expected_ir": 0.069, "ratio": 3.9, "verdict": "IMPLAUSIBLE" }
  },

  "baseline_significance": { "baseline": "momentum_12_1", "t_stat": 1.84 },
  "cost_sensitivity":      { "zero_cost_sharpe": 1.01, "cost_drag_bps_yr": 136 },
  "multiple_comparison":   { "n_variants": 15 },
  "walk_forward":          { "first_test_date": "2020-10-30", "n_test_months": 71 },
  "conventions":           { "...": "..." },
  "verdict": "..."
}
```

缺少任一**高严重度**字段 → `red_team_checklist` 判 FAIL，报告不得交付。

---

## 3. 红队清单（14 条，逐条回答）

1. 脚本路径 + UTC 生成时间？（provenance）
2. 数据窗口起止？（data_window）
3. 价格源与复权/分红/时区口径？（price_source）
4. 股票池是否 point-in-time？有无 survivorship 说明？（universe_snapshot）
5. 头号指标的 **t 值 / 标准误 / 置信区间**（不只是均值）？（significance）
6. 换测试窗后的**符号一致率**？（window_stability）
7. 分数分布：唯一值数、并列比例、top-N 边界并列数？（score_distribution）
8. 与**最简单基线**差异的显著性？（baseline_significance）
9. 一共试了多少配置？有无多重比较修正？（multiple_comparison）
10. 成本敏感性（零成本 vs 高成本、成本拖累 bps/年）？（cost_sensitivity）
11. 实际成本总额 / bps per rebalance？（cost_reporting）
12. 真正的样本外口径（walk-forward / holdout / 时间切分）？（out_of_sample）
13. 交易约定（信号时点、执行时点、做空、成本、平仓）？（conventions）
14. 明确带方向的结论，并引用上述检验？（verdict）

---

## 4. 严重度定义（审计输出用）

| 级别 | 含义 |
|---|---|
| **GREEN** | 所有可计算的检查通过，且高严重度字段齐备 → 可交付。 |
| **AMBER** | 结论「未被证明」：显著性 NOISE/不可计算、变体族符号不一致、或必答字段缺失（流程缺陷）。 |
| **RED** | 数字**自相矛盾**：IC 与 Sharpe 不可能同时成立、显著的**负**结果、**同一策略**换窗符号翻转、top-N 边界由并列决定、孤立参数峰。 |

> 缺字段 ≠ 数字错。缺字段是 AMBER（交付卫生），数字互斥才是 RED（结论不可用）。

## 部署门槛（team-lead 补充，关键操作规则）

RED/AMBER 是**报告分诊**（给报告分类），不是**上线许可**。两者用途不同：

| 用途 | 规则 |
|---|---|
| 报告分诊 | RED = 自称结论被证伪；AMBER = 结论未被证伪也未被证实；GREEN = 可交付 |
| **上真钱门槛** | 一个**策略**（不是报告）必须 `significance = PASS` **且** `window_stability = PASS` **且** `boundary_stability = 无并列决定边界`——**三项全 PASS 才可部署**；AMBER 级别的问题足以阻止部署 |

理由（两个实例教训）：
- **GBM**：`significance` 不可计算（缺字段）→ 属 AMBER，但它照样上线了 paper trading，因为没人把「AMBER」当成阻断条件
- **weekend_gap**：p=0.053 属 NOISE → 按分诊是 AMBER，但它被写成 playbook v1.0 让用户上真钱——如果当时有「NOISE 阻断部署」这条硬规则，红队审计的发现本可以在第一天就生效

**因此：AMBER 阻止交付策略，但不阻止交付报告。** 报告可以带 AMBER 存在（用于记录与迭代），策略不行。

---

## 5. 怎么跑

```bash
# 单元测试（合成数据，已知答案）
poetry run pytest tests/validation/ -q

# 对 reports/ 下既有报告做回顾性体检 → reports/validation_audit.json
poetry run python scripts/validate_reports.py
```

新策略脚本的推荐写法：把 §2 的字段直接写进报告 JSON，并在脚本末尾调用一次
`red_team_checklist(report)`，让它自己把缺项打印出来再退出。

---

## 6. 首次全量体检结果（2026-09-15）

`reports/validation_audit.json`：**4 RED / 9 AMBER / 0 GREEN**。

- RED：`xsec_gbm_results`、`xsec_gbm_sp500`、`gbm_attribution`、`onchain_btc_backtest`
- 全仓**没有任何一份报告**回答了 significance / window_stability / score_distribution / baseline_significance（13/13 缺失）

**本该在第一次就暴露的四个问题**：① IC 的 SE 从未与均值并列（t≈0.84）；② IC 跨年符号翻转的证据早已在 `gbm_attribution` 里（7 年翻 3 次）；③ 分数并列在 shipped `gbm_picks` 里肉眼可见（2026-10 top-10 只有 5 个唯一分数）；④ 与动量的显著性对比从未做过（月度超额 t≈1.84）。
