# 策略验证规范（Validation Standard）

**v1.1 — 2026-09-15** | 起因：GBM 选股线的可信度危机。既有的 purged walk-forward / 超参锁定 / 成本模型防住了**作弊**，却没防住**噪音**：Sharpe 0.963 与月均 IC 0.0082（SE≈0.0095，t≈0.84）在同一份报告里躺了两天；换个测试窗 IC 就翻号；`--as-of 2021-06` 因分数并列报出 2/10 MISMATCH。

> **核心原则：一份策略报告，如果没有回答下面 9 个检查 + 15 条红队问题，并且通过独立红队评审，就等于没写完，不具有决策资格。**

---

## 1. 每条规则防的是什么失败

### 1.1 截面 / 月度策略

| # | 检查 | 判定线 | 防哪种失败（GBM 教训） |
|---|---|---|---|
| 1 | **显著性** `significance(ic_series)` | 需 \|t\| ≥ 2；否则 PASS/FAIL/**NOISE**；附带 Student-t p 值 | GBM 只报「月均 IC 0.0082」不报 SE。真实 SE≈0.0095 → t≈0.84，**与零无法区分**却被写成 OUTPERFORM。 |
| 2 | **多窗口** `multi_window(fn, windows)` | 跨窗**符号一致率 ≥ 80%**，否则 UNSTABLE | 同一策略换测试窗起点（2020-10 → 2021-05）IC 从 +0.0082 翻到 **-0.0076**，脚本结论从「打败动量」变成「NO EDGE」。点估计永远看不出这个。 |
| 3 | **参数邻域** `neighborhood_stability(fn, grid)` | 孤立峰值（isolation ratio > 0.5）判 **OVERFIT** | 「最优」配置若在 depth=4 好、depth=3/5 差，就是拟合噪声；只有平坦山丘才可能是真效应。 |
| 4 | **边界稳定性** `boundary_stability(scores_df, top_n)` | 扰动后 top-N **翻转率 > 20%** 判 ARBITRARY；并强制报告**并列比例** | GBM 模型分数大量并列（2026-10 top-10 只有 5 个唯一分数），top-10 边界由排序平局决定 → 2/10 重合；补 21 只股票 + 1 行数据（BAYRY）就翻转整月选股。 |
| 5 | **内部一致性** `internal_consistency(ic, sharpe, turnover, n_months)` | 实际 Sharpe 不得超出 IC 隐含 IR 的 **3 倍**（IR ≈ IC × √breadth） | IC 0.008 + 71 月 → 期望 IR≈0.07，而实际 Sharpe 0.963 高一个数量级（对基准调整后仍有 3.9 倍）。两个数字不可能同时为真。 |

### 1.2 事件 / 低频策略（≥1.1 新增）

事件策略没有月度 IC 序列，只有**十几到几十笔交易**，统计问题比 GBM 更尖锐：一次极端交易就能主导均值，正态近似过于乐观。

| # | 检查 | 判定线 | 说明 |
|---|---|---|---|
| 6 | **事件级显著性** `event_significance(trade_returns)` | 与 `significance` 同线（\|t\| ≥ 2）；同时输出 **bootstrap 均值 CI** 与**胜率 Wilson 区间**；n<30 标记 `small_sample` | weekend_gap 只有 25 笔：T+1 口径 **t=0.48**、胜率 64% 的 Wilson 区间 **[0.445, 0.798]** 跨过 50% → NOISE。报告中 Sharpe 0.288 的数字掩盖了「25 笔讲不出故事」。 |
| 6b | **小样本判据优先级**（team-lead 规则） | **n < 30 时以 bootstrap 均值 CI 为主判据**；正态近似与 Bonferroni 阈值只作**上界参考** | 极小样本下 t 分布尾巴更厚，正态近似会低估不确定性（GBM/周末缺口两案都踩过）。两者并列输出，结论以 bootstrap CI 为准。 |
| 7 | **事件分窗** `event_window_stats(trades, {"2021-23": .., "2024-26": ..})` | 各子段**事件数 + 收益分布**，跨段符号一致率 ≥ 80% | 低频策略必须证明「不是某一段行情的产物」；分段把 25 笔拆成 13+12，任何一段都不能单独支撑结论。 |

### 1.3 所有策略（≥1.1 新增）

| # | 检查 | 判定线 | 说明 |
|---|---|---|---|
| 8 | **多重比较** `multiple_comparisons(N, best)` | 校正后 p ≤ α 才 SURVIVES；否则 FAILS | 平台已搜过 **N=27 个方向族**（funding×2、onchain×2、FOMC×2、gap×3、订单簿、PM、量比、meta-label、GBM×2、动量、入场/仓位×3、出场×2、风控开关×3、DCA、基本面、weekend_gap），**变体级上限 ~60**。N=27 时空假设下**最优 \|t\| 期望就有 ~2.57**，Bonferroni 要求线 **3.11**（N=60 时 3.34）。当前全平台**存过的最优 t=1.84** → 两个口径都 FAILS，没有任何一个结论能通过校正。 |
| 9 | **复现探针** `reproducibility_probe(stored, rerun, top_n=..)` | top-N 重合率 ≥ 90% 且无分数错配 → REPRODUCIBLE | 正是这个探针抓到了 GBM 危机（2/10 重合）。见 §4 的硬性要求。 |

---

## 2. 任何报告必须包含的字段

```jsonc
{
  "meta": { "script": "...", "generated_at": "...", "data_range": ["...", "..."],
            "price_data": "...", "universe": { "point_in_time": "data/universe/xxx_YYYY.json" } },

  "monthly_ic": { "mean": 0.0082, "std": 0.080, "se": 0.0095, "t_stat": 0.84, "p_value": 0.391, "n_months": 71 },

  "validation": {
    "significance":        { "verdict": "NOISE", "ci": [-0.011, 0.027] },
    "multi_window":        { "sign_consistency": 0.57, "windows": { "...": 0.0 } },
    "boundary_stability":  { "tie_ratio": 0.5, "boundary_ties": 3, "flip_rate": 0.0 },
    "neighborhood":        { "isolation_ratio": 0.15, "verdict": "SMOOTH" },
    "internal_consistency":{ "expected_ir": 0.069, "ratio": 3.9, "verdict": "IMPLAUSIBLE" }
  },

  "events": { "n_events": 25, "t_stat": 0.48, "p_value": 0.63,
              "win_rate": 0.64, "win_rate_wilson": [0.445, 0.798],
              "bootstrap_mean_ci": [-0.9, 2.8], "small_sample": true },

  "multiple_comparisons": { "n_hypotheses": 20, "observed_t": 1.84, "required_t": 3.02, "verdict": "FAILS" },
  "reproducibility":      { "verdict": "REPRODUCIBLE", "top_n": 10, "overlap_ratio": 1.0, "n_value_mismatches": 0 },

  "baseline_significance": { "baseline": "momentum_12_1", "t_stat": 1.84 },
  "cost_sensitivity":      { "zero_cost_sharpe": 1.01, "cost_drag_bps_yr": 136 },
  "walk_forward":          { "first_test_date": "2020-10-30", "n_test_months": 71 },
  "conventions":           { "...": "..." },
  "verdict": "..."
}
```

缺少任一**高严重度**字段 → `red_team_checklist` 判 FAIL，报告不得交付。

---

## 3. 红队清单（15 条，逐条回答）

1. 脚本路径 + UTC 生成时间？（provenance）
2. 数据窗口起止？（data_window）
3. 价格源与复权/分红/时区口径？（price_source）
4. 股票池是否 point-in-time？有无 survivorship 说明？（universe_snapshot）
5. 头号指标的 **t 值 / 标准误 / 置信区间**（不只是均值）？（significance）
6. 换测试窗/子样本后的**符号一致率**？（window_stability）
7. 分数分布：唯一值数、并列比例、top-N 边界并列数？（score_distribution）
8. 与**最简单基线**差异的显著性？（baseline_significance）
9. 一共试了多少配置/方向？多重比较校正后还显著吗？（multiple_comparison）
10. 成本敏感性（零成本 vs 高成本、成本拖累 bps/年）？（cost_sensitivity）
11. 实际成本总额 / bps per rebalance？（cost_reporting）
12. 真正的样本外口径（walk-forward / holdout / 时间切分）？（out_of_sample）
13. **复现探针：换数据快照 / 换执行路径，top-N 重合度是多少？**？（reproducibility）
14. 交易约定（信号时点、执行时点、做空、成本、平仓）？（conventions）
15. 明确带方向的结论，并引用上述检验？（verdict）

---

## 4. 要求 B：复现探针是硬性步骤

**规则：任何策略结论必须能证明「在另一个数据快照 / 另一个执行路径下仍能复现」。**

- 月频截面策略：至少重跑 1 个月（`--as-of YYYY-MM`，与 `gbm_picks/` 存档对比 top-N 重合率）；
- 事件策略：至少重跑 1 个事件（重算入场/出场价与收益），或换一次数据拉取重跑全窗；
- 结论：`reproducibility_probe()` 判 **REPRODUCIBLE**（重合率 ≥ 90% 且无分数错配）才允许交付。**NON_REPRODUCIBLE 直接判 RED。**

实现模板（放进任何报告脚本，一行出字段）：

```python
from src.validation import reproducibility_probe

# 存档产物 vs 本次重跑产物（允许 list[str] / {symbol: score} / [{symbol, score}]）
probe = reproducibility_probe(stored_picks, rerun_picks, top_n=10, pass_line=0.90)
report["reproducibility"] = probe
if probe["verdict"] != "REPRODUCIBLE":
    raise SystemExit(f"reproducibility probe failed: {probe['interpretation']}")
```

反例（GBM 危机实测）：`run_monthly_gbm.py --as-of 2021-06` 重跑 top-10 与存档只重合 **2/10** →
`overlap_ratio=0.2` → `NON_REPRODUCIBLE`。同时 `reproducibility_probe` 也能抓「分数并列导致两次重跑
排序不同」这类静默错误（`n_value_mismatches > 0`）。

正例：`reports/risk_gate.json` 与 `reports/gbm_attribution.json` 自带
`verification_vs_source_report`（71/71 月持仓吻合）→ 探针判 REPRODUCIBLE。**这两份报告的做法应成为默认动作。**

---

## 5. 要求 C：红队评审的操作定义

**红队评审 = 由「未参与该策略开发」的独立评审者（另一个 agent 或人）执行的一次对抗性复检。**

- ❌ 不允许由原开发者自己写评审结论；原开发者只能提供材料。
- ✅ 评审结论**必须独立成文**（独立文件 / 独立 commit，如 `reports/red_team_<strategy>.md` 或
  `docs/*_audit.md`），并明确写出评审者身份与所依据的产物版本（commit hash / 报告文件名）。
- ✅ 评审者必须逐条给出**判定 + 证据（文件路径 / 数字）**，不能只写「已检查」。
- ✅ 允许结论是「无法判定（数据不足）」，但必须写明缺哪个数据。

**评审必须逐条回答的 7 个问题：**

| # | 问题 | 用哪个检查支撑 |
|---|---|---|
| ① | 换个数据快照还成立吗？ | `reproducibility_probe` |
| ② | 换窗 / 换子样本符号一致吗？ | `multi_window` / `event_window_stats` |
| ③ | 与最简单基线相比有增量吗？且差异显著吗？ | `significance` + 基线 t 值 |
| ④ | 收益是否只是某种已知 beta（如杠杆暴露、行业暴露、BTC beta）？ | 回归/归因（beta、R²、残差 alpha） |
| ⑤ | 成本 / 税 / 滑点敏感性如何？ | `cost_sensitivity`（零成本 vs 高成本） |
| ⑥ | 试验次数校正后还显著吗？ | `multiple_comparisons` |
| ⑦ | 如果要归零，最可能的机制是什么？ | 评审者定性判断 + 指向最脆弱的假设 |

若材料不足以回答某条 → 该条判 **UNRESOLVED**，策略不得升级到实盘。

---

## 6. 严重度定义（审计输出用）

| 级别 | 含义 |
|---|---|
| **GREEN** | 所有可计算的检查通过，且高严重度字段齐备 → 可交付。 |
| **AMBER** | 结论「未被证明」：显著性 NOISE/不可计算、**多重比较校正后不显著**、变体族符号不一致、事件样本太小、或必答字段缺失（流程缺陷）。 |
| **RED** | 数字**自相矛盾或不可复现**：IC 与 Sharpe 不可能同时成立、显著的**负**结果、**同一策略**换窗符号翻转、top-N 边界由并列决定、孤立参数峰、**复现探针 NON_REPRODUCIBLE**。 |

> 缺字段 ≠ 数字错。缺字段是 AMBER（交付卫生），数字互斥/不可复现才是 RED（结论不可用）。

### 6.1 部署门槛（关键操作规则：AMBER 阻止策略，但不阻止报告）

RED/AMBER 是**报告分诊**（给报告分类），不是**上线许可**。两者用途不同：

| 用途 | 规则 |
|---|---|
| 报告分诊 | RED = 自称结论被证伪；AMBER = 结论未被证伪也未被证实；GREEN = 可交付 |
| **上真钱门槛** | 一个**策略**（不是报告）必须 `significance = PASS` **且** `window_stability = PASS` **且** `boundary_stability = 无并列决定边界`——**三项全 PASS 才可部署**；AMBER 级别的问题足以阻止部署 |

理由（两个实例教训）：
- **GBM**：`significance` 不可计算（缺字段）→ 属 AMBER，但它照样上线了 paper trading，因为没人把「AMBER」当成阻断条件
- **weekend_gap**：p=0.053 属 NOISE → 按分诊是 AMBER，但它被写成 playbook v1.0 让用户上真钱——如果当时有「NOISE 阻断部署」这条硬规则，红队审计的发现本可以在第一天就生效

**因此：AMBER 阻止交付策略，但不阻止交付报告。** 报告可以带 AMBER 存在（用于记录与迭代），策略不行。

为避免这条规则又变成「只写在文档里的散文」，它已实现为可执行函数 `deployment_gate()`：

```python
from src.validation import deployment_gate

gate = deployment_gate({
    "significance":        {"verdict": "NOISE"},              # 不是 PASS 就阻断
    "multi_window":        {"verdict": "STABLE", "scope": "windows"},
    "boundary":            {"verdict": "STABLE", "has_exact_ties": False},
})
assert gate["verdict"] == "DEPLOYMENT_BLOCKED"
```

判定：三个门槛全 PASS → `LIVE_ALLOWED`；任一未过或无法判定 → `DEPLOYMENT_BLOCKED`（`reasons` 里给出是哪一条）。
`multi_window` 若 `scope="variants"`（消融/变体族而非同一策略换窗），窗口门槛判为**未解决**，同样阻断。

---

## 7. 怎么跑

```bash
# 单元测试（合成数据，已知答案 + GBM 危机校准回归）
poetry run pytest tests/validation/ -q

# 对 reports/ 下既有报告做回顾性体检 → reports/validation_audit.json
poetry run python scripts/validate_reports.py
```

新策略脚本的推荐写法：把 §2 的字段直接写进报告 JSON，并在脚本末尾调用一次
`red_team_checklist(report)` 与 `reproducibility_probe(...)`，让它们自己把缺项/不可复现打印出来再退出。

---

## 8. 首次全量体检结果（2026-09-15）

`reports/validation_audit.json`：**4 RED / 9 AMBER / 0 GREEN**。
每条策略同时带 `deployment_gate`：**0 LIVE / 13 BLOCKED** —— 报告可以带 AMBER 交付，
但没有任何一个策略达到上真钱门槛（见 §6.1）。

- RED：`xsec_gbm_results`、`xsec_gbm_sp500`、`gbm_attribution`、`onchain_btc_backtest`
- 全仓**没有任何一份报告**回答了 significance / window_stability / score_distribution / baseline_significance（13/13 缺失）
- **平台级多重比较**：存过的最优 \|t\| = **1.84**（gbm_attribution 的 pre-2024 月度超额），
  N=27（方向族）时要求线 **3.11**，空假设下 27 次试验的最优期望就有 **2.57**；N=60（变体级上限）时要求线 **3.34** → **两个口径都 FAILS：全平台没有一个结论能通过搜索校正。**
- **复现探针**：仅 2/13 报告带复现证据（risk_gate、gbm_attribution，71/71）；旗舰报告 INSUFFICIENT。
- 事件级检查（新增）：weekend_gap T+1 **t=0.48 / 胜率 Wilson [0.445, 0.798]**；meta-label 50 笔 **t=1.14 / 胜率 44% [0.31, 0.58]**；量比高桶 **z=1.61 vs 要求 2.73（N=8）**。

**本该在第一次就暴露的五个问题**：① IC 的 SE 从未与均值并列（t≈0.84）；② IC 跨年符号翻转的证据早已在 `gbm_attribution` 里（7 年翻 3 次）；③ 分数并列在 shipped `gbm_picks` 里肉眼可见；④ 与动量的显著性对比从未做过，且 t=1.84 在「搜过 27 个方向族」的背景下连单次检验线都过不了；⑤ 没有任何报告主动做复现探针——而它才是真正抓住危机的那把尺子。
