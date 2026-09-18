# 策略验证规范（Validation Standard）

**v1.2 — 2026-09-18**（在 v1.1 基础上增补）｜**v1.1 — 2026-09-15** 起因：GBM 选股线的可信度危机。既有的 purged walk-forward / 超参锁定 / 成本模型防住了**作弊**，却没防住**噪音**：Sharpe 0.963 与月均 IC 0.0082（SE≈0.0095，t≈0.84）在同一份报告里躺了两天；换个测试窗 IC 就翻号；`--as-of 2021-06` 因分数并列报出 2/10 MISMATCH。
**v1.2 增补**「抗扰动检验（鲁棒性套餐）」（§1.4）：把 weekend_gap 红队**手工**做过的「去掉最好的 2 笔」制度化，让每个策略 / 信号自动接受同一套扰动检验。

> **核心原则：一份策略报告，如果没有回答下面 14 个检查 + 15 条红队问题，并且通过独立红队评审，就等于没写完，不具有决策资格。**

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

### 1.4 抗扰动检验（鲁棒性套餐，v1.2 新增）

前 9 条回答「这个数字统计上成立吗」；这一节回答**下一步**的问题：「这个数字是真的，还是少数几笔观测的算术结果？」
两者独立：一个 t=2.5、p=0.03 的结论完全可能由 2 笔交易撑起（weekend_gap 就是）。

实现：`src/validation/robustness.py`，入口 `robustness_battery(returns, eras=..)`。
`returns` 是**逐笔事件收益**或**逐期（月度/年度）收益**序列——两者统计处理完全相同，只有标签不同（`kind="events"` / `kind="period"`）。
每次扰动重算都复用与 `significance()` 相同的 Student-t 判定线（|t| ≥ 2），所以「扛得住套餐」= 在任何一种扰动下头号检验仍然成立。

| # | 检查 | 函数 | 判定线 | 防哪种失败 |
|---|---|---|---|---|
| 10 | **去掉最好的 k 笔** | `leave_k_best_out(returns, k)` | k 从 0 到 `min(5, n//4)`；若结论在 **k ≤ 2** 就失去显著性/翻号 → 标 `FRAGILE_BY_FEW_WINNERS`，并给出「需要几笔最好交易才能撑起结论」 | weekend_gap 红队手工发现：去掉最好的 2 笔，双侧 p 从 0.053 变 0.18 → 部署决策被推翻。13 笔做多腿实测 k=2。 |
| 11 | **去掉最差的 k 笔** | `leave_k_worst_out(returns, k)` | 对称：结论是否靠少数几笔**亏损**反向支撑（去掉后翻号/失去显著性）→ `FRAGILE_BY_FEW_LOSERS` | 防止「负结论只是几笔大亏造成」；对正结论通常只会更强，所以这是对照，不是主判据。 |
| 12 | **随机丢弃 x%** | `drop_fraction_sweep(returns, fractions=(0.1..0.7), n_trials=50, seed=42)` | 报告各丢弃率下均值/t 的分位与**符号翻转率**；**稳健的结论在丢 70% 数据后仍同号**（翻转率 ≤ 10%） | 点估计的符号可能是小样本巧合；固定 seed 保证可复现。 |
| 13 | **留一年代** | `leave_one_era_out(returns, eras)` | 逐段剔除后重算；某一段被剔除就翻号/失去显著性 → `FRAGILE_BY_ERA` | 来自 FOMC 教训：效应可能全是 2020-22 ZIRP 环境的产物；回答「未来没有那种环境还成立吗」。 |
| 14 | **集中度画像** | `concentration_profile(returns)` | top-1/2/3 笔占**毛利**比例、HHI、有效笔数、最大单笔占比、去掉 top 5% 后的收益；单笔 ≥ 50% 毛利 → `SINGLE_EVENT_DRIVEN`；top-2 ≥ 60% 记 WARN | 专治「2 笔占 63% 毛利」；让「策略其实是一笔交易」在纸面上无法隐藏。 |

总判定 `robustness_battery`（优先级从上到下）：

| 判定 | 条件 |
|---|---|
| `INSUFFICIENT` | n < 5：扰动统计变成组合数学，不做诊断 |
| `SINGLE_EVENT_DRIVEN` | 单笔 ≥ 50% 毛利 |
| `FRAGILE` | 任一扰动标志触发（`FRAGILE_BY_FEW_WINNERS` / `FRAGILE_BY_FEW_LOSERS` / `UNSTABLE_UNDER_RESAMPLING` / `FRAGILE_BY_ERA`） |
| `ROBUST` | 全部扰动下头号检验仍成立 |

**RED / AMBER 归属（写进 `_severity`）**：

| 鲁棒性结果 | 严重度 | 理由 |
|---|---|---|
| `SINGLE_EVENT_DRIVEN` | **RED** | 报告自称的策略其实是一笔观测，结论不成立。 |
| `FRAGILE_BY_FEW_WINNERS`（k ≤ 2） | **RED** | 结论由 ≤2 笔观测决定；这不是稳健性不足，而是结论本身错误。weekend_gap 因此从 AMBER 升为 RED。 |
| 其他 `FRAGILE`（resample / era / few-losers） | **AMBER** | 结论「未被证明稳健」，需补充证据，但尚不构成自相矛盾。 |
| `INSUFFICIENT`（报告没存逐笔/逐期序列） | **AMBER** | 流程缺陷：与「缺必答字段」同类。**禁止伪造序列**。 |

> 已核对的判定线常量写在 `src/validation/robustness.py` 顶部（`FRAGILE_K_MAX=2`、`SIGN_FLIP_RATE_MAX=0.10`、`SINGLE_EVENT_SHARE=0.50`、`TOP2_CONCENTRATION_SHARE=0.60`、`MIN_N_BATTERY=5`），每条都带注释说明理由。

### 1.5 统计去膨胀（PSR / DSR / MutIC）与频率口径（v1.3 新增）

§1.3 的多重比较用的是**自制 Bonferroni-on-t**（把 27 个假设族写进账本后要求 |t|=3.11）。它有两个盲点：**（a）把非正态收益当成正态**——PSR 显式修正偏度/峰度；**（b）基准是 0 而不是搜索噪音**——DSR 把基准提升到「N 次纯噪音试验的最大 Sharpe 期望」。业界标准是 **Bailey & López de Prado 的 PSR / DSR**。

> **实测纠正（2026-09-18）**：任务书曾假设我们的失败源于重尾（weekend_gap 靠 2 笔）。实测 weekend_gap 做多腿 13 笔**偏度 ~0.07（近对称）、原始峰度 2.21（比正态还薄尾）**——它不是肥尾问题。它的 DSR 失败来自「**n=13 面对 126 个变体的搜索**」。教训：先量分布，再谈分布假设；不要用一个未验证的分布故事解释失败。

实现：`src/validation/deflation.py`（入口 `deflation_report(returns, n_trials)` / `deflation_from_stats(sr, n, n_trials)`）。

| # | 检查 | 函数 | 判定线 | 防哪种失败 |
|---|---|---|---|---|
| 15 | **概率化 Sharpe** | `probabilistic_sharpe_ratio(sr, n, skew, kurtosis, sr_benchmark=0)` | Φ[(SR−SR*)·√(n−1)/√(1−γ₃·SR+((γ₄−1)/4)·SR²)] | 同样的 SR，负偏/肥尾下证据更弱；PSR 把这一点算进去 |
| 16 | **噪音天花板** | `expected_max_sharpe(N, σ_SR)` | σ·[(1−γ)Φ⁻¹(1−1/N)+γΦ⁻¹(1−1/(N·e))]（γ=欧拉常数） | N 次试验纯噪音下的最大 Sharpe 期望；N=27 时 σ=1 对应 **2.03** |
| 17 | **去膨胀 Sharpe** | `deflated_sharpe_ratio(sr, n, skew, kurt, N, σ_SR)` | = 以 #16 为基准的 #15；**DSR > 0.95 才算真结构** | 冠军是否只是「搜出来的最大值」 |
| 18 | **MutIC 冗余惩罚** | `mutic_adjusted_ic(ic_raw, max_corr, lam=0.5)` | `IC_adj = IC_raw − 0.5·max_corr`；池内两两相关目标 < 0.30 | 新信号若与池内已有信号高度相关，其边际信息应被折价 |

**判定线与严重度归属（写进 `scripts/validate_reports.py` 的 `_severity` / `_red_flags`）：**

| DSR 结果 | 报告是否声称有 edge | 严重度 | 理由 |
|---|---|---|---|
| `FAILS`（DSR ≤ 0.95） | 是 | **RED** | 结论「不可用」：冠军落在自己那次搜索的噪音天花板之内，却仍在对外声称有效。 |
| `FAILS` | 否（诚实负结果） | **AMBER** | 没有正结论可证伪；DSR 复核了「无 edge」这一结论（与 `multiple_comparisons=FAILS` 同级）。 |
| `INSUFFICIENT`（缺序列或缺 N） | — | **AMBER** | 流程缺陷：与「缺必答字段」同类。**禁止伪造序列或 N**。 |

判定线常量写在 `src/validation/deflation.py` 顶部，各带注释：`DSR_THRESHOLD=0.95`（≥0.95 置信才认作真结构，出处为 SOPHIE《Formulaic Alpha Mining》及 Bailey & LdP 原文）、`MUTIC_LAMBDA_DEFAULT=0.5`、`MUTIC_MAX_CORR=0.30`、`NORMAL_KURTOSIS=3.0`、`MIN_N_DEFLATION=5`。

**两个必须写明的口径（否则 DSR 会静默失效）：**

1. **SR 与 n 必须同频**（频率口径）：日频 SR 配日频 n，或年化 SR 配「年数」。审计脚本对每份报告都显式标注用的是哪种口径（`checks.deflation.frequency`），并在把报告里存的**年化** Sharpe 转回期频时除以 √periods_per_year（`_per_period_sr(value, periods_per_year)`）。**该陷阱已加自动告警**：凡字段名声称期频（monthly/daily/weekly）却被频率转换过的输入，会标 `suspicious_frequency_naming`，汇总进 `deflation_audit.json:summary.frequency_suspects` 并写入该策略的 `red_flags`（当前精确命中 1 个：`exit_mechanism`）。
2. **峰度用「原始峰度」**（正态=3），不是超额峰度：PSR 分母的 `(γ₄−1)/4` 只有在 γ₄=3 时才退化为正态下的 `sqrt(1+0.5·SR²)`。传超额峰度（正态=0）是最常见的实现 bug。

#### 频率口径陷阱：一个命名错误如何制造「假通过」

这不是「数字算错」，而是**同一个数字、两套单位解释**——而 DSR 对单位错误格外敏感，因为频率同时出现在公式的**两处**：

```
PSR = Φ[ (SR − SR*)·√(n−1) / √(1 − γ₃·SR + ((γ₄−1)/4)·SR²) ]     ← 分子有 √(n−1)
DSR = PSR( SR* = E[max SR] = σ_SR · EVT(N) )                        ← 基准 σ_SR 必须与 SR 同频
```

- **分子**：把年化 SR 当成期频 SR（但 n 仍是期数），分子被整体放大 √(每年期数)（月频 ≈ √12 ≈ 3.46），z 值被人为抬高。
- **基准**：`E[max SR]` 的 `σ_SR` 必须和 SR 同频；SR 与 σ_SR 口径不一致，噪音天花板就差一个换算因子。
两处叠加，足以把「落在噪音天花板之内」翻成「远超天花板」。

**真实案例（`exit_mechanism`，同一份报告、同一串数字）**：`baseline.monthly_sharpe=0.922` 名字像月度，实为**年化**（月度收益 × √12），n=71 个月，N=15 变体：

| 解释 | 期频 SR | σ_SR(默认估计) | E[max SR] | **DSR** | 判定 |
|---|---|---|---|---|---|
| 当作月度（**错**） | 0.922 | 0.1427 | 0.253 | **≈1.00** | 假 PASS |
| 除以 √12 转回月度（**对**） | 0.266 | 0.1216 | 0.215 | **0.662** | FAILS |

同一份数据、同一份报告，**只差一个单位解释**：`1.00 → 0.662`，结论从「通过」变成「落在搜索噪音内」。这就是为什么「频率口径」被列为**必须显式声明且自动校验**的项。

**主线**：这与本周所有病灶同源——**结论由口径而非事实决定**。把频率口径写进输出（`checks.deflation.frequency`）并用启发式**自动**识别名不副实的字段（而不是让作者自我声明），是这条纪律在 DSR 上的具体落实。将来若把「频率口径」升级为红队必答第 16 条，本节即其操作定义（当前因会破坏 `test_checklist.py` 的固定断言而未改 checklist，见 §8.2）。


**EVT 天花板锚点（σ_SR=1）**：公式值 1.5746 / 2.5306 / 3.2551 / 3.8607（N=10/100/1000/10000），与「N 个标准正态最大值期望」的**精确解** 1.538753 / 2.507594 / 3.241436 / 3.851616 相差 ≤0.036（精确解由 `scipy.integrate.quad` 对 `∫x·N·φ(x)·Φ(x)^(N−1)dx` 确定性积分得到，误差 <1e-8，另有 2×10⁶ 次蒙特卡洛佐证）。

> **⚠️ 来源分歧（请勿按外部表格"修正"本函数）**：网上流传的一张表（SOPHIE《Formulaic Alpha Mining》）给出 ~1.50/2.20/2.80/3.20。独立验证表明它**既不是** B&LdP 公式的输出，也**不是** N 个正态最大值精确解，且与公式的比值（0.95/0.87/0.86/0.83）不是常数——说明它不是「同一公式配不同 σ」。本实现以公式 + 精确解为准；那张表在测试里只作**下界**断言。若将来有人看到不一致想"修"回表值，请先读 `src/validation/deflation.py:expected_max_sharpe` 的警告段与 `reports/deflation_audit.json:meta.source_discrepancy`。

**平台级交叉验证（关键）**：给定平台存过的最优 |t|=**1.84**（gbm_attribution pre-2024 月度超额，n=38 月），
- Bonferroni-on-t：N=27 时空假设最优 |t| 期望 **2.57**、要求线 **3.11** → **FAILS**；
- DSR：期频 SR=0.298，噪音天花板 E[max SR]=**0.341**（σ_SR 用零 alpha 单次 Sharpe 估计量的抽样 std 估），**PSR=0.962 → DSR=0.399 ≤ 0.95 → FAILS**。

**两个框架结论一致：全平台没有任何一个存过的结论能通过搜索校正。** 而且 DSR 的读法更狠：最优冠军不只没过 Bonferroni 线，它甚至落在自己那次搜索噪音天花板的**中位数以下**。

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

  // v1.2: robustness battery (per-event or per-period return series required)
  "robustness": {
    "verdict": "FRAGILE",                       // ROBUST / FRAGILE / SINGLE_EVENT_DRIVEN / INSUFFICIENT
    "flags": ["FRAGILE_BY_FEW_WINNERS"],
    "leave_k_best_out":   { "n_best_to_sustain": 2, "first_failure_k": 2 },
    "concentration":      { "top1_share": 0.22, "top2_share": 0.39 },
    "drop_fraction_sweep":{ "sign_stable_through": 0.7 }
  },

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
| **AMBER** | 结论「未被证明」：显著性 NOISE/不可计算、**多重比较校正后不显著**、变体族符号不一致、事件样本太小、必答字段缺失（流程缺陷）、鲁棒性 `FRAGILE`（非少数事件型）或 `INSUFFICIENT`（未存逐笔/逐期序列）、**统计去膨胀 `FAILS` 但报告是诚实负结果**、**统计去膨胀 `INSUFFICIENT`**（未存序列或未给 N）。 |
| **RED** | 数字**自相矛盾或不可复现**：IC 与 Sharpe 不可能同时成立、显著的**负**结果、**同一策略**换窗符号翻转、top-N 边界由并列决定、孤立参数峰、**复现探针 NON_REPRODUCIBLE**、**鲁棒性 `SINGLE_EVENT_DRIVEN`**（一笔观测 > 50% 毛利）、**鲁棒性 `FRAGILE_BY_FEW_WINNERS` 且 k ≤ 2**（结论由最好的 ≤2 笔观测决定）、**统计去膨胀 `FAILS`（DSR ≤ 0.95）且报告声称有 edge**（冠军落在自己那次搜索的噪音天花板内却仍声称有效）。 |

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
# 同时产出鲁棒性套餐校准报告 → reports/robustness_battery.json
# 以及逐策略 DSR + 平台级两框架交叉验证 → reports/deflation_audit.json
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

---

## 8.1 v1.2 抗扰动体检（2026-09-18）

在 §8 的 9 项检查之外跑 §1.4 的鲁棒性套餐，逐策略结果（完整数据见 `reports/robustness_battery.json` 的 `cases`，逐策略条目见 `reports/validation_audit.json` 的 `checks.robustness`）：

| 策略 | 序列 | n | 判定 | 标志 |
|---|---|---|---|---|
| `exit_rules_backtest`（weekend_gap 做多腿） | 13 笔逐笔 | 13 | **FRAGILE** | `FRAGILE_BY_FEW_WINNERS`（**k=2**：去掉最好的 2 笔，双侧 p 从 0.026 → 0.109） |
| `meta_label_results` | 50 笔逐笔 | 50 | FRAGILE | `UNSTABLE_UNDER_RESAMPLING`（基线本就 NOISE，t=1.14） |
| `xsec_gbm_results` | 7 个年度 IC 均值 | 7 | FRAGILE | `UNSTABLE_UNDER_RESAMPLING`；top-2 = 60% 毛利 |
| `xsec_gbm_sp500` | 7 个年度 IC 均值 | 7 | SINGLE_EVENT_DRIVEN | 单年占 60% 毛利 |
| `gbm_attribution`（rolling 12m IC） | 59 个重叠月度观测 | 59 | ROBUST | 注意：重叠窗口导致序列相关，朴素 t=4.54 被高估 |
| 其余 8 份报告 | 未存逐笔/逐期序列 | — | INSUFFICIENT | 报告只存汇总数字，扰动检验无法计算（禁止伪造） |

- 体检总览由 **4 RED / 9 AMBER** 变为 **5 RED / 8 AMBER**：新增的 RED 是 `exit_rules_backtest` —— 鲁棒性套餐**抓住了红队手工才发现的问题**（k=2）。
- 鲁棒性计数：`ROBUST=1, FRAGILE=3, SINGLE_EVENT_DRIVEN=1, INSUFFICIENT=8`。
- 校准测试 `reports/robustness_battery.json`：**7/7 期望全部命中**，其中 weekend_gap 的 `n_best_to_sustain=2`。

---

## 8.2 v1.3 统计去膨胀体检（2026-09-18）

逐策略 DSR（完整数据见 `reports/deflation_audit.json`；N 及其依据写在每条的 `n_trials` / `n_trials_basis`）：

| 策略 | N（依据） | n（口径） | SR | PSR | **DSR** | 判定 |
|---|---|---|---|---|---|---|
| `xsec_gbm_results` | 8（INNER_CV_GRID A–H） | 71（月） | 0.278 | 0.989 | **0.795** | FAILS |
| `xsec_gbm_sp500` | 8（同网格） | 71（月） | 0.105 | 0.808 | 0.279 | FAILS |
| `gbm_attribution` | 8（同网格） | 38（月） | 0.298 | 0.962 | **0.624** | FAILS |
| `outofsample_backtest` | 3（OOS 场景） | 544（日） | 0.020 | 0.680 | **0.350** | FAILS |
| `exit_rules_backtest`（weekend_gap 做多腿） | 126（6 出场×3 标的×7 阈值；registry 无变体数） | 13（逐笔） | 0.704 | 0.990 | **0.325** | FAILS |
| `exit_mechanism` | 15（报告自述） | 71（月） | 0.266 | 0.994 | 0.662 | FAILS |
| `risk_gate` | 15（6+3+6 开关变体） | 71（月） | 0.271 | 0.987 | 0.675 | FAILS |
| `meta_label_results` | 4（2 模型×2 CV） | 50（逐笔） | 0.161 | 0.895 | 0.531 | FAILS |
| `funding_rolling_backtest` | 5（阈值变体） | 10（逐笔） | 0.371 | 0.884 | 0.449 | FAILS |
| `combined_signals` | 7（场景/消融） | 862（日） | −0.032 | 0.176 | 0.010 | FAILS |
| `combined_signals_replication` | 5（组合变体） | 884（日） | −0.032 | 0.170 | 0.016 | FAILS |
| `onchain_btc_backtest` | 2（basket / BTC 直接） | 2632（日） | −0.021 | 0.136 | 0.053 | FAILS |
| `volume_confirm` | 8（2 集×4 阈值） | — | — | — | — | **INSUFFICIENT**（未存 Sharpe、未存逐腿序列） |

- 去膨胀计数：**`SURVIVES=0, FAILS=12, INSUFFICIENT=1`** —— 全平台没有任何一个结论的 DSR 超过 0.95。
- 体检总览由 **5 RED / 8 AMBER** 变为 **6 RED / 7 AMBER**：新增的 RED 是 `outofsample_backtest`（它对外声称 OOS 为正、DSR=0.35）。另有 3 份已 RED 的报告（GBM 旗舰、gbm_attribution、weekend_gap）同时被 DSR 命中。
- **平台级交叉验证（两个框架必须一致）**：最优 \|t\|=1.84（gbm_attribution，n=38 月）
  - Bonferroni-on-t：N=27 时要求线 3.11、空假设最优期望 2.57 → **FAILS**；
  - DSR：期频 SR=0.298，E[max SR]=0.341 → PSR=0.962、**DSR=0.399 ≤ 0.95 → FAILS**；
  - `reports/deflation_audit.json:platform_cross_check.consistent = true`。
- **口径陷阱实录（已制度化）**：`exit_mechanism` 的 `baseline.monthly_sharpe=0.922` 名字像期频，其实是**年化**值；若直接当期频 SR 会得到 DSR≈1.0（假通过）。代码里用 `_per_period_sr(...,12)` 转回月度后 DSR=0.662（FAILS）。这正是 §1.5 强调 SR/n 必须同频的真实案例。
  - **检查清单未改动**：评估过给 `checklist.py` 加第 16 条必答问题（频率口径），但会打破 `tests/validation/test_checklist.py`（`test_complete_report_passes` 断言 `n_unanswered==0`、`test_medium_only_gaps_yield_warn` 断言固定 `COMPLETE_REPORT` 为 WARN）→ 按预案**不改 checklist**。
  - **改用启发式告警**：`scripts/validate_reports.py` 对任何「被频率转换过、且字段名却声称是期频（monthly/daily/weekly）」的输入打标，写入 `deflation_audit.json:summary.frequency_suspects` 与该策略的 `red_flags`。当前命中 **1 个：`exit_mechanism`**。
- **来源分歧留档**：曾流传的 EVT 锚点表（1.50/2.20/2.80/3.20）经独立验证**不是** B&LdP 公式的输出，也与 N 个正态最大值精确解不符；本框架以公式 + scipy 确定性积分精确解（1.538753/2.507594/3.241436/3.851616，误差 <1e-8）为准，该表仅作下界。完整记录见 `deflation_audit.json:meta.source_discrepancy`。纪律：**引用来源 → 独立验证 → 发现来源表格有误 → 以可验证的一侧为准**。

---

## 9. 失败基准飞轮（v1.3 新增）

§8/§8.1 的每一项发现都是**手工**变成一次性检查的。§9 把它制度化：每发现一种「结论可以是错的」的方式，就登记成一条**会被永久重放**的失败基准。

**单一真源** `tests/validation/benchmarks/failure_benchmarks.json`，一条一记录：`id / discovered_at / discovered_by / title / failure_mode / symptom / detector / expected_verdict / assertions / input / evidence / regression_test`。`detector` 是**哪个框架组件该抓到它**（如 `concentration_profile`、`leave_k_best_out`、`multi_window`），`expected_verdict` 是**该输出的具体判定**（如 `FRAGILE_BY_FEW_WINNERS`、`SINGLE_EVENT_DRIVEN`、`NOISE`、`UNSTABLE`、`IMPLAUSIBLE`）—— 不允许写「某处有问题」。

**登记门槛（关键）**：`scripts/add_failure_benchmark.py --spec <json>`（或 `--interactive`）在登记前会**实际运行一次**对应 detector，**确认它确实检出**了该失败；检不出则拒绝登记，并按失败阶段给出明确报错：`resolve`（detector 路径错）/ `input`（输入拼不出）/ `run`（detector 抛异常）/ `assert`（跑了但没输出期望判定）。`assert` 阶段失败意味着三条之一：该模式是新的（→ 进 `known_gaps`）、detector 该扩展、或 `expected_verdict` 写错。这样保证清单里没有「想象中的失败模式」。

**自动回归**：`tests/validation/test_failure_benchmarks.py` 是**账本驱动**的 meta-test —— 遍历清单逐条重跑 detector，一条一断言（失败信息直接给出 `failure_mode` + detector）。新增基准是**数据编辑，不是改测试代码**。同时校验账本完整性（必填字段、id 唯一、引用的 report 存在、每条都有 guardian test）。

**诚实地记录洞**：清单另有 `known_gaps` 段，登记「我们确实遇到过、但目前没有任何组件能抓」的模式。meta-test 的 `test_known_gap_is_still_open` 断言这些洞**仍然是洞**——哪天框架补上，它会变红，逼着把 gap 升格为 benchmark。

**当前状态**：**14 条失败基准全部检出（14/14）**；**1 个已知的洞**——`gbm-asof-boundary-churn`（加 1 行翻转整月 top-10，但扰动快照未存档，`boundary_stability` 只能报 `has_exact_ties` 不能报 `ARBITRARY`）。

**Gap 1 已闭合（2026-09-18）**：`funding-ftx-loss-tail` 曾是最大的洞，且暴露了框架定义本身的**不对称**——「单事件驱动」只定义在毛利侧（`SINGLE_EVENT_DRIVEN` 的分母是 gross positive return），于是单笔灾难性**亏损**（FTX -21.1% = 68% 毛亏损）结构性不可见。修复是严格镜像：新增 `loss_concentration_profile`（`SINGLE_LOSS_SHARE=0.50`、`TOP2_LOSS_CONCENTRATION_SHARE=0.60`，分母 = `sum(|负收益|)`）与判定 `SINGLE_LOSS_DRIVEN`，并在 `robustness_battery` 的 flags 里并入（优先级：`INSUFFICIENT → SINGLE_EVENT_DRIVEN → SINGLE_LOSS_DRIVEN（唯一 flag 时）→ FRAGILE → ROBUST`，因此既有的 weekend_gap / GBM FRAGILE 判定不变）。区分「单笔巨亏主导」与「多笔小亏累积」：只有单笔 >= 50% 毛亏损才触发。该条已从 `known_gaps` 升格为 benchmark。完整清单与用法见 `docs/failure_benchmarks.md`。

跑法：

```bash
poetry run python scripts/add_failure_benchmark.py --list    # 查看账本
poetry run python scripts/add_failure_benchmark.py --check   # 重新验证全部（有未检出则退出码 1）
poetry run pytest tests/validation/test_failure_benchmarks.py -q
```
