# 失败基准飞轮（Failure-Benchmark Flywheel）

> 每发现一种「结论可以是错的」的方式，就把它永久变成框架的一个回归基准。
> 这一页是飞轮的说明书 + 当前账本。

## 为什么需要它

这一周我们发现了许多失败模式，但每一个都是**手工**变成一次性检查的：

- GBM 分数并列 → `boundary_stability`
- GBM 换测试窗 IC 翻号 → `multi_window`
- weekend_gap 两笔撑起 63% 毛利 → `robustness.leave_k_best_out`
- S&P500 单年彩票 → `robustness.concentration_profile`
- FOMC 效应是 ZIRP 产物、overnight 腿 2023 后失效、meta-label / funding 是 NOISE → `multi_window` / `event_significance`

检查本身都很好，缺的是**入口**：检查与它的校准案例硬编码在个别测试里，新增失败模式没有标准入口。下一个人（或 agent）发现新病，很可能只写进某篇报告，然后被遗忘。飞轮把「我们发现了一种新的犯错方式」变成一件**可执行、会被永久重放**的产物。

## 飞轮怎么转

```
发现一个失败模式
        │
        ▼
写一条基准：detector（哪个框架组件该抓）+ expected_verdict（该输出什么判定）
        │
        ▼
scripts/add_failure_benchmark.py  ── 真的跑一次 detector ──► 抓不到就拒绝登记
        │
        ▼
追加到单一真源 tests/validation/benchmarks/failure_benchmarks.json
        │
        ▼
tests/validation/test_failure_benchmarks.py 永久重放整本账本
        （防止灾难性遗忘：改坏了框架，账本里每一条都会亮红）
        │
        ▼
抓不到、但我们确实遇到过的模式 → 记入 known_gaps（诚实的洞）
        │
        └──► 框架补上后，meta-test 会失败，逼你把该条从 known_gaps 升格为 benchmark
```

**关键约束**：登记时必须**实际运行** detector，确认它确实检出了该失败模式。想象出来的失败模式进不了账本——这保证清单里的每一条都是真的、活的。

## 用法

```bash
# 1) 从 spec 文件登记（会先跑 detector 验证）
poetry run python scripts/add_failure_benchmark.py --spec my_benchmark.json

# 2) 交互式登记
poetry run python scripts/add_failure_benchmark.py --interactive

# 3) 查看当前账本 / 重新验证全部
poetry run python scripts/add_failure_benchmark.py --list
poetry run python scripts/add_failure_benchmark.py --check

# 4) 回归（meta-test 由账本驱动，新增基准无需改测试代码）
poetry run pytest tests/validation/test_failure_benchmarks.py -q
```

`--spec -` 从 stdin 读取；`--update` 覆盖同 id 的旧记录（仍然会重新验证）；`--skeleton` 在 detector 尚无单元测试时生成一个骨架。

### 一条基准记录长什么样

```json
{
  "id": "weekend-gap-few-winners",
  "discovered_at": "2026-09-15",
  "discovered_by": "red-team",
  "title": "weekend_gap long leg needs its best 2 trades",
  "failure_mode": "结论就是那几个观测的算术：去掉最好的 k<=2 笔，显著性就没了。",
  "symptom": "13 笔里去掉最好的 2 笔，双侧 p 从 0.026 到 ~0.11。",
  "detector": "src.validation.robustness.leave_k_best_out",
  "expected_verdict": "FRAGILE_BY_FEW_WINNERS",
  "assertions": {"flag": "FRAGILE_BY_FEW_WINNERS", "first_failure_k": 2},
  "input": {"kind": "series", "report": "exit_rules_backtest.json",
             "path": "trades_full_window.long_only.t_plus_1", "value_key": "ret_pct"},
  "evidence": "reports/exit_rules_backtest.json:...",
  "regression_test": "tests/validation/test_failure_benchmarks.py::test_failure_benchmark_is_caught[weekend-gap-few-winners]"
}
```

**必填**：`id / title / failure_mode / symptom / detector / expected_verdict / input / assertions`。
`detector` 必须是可导入的框架组件；`expected_verdict` 必须是具体判定（`FRAGILE_BY_FEW_WINNERS` / `ARBITRARY` / `SINGLE_EVENT_DRIVEN` / `NOISE` / `UNSTABLE` / `IMPLAUSIBLE` / `NON_REPRODUCIBLE` / `TIE_DETERMINED` …），写「某处有问题」会被拒绝。

`input.kind` 支持：`series`、`mapping`、`consistency`、`cross_section_picks`、`selections`。

## 登记被拒绝时怎么读报错

`add_failure_benchmark.py` 失败时的 `phase` 直接说明问题在哪：

| phase | 含义 | 怎么办 |
|---|---|---|
| `resolve` | detector 路径不存在 / 不可调用 | 改 `detector` 字段 |
| `input` | input spec 拼不出参数 | 改 `input` 字段 |
| `run` | detector 抛异常 | 检查输入是否合法 |
| `assert` | detector 跑了，但没输出期望判定 | 模式是新的 → 进 `known_gaps`；或 detector 该扩展；或 `expected_verdict` 写错了 |

## 当前账本

**14 条失败基准，全部被框架检出（14/14）；1 个已知的洞（known_gaps）。**

| id | title | detector | expected verdict |
|---|---|---|---|
| `gbm-boundary-tie-determined` | GBM top-N boundary decided by identical model scores | `boundary_stability` | `TIE_DETERMINED` |
| `gbm-window-ic-sign-flip` | GBM mean IC flips sign across years | `multi_window` | `UNSTABLE` |
| `gbm-sp500-window-sign-flip` | S&P500 GBM Sharpe flips sign with the test window | `multi_window` | `UNSTABLE` |
| `gbm-internal-consistency-implausible` | Reported Sharpe is an order of magnitude above the IC-implied IR | `internal_consistency` | `IMPLAUSIBLE` |
| `gbm-sp500-single-year-lottery` | S&P500 expansion result carried by a single year (the distressed-stock lottery) | `concentration_profile` | `SINGLE_EVENT_DRIVEN` |
| `weekend-gap-few-winners` | weekend_gap long leg needs its best 2 trades | `leave_k_best_out` | `FRAGILE_BY_FEW_WINNERS` |
| `weekend-gap-redteam-published-series` | weekend_gap red-team series is destroyed by ONE observation | `leave_k_best_out` | `FRAGILE_BY_FEW_WINNERS` |
| `meta-label-honest-noise` | meta-labeling is indistinguishable from zero (honest negative result) | `event_significance` | `NOISE` |
| `funding-no-edge-noise` | Funding-rate signal has no edge over buy-and-hold | `event_significance` | `NOISE` |
| `onchain-yearly-sign-flip` | On-chain BTC signal Sharpe is not sign-consistent across years | `multi_window` | `UNSTABLE` |
| `overnight-leg-sign-flip` | overnight_gap daily-frequency leg dies after 2023 | `multi_window` | `UNSTABLE` |
| `fomc-zirp-era-artifact` | FOMC decision-day effect is a 2020-22 ZIRP artifact | `multi_window` | `UNSTABLE` |
| `gbm-asof-reproducibility-mismatch` | Same month re-run gives a 2/10 overlap with the shipped picks | `reproducibility_probe` | `NON_REPRODUCIBLE` |
| `funding-ftx-loss-tail` | FTX single-event LOSS dominance（损失侧的单事件驱动） | `loss_concentration_profile` | `SINGLE_LOSS_DRIVEN` |

### 已知的洞（known_gaps，等待框架扩展）

| id | 病 | 为什么现在抓不到 | 需要什么 |
|---|---|---|---|
| `gbm-asof-boundary-churn` | 加 1 行 / 21 只股票翻转整月 top-10（2/10） | 存档只有两个月的 picks，并列块没跨过 top-N 切点 → `boundary_stability` 判 STABLE（只报 `has_exact_ties`） | 把扰动 universe 的分数快照与 shipped picks 一起存档，再用 `boundary_stability` / `reproducibility_probe` 直接比对 |

> **补齐进行中（2026-09-18，Gap 2 方案 A）**：`scripts/run_monthly_gbm.py` 现在每次运行都冻结**整池**分数（`pool_scores` + `pool_hash`），并在同月已有旧快照时自动跑 `reproducibility_probe(旧, 新, top_n)` 落 `reproducibility`。但洞**仍然开放**——要等两次可比快照积累出来才能重算 churn，因此该条仍留在 `known_gaps`，元测试继续断言它是洞（等有数据后再按飞轮升格为 benchmark）。

> **已闭合的洞（2026-09-18）**：`funding-ftx-loss-tail` 曾是 Gap 1。根因是「单事件驱动」只定义在**毛利侧**（`SINGLE_EVENT_DRIVEN` 的分母是 gross positive return），所以一笔灾难性**亏损**（FTX -21.1% = 68% 毛亏损）结构性不可见。修复是它的镜像：`loss_concentration_profile`（`SINGLE_LOSS_SHARE=0.50`、`TOP2_LOSS_CONCENTRATION_SHARE=0.60`，分母 = `sum(|负收益|)`），并在 `robustness_battery` 的 flags 里加入 `SINGLE_LOSS_DRIVEN`。**多笔小亏累积不触发**（top-1 占比低），只有单笔巨亏触发。

Meta-test `test_known_gap_is_still_open` 断言这些洞**现在仍然是洞**。哪天有人把框架补上，那条测试会变红，逼着把 gap 升格成 benchmark——飞轮两个方向都转。

## 对应关系：账本 ↔ 审计

`reports/robustness_battery.json` 的 `cases`（7 个校准案例）与 `reports/validation_audit.json` 的 `checks.robustness` 是**同一批**已知发现的一次性视图；本账本把它们提升为**单一真源 + 永久重放**。两者应保持一致：若审计的校准案例与账本分歧，以账本为准并更新审计。
