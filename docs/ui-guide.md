# AI Hedge Fund — 前端使用手册

## 启动

```bash
./run.sh
```

浏览器访问 **http://localhost:5173**

---

## 第一步：配置 API Key（必须先做）

点击右上角 **⚙** 图标 → Settings

填写至少一个 LLM 的 Key，推荐 OpenAI：

```
OpenAI API Key  →  sk-...
```

如果要回测免费 ticker（AAPL / GOOGL / MSFT / NVDA / TSLA）以外的美股，还需要：

```
Financial Datasets API Key  →  ...
```

> 填完自动保存，无需点确认。

---

## 界面布局

```
┌─────────────────────── 顶部导航栏 ───────────────────────┐
│  [☰ 左栏]  [⊡ 底栏]  [☰ 右栏]      [⚙ 设置]  [⊞ 工作区] │
├───────────┬──────────────────────────┬───────────────────┤
│           │                          │                   │
│  左侧栏   │       主内容区            │     右侧栏        │
│  Flow列表 │  (Flow编辑器 / 工作区)    │    组件面板       │
│           │                          │                   │
├───────────┴──────────────────────────┴───────────────────┤
│                      底部输出面板                         │
└──────────────────────────────────────────────────────────┘
```

| 快捷键 | 效果 |
|--------|------|
| `Cmd+B` | 显/隐左侧 Flow 列表 |
| `Cmd+I` | 显/隐右侧组件面板 |
| `Cmd+J` | 显/隐底部输出日志 |
| `Cmd+,` | 打开设置 |
| `Cmd+O` | 画布自适应视图 |
| `Cmd+Z` / `Cmd+Shift+Z` | 撤销 / 重做 |

---

## 场景 A — 快速回测策略

点击顶部 **⊞**（Workspace）→ 左侧「策略回测」面板

### ① 选资产类型

```
[美股]  [加密货币]
```

切换后，标的和可用因子会自动重置。

### ② 选分析因子

勾选 AI 用来分析的视角，点击**组标题**可一键全选/全取消：

| 组 | 因子 | 适用 |
|----|------|------|
| **Trend / Momentum** | Technical Analyst、Stanley Druckenmiller、Cathie Wood | 美股 + 加密 |
| **Fundamentals / Value** | Warren Buffett、Ben Graham、Charlie Munger、Michael Burry 等 13 个 | 仅美股 |
| **Sentiment / Market Micro** | Sentiment Analyst、News Sentiment、OB Signal、Polymarket Signal | 美股 + 加密 |

> 加密货币模式下，Fundamentals / Value 组自动禁用（显示灰色）。

### ③ 填参数

| 字段 | 说明 | 默认值 |
|------|------|--------|
| 标的 | 逗号分隔；美股如 `AAPL,MSFT,NVDA`，加密如 `BTC/USDT` | AAPL,MSFT,NVDA |
| 开始 / 结束日期 | 建议跨度 ≥ 3 个月 | 2024-01-01 ~ 2024-12-31 |
| 初始资金 ($) | 模拟起始资金 | 100,000 |
| 滑点 (bps) | 模拟交易摩擦；加密建议 10–20 | 5 |
| 模型 | 驱动 Agent 的 LLM | gpt-4.1 |

### ④ 运行与结果

点击 **Run Backtest**，左侧面板滚动显示实时进度日志。完成后展示：

```
Total Return   Sharpe   Max DD
  +18.4%        1.32    -8.7%

Sortino: 1.81   Days: 252   Final: $118,400
```

| 指标 | 颜色含义 |
|------|---------|
| Total Return | 绿色 = 盈利，红色 = 亏损 |
| Sharpe | ≥1 绿色（良好），0–1 黄色（一般），<0 红色 |
| Max Drawdown | 越小（绝对值）越好，始终显示红色 |

运行中点 **Stop** 可随时中止。

---

## 场景 B — 可视化搭建 Agent 工作流

左侧栏（`Cmd+B`）→ **New Flow**，进入拖拽画布。

右侧面板（`Cmd+I`）提供可用节点：

| 节点类型 | 作用 |
|----------|------|
| **Portfolio Input** | 起点，传入投资组合和日期范围 |
| **Stock Input** | 传入特定股票代码 |
| **Analysts** | 各 AI 分析师，产出交易信号 |
| **Swarms** | 预设分析师组合（快速搭建） |
| **Portfolio Manager** | 终点，汇总所有信号输出最终决策 |

**连线：** 拖动节点右侧圆点 → 连接到下一个节点左侧圆点

典型结构：

```
Portfolio Input ──→ Warren Buffett    ──→
                                           Portfolio Manager
Portfolio Input ──→ Technical Analyst ──→
```

构建完成后点顶部 **Run**，底部面板（`Cmd+J`）实时展示每个 Agent 的分析过程和决策。

> 所有改动每隔 1 秒自动保存，无需手动操作。

---

## 场景 C — 采集本地数据（加密回测前置步骤）

加密回测依赖本地历史数据库。点击 **⊞** Workspace → 右侧「数据采集」面板。

### 首次初始化（按顺序执行）

| 步骤 | 脚本 | 说明 |
|------|------|------|
| 1 | **Seed BTC History** | 回填 3 年 BTC OHLCV + 资金费率，需几分钟 |
| 2 | **Collect Crypto Data** | 下载近期 OHLCV 补齐至今 |
| 3（可选） | **Collect Macro Data** | 下载 DXY、VIX 等宏观指标 |
| 3（可选） | **Backfill Onchain** | 同步链上指标 |

### 持续实时采集（可选）

**Collect Orderbook** 是常驻守护进程，启动后持续抓取 BTC/USDT 订单簿和成交数据，点 **Stop** 才会终止。

### 查看数据状态

面板底部 **Store Status 表格**，显示各数据库的行数和最新时间戳，可确认数据是否齐全。点 **Refresh** 刷新。

---

## 常见问题

**Q: Run Backtest 点了没反应？**
检查：① 至少勾选了一个因子；② 标的字段不为空；③ Settings 里有有效的 LLM API Key。

**Q: 回测结果全是 `—`？**
没有匹配到价格数据。美股免费 ticker 只有 AAPL/GOOGL/MSFT/NVDA/TSLA，其他股票需要 Financial Datasets API Key；加密需要先运行 Seed BTC History。

**Q: 加密模式下基本面因子是灰色的？**
正常现象。Fundamentals / Value 组仅支持美股，切换到加密货币时自动禁用。

**Q: Flow 运行后底部没有输出？**
按 `Cmd+J` 展开底部面板；如果仍为空，检查 Flow 是否有完整的 `Portfolio Input → Analyst → Portfolio Manager` 连线。
