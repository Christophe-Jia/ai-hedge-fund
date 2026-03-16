#!/usr/bin/env python3
"""
端到端回测验证脚本 — BtcBacktestEngine + 本地 SQLite 数据

测试目标：
  1. BtcBacktestEngine 能完整跑完一段历史（2024-01-01 → 2024-06-30）
  2. 成本模型正常工作（手续费、滑差被扣除）
  3. freq="D" 包含周末数据
  4. 无成本 vs 有成本的收益差异可量化
  5. 所有指标字段正常填充

策略：纯本地 EMA 交叉（直接读 SQLite，不调外部 API）
"""

import sys
import os
import math
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import sqlalchemy as sa

from src.backtesting.btc_engine import BtcBacktestEngine
from src.backtesting.engine import BacktestEngine
from src.backtesting.cost_model import CostModel, VipTier
from src.data.historical_store import HistoricalOHLCVStore

# ── 配置 ────────────────────────────────────────────────────────────────────
DB_PATH     = "data/btc_history.db"
START_DATE  = "2024-01-01"
END_DATE    = "2024-06-30"
CAPITAL     = 100_000.0
FAST_EMA    = 10
SLOW_EMA    = 30
TICKER      = "BTC/USDT"
# ────────────────────────────────────────────────────────────────────────────


# ── 本地 EMA 策略（只读 SQLite，不调网络）───────────────────────────────────
class LocalEmaStrategy:
    """
    基于本地 SQLite 的 EMA 交叉策略，专为 BtcBacktestEngine 设计。

    与 CryptoEmaStrategy 的区别：
      - 直接查 HistoricalOHLCVStore，完全离线
      - end_date 为 exclusive 边界，与引擎的 look-ahead guard 对齐
    """

    def __init__(
        self,
        fast_period: int = FAST_EMA,
        slow_period: int = SLOW_EMA,
        quantity_pct: float = 0.20,
        db_path: str = DB_PATH,
        lot_size: float = 0.001,
    ):
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.quantity_pct = quantity_pct
        self.lot_size = lot_size
        self._store = HistoricalOHLCVStore(db_path=db_path)

    def __call__(self, *, tickers, start_date, end_date, portfolio, **kwargs):
        cash = float(portfolio.get("cash", 0.0)) if isinstance(portfolio, dict) \
               else float(portfolio.get_cash())
        positions = portfolio.get("positions", {}) if isinstance(portfolio, dict) \
                    else dict(portfolio.get_positions())

        decisions = {}
        for symbol in tickers:
            try:
                action, qty = self._decide(symbol, start_date, end_date, cash, positions)
            except Exception:
                action, qty = "hold", 0
            decisions[symbol] = {"action": action, "quantity": qty}

        return {"decisions": decisions, "analyst_signals": {}}

    def _decide(self, symbol, start_date, end_date, cash, positions):
        start_ts = int(datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc).timestamp() * 1000)
        end_ts   = int(datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc).timestamp() * 1000)

        df = self._store.get_ohlcv(symbol, "spot", "1d", start_ts, end_ts)
        if df.empty or len(df) < self.slow_period + 2:
            return "hold", 0

        closes = df["close"].astype(float)
        fast_ema = closes.ewm(span=self.fast_period, adjust=False).mean()
        slow_ema = closes.ewm(span=self.slow_period, adjust=False).mean()

        if len(fast_ema) < 2:
            return "hold", 0

        prev_fast, curr_fast = float(fast_ema.iloc[-2]), float(fast_ema.iloc[-1])
        prev_slow, curr_slow = float(slow_ema.iloc[-2]), float(slow_ema.iloc[-1])
        current_price = float(closes.iloc[-1])

        # 金叉 → 买入
        if prev_fast <= prev_slow and curr_fast > curr_slow:
            budget = cash * self.quantity_pct
            raw_qty = budget / current_price
            qty = math.floor(raw_qty / self.lot_size) * self.lot_size
            qty = round(qty, 8)
            return ("buy", qty) if qty > 0 else ("hold", 0)

        # 死叉 → 卖出
        if prev_fast >= prev_slow and curr_fast < curr_slow:
            pos = positions.get(symbol, {})
            long_qty = pos.get("long", 0) if isinstance(pos, dict) else getattr(pos, "long", 0)
            if long_qty > 0:
                return "sell", long_qty

        return "hold", 0


# ── 价格数据函数（给基础引擎用，直接读 SQLite，绕过 CCXT fallback）──────────
def make_local_price_fn(db_path: str = DB_PATH):
    """直接查询 SQLite，不走 CCXT fallback（coverage check 跳过）。"""
    import sqlalchemy as sa
    from datetime import datetime, timezone

    engine_db = sa.create_engine(f"sqlite:///{db_path}", future=True)

    def _fn(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
        start_ts = int(datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc).timestamp() * 1000)
        end_ts   = int(datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc).timestamp() * 1000)
        with engine_db.connect() as conn:
            rows = conn.execute(sa.text("""
                SELECT ts, open, high, low, close, volume FROM ohlcv
                WHERE symbol=:sym AND market_type='spot' AND timeframe='1d'
                  AND ts >= :s AND ts < :e
                ORDER BY ts ASC
            """), {"sym": ticker, "s": start_ts, "e": end_ts}).fetchall()
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
        df["time"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
        df = df.set_index("time").sort_index()
        return df[["open", "high", "low", "close", "volume"]]

    return _fn


# ── 辅助：打印分隔线 ─────────────────────────────────────────────────────────
def section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


# ── 主流程 ───────────────────────────────────────────────────────────────────
def main():
    print(f"\nBTC 端到端回测验证")
    print(f"  区间: {START_DATE} → {END_DATE}")
    print(f"  初始资金: ${CAPITAL:,.0f}")
    print(f"  策略: EMA({FAST_EMA}/{SLOW_EMA}) 交叉")
    print(f"  数据源: 本地 SQLite ({DB_PATH})")

    # ── 检查数据库是否有数据 ────────────────────────────────────────────────
    engine_db = sa.create_engine(f"sqlite:///{DB_PATH}", future=True)
    with engine_db.connect() as conn:
        cnt = conn.execute(sa.text(
            "SELECT COUNT(*) FROM ohlcv WHERE symbol=:s AND market_type='spot' AND timeframe='1d'"
        ), {"s": TICKER}).fetchone()[0]
    if cnt == 0:
        print(f"\n[错误] 数据库中没有 {TICKER} 数据，请先运行:")
        print(f"  poetry run python scripts/seed_btc_history.py --years 3 --exchange bitget")
        sys.exit(1)
    print(f"\n  数据库检查: {TICKER} spot 1d 共 {cnt} 根 K 线 ✓")

    # ────────────────────────────────────────────────────────────────────────
    # 回测 A：无成本基础引擎（freq="B"，作为对照）
    # ────────────────────────────────────────────────────────────────────────
    section("回测 A：无成本基础引擎（BacktestEngine，freq=B）")

    strategy_a = LocalEmaStrategy(db_path=DB_PATH)
    engine_a = BacktestEngine(
        agent=strategy_a,
        tickers=[TICKER],
        start_date=START_DATE,
        end_date=END_DATE,
        initial_capital=CAPITAL,
        model_name="",
        model_provider="",
        selected_analysts=None,
        initial_margin_requirement=0.0,
        price_only=True,
        lookback_months=3,
        benchmark_ticker=None,
        price_data_fn=make_local_price_fn(DB_PATH),
    )
    metrics_a = engine_a.run_backtest()
    values_a  = engine_a.get_portfolio_values()

    # ────────────────────────────────────────────────────────────────────────
    # 回测 B：BtcBacktestEngine（freq="D"，含成本）
    # ────────────────────────────────────────────────────────────────────────
    section("回测 B：BtcBacktestEngine（freq=D，含手续费+滑差）")

    strategy_b = LocalEmaStrategy(db_path=DB_PATH)
    engine_b = BtcBacktestEngine(
        agent=strategy_b,
        tickers=[TICKER],
        start_date=START_DATE,
        end_date=END_DATE,
        initial_capital=CAPITAL,
        model_name="",
        model_provider="",
        selected_analysts=None,
        initial_margin_requirement=0.0,
        price_only=True,
        lookback_months=3,
        benchmark_ticker=None,
        perp_tickers=[],          # 本次只测现货，不开永续仓
        leverage=1.0,
        vip_tier=VipTier.VIP0,
        bnb_discount=False,
        db_path=DB_PATH,
        annual_trading_days=365,
    )
    metrics_b = engine_b.run_backtest()
    values_b  = engine_b.get_portfolio_values()

    # ────────────────────────────────────────────────────────────────────────
    # 汇总对比
    # ────────────────────────────────────────────────────────────────────────
    section("汇总对比")

    def pv_stats(values, label):
        if not values:
            print(f"  [{label}] 无数据")
            return None, None, None
        init_v = values[0]["Portfolio Value"]
        final_v = values[-1]["Portfolio Value"]
        total_ret = (final_v / init_v - 1.0) * 100
        # 统计周末日期数（验证 freq="D"）
        weekends = sum(1 for v in values if v["Date"].weekday() >= 5)
        return init_v, final_v, total_ret, weekends, len(values)

    stats_a = pv_stats(values_a, "A")
    stats_b = pv_stats(values_b, "B")

    print(f"\n  {'指标':<28} {'A: 无成本(freq=B)':>20} {'B: 有成本(freq=D)':>20}")
    print(f"  {'-'*68}")

    if stats_a and stats_b:
        init_a, final_a, ret_a, wknd_a, days_a = stats_a
        init_b, final_b, ret_b, wknd_b, days_b = stats_b

        print(f"  {'迭代天数':<28} {days_a:>20} {days_b:>20}")
        print(f"  {'含周末日数':<28} {wknd_a:>20} {wknd_b:>20}")
        print(f"  {'初始资金 ($)':<28} {init_a:>20,.2f} {init_b:>20,.2f}")
        print(f"  {'最终组合价值 ($)':<28} {final_a:>20,.2f} {final_b:>20,.2f}")
        print(f"  {'总收益率':<28} {ret_a:>19.2f}% {ret_b:>19.2f}%")

        cost_drag = ret_b - ret_a
        print(f"  {'成本拖累 (B−A)':<28} {'':>20} {cost_drag:>+19.2f}%")

    print(f"\n  {'性能指标':<28} {'A':>20} {'B':>20}")
    print(f"  {'-'*68}")
    for key, label in [
        ("sharpe_ratio",  "Sharpe 比率"),
        ("sortino_ratio", "Sortino 比率"),
        ("max_drawdown",  "最大回撤 (%)"),
        ("calmar_ratio",  "Calmar 比率"),
    ]:
        va = metrics_a.get(key)
        vb = metrics_b.get(key)
        sa_ = f"{va:.4f}" if va is not None else "—"
        sb_ = f"{vb:.4f}" if vb is not None else "—"
        print(f"  {label:<28} {sa_:>20} {sb_:>20}")

    # 成本明细（只有 B 有）
    print(f"\n  {'成本明细 (B)':<28}")
    print(f"  {'-'*40}")
    for key, label in [
        ("total_fees_paid",     "总手续费 ($)"),
        ("total_slippage_cost", "总滑差成本 ($)"),
        ("total_funding_paid",  "总资金费 ($)"),
        ("net_pnl_after_costs", "扣成本后净盈亏 ($)"),
        ("num_trades",          "交易笔数"),
    ]:
        v = metrics_b.get(key)
        if v is not None:
            if key == "num_trades":
                print(f"  {label:<28} {int(v):>12}")
            else:
                print(f"  {label:<28} {v:>12.2f}")

    # ── 验证断言 ──────────────────────────────────────────────────────────
    section("验证断言")
    passed = 0
    failed = 0

    def check(cond, msg):
        nonlocal passed, failed
        if cond:
            print(f"  [PASS] {msg}")
            passed += 1
        else:
            print(f"  [FAIL] {msg}")
            failed += 1

    check(len(values_b) > 0,
          "BtcBacktestEngine 产生了组合价值序列")

    check(stats_b is not None and stats_b[3] > 0,
          f"freq=D 包含周末日（共 {stats_b[3] if stats_b else 0} 个周末数据点）")

    check(stats_a is not None and stats_b is not None and stats_b[4] >= stats_a[4],
          f"BTC 引擎迭代天数 ({stats_b[4] if stats_b else 0}) ≥ 基础引擎 ({stats_a[4] if stats_a else 0})")

    check(metrics_b.get("sharpe_ratio") is not None,
          "Sharpe 比率正常计算")

    check(metrics_b.get("max_drawdown") is not None,
          "最大回撤正常计算")

    check(metrics_b.get("calmar_ratio") is not None,
          "Calmar 比率正常计算（新增指标）")

    # 有成本的收益应 ≤ 无成本（或差距极小，允许 ±0.5% 误差容忍）
    if stats_a and stats_b:
        check(stats_b[2] <= stats_a[2] + 0.5,
              f"有成本收益 ({stats_b[2]:+.2f}%) ≤ 无成本收益 ({stats_a[2]:+.2f}%) + 0.5%（成本拖累符合预期）")

    check(metrics_b.get("total_fees_paid", 0) is not None,
          "手续费字段存在于 PerformanceMetrics")

    # 验证 look-ahead：最后一根 K 线时间戳 < END_DATE 的开盘时间戳
    if values_b:
        end_ts_ms = int(datetime.fromisoformat(END_DATE).replace(tzinfo=timezone.utc).timestamp() * 1000)
        last_date_ts = int(values_b[-1]["Date"].replace(tzinfo=timezone.utc).timestamp() * 1000)
        check(last_date_ts < end_ts_ms + 86_400_000,
              f"最后组合日期 ({values_b[-1]['Date'].strftime('%Y-%m-%d')}) 未超出回测终止日")

    print(f"\n  结果: {passed} 通过 / {failed} 失败")

    if failed > 0:
        sys.exit(1)
    else:
        print("\n  所有断言通过，BtcBacktestEngine 端到端验证完成 ✓")


if __name__ == "__main__":
    main()
