"""
EMA 交叉策略 — src/strategies/ema_strategy.py

纯技术策略，不依赖 LLM，可直接接入 BacktestEngine：

    from src.strategies.ema_strategy import EmaStrategy
    from src.backtesting.engine import BacktestEngine

    engine = BacktestEngine(
        agent=EmaStrategy(fast_period=10, slow_period=30),
        tickers=["NVDA"],
        start_date="2024-01-01",
        end_date="2024-12-31",
        initial_capital=100_000,
        model_name="",
        model_provider="",
        selected_analysts=None,
        initial_margin_requirement=0.0,
    )
    metrics = engine.run_backtest()

────────────────────────────────────────────────────────────
如何扩展新技术因子
────────────────────────────────────────────────────────────
1. 在 src/agents/technicals.py 里添加 calculate_xxx(df) 函数
   （参考 calculate_ema / calculate_rsi / calculate_bollinger_bands）

2. 在本文件 _compute_signal() 方法里调用新函数，作为辅助确认信号：
       rsi = calculate_rsi(df, 14)
       if rsi.iloc[-1] < 30:   # 超卖确认买入
           ...

3. 多策略聚合：新建 src/strategies/combined_strategy.py，
   在 __call__ 里把多个策略的信号加权投票后输出 AgentOutput。
────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import pandas as pd

from src.agents.technicals import calculate_ema
from src.tools.api import get_prices, prices_to_df


class EmaStrategy:
    """
    双 EMA 交叉策略（无 LLM）。

    参数：
        fast_period  : 快线周期，默认 10
        slow_period  : 慢线周期，默认 30
        quantity_pct : 每次开仓用当前现金的比例，默认 0.10（10%）

    信号逻辑：
        金叉（fast 上穿 slow）→ buy
        死叉（fast 下穿 slow）→ sell（清仓）
        其他                 → hold
    """

    def __init__(
        self,
        fast_period: int = 10,
        slow_period: int = 30,
        quantity_pct: float = 0.10,
    ) -> None:
        if fast_period >= slow_period:
            raise ValueError(
                f"fast_period ({fast_period}) must be less than slow_period ({slow_period})"
            )
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.quantity_pct = quantity_pct

    # ─────────────────────────────────────────────────────────────────────────
    # BacktestEngine / AgentController 接口
    # ─────────────────────────────────────────────────────────────────────────

    def __call__(
        self,
        *,
        tickers: list[str],
        start_date: str,
        end_date: str,
        portfolio,
        **kwargs,
    ) -> dict:
        """
        AgentController 会把 portfolio.get_snapshot() 传进来，
        所以 portfolio 可能是 dict（PortfolioSnapshot）或 Portfolio 对象。
        """
        # 统一取 cash 值
        if isinstance(portfolio, dict):
            cash: float = float(portfolio.get("cash", 0.0))
            positions: dict = portfolio.get("positions", {})
        else:
            cash = float(portfolio.cash)
            positions = portfolio.positions  # type: ignore[attr-defined]

        decisions: dict = {}

        for ticker in tickers:
            try:
                action, quantity = self._decide(
                    ticker=ticker,
                    start_date=start_date,
                    end_date=end_date,
                    cash=cash,
                    positions=positions,
                )
            except Exception:
                action, quantity = "hold", 0

            decisions[ticker] = {"action": action, "quantity": quantity}

        return {"decisions": decisions, "analyst_signals": {}}

    # ─────────────────────────────────────────────────────────────────────────
    # 内部实现
    # ─────────────────────────────────────────────────────────────────────────

    def _decide(
        self,
        ticker: str,
        start_date: str,
        end_date: str,
        cash: float,
        positions: dict,
    ) -> tuple[str, int]:
        """
        为单个 ticker 计算 EMA 交叉信号并返回 (action, quantity)。

        Returns:
            ("buy"|"sell"|"hold", quantity)
        """
        prices = get_prices(ticker, start_date, end_date)
        if not prices or len(prices) < 3:
            return "hold", 0

        df = prices_to_df(prices)
        fast_ema: pd.Series = calculate_ema(df, self.fast_period)
        slow_ema: pd.Series = calculate_ema(df, self.slow_period)

        current_price = float(df["close"].iloc[-1])
        if current_price <= 0:
            return "hold", 0

        action, quantity = _crossover_signal(
            fast=fast_ema,
            slow=slow_ema,
            current_price=current_price,
            cash=cash,
            quantity_pct=self.quantity_pct,
            long_shares=_long_shares(positions, ticker),
        )
        return action, quantity


# ─────────────────────────────────────────────────────────────────────────────
# 纯函数辅助（方便单元测试）
# ─────────────────────────────────────────────────────────────────────────────

def _crossover_signal(
    fast: pd.Series,
    slow: pd.Series,
    current_price: float,
    cash: float,
    quantity_pct: float,
    long_shares: int,
) -> tuple[str, int]:
    """
    判断 EMA 金叉/死叉，返回 (action, quantity)。

    金叉：前一根 fast <= slow，当前 fast > slow  → buy
    死叉：前一根 fast >= slow，当前 fast < slow  → sell（清多仓）
    其他：hold
    """
    if len(fast) < 2 or len(slow) < 2:
        return "hold", 0

    prev_fast, curr_fast = float(fast.iloc[-2]), float(fast.iloc[-1])
    prev_slow, curr_slow = float(slow.iloc[-2]), float(slow.iloc[-1])

    # 金叉
    if prev_fast <= prev_slow and curr_fast > curr_slow:
        affordable = int(cash * quantity_pct / current_price)
        if affordable > 0:
            return "buy", affordable
        return "hold", 0

    # 死叉
    if prev_fast >= prev_slow and curr_fast < curr_slow:
        if long_shares > 0:
            return "sell", long_shares
        return "hold", 0

    return "hold", 0


def _long_shares(positions: dict, ticker: str) -> int:
    """从 PortfolioSnapshot.positions 里取 ticker 的多头持仓数量。"""
    pos = positions.get(ticker, {})
    if isinstance(pos, dict):
        return int(pos.get("long", 0))
    # Portfolio.Position 对象（有 .long 属性）
    return int(getattr(pos, "long", 0))
