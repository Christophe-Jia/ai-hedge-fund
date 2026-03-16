from __future__ import annotations

from typing import Optional

from .portfolio import Portfolio
from .types import ActionLiteral, Action


class TradeExecutor:
    """Executes trades against a Portfolio with Backtester-identical semantics.

    When a CostModel is provided, each trade applies slippage to the
    execution price and deducts the exchange fee from cash after fill.
    When no CostModel is provided (default), behaviour is identical to the
    original implementation — fully backward compatible.
    """

    def __init__(self, cost_model=None) -> None:
        self._cost_model = cost_model

    def execute_trade(
        self,
        ticker: str,
        action: ActionLiteral,
        quantity: float,
        current_price: float,
        portfolio: Portfolio,
        market_type: str = "spot",
    ) -> int:
        if quantity is None or quantity <= 0:
            return 0

        # Coerce to enum if strings provided
        try:
            action_enum = Action(action) if not isinstance(action, Action) else action
        except Exception:
            action_enum = Action.HOLD

        if self._cost_model is None:
            return self._execute_no_cost(action_enum, ticker, quantity, current_price, portfolio)

        return self._execute_with_cost(
            action_enum, ticker, quantity, current_price, portfolio, market_type
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _execute_no_cost(
        self,
        action_enum: Action,
        ticker: str,
        quantity: float,
        current_price: float,
        portfolio: Portfolio,
    ) -> int:
        """Original cost-free execution path."""
        if action_enum == Action.BUY:
            return int(portfolio.apply_long_buy(ticker, quantity, float(current_price)))
        if action_enum == Action.SELL:
            return int(portfolio.apply_long_sell(ticker, quantity, float(current_price)))
        if action_enum == Action.SHORT:
            return int(portfolio.apply_short_open(ticker, quantity, float(current_price)))
        if action_enum == Action.COVER:
            return int(portfolio.apply_short_cover(ticker, quantity, float(current_price)))
        return 0

    def _execute_with_cost(
        self,
        action_enum: Action,
        ticker: str,
        quantity: float,
        current_price: float,
        portfolio: Portfolio,
        market_type: str,
    ) -> int:
        """Cost-aware execution: apply slippage to price, then deduct fee."""
        notional = quantity * float(current_price)
        slippage_pct = self._cost_model.slippage_as_pct(notional)
        fee_usd = self._cost_model.compute_trade_cost(notional, market_type)

        qty: float = 0.0
        if action_enum == Action.BUY:
            # Slippage raises the buy price
            qty = portfolio.apply_long_buy(
                ticker, quantity, float(current_price), slippage_pct=slippage_pct
            )
        elif action_enum == Action.SELL:
            # Slippage lowers the sell price
            qty = portfolio.apply_long_sell(
                ticker, quantity, float(current_price), slippage_pct=slippage_pct
            )
        elif action_enum == Action.SHORT:
            qty = portfolio.apply_short_open(ticker, quantity, float(current_price))
        elif action_enum == Action.COVER:
            qty = portfolio.apply_short_cover(ticker, quantity, float(current_price))

        if qty > 0 and fee_usd > 0:
            portfolio.deduct_fee(fee_usd)

        return int(qty)
