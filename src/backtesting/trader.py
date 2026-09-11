from __future__ import annotations

from typing import Optional

from .portfolio import Portfolio
from .perpetual import PerpPortfolio
from .types import ActionLiteral, Action


class TradeExecutor:
    """Executes trades against a Portfolio with Backtester-identical semantics.

    When a CostModel is provided, each trade applies slippage to the
    execution price and deducts the exchange fee from cash after fill.
    When no CostModel is provided (default), behaviour is identical to the
    original implementation — fully backward compatible.

    Perpetual wiring (P1D): when `perp_portfolio` is provided and
    `market_type == "perp"`, decisions are routed to the PerpPortfolio —
    BUY/SHORT open isolated-margin positions (margin + fees debited from
    spot cash), SELL/COVER close them (margin + PnL credited back).
    """

    def __init__(self, cost_model=None, perp_portfolio: Optional[PerpPortfolio] = None,
                 leverage: float = 1.0) -> None:
        self._cost_model = cost_model
        self._perp_portfolio = perp_portfolio
        self._leverage = leverage

    def execute_trade(
        self,
        ticker: str,
        action: ActionLiteral,
        quantity: float,
        current_price: float,
        portfolio: Portfolio,
        market_type: str = "spot",
        timestamp: str = "",
    ) -> float:
        if quantity is None or quantity <= 0:
            return 0.0

        # Coerce to enum if strings provided
        try:
            action_enum = Action(action) if not isinstance(action, Action) else action
        except Exception:
            action_enum = Action.HOLD

        if action_enum == Action.HOLD:
            return 0.0

        # --- Perp routing -------------------------------------------------
        if market_type == "perp" and self._perp_portfolio is not None:
            return self._execute_perp(
                action_enum, ticker, quantity, float(current_price),
                portfolio, timestamp,
            )

        if self._cost_model is None:
            return self._execute_no_cost(action_enum, ticker, quantity, current_price, portfolio)

        return self._execute_with_cost(
            action_enum, ticker, quantity, current_price, portfolio, market_type
        )

    # ------------------------------------------------------------------
    # Perp execution
    # ------------------------------------------------------------------

    def _execute_perp(
        self,
        action_enum: Action,
        ticker: str,
        quantity: float,
        price: float,
        portfolio: Portfolio,
        timestamp: str,
    ) -> float:
        notional = quantity * price
        if self._cost_model is not None:
            fee_usd = self._cost_model.compute_trade_cost(notional, "perp", symbol=ticker)
            slippage_usd = self._cost_model.compute_slippage_only(notional, symbol=ticker)
        else:
            fee_usd, slippage_usd = 0.0, 0.0

        if action_enum in (Action.BUY, Action.SHORT):
            side = "long" if action_enum == Action.BUY else "short"
            pos, consumed = self._perp_portfolio.open_position(
                ticker, side, quantity, price, self._leverage,
                available_cash=portfolio.get_cash(),
                timestamp=timestamp,
                fee_usd=fee_usd,
                slippage_usd=slippage_usd,
            )
            if pos is None:
                return 0.0  # insufficient cash for margin + costs
            portfolio.debit_cash(consumed)
            return quantity

        if action_enum in (Action.SELL, Action.COVER):
            pos = self._perp_portfolio.get_positions().get(ticker)
            if pos is None:
                return 0.0  # nothing to close
            closed = min(quantity, pos.size)
            _realized, cash_returned = self._perp_portfolio.close_position(
                ticker, price, timestamp=timestamp,
                fee_usd=fee_usd, slippage_usd=slippage_usd,
            )
            portfolio.credit_cash(cash_returned)
            return closed

        return 0.0

    # ------------------------------------------------------------------
    # Internal helpers (spot)
    # ------------------------------------------------------------------

    def _execute_no_cost(
        self,
        action_enum: Action,
        ticker: str,
        quantity: float,
        current_price: float,
        portfolio: Portfolio,
    ) -> float:
        """Original cost-free execution path."""
        if action_enum == Action.BUY:
            return portfolio.apply_long_buy(ticker, quantity, float(current_price))
        if action_enum == Action.SELL:
            return portfolio.apply_long_sell(ticker, quantity, float(current_price))
        if action_enum == Action.SHORT:
            return portfolio.apply_short_open(ticker, quantity, float(current_price))
        if action_enum == Action.COVER:
            return portfolio.apply_short_cover(ticker, quantity, float(current_price))
        return 0.0

    def _execute_with_cost(
        self,
        action_enum: Action,
        ticker: str,
        quantity: float,
        current_price: float,
        portfolio: Portfolio,
        market_type: str,
    ) -> float:
        """Cost-aware execution: apply slippage to price, then deduct fee."""
        notional = quantity * float(current_price)
        slippage_pct = self._cost_model.slippage_as_pct(notional, symbol=ticker)
        fee_usd = self._cost_model.compute_trade_cost(notional, market_type, symbol=ticker)

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

        return float(qty)
