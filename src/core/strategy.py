"""
BaseStrategy – target-position model strategy template.

Key ideas (from vnpy SpreadStrategyTemplate, simplified):
* ``target_data``   – what the strategy *wants* to hold.
* ``pos_data``      – what the strategy *actually* holds (updated by OMS).
* ``execute_trading`` – computes the diff and fires buy/sell orders.
* Setting injection  – constructor merges a ``setting`` dict into class attrs.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from typing import TYPE_CHECKING

from src.core.constant import Action, Direction, OrderType
from src.core.objects import BarData, CancelRequest, OrderData, OrderRequest

if TYPE_CHECKING:
    from src.core.gateway import BaseGateway
    from src.core.oms import OmsEngine


class BaseStrategy(ABC):
    """
    Abstract base class for all trading strategies.

    Subclasses implement ``on_init``, ``on_bar``, and ``on_signal``.
    The ``execute_trading`` method handles order generation automatically
    based on the difference between target and actual positions.
    """

    # Class-level parameter defaults – override in subclass.
    # These are injected from ``setting`` at construction time.
    parameters: list[str] = []
    variables: list[str] = []

    def __init__(
        self,
        engine: "OmsEngine",
        gateway: "BaseGateway",
        name: str,
        symbols: list[str],
        setting: dict,
    ) -> None:
        self.engine: OmsEngine = engine
        self.gateway: BaseGateway = gateway
        self.strategy_name: str = name
        self.symbols: list[str] = symbols

        # Position tracking (updated from OMS or on fill)
        self.pos_data: dict[str, float] = defaultdict(float)
        self.target_data: dict[str, float] = defaultdict(float)

        # Order tracking
        self.active_orderids: set[str] = set()
        self.orders: dict[str, OrderData] = {}

        # Inject settings into class attributes
        for key, value in setting.items():
            if hasattr(self, key):
                setattr(self, key, value)

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------
    @abstractmethod
    def on_init(self) -> None:
        """Called once before trading begins."""

    @abstractmethod
    def on_bar(self, bars: dict[str, BarData]) -> None:
        """Called on each new bar for all subscribed symbols."""

    @abstractmethod
    def on_signal(self, signal: dict) -> None:
        """Called when an external signal (e.g. from an LLM agent) arrives."""

    # ------------------------------------------------------------------
    # Target-position interface
    # ------------------------------------------------------------------
    def set_target(self, symbol: str, target: float) -> None:
        """Set the desired holding for *symbol* (in shares / contracts)."""
        self.target_data[symbol] = target

    def get_target(self, symbol: str) -> float:
        """Return current target for *symbol* (default 0)."""
        return self.target_data[symbol]

    def get_pos(self, symbol: str) -> float:
        """Return current actual position for *symbol* (default 0)."""
        return self.pos_data[symbol]

    # ------------------------------------------------------------------
    # Order execution
    # ------------------------------------------------------------------
    def execute_trading(
        self, bars: dict[str, BarData], price_add: float = 0.0
    ) -> None:
        """
        Cancel all active orders, then submit new orders to close the gap
        between target and actual positions.

        Args:
            bars:       Current bar data keyed by symbol.
            price_add:  Fractional slippage added to the limit price
                        (e.g. 0.001 = 0.1% above close for buys).
        """
        self.cancel_all()

        for symbol, bar in bars.items():
            diff = self.get_target(symbol) - self.get_pos(symbol)
            if diff > 0:
                price = bar.close * (1.0 + price_add)
                self.buy(symbol, price, abs(diff))
            elif diff < 0:
                price = bar.close * (1.0 - price_add)
                self.sell(symbol, price, abs(diff))

    def buy(self, symbol: str, price: float, volume: float) -> list[str]:
        """Send a LONG OPEN limit order."""
        return self._send_order(
            symbol, Direction.LONG, Action.OPEN, OrderType.LIMIT, price, volume
        )

    def sell(self, symbol: str, price: float, volume: float) -> list[str]:
        """Send a SHORT CLOSE limit order (sell existing long)."""
        return self._send_order(
            symbol, Direction.SHORT, Action.CLOSE, OrderType.LIMIT, price, volume
        )

    def short(self, symbol: str, price: float, volume: float) -> list[str]:
        """Send a SHORT OPEN limit order."""
        return self._send_order(
            symbol, Direction.SHORT, Action.OPEN, OrderType.LIMIT, price, volume
        )

    def cover(self, symbol: str, price: float, volume: float) -> list[str]:
        """Send a LONG CLOSE limit order (cover short)."""
        return self._send_order(
            symbol, Direction.LONG, Action.CLOSE, OrderType.LIMIT, price, volume
        )

    def cancel_all(self) -> None:
        """Cancel every active order owned by this strategy."""
        for vt_orderid in list(self.active_orderids):
            order = self.orders.get(vt_orderid)
            if order and order.is_active():
                req = order.create_cancel_request()
                self.gateway.cancel_order(req)

    # ------------------------------------------------------------------
    # Order lifecycle callback (called by engine)
    # ------------------------------------------------------------------
    def on_order(self, order: OrderData) -> None:
        """Update internal order state; remove from active set when done."""
        self.orders[order.vt_orderid] = order
        if order.is_active():
            self.active_orderids.add(order.vt_orderid)
        else:
            self.active_orderids.discard(order.vt_orderid)

    # ------------------------------------------------------------------
    # Portfolio queries (delegated to OMS / gateway)
    # ------------------------------------------------------------------
    def get_cash_available(self) -> float:
        """Return available cash from the latest account snapshot."""
        account = self.engine.get_account()
        if account is None:
            return 0.0
        return account.available

    def get_portfolio_value(self) -> float:
        """
        Approximate portfolio value = cash + sum(position * last_price).
        Falls back gracefully when price data is unavailable.
        """
        account = self.engine.get_account()
        cash = account.available if account else 0.0

        holdings = 0.0
        for symbol in self.symbols:
            pos = self.engine.get_position_by_symbol(symbol, Direction.LONG)
            if pos and pos.volume > 0:
                bar = self.engine.get_bar(symbol)
                price = bar.close if bar else pos.avg_price
                holdings += pos.volume * price

        return cash + holdings

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _send_order(
        self,
        symbol: str,
        direction: Direction,
        action: Action,
        order_type: OrderType,
        price: float,
        volume: float,
    ) -> list[str]:
        req = OrderRequest(
            symbol=symbol,
            direction=direction,
            action=action,
            order_type=order_type,
            volume=volume,
            price=price,
            reference=self.strategy_name,
        )
        vt_orderid = self.gateway.send_order(req)
        if vt_orderid:
            self.active_orderids.add(vt_orderid)
        return [vt_orderid] if vt_orderid else []
