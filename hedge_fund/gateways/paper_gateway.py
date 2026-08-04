"""
PaperGateway – in-process simulated exchange for testing and paper trading.

Behaviour:
* ``send_order`` immediately emits EVENT_ORDER(SUBMITTING).
* Market orders are filled instantly at the given price (or last bar close).
* Limit orders are queued and filled when ``tick`` price crosses the limit.
* Maintains a simple cash + position ledger for balance queries.
"""

from __future__ import annotations

import threading
import uuid
from collections import defaultdict
from datetime import datetime

from src.core.constant import Action, Direction, OrderType, Status
from src.core.event import EVENT_BAR, EVENT_TICK, Event, EventEngine
from src.core.gateway import BaseGateway
from src.core.objects import (
    AccountData,
    BarData,
    CancelRequest,
    OrderData,
    OrderRequest,
    PositionData,
    TradeData,
)


class PaperGateway(BaseGateway):
    """
    Simulated paper-trading gateway.

    Useful for:
    * Unit / integration tests (no real exchange needed).
    * Paper trading with live market data feeds.
    * Backtesting harnesses that push BarData events.
    """

    default_name: str = "PAPER"
    default_setting: dict = {
        "initial_cash": 100_000.0,
    }

    def __init__(self, event_engine: EventEngine, initial_cash: float = 100_000.0) -> None:
        super().__init__(event_engine, self.default_name)
        self._cash: float = initial_cash
        self._positions: dict[str, float] = defaultdict(float)    # symbol -> volume
        self._avg_prices: dict[str, float] = defaultdict(float)   # symbol -> avg cost
        self._pending_orders: dict[str, OrderData] = {}           # orderid -> order
        self._last_prices: dict[str, float] = {}                  # symbol -> last price
        self._lock = threading.Lock()
        self._order_counter: int = 0

        # Listen for price updates to fill pending limit orders
        self.event_engine.register(EVENT_BAR, self._on_bar_event)
        self.event_engine.register(EVENT_TICK, self._on_tick_event)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def connect(self, setting: dict) -> None:
        cash = setting.get("initial_cash", 100_000.0)
        self._cash = float(cash)
        self.query_account()

    def subscribe(self, symbol: str) -> None:
        pass  # No-op: paper gateway accepts any symbol

    def close(self) -> None:
        pass

    # ------------------------------------------------------------------
    # Order management
    # ------------------------------------------------------------------
    def send_order(self, req: OrderRequest) -> str:
        """
        Accept an order request.
        * MARKET orders → fill immediately at last known price (or req.price).
        * LIMIT orders  → queue for deferred fill when price is reached.
        """
        with self._lock:
            self._order_counter += 1
            orderid = f"PAPER{self._order_counter:06d}"

        order = req.create_order_data(orderid)
        self.on_order(order)  # SUBMITTING

        if req.order_type == OrderType.MARKET:
            fill_price = self._last_prices.get(req.symbol, req.price)
            if fill_price <= 0:
                fill_price = req.price
            self._fill_order(order, fill_price)
        else:
            # Queue limit order
            with self._lock:
                self._pending_orders[orderid] = order
            # Attempt immediate fill if price already satisfies limit
            last = self._last_prices.get(req.symbol, 0.0)
            if last > 0:
                self._try_fill_limit(order, last)

        return order.vt_orderid

    def cancel_order(self, req: CancelRequest) -> None:
        with self._lock:
            order = self._pending_orders.pop(req.orderid, None)
        if order:
            order.status = Status.CANCELLED
            self.on_order(order)

    # ------------------------------------------------------------------
    # Account / position queries
    # ------------------------------------------------------------------
    def query_account(self) -> None:
        with self._lock:
            balance = self._cash
        account = AccountData(accountid="PAPER", balance=balance, frozen=0.0)
        self.on_account(account)

    def query_position(self) -> None:
        with self._lock:
            snapshot = dict(self._positions)
            avg_snapshot = dict(self._avg_prices)
        for symbol, volume in snapshot.items():
            if volume == 0:
                continue
            pos = PositionData(
                symbol=symbol,
                direction=Direction.LONG,
                volume=volume,
                avg_price=avg_snapshot.get(symbol, 0.0),
            )
            self.on_position(pos)

    # ------------------------------------------------------------------
    # Internal: price event handlers
    # ------------------------------------------------------------------
    def _on_bar_event(self, event: Event) -> None:
        bar: BarData = event.data
        self._last_prices[bar.symbol] = bar.close
        self._check_pending_orders(bar.symbol, bar.close)

    def _on_tick_event(self, event: Event) -> None:
        from src.core.objects import TickData
        tick: TickData = event.data
        mid = (tick.bid_price + tick.ask_price) / 2.0
        self._last_prices[tick.symbol] = mid
        self._check_pending_orders(tick.symbol, mid)

    def _check_pending_orders(self, symbol: str, price: float) -> None:
        with self._lock:
            pending = [o for o in self._pending_orders.values() if o.symbol == symbol]
        for order in pending:
            self._try_fill_limit(order, price)

    def _try_fill_limit(self, order: OrderData, market_price: float) -> None:
        """Fill a limit order if market price satisfies the limit."""
        if order.direction == Direction.LONG:
            # Buy limit: fill when market_price <= limit price
            if market_price <= order.price:
                with self._lock:
                    self._pending_orders.pop(order.orderid, None)
                self._fill_order(order, order.price)
        else:
            # Sell limit: fill when market_price >= limit price
            if market_price >= order.price:
                with self._lock:
                    self._pending_orders.pop(order.orderid, None)
                self._fill_order(order, order.price)

    def _fill_order(self, order: OrderData, fill_price: float) -> None:
        """Execute a fill: update ledger, emit ORDER(ALLTRADED) + TRADE events."""
        volume = order.volume

        with self._lock:
            if order.direction == Direction.LONG:
                cost = fill_price * volume
                if cost > self._cash:
                    # Partial fill not implemented: reject if insufficient funds
                    order.status = Status.REJECTED
                    self.on_order(order)
                    return
                self._cash -= cost
                old_vol = self._positions[order.symbol]
                old_avg = self._avg_prices[order.symbol]
                new_vol = old_vol + volume
                self._avg_prices[order.symbol] = (
                    (old_avg * old_vol + fill_price * volume) / new_vol
                    if new_vol > 0 else 0.0
                )
                self._positions[order.symbol] = new_vol
            else:
                # Sell / close long
                held = self._positions.get(order.symbol, 0.0)
                actual_vol = min(volume, held)
                if actual_vol <= 0:
                    order.status = Status.REJECTED
                    self.on_order(order)
                    return
                self._cash += fill_price * actual_vol
                self._positions[order.symbol] = held - actual_vol
                volume = actual_vol  # adjust for partial close

        # Emit filled order
        order.status = Status.ALLTRADED
        order.traded = volume
        self.on_order(order)

        # Emit trade record
        trade = TradeData(
            symbol=order.symbol,
            orderid=order.orderid,
            tradeid=str(uuid.uuid4())[:8],
            direction=order.direction,
            price=fill_price,
            volume=volume,
            datetime=datetime.now(),
        )
        self.on_trade(trade)

        # Refresh account snapshot
        self.query_account()
