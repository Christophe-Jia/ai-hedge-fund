"""
OMS (Order Management System) – subscribes to EventEngine and maintains
an in-memory snapshot of the entire trading state.

Responsibilities
----------------
* Keep a full order book (all-time) plus a live active-order sub-index.
* Accumulate TradeData and update PositionData in real time.
* Provide O(1) query methods to avoid repeated exchange round-trips.
"""

from __future__ import annotations

from src.core.constant import Direction
from src.core.event import (
    EVENT_ACCOUNT,
    EVENT_BAR,
    EVENT_ORDER,
    EVENT_POSITION,
    EVENT_TICK,
    EVENT_TRADE,
    Event,
    EventEngine,
)
from src.core.objects import (
    AccountData,
    BarData,
    OrderData,
    PositionData,
    TickData,
    TradeData,
)


class OmsEngine:
    """
    In-memory state manager driven by EventEngine events.

    All state is keyed by ``vt_*`` identifiers to be consistent with the
    data objects.
    """

    def __init__(self, event_engine: EventEngine) -> None:
        self.event_engine: EventEngine = event_engine

        # Current market data
        self.ticks: dict[str, TickData] = {}
        self.bars: dict[str, BarData] = {}

        # Order state
        self.orders: dict[str, OrderData] = {}           # full history
        self.active_orders: dict[str, OrderData] = {}   # live sub-index

        # Trade history
        self.trades: dict[str, TradeData] = {}

        # Position & account
        self.positions: dict[str, PositionData] = {}
        self.account: AccountData | None = None

        self._register_events()

    # ------------------------------------------------------------------
    # Event registration
    # ------------------------------------------------------------------
    def _register_events(self) -> None:
        self.event_engine.register(EVENT_TICK, self._process_tick_event)
        self.event_engine.register(EVENT_BAR, self._process_bar_event)
        self.event_engine.register(EVENT_ORDER, self._process_order_event)
        self.event_engine.register(EVENT_TRADE, self._process_trade_event)
        self.event_engine.register(EVENT_POSITION, self._process_position_event)
        self.event_engine.register(EVENT_ACCOUNT, self._process_account_event)

    # ------------------------------------------------------------------
    # Event processors
    # ------------------------------------------------------------------
    def _process_tick_event(self, event: Event) -> None:
        tick: TickData = event.data
        self.ticks[tick.symbol] = tick

    def _process_bar_event(self, event: Event) -> None:
        bar: BarData = event.data
        self.bars[bar.symbol] = bar

    def _process_order_event(self, event: Event) -> None:
        order: OrderData = event.data
        self.orders[order.vt_orderid] = order

        if order.is_active():
            self.active_orders[order.vt_orderid] = order
        else:
            self.active_orders.pop(order.vt_orderid, None)

    def _process_trade_event(self, event: Event) -> None:
        trade: TradeData = event.data
        self.trades[trade.vt_tradeid] = trade
        self._update_position_from_trade(trade)

    def _process_position_event(self, event: Event) -> None:
        position: PositionData = event.data
        self.positions[position.vt_positionid] = position

    def _process_account_event(self, event: Event) -> None:
        self.account = event.data

    # ------------------------------------------------------------------
    # Position update from trade
    # ------------------------------------------------------------------
    def _update_position_from_trade(self, trade: TradeData) -> None:
        """Incrementally update PositionData based on a fill."""
        pos_id = f"{trade.symbol}.{trade.direction.value}"

        if pos_id not in self.positions:
            self.positions[pos_id] = PositionData(
                symbol=trade.symbol,
                direction=trade.direction,
            )

        pos = self.positions[pos_id]

        # Recalculate average price and volume
        old_volume = pos.volume
        old_avg = pos.avg_price
        new_volume = old_volume + trade.volume

        if new_volume > 0:
            pos.avg_price = (old_avg * old_volume + trade.price * trade.volume) / new_volume
        else:
            pos.avg_price = 0.0

        pos.volume = new_volume

    # ------------------------------------------------------------------
    # Query interface
    # ------------------------------------------------------------------
    def get_tick(self, symbol: str) -> TickData | None:
        return self.ticks.get(symbol)

    def get_bar(self, symbol: str) -> BarData | None:
        return self.bars.get(symbol)

    def get_order(self, vt_orderid: str) -> OrderData | None:
        return self.orders.get(vt_orderid)

    def get_all_orders(self) -> list[OrderData]:
        return list(self.orders.values())

    def get_all_active_orders(self) -> list[OrderData]:
        return list(self.active_orders.values())

    def get_trade(self, vt_tradeid: str) -> TradeData | None:
        return self.trades.get(vt_tradeid)

    def get_all_trades(self) -> list[TradeData]:
        return list(self.trades.values())

    def get_position(self, vt_positionid: str) -> PositionData | None:
        """Look up by full vt_positionid (e.g. 'AAPL.long')."""
        return self.positions.get(vt_positionid)

    def get_position_by_symbol(self, symbol: str, direction: Direction) -> PositionData | None:
        return self.positions.get(f"{symbol}.{direction.value}")

    def get_all_positions(self) -> list[PositionData]:
        return list(self.positions.values())

    def get_account(self) -> AccountData | None:
        return self.account
