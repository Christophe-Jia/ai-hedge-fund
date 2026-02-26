"""
BaseGateway – abstract interface every exchange adapter must implement.

Design principles (from vnpy):
* ``connect`` / ``subscribe`` / ``send_order`` / ``cancel_order`` are the
  four mandatory operations.
* ``query_account`` and ``query_position`` pull snapshots from the exchange
  and push the results back via ``on_account`` / ``on_position``.
* All callbacks (``on_*``) push ``Event`` objects into the shared
  ``EventEngine`` – the caller never knows which thread calls back.
* ``default_setting`` class attribute self-describes required config keys,
  so configuration UIs can render forms without instantiation.
"""

from __future__ import annotations

import dataclasses
from abc import ABC, abstractmethod
from datetime import datetime

from src.core.event import (
    EVENT_ACCOUNT,
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
    CancelRequest,
    OrderData,
    OrderRequest,
    PositionData,
    TickData,
    TradeData,
)


class BaseGateway(ABC):
    """
    Abstract base class for exchange gateways.

    Subclasses override the abstract methods and call the ``on_*`` callbacks
    to publish state changes into the event engine.
    """

    # Override in subclass to declare required connection parameters.
    # e.g. {"api_key": "", "api_secret": "", "paper": True}
    default_name: str = ""
    default_setting: dict = {}

    def __init__(self, event_engine: EventEngine, gateway_name: str = "") -> None:
        self.event_engine: EventEngine = event_engine
        self.gateway_name: str = gateway_name or self.default_name

    # ------------------------------------------------------------------
    # Abstract interface – subclasses must implement
    # ------------------------------------------------------------------
    @abstractmethod
    def connect(self, setting: dict) -> None:
        """Establish connection to the exchange using *setting*."""

    @abstractmethod
    def subscribe(self, symbol: str) -> None:
        """Subscribe to market data for *symbol*."""

    @abstractmethod
    def send_order(self, req: OrderRequest) -> str:
        """
        Submit an order.

        Returns the exchange-assigned order ID string.
        Must call ``on_order`` with SUBMITTING status before returning.
        """

    @abstractmethod
    def cancel_order(self, req: CancelRequest) -> None:
        """Request cancellation of an existing order."""

    @abstractmethod
    def query_account(self) -> None:
        """Fetch account balance; result delivered via ``on_account``."""

    @abstractmethod
    def query_position(self) -> None:
        """Fetch current positions; result delivered via ``on_position``."""

    # ------------------------------------------------------------------
    # Optional override
    # ------------------------------------------------------------------
    def query_history(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        interval: str = "1d",
    ) -> list[BarData]:
        """
        Fetch historical OHLCV bars.  Default returns empty list;
        subclasses that support history should override.
        """
        return []

    def close(self) -> None:
        """Clean up resources (optional override)."""

    # ------------------------------------------------------------------
    # Callbacks – push data into the event engine
    # ------------------------------------------------------------------
    def on_tick(self, tick: TickData) -> None:
        self.event_engine.put(Event(EVENT_TICK + tick.symbol, tick))
        self.event_engine.put(Event(EVENT_TICK, tick))

    def on_order(self, order: OrderData) -> None:
        # Snapshot the dataclass so later mutations don't affect queued events
        snapshot = dataclasses.replace(order)
        self.event_engine.put(Event(EVENT_ORDER + snapshot.vt_orderid, snapshot))
        self.event_engine.put(Event(EVENT_ORDER, snapshot))

    def on_trade(self, trade: TradeData) -> None:
        from src.core.event import EVENT_TRADE
        self.event_engine.put(Event(EVENT_TRADE + trade.vt_tradeid, trade))
        self.event_engine.put(Event(EVENT_TRADE, trade))

    def on_position(self, position: PositionData) -> None:
        self.event_engine.put(Event(EVENT_POSITION, position))

    def on_account(self, account: AccountData) -> None:
        self.event_engine.put(Event(EVENT_ACCOUNT, account))
