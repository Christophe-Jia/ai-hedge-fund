from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from src.core.constant import Action, Direction, OrderType, Status

# Active statuses - orders in these states are still pending/open
ACTIVE_STATUSES = {Status.SUBMITTING, Status.NOTTRADED, Status.PARTTRADED}


@dataclass
class BarData:
    """OHLCV bar data for a single interval."""

    symbol: str
    datetime: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float

    def __post_init__(self) -> None:
        self.vt_symbol: str = self.symbol


@dataclass
class TickData:
    """Real-time tick data snapshot."""

    symbol: str
    datetime: datetime
    last_price: float
    bid_price: float
    ask_price: float
    bid_volume: float
    ask_volume: float
    volume: float

    def __post_init__(self) -> None:
        self.vt_symbol: str = self.symbol


@dataclass
class OrderData:
    """Order state object - tracks the lifecycle of a submitted order."""

    symbol: str
    orderid: str
    direction: Direction
    action: Action
    order_type: OrderType
    price: float
    volume: float
    traded: float = 0.0
    status: Status = Status.SUBMITTING
    datetime: Optional[datetime] = None

    def __post_init__(self) -> None:
        self.vt_orderid: str = f"{self.symbol}.{self.orderid}"

    def is_active(self) -> bool:
        """Return True if the order is still pending or partially filled."""
        return self.status in ACTIVE_STATUSES

    def create_cancel_request(self) -> "CancelRequest":
        """Create a CancelRequest from this order."""
        return CancelRequest(symbol=self.symbol, orderid=self.orderid)


@dataclass
class TradeData:
    """A single fill event - one order can produce multiple TradeData records."""

    symbol: str
    orderid: str
    tradeid: str
    direction: Direction
    price: float
    volume: float
    datetime: datetime

    def __post_init__(self) -> None:
        self.vt_orderid: str = f"{self.symbol}.{self.orderid}"
        self.vt_tradeid: str = f"{self.symbol}.{self.tradeid}"


@dataclass
class PositionData:
    """Current position state for one symbol/direction pair."""

    symbol: str
    direction: Direction
    volume: float = 0.0
    frozen: float = 0.0
    avg_price: float = 0.0
    pnl: float = 0.0

    def __post_init__(self) -> None:
        self.vt_positionid: str = f"{self.symbol}.{self.direction.value}"


@dataclass
class AccountData:
    """Account balance snapshot."""

    accountid: str
    balance: float = 0.0
    frozen: float = 0.0

    def __post_init__(self) -> None:
        self.available: float = self.balance - self.frozen


@dataclass
class OrderRequest:
    """Intent to place an order - sent to a gateway."""

    symbol: str
    direction: Direction
    action: Action
    order_type: OrderType
    volume: float
    price: float = 0.0
    reference: str = ""  # which strategy/agent originated this request

    def create_order_data(self, orderid: str) -> OrderData:
        """Create an OrderData from this request with the assigned orderid."""
        return OrderData(
            symbol=self.symbol,
            orderid=orderid,
            direction=self.direction,
            action=self.action,
            order_type=self.order_type,
            price=self.price,
            volume=self.volume,
            traded=0.0,
            status=Status.SUBMITTING,
            datetime=datetime.now(),
        )


@dataclass
class CancelRequest:
    """Intent to cancel an existing order."""

    symbol: str
    orderid: str
