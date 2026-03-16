"""
Order data types for the trading execution engine.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Literal, Optional


class OrderStatus(str, Enum):
    PENDING = "pending"
    OPEN = "open"
    FILLED = "filled"
    PARTIALLY_FILLED = "partially_filled"
    CANCELLED = "cancelled"
    FAILED = "failed"


@dataclass
class Order:
    """Represents a single trade order, normalised across exchanges."""

    symbol: str
    side: Literal["buy", "sell"]
    quantity: float
    order_type: Literal["market", "limit"]
    status: OrderStatus
    id: str = ""
    price: Optional[float] = None
    filled_qty: float = 0.0
    filled_avg_price: Optional[float] = None
    error: Optional[str] = None
    raw: dict = field(default_factory=dict)

    @property
    def is_filled(self) -> bool:
        return self.status == OrderStatus.FILLED

    @property
    def is_failed(self) -> bool:
        return self.status == OrderStatus.FAILED

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "symbol": self.symbol,
            "side": self.side,
            "quantity": self.quantity,
            "order_type": self.order_type,
            "price": self.price,
            "filled_qty": self.filled_qty,
            "filled_avg_price": self.filled_avg_price,
            "status": self.status.value,
            "error": self.error,
        }
