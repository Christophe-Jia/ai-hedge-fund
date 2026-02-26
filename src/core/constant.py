from enum import Enum


class Direction(Enum):
    LONG = "long"
    SHORT = "short"


class Action(Enum):
    OPEN = "open"
    CLOSE = "close"


class OrderType(Enum):
    LIMIT = "limit"
    MARKET = "market"


class Status(Enum):
    SUBMITTING = "submitting"
    NOTTRADED = "not_traded"
    PARTTRADED = "part_traded"
    ALLTRADED = "all_traded"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
