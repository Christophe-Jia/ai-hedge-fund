"""Tests for src.core.objects data classes."""

import pytest
from datetime import datetime

from src.core.constant import Action, Direction, OrderType, Status
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


# ---------------------------------------------------------------------------
# OrderData
# ---------------------------------------------------------------------------

def _make_order(status: Status = Status.SUBMITTING) -> OrderData:
    return OrderData(
        symbol="AAPL",
        orderid="001",
        direction=Direction.LONG,
        action=Action.OPEN,
        order_type=OrderType.LIMIT,
        price=150.0,
        volume=10.0,
        status=status,
    )


def test_order_vt_orderid():
    order = _make_order()
    assert order.vt_orderid == "AAPL.001"


def test_order_is_active_submitting():
    order = _make_order(Status.SUBMITTING)
    assert order.is_active() is True


def test_order_is_active_nottraded():
    order = _make_order(Status.NOTTRADED)
    assert order.is_active() is True


def test_order_is_active_parttraded():
    order = _make_order(Status.PARTTRADED)
    assert order.is_active() is True


def test_order_is_active_alltraded():
    order = _make_order(Status.ALLTRADED)
    assert order.is_active() is False


def test_order_is_active_cancelled():
    order = _make_order(Status.CANCELLED)
    assert order.is_active() is False


def test_order_is_active_rejected():
    order = _make_order(Status.REJECTED)
    assert order.is_active() is False


def test_order_create_cancel_request():
    order = _make_order()
    req = order.create_cancel_request()
    assert isinstance(req, CancelRequest)
    assert req.symbol == "AAPL"
    assert req.orderid == "001"


# ---------------------------------------------------------------------------
# AccountData
# ---------------------------------------------------------------------------

def test_account_available_auto_calculated():
    account = AccountData(accountid="acc1", balance=10_000.0, frozen=2_000.0)
    assert account.available == pytest.approx(8_000.0)


def test_account_available_zero_frozen():
    account = AccountData(accountid="acc1", balance=5_000.0, frozen=0.0)
    assert account.available == pytest.approx(5_000.0)


# ---------------------------------------------------------------------------
# OrderRequest -> OrderData
# ---------------------------------------------------------------------------

def test_order_request_create_order_data():
    req = OrderRequest(
        symbol="AAPL",
        direction=Direction.LONG,
        action=Action.OPEN,
        order_type=OrderType.LIMIT,
        volume=10.0,
        price=150.0,
        reference="test_strategy",
    )
    order = req.create_order_data("002")
    assert order.symbol == "AAPL"
    assert order.orderid == "002"
    assert order.status == Status.SUBMITTING
    assert order.vt_orderid == "AAPL.002"
    assert order.traded == 0.0


# ---------------------------------------------------------------------------
# PositionData
# ---------------------------------------------------------------------------

def test_position_vt_positionid_long():
    pos = PositionData(symbol="AAPL", direction=Direction.LONG, volume=100.0)
    assert pos.vt_positionid == "AAPL.long"


def test_position_vt_positionid_short():
    pos = PositionData(symbol="AAPL", direction=Direction.SHORT, volume=50.0)
    assert pos.vt_positionid == "AAPL.short"


# ---------------------------------------------------------------------------
# BarData / TickData
# ---------------------------------------------------------------------------

def test_bar_vt_symbol():
    bar = BarData(
        symbol="AAPL",
        datetime=datetime(2024, 1, 1),
        open=150.0,
        high=155.0,
        low=149.0,
        close=153.0,
        volume=1_000_000.0,
    )
    assert bar.vt_symbol == "AAPL"


def test_tick_vt_symbol():
    tick = TickData(
        symbol="BTC/USDT",
        datetime=datetime(2024, 1, 1),
        last_price=40_000.0,
        bid_price=39_990.0,
        ask_price=40_010.0,
        bid_volume=1.5,
        ask_volume=2.0,
        volume=500.0,
    )
    assert tick.vt_symbol == "BTC/USDT"


# ---------------------------------------------------------------------------
# TradeData
# ---------------------------------------------------------------------------

def test_trade_vt_ids():
    trade = TradeData(
        symbol="AAPL",
        orderid="001",
        tradeid="T001",
        direction=Direction.LONG,
        price=150.5,
        volume=10.0,
        datetime=datetime(2024, 1, 2),
    )
    assert trade.vt_orderid == "AAPL.001"
    assert trade.vt_tradeid == "AAPL.T001"
