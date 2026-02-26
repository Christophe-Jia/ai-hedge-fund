"""Tests for src.core.oms OmsEngine."""

import time
from datetime import datetime

import pytest

from src.core.constant import Action, Direction, OrderType, Status
from src.core.event import (
    EVENT_ACCOUNT,
    EVENT_ORDER,
    EVENT_TRADE,
    Event,
    EventEngine,
)
from src.core.objects import AccountData, OrderData, TradeData
from src.core.oms import OmsEngine


@pytest.fixture
def setup():
    engine = EventEngine(interval=10)  # long interval so timer doesn't interfere
    engine.start()
    oms = OmsEngine(engine)
    yield engine, oms
    engine.stop()


def _make_order(status: Status = Status.SUBMITTING, orderid: str = "001") -> OrderData:
    return OrderData(
        symbol="AAPL",
        orderid=orderid,
        direction=Direction.LONG,
        action=Action.OPEN,
        order_type=OrderType.LIMIT,
        price=150.0,
        volume=10.0,
        status=status,
    )


# ---------------------------------------------------------------------------
# Active orders management
# ---------------------------------------------------------------------------

def test_active_orders_updated_on_submitting(setup):
    engine, oms = setup
    order = _make_order(Status.SUBMITTING)
    engine.put(Event(EVENT_ORDER, order))
    time.sleep(0.1)
    assert order.vt_orderid in oms.active_orders


def test_active_orders_removed_on_alltraded(setup):
    engine, oms = setup
    order = _make_order(Status.SUBMITTING)
    engine.put(Event(EVENT_ORDER, order))
    time.sleep(0.1)
    assert order.vt_orderid in oms.active_orders

    order.status = Status.ALLTRADED
    engine.put(Event(EVENT_ORDER, order))
    time.sleep(0.1)
    assert order.vt_orderid not in oms.active_orders


def test_active_orders_removed_on_cancelled(setup):
    engine, oms = setup
    order = _make_order(Status.SUBMITTING)
    engine.put(Event(EVENT_ORDER, order))
    time.sleep(0.1)

    order.status = Status.CANCELLED
    engine.put(Event(EVENT_ORDER, order))
    time.sleep(0.1)
    assert order.vt_orderid not in oms.active_orders


def test_get_all_active_orders_returns_only_active(setup):
    engine, oms = setup
    active_order = _make_order(Status.SUBMITTING, "001")
    done_order = _make_order(Status.ALLTRADED, "002")

    engine.put(Event(EVENT_ORDER, active_order))
    engine.put(Event(EVENT_ORDER, done_order))
    time.sleep(0.15)

    active = oms.get_all_active_orders()
    ids = [o.vt_orderid for o in active]
    assert "AAPL.001" in ids
    assert "AAPL.002" not in ids


# ---------------------------------------------------------------------------
# Position update from trade
# ---------------------------------------------------------------------------

def test_position_updated_on_trade(setup):
    engine, oms = setup
    trade = TradeData(
        symbol="AAPL",
        orderid="001",
        tradeid="T001",
        direction=Direction.LONG,
        price=150.0,
        volume=10.0,
        datetime=datetime.now(),
    )
    engine.put(Event(EVENT_TRADE, trade))
    time.sleep(0.1)

    pos = oms.get_position_by_symbol("AAPL", Direction.LONG)
    assert pos is not None
    assert pos.volume == pytest.approx(10.0)
    assert pos.avg_price == pytest.approx(150.0)


def test_position_avg_price_updated_on_second_trade(setup):
    engine, oms = setup
    trade1 = TradeData(
        symbol="AAPL", orderid="001", tradeid="T001",
        direction=Direction.LONG, price=100.0, volume=10.0,
        datetime=datetime.now(),
    )
    trade2 = TradeData(
        symbol="AAPL", orderid="002", tradeid="T002",
        direction=Direction.LONG, price=200.0, volume=10.0,
        datetime=datetime.now(),
    )
    engine.put(Event(EVENT_TRADE, trade1))
    time.sleep(0.1)
    engine.put(Event(EVENT_TRADE, trade2))
    time.sleep(0.1)

    pos = oms.get_position_by_symbol("AAPL", Direction.LONG)
    assert pos.volume == pytest.approx(20.0)
    assert pos.avg_price == pytest.approx(150.0)  # (100*10 + 200*10) / 20


# ---------------------------------------------------------------------------
# Account update
# ---------------------------------------------------------------------------

def test_account_updated_on_account_event(setup):
    engine, oms = setup
    account = AccountData(accountid="test", balance=50_000.0, frozen=1_000.0)
    engine.put(Event(EVENT_ACCOUNT, account))
    time.sleep(0.1)

    result = oms.get_account()
    assert result is not None
    assert result.balance == pytest.approx(50_000.0)
    assert result.available == pytest.approx(49_000.0)
