"""Tests for PaperGateway (validates BaseGateway contract)."""

import time
from datetime import datetime

import pytest

from src.core.constant import Action, Direction, OrderType, Status
from src.core.event import (
    EVENT_ACCOUNT,
    EVENT_ORDER,
    EVENT_TRADE,
    EVENT_BAR,
    Event,
    EventEngine,
)
from src.core.objects import BarData, OrderRequest
from src.gateways.paper_gateway import PaperGateway


@pytest.fixture
def setup():
    engine = EventEngine(interval=10)
    engine.start()
    gw = PaperGateway(event_engine=engine, initial_cash=100_000.0)
    yield engine, gw
    engine.stop()


def _buy_req(symbol: str = "AAPL", volume: float = 10.0, price: float = 150.0,
             order_type: OrderType = OrderType.MARKET) -> OrderRequest:
    return OrderRequest(
        symbol=symbol,
        direction=Direction.LONG,
        action=Action.OPEN,
        order_type=order_type,
        volume=volume,
        price=price,
    )


# ---------------------------------------------------------------------------
# send_order
# ---------------------------------------------------------------------------

def test_send_order_returns_vt_orderid(setup):
    engine, gw = setup
    req = _buy_req()
    vt_orderid = gw.send_order(req)
    assert vt_orderid != ""
    assert "AAPL" in vt_orderid


def test_market_order_emits_order_event(setup):
    engine, gw = setup
    received = []
    engine.register(EVENT_ORDER, lambda e: received.append(e))

    gw.send_order(_buy_req(order_type=OrderType.MARKET))
    time.sleep(0.15)

    statuses = [e.data.status for e in received]
    assert Status.SUBMITTING in statuses
    assert Status.ALLTRADED in statuses


def test_market_order_emits_trade_event(setup):
    engine, gw = setup
    trades = []
    engine.register(EVENT_TRADE, lambda e: trades.append(e))

    gw.send_order(_buy_req(order_type=OrderType.MARKET))
    time.sleep(0.15)
    assert len(trades) == 1
    assert trades[0].data.volume == pytest.approx(10.0)


def test_paper_gateway_fills_market_order_immediately(setup):
    engine, gw = setup
    orders = []
    engine.register(EVENT_ORDER, lambda e: orders.append(e))

    gw.send_order(_buy_req(order_type=OrderType.MARKET, price=100.0))
    time.sleep(0.15)

    statuses = [o.data.status for o in orders]
    assert Status.ALLTRADED in statuses


# ---------------------------------------------------------------------------
# Limit order deferred fill
# ---------------------------------------------------------------------------

def test_limit_order_fills_when_price_drops(setup):
    engine, gw = setup
    trades = []
    engine.register(EVENT_TRADE, lambda e: trades.append(e))

    # Place a buy limit at 140, current price unknown
    req = _buy_req(order_type=OrderType.LIMIT, price=140.0)
    gw.send_order(req)
    time.sleep(0.1)
    assert len(trades) == 0  # not filled yet

    # Emit a bar with close=135 (below limit of 140 → should fill)
    bar = BarData(
        symbol="AAPL", datetime=datetime.now(),
        open=136.0, high=137.0, low=134.0, close=135.0, volume=1_000.0,
    )
    engine.put(Event(EVENT_BAR, bar))
    time.sleep(0.2)
    assert len(trades) == 1


# ---------------------------------------------------------------------------
# Cancel order
# ---------------------------------------------------------------------------

def test_cancel_order_changes_status_to_cancelled(setup):
    engine, gw = setup
    order_events = []
    engine.register(EVENT_ORDER, lambda e: order_events.append(e))

    req = _buy_req(order_type=OrderType.LIMIT, price=50.0)  # won't fill at 50
    vt_orderid = gw.send_order(req)
    time.sleep(0.1)

    from src.core.objects import CancelRequest
    orderid = vt_orderid.split(".", 1)[1]
    gw.cancel_order(CancelRequest(symbol="AAPL", orderid=orderid))
    time.sleep(0.1)

    statuses = [e.data.status for e in order_events]
    assert Status.CANCELLED in statuses


# ---------------------------------------------------------------------------
# Account query
# ---------------------------------------------------------------------------

def test_query_account_emits_account_event(setup):
    engine, gw = setup
    accounts = []
    engine.register(EVENT_ACCOUNT, lambda e: accounts.append(e))
    gw.query_account()
    time.sleep(0.1)
    assert len(accounts) >= 1
    assert accounts[0].data.balance == pytest.approx(100_000.0)
