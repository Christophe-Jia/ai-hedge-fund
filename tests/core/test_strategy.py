"""Tests for src.core.strategy BaseStrategy target-position model."""

import time
from datetime import datetime
from collections import defaultdict

import pytest

from src.core.constant import Action, Direction, OrderType, Status
from src.core.event import EVENT_BAR, EVENT_ORDER, EVENT_TRADE, Event, EventEngine
from src.core.objects import BarData, OrderData, OrderRequest
from src.core.oms import OmsEngine
from src.core.strategy import BaseStrategy
from src.gateways.paper_gateway import PaperGateway


# ---------------------------------------------------------------------------
# Concrete test strategy
# ---------------------------------------------------------------------------

class DemoStrategy(BaseStrategy):
    top_k: int = 5  # class-level parameter for injection test

    def on_init(self) -> None:
        pass

    def on_bar(self, bars: dict) -> None:
        pass

    def on_signal(self, signal: dict) -> None:
        for symbol, target in signal.items():
            self.set_target(symbol, target)


def _make_bars(prices: dict[str, float]) -> dict[str, BarData]:
    return {
        symbol: BarData(
            symbol=symbol,
            datetime=datetime(2024, 1, 1),
            open=price,
            high=price * 1.01,
            low=price * 0.99,
            close=price,
            volume=1_000_000.0,
        )
        for symbol, price in prices.items()
    }


@pytest.fixture
def trading_setup():
    engine = EventEngine(interval=10)
    engine.start()
    oms = OmsEngine(engine)
    gw = PaperGateway(event_engine=engine, initial_cash=1_000_000.0)
    strategy = DemoStrategy(
        engine=oms,
        gateway=gw,
        name="demo",
        symbols=["AAPL"],
        setting={},
    )
    yield engine, oms, gw, strategy
    engine.stop()


# ---------------------------------------------------------------------------
# set_target / get_target / get_pos
# ---------------------------------------------------------------------------

def test_set_and_get_target(trading_setup):
    _, _, _, strategy = trading_setup
    strategy.set_target("AAPL", 100.0)
    assert strategy.get_target("AAPL") == pytest.approx(100.0)


def test_get_pos_default_zero(trading_setup):
    _, _, _, strategy = trading_setup
    assert strategy.get_pos("AAPL") == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# execute_trading
# ---------------------------------------------------------------------------

def test_execute_trading_buys_when_target_above_pos(trading_setup):
    engine, oms, gw, strategy = trading_setup
    trades = []
    engine.register(EVENT_TRADE, lambda e: trades.append(e))

    # Pre-seed last_price so the limit order fills immediately
    bar = BarData(
        symbol="AAPL", datetime=datetime(2024, 1, 1),
        open=150.0, high=152.0, low=148.0, close=150.0, volume=1_000_000.0,
    )
    engine.put(Event(EVENT_BAR, bar))
    time.sleep(0.15)

    strategy.pos_data["AAPL"] = 0.0
    strategy.set_target("AAPL", 10.0)
    strategy.execute_trading(bars=_make_bars({"AAPL": 150.0}))
    time.sleep(0.3)

    # A buy order should have been sent; limit order fills (price == last_price)
    assert len(trades) >= 1
    assert trades[0].data.direction == Direction.LONG


def test_execute_trading_does_nothing_when_target_equals_pos(trading_setup):
    engine, oms, gw, strategy = trading_setup
    orders = []
    engine.register(EVENT_ORDER, lambda e: orders.append(e))

    strategy.pos_data["AAPL"] = 10.0
    strategy.set_target("AAPL", 10.0)
    strategy.execute_trading(bars=_make_bars({"AAPL": 150.0}))
    time.sleep(0.15)

    # No orders should be placed
    assert len(orders) == 0


def test_execute_trading_cancels_active_before_placing_new(trading_setup):
    engine, oms, gw, strategy = trading_setup
    cancelled = []
    engine.register(EVENT_ORDER, lambda e: cancelled.append(e) if e.data.status == Status.CANCELLED else None)

    # First: place a limit order that won't fill (price too low)
    req = OrderRequest(
        symbol="AAPL", direction=Direction.LONG, action=Action.OPEN,
        order_type=OrderType.LIMIT, volume=5.0, price=1.0,
        reference="demo",
    )
    vt_orderid = gw.send_order(req)
    strategy.active_orderids.add(vt_orderid)
    from src.core.objects import OrderData as OD
    from src.core.constant import Status as S
    order_mock = OD(symbol="AAPL", orderid=vt_orderid.split(".", 1)[1],
                    direction=Direction.LONG, action=Action.OPEN,
                    order_type=OrderType.LIMIT, price=1.0, volume=5.0)
    strategy.orders[vt_orderid] = order_mock
    time.sleep(0.1)

    # Now call execute_trading – should cancel the limit order first
    strategy.set_target("AAPL", 10.0)
    strategy.execute_trading(bars=_make_bars({"AAPL": 150.0}))
    time.sleep(0.2)
    assert len(cancelled) >= 1


# ---------------------------------------------------------------------------
# Setting injection
# ---------------------------------------------------------------------------

def test_setting_injection_overrides_class_attribute():
    engine = EventEngine(interval=10)
    engine.start()
    oms = OmsEngine(engine)
    gw = PaperGateway(event_engine=engine)
    strategy = DemoStrategy(
        engine=oms, gateway=gw, name="demo", symbols=["AAPL"],
        setting={"top_k": 20},
    )
    engine.stop()
    assert strategy.top_k == 20


def test_setting_injection_ignores_unknown_keys():
    engine = EventEngine(interval=10)
    engine.start()
    oms = OmsEngine(engine)
    gw = PaperGateway(event_engine=engine)
    # Should not raise even if setting has a key not on the class
    strategy = DemoStrategy(
        engine=oms, gateway=gw, name="demo", symbols=["AAPL"],
        setting={"unknown_param": 999},
    )
    engine.stop()
    assert not hasattr(strategy, "unknown_param")
