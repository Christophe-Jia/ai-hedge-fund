"""
Integration test: signal → order → fill → OMS position update.

Chain:
  DemoStrategy.on_signal({"AAPL": 100})
  → set_target("AAPL", 100)
  → execute_trading()
  → PaperGateway.send_order(market order)
  → EVENT_ORDER(SUBMITTING) → OMS tracks
  → EVENT_ORDER(ALLTRADED) → OMS removes from active
  → EVENT_TRADE → OMS updates position
  → assert oms.get_position_by_symbol("AAPL", LONG).volume == 100
"""

import time
from datetime import datetime

import pytest

from src.core.constant import Action, Direction, OrderType, Status
from src.core.event import EVENT_BAR, EventEngine, Event
from src.core.objects import BarData
from src.core.oms import OmsEngine
from src.core.strategy import BaseStrategy
from src.gateways.paper_gateway import PaperGateway


class SignalStrategy(BaseStrategy):
    def on_init(self) -> None:
        pass

    def on_bar(self, bars: dict) -> None:
        pass

    def on_signal(self, signal: dict) -> None:
        for symbol, target in signal.items():
            self.set_target(symbol, target)
        bars = {
            s: BarData(
                symbol=s,
                datetime=datetime.now(),
                open=150.0, high=152.0, low=149.0, close=150.0,
                volume=1_000_000.0,
            )
            for s in signal
        }
        self.execute_trading(bars=bars)


@pytest.fixture
def full_setup():
    engine = EventEngine(interval=10)
    engine.start()
    oms = OmsEngine(engine)
    gw = PaperGateway(event_engine=engine, initial_cash=1_000_000.0)
    strategy = SignalStrategy(
        engine=oms, gateway=gw, name="signal_strategy",
        symbols=["AAPL"], setting={},
    )
    yield engine, oms, gw, strategy
    engine.stop()


def test_signal_to_position_update(full_setup):
    """Full chain: signal → order → fill → OMS position."""
    engine, oms, gw, strategy = full_setup

    # Pre-seed last_price so the limit order from execute_trading fills immediately
    bar = BarData(
        symbol="AAPL", datetime=datetime.now(),
        open=150.0, high=152.0, low=148.0, close=150.0, volume=1_000_000.0,
    )
    engine.put(Event(EVENT_BAR, bar))
    time.sleep(0.15)

    strategy.on_signal({"AAPL": 100.0})
    time.sleep(0.4)

    pos = oms.get_position_by_symbol("AAPL", Direction.LONG)
    assert pos is not None
    assert pos.volume == pytest.approx(100.0)


def test_no_orders_when_already_at_target(full_setup):
    """If pos == target, execute_trading should not place any orders."""
    from src.core.event import EVENT_ORDER
    engine, oms, gw, strategy = full_setup

    orders_sent = []
    engine.register(EVENT_ORDER, lambda e: orders_sent.append(e))

    strategy.pos_data["AAPL"] = 50.0
    strategy.set_target("AAPL", 50.0)
    bars = {
        "AAPL": BarData(
            symbol="AAPL", datetime=datetime.now(),
            open=150.0, high=152.0, low=149.0, close=150.0, volume=1_000_000.0,
        )
    }
    strategy.execute_trading(bars=bars)
    time.sleep(0.2)

    assert len(orders_sent) == 0


def test_cancel_and_reorder_on_new_signal(full_setup):
    """
    Existing limit orders are cancelled when execute_trading is called again.
    """
    from src.core.event import EVENT_ORDER
    engine, oms, gw, strategy = full_setup

    cancelled_events = []
    engine.register(
        EVENT_ORDER,
        lambda e: cancelled_events.append(e) if e.data.status == Status.CANCELLED else None,
    )

    # Place a limit order manually (price too low so it won't fill)
    from src.core.objects import OrderRequest, CancelRequest
    req = OrderRequest(
        symbol="AAPL", direction=Direction.LONG, action=Action.OPEN,
        order_type=OrderType.LIMIT, volume=10.0, price=1.0,
        reference="signal_strategy",
    )
    vt_orderid = gw.send_order(req)
    # Register it with the strategy so cancel_all knows about it
    from src.core.objects import OrderData
    order_stub = OrderData(
        symbol="AAPL", orderid=vt_orderid.split(".", 1)[1],
        direction=Direction.LONG, action=Action.OPEN,
        order_type=OrderType.LIMIT, price=1.0, volume=10.0,
    )
    strategy.orders[vt_orderid] = order_stub
    strategy.active_orderids.add(vt_orderid)
    time.sleep(0.1)

    # New signal triggers execute_trading which cancels then reorders
    strategy.on_signal({"AAPL": 20.0})
    time.sleep(0.3)

    assert len(cancelled_events) >= 1
