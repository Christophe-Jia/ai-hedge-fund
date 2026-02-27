"""Tests for LlmCryptoStrategy – action → target mapping and execution."""

import time
from datetime import datetime

import pytest

from src.core.constant import Direction, Status
from src.core.event import EVENT_ORDER, EVENT_TRADE, Event, EventEngine
from src.core.objects import BarData
from src.core.oms import OmsEngine
from src.gateways.paper_gateway import PaperGateway
from src.strategies.llm_crypto_strategy import LlmCryptoStrategy


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_bar(symbol: str, price: float) -> BarData:
    return BarData(
        symbol=symbol,
        datetime=datetime(2024, 1, 1),
        open=price,
        high=price * 1.01,
        low=price * 0.99,
        close=price,
        volume=1_000_000.0,
    )


@pytest.fixture
def setup():
    ee = EventEngine(interval=10)
    ee.start()
    oms = OmsEngine(ee)
    gw = PaperGateway(event_engine=ee, initial_cash=1_000_000.0)
    strategy = LlmCryptoStrategy(
        engine=oms,
        gateway=gw,
        name="llm_crypto",
        symbols=["BTC/USDT", "ETH/USDT"],
        setting={"price_add": 0.001},
    )
    strategy.on_init()
    yield ee, oms, gw, strategy
    ee.stop()


# ---------------------------------------------------------------------------
# Target mapping tests (no execution needed)
# ---------------------------------------------------------------------------

def test_buy_increases_target(setup):
    _, _, _, strategy = setup
    strategy.pos_data["BTC/USDT"] = 0.0
    signal = {"BTC/USDT": {"action": "buy", "quantity": 0.5, "confidence": 80}}
    # Patch engine.get_bar to return None so execute_trading is skipped
    strategy.engine.get_bar = lambda sym: None

    strategy.on_signal(signal)

    assert strategy.get_target("BTC/USDT") == pytest.approx(0.5)


def test_sell_reduces_target_floor_zero(setup):
    _, _, _, strategy = setup
    strategy.pos_data["BTC/USDT"] = 0.3
    strategy.set_target("BTC/USDT", 0.3)
    strategy.engine.get_bar = lambda sym: None

    # Sell more than held – should floor at 0
    signal = {"BTC/USDT": {"action": "sell", "quantity": 1.0, "confidence": 70}}
    strategy.on_signal(signal)

    assert strategy.get_target("BTC/USDT") == pytest.approx(0.0)


def test_sell_partial(setup):
    _, _, _, strategy = setup
    strategy.pos_data["BTC/USDT"] = 1.0
    strategy.set_target("BTC/USDT", 1.0)
    strategy.engine.get_bar = lambda sym: None

    signal = {"BTC/USDT": {"action": "sell", "quantity": 0.4, "confidence": 65}}
    strategy.on_signal(signal)

    assert strategy.get_target("BTC/USDT") == pytest.approx(0.6)


def test_hold_does_not_change_target(setup):
    _, _, _, strategy = setup
    strategy.set_target("ETH/USDT", 2.0)
    strategy.engine.get_bar = lambda sym: None

    signal = {"ETH/USDT": {"action": "hold", "quantity": 0.0, "confidence": 50}}
    strategy.on_signal(signal)

    assert strategy.get_target("ETH/USDT") == pytest.approx(2.0)


def test_empty_signal_is_noop(setup):
    _, _, _, strategy = setup
    strategy.set_target("BTC/USDT", 1.0)
    # Should not raise
    strategy.on_signal({})
    assert strategy.get_target("BTC/USDT") == pytest.approx(1.0)


def test_cover_increases_target(setup):
    _, _, _, strategy = setup
    strategy.pos_data["BTC/USDT"] = -0.5  # short position
    strategy.set_target("BTC/USDT", -0.5)
    strategy.engine.get_bar = lambda sym: None

    signal = {"BTC/USDT": {"action": "cover", "quantity": 0.5, "confidence": 60}}
    strategy.on_signal(signal)

    assert strategy.get_target("BTC/USDT") == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# Setting injection
# ---------------------------------------------------------------------------

def test_price_add_setting_injection():
    ee = EventEngine(interval=10)
    ee.start()
    oms = OmsEngine(ee)
    gw = PaperGateway(event_engine=ee)
    strategy = LlmCryptoStrategy(
        engine=oms,
        gateway=gw,
        name="llm_crypto",
        symbols=["BTC/USDT"],
        setting={"price_add": 0.005},
    )
    ee.stop()
    assert strategy.price_add == pytest.approx(0.005)


# ---------------------------------------------------------------------------
# End-to-end: signal → execute_trading → order placed
# ---------------------------------------------------------------------------

def test_buy_signal_places_order(setup):
    ee, oms, gw, strategy = setup
    trades = []
    ee.register(EVENT_TRADE, lambda e: trades.append(e))

    # Seed a bar so PaperGateway can match the limit order
    bar = _make_bar("BTC/USDT", 50_000.0)
    from src.core.event import EVENT_BAR, Event
    ee.put(Event(EVENT_BAR, bar))
    time.sleep(0.15)

    # Mock engine.get_bar to return the bar (so execute_trading fires)
    strategy.engine.get_bar = lambda sym: bar if sym == "BTC/USDT" else None

    strategy.pos_data["BTC/USDT"] = 0.0
    signal = {"BTC/USDT": {"action": "buy", "quantity": 0.01, "confidence": 75}}
    strategy.on_signal(signal)
    time.sleep(0.3)

    assert len(trades) >= 1
    assert trades[0].data.direction == Direction.LONG
