"""Tests for src.core.event EventEngine."""

import threading
import time

import pytest

from src.core.event import (
    EVENT_ORDER,
    EVENT_TICK,
    EVENT_TIMER,
    Event,
    EventEngine,
)


@pytest.fixture
def engine():
    e = EventEngine(interval=0.1)
    e.start()
    yield e
    e.stop()


# ---------------------------------------------------------------------------
# Basic routing
# ---------------------------------------------------------------------------

def test_specific_handler_receives_matching_event(engine):
    received = []
    engine.register(EVENT_ORDER + "AAPL", lambda e: received.append(e))
    engine.put(Event(EVENT_ORDER + "AAPL", "data"))
    time.sleep(0.15)
    assert len(received) == 1
    assert received[0].data == "data"


def test_specific_handler_not_triggered_by_other_symbol(engine):
    received_aapl = []
    received_msft = []
    engine.register(EVENT_ORDER + "AAPL", lambda e: received_aapl.append(e))
    engine.register(EVENT_ORDER + "MSFT", lambda e: received_msft.append(e))
    engine.put(Event(EVENT_ORDER + "AAPL", "aapl"))
    time.sleep(0.15)
    assert len(received_aapl) == 1
    assert len(received_msft) == 0


def test_global_handler_receives_all_events(engine):
    all_events = []
    engine.register_general(lambda e: all_events.append(e))
    engine.put(Event(EVENT_ORDER, "o1"))
    engine.put(Event(EVENT_TICK, "t1"))
    time.sleep(0.2)
    types = [e.type for e in all_events if e.type != "_stop_"]
    assert EVENT_ORDER in types
    assert EVENT_TICK in types


def test_unregister_stops_handler(engine):
    received = []
    handler = lambda e: received.append(e)
    engine.register(EVENT_ORDER, handler)
    engine.put(Event(EVENT_ORDER, "first"))
    time.sleep(0.15)
    assert len(received) == 1

    engine.unregister(EVENT_ORDER, handler)
    engine.put(Event(EVENT_ORDER, "second"))
    time.sleep(0.15)
    assert len(received) == 1  # still 1, handler was removed


# ---------------------------------------------------------------------------
# Timer
# ---------------------------------------------------------------------------

def test_timer_event_fires(engine):
    timers = []
    engine.register(EVENT_TIMER, lambda e: timers.append(1))
    time.sleep(0.45)
    assert len(timers) >= 3


# ---------------------------------------------------------------------------
# Thread safety
# ---------------------------------------------------------------------------

def test_thread_safe_put_from_multiple_threads(engine):
    received = []
    engine.register(EVENT_ORDER, lambda e: received.append(e))

    def producer():
        for _ in range(20):
            engine.put(Event(EVENT_ORDER, "x"))

    threads = [threading.Thread(target=producer) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    time.sleep(0.3)
    assert len(received) == 100  # 5 threads × 20 events
