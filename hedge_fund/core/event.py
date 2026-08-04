"""
Event engine ported from vnpy - single Queue + dedicated processing thread,
separate Timer thread, dual-layer routing (global + per-symbol).
"""

from __future__ import annotations

import threading
from collections import defaultdict
from queue import Empty, Queue
from time import sleep
from typing import Callable

# ---------------------------------------------------------------------------
# Event type constants
# ---------------------------------------------------------------------------
EVENT_TICK = "eTick"
EVENT_BAR = "eBar"
EVENT_ORDER = "eOrder"
EVENT_TRADE = "eTrade"
EVENT_POSITION = "ePosition"
EVENT_ACCOUNT = "eAccount"
EVENT_LOG = "eLog"
EVENT_TIMER = "eTimer"
EVENT_SIGNAL = "eSignal"  # LLM / factor signal


# ---------------------------------------------------------------------------
# Event
# ---------------------------------------------------------------------------
class Event:
    """Minimal event envelope."""

    __slots__ = ("type", "data")

    def __init__(self, type_: str, data: object = None) -> None:
        self.type: str = type_
        self.data: object = data


# ---------------------------------------------------------------------------
# EventEngine
# ---------------------------------------------------------------------------
HandlerType = Callable[[Event], None]


class EventEngine:
    """
    Thread-safe event engine.

    * Single Queue consumed by one processing thread.
    * Separate Timer thread that puts EVENT_TIMER events at a fixed interval.
    * Two-level handler registry:
        - ``_handlers[event_type]``  – type-specific handlers
        - ``_general_handlers``      – receive every event (logging, monitoring)
    """

    def __init__(self, interval: float = 1.0) -> None:
        """
        Args:
            interval: Timer tick interval in seconds (default 1 s).
        """
        self._interval: float = interval
        self._queue: Queue[Event] = Queue()
        self._active: bool = False

        self._handlers: dict[str, list[HandlerType]] = defaultdict(list)
        self._general_handlers: list[HandlerType] = []

        self._thread: threading.Thread = threading.Thread(
            target=self._run, daemon=True, name="EventEngine"
        )
        self._timer: threading.Thread = threading.Thread(
            target=self._run_timer, daemon=True, name="EventTimer"
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self) -> None:
        """Start the processing and timer threads."""
        self._active = True
        self._thread.start()
        self._timer.start()

    def stop(self) -> None:
        """Signal threads to stop and join them."""
        self._active = False
        # Unblock the processing thread if it is waiting on an empty queue
        self._queue.put(Event("_stop_"))
        self._thread.join()
        self._timer.join()

    # ------------------------------------------------------------------
    # Publish
    # ------------------------------------------------------------------
    def put(self, event: Event) -> None:
        """Put an event into the queue (thread-safe, non-blocking)."""
        self._queue.put(event)

    # ------------------------------------------------------------------
    # Subscribe
    # ------------------------------------------------------------------
    def register(self, type_: str, handler: HandlerType) -> None:
        """Register *handler* for events of *type_*."""
        handler_list = self._handlers[type_]
        if handler not in handler_list:
            handler_list.append(handler)

    def unregister(self, type_: str, handler: HandlerType) -> None:
        """Remove *handler* from events of *type_*."""
        handler_list = self._handlers[type_]
        if handler in handler_list:
            handler_list.remove(handler)

    def register_general(self, handler: HandlerType) -> None:
        """Register a handler that receives **every** event."""
        if handler not in self._general_handlers:
            self._general_handlers.append(handler)

    def unregister_general(self, handler: HandlerType) -> None:
        """Remove a general handler."""
        if handler in self._general_handlers:
            self._general_handlers.remove(handler)

    # ------------------------------------------------------------------
    # Internal threads
    # ------------------------------------------------------------------
    def _run(self) -> None:
        """Main processing loop – runs in the dedicated event thread."""
        while self._active:
            try:
                event: Event = self._queue.get(block=True, timeout=0.1)
            except Empty:
                continue

            if event.type == "_stop_":
                break

            self._process(event)

    def _run_timer(self) -> None:
        """Timer loop – fires EVENT_TIMER at the configured interval."""
        while self._active:
            sleep(self._interval)
            if self._active:
                self.put(Event(EVENT_TIMER))

    def _process(self, event: Event) -> None:
        """Dispatch event to all registered handlers."""
        # Type-specific handlers
        if event.type in self._handlers:
            for handler in list(self._handlers[event.type]):
                handler(event)

        # General handlers
        for handler in list(self._general_handlers):
            handler(event)
