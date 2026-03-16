"""
LiveTradingManager – singleton-like service that manages live trading sessions.

Each session is identified by a session_id (str). Multiple sessions can run
concurrently (useful for multi-strategy setups), but typically only one is
active in the UI.

Thread safety: uses threading.Lock for dict mutations; individual scheduler /
monitor objects are themselves thread-safe.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


@dataclass
class LiveTradingConfig:
    market: str = "crypto"
    tickers: List[str] = field(default_factory=lambda: ["BTC/USDT"])
    interval_minutes: int = 60
    paper: bool = True
    model_name: str = "gpt-4o"
    model_provider: str = "openai"
    exchange_id: Optional[str] = None


@dataclass
class LiveTradingStatus:
    session_id: str
    status: str  # "stopped", "running", "error"
    config: Optional[LiveTradingConfig] = None
    started_at: Optional[str] = None
    error_message: Optional[str] = None


class _Session:
    """Internal session container."""

    def __init__(self, config: LiveTradingConfig):
        from src.live.scheduler import LiveTradingScheduler
        from src.live.monitor import LiveMonitor

        self.config = config
        self.started_at = datetime.now(tz=timezone.utc).isoformat()
        self.status = "running"
        self.error_message: Optional[str] = None

        # SSE subscribers: list of asyncio Queues (filled from the monitor callback)
        self._subscribers: list = []
        self._sub_lock = threading.Lock()

        # Scheduler
        self.scheduler = LiveTradingScheduler(
            market=config.market,
            tickers=config.tickers,
            interval_minutes=config.interval_minutes,
            paper=config.paper,
            exchange_id=config.exchange_id,
        )

        # Monitor with callback
        def _on_monitor_update(snapshot: dict):
            self._broadcast(snapshot)

        self.monitor = LiveMonitor(
            executor=self.scheduler.executor,
            poll_interval_sec=30,
            on_update=_on_monitor_update,
        )
        self.monitor.start()

        # Scheduler thread (blocks inside .start())
        self._thread = threading.Thread(
            target=self._run_scheduler,
            daemon=True,
            name=f"live-trading-scheduler",
        )
        self._thread.start()

    def _run_scheduler(self):
        try:
            self.scheduler.start()
        except Exception as exc:
            self.status = "error"
            self.error_message = str(exc)

    def stop(self):
        self.scheduler.stop()
        self.monitor.stop()
        self.status = "stopped"

    # ------------------------------------------------------------------
    # SSE pub/sub
    # ------------------------------------------------------------------

    def subscribe(self) -> "asyncio.Queue":
        import asyncio
        q: asyncio.Queue = asyncio.Queue()
        with self._sub_lock:
            self._subscribers.append(q)
        return q

    def unsubscribe(self, q) -> None:
        with self._sub_lock:
            try:
                self._subscribers.remove(q)
            except ValueError:
                pass

    def _broadcast(self, snapshot: dict) -> None:
        """Push snapshot to all SSE subscribers (thread-safe, non-blocking)."""
        with self._sub_lock:
            dead = []
            for q in self._subscribers:
                try:
                    q.put_nowait(snapshot)
                except Exception:
                    dead.append(q)
            for q in dead:
                self._subscribers.remove(q)

    # ------------------------------------------------------------------
    # Getters
    # ------------------------------------------------------------------

    def get_positions(self) -> Dict[str, Any]:
        """Return latest position snapshot from monitor."""
        snap = self.monitor.get_latest()
        balance = snap.get("balance", {})
        if isinstance(balance, dict) and "positions" in balance:
            return balance["positions"]
        # Crypto balance: {asset: qty, ...}
        positions = {}
        for asset, qty in balance.items():
            if qty and asset not in ("USDT", "USD"):
                positions[asset] = {
                    "symbol": asset,
                    "qty": qty,
                    "market_value": None,
                }
        return positions

    def get_order_history(self) -> List[Dict[str, Any]]:
        """Return last 100 orders from scheduler."""
        history = self.scheduler.get_order_history()
        return history[-100:]

    def get_monitor_snapshot(self) -> Dict[str, Any]:
        return self.monitor.get_latest()


class LiveTradingManager:
    """
    Manages one active live trading session per application instance.

    Usage:
        manager = LiveTradingManager.get_instance()
        manager.start("main", config)
        status = manager.get_status("main")
        manager.stop("main")
    """

    _instance: Optional["LiveTradingManager"] = None
    _lock = threading.Lock()

    def __init__(self):
        self._sessions: Dict[str, _Session] = {}
        self._sessions_lock = threading.Lock()

    @classmethod
    def get_instance(cls) -> "LiveTradingManager":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = LiveTradingManager()
        return cls._instance

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self, session_id: str, config: LiveTradingConfig) -> None:
        with self._sessions_lock:
            existing = self._sessions.get(session_id)
            if existing and existing.status == "running":
                raise RuntimeError(f"Session '{session_id}' is already running")
            self._sessions[session_id] = _Session(config)

    def stop(self, session_id: str) -> None:
        with self._sessions_lock:
            session = self._sessions.get(session_id)
        if session is None:
            raise KeyError(f"No session '{session_id}'")
        session.stop()

    def get_status(self, session_id: str) -> LiveTradingStatus:
        session = self._sessions.get(session_id)
        if session is None:
            return LiveTradingStatus(session_id=session_id, status="stopped")
        return LiveTradingStatus(
            session_id=session_id,
            status=session.status,
            config=session.config,
            started_at=session.started_at,
            error_message=session.error_message,
        )

    def get_positions(self, session_id: str) -> Dict[str, Any]:
        session = self._sessions.get(session_id)
        if session is None:
            return {}
        return session.get_positions()

    def get_order_history(self, session_id: str) -> List[Dict[str, Any]]:
        session = self._sessions.get(session_id)
        if session is None:
            return []
        return session.get_order_history()

    def get_monitor_snapshot(self, session_id: str) -> Dict[str, Any]:
        session = self._sessions.get(session_id)
        if session is None:
            return {}
        return session.get_monitor_snapshot()

    def subscribe(self, session_id: str):
        """Return an asyncio.Queue that receives monitor snapshots via SSE."""
        session = self._sessions.get(session_id)
        if session is None:
            return None
        return session.subscribe()

    def unsubscribe(self, session_id: str, queue) -> None:
        session = self._sessions.get(session_id)
        if session:
            session.unsubscribe(queue)
