"""
Live trading monitor – real-time position and P&L tracker.

Polls the exchange at regular intervals and prints/stores a summary.
Integrates with AlertEngine if provided.
"""

import threading
import time
from datetime import datetime, timezone
from typing import Optional

from src.trading.executor import TradeExecutor


class LiveMonitor:
    """
    Polls the exchange for account balance and open orders,
    printing a summary and optionally calling a callback.

    Args:
        executor:          Configured TradeExecutor instance
        poll_interval_sec: How often to poll (default 30s)
        on_update:         Optional callback called with the latest snapshot dict
        alert_engine:      Optional AlertEngine; checked on every snapshot
    """

    def __init__(
        self,
        executor: TradeExecutor,
        poll_interval_sec: int = 30,
        on_update=None,
        alert_engine=None,
    ):
        self.executor = executor
        self.poll_interval = poll_interval_sec
        self.on_update = on_update
        self.alert_engine = alert_engine  # Optional[AlertEngine]
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._latest_snapshot: dict = {}

    def start(self) -> None:
        """Start monitoring in a background daemon thread."""
        self._thread = threading.Thread(target=self._loop, daemon=True, name="live-monitor")
        self._thread.start()
        print(f"[monitor] Started (interval={self.poll_interval}s)")

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)

    def get_latest(self) -> dict:
        return dict(self._latest_snapshot)

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                balance = self.executor.get_balance()
                open_orders = self.executor.get_open_orders()
                snapshot = {
                    "timestamp": datetime.now(tz=timezone.utc).isoformat(),
                    "balance": balance,
                    "open_orders": open_orders,
                }
                self._latest_snapshot = snapshot
                self._print_snapshot(snapshot)

                # Fire alert engine if configured
                if self.alert_engine is not None:
                    try:
                        self.alert_engine.check(snapshot)
                    except Exception as ae:
                        print(f"[monitor] AlertEngine error: {ae}")

                if self.on_update:
                    self.on_update(snapshot)
            except Exception as e:
                print(f"[monitor] Poll error: {e}")
            self._stop_event.wait(timeout=self.poll_interval)

    @staticmethod
    def _print_snapshot(snapshot: dict) -> None:
        ts = snapshot["timestamp"]
        balance = snapshot["balance"]
        open_orders = snapshot["open_orders"]
        print(f"\n[monitor] {ts}")
        if isinstance(balance, dict) and "cash" in balance:
            print(f"  Cash: ${balance['cash']:,.2f}  Portfolio: ${balance.get('portfolio_value', 0):,.2f}")
            for pos in balance.get("positions", []):
                print(f"    {pos['symbol']:10s}  qty={pos['qty']:>10.4f}  value=${pos.get('market_value', 0):>12,.2f}")
        else:
            for asset, qty in balance.items():
                if qty:
                    print(f"  {asset}: {qty}")
        if open_orders:
            print(f"  Open orders: {len(open_orders)}")
            for o in open_orders[:5]:
                print(f"    {o['id'][:12]}  {o['symbol']} {o['side']} qty={o['qty']}")
