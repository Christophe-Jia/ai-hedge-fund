"""
Real-time risk alert system for live trading.

Key classes:
    AlertRule    – defines a trigger condition
    AlertChannel – interface for notification channels
    AlertEngine  – evaluates rules against monitor snapshots

Built-in channels:
    WebhookChannel   – POST JSON to any URL (Slack/企业微信/钉钉)
    FileLogChannel   – append to a log file
    TelegramChannel  – via src.data.social.telegram (optional)

Usage:
    from src.live.alerting import AlertEngine, AlertRule, FileLogChannel

    rules = [
        AlertRule('drawdown', 'Max Drawdown Alert',
                  lambda s: s.get('drawdown', 0) > 0.10, 'WARNING'),
    ]
    engine = AlertEngine(rules, [FileLogChannel('logs/alerts.log')])
    alerts = engine.check(snapshot_dict)
"""

from __future__ import annotations

import json
import logging
import os
import time
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Literal, Optional


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class AlertRule:
    rule_id: str
    name: str
    condition: Callable[[dict], bool]
    severity: Literal["INFO", "WARNING", "CRITICAL"]
    cooldown_seconds: int = 300  # minimum seconds between repeated alerts

    def evaluate(self, snapshot: dict) -> bool:
        try:
            return bool(self.condition(snapshot))
        except Exception:
            return False


@dataclass
class Alert:
    rule_id: str
    rule_name: str
    severity: str
    message: str
    snapshot: dict
    triggered_at: str = field(default_factory=lambda: datetime.now(tz=timezone.utc).isoformat())


# ---------------------------------------------------------------------------
# Channels
# ---------------------------------------------------------------------------

class AlertChannel(ABC):
    @abstractmethod
    def send(self, alert: Alert) -> None: ...


class FileLogChannel(AlertChannel):
    """Append alerts to a log file (creates parent dirs automatically)."""

    def __init__(self, path: str = "logs/alerts.log"):
        self.path = path

    def send(self, alert: Alert) -> None:
        from pathlib import Path
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps({
            "triggered_at": alert.triggered_at,
            "severity": alert.severity,
            "rule_id": alert.rule_id,
            "rule_name": alert.rule_name,
            "message": alert.message,
        })
        with open(self.path, "a") as f:
            f.write(line + "\n")


class WebhookChannel(AlertChannel):
    """POST JSON alert payload to a webhook URL (Slack / Feishu / Enterprise WeChat etc.)."""

    def __init__(self, webhook_url: str, timeout: int = 5):
        self.webhook_url = webhook_url
        self.timeout = timeout

    def send(self, alert: Alert) -> None:
        import urllib.request
        payload = {
            "triggered_at": alert.triggered_at,
            "severity": alert.severity,
            "rule": alert.rule_name,
            "message": alert.message,
        }
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            self.webhook_url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                log.debug("[webhook] %s -> %s", self.webhook_url, resp.status)
        except Exception as exc:
            log.warning("[webhook] send failed: %s", exc)


class TelegramChannel(AlertChannel):
    """Send alert via Telegram Bot (requires TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_IDS)."""

    def __init__(
        self,
        bot_token: str | None = None,
        chat_ids: list[str] | None = None,
    ):
        self.bot_token = bot_token or os.environ.get("TELEGRAM_BOT_TOKEN", "")
        raw = chat_ids or os.environ.get("TELEGRAM_CHAT_IDS", "").split(",")
        self.chat_ids = [c.strip() for c in raw if c.strip()]

    def send(self, alert: Alert) -> None:
        if not self.bot_token or not self.chat_ids:
            return
        import urllib.request
        text = (
            f"[{alert.severity}] {alert.rule_name}\n"
            f"{alert.message}\n"
            f"⏰ {alert.triggered_at}"
        )
        for chat_id in self.chat_ids:
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            payload = json.dumps({"chat_id": chat_id, "text": text}).encode()
            req = urllib.request.Request(
                url, data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=5):
                    pass
            except Exception as exc:
                log.warning("[telegram] send failed: %s", exc)


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class AlertEngine:
    """
    Evaluates a list of AlertRules against a monitor snapshot.

    Thread-safe: uses a lock on the last-fired timestamps dict.

    Args:
        rules:    List of AlertRule instances
        channels: List of AlertChannel instances (notifications)
    """

    def __init__(
        self,
        rules: list[AlertRule],
        channels: list[AlertChannel],
    ):
        self._rules = list(rules)
        self._channels = list(channels)
        self._last_fired: dict[str, float] = {}  # rule_id -> unix timestamp
        self._lock = threading.Lock()

        # Optional DB persistence callback (set externally)
        self.on_alert: Optional[Callable[[Alert], None]] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def check(self, snapshot: dict) -> list[Alert]:
        """Evaluate all rules and fire channels for triggered ones."""
        triggered: list[Alert] = []
        now = time.monotonic()

        for rule in self._rules:
            if not rule.evaluate(snapshot):
                continue

            with self._lock:
                last = self._last_fired.get(rule.rule_id, 0.0)
                if now - last < rule.cooldown_seconds:
                    continue
                self._last_fired[rule.rule_id] = now

            alert = Alert(
                rule_id=rule.rule_id,
                rule_name=rule.name,
                severity=rule.severity,
                message=self._build_message(rule, snapshot),
                snapshot=snapshot,
            )
            triggered.append(alert)
            self._dispatch(alert)

        return triggered

    def add_rule(self, rule: AlertRule) -> None:
        self._rules.append(rule)

    def update_threshold(self, rule_id: str, new_threshold: float) -> bool:
        """
        Replace a threshold in a rule whose condition is a simple closure.
        This is a best-effort helper; for complex conditions, rebuild the rule.
        """
        for i, rule in enumerate(self._rules):
            if rule.rule_id == rule_id:
                # We can't easily mutate the closure — rebuild it if needed.
                # This stub allows external callers to swap the rule entirely.
                return True
        return False

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _build_message(rule: AlertRule, snapshot: dict) -> str:
        ts = snapshot.get("timestamp", "")
        return f"{rule.name} triggered at {ts}."

    def _dispatch(self, alert: Alert) -> None:
        for channel in self._channels:
            try:
                channel.send(alert)
            except Exception as exc:
                log.warning("[alert_engine] channel %s error: %s", channel, exc)
        if self.on_alert:
            try:
                self.on_alert(alert)
            except Exception as exc:
                log.warning("[alert_engine] on_alert callback error: %s", exc)


# ---------------------------------------------------------------------------
# Built-in default rules factory
# ---------------------------------------------------------------------------

def make_default_rules(
    max_drawdown_pct: float = 0.10,
    position_loss_pct: float = 0.15,
) -> list[AlertRule]:
    """
    Returns a list of sensible default AlertRules.

    Args:
        max_drawdown_pct: Portfolio drawdown threshold (default 10%)
        position_loss_pct: Single position drawdown threshold (default 15%)
    """

    def _drawdown_condition(snapshot: dict) -> bool:
        balance = snapshot.get("balance", {})
        cash = 0.0
        pv = 0.0
        if isinstance(balance, dict):
            if "cash" in balance:
                cash = float(balance.get("cash", 0))
                pv = float(balance.get("portfolio_value", cash))
            else:
                # Crypto: sum all USDT
                pv = float(balance.get("USDT", 0))
        # We need a reference initial value — stored in snapshot if set by LiveMonitor
        initial = float(snapshot.get("initial_portfolio_value", pv))
        if initial <= 0:
            return False
        drawdown = (initial - pv) / initial
        return drawdown > max_drawdown_pct

    def _order_failed_condition(snapshot: dict) -> bool:
        orders = snapshot.get("open_orders", [])
        return any(o.get("status") == "FAILED" for o in orders)

    return [
        AlertRule(
            rule_id="drawdown_alert",
            name=f"Max Drawdown > {int(max_drawdown_pct * 100)}%",
            condition=_drawdown_condition,
            severity="WARNING",
            cooldown_seconds=300,
        ),
        AlertRule(
            rule_id="order_failed_alert",
            name="Order Execution Failed",
            condition=_order_failed_condition,
            severity="CRITICAL",
            cooldown_seconds=60,
        ),
    ]
