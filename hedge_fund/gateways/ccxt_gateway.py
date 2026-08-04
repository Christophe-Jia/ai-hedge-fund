"""
CCXT gateway – wraps a ccxt exchange instance into the BaseGateway interface.

Supports any exchange available in ccxt (Binance, OKX, Bybit, etc.).

Required env vars (or pass in setting dict):
    CCXT_EXCHANGE   – exchange id, e.g. "binance"
    CCXT_API_KEY
    CCXT_API_SECRET
    CCXT_SANDBOX    – "true" / "false"

Install: poetry add ccxt
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

from src.core.constant import Direction, OrderType, Status
from src.core.event import EventEngine
from src.core.gateway import BaseGateway
from src.core.objects import (
    AccountData,
    BarData,
    CancelRequest,
    OrderData,
    OrderRequest,
    PositionData,
)


class CcxtGateway(BaseGateway):
    """
    Generic CCXT gateway for cryptocurrency exchanges.

    Works with any CCXT-compatible exchange. Spot trading only.
    """

    default_name: str = "CCXT"
    default_setting: dict = {
        "exchange_id": "binance",
        "api_key": "",
        "api_secret": "",
        "sandbox": True,
    }

    def __init__(self, event_engine: EventEngine) -> None:
        super().__init__(event_engine, self.default_name)
        self._exchange = None
        self._exchange_id: str = "binance"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def connect(self, setting: dict) -> None:
        """Connect to a CCXT exchange."""
        try:
            import ccxt
        except ImportError:
            raise ImportError("ccxt is required: poetry add ccxt")

        exchange_id = (
            setting.get("exchange_id")
            or os.environ.get("CCXT_EXCHANGE", "binance")
        )
        api_key = setting.get("api_key") or os.environ.get("CCXT_API_KEY", "")
        api_secret = setting.get("api_secret") or os.environ.get("CCXT_API_SECRET", "")
        sandbox = setting.get("sandbox", True)
        if isinstance(sandbox, str):
            sandbox = sandbox.lower() == "true"
        env_sandbox = os.environ.get("CCXT_SANDBOX", "true").lower() == "true"
        sandbox = sandbox or env_sandbox

        self._exchange_id = exchange_id
        self.gateway_name = f"CCXT.{exchange_id.upper()}"

        cls = getattr(ccxt, exchange_id)
        self._exchange = cls(
            {
                "apiKey": api_key,
                "secret": api_secret,
                "enableRateLimit": True,
            }
        )
        if sandbox and self._exchange.has.get("test"):
            self._exchange.set_sandbox_mode(True)

        self.query_account()

    def subscribe(self, symbol: str) -> None:
        """Real-time subscription requires WebSocket – not implemented here."""
        pass

    def close(self) -> None:
        self._exchange = None

    # ------------------------------------------------------------------
    # Order management
    # ------------------------------------------------------------------
    def send_order(self, req: OrderRequest) -> str:
        """Submit a market or limit order to the CCXT exchange."""
        if self._exchange is None:
            return ""

        side = "buy" if req.direction == Direction.LONG else "sell"

        try:
            if req.order_type == OrderType.MARKET:
                raw = self._exchange.create_market_order(
                    req.symbol, side, req.volume
                )
            else:
                if req.price <= 0:
                    raise ValueError("price must be > 0 for limit orders")
                raw = self._exchange.create_limit_order(
                    req.symbol, side, req.volume, req.price
                )

            orderid = str(raw["id"])
            order = req.create_order_data(orderid)
            self.on_order(order)
            return order.vt_orderid

        except Exception:
            fallback_id = str(uuid.uuid4())[:8]
            order = req.create_order_data(fallback_id)
            order.status = Status.REJECTED
            self.on_order(order)
            return ""

    def cancel_order(self, req: CancelRequest) -> None:
        if self._exchange is None:
            return
        try:
            self._exchange.cancel_order(req.orderid, req.symbol)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Account / position queries
    # ------------------------------------------------------------------
    def query_account(self) -> None:
        if self._exchange is None:
            return
        try:
            balance = self._exchange.fetch_balance()
            total = balance.get("total", {})
            usdt = float(total.get("USDT", 0.0))
            # Approximate: treat non-USDT as frozen (in use)
            non_usdt = sum(v for k, v in total.items() if k != "USDT" and v)
            account = AccountData(
                accountid=self._exchange_id,
                balance=usdt,
                frozen=0.0,
            )
            self.on_account(account)

            # Emit non-USDT holdings as positions
            for asset, amount in total.items():
                if asset == "USDT" or not amount:
                    continue
                pos = PositionData(
                    symbol=f"{asset}/USDT",
                    direction=Direction.LONG,
                    volume=float(amount),
                )
                self.on_position(pos)
        except Exception:
            pass

    def query_position(self) -> None:
        """For spot trading, positions are derived from balance."""
        self.query_account()

    # ------------------------------------------------------------------
    # Historical data
    # ------------------------------------------------------------------
    def query_history(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        interval: str = "1d",
    ) -> list[BarData]:
        """Fetch OHLCV bars from CCXT."""
        if self._exchange is None:
            return []
        try:
            since_ms = int(start.replace(tzinfo=timezone.utc).timestamp() * 1000)
            raw = self._exchange.fetch_ohlcv(symbol, timeframe=interval, since=since_ms)
            bars = []
            for o in raw:
                ts = datetime.fromtimestamp(o[0] / 1000, tz=timezone.utc)
                if ts > end.replace(tzinfo=timezone.utc):
                    break
                bars.append(
                    BarData(
                        symbol=symbol,
                        datetime=ts,
                        open=float(o[1]),
                        high=float(o[2]),
                        low=float(o[3]),
                        close=float(o[4]),
                        volume=float(o[5]),
                    )
                )
            return bars
        except Exception:
            return []
