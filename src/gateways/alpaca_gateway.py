"""
Alpaca gateway – wraps alpaca-py TradingClient into the BaseGateway interface.

Required env vars:
    ALPACA_API_KEY
    ALPACA_API_SECRET
    ALPACA_BASE_URL  (optional, defaults to paper endpoint)

Install: poetry add alpaca-py
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime

from src.core.constant import Action, Direction, OrderType, Status
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


class AlpacaGateway(BaseGateway):
    """
    Gateway for US equities via Alpaca.

    Supports market and limit orders, account/position queries,
    and basic historical bar fetching.
    """

    default_name: str = "ALPACA"
    default_setting: dict = {
        "api_key": "",
        "api_secret": "",
        "paper": True,
    }

    def __init__(self, event_engine: EventEngine) -> None:
        super().__init__(event_engine, self.default_name)
        self._client = None
        self._data_client = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def connect(self, setting: dict) -> None:
        """Connect to Alpaca. setting keys: api_key, api_secret, paper."""
        try:
            from alpaca.trading.client import TradingClient
        except ImportError:
            raise ImportError("alpaca-py is required: poetry add alpaca-py")

        api_key = setting.get("api_key") or os.environ.get("ALPACA_API_KEY", "")
        api_secret = setting.get("api_secret") or os.environ.get("ALPACA_API_SECRET", "")
        paper = setting.get("paper", True)

        self._client = TradingClient(
            api_key=api_key,
            secret_key=api_secret,
            paper=paper,
        )

        # Verify connection by querying account
        self.query_account()

    def subscribe(self, symbol: str) -> None:
        """Alpaca push subscriptions require a WebSocket connection (not implemented here)."""
        pass

    def close(self) -> None:
        self._client = None

    # ------------------------------------------------------------------
    # Order management
    # ------------------------------------------------------------------
    def send_order(self, req: OrderRequest) -> str:
        """Submit a market or limit order. Returns vt_orderid."""
        if self._client is None:
            return ""

        try:
            from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest
            from alpaca.trading.enums import OrderSide, TimeInForce

            alpaca_side = OrderSide.BUY if req.direction == Direction.LONG else OrderSide.SELL

            if req.order_type == OrderType.MARKET:
                alpaca_req = MarketOrderRequest(
                    symbol=req.symbol,
                    qty=req.volume,
                    side=alpaca_side,
                    time_in_force=TimeInForce.DAY,
                )
            else:
                alpaca_req = LimitOrderRequest(
                    symbol=req.symbol,
                    qty=req.volume,
                    side=alpaca_side,
                    time_in_force=TimeInForce.DAY,
                    limit_price=req.price,
                )

            raw = self._client.submit_order(alpaca_req)
            orderid = str(raw.id)

            order = req.create_order_data(orderid)
            self.on_order(order)
            return order.vt_orderid

        except Exception as e:
            # Emit a rejected order so OMS can track the failure
            fallback_id = str(uuid.uuid4())[:8]
            order = req.create_order_data(fallback_id)
            order.status = Status.REJECTED
            self.on_order(order)
            return ""

    def cancel_order(self, req: CancelRequest) -> None:
        if self._client is None:
            return
        try:
            self._client.cancel_order_by_id(req.orderid)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Account / position queries
    # ------------------------------------------------------------------
    def query_account(self) -> None:
        if self._client is None:
            return
        try:
            acct = self._client.get_account()
            account = AccountData(
                accountid="alpaca",
                balance=float(acct.portfolio_value),
                frozen=float(acct.portfolio_value) - float(acct.cash),
            )
            self.on_account(account)
        except Exception:
            pass

    def query_position(self) -> None:
        if self._client is None:
            return
        try:
            positions = self._client.get_all_positions()
            for p in positions:
                qty = float(p.qty)
                direction = Direction.LONG if qty >= 0 else Direction.SHORT
                pos = PositionData(
                    symbol=p.symbol,
                    direction=direction,
                    volume=abs(qty),
                    avg_price=float(p.avg_entry_price or 0),
                    pnl=float(p.unrealized_pl or 0),
                )
                self.on_position(pos)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Historical data
    # ------------------------------------------------------------------
    def query_history(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        interval: str = "1Day",
    ) -> list[BarData]:
        """Fetch historical bars via Alpaca StockHistoricalDataClient."""
        try:
            from alpaca.data.historical import StockHistoricalDataClient
            from alpaca.data.requests import StockBarsRequest
            from alpaca.data.timeframe import TimeFrame

            if self._data_client is None:
                self._data_client = StockHistoricalDataClient()

            tf_map = {
                "1m": TimeFrame.Minute,
                "1h": TimeFrame.Hour,
                "1d": TimeFrame.Day,
                "1Day": TimeFrame.Day,
            }
            tf = tf_map.get(interval, TimeFrame.Day)

            req = StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=tf,
                start=start,
                end=end,
            )
            bars_resp = self._data_client.get_stock_bars(req)
            bars = []
            for b in bars_resp[symbol]:
                bars.append(
                    BarData(
                        symbol=symbol,
                        datetime=b.timestamp,
                        open=float(b.open),
                        high=float(b.high),
                        low=float(b.low),
                        close=float(b.close),
                        volume=float(b.volume),
                    )
                )
            return bars
        except Exception:
            return []
