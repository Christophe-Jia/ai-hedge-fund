"""
Unified trade execution engine.

Dispatches orders to:
  - CCXT  → crypto exchanges (Binance, OKX, etc.)
  - Alpaca → US equities (paper or live)

Usage:
    executor = TradeExecutor(market="crypto")   # or "alpaca"
    order = executor.place_order(
        symbol="BTC/USDT",
        side="buy",
        quantity=0.01,
        order_type="market",
    )

Env vars required:
  Crypto: CCXT_EXCHANGE, CCXT_API_KEY, CCXT_API_SECRET, CCXT_SANDBOX
  Alpaca: ALPACA_API_KEY, ALPACA_API_SECRET, ALPACA_BASE_URL
"""

import os
from typing import Literal

from src.trading.orders import Order, OrderStatus


MarketType = Literal["crypto", "alpaca"]


class TradeExecutor:
    """
    Unified wrapper that routes trade orders to the correct exchange client.

    Args:
        market:      "crypto" (CCXT) or "alpaca" (Alpaca py)
        exchange_id: CCXT exchange name, e.g. "binance". Ignored for Alpaca.
        paper:       Force sandbox/paper trading mode (default: True for safety)
    """

    def __init__(self, market: MarketType = "crypto", exchange_id: str = None, paper: bool = True):
        self.market = market
        self.paper = paper
        self._client = None

        if market == "crypto":
            self._client = _build_ccxt_exchange(exchange_id, paper)
        elif market == "alpaca":
            self._client = _build_alpaca_client(paper)
        else:
            raise ValueError(f"Unknown market type: {market!r}")

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def place_order(
        self,
        symbol: str,
        side: Literal["buy", "sell"],
        quantity: float,
        order_type: Literal["market", "limit"] = "market",
        price: float = None,
    ) -> Order:
        """
        Place a trade order.

        Args:
            symbol:     CCXT symbol (e.g. "BTC/USDT") or Alpaca symbol (e.g. "AAPL")
            side:       "buy" or "sell"
            quantity:   Amount to trade (base asset for crypto, shares for stocks)
            order_type: "market" or "limit"
            price:      Required for limit orders

        Returns:
            Order dataclass with status and exchange order ID.
        """
        if self.market == "crypto":
            return self._place_ccxt_order(symbol, side, quantity, order_type, price)
        return self._place_alpaca_order(symbol, side, quantity, order_type, price)

    def cancel_order(self, order_id: str, symbol: str = None) -> bool:
        """Cancel an open order. Returns True on success."""
        if self.market == "crypto":
            try:
                self._client.cancel_order(order_id, symbol)
                return True
            except Exception as e:
                print(f"[executor] Cancel failed: {e}")
                return False
        # Alpaca
        try:
            self._client.cancel_order_by_id(order_id)
            return True
        except Exception as e:
            print(f"[executor] Alpaca cancel failed: {e}")
            return False

    def get_balance(self) -> dict:
        """
        Return current balances.

        For crypto: {"BTC": {"free": ..., "total": ...}, ...}
        For Alpaca: {"cash": ..., "portfolio_value": ..., "positions": [...]}
        """
        if self.market == "crypto":
            balance = self._client.fetch_balance()
            return {k: v for k, v in balance["total"].items() if v and v > 0}
        # Alpaca
        account = self._client.get_account()
        positions = self._client.get_all_positions()
        return {
            "cash": float(account.cash),
            "portfolio_value": float(account.portfolio_value),
            "positions": [
                {"symbol": p.symbol, "qty": float(p.qty), "market_value": float(p.market_value)}
                for p in positions
            ],
        }

    def get_open_orders(self, symbol: str = None) -> list[dict]:
        """Return a list of open orders."""
        if self.market == "crypto":
            raw = self._client.fetch_open_orders(symbol)
            return [_normalise_ccxt_order(o) for o in raw]
        raw = self._client.get_orders(status="open")
        return [_normalise_alpaca_order(o) for o in raw]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _place_ccxt_order(self, symbol, side, quantity, order_type, price) -> Order:
        try:
            if order_type == "market":
                raw = self._client.create_market_order(symbol, side, quantity)
            else:
                if price is None:
                    raise ValueError("price is required for limit orders")
                raw = self._client.create_limit_order(symbol, side, quantity, price)
            return Order(
                id=str(raw["id"]),
                symbol=symbol,
                side=side,
                quantity=quantity,
                order_type=order_type,
                price=raw.get("price") or price,
                status=OrderStatus.OPEN,
                raw=raw,
            )
        except Exception as e:
            return Order(
                id="",
                symbol=symbol,
                side=side,
                quantity=quantity,
                order_type=order_type,
                price=price,
                status=OrderStatus.FAILED,
                error=str(e),
            )

    def _place_alpaca_order(self, symbol, side, quantity, order_type, price) -> Order:
        try:
            from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest
            from alpaca.trading.enums import OrderSide, TimeInForce

            alpaca_side = OrderSide.BUY if side == "buy" else OrderSide.SELL
            if order_type == "market":
                req = MarketOrderRequest(symbol=symbol, qty=quantity, side=alpaca_side, time_in_force=TimeInForce.DAY)
            else:
                req = LimitOrderRequest(symbol=symbol, qty=quantity, side=alpaca_side, time_in_force=TimeInForce.DAY, limit_price=price)

            raw = self._client.submit_order(req)
            return Order(
                id=str(raw.id),
                symbol=symbol,
                side=side,
                quantity=quantity,
                order_type=order_type,
                price=float(raw.limit_price) if raw.limit_price else None,
                status=OrderStatus.OPEN,
                raw=raw.__dict__ if hasattr(raw, "__dict__") else {},
            )
        except Exception as e:
            return Order(
                id="",
                symbol=symbol,
                side=side,
                quantity=quantity,
                order_type=order_type,
                price=price,
                status=OrderStatus.FAILED,
                error=str(e),
            )


# ------------------------------------------------------------------
# Factory helpers
# ------------------------------------------------------------------

def _build_ccxt_exchange(exchange_id: str = None, paper: bool = True):
    try:
        import ccxt
    except ImportError:
        raise ImportError("ccxt is required. Run: poetry add ccxt")

    exchange_id = exchange_id or os.environ.get("CCXT_EXCHANGE", "binance")
    sandbox = paper or os.environ.get("CCXT_SANDBOX", "true").lower() == "true"

    cls = getattr(ccxt, exchange_id)
    exchange = cls(
        {
            "apiKey": os.environ.get("CCXT_API_KEY", ""),
            "secret": os.environ.get("CCXT_API_SECRET", ""),
            "enableRateLimit": True,
        }
    )
    if sandbox and exchange.has.get("test"):
        exchange.set_sandbox_mode(True)
    return exchange


def _build_alpaca_client(paper: bool = True):
    try:
        from alpaca.trading.client import TradingClient
    except ImportError:
        raise ImportError("alpaca-py is required. Run: poetry add alpaca-py")

    base_url = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets" if paper else "https://api.alpaca.markets")
    return TradingClient(
        api_key=os.environ.get("ALPACA_API_KEY", ""),
        secret_key=os.environ.get("ALPACA_API_SECRET", ""),
        paper=paper,
    )


def _normalise_ccxt_order(o: dict) -> dict:
    return {"id": o.get("id"), "symbol": o.get("symbol"), "side": o.get("side"), "qty": o.get("amount"), "price": o.get("price"), "status": o.get("status")}


def _normalise_alpaca_order(o) -> dict:
    return {"id": str(o.id), "symbol": o.symbol, "side": str(o.side), "qty": float(o.qty or 0), "price": float(o.limit_price or 0), "status": str(o.status)}
