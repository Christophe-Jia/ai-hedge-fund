"""
LlmCryptoStrategy – bridges LLM portfolio_manager decisions to BaseStrategy.

Translates the portfolio_management_agent output format:
    {"BTC/USDT": {"action": "buy|sell|hold", "quantity": 0.01, "confidence": 75}}

into set_target() calls, then fires execute_trading() to generate limit orders
through the OmsEngine / gateway layer.

Usage:
    from src.core.event import EventEngine
    from src.core.oms import OmsEngine
    from src.gateways.paper_gateway import PaperGateway
    from src.strategies.llm_crypto_strategy import LlmCryptoStrategy

    ee = EventEngine()
    ee.start()
    oms = OmsEngine(ee)
    gw = PaperGateway(event_engine=ee, initial_cash=10_000.0)

    strategy = LlmCryptoStrategy(
        engine=oms,
        gateway=gw,
        name="llm_crypto",
        symbols=["BTC/USDT", "ETH/USDT"],
        setting={"price_add": 0.001},
    )
    strategy.on_init()
    strategy.on_signal(pm_decisions)  # pm_decisions from portfolio_management_agent
"""

from __future__ import annotations

from src.core.objects import BarData
from src.core.strategy import BaseStrategy


class LlmCryptoStrategy(BaseStrategy):
    """
    Strategy adapter that converts LLM portfolio_manager decisions into
    target-position changes executed through the core OMS/gateway stack.

    Class-level parameters (injectable via ``setting``):
        price_add (float): Fractional slippage applied to limit order prices.
                           E.g. 0.001 = 0.1% above close for buys (default).
    """

    price_add: float = 0.001  # 0.1% slippage on limit orders

    # ------------------------------------------------------------------
    # BaseStrategy interface
    # ------------------------------------------------------------------

    def on_init(self) -> None:
        """No warm-up required for an LLM-driven strategy."""

    def on_bar(self, bars: dict[str, BarData]) -> None:
        """No bar-by-bar logic; signals arrive via on_signal()."""

    def on_signal(self, signal: dict) -> None:
        """
        Process portfolio_manager decisions and execute trades.

        Args:
            signal: Dict mapping symbol → decision, e.g.::

                {
                    "BTC/USDT": {"action": "buy",  "quantity": 0.01, "confidence": 75},
                    "ETH/USDT": {"action": "hold", "quantity": 0.0,  "confidence": 40},
                    "SOL/USDT": {"action": "sell", "quantity": 0.5,  "confidence": 60},
                }

        Actions:
            buy    – add ``quantity`` to current long position target
            sell   – reduce long position target by ``quantity`` (floor 0)
            short  – add ``quantity`` to short position target (negative)
            cover  – reduce short position (increase target toward 0)
            hold   – no change
        """
        if not signal:
            return

        for symbol, decision in signal.items():
            action = decision.get("action", "hold").lower()
            qty = float(decision.get("quantity", 0))
            current_pos = self.get_pos(symbol)

            if action == "buy":
                self.set_target(symbol, current_pos + qty)
            elif action in ("sell",):
                self.set_target(symbol, max(0.0, current_pos - qty))
            elif action == "short":
                self.set_target(symbol, current_pos - qty)
            elif action == "cover":
                self.set_target(symbol, current_pos + qty)
            # "hold" → no change to target

        # Build bar map for symbols mentioned in the signal
        bars: dict[str, BarData] = {}
        for sym in signal:
            bar = self.engine.get_bar(sym)
            if bar is not None:
                bars[sym] = bar

        if bars:
            self.execute_trading(bars=bars, price_add=self.price_add)
