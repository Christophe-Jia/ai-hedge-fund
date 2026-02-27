"""
Live Trading Scheduler

Runs the multi-agent LangGraph workflow on a configurable schedule
and routes decisions to the TradeExecutor.

Usage:
    from src.live.scheduler import LiveTradingScheduler

    scheduler = LiveTradingScheduler(
        market="crypto",
        tickers=["BTC/USDT"],
        interval_minutes=60,
        paper=True,
    )
    scheduler.start()   # Blocks until Ctrl-C

Env vars:
  See TradeExecutor for exchange credentials.
  LIVE_TRADING_PAPER=true  – override to force paper mode
"""

import json
import signal
import threading
import time
from datetime import datetime, timezone
from typing import Callable, Literal, Optional

import schedule

from src.trading.executor import TradeExecutor
from src.trading.orders import Order, OrderStatus


MarketType = Literal["crypto", "alpaca"]


class LiveTradingScheduler:
    """
    Orchestrates periodic execution of a trading workflow.

    Args:
        market:            "crypto" or "alpaca"
        tickers:           List of symbols to trade
        interval_minutes:  How often to run the workflow (minutes)
        paper:             Paper/sandbox mode (default True)
        workflow_fn:       Callable that accepts (tickers, start_date, end_date) and
                           returns a dict of {symbol: {action, quantity, confidence}}.
                           If None, uses the default crypto LangGraph workflow.
        exchange_id:       CCXT exchange name (ignored for Alpaca)
        on_order:          Optional callback invoked after each order is placed.
        use_new_stack:     When True, routes decisions through LlmCryptoStrategy +
                           OmsEngine instead of the legacy TradeExecutor.place_order()
                           path.  Defaults to False (legacy path) for safety.
    """

    def __init__(
        self,
        market: MarketType = "crypto",
        tickers: list[str] = None,
        interval_minutes: int = 60,
        paper: bool = True,
        workflow_fn: Optional[Callable] = None,
        exchange_id: str = None,
        on_order: Optional[Callable[[Order], None]] = None,
        use_new_stack: bool = False,
    ):
        self.market = market
        self.tickers = tickers or ["BTC/USDT"]
        self.interval_minutes = interval_minutes
        self.paper = paper
        self.on_order = on_order
        self.use_new_stack = use_new_stack
        self._stop_event = threading.Event()

        self.executor = TradeExecutor(market=market, exchange_id=exchange_id, paper=paper)

        if workflow_fn is not None:
            self._workflow_fn = workflow_fn
        else:
            self._workflow_fn = self._default_crypto_workflow

        self._order_history: list[dict] = []

        # New-stack components (initialised lazily when use_new_stack=True)
        self._event_engine = None
        self._oms = None
        self._gateway = None
        self._strategy = None

        if use_new_stack:
            self._init_new_stack(exchange_id=exchange_id)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the scheduler. Blocks until stop() is called or Ctrl-C."""
        stack_label = "new (OMS+Strategy)" if self.use_new_stack else "legacy (TradeExecutor)"
        print(f"[scheduler] Starting live trading | market={self.market} | tickers={self.tickers} | interval={self.interval_minutes}m | paper={self.paper} | stack={stack_label}")

        # Run once immediately
        self._run_cycle()

        # Schedule recurring execution
        schedule.every(self.interval_minutes).minutes.do(self._run_cycle)

        # Handle Ctrl-C gracefully
        signal.signal(signal.SIGINT, self._handle_sigint)
        signal.signal(signal.SIGTERM, self._handle_sigint)

        while not self._stop_event.is_set():
            schedule.run_pending()
            time.sleep(1)

        print("[scheduler] Stopped.")

    def stop(self) -> None:
        """Signal the scheduler to stop after the current cycle."""
        self._stop_event.set()
        if self._event_engine is not None:
            try:
                self._event_engine.stop()
            except Exception:
                pass

    def get_order_history(self) -> list[dict]:
        """Return a copy of all placed orders."""
        return list(self._order_history)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _run_cycle(self) -> None:
        now = datetime.now(tz=timezone.utc)
        print(f"\n[scheduler] Cycle start: {now.isoformat()}")

        # Use the last 90 days as the lookback window for analysis
        from datetime import timedelta
        end_date = now.strftime("%Y-%m-%d")
        start_date = (now - timedelta(days=90)).strftime("%Y-%m-%d")

        try:
            decisions = self._workflow_fn(self.tickers, start_date, end_date)
        except Exception as e:
            print(f"[scheduler] Workflow error: {e}")
            return

        if self.use_new_stack:
            self._execute_via_strategy(decisions, now)
        else:
            self._execute_via_executor(decisions, now)

    def _execute_via_executor(self, decisions: dict, now: datetime) -> None:
        """Legacy path: route decisions through TradeExecutor.place_order()."""
        for symbol, decision in decisions.items():
            action = decision.get("action", "hold").lower()
            quantity = float(decision.get("quantity", 0))
            confidence = int(decision.get("confidence", 0))

            print(f"[scheduler] {symbol}: action={action} qty={quantity} conf={confidence}%")

            if action in ("hold",) or quantity <= 0:
                continue

            side = "buy" if action in ("buy", "cover") else "sell"
            order = self.executor.place_order(
                symbol=symbol,
                side=side,
                quantity=quantity,
                order_type="market",
            )

            order_dict = {**order.to_dict(), "timestamp": now.isoformat(), "confidence": confidence}
            self._order_history.append(order_dict)

            status_str = "OK" if not order.is_failed else f"FAILED: {order.error}"
            print(f"[scheduler]   Order {order.id} → {status_str}")

            if self.on_order:
                try:
                    self.on_order(order)
                except Exception as e:
                    print(f"[scheduler] on_order callback error: {e}")

    def _execute_via_strategy(self, decisions: dict, now: datetime) -> None:
        """New-stack path: route decisions through LlmCryptoStrategy + OmsEngine."""
        if self._strategy is None:
            print("[scheduler] New stack not initialised; falling back to executor path.")
            self._execute_via_executor(decisions, now)
            return

        for symbol, decision in decisions.items():
            action = decision.get("action", "hold").lower()
            quantity = float(decision.get("quantity", 0))
            confidence = int(decision.get("confidence", 0))
            print(f"[scheduler] {symbol}: action={action} qty={quantity} conf={confidence}% [new-stack]")

        self._strategy.on_signal(decisions)

        # Snapshot OMS positions for the order history log
        from src.core.constant import Direction
        for symbol in decisions:
            pos = self._oms.get_position_by_symbol(symbol, Direction.LONG)
            if pos:
                self._order_history.append({
                    "symbol": symbol,
                    "timestamp": now.isoformat(),
                    "volume": pos.volume,
                    "source": "new-stack",
                })

    def _default_crypto_workflow(self, tickers: list[str], start_date: str, end_date: str) -> dict:
        """
        Default workflow: runs the crypto LangGraph multi-agent graph.
        Returns {symbol: {"action": str, "quantity": float, "confidence": int}}.
        """
        from src.graph.state import AgentState
        from src.agents.crypto import crypto_technical_agent, crypto_sentiment_agent, crypto_risk_agent
        from src.agents.portfolio_manager import portfolio_management_agent

        # Build initial state
        state: AgentState = {
            "messages": [],
            "data": {
                "tickers": tickers,
                "start_date": start_date,
                "end_date": end_date,
                "analyst_signals": {},
                "portfolio": self._get_current_portfolio(),
            },
            "metadata": {
                "show_reasoning": False,
                "model_name": "gpt-4o",
                "model_provider": "openai",
            },
        }

        # Sequential agent pipeline
        state = crypto_technical_agent(state)
        state = crypto_sentiment_agent(state)
        state = crypto_risk_agent(state)
        state = portfolio_management_agent(state, agent_id="portfolio_manager")

        # portfolio_management_agent writes decisions as JSON into the last message
        last_msg = state["messages"][-1] if state["messages"] else None
        try:
            pm_signals = json.loads(last_msg.content)
            # Expected format: {"BTC/USDT": {"action": "buy", "quantity": 0.01, "confidence": 75}}
            return pm_signals
        except (json.JSONDecodeError, AttributeError):
            return {}

    def _get_current_portfolio(self) -> dict:
        """Fetch live portfolio state from the exchange."""
        try:
            balance = self.executor.get_balance()
            if self.market == "crypto":
                cash = balance.get("USDT", 0.0)
                positions = {
                    k: {"cash": v * 0.0, "shares": v, "ticker": k}
                    for k, v in balance.items()
                    if k != "USDT"
                }
                return {"total_cash": cash, "positions": positions}
            else:
                cash = balance.get("cash", 0.0)
                positions = {
                    p["symbol"]: {"cash": p["market_value"], "shares": p["qty"], "ticker": p["symbol"]}
                    for p in balance.get("positions", [])
                }
                return {"total_cash": cash, "positions": positions}
        except Exception:
            return {"total_cash": 10000.0, "positions": {}}

    def _init_new_stack(self, exchange_id: str = None) -> None:
        """
        Initialise the OmsEngine + gateway + LlmCryptoStrategy stack.

        In paper mode a PaperGateway is used (no real orders).
        In live mode a CcxtGateway is connected to the configured exchange.
        """
        from src.core.event import EventEngine
        from src.core.oms import OmsEngine
        from src.strategies.llm_crypto_strategy import LlmCryptoStrategy

        self._event_engine = EventEngine()
        self._event_engine.start()
        self._oms = OmsEngine(self._event_engine)

        if self.paper:
            from src.gateways.paper_gateway import PaperGateway
            self._gateway = PaperGateway(
                event_engine=self._event_engine,
                initial_cash=10_000.0,
            )
            print("[scheduler] New stack: PaperGateway initialised")
        else:
            from src.gateways.ccxt_gateway import CcxtGateway
            self._gateway = CcxtGateway(event_engine=self._event_engine)
            self._gateway.connect({
                "exchange_id": exchange_id or "binance",
                "sandbox": False,
            })
            print(f"[scheduler] New stack: CcxtGateway connected ({exchange_id or 'binance'})")

        self._strategy = LlmCryptoStrategy(
            engine=self._oms,
            gateway=self._gateway,
            name="llm_crypto",
            symbols=self.tickers,
            setting={"price_add": 0.001},
        )
        self._strategy.on_init()
        print(f"[scheduler] New stack ready | symbols={self.tickers}")

    def _handle_sigint(self, signum, frame) -> None:
        print("\n[scheduler] Interrupt received; stopping...")
        self.stop()
