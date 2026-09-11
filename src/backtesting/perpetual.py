"""
Perpetual futures position and portfolio management for BTC backtesting.

Implements isolated margin model with Binance-style tiered maintenance
margin rates and funding rate application every 8 hours.

Liquidation price formulas (isolated margin):
  Long:  entry_price * (1 - 1/leverage + maintenance_margin_rate)
  Short: entry_price * (1 + 1/leverage - maintenance_margin_rate)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Literal

from .types import PerpPositionState, TradeRecord


# ---------------------------------------------------------------------------
# Binance tiered maintenance margin rates (USDT-M perpetuals)
# ---------------------------------------------------------------------------

def _maintenance_margin_rate(notional_usd: float) -> float:
    """Return the maintenance margin rate for a given notional size."""
    if notional_usd < 50_000:
        return 0.005   # 0.5%
    if notional_usd < 250_000:
        return 0.010   # 1.0%
    if notional_usd < 1_000_000:
        return 0.015   # 1.5%
    return 0.025       # 2.5%


def _calc_liquidation_price(
    entry_price: float,
    leverage: float,
    side: Literal["long", "short"],
    notional_usd: float,
) -> float:
    """
    Compute the liquidation price under the isolated margin model.

    Long:  liq_price = entry_price * (1 - 1/leverage + mmr)
    Short: liq_price = entry_price * (1 + 1/leverage - mmr)
    """
    mmr = _maintenance_margin_rate(notional_usd)
    if side == "long":
        return entry_price * (1.0 - 1.0 / leverage + mmr)
    else:
        return entry_price * (1.0 + 1.0 / leverage - mmr)


# ---------------------------------------------------------------------------
# PerpPosition — a single isolated-margin position
# ---------------------------------------------------------------------------

@dataclass
class PerpPosition:
    """A single perpetual futures position under isolated margin."""

    symbol: str
    side: Literal["long", "short"]
    size: float           # base currency (e.g. BTC)
    entry_price: float    # USD
    leverage: float

    # Computed at open time
    initial_margin: float = field(init=False)
    liquidation_price: float = field(init=False)
    unrealized_pnl: float = field(default=0.0)
    realized_pnl: float = field(default=0.0)
    cumulative_funding: float = field(default=0.0)  # negative = paid
    is_liquidated: bool = field(default=False)

    def __post_init__(self) -> None:
        notional = self.size * self.entry_price
        self.initial_margin = notional / self.leverage
        self.liquidation_price = _calc_liquidation_price(
            self.entry_price, self.leverage, self.side, notional
        )

    # ------------------------------------------------------------------
    # Mark-to-market
    # ------------------------------------------------------------------

    def update_unrealized_pnl(self, mark_price: float) -> None:
        """Recompute unrealized PnL at the given mark price."""
        if self.side == "long":
            self.unrealized_pnl = (mark_price - self.entry_price) * self.size
        else:
            self.unrealized_pnl = (self.entry_price - mark_price) * self.size

    # ------------------------------------------------------------------
    # Liquidation check
    # ------------------------------------------------------------------

    def check_liquidation(self, mark_price: float) -> bool:
        """Return True if this position should be liquidated at mark_price."""
        if self.is_liquidated:
            return True
        if self.side == "long":
            return mark_price <= self.liquidation_price
        else:
            return mark_price >= self.liquidation_price

    def check_liquidation_wick(self, bar_low: float, bar_high: float) -> bool:
        """
        Return True if this position would have been liquidated at any point
        inside a bar (intraday wick detection).

        Long:  liquidated if the bar's low touched/crossed the liquidation price.
        Short: liquidated if the bar's high touched/crossed the liquidation price.
        """
        if self.is_liquidated:
            return True
        if self.side == "long":
            return bar_low <= self.liquidation_price
        else:
            return bar_high >= self.liquidation_price

    def liquidate(self, liq_fee_bps: float = 1.25) -> float:
        """
        Mark position as liquidated — all initial margin is lost.

        Returns the total realized loss (margin forfeiture). A liquidation
        penalty of `liq_fee_bps` of notional is informational: under isolated
        margin the entire initial margin is forfeited to the exchange, which
        is the conservative upper bound on liquidation cost.
        """
        self.is_liquidated = True
        self.realized_pnl += -self.initial_margin  # margin forfeiture
        self.unrealized_pnl = 0.0
        return -self.initial_margin

    # ------------------------------------------------------------------
    # Funding
    # ------------------------------------------------------------------

    def apply_funding(self, rate: float, mark_price: float) -> float:
        """
        Apply one funding settlement.

        Cash flow (positive = received, negative = paid):
          Long:  -size * mark_price * rate
          Short: +size * mark_price * rate

        Returns the cash flow amount.
        """
        payment = self.size * mark_price * rate
        if self.side == "long":
            cash_flow = -payment
        else:
            cash_flow = payment
        self.cumulative_funding += cash_flow
        return cash_flow

    # ------------------------------------------------------------------
    # Margin ratio
    # ------------------------------------------------------------------

    def compute_margin_ratio(self, mark_price: float) -> float:
        """
        Current margin ratio = (initial_margin + unrealized_pnl) / notional.
        A margin ratio below MMR triggers liquidation.
        """
        self.update_unrealized_pnl(mark_price)
        notional = self.size * mark_price
        if notional <= 0:
            return 0.0
        equity = self.initial_margin + self.unrealized_pnl
        return equity / notional

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_state(self, mark_price: float | None = None) -> PerpPositionState:
        """Return a TypedDict snapshot of this position."""
        if mark_price is not None:
            self.update_unrealized_pnl(mark_price)
        return {
            "symbol": self.symbol,
            "side": self.side,
            "size": self.size,
            "entry_price": self.entry_price,
            "leverage": self.leverage,
            "initial_margin": self.initial_margin,
            "unrealized_pnl": self.unrealized_pnl,
            "realized_pnl": self.realized_pnl,
            "cumulative_funding": self.cumulative_funding,
            "liquidation_price": self.liquidation_price,
            "is_liquidated": self.is_liquidated,
        }


# ---------------------------------------------------------------------------
# PerpPortfolio — manages multiple perpetual positions
# ---------------------------------------------------------------------------

class PerpPortfolio:
    """
    Portfolio of perpetual futures positions (isolated margin).

    Integrates with Portfolio (spot) via cash injection / deduction.
    All margin is held separately per position; no cross-margining.
    """

    def __init__(self) -> None:
        self._positions: Dict[str, PerpPosition] = {}
        self._trade_records: List[TradeRecord] = []
        self._realized_liquidation_losses: float = 0.0
        self._num_liquidations: int = 0
        # Per-round-trip realized PnL ledger (for win rate / profit factor).
        self.realized_pnl_ledger: List[float] = []

    # ------------------------------------------------------------------
    # Position management
    # ------------------------------------------------------------------

    def open_position(
        self,
        symbol: str,
        side: Literal["long", "short"],
        size: float,
        entry_price: float,
        leverage: float,
        available_cash: float,
        timestamp: str = "",
        fee_usd: float = 0.0,
        slippage_usd: float = 0.0,
    ) -> Tuple[Optional[PerpPosition], float]:
        """
        Open or add to a perpetual position.

        Returns:
            (position, margin_consumed_usd)

        margin_consumed_usd is the USD that must be deducted from the
        spot portfolio's cash (initial_margin + fees).
        """
        notional = size * entry_price
        initial_margin = notional / leverage
        total_cash_needed = initial_margin + fee_usd + slippage_usd

        if total_cash_needed > available_cash:
            return None, 0.0

        if symbol in self._positions and not self._positions[symbol].is_liquidated:
            # Average up existing position (simple add — same side only)
            existing = self._positions[symbol]
            if existing.side != side:
                return None, 0.0  # no partial flip in this model

            total_size = existing.size + size
            existing.entry_price = (
                existing.entry_price * existing.size + entry_price * size
            ) / total_size
            existing.size = total_size
            notional_total = total_size * existing.entry_price
            existing.initial_margin += initial_margin
            existing.liquidation_price = _calc_liquidation_price(
                existing.entry_price, leverage, side, notional_total
            )
        else:
            self._positions[symbol] = PerpPosition(
                symbol=symbol,
                side=side,
                size=size,
                entry_price=entry_price,
                leverage=leverage,
            )

        self._record_trade(
            timestamp=timestamp,
            symbol=symbol,
            market_type="perp",
            side="buy" if side == "long" else "sell",
            quantity=size,
            # side-aware slippage: opening a long pays up, opening a short sells down
            price=(
                entry_price + (slippage_usd / size if size > 0 else 0)
                if side == "long"
                else entry_price - (slippage_usd / size if size > 0 else 0)
            ),
            notional=notional,
            fee_usd=fee_usd,
            slippage_usd=slippage_usd,
        )

        return self._positions[symbol], total_cash_needed

    def reduce_position(
        self,
        symbol: str,
        quantity: float,
        close_price: float,
        timestamp: str = "",
        fee_usd: float = 0.0,
        slippage_usd: float = 0.0,
    ) -> Tuple[float, float]:
        """
        Partially close an existing position.

        Returns:
            (realized_pnl, cash_returned)

        cash_returned is the pro-rata margin + realized PnL for the closed
        chunk, minus fees. Closing the entire position degenerates to
        close_position semantics.
        """
        pos = self._positions.get(symbol)
        if pos is None or pos.is_liquidated or quantity <= 0:
            return 0.0, 0.0

        qty = min(quantity, pos.size)
        pos.update_unrealized_pnl(close_price)
        price_per_unit_pnl = (
            (close_price - pos.entry_price) if pos.side == "long" else (pos.entry_price - close_price)
        )
        realized = price_per_unit_pnl * qty
        margin_released = pos.initial_margin * (qty / pos.size)

        pos.realized_pnl += realized
        self.realized_pnl_ledger.append(realized)
        pos.size -= qty
        pos.initial_margin -= margin_released
        if pos.size <= 1e-12:
            pos.size = 0.0
            pos.unrealized_pnl = 0.0
            del self._positions[symbol]
        else:
            pos.unrealized_pnl = (
                (close_price - pos.entry_price) if pos.side == "long" else (pos.entry_price - close_price)
            ) * pos.size

        cash_returned = margin_released + realized - fee_usd - slippage_usd

        self._record_trade(
            timestamp=timestamp,
            symbol=symbol,
            market_type="perp",
            side="sell" if pos.side == "long" else "buy",
            quantity=qty,
            # closing a long sells down into slippage; closing a short buys up
            price=(
                close_price - (slippage_usd / qty if qty > 0 else 0)
                if pos.side == "long"
                else close_price + (slippage_usd / qty if qty > 0 else 0)
            ),
            notional=qty * close_price,
            fee_usd=fee_usd,
            slippage_usd=slippage_usd,
        )
        return realized, cash_returned

    def close_position(
        self,
        symbol: str,
        close_price: float,
        timestamp: str = "",
        fee_usd: float = 0.0,
        slippage_usd: float = 0.0,
    ) -> Tuple[float, float]:
        """
        Close an existing position entirely.

        Returns:
            (realized_pnl, cash_returned)

        cash_returned is the amount to add back to the spot cash balance
        (initial_margin + realized_pnl - fees). Negative if net loss.
        """
        pos = self._positions.get(symbol)
        if pos is None or pos.is_liquidated:
            return 0.0, 0.0

        return self.reduce_position(
            symbol, pos.size, close_price, timestamp, fee_usd, slippage_usd
        )

    # ------------------------------------------------------------------
    # Liquidation
    # ------------------------------------------------------------------

    def check_liquidations(
        self, prices: Dict[str, float]
    ) -> List[str]:
        """
        Check all open positions for liquidation at the given mark prices.

        Kept for backward compatibility (mark-price check). Prefer
        `check_liquidations_bars` for intraday-wick-aware detection.

        Returns a list of symbols that were liquidated. Liquidated positions
        lose all initial margin — no cash is returned.
        """
        bars = {
            symbol: (mark, mark)
            for symbol, mark in prices.items()
        }
        events = self.check_liquidations_bars(bars)
        return [symbol for symbol, _loss in events]

    def check_liquidations_bars(
        self,
        bars: Dict[str, Tuple[float, float]],
        timestamp: str = "",
        liq_fee_bps: float = 1.25,
    ) -> List[Tuple[str, float]]:
        """
        Check all open positions for liquidation using bar low/high.

        Args:
            bars: {symbol: (bar_low, bar_high)} — e.g. from the day's 1h or
                  daily candle range.
            timestamp: stamped on the liquidation trade record.

        Returns:
            List of (symbol, realized_loss_usd). Liquidated positions are
            REMOVED from the portfolio; their entire initial margin is
            forfeited (no cash returns to the spot book).
        """
        events: List[Tuple[str, float]] = []
        for symbol, pos in list(self._positions.items()):
            if pos.is_liquidated:
                continue
            rng = bars.get(symbol)
            if rng is None:
                continue
            bar_low, bar_high = rng
            if not pos.check_liquidation_wick(bar_low, bar_high):
                continue

            loss = pos.liquidate(liq_fee_bps)
            notional = pos.size * pos.entry_price
            self._record_trade(
                timestamp=timestamp,
                symbol=symbol,
                market_type="perp",
                side="sell" if pos.side == "long" else "buy",
                quantity=pos.size,
                price=pos.liquidation_price,
                notional=notional,
                fee_usd=notional * liq_fee_bps / 10_000,
                slippage_usd=0.0,
                event="liquidation",
            )
            self._realized_liquidation_losses += -loss
            self._num_liquidations += 1
            del self._positions[symbol]
            events.append((symbol, loss))
        return events

    def get_realized_liquidation_losses(self) -> float:
        """Cumulative realized USD losses from liquidations (positive number)."""
        return self._realized_liquidation_losses

    def get_num_liquidations(self) -> int:
        return self._num_liquidations

    # ------------------------------------------------------------------
    # Funding rate application
    # ------------------------------------------------------------------

    def apply_funding_rates(
        self,
        rates: Dict[str, float],
        prices: Dict[str, float],
    ) -> Dict[str, float]:
        """
        Apply funding payments for all open positions.

        Args:
            rates:  {symbol: funding_rate}
            prices: {symbol: mark_price}

        Returns:
            {symbol: cash_flow} — net cash flows to add to spot portfolio.
            Negative means cash left the portfolio (long paid funding).
        """
        cash_flows: Dict[str, float] = {}
        for symbol, pos in self._positions.items():
            if pos.is_liquidated:
                continue
            rate = rates.get(symbol, 0.0)
            mark_price = prices.get(symbol)
            if mark_price is None:
                continue
            cf = pos.apply_funding(rate, mark_price)
            cash_flows[symbol] = cf
        return cash_flows

    # ------------------------------------------------------------------
    # Valuation
    # ------------------------------------------------------------------

    def get_total_unrealized_pnl(self, prices: Dict[str, float]) -> float:
        """Sum of unrealized PnL across all open (non-liquidated) positions."""
        total = 0.0
        for symbol, pos in self._positions.items():
            if pos.is_liquidated:
                continue
            mark_price = prices.get(symbol)
            if mark_price is None:
                continue
            pos.update_unrealized_pnl(mark_price)
            total += pos.unrealized_pnl
        return total

    def get_total_margin_locked(self) -> float:
        """Total initial margin locked across all open positions."""
        return sum(
            pos.initial_margin
            for pos in self._positions.values()
            if not pos.is_liquidated
        )

    def get_positions(self) -> Dict[str, PerpPosition]:
        return {k: v for k, v in self._positions.items() if not v.is_liquidated}

    def get_trade_records(self) -> List[TradeRecord]:
        return list(self._trade_records)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _record_trade(
        self,
        timestamp: str,
        symbol: str,
        market_type: str,
        side: str,
        quantity: float,
        price: float,
        notional: float,
        fee_usd: float,
        slippage_usd: float,
        event: str = "",
    ) -> None:
        record: TradeRecord = {
            "timestamp": timestamp,
            "symbol": symbol,
            "market_type": market_type,
            "side": side,
            "quantity": quantity,
            "price": price,
            "notional": notional,
            "fee_usd": fee_usd,
            "slippage_usd": slippage_usd,
            "total_cost_usd": fee_usd + slippage_usd,
        }
        if event:
            record["event"] = event
        self._trade_records.append(record)
