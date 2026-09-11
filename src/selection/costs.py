"""IBKR-style trading cost model for US equities.

Tiered fixed pricing (IBKR Pro default): $0.005 per share, minimum $1.00
per order. Plus a one-way half-spread assumption in bps. At low-frequency
monthly rebalances on S&P 100 names these costs are small but measurable —
the point of modelling them is honest net-of-cost results.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class IbkrCostModel:
    commission_per_share: float = 0.005
    min_commission_per_order: float = 1.0
    # One-way cost in basis points of trade notional (half-spread + slippage
    # for liquid large caps; S&P 100 names typically 2-5 bps)
    spread_bps: float = 5.0

    def trade_cost(self, shares: float, price: float) -> float:
        """Cost in USD to trade `shares` at `price` (buy or sell)."""
        if shares <= 0 or price <= 0:
            return 0.0
        notional = shares * price
        commission = max(shares * self.commission_per_share, self.min_commission_per_order)
        spread = notional * self.spread_bps / 10_000.0
        return commission + spread
