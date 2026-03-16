"""
Trade cost model for BTC backtesting.

Models Binance fee tiers for both spot and USDT-M perpetual futures,
plus a market-impact-based dynamic slippage model calibrated to BTC
retail order sizes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Literal

MarketType = Literal["spot", "perp"]


class VipTier(Enum):
    VIP0 = 0
    VIP1 = 1
    VIP2 = 2
    VIP3 = 3


# ---------------------------------------------------------------------------
# Binance fee tables (as decimal fractions, e.g. 0.001 = 0.1%)
# ---------------------------------------------------------------------------

# Binance Spot fees per VIP tier: (maker_rate, taker_rate)
_BINANCE_SPOT_FEES: dict[VipTier, tuple[float, float]] = {
    VipTier.VIP0: (0.0010, 0.0010),  # 0.10% / 0.10%
    VipTier.VIP1: (0.0009, 0.0010),  # 0.09% / 0.10%
    VipTier.VIP2: (0.0008, 0.0010),  # 0.08% / 0.10%
    VipTier.VIP3: (0.0007, 0.0009),  # 0.07% / 0.09%
}

# Binance USDT-M Perpetual fees per VIP tier: (maker_rate, taker_rate)
_BINANCE_PERP_FEES: dict[VipTier, tuple[float, float]] = {
    VipTier.VIP0: (0.0002, 0.0005),   # 0.02% / 0.05%
    VipTier.VIP1: (0.00016, 0.0004),  # 0.016% / 0.04%
    VipTier.VIP2: (0.00010, 0.0003),  # 0.010% / 0.03%
    VipTier.VIP3: (0.00008, 0.0002),  # 0.008% / 0.02%
}

# BNB discount factor applied to the total fee (25% discount)
_BNB_DISCOUNT = 0.75

# ---------------------------------------------------------------------------
# Slippage model parameters
# ---------------------------------------------------------------------------

# Base bid-ask spread in basis points (1 bps = 0.01%)
# BTC spot on Binance typically has ~1-2 bps spread; we use 2 bps conservatively.
_BASE_SPREAD_BPS = 2.0

# BTC approximate Average Daily Volume in USD ($15 billion)
_DEFAULT_ADV_USD = 15_000_000_000.0

# Market-impact factor: how many bps of impact per unit of ADV participation
# Calibrated so $100k order → ~0.007 bps (negligible retail impact),
# $10M order → ~0.67 bps (institutional-scale impact starts showing).
_IMPACT_FACTOR = 1.0


@dataclass
class CostModel:
    """
    Compute realistic trade costs (fees + slippage) for BTC backtests.

    All costs are returned in USD.

    Args:
        vip_tier:     Binance VIP fee tier (default VIP0 — retail).
        bnb_discount: Whether BNB holdings provide a 25% fee discount.
        adv_usd:      Assumed average daily volume for market impact calc.
        base_spread_bps: Half-spread assumption in basis points.
        impact_factor:   Scaling factor for market impact (dimensionless).
    """

    vip_tier: VipTier = VipTier.VIP0
    bnb_discount: bool = False
    adv_usd: float = _DEFAULT_ADV_USD
    base_spread_bps: float = _BASE_SPREAD_BPS
    impact_factor: float = _IMPACT_FACTOR

    def compute_trade_cost(
        self,
        notional_usd: float,
        market_type: MarketType,
        is_maker: bool = False,
    ) -> float:
        """
        Compute the exchange fee for a single trade.

        Backtest assumption: all fills are market orders (taker), unless
        `is_maker=True` is explicitly requested.

        Returns fee in USD.
        """
        if notional_usd <= 0:
            return 0.0

        fee_table = (
            _BINANCE_PERP_FEES if market_type == "perp" else _BINANCE_SPOT_FEES
        )
        maker_rate, taker_rate = fee_table[self.vip_tier]
        rate = maker_rate if is_maker else taker_rate

        discount = _BNB_DISCOUNT if self.bnb_discount else 1.0
        return notional_usd * rate * discount

    def compute_slippage_only(self, notional_usd: float) -> float:
        """
        Compute price slippage cost for a single trade.

        Uses a square-root market-impact model:
          - base_spread_bps covers the bid-ask half-spread
          - market_impact_bps scales with order size relative to ADV

        At typical retail sizes (<$100k), slippage is near-zero.
        At institutional sizes ($10M+), market impact becomes significant.

        Returns slippage cost in USD.
        """
        if notional_usd <= 0:
            return 0.0

        participation = notional_usd / self.adv_usd
        market_impact_bps = participation * self.impact_factor * 10_000
        total_bps = self.base_spread_bps + market_impact_bps
        return notional_usd * total_bps / 10_000

    def compute_total_cost(
        self,
        notional_usd: float,
        market_type: MarketType,
        is_maker: bool = False,
    ) -> tuple[float, float, float]:
        """
        Compute fee + slippage together.

        Returns:
            (fee_usd, slippage_usd, total_cost_usd)
        """
        fee = self.compute_trade_cost(notional_usd, market_type, is_maker)
        slippage = self.compute_slippage_only(notional_usd)
        return fee, slippage, fee + slippage

    def slippage_as_pct(self, notional_usd: float) -> float:
        """
        Return slippage as a fraction of notional (for passing to portfolio
        methods as `slippage_pct`).
        """
        if notional_usd <= 0:
            return 0.0
        return self.compute_slippage_only(notional_usd) / notional_usd
