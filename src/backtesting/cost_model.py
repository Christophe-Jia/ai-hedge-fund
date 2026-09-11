"""
Trade cost model for crypto backtesting.

Models Binance fee tiers for both spot and USDT-M perpetual futures,
plus a per-symbol square-root market-impact slippage model.

Slippage model:
    impact_bps = impact_factor * 10_000 * sqrt(notional / adv_usd)
    total_bps  = base_spread_bps + impact_bps

Calibrated so (impact_factor=0.01, BTC ADV $15B):
    $100k order  -> ~0.26 bps impact (negligible retail)
    $10M order   -> ~2.58 bps impact (institutional-scale impact)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Literal, Mapping

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
# Per-symbol slippage configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SymbolCostConfig:
    """Slippage parameters for one trading symbol.

    Args:
        adv_usd:         Approximate average daily notional volume (USD).
        base_spread_bps: Bid-ask half-spread in basis points.
        impact_factor:   Sqrt-impact scaling (see module docstring).
    """

    adv_usd: float
    base_spread_bps: float
    impact_factor: float = 0.01


# Rough 2026-era ADV / spread assumptions for the symbols we backtest.
# These are order-of-magnitude inputs — the sqrt model is deliberately
# insensitive to small errors in ADV (impact scales with sqrt(1/ADV)).
DEFAULT_SYMBOL_COSTS: dict[str, SymbolCostConfig] = {
    "BTC/USDT": SymbolCostConfig(adv_usd=15e9, base_spread_bps=2.0),
    "BTC/USDT:USDT": SymbolCostConfig(adv_usd=20e9, base_spread_bps=1.5),
    "ETH/USDT": SymbolCostConfig(adv_usd=8e9, base_spread_bps=2.0),
    "ETH/USDT:USDT": SymbolCostConfig(adv_usd=10e9, base_spread_bps=1.5),
    "SOL/USDT": SymbolCostConfig(adv_usd=2e9, base_spread_bps=4.0),
    "SOL/USDT:USDT": SymbolCostConfig(adv_usd=3e9, base_spread_bps=3.0),
}

# Fallback for symbols not in the table (conservative small-cap assumptions).
DEFAULT_COST_CONFIG = SymbolCostConfig(adv_usd=2e9, base_spread_bps=5.0)


@dataclass
class CostModel:
    """
    Compute realistic trade costs (fees + slippage) for crypto backtests.

    All costs are returned in USD.

    Args:
        vip_tier:       Binance VIP fee tier (default VIP0 — retail).
        bnb_discount:   Whether BNB holdings provide a 25% fee discount.
        symbol_configs: Per-symbol slippage parameters; looked up by the
                        `symbol` argument of the cost methods.
        default_config: Slippage parameters used when `symbol` is empty or
                        unknown to `symbol_configs`.
    """

    vip_tier: VipTier = VipTier.VIP0
    bnb_discount: bool = False
    symbol_configs: Mapping[str, SymbolCostConfig] = field(
        default_factory=lambda: DEFAULT_SYMBOL_COSTS
    )
    default_config: SymbolCostConfig = DEFAULT_COST_CONFIG

    def _config_for(self, symbol: str) -> SymbolCostConfig:
        if symbol:
            cfg = self.symbol_configs.get(symbol)
            if cfg is not None:
                return cfg
        return self.default_config

    def compute_trade_cost(
        self,
        notional_usd: float,
        market_type: MarketType,
        is_maker: bool = False,
        symbol: str = "",
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

    def compute_slippage_only(self, notional_usd: float, symbol: str = "") -> float:
        """
        Compute price slippage cost for a single trade.

        Square-root market-impact model per symbol:
          - base_spread_bps covers the bid-ask half-spread
          - impact_bps scales with sqrt(order size / symbol ADV)

        At typical retail sizes (<$100k), slippage is dominated by the spread.
        At institutional sizes ($10M+), market impact becomes significant.

        Returns slippage cost in USD.
        """
        if notional_usd <= 0:
            return 0.0

        cfg = self._config_for(symbol)
        impact_bps = cfg.impact_factor * 10_000 * (notional_usd / cfg.adv_usd) ** 0.5
        total_bps = cfg.base_spread_bps + impact_bps
        return notional_usd * total_bps / 10_000

    def compute_total_cost(
        self,
        notional_usd: float,
        market_type: MarketType,
        is_maker: bool = False,
        symbol: str = "",
    ) -> tuple[float, float, float]:
        """
        Compute fee + slippage together.

        Returns:
            (fee_usd, slippage_usd, total_cost_usd)
        """
        fee = self.compute_trade_cost(notional_usd, market_type, is_maker, symbol)
        slippage = self.compute_slippage_only(notional_usd, symbol)
        return fee, slippage, fee + slippage

    def slippage_as_pct(self, notional_usd: float, symbol: str = "") -> float:
        """
        Return slippage as a fraction of notional (for passing to portfolio
        methods as `slippage_pct`).
        """
        if notional_usd <= 0:
            return 0.0
        return self.compute_slippage_only(notional_usd, symbol) / notional_usd
