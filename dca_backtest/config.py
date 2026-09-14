"""Cost model and configuration for the DCA backtest framework.

All annual figures are expressed as decimals (0.0020 = 0.20%).

Important cost notes:
    - Real ETF price data (auto-adjusted close) already includes the fund's own
      expense ratio and dividend reinvestment, so ER is only charged explicitly
      on synthetic (backfilled) segments.
    - US dividend withholding tax is NOT included in adjusted close. Chinese
      investors typically pay 10% (US-China tax treaty rate) on dividends, so we
      apply an extra drag of withholding_tax * dividend_yield on real segments.
    - Synthetic TQQQ (pre-2010) is modeled as 3x daily NDX price return minus
      financing on 2x borrowed notional at (T-bill + financing spread) minus ER.
"""

from __future__ import annotations

from dataclasses import dataclass, replace

TRADING_DAYS = 252


@dataclass(frozen=True)
class InstrumentSpec:
    """Per-instrument cost assumptions."""

    symbol: str
    name: str
    # one-way trading cost (bid-ask spread + slippage), in basis points
    spread_bps: float = 5.0
    # fixed commission per order in USD
    commission_usd: float = 0.0
    # expense ratio charged ONLY on synthetic backfilled segments (annual)
    er_annual: float = 0.0
    # gross dividend yield assumed when backfilling from a price-only index (annual)
    div_yield_annual: float = 0.0
    # approximate dividend yield used to size the withholding-tax drag on real segments
    div_yield_for_tax: float = 0.0
    # levered ETFs only: financing spread over T-bill paid on borrowed notional (annual)
    financing_spread: float = 0.0


INSTRUMENTS: dict[str, InstrumentSpec] = {
    "QQQ": InstrumentSpec(
        symbol="QQQ",
        name="Invesco QQQ (Nasdaq-100)",
        spread_bps=5.0,
        er_annual=0.0020,
        div_yield_annual=0.004,   # NDX paid little in dividends pre-1999
        div_yield_for_tax=0.006,
    ),
    "VOO": InstrumentSpec(
        symbol="VOO",
        name="Vanguard S&P 500",
        spread_bps=5.0,
        er_annual=0.0003,
        div_yield_annual=0.030,   # S&P 500 yielded ~3% in the late 80s
        div_yield_for_tax=0.013,
    ),
    "TQQQ": InstrumentSpec(
        symbol="TQQQ",
        name="ProShares UltraPro QQQ (3x daily)",
        spread_bps=10.0,
        er_annual=0.0086,
        div_yield_for_tax=0.001,
        financing_spread=0.005,
    ),
}


@dataclass
class BacktestConfig:
    """Backtest parameters."""

    monthly_contribution_rmb: float = 20_000.0
    # day of month the contribution is invested (rolled to next trading day)
    contribution_day: int = 1
    # annual growth rate of the monthly contribution (salary raises), 0 = fixed
    contribution_growth_annual: float = 0.0
    # RMB -> USD conversion cost, in basis points
    fx_spread_bps: float = 20.0
    # US dividend withholding tax applied on real ETF segments (10% treaty rate)
    dividend_withholding_tax: float = 0.10
    # rebalancing back to target weights: "none" | "monthly" | "annual"
    rebalance: str = "none"


def with_overrides(
    tqqq_financing_spread: float | None = None,
) -> dict[str, InstrumentSpec]:
    """Return a copy of INSTRUMENTS with optional overrides."""
    instruments = dict(INSTRUMENTS)
    if tqqq_financing_spread is not None:
        instruments["TQQQ"] = replace(
            INSTRUMENTS["TQQQ"], financing_spread=tqqq_financing_spread
        )
    return instruments
