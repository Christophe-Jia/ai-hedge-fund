"""Unit tests for the per-symbol sqrt market-impact cost model."""

from __future__ import annotations

import math

import pytest

from src.backtesting.cost_model import (
    DEFAULT_SYMBOL_COSTS,
    CostModel,
    SymbolCostConfig,
    VipTier,
)


class TestFeeTable:
    def test_spot_taker_vip0(self):
        cm = CostModel(vip_tier=VipTier.VIP0)
        assert cm.compute_trade_cost(10_000.0, "spot") == pytest.approx(10.0)

    def test_spot_maker_vip0(self):
        cm = CostModel(vip_tier=VipTier.VIP0)
        assert cm.compute_trade_cost(10_000.0, "spot", is_maker=True) == pytest.approx(10.0)

    def test_perp_taker_vip0(self):
        cm = CostModel(vip_tier=VipTier.VIP0)
        assert cm.compute_trade_cost(10_000.0, "perp") == pytest.approx(5.0)

    def test_perp_maker_vip0(self):
        cm = CostModel(vip_tier=VipTier.VIP0)
        assert cm.compute_trade_cost(10_000.0, "perp", is_maker=True) == pytest.approx(2.0)

    def test_bnb_discount(self):
        cm = CostModel(vip_tier=VipTier.VIP0, bnb_discount=True)
        assert cm.compute_trade_cost(10_000.0, "perp") == pytest.approx(5.0 * 0.75)

    def test_zero_notional(self):
        cm = CostModel()
        assert cm.compute_trade_cost(0.0, "spot") == 0.0
        assert cm.compute_slippage_only(0.0, "BTC/USDT") == 0.0


class TestSqrtSlippage:
    def test_hand_computed_btc_10m(self):
        """$10M on BTC spot: sqrt(10e6/15e9)=0.02582 -> 2.582 bps impact + 2 bps spread."""
        cm = CostModel(vip_tier=VipTier.VIP0)
        notional = 10_000_000.0
        cfg = DEFAULT_SYMBOL_COSTS["BTC/USDT"]
        expected_impact_bps = 0.01 * 10_000 * math.sqrt(notional / cfg.adv_usd)
        expected = notional * (cfg.base_spread_bps + expected_impact_bps) / 10_000
        got = cm.compute_slippage_only(notional, "BTC/USDT")
        assert got == pytest.approx(expected)
        assert got == pytest.approx(4_582.0, rel=1e-3)

    def test_retail_size_dominated_by_spread(self):
        """$100k on BTC: impact ~0.26 bps, spread 2 bps -> ~$22.6."""
        cm = CostModel()
        got = cm.compute_slippage_only(100_000.0, "BTC/USDT")
        assert got == pytest.approx(22.58, rel=1e-2)

    def test_sqrt_not_linear(self):
        """4x the notional: bps impact doubles (sqrt), USD impact 8x (4x N * 2x bps)."""
        cm = CostModel()
        cfg = DEFAULT_SYMBOL_COSTS["BTC/USDT"]
        small = cm.compute_slippage_only(1_000_000.0, "BTC/USDT")
        big = cm.compute_slippage_only(4_000_000.0, "BTC/USDT")
        spread_small = 1_000_000.0 * cfg.base_spread_bps / 10_000
        spread_big = 4_000_000.0 * cfg.base_spread_bps / 10_000
        impact_small = small - spread_small
        impact_big = big - spread_big
        # USD impact ~ N * sqrt(N)  ->  ratio (4M/1M)^1.5 = 8
        assert impact_big == pytest.approx(8.0 * impact_small, rel=1e-9)

    def test_per_symbol_lookup(self):
        """Same notional costs more on SOL (thin book) than BTC."""
        cm = CostModel()
        btc = cm.compute_slippage_only(5_000_000.0, "BTC/USDT")
        sol = cm.compute_slippage_only(5_000_000.0, "SOL/USDT")
        assert sol > btc

    def test_unknown_symbol_uses_default(self):
        cm = CostModel()
        got = cm.compute_slippage_only(1_000_000.0, "DOGE/USDT")
        cfg = cm.default_config
        expected_impact = 0.01 * 10_000 * math.sqrt(1_000_000.0 / cfg.adv_usd)
        expected = 1_000_000.0 * (cfg.base_spread_bps + expected_impact) / 10_000
        assert got == pytest.approx(expected)

    def test_empty_symbol_uses_default(self):
        cm = CostModel()
        assert cm.compute_slippage_only(1_000.0, "") == cm.compute_slippage_only(1_000.0, "DOGE/USDT")

    def test_custom_configs_override(self):
        cm = CostModel(
            symbol_configs={"XYZ/USDT": SymbolCostConfig(adv_usd=1e9, base_spread_bps=10.0)}
        )
        got = cm.compute_slippage_only(1_000_000.0, "XYZ/USDT")
        expected_impact_bps = 0.01 * 10_000 * math.sqrt(1_000_000.0 / 1e9)
        expected = 1_000_000.0 * (10.0 + expected_impact_bps) / 10_000
        assert got == pytest.approx(expected)
        # sanity: unknown symbol still uses default config
        assert cm.compute_slippage_only(1_000_000.0, "ABC/USDT") != pytest.approx(expected)


class TestTotalCost:
    def test_total_cost_tuple(self):
        cm = CostModel(vip_tier=VipTier.VIP0)
        fee, slip, total = cm.compute_total_cost(10_000.0, "spot", symbol="BTC/USDT")
        assert fee == pytest.approx(10.0)
        # retail size: spread 2bps ($2) + tiny sqrt impact (~0.08bps) -> ~$2.08
        expected_slip = cm.compute_slippage_only(10_000.0, "BTC/USDT")
        assert slip == pytest.approx(expected_slip)
        assert 2.0 < slip < 2.2
        assert total == pytest.approx(fee + slip)

    def test_slippage_as_pct(self):
        cm = CostModel()
        pct = cm.slippage_as_pct(1_000_000.0, "BTC/USDT")
        slip = cm.compute_slippage_only(1_000_000.0, "BTC/USDT")
        assert pct == pytest.approx(slip / 1_000_000.0)
