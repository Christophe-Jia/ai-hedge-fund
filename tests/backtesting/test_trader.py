"""Unit tests for TradeExecutor: spot routing + the P1D perp wiring fix."""

from __future__ import annotations

import pytest

from src.backtesting.cost_model import CostModel, VipTier
from src.backtesting.perpetual import PerpPortfolio
from src.backtesting.portfolio import Portfolio
from src.backtesting.trader import TradeExecutor


@pytest.fixture
def spot() -> Portfolio:
    return Portfolio(tickers=["BTC/USDT"], initial_cash=100_000.0, margin_requirement=0.5)


@pytest.fixture
def perp() -> PerpPortfolio:
    return PerpPortfolio()


class TestPerpWiring:
    def test_buy_opens_perp_position_and_debits_margin(self, spot, perp):
        cm = CostModel(VipTier.VIP0)
        ex = TradeExecutor(cost_model=cm, perp_portfolio=perp, leverage=5.0)
        filled = ex.execute_trade(
            "BTC/USDT", "buy", 1.0, 50_000.0, spot, market_type="perp", timestamp="2026-01-01",
        )
        assert filled == pytest.approx(1.0)
        pos = perp.get_positions()["BTC/USDT"]
        assert pos.side == "long"
        assert pos.size == pytest.approx(1.0)
        # margin 10k + taker fee 25 + slippage
        expected_slip = cm.compute_slippage_only(50_000.0, symbol="BTC/USDT")
        assert spot.get_cash() == pytest.approx(100_000.0 - 10_000.0 - 25.0 - expected_slip)
        assert perp.get_total_margin_locked() == pytest.approx(10_000.0)

    def test_short_opens_short_perp(self, spot, perp):
        ex = TradeExecutor(cost_model=CostModel(VipTier.VIP0), perp_portfolio=perp, leverage=5.0)
        ex.execute_trade("BTC/USDT", "short", 0.5, 50_000.0, spot, market_type="perp")
        pos = perp.get_positions()["BTC/USDT"]
        assert pos.side == "short"
        assert pos.size == pytest.approx(0.5)

    def test_sell_closes_perp_and_credits_cash(self, spot, perp):
        cm = CostModel(VipTier.VIP0)
        ex = TradeExecutor(cost_model=cm, perp_portfolio=perp, leverage=5.0)
        ex.execute_trade("BTC/USDT", "buy", 1.0, 50_000.0, spot, market_type="perp")
        cash_after_open = spot.get_cash()

        filled = ex.execute_trade(
            "BTC/USDT", "sell", 1.0, 60_000.0, spot, market_type="perp", timestamp="2026-01-02",
        )
        assert filled == pytest.approx(1.0)
        assert perp.get_positions() == {}
        # close at 60k: +10k pnl, margin 10k back, fee 0.05%*60k + slippage
        close_slip = cm.compute_slippage_only(60_000.0, symbol="BTC/USDT")
        assert spot.get_cash() == pytest.approx(cash_after_open + 10_000.0 + 10_000.0 - 30.0 - close_slip)

    def test_sell_with_no_position_is_noop(self, spot, perp):
        ex = TradeExecutor(perp_portfolio=perp, leverage=5.0)
        filled = ex.execute_trade("BTC/USDT", "sell", 1.0, 50_000.0, spot, market_type="perp")
        assert filled == 0.0
        assert spot.get_cash() == pytest.approx(100_000.0)

    def test_insufficient_margin_rejected(self, perp):
        small = Portfolio(tickers=["BTC/USDT"], initial_cash=5_000.0, margin_requirement=0.5)
        ex = TradeExecutor(perp_portfolio=perp, leverage=5.0)
        # needs 10k margin but only 5k cash
        filled = ex.execute_trade("BTC/USDT", "buy", 1.0, 50_000.0, small, market_type="perp")
        assert filled == 0.0
        assert small.get_cash() == pytest.approx(5_000.0)
        assert perp.get_positions() == {}

    def test_fractional_quantity_preserved(self, spot, perp):
        ex = TradeExecutor(perp_portfolio=perp, leverage=5.0)
        filled = ex.execute_trade("BTC/USDT", "buy", 0.12345, 50_000.0, spot, market_type="perp")
        assert filled == pytest.approx(0.12345)
        assert perp.get_positions()["BTC/USDT"].size == pytest.approx(0.12345)

    def test_perp_without_portfolio_falls_back_to_spot(self, spot):
        """market_type=perp but no perp_portfolio wired: legacy behaviour."""
        ex = TradeExecutor()
        filled = ex.execute_trade("BTC/USDT", "buy", 1.0, 50_000.0, spot, market_type="perp")
        assert filled == pytest.approx(1.0)
        assert spot.get_positions()["BTC/USDT"]["long"] == pytest.approx(1.0)

    def test_round_trip_no_cost_model(self, spot, perp):
        ex = TradeExecutor(perp_portfolio=perp, leverage=2.0)
        ex.execute_trade("BTC/USDT", "buy", 1.0, 50_000.0, spot, market_type="perp")
        assert spot.get_cash() == pytest.approx(75_000.0)  # 25k margin, no fees
        ex.execute_trade("BTC/USDT", "sell", 1.0, 54_000.0, spot, market_type="perp")
        # 25k margin + 4k pnl back
        assert spot.get_cash() == pytest.approx(104_000.0)


class TestSpotRegression:
    def test_spot_buy_sell_with_costs(self, spot):
        cm = CostModel(VipTier.VIP0)
        ex = TradeExecutor(cost_model=cm)
        filled = ex.execute_trade("BTC/USDT", "buy", 1.0, 50_000.0, spot, market_type="spot")
        assert filled == pytest.approx(1.0)
        # buy price inflated by slippage; fee 0.10% * 50k = 50
        slip = cm.compute_slippage_only(50_000.0, symbol="BTC/USDT")
        assert spot.get_cash() == pytest.approx(100_000.0 - 50_000.0 - slip - 50.0)
        assert spot.get_total_fees_paid() == pytest.approx(50.0)

    def test_spot_returns_float(self, spot):
        ex = TradeExecutor()
        filled = ex.execute_trade("BTC/USDT", "buy", 0.5, 50_000.0, spot)
        assert isinstance(filled, float)
        assert filled == pytest.approx(0.5)

    def test_hold_returns_zero(self, spot):
        ex = TradeExecutor()
        assert ex.execute_trade("BTC/USDT", "hold", 1.0, 50_000.0, spot) == 0.0
        assert ex.execute_trade("BTC/USDT", "bogus-action", 1.0, 50_000.0, spot) == 0.0
        assert ex.execute_trade("BTC/USDT", "buy", 0.0, 50_000.0, spot) == 0.0
