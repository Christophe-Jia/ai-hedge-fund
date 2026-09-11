"""Unit tests for PerpPortfolio: liquidation, funding, partial closes."""

from __future__ import annotations

import pytest

from src.backtesting.perpetual import PerpPortfolio, _calc_liquidation_price

# Default test position: 1 BTC @ 49k = $49k notional -> 0.5% MMR tier,
# 5x leverage -> margin 9,800, long liq price 39,445, short liq 58,555.
PX = 49_000.0
LIQ_LONG = PX * (1 - 0.2 + 0.005)   # 39,445
LIQ_SHORT = PX * (1 + 0.2 - 0.005)  # 58,555
MARGIN = PX / 5.0                    # 9,800


def _open(pp: PerpPortfolio, symbol="BTC/USDT:USDT", side="long", size=1.0,
          price=PX, leverage=5.0, cash=1e9, **kw):
    return pp.open_position(symbol, side, size, price, leverage, cash, **kw)


class TestLiquidationPrice:
    def test_long_5x(self):
        assert _calc_liquidation_price(PX, 5.0, "long", PX) == pytest.approx(LIQ_LONG)

    def test_short_5x(self):
        assert _calc_liquidation_price(PX, 5.0, "short", PX) == pytest.approx(LIQ_SHORT)

    def test_higher_tier_mmr(self):
        # $200k notional -> 1.0% MMR tier
        assert _calc_liquidation_price(200_000, 5.0, "long", 200_000) == pytest.approx(
            200_000 * (1 - 0.2 + 0.01)
        )


class TestOpenAndMargin:
    def test_open_locks_margin(self):
        pp = PerpPortfolio()
        pos, consumed = _open(pp)
        assert pos is not None
        assert consumed == pytest.approx(MARGIN)
        assert pp.get_total_margin_locked() == pytest.approx(MARGIN)

    def test_insufficient_cash_rejected(self):
        pp = PerpPortfolio()
        pos, consumed = _open(pp, cash=5_000.0)
        assert pos is None
        assert consumed == 0.0
        assert pp.get_positions() == {}

    def test_avg_up_accumulates(self):
        pp = PerpPortfolio()
        _open(pp, size=1.0, price=49_000.0)
        _open(pp, size=1.0, price=60_000.0)
        pos = pp.get_positions()["BTC/USDT:USDT"]
        assert pos.size == pytest.approx(2.0)
        assert pos.entry_price == pytest.approx(54_500.0)
        assert pos.initial_margin == pytest.approx(49_000 / 5 + 60_000 / 5)

    def test_opposite_side_rejected(self):
        pp = PerpPortfolio()
        _open(pp, side="long")
        pos, consumed = _open(pp, side="short")
        assert pos is None and consumed == 0.0

    def test_side_aware_slippage_in_record(self):
        pp = PerpPortfolio()
        _open(pp, side="long", slippage_usd=100.0)
        rec_long = pp.get_trade_records()[-1]
        assert rec_long["price"] == pytest.approx(PX + 100.0)

        pp2 = PerpPortfolio()
        _open(pp2, side="short", slippage_usd=100.0)
        rec_short = pp2.get_trade_records()[-1]
        assert rec_short["price"] == pytest.approx(PX - 100.0)


class TestFunding:
    def test_long_pays_positive_rate(self):
        pp = PerpPortfolio()
        _open(pp)
        flows = pp.apply_funding_rates(
            {"BTC/USDT:USDT": 0.0001}, {"BTC/USDT:USDT": PX}
        )
        # long pays size * mark * rate = 1 * 49k * 0.0001 = 4.9
        assert flows["BTC/USDT:USDT"] == pytest.approx(-4.9)

    def test_short_receives_positive_rate(self):
        pp = PerpPortfolio()
        _open(pp, side="short")
        flows = pp.apply_funding_rates(
            {"BTC/USDT:USDT": 0.0001}, {"BTC/USDT:USDT": PX}
        )
        assert flows["BTC/USDT:USDT"] == pytest.approx(4.9)


class TestIntradayLiquidation:
    def test_wick_triggers_long_liquidation(self):
        """Bar close 46k > liq 39,445, but the wick to 39k must trigger."""
        pp = PerpPortfolio()
        _open(pp)
        events = pp.check_liquidations_bars({"BTC/USDT:USDT": (39_000.0, 46_000.0)})
        assert len(events) == 1
        symbol, loss = events[0]
        assert symbol == "BTC/USDT:USDT"
        assert loss == pytest.approx(-MARGIN)  # full margin forfeited
        # evicted
        assert "BTC/USDT:USDT" not in pp.get_positions()
        assert pp.get_realized_liquidation_losses() == pytest.approx(MARGIN)
        assert pp.get_num_liquidations() == 1

    def test_wick_triggers_short_liquidation(self):
        pp = PerpPortfolio()
        _open(pp, side="short")
        events = pp.check_liquidations_bars({"BTC/USDT:USDT": (40_000.0, 59_000.0)})
        assert len(events) == 1
        assert events[0][1] == pytest.approx(-MARGIN)

    def test_no_liquidation_when_wick_misses(self):
        pp = PerpPortfolio()
        _open(pp)
        events = pp.check_liquidations_bars({"BTC/USDT:USDT": (40_000.0, 52_000.0)})
        assert events == []
        assert pp.get_positions() != {}

    def test_liquidation_record_has_event(self):
        pp = PerpPortfolio()
        _open(pp)
        pp.check_liquidations_bars({"BTC/USDT:USDT": (39_000.0, 46_000.0)}, timestamp="2026-01-01")
        recs = pp.get_trade_records()
        assert any(r.get("event") == "liquidation" for r in recs)
        liq_rec = [r for r in recs if r.get("event") == "liquidation"][0]
        assert liq_rec["fee_usd"] == pytest.approx(PX * 1.25 / 10_000)

    def test_reopen_after_liquidation(self):
        pp = PerpPortfolio()
        _open(pp)
        pp.check_liquidations_bars({"BTC/USDT:USDT": (39_000.0, 46_000.0)})
        pos, consumed = _open(pp, price=45_000.0)
        assert pos is not None
        assert consumed == pytest.approx(45_000.0 / 5.0)

    def test_mark_price_compat_wrapper(self):
        pp = PerpPortfolio()
        _open(pp)
        liquidated = pp.check_liquidations({"BTC/USDT:USDT": 39_000.0})
        assert liquidated == ["BTC/USDT:USDT"]
        assert pp.get_positions() == {}


class TestPartialClose:
    def test_reduce_half(self):
        pp = PerpPortfolio()
        _open(pp)  # 1 BTC, margin 9,800
        realized, cash = pp.reduce_position("BTC/USDT:USDT", 0.5, 60_000.0)
        assert realized == pytest.approx(5_500.0)  # (60k-49k)*0.5
        assert cash == pytest.approx(5_500.0 + MARGIN / 2)  # margin released + pnl
        pos = pp.get_positions()["BTC/USDT:USDT"]
        assert pos.size == pytest.approx(0.5)
        assert pos.initial_margin == pytest.approx(MARGIN / 2)

    def test_reduce_more_than_size_closes_all(self):
        pp = PerpPortfolio()
        _open(pp)
        realized, cash = pp.reduce_position("BTC/USDT:USDT", 5.0, 60_000.0)
        assert realized == pytest.approx(11_000.0)
        assert cash == pytest.approx(MARGIN + 11_000.0)
        assert pp.get_positions() == {}

    def test_close_position_delegates(self):
        pp = PerpPortfolio()
        _open(pp, side="short")
        realized, cash = pp.close_position("BTC/USDT:USDT", 45_000.0)
        # short: (49k - 45k) * 1 = 4k profit
        assert realized == pytest.approx(4_000.0)
        assert cash == pytest.approx(MARGIN + 4_000.0)
        rec = pp.get_trade_records()[-1]
        assert rec["side"] == "buy"  # closing a short buys back
        assert rec["quantity"] == pytest.approx(1.0)

    def test_short_close_slippage_raises_price(self):
        pp = PerpPortfolio()
        _open(pp, side="short")
        pp.close_position("BTC/USDT:USDT", 45_000.0, slippage_usd=50.0)
        rec = pp.get_trade_records()[-1]
        assert rec["price"] == pytest.approx(45_050.0)
