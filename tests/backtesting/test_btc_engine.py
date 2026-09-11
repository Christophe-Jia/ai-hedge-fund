"""Integration tests for BtcBacktestEngine: perp wiring, wick liquidation,
funding settlement, verbose mode, and data-gap abort."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.backtesting.btc_engine import BtcBacktestEngine, CryptoBacktestEngine
from src.backtesting.types import DataGapError
from src.data.funding_rates import FundingRateStore
from src.data.historical_store import HistoricalOHLCVStore

SYM = "BTC/USDT:USDT"


def _ts(day: int, hour: int = 0) -> int:
    """Millisecond timestamp for 2026-01-{day+1} {hour}:00 UTC."""
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return int((base + timedelta(days=day, hours=hour)).timestamp() * 1000)


def _seed_prices(store: HistoricalOHLCVStore, days: list[tuple]) -> None:
    """days: list of (open, high, low, close)."""
    rows = [
        [_ts(i), o, h, l, c, 1000.0]
        for i, (o, h, l, c) in enumerate(days)
    ]
    store.upsert_ohlcv(SYM, "perp", "1d", rows)


def _make_engine(tmp_path, agent, start="2026-01-01", end="2026-01-15",
                 leverage=5.0, verbose=True, **kw):
    kw.setdefault("allow_fetch", False)
    return BtcBacktestEngine(
        agent=agent,
        tickers=[SYM],
        start_date=start,
        end_date=end,
        initial_capital=100_000.0,
        model_name="test",
        model_provider="test",
        selected_analysts=None,
        initial_margin_requirement=0.5,
        perp_tickers=[SYM],
        leverage=leverage,
        db_path=str(tmp_path / "t.db"),
        lookback_months=1,
        verbose=verbose,
        **kw,
    )


class BuyOnceAgent:
    """Buys 1 perp BTC on the first call, holds thereafter."""

    def __init__(self):
        self.calls = 0

    def __call__(self, tickers, start_date, end_date, portfolio,
                 model_name, model_provider, selected_analysts):
        self.calls += 1
        if self.calls == 1:
            return {"decisions": {tickers[0]: {"action": "buy", "quantity": 1.0}},
                    "analyst_signals": {}}
        return {"decisions": {tickers[0]: {"action": "hold", "quantity": 0}},
                "analyst_signals": {}}


class TestPerpWiringE2E:
    def test_perp_position_actually_opens(self, tmp_path):
        """The P1D fix: a perp BUY must create a real PerpPortfolio position."""
        store = HistoricalOHLCVStore(db_path=str(tmp_path / "t.db"))
        # 5 flat days at 50k
        _seed_prices(store, [(50_000, 50_100, 49_900, 50_000)] * 5)
        # extend to cover the engine window
        _seed_prices(store, [(50_000, 50_100, 49_900, 50_000)] * 10)

        agent = BuyOnceAgent()
        engine = _make_engine(tmp_path, agent, end="2026-01-05", verbose=False)
        engine.run_backtest()

        records = engine.get_trade_records()
        buys = [r for r in records if r["side"] == "buy"]
        assert len(buys) >= 1
        assert buys[0]["quantity"] == pytest.approx(1.0)
        assert buys[0]["market_type"] == "perp"
        # Margin 50k/5 = 10k + fee 25 + slippage were debited from cash
        cash = engine._portfolio.get_cash()
        assert cash < 100_000.0 - 10_000.0  # fees+slippage on top of margin

    def test_wick_day_triggers_liquidation(self, tmp_path, capsys):
        """Daily close above liq price, but intraday low below it -> liquidated."""
        store = HistoricalOHLCVStore(db_path=str(tmp_path / "t.db"))
        days = [
            (50_000, 50_500, 49_800, 50_200),   # day0: entry at open 50k, liq=40.5k
            (50_200, 51_000, 50_000, 50_800),   # day1: up
            (50_800, 51_500, 50_500, 51_200),   # day2: up
            (51_200, 51_800, 39_000, 50_900),   # day3: WICK to 39k -> liquidation
            (50_900, 51_000, 50_500, 50_800),   # day4: after
        ] * 3
        _seed_prices(store, days)

        agent = BuyOnceAgent()
        engine = _make_engine(tmp_path, agent, end="2026-01-06")
        engine.run_backtest()

        out = capsys.readouterr().out
        assert "LIQUIDATION" in out
        # liquidation record with event marker
        liq_recs = [r for r in engine.get_trade_records() if r.get("event") == "liquidation"]
        assert len(liq_recs) == 1
        # margin (10k) forfeited -> cash roughly 100k - costs - 10k
        assert engine._portfolio.get_cash() < 91_000.0

    def test_funding_settled_from_store(self, tmp_path):
        """Seed a funding event; long must pay it (cash decreases)."""
        db = str(tmp_path / "t.db")
        store = HistoricalOHLCVStore(db_path=db)
        _seed_prices(store, [(50_000, 50_100, 49_900, 50_000)] * 15)
        funding = FundingRateStore(db_path=db)
        # funding event at day2 00:00 UTC, positive rate = longs pay
        funding.upsert_rates(SYM, [(_ts(2), 0.001)])

        agent = BuyOnceAgent()
        engine = _make_engine(tmp_path, agent, end="2026-01-06", verbose=False)
        engine.run_backtest()

        # 0.001 * 1 BTC * ~50k mark = ~50 USD paid
        assert engine._total_funding_paid == pytest.approx(50.0, abs=5.0)
        assert engine._performance_metrics["total_funding_paid"] == pytest.approx(
            engine._total_funding_paid
        )


class TestEngineBehaviour:
    def test_verbose_false_prints_nothing(self, tmp_path, capsys):
        store = HistoricalOHLCVStore(db_path=str(tmp_path / "t.db"))
        _seed_prices(store, [(50_000, 50_100, 49_900, 50_000)] * 15)

        engine = _make_engine(tmp_path, BuyOnceAgent(), end="2026-01-05", verbose=False)
        engine.run_backtest()
        assert capsys.readouterr().out == ""

    def test_metrics_complete(self, tmp_path):
        store = HistoricalOHLCVStore(db_path=str(tmp_path / "t.db"))
        _seed_prices(store, [(50_000, 50_100, 49_900, 50_000)] * 15)

        engine = _make_engine(tmp_path, BuyOnceAgent(), end="2026-01-05", verbose=False)
        m = engine.run_backtest()
        for key in ("total_return", "total_fees_paid", "num_trades",
                    "win_rate", "profit_factor"):
            assert key in m, f"missing metric: {key}"

    def test_data_gap_raises(self, tmp_path):
        """6+ consecutive days with no data -> DataGapError."""
        store = HistoricalOHLCVStore(db_path=str(tmp_path / "t.db"))
        # only 2 days of data in a 15-day window
        _seed_prices(store, [(50_000, 50_100, 49_900, 50_000)] * 2)

        engine = _make_engine(tmp_path, BuyOnceAgent(), verbose=False)
        with pytest.raises(DataGapError):
            engine.run_backtest()

    def test_alias(self):
        assert CryptoBacktestEngine is BtcBacktestEngine
