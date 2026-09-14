"""Unit tests for NasdaqDailyStore (offline; live fetch is network-marked).

Covers the ETF path (assetclass="etf", the default). The store superseded
the old EtfDailyStore in the M1 refactor; parsing/query semantics carry
over unchanged.
"""

from __future__ import annotations

import pytest

from src.data.nasdaq_store import NasdaqDailyStore

_SAMPLE_PAYLOAD = {
    "data": {
        "symbol": "QQQ",
        "totalRecords": 3,
        "tradesTable": {
            "headers": {"date": "Date", "close": "Close/Last", "volume": "Volume",
                        "open": "Open", "high": "High", "low": "Low"},
            "rows": [
                # newest-first, comma numbers, MM/DD/YYYY
                {"date": "01/06/2026", "close": "483.70", "volume": "28,841,000",
                 "open": "482.00", "high": "484.10", "low": "481.20"},
                {"date": "01/05/2026", "close": "481.90", "volume": "31,102,000",
                 "open": "480.10", "high": "482.50", "low": "479.80"},
                {"date": "01/02/2026", "close": "479.40", "volume": "35,120,000",
                 "open": "483.50", "high": "483.60", "low": "478.90"},
            ],
        },
    },
    "status": {"rCode": 200},
}

_PAYLOAD_WITH_JUNK = {
    "data": {
        "symbol": "VOO",
        "tradesTable": {
            "rows": [
                {"date": "01/06/2026", "close": "483.70", "volume": "N/A",
                 "open": "482.00", "high": "484.10", "low": "481.20"},
                {"date": "garbage", "close": "x", "volume": "1", "open": "1", "high": "2", "low": "0.5"},
                {"date": "01/05/2026", "close": "481.90", "volume": "31,102,000",
                 "open": "480.10", "high": "482.50", "low": "479.80"},
            ],
        },
    }
}


class TestPayloadParsing:
    def test_parse_basic(self):
        rows = NasdaqDailyStore._parse_payload(_SAMPLE_PAYLOAD)
        assert len(rows) == 3
        # sorted oldest-first: 2026-01-02 first
        assert rows[0][0] == 1767312000000  # 2026-01-02 UTC ms
        assert rows[0][1] == pytest.approx(483.50)
        assert rows[0][4] == pytest.approx(479.40)
        assert rows[0][5] == pytest.approx(35_120_000)  # comma-stripped volume

    def test_parse_skips_malformed(self):
        rows = NasdaqDailyStore._parse_payload(_PAYLOAD_WITH_JUNK)
        assert len(rows) == 2
        # N/A volume -> 0.0, garbage row dropped
        assert rows[1][5] == 0.0

    def test_parse_empty(self):
        assert NasdaqDailyStore._parse_payload(None) == []
        assert NasdaqDailyStore._parse_payload({"data": None}) == []
        assert NasdaqDailyStore._parse_payload({"data": {"tradesTable": {"rows": None}}}) == []


class TestStoreRoundTrip:
    def test_upsert_and_query_lookahead_safe(self, tmp_path):
        store = NasdaqDailyStore(db_path=str(tmp_path / "etf.db"))
        rows = NasdaqDailyStore._parse_payload(_SAMPLE_PAYLOAD)
        n = store._store.upsert_ohlcv("QQQ", "etf", "1d", rows)
        assert n == 3

        df = store.get_daily("QQQ", "2026-01-01", "2026-01-31")
        assert len(df) == 3
        assert df["close"].iloc[0] == pytest.approx(479.40)  # oldest first

        # exclusive upper bound: end=2026-01-06 excludes 01-06
        df2 = store.get_daily("QQQ", "2026-01-01", "2026-01-06")
        assert len(df2) == 2
        assert df2.index[-1].strftime("%Y-%m-%d") == "2026-01-05"

    def test_close_series_dividend_accrual(self, tmp_path):
        store = NasdaqDailyStore(db_path=str(tmp_path / "etf.db"))
        rows = NasdaqDailyStore._parse_payload(_SAMPLE_PAYLOAD)
        store._store.upsert_ohlcv("VOO", "etf", "1d", rows)

        raw = store.get_close_series("VOO", "2026-01-01", "2026-01-31", div_yield_annual=0.0)
        total = store.get_close_series("VOO", "2026-01-01", "2026-01-31", div_yield_annual=0.013)
        assert total.iloc[0] == pytest.approx(raw.iloc[0])
        assert total.iloc[-1] > raw.iloc[-1]
        uplift = total.iloc[-1] / raw.iloc[-1] - 1.0
        assert 0.0 < uplift < 0.001  # ~1.3%/yr over 2 days

    def test_unknown_symbol_raises(self, tmp_path):
        store = NasdaqDailyStore(db_path=str(tmp_path / "etf.db"))
        with pytest.raises(ValueError, match="Unsupported ETF"):
            store.fetch_and_store("NOPE")

    def test_coverage_empty(self, tmp_path):
        store = NasdaqDailyStore(db_path=str(tmp_path / "etf.db"))
        assert store.get_coverage("QQQ") == (None, None, 0)


@pytest.mark.network
class TestNetwork:
    def test_fetch_qqq_smoke(self, tmp_path):
        """Live fetch (opt-in: pytest -m network). ~10y of daily bars."""
        store = NasdaqDailyStore(db_path=str(tmp_path / "etf.db"))
        n = store.fetch_and_store("QQQ")
        assert n > 2000
        first, last, total = store.get_coverage("QQQ")
        assert first is not None and first.startswith(("2015", "2016"))
        assert last is not None
