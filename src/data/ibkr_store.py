"""
IBKR historical data bridge via ib_async (TWS API).

Fetches daily OHLCV from Interactive Brokers — the only source in our
stack that serves DELISTED securities and pre-2016 history — and upserts
into the shared HistoricalOHLCVStore (market_type='stocks').

Requires a running TWS or IB Gateway with API enabled:
  - IB Gateway paper:  host=127.0.0.1 port=4002   (recommended default)
  - IB Gateway live:   host=127.0.0.1 port=4001
  - TWS paper:         host=127.0.0.1 port=7497
  - TWS live:          host=127.0.0.1 port=7496

Setup checklist (one-time):
  1. IBKR Client Portal -> Settings -> Account Settings -> API settings:
     enable API access, create a paper account if needed.
  2. Install IB Gateway (stable version), log in with paper credentials.
  3. Gateway configure: Settings -> API -> Settings:
     enable ActiveX & socket clients, port 4002, trust 127.0.0.1.

Usage:
    from src.data.ibkr_store import IbkrHistoricalStore
    store = IbkrHistoricalStore()
    if store.connect():
        store.fetch_and_store("AAPL", duration="10 Y")
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from .historical_store import HistoricalOHLCVStore


class IbkrHistoricalStore:
    """Historical daily bars from IBKR into the shared SQLite store."""

    def __init__(
        self,
        db_path: Optional[str] = None,
        host: str = "127.0.0.1",
        port: int = 4002,
        client_id: int = 17,
        timeout: float = 15.0,
    ) -> None:
        self._store = HistoricalOHLCVStore(
            **({"db_path": db_path} if db_path else {}),
            allow_fetch=False,
        )
        self._host = host
        self._port = port
        self._client_id = client_id
        self._timeout = timeout
        self._ib = None

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def connect(self) -> bool:
        """Connect to TWS/IB Gateway. Returns False (with instructions) on failure."""
        try:
            from ib_async import IB
        except ImportError as exc:
            raise SystemExit("ib_async missing. Run: poetry install") from exc

        self._ib = IB()
        try:
            self._ib.connect(
                self._host, self._port, clientId=self._client_id, timeout=self._timeout
            )
            return self._ib.isConnected()
        except Exception:  # noqa: BLE001 — connection errors vary
            return False

    def disconnect(self) -> None:
        if self._ib is not None and self._ib.isConnected():
            self._ib.disconnect()

    @staticmethod
    def connection_help() -> str:
        return (
            "IBKR 连接失败。检查清单：\n"
            "  1. IB Gateway 是否在运行？(推荐 paper 账户登录)\n"
            "  2. Gateway 菜单 Configure -> Settings -> API -> Settings:\n"
            "     - Enable ActiveX and Socket Clients ✔\n"
            "     - Socket port = 4002 (paper) / 4001 (live)\n"
            "     - Trusted IP: 127.0.0.1\n"
            "  3. IBKR 客户端 Portal -> Settings -> Account Settings -> API 设置里\n"
            "     已启用 API 访问\n"
            "  4. TWS 用户换端口: 7497 (paper) / 7496 (live)\n"
        )

    # ------------------------------------------------------------------
    # Data
    # ------------------------------------------------------------------

    def fetch_and_store(
        self,
        symbol: str,
        duration: str = "10 Y",
        end: Optional[str] = None,
    ) -> int:
        """Fetch daily bars and upsert. Returns rows written.

        `duration`: IBKR duration string ('10 Y', '5 Y', '1 Y', '30 D'...).
        Delisted tickers that no longer resolve raise a clear error.
        """
        if self._ib is None or not self._ib.isConnected():
            raise RuntimeError("not connected — call connect() first")

        from ib_async import Stock

        contract = Stock(symbol.upper(), "SMART", "USD")
        qualified = self._ib.qualifyContracts(contract)
        if not qualified:
            raise ValueError(
                f"{symbol}: contract not found (delisted without successor? "
                f"Try the current ticker — renamed companies (FB->META, "
                f"PCLN->BKNG) carry their history under the new symbol."
            )
        contract = qualified[0]

        end_dt = end or ""  # '' = up to now
        bars = self._ib.reqHistoricalData(
            contract,
            endDateTime=end_dt,
            durationStr=duration,
            barSizeSetting="1 day",
            whatToShow="TRADES",
            useRTH=True,
            formatDate=1,
        )
        if not bars:
            return 0

        rows = []
        for b in bars:
            ts = int(
                datetime(
                    b.date.year, b.date.month, b.date.day, tzinfo=timezone.utc
                ).timestamp()
                * 1000
            )
            rows.append([ts, float(b.open), float(b.high), float(b.low),
                         float(b.close), float(b.volume)])
        return self._store.upsert_ohlcv(symbol.upper(), "stocks", "1d", rows)

    def coverage_missing(
        self, symbols: list[str], min_rows: int = 2000
    ) -> list[str]:
        """Symbols from `symbols` with fewer than min_rows of local data."""
        out = []
        for sym in symbols:
            _first, _last, n = self._store.get_coverage(sym)
            if n < min_rows:
                out.append(sym)
        return out
