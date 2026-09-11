"""
FRED (Federal Reserve Economic Data) daily series loader with local caching.

Serves long-history daily series that the Nasdaq quote API can't (10y cap):
NASDAQCOM (Nasdaq Composite, 1971→), DTB3 (3-month T-bill, 1954→), etc.
FRED is reachable from mainland China corporate networks.

CSV format: "observation_date,VALUE" with "." for missing days (holidays).

Usage:
    from src.data.fred_store import FredSeries
    fred = FredSeries()
    nasdaq = fred.get("NASDAQCOM")     # pd.Series, float, tz-naive dates
    tbill = fred.get("DTB3")           # annualized percent, e.g. 5.25
"""

from __future__ import annotations

import subprocess
import urllib.request
from datetime import datetime
from pathlib import Path

import pandas as pd

_FRED_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
_DEFAULT_CACHE = Path(__file__).resolve().parents[2] / "data" / "fred"
_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


class FredSeries:
    def __init__(self, cache_dir: Path | str | None = None, timeout: float = 30.0) -> None:
        self._cache = Path(cache_dir) if cache_dir else _DEFAULT_CACHE
        self._timeout = timeout

    def get(self, series_id: str, refresh: bool = False) -> pd.Series:
        """Return the daily series (dates as naive UTC timestamps, float values).

        '.' missing values are dropped. Cached locally as CSV; pass
        refresh=True to re-download.
        """
        path = self._cache / f"{series_id.lower()}.csv"
        if refresh or not path.exists():
            self._download(series_id, path)
        return self._parse(path)

    def _download(self, series_id: str, path: Path) -> None:
        url = _FRED_URL.format(series_id=series_id)
        try:
            req = urllib.request.Request(url, headers={"User-Agent": _UA})
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                text = resp.read().decode("utf-8")
        except Exception:  # noqa: BLE001 — fall back to curl
            out = subprocess.run(
                ["curl", "-s", "-m", "60", url, "-H", f"User-Agent: {_UA}"],
                capture_output=True, text=True, timeout=90,
            )
            if out.returncode != 0 or "," not in out.stdout[:100]:
                raise RuntimeError(f"FRED download failed for {series_id}")
            text = out.stdout
        if "observation_date" not in text[:200]:
            raise RuntimeError(f"FRED returned unexpected payload for {series_id}: {text[:120]!r}")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    @staticmethod
    def _parse(path: Path) -> pd.Series:
        df = pd.read_csv(path)
        series_id = df.columns[1]
        # NOTE: to_numpy() — constructing a Series from another Series with a
        # new index ALIGNS (all-NaN), it does not place positionally.
        vals = pd.to_numeric(df[series_id].replace(".", pd.NA), errors="coerce").to_numpy()
        idx = pd.to_datetime(df["observation_date"])
        return pd.Series(vals, index=idx, name=series_id).dropna().sort_index()
