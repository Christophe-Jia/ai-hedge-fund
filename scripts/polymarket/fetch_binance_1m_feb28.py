"""
Binance Vision 1m volume anomaly analysis for BTC Feb 28, 2026.

Downloads BTCUSDT-1m data from Binance Vision (monthly zip),
falls back to data-api.binance.vision REST if zip unavailable,
then computes volume/trade-count z-scores and iceberg flags
around the 06:00 UTC crash on 2026-02-28.

Usage:
    poetry run python scripts/polymarket/fetch_binance_1m_feb28.py
    poetry run python scripts/polymarket/fetch_binance_1m_feb28.py --window "04:00-08:00"
"""

from __future__ import annotations

import argparse
import io
import os
import sys
import time
import zipfile
import csv
from datetime import datetime, timezone
from pathlib import Path

# Ensure project root is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

try:
    import pandas as pd
    import numpy as np
except ImportError as exc:
    raise SystemExit("Required: pandas, numpy.\nRun: poetry install") from exc

try:
    import requests
except ImportError as exc:
    raise SystemExit("Required: requests.\nRun: poetry install") from exc

try:
    from colorama import Fore, Style, init as colorama_init
    colorama_init(autoreset=True)
    _COLORAMA = True
except ImportError:
    _COLORAMA = False

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------

DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "polymarket"
DATA_DIR.mkdir(parents=True, exist_ok=True)

_ZIP_CACHE = DATA_DIR / "BTCUSDT-1m-2026-02.zip"
_CSV_OUT = DATA_DIR / "btc_1m_feb28_binance.csv"

# Binance Vision base URLs (replicated from seed_btc_history.py)
_VISION_SPOT_BASE = "https://data.binance.vision/data/spot/monthly/klines"
_VISION_API_KLINES = "https://data-api.binance.vision/api/v3/klines"

_SESSION = requests.Session()
_SESSION.headers["User-Agent"] = "btc-1m-analysis/1.0"

# Binance Vision CSV column names (12 cols, no header row)
_COLS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "n_trades",
    "taker_buy_volume",
    "taker_buy_quote_volume",
    "ignore",
]

# Rolling window for z-score baseline (60 1m bars = 1 hour)
_ROLLING_WINDOW = 60

# Binance Vision 2026 files use MICROSECOND timestamps (not milliseconds).
# All _TS_* constants are in microseconds (us).
def _ts_us(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1_000_000)

# Analysis window: Feb 27 22:00 → Mar 1 00:00 UTC (warmup + full Feb 28)
_ANALYSIS_START_US = _ts_us(datetime(2026, 2, 27, 22, 0, tzinfo=timezone.utc))
_ANALYSIS_END_US   = _ts_us(datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc))

# Full Feb 2026 range for download
_FEB_START_US = _ts_us(datetime(2026, 2, 1, tzinfo=timezone.utc))
_FEB_END_US   = _ts_us(datetime(2026, 3, 1, tzinfo=timezone.utc))

# Valid microsecond range guard (2020-01-01 to 2030-01-01)
_TS_US_MIN = _ts_us(datetime(2020, 1, 1, tzinfo=timezone.utc))
_TS_US_MAX = _ts_us(datetime(2030, 1, 1, tzinfo=timezone.utc))


# ---------------------------------------------------------------------------
# Download helpers (mirrors seed_btc_history.py patterns)
# ---------------------------------------------------------------------------

def _download_zip_csv(url: str) -> list[list[str]] | None:
    """Download a zip and return parsed CSV rows, or None on 404."""
    resp = _SESSION.get(url, timeout=60)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        csv_name = zf.namelist()[0]
        with zf.open(csv_name) as f:
            reader = csv.reader(io.TextIOWrapper(f))
            rows = list(reader)
    return rows


def _fetch_via_zip(year: int, month: int) -> list[list[str]] | None:
    """Try Binance Vision monthly zip for BTCUSDT 1m."""
    url = (
        f"{_VISION_SPOT_BASE}/BTCUSDT/1m/"
        f"BTCUSDT-1m-{year}-{month:02d}.zip"
    )
    print(f"  Trying Vision zip: {url}")
    return _download_zip_csv(url)


def _fetch_via_rest(start_us: int, end_us: int) -> list[list]:
    """Fallback: REST API from data-api.binance.vision (no auth required).
    REST API uses milliseconds; we convert to/from microseconds internally."""
    print("  Zip not available — falling back to REST API ...")
    # REST API expects ms, convert us→ms
    start_ms = start_us // 1000
    end_ms = end_us // 1000
    all_rows: list[list] = []
    since = start_ms
    while since < end_ms:
        params = {
            "symbol": "BTCUSDT",
            "interval": "1m",
            "startTime": since,
            "endTime": end_ms - 1,
            "limit": 1000,
        }
        resp = _SESSION.get(_VISION_API_KLINES, params=params, timeout=30)
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        for row in batch:
            if int(row[0]) < end_ms:
                # REST returns ms — convert col0 and col6 to us for consistency
                row_us = list(row)
                row_us[0] = str(int(row[0]) * 1000)
                row_us[6] = str(int(row[6]) * 1000)
                all_rows.append([str(x) for x in row_us])
        if len(batch) < 1000:
            break
        since = int(batch[-1][0]) + 1
        time.sleep(0.25)
        print(f"    fetched up to {datetime.fromtimestamp(since/1000, tz=timezone.utc)}", flush=True)
    return all_rows


def download_and_parse() -> pd.DataFrame:
    """
    Download Feb 2026 1m BTCUSDT data, cache zip locally, parse all 12 columns.
    Returns full-month DataFrame.
    """
    raw_rows: list[list[str]] | None = None

    # Try loading cached zip first
    if _ZIP_CACHE.exists():
        print(f"  Loading cached zip: {_ZIP_CACHE}")
        try:
            with zipfile.ZipFile(_ZIP_CACHE) as zf:
                csv_name = zf.namelist()[0]
                with zf.open(csv_name) as f:
                    reader = csv.reader(io.TextIOWrapper(f))
                    raw_rows = list(reader)
            print(f"  Cache hit: {len(raw_rows)} rows")
        except Exception as e:
            print(f"  Cache corrupted ({e}), re-downloading ...")
            raw_rows = None

    if raw_rows is None:
        # Try Vision zip
        raw_rows = _fetch_via_zip(2026, 2)
        if raw_rows is not None:
            # Save zip for re-use — need to download raw bytes for caching
            url = f"{_VISION_SPOT_BASE}/BTCUSDT/1m/BTCUSDT-1m-2026-02.zip"
            resp = _SESSION.get(url, timeout=60)
            if resp.ok:
                _ZIP_CACHE.write_bytes(resp.content)
                print(f"  Cached zip → {_ZIP_CACHE}")
        else:
            # REST fallback
            raw_rows = _fetch_via_rest(_FEB_START_US, _FEB_END_US)

    if not raw_rows:
        raise RuntimeError("No data fetched — check network and Binance availability.")

    # Parse rows — skip header/empty lines; validate open_time is a plausible us timestamp
    # Valid range: 2020-01-01 to 2030-01-01 in epoch microseconds
    records = []
    for row in raw_rows:
        if not row:
            continue
        col0 = str(row[0]).strip()
        if not col0.lstrip("-").isdigit():
            continue  # skip header row or non-numeric
        try:
            open_time = int(col0)
        except ValueError:
            continue
        if not (_TS_US_MIN <= open_time <= _TS_US_MAX):
            continue  # skip out-of-range timestamps
        try:
            records.append({
                "open_time": open_time,
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
                "close_time": int(row[6]),
                "quote_volume": float(row[7]),
                "n_trades": int(row[8]),
                "taker_buy_volume": float(row[9]),
                "taker_buy_quote_volume": float(row[10]),
            })
        except (IndexError, ValueError):
            continue

    df = pd.DataFrame(records)
    # Binance Vision 2026 files use microsecond timestamps
    df["datetime_utc"] = pd.to_datetime(df["open_time"], unit="us", utc=True)
    df = df.sort_values("open_time").reset_index(drop=True)
    print(f"  Parsed {len(df)} 1m bars  ({df['datetime_utc'].iloc[0]} → {df['datetime_utc'].iloc[-1]})")
    return df


# ---------------------------------------------------------------------------
# Anomaly metrics
# ---------------------------------------------------------------------------

def compute_anomaly_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute rolling z-scores and derived anomaly metrics.
    Rolling window starts from Feb 27 22:00 UTC to ensure Feb 28 00:00 has baseline.
    """
    # Filter to analysis range (includes warmup rows before 28th)
    mask = (df["open_time"] >= _ANALYSIS_START_US) & (df["open_time"] < _ANALYSIS_END_US)
    out = df[mask].copy().reset_index(drop=True)

    roll = out["volume"].rolling(_ROLLING_WINDOW, min_periods=10)
    out["vol_rolling_mean"] = roll.mean()
    out["vol_rolling_std"] = roll.std()
    out["vol_z"] = (out["volume"] - out["vol_rolling_mean"]) / (out["vol_rolling_std"] + 1e-9)

    roll_n = out["n_trades"].rolling(_ROLLING_WINDOW, min_periods=10)
    out["n_trades_rolling_mean"] = roll_n.mean()
    out["n_trades_rolling_std"] = roll_n.std()
    out["n_trades_z"] = (out["n_trades"] - out["n_trades_rolling_mean"]) / (out["n_trades_rolling_std"] + 1e-9)

    out["taker_buy_ratio"] = out["taker_buy_volume"] / (out["volume"] + 1e-9)
    out["price_drop_pct"] = (out["close"] - out["open"]) / (out["open"] + 1e-9) * 100
    out["avg_trade_size"] = out["volume"] / (out["n_trades"] + 1e-9)

    # Iceberg heuristic: many small trades (n_trades_z high) but volume not proportionally high
    out["iceberg_flag"] = (out["n_trades_z"] > 2) & (out["vol_z"] < out["n_trades_z"] * 0.5)

    return out


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def _red(s: str) -> str:
    if _COLORAMA:
        return f"{Fore.RED}{Style.BRIGHT}{s}{Style.RESET_ALL}"
    return f"*** {s} ***"


def _yellow(s: str) -> str:
    if _COLORAMA:
        return f"{Fore.YELLOW}{s}{Style.RESET_ALL}"
    return f"! {s}"


def _green(s: str) -> str:
    if _COLORAMA:
        return f"{Fore.GREEN}{s}{Style.RESET_ALL}"
    return s


def print_report(df: pd.DataFrame, window: str = "05:30–07:30") -> None:
    """Print minute-by-minute anomaly table for the analysis window."""
    # Parse window arg
    try:
        parts = window.replace("–", "-").split("-")
        start_h, start_m = map(int, parts[0].split(":"))
        end_h, end_m = map(int, parts[1].split(":"))
        window_start = datetime(2026, 2, 28, start_h, start_m, tzinfo=timezone.utc)
        window_end = datetime(2026, 2, 28, end_h, end_m, tzinfo=timezone.utc)
    except Exception:
        window_start = datetime(2026, 2, 28, 5, 30, tzinfo=timezone.utc)
        window_end = datetime(2026, 2, 28, 7, 30, tzinfo=timezone.utc)

    window_start_us = int(window_start.timestamp() * 1_000_000)
    window_end_us = int(window_end.timestamp() * 1_000_000)

    mask = (df["open_time"] >= window_start_us) & (df["open_time"] <= window_end_us)
    view = df[mask].copy()

    if view.empty:
        print(f"[!] No data in window {window}. Available range: {df['datetime_utc'].iloc[0]} → {df['datetime_utc'].iloc[-1]}")
        return

    header = (
        f"{'Time UTC':<10}  {'Close':>9}  {'Drop%':>7}  {'Vol':>12}  {'vol_z':>7}  "
        f"{'n_trades':>8}  {'n_z':>7}  {'tbuy%':>6}  {'avgSz':>8}  {'iceberg':>7}  {'anomaly'}"
    )
    print("\n" + "=" * 110)
    print(f"  BTC/USDT 1m Analysis — {window_start.strftime('%Y-%m-%d %H:%M')} to {window_end.strftime('%H:%M')} UTC")
    print("=" * 110)
    print(header)
    print("-" * 110)

    anomaly_count = 0
    for _, row in view.iterrows():
        t_str = pd.Timestamp(row["open_time"], unit="us", tz="UTC").strftime("%H:%M")
        vol_z = row["vol_z"]
        n_z = row["n_trades_z"]
        drop = row["price_drop_pct"]
        iceberg = row["iceberg_flag"]

        is_anomaly = abs(vol_z) > 3 or abs(n_z) > 3 or abs(drop) > 1.0
        if is_anomaly:
            anomaly_count += 1

        line = (
            f"{t_str:<10}  {row['close']:>9.1f}  {drop:>+7.3f}  {row['volume']:>12.2f}  "
            f"{vol_z:>+7.2f}  {int(row['n_trades']):>8,}  {n_z:>+7.2f}  "
            f"{row['taker_buy_ratio']:>6.3f}  {row['avg_trade_size']:>8.4f}  "
            f"{'YES' if iceberg else 'no':>7}  "
        )

        if is_anomaly and (abs(vol_z) > 3 or abs(drop) > 1.5):
            print(_red(line + "*** ANOMALY ***"))
        elif is_anomaly:
            print(_yellow(line + "* anomaly"))
        else:
            print(line)

    print("-" * 110)
    print(f"  Total anomaly bars in window: {anomaly_count}")

    # Highlight the crash minute
    crash_start_us = int(datetime(2026, 2, 28, 6, 0, tzinfo=timezone.utc).timestamp() * 1_000_000)
    crash_end_us = crash_start_us + 5 * 60 * 1_000_000  # 06:00–06:05
    crash_rows = df[(df["open_time"] >= crash_start_us) & (df["open_time"] < crash_end_us)]
    if not crash_rows.empty:
        print(f"\n  === Crash focus: 06:00–06:05 UTC ===")
        for _, row in crash_rows.iterrows():
            t_str = pd.Timestamp(row["open_time"], unit="us", tz="UTC").strftime("%H:%M")
            print(_red(
                f"  {t_str}  close={row['close']:.1f}  drop={row['price_drop_pct']:+.3f}%  "
                f"vol_z={row['vol_z']:+.2f}  n_z={row['n_trades_z']:+.2f}  "
                f"tbuy_ratio={row['taker_buy_ratio']:.3f}  iceberg={'YES' if row['iceberg_flag'] else 'no'}"
            ))


def print_summary_stats(df: pd.DataFrame) -> None:
    """Print peak anomaly statistics for the full Feb 28 period."""
    feb28_start = int(datetime(2026, 2, 28, tzinfo=timezone.utc).timestamp() * 1_000_000)
    feb28_end = int(datetime(2026, 3, 1, tzinfo=timezone.utc).timestamp() * 1_000_000)
    day = df[(df["open_time"] >= feb28_start) & (df["open_time"] < feb28_end)]

    if day.empty:
        print("[!] No Feb 28 data found.")
        return

    print("\n=== Feb 28 2026 Summary Statistics ===")
    print(f"  Total 1m bars : {len(day)}")
    print(f"  vol_z  max    : {day['vol_z'].max():.2f}  at {day.loc[day['vol_z'].idxmax(), 'datetime_utc']}")
    print(f"  vol_z  min    : {day['vol_z'].min():.2f}  at {day.loc[day['vol_z'].idxmin(), 'datetime_utc']}")
    print(f"  n_z    max    : {day['n_trades_z'].max():.2f}  at {day.loc[day['n_trades_z'].idxmax(), 'datetime_utc']}")
    print(f"  price_drop max: {day['price_drop_pct'].min():.3f}%  at {day.loc[day['price_drop_pct'].idxmin(), 'datetime_utc']}")
    print(f"  iceberg bars  : {day['iceberg_flag'].sum()}")

    top5 = day.nlargest(5, "vol_z")[["datetime_utc", "vol_z", "n_trades_z", "price_drop_pct", "taker_buy_ratio", "iceberg_flag"]]
    print("\n  Top 5 volume-spike bars (by vol_z):")
    for _, row in top5.iterrows():
        print(
            f"    {row['datetime_utc'].strftime('%H:%M UTC')}  "
            f"vol_z={row['vol_z']:+.2f}  n_z={row['n_trades_z']:+.2f}  "
            f"drop={row['price_drop_pct']:+.3f}%  tbuy={row['taker_buy_ratio']:.3f}  "
            f"iceberg={'YES' if row['iceberg_flag'] else 'no'}"
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Binance 1m anomaly analysis for BTC Feb 28 2026")
    parser.add_argument(
        "--window",
        default="05:30-07:30",
        help="UTC time window to display in report (default: 05:30-07:30)",
    )
    args = parser.parse_args()

    print("=== Binance 1m Volume Anomaly Analysis — BTC Feb 28, 2026 ===\n")

    # Step 1: Download / load data
    print("[1/3] Downloading/loading Feb 2026 1m data ...")
    df_raw = download_and_parse()

    # Step 2: Compute metrics
    print("\n[2/3] Computing anomaly metrics (rolling window = 60 bars) ...")
    df = compute_anomaly_metrics(df_raw)
    print(f"  Analysis range: {df['datetime_utc'].iloc[0]} → {df['datetime_utc'].iloc[-1]}")
    print(f"  Total bars in analysis range: {len(df)}")

    # Step 3: Save CSV (Feb 28 only)
    feb28_mask = df["open_time"] >= int(datetime(2026, 2, 28, tzinfo=timezone.utc).timestamp() * 1_000_000)
    df_feb28 = df[feb28_mask].copy()
    df_feb28.to_csv(_CSV_OUT, index=False)
    print(f"\n[3/3] Saved {len(df_feb28)} bars → {_CSV_OUT}")

    # Step 4: Print reports
    print_summary_stats(df)
    print_report(df, window=args.window)

    print(f"\nDone. Full CSV at: {_CSV_OUT}")


if __name__ == "__main__":
    main()
