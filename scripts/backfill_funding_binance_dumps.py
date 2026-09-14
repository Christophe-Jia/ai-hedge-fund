"""
Backfill perpetual funding rates from Binance official data dumps.

Gate.io's funding_rate endpoint only serves the most recent 180 days, so
pre-2023 history cannot come from Gate. Binance publishes complete monthly
funding-rate CSV dumps on its public S3 mirror:

    https://data.binance.vision/data/futures/um/monthly/fundingRate/BTCUSDT/

Each zip contains a CSV with columns:
    calc_time, funding_interval_hours, last_funding_rate

`calc_time` is the settlement timestamp in milliseconds (with small
sub-second jitter), matching the semantics of the `ts` column in
funding_rates. `last_funding_rate` is a decimal (0.0001 = 0.01%).

IMPORTANT: rates from Binance differ numerically from Gate.io rates
(different premium-index constructions), so mixing sources creates a
seam in the data. The seam location is reported by this script.

Only rows with ts < the earliest already-stored ts for the symbol are
inserted, so existing (Gate) data is never touched and no gap is left at
the seam.

Example:
    poetry run python scripts/backfill_funding_binance_dumps.py \
        --binance-symbol BTCUSDT --symbol BTC/USDT:USDT \
        --start 2021-01 --end 2023-03
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import sys
import tempfile
import zipfile
from datetime import datetime, timezone

import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.data.funding_rates import FundingRateStore

BASE_URL = "https://data.binance.vision/data/futures/um/monthly/fundingRate"


def month_range(start: str, end: str) -> list[str]:
    """Expand '2021-01' .. '2023-03' into a list of 'YYYY-MM' strings."""
    sy, sm = int(start[:4]), int(start[5:7])
    ey, em = int(end[:4]), int(end[5:7])
    if (sy, sm) > (ey, em):
        raise SystemExit(f"start {start} is after end {end}")
    months = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        months.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return months


def fetch_month(binance_symbol: str, month: str, session: requests.Session) -> list[tuple[int, float]]:
    """Download and parse one monthly funding-rate zip. Returns [(ts_ms, rate)]."""
    url = f"{BASE_URL}/{binance_symbol}/{binance_symbol}-fundingRate-{month}.zip"
    resp = session.get(url, timeout=60)
    resp.raise_for_status()

    rows: list[tuple[int, float]] = []
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        names = [n for n in zf.namelist() if n.endswith(".csv")]
        if not names:
            raise SystemExit(f"{url}: zip contains no CSV")
        with zf.open(names[0]) as f:
            text = io.TextIOWrapper(f, encoding="utf-8")
            for rec in csv.DictReader(text):
                ts = int(rec["calc_time"])
                rate = float(rec["last_funding_rate"])
                interval = int(rec.get("funding_interval_hours") or 8)
                if interval != 8:
                    print(f"  [WARN] {month}: non-8h interval {interval}h at ts={ts}")
                rows.append((ts, rate))
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--binance-symbol", default="BTCUSDT", help="Binance symbol (default: BTCUSDT)")
    parser.add_argument("--symbol", default="BTC/USDT:USDT", help="DB symbol (default: BTC/USDT:USDT)")
    parser.add_argument("--start", required=True, help="First month, YYYY-MM (e.g. 2021-01)")
    parser.add_argument("--end", required=True, help="Last month, YYYY-MM inclusive (e.g. 2023-03)")
    parser.add_argument("--db", default=None, help="SQLite DB path (default: data/btc_history.db)")
    parser.add_argument("--dry-run", action="store_true", help="Download and parse but do not write to DB")
    args = parser.parse_args()

    months = month_range(args.start, args.end)
    db_path_kwarg = {"db_path": args.db} if args.db else {}
    store = FundingRateStore(**db_path_kwarg, exchange_id="binance")

    # Seam: never write rows at/after the earliest already-stored ts, so
    # existing data is preserved and the join is gap-free.
    with store._engine.connect() as conn:  # noqa: SLF001 - small script
        import sqlalchemy as sa

        row = conn.execute(
            sa.text("SELECT MIN(ts) FROM funding_rates WHERE symbol = :s"),
            {"s": args.symbol},
        ).fetchone()
    seam_ts = int(row[0]) if row and row[0] is not None else None
    if seam_ts is not None:
        print(f"Existing earliest ts: {seam_ts} "
              f"({datetime.fromtimestamp(seam_ts / 1000, tz=timezone.utc)} UTC) — source seam")
    else:
        print("No existing data for symbol — no seam constraint.")

    start_ms = int(datetime.strptime(args.start, "%Y-%m").replace(tzinfo=timezone.utc).timestamp() * 1000)

    session = requests.Session()
    all_rows: dict[int, float] = {}
    for month in months:
        rows = fetch_month(args.binance_symbol, month, session)
        kept = [r for r in rows if r[0] >= start_ms and (seam_ts is None or r[0] < seam_ts)]
        n_before = len(all_rows)
        all_rows.update(dict(kept))
        skipped = len(rows) - len(kept)
        print(
            f"  {month}: {len(rows)} rows in dump, {len(kept)} in backfill window "
            f"({skipped} skipped: before start/at-or-after seam), "
            f"+{len(all_rows) - n_before} new unique ts"
        )
        if rows:
            print(
                f"    dump range: {datetime.fromtimestamp(rows[0][0] / 1000, tz=timezone.utc)} "
                f"→ {datetime.fromtimestamp(rows[-1][0] / 1000, tz=timezone.utc)}"
            )

    final_rows = sorted(all_rows.items())
    if not final_rows:
        print("Nothing to write.")
        return

    print(
        f"\nTotal unique rows to write: {len(final_rows)}  "
        f"({datetime.fromtimestamp(final_rows[0][0] / 1000, tz=timezone.utc)} "
        f"→ {datetime.fromtimestamp(final_rows[-1][0] / 1000, tz=timezone.utc)})"
    )
    if args.dry_run:
        print("(dry-run) Not writing to DB.")
        return

    n = store.upsert_rates(args.symbol, final_rows)
    print(f"Wrote {n} rows to funding_rates.")


if __name__ == "__main__":
    main()
