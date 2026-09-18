#!/usr/bin/env python3
"""Data-availability check / fetch for the Merrill Investment Clock hypothesis.

Registered as ``merrill_clock_regime_rotation`` (see hypotheses/registry.jsonl).
The clock needs two classes of input:

1. **Macro series (FRED)** — the Growth and Inflation composites.  Growth Z is
   50% OECD CLI + 20% INDPRO + 15% *inverted* initial claims + 15% *inverted*
   unemployment; Inflation Z is 30% 5y breakeven + 25% core CPI + 20% PPI +
   15% CPI MoM + 10% capacity utilisation.  Each input is exponentially
   standardised over a 24-month span before weighting.

2. **Tradables (Nasdaq daily)** — the quadrant's three sector ETFs and its
   best-asset ETF, plus SPY as the benchmark.

This script is *data preparation only* — it never scores the hypothesis.  It
reports, for every required input, whether it is already cached locally and,
with ``--fetch``, tries to download what is missing.  It writes nothing to the
registry.

Usage::

    poetry run python scripts/check_merrill_clock_data.py            # report
    poetry run python scripts/check_merrill_clock_data.py --fetch    # fetch gaps
    poetry run python scripts/check_merrill_clock_data.py --fetch --only-fred
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.data.fred_store import FredSeries  # noqa: E402
from src.data.nasdaq_store import NasdaqDailyStore  # noqa: E402

# ---------------------------------------------------------------------------
# Required inputs (single source of truth, mirrored into the registry entry)
# ---------------------------------------------------------------------------

#: FRED id -> (weight in composite, transform, frequency, human label)
FRED_GROWTH: dict[str, tuple[float, str, str, str]] = {
    "USALOLITONOSTSAM": (0.50, "level", "monthly", "OECD US Composite Leading Indicator (amplitude adj.)"),
    "INDPRO": (0.20, "level", "monthly", "Industrial Production: Total Index"),
    "ICSA": (0.15, "invert", "weekly", "Initial Claims (inverted)"),
    "UNRATE": (0.15, "invert", "monthly", "Unemployment Rate (inverted)"),
}
FRED_INFLATION: dict[str, tuple[float, str, str, str]] = {
    "T5YIE": (0.30, "level", "daily", "5-Year Breakeven Inflation Rate"),
    "CPILFESL": (0.25, "yoy", "monthly", "Core CPI (YoY)"),
    "PPIFIS": (0.20, "yoy", "monthly", "PPI: Final Demand (YoY)"),
    "CPIAUCSL": (0.15, "mom", "monthly", "CPI All Items (MoM)"),
    "TCU": (0.10, "level", "monthly", "Capacity Utilisation"),
}
FRED_REQUIRED = {**FRED_GROWTH, **FRED_INFLATION}

#: Quadrant -> (growth sign, inflation sign, best-asset ETF, 3 sector ETFs)
#: The mapping is the conventional published Merrill-clock mapping and is
#: frozen in the registry entry; it is an explicit, declarable assumption.
QUADRANTS: dict[str, dict] = {
    "reflation":  {"growth": "falling", "inflation": "falling", "best_asset": "TLT",
                   "sectors": ["XLU", "XLP", "XLV"]},
    "recovery":   {"growth": "rising",  "inflation": "falling", "best_asset": "SPY",
                   "sectors": ["XLK", "XLY", "XLF"]},
    "overheat":   {"growth": "rising",  "inflation": "rising",  "best_asset": "DBC",
                   "sectors": ["XLE", "XLB", "XLI"]},
    "stagflation": {"growth": "falling", "inflation": "rising", "best_asset": "BIL",
                    "sectors": ["XLP", "XLV", "XLU"]},
}
BENCHMARK_ETF = "SPY"
ETF_REQUIRED = sorted(
    {BENCHMARK_ETF}
    | {q["best_asset"] for q in QUADRANTS.values()}
    | {s for q in QUADRANTS.values() for s in q["sectors"]}
)


def check_fred(fetch: bool) -> list[tuple[str, str, str]]:
    store = FredSeries()
    rows: list[tuple[str, str, str]] = []
    for sid, (weight, transform, freq, label) in FRED_REQUIRED.items():
        path = store._cache / f"{sid.lower()}.csv"  # noqa: SLF001 — cache path is the point
        cached = path.exists()
        status, span = "cached" if cached else "MISSING", ""
        if fetch and not cached:
            try:
                s = store.get(sid)
                status = "fetched"
                span = f"{s.index[0].date()}..{s.index[-1].date()} ({len(s)})"
            except Exception as exc:  # noqa: BLE001 — report, don't crash
                status = f"fetch-failed: {type(exc).__name__}"
        elif cached:
            try:
                s = store.get(sid)
                span = f"{s.index[0].date()}..{s.index[-1].date()} ({len(s)})"
            except Exception:  # noqa: BLE001
                span = "(unreadable cache)"
        rows.append((sid, f"w={weight:.2f} {transform:6s} {freq:7s} {label}", f"{status:>22s} {span}"))
    return rows


def check_etfs(fetch: bool) -> list[tuple[str, str, str]]:
    store = NasdaqDailyStore()
    rows: list[tuple[str, str, str]] = []
    for sym in ETF_REQUIRED:
        role = []
        if sym == BENCHMARK_ETF:
            role.append("benchmark")
        for q, spec in QUADRANTS.items():
            if spec["best_asset"] == sym:
                role.append(f"{q}:best")
            if sym in spec["sectors"]:
                role.append(f"{q}:sector")
        first, last, n = store.get_coverage(sym)
        status = "cached" if n else "MISSING"
        if fetch and not n:
            try:
                written = store.fetch_and_store(sym)
                first, last, n = store.get_coverage(sym)
                status = f"fetched({written})"
            except Exception as exc:  # noqa: BLE001
                status = f"fetch-failed: {type(exc).__name__}"
        rows.append((sym, ",".join(role), f"{status:>15s} {first}..{last} n={n}"))
    return rows


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--fetch", action="store_true", help="download missing inputs")
    p.add_argument("--only-fred", action="store_true")
    p.add_argument("--only-etf", action="store_true")
    args = p.parse_args(argv)

    gaps = 0

    def report(title: str, rows: list[tuple[str, str, str]]) -> None:
        nonlocal gaps
        print(f"\n{title}")
        print("-" * len(title))
        for key, meta, status in rows:
            print(f"  {key:18s} {meta:52s} {status}")
            if "MISSING" in status or "failed" in status:
                gaps += 1

    if not args.only_etf:
        report("FRED macro inputs", check_fred(args.fetch))
    if not args.only_fred:
        report("Nasdaq tradables", check_etfs(args.fetch))

    print(f"\ngaps: {gaps}" + ("" if args.fetch else "  (re-run with --fetch to fill)"))
    return 0 if gaps == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
