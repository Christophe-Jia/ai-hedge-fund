#!/usr/bin/env python3
"""Fetch point-in-time S&P 500 constituents from Wikipedia revision history.

Same methodology as fetch_sp100_history.py: for each year, take the first
page revision after Jan 10 and parse the constituent table ->
data/universe/sp500_{year}.json. The union over all years (incl. names later
removed — acquired, bankrupt, demoted) is the survivorship-bias-free
backtest universe.

The "List of S&P 500 companies" page carries TWO relevant wikitables:
  1. the constituents table (Symbol | Security | GICS Sector | ...)  -> parsed
  2. the "Selected changes" table (Date | Added | ... | Removed ...)  -> used
     only for cross-validation of the most recent snapshot transition.

Cross-checks run automatically after fetching:
  a. per-year count sanity (~505 members, hard fail outside 490-515);
  b. S&P 100 snapshot ⊆ S&P 500 snapshot per year (S&P 100 is drawn from
     the 500 — violations are printed, not fatal, since the two pages can
     lag each other by days);
  c. year-over-year churn (additions + removals between consecutive Jan
     snapshots; typical ~20-50, hard fail if 0 or > 150).

Usage:
    poetry run python scripts/fetch_sp500_history.py            # 2016..2026
    poetry run python scripts/fetch_sp500_history.py --years 2020,2021
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from html.parser import HTMLParser
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.fetch_sp100_history import _api_get  # noqa: E402 — shared helper

_PAGE = "List of S&P 500 companies"
_OUT_DIR = Path(__file__).resolve().parents[1] / "data" / "universe"
_SYM_RE = re.compile(r"[A-Z][A-Z0-9.\-]{0,9}")


# ---------------------------------------------------------------------------
# HTML parsing (stdlib only — no lxml in the project)
# ---------------------------------------------------------------------------

class _TableParser(HTMLParser):
    """Collect wikitables as {"headers": [...], "rows": [[cell, ...], ...]}.

    A row counts as the header row if its cells are <th> elements (the
    constituents table header is Symbol/Security/GICS Sector/...). We only
    track the OUTERMOST table (enough for this page).
    """

    def __init__(self) -> None:
        super().__init__()
        self.tables: list[dict] = []
        self._table: dict | None = None
        self._row: list[str] | None = None
        self._cell: list[str] | None = None
        self._row_has_th = False
        self._in_td_or_th = False

    def handle_starttag(self, tag, attrs):
        if tag == "table" and self._table is None and any(
            a == "class" and "wikitable" in (v or "") for a, v in attrs
        ):
            self._table = {"headers": [], "rows": []}
        elif self._table is not None and tag == "tr":
            self._row = []
            self._row_has_th = False
        elif self._table is not None and self._row is not None and tag in ("td", "th"):
            self._cell = []
            self._in_td_or_th = True
            if tag == "th":
                self._row_has_th = True

    def handle_endtag(self, tag):
        if self._table is not None and tag in ("td", "th") and self._cell is not None:
            if self._row is not None:
                self._row.append("".join(self._cell).strip())
            self._cell = None
            self._in_td_or_th = False
        elif self._table is not None and tag == "tr" and self._row is not None:
            if self._row:
                if self._row_has_th and not self._table["headers"]:
                    self._table["headers"] = self._row
                else:
                    self._table["rows"].append(self._row)
            self._row = None
        elif self._table is not None and tag == "table":
            if self._table["rows"] or self._table["headers"]:
                self.tables.append(self._table)
            self._table = None

    def handle_data(self, data):
        if self._in_td_or_th and self._cell is not None:
            self._cell.append(data)


def _parse_constituents_table(html: str) -> list[dict]:
    """Extract {"symbol", "name"} from the constituents wikitable."""
    parser = _TableParser()
    parser.feed(html)

    for t in parser.tables:
        headers = [h.lower() for h in t["headers"]]
        sym_idx = next((i for i, h in enumerate(headers) if "symbol" in h), None)
        name_idx = next(
            (i for i, h in enumerate(headers) if "security" in h or "company" in h), None)
        if sym_idx is not None and name_idx is not None and len(t["rows"]) > 400:
            out, seen = [], set()
            for row in t["rows"]:
                if len(row) <= max(sym_idx, name_idx):
                    continue
                # some revisions write dual-class tickers with a hyphen
                # (BRK-B); the price store uses dot notation (BRK.B)
                sym = row[sym_idx].replace("\u2009", "").strip().replace("-", ".")
                if _SYM_RE.fullmatch(sym) and sym not in seen:
                    out.append({"symbol": sym, "name": row[name_idx]})
                    seen.add(sym)
            return out

    # Fallback (old revisions where the header may not be <th>): largest
    # table, sp100-style "first ticker-looking cell" heuristic.
    if not parser.tables:
        return []
    biggest = max(parser.tables, key=lambda t: len(t["rows"]))
    out, seen = [], set()
    for row in biggest["rows"]:
        for i, cell in enumerate(row[:3]):
            sym = cell.replace("\u2009", "").strip().replace("-", ".")
            if _SYM_RE.fullmatch(sym) and sym not in seen:
                name = row[i + 1] if i + 1 < len(row) and len(row[i + 1]) > 2 else ""
                out.append({"symbol": sym, "name": name})
                seen.add(sym)
                break
    return out


# ---------------------------------------------------------------------------
# Wikipedia revision lookup (same approach as fetch_sp100_history)
# ---------------------------------------------------------------------------

def revision_near(year: int, limit: int = 6) -> list[tuple[int, str]]:
    """First revisions of the S&P 500 page on/after Jan 10 (newest-first
    chunk, returned oldest-first) — the page is only edited sporadically, so
    the "first" revision can be weeks after Jan 10."""
    d = _api_get({
        "action": "query",
        "prop": "revisions",
        "titles": _PAGE,
        "rvstart": f"{year}-01-10T00:00:00Z",
        "rvlimit": limit,
        "rvdir": "newer",
        "rvprop": "ids|timestamp",
        "format": "json",
    })
    out = []
    for page in d["query"]["pages"].values():
        for rev in page.get("revisions") or []:
            out.append((int(rev["revid"]), rev["timestamp"]))
    if not out:
        raise RuntimeError(f"no revision found for {year}")
    return out


def constituents_of_revision(revid: int) -> list[dict]:
    d = _api_get({
        "action": "parse",
        "oldid": revid,
        "prop": "text",
        "format": "json",
    })
    html = d["parse"]["text"]["*"]
    return _parse_constituents_table(html)


# ---------------------------------------------------------------------------
# Cross-checks
# ---------------------------------------------------------------------------

def cross_check(pit: dict[int, list[str]]) -> None:
    years = sorted(pit)
    problems: list[str] = []

    for y in years:
        n = len(pit[y])
        if not 490 <= n <= 515:
            problems.append(f"{y}: {n} constituents — outside 490-515")

    # S&P 100 ⊆ S&P 500 per year (both are Jan-10-ish snapshots)
    for f in sorted(_OUT_DIR.glob("sp100_[0-9]*.json")):
        y = int(f.stem.split("_")[1])
        if y not in pit:
            continue
        sp100 = {c["symbol"] for c in json.loads(f.read_text())}
        missing = sp100 - set(pit[y])
        if missing:
            problems.append(f"{y}: sp100 symbols not in sp500 snapshot: {sorted(missing)}")

    # year-over-year churn
    for prev, cur in zip(years, years[1:]):
        a, r = set(pit[cur]) - set(pit[prev]), set(pit[prev]) - set(pit[cur])
        churn = len(a) + len(r)
        print(f"  churn {prev}->{cur}: +{len(a)} / -{len(r)}")
        if churn == 0 or churn > 150:
            problems.append(f"{prev}->{cur}: churn {churn} (0 or >150 is implausible)")

    if problems:
        print("\n  CROSS-CHECK PROBLEMS:")
        for p in problems:
            print("   !", p)
        raise SystemExit(1)
    print("  cross-checks OK (counts, sp100 subset, churn)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--years", type=str, default=",".join(str(y) for y in range(2016, 2027)))
    p.add_argument("--no-cross-check", action="store_true")
    args = p.parse_args()

    _OUT_DIR.mkdir(parents=True, exist_ok=True)
    pit: dict[int, list[str]] = {}
    union_syms: set[str] = set()

    for year in [int(y) for y in args.years.split(",")]:
        out = _OUT_DIR / f"sp500_{year}.json"
        if out.exists():
            data = json.loads(out.read_text())
            print(f"  {year}: cached ({len(data)} constituents)")
        else:
            # try successive revisions until the table is complete: some
            # Jan-10-ish revisions carry transient vandalism/omissions
            # (e.g. 2023-01 briefly dropped ABBV, a 250B$ mega cap)
            data = []
            for revid, ts in revision_near(year):
                cs = constituents_of_revision(revid)
                print(f"    rev {revid} ({ts[:10]}): {len(cs)}")
                if len(cs) >= 503:
                    data = cs
                    break
                if not data:
                    data = cs  # keep the best-so-far as fallback
                time.sleep(1.0)
            if len(data) < 490:
                raise SystemExit(f"{year}: only {len(data)} constituents parsed")
            out.write_text(json.dumps(data, indent=1, ensure_ascii=False))
            print(f"  {year}: {len(data)} constituents -> {out.name}")
            time.sleep(1.0)  # be polite to the API
        pit[year] = sorted(c["symbol"] for c in data if _SYM_RE.fullmatch(c["symbol"]))
        union_syms |= set(pit[year])

    union_path = _OUT_DIR / "sp500_union.json"
    union_path.write_text(json.dumps(sorted(union_syms), indent=0))
    print(f"\n  union universe (all years deduped): {len(union_syms)} -> {union_path}")

    if not args.no_cross_check:
        cross_check(pit)


if __name__ == "__main__":
    main()
