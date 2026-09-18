#!/usr/bin/env python3
"""Extract dated S&P 500 index additions / removals from Wikipedia.

The "List of S&P 500 companies" page carries a SECOND wikitable — "Selected
changes to the list of S&P 500 components" — whose rows are dated index
events.  Its ``Date`` column is the **effective** date of the change (the day
the constituent set changed), which is what an index-inclusion event study
needs; the constituent snapshots in ``data/universe/sp500_{year}.json`` only
give annual membership and cannot date an event.

Output -> ``data/universe/sp500_changes.json``::

    [{"date": "2025-03-24", "date_raw": "March 24, 2025",
      "date_precision": "day",
      "added_ticker": "DASH", "added_name": "DoorDash",
      "removed_ticker": null, "removed_name": null, "reason": "..."},
     ...]

One swap therefore yields two records (one addition, one removal).

Usage::

    poetry run python scripts/fetch_sp500_changes.py              # fetch + parse
    poetry run python scripts/fetch_sp500_changes.py --probe      # dump table shapes
    poetry run python scripts/fetch_sp500_changes.py --self-test  # offline parser check
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.fetch_sp100_history import _api_get  # noqa: E402 — shared helper
from scripts.fetch_sp500_history import _TableParser  # noqa: E402 — shared parser

_PAGE = "List of S&P 500 companies"
_OUT_DIR = Path(__file__).resolve().parents[1] / "data" / "universe"
_HTML_CACHE = _OUT_DIR / "sp500_changes.html"
_OUT_JSON = _OUT_DIR / "sp500_changes.json"

# Ticker: uppercase, up to 10 chars, allows dot (BRK.B) and hyphen.
_SYM_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")
# A ticker sitting inside a parenthetical or at the start of a cell.
_SYM_IN_CELL_RE = re.compile(r"\(([A-Z][A-Z0-9.\-]{0,9})\)|^\s*([A-Z][A-Z0-9.\-]{0,9})\b")

_DATE_FORMATS_DAY = ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%m/%d/%Y", "%d %B %Y", "%d %b %Y")
_DATE_FORMATS_COARSE = (("%B %Y", "month"), ("%b %Y", "month"), ("%Y", "year"))


def _norm(text: str) -> str:
    return re.sub(r"[^a-z]", "", (text or "").lower())


def _clean_cell(raw: str) -> str:
    """Strip Wikipedia footnote markers and bracketed refs from a table cell."""
    s = re.sub(r"\[\s*\d+\s*\]", "", raw or "")
    return s.replace("\u2009", "").replace("\xa0", " ").strip()


def _extract_ticker(cell: str) -> str | None:
    """Pull a ticker out of cells like ``DASH``, ``DASH (DoorDash)`` or
    ``DoorDash (DASH)``; return None when the cell is not ticker-bearing."""
    cell = _clean_cell(cell)
    if not cell:
        return None
    for m in _SYM_IN_CELL_RE.finditer(cell):
        tok = m.group(1) or m.group(2)
        if tok and _SYM_RE.match(tok):
            return tok.replace("-", ".")
    return None


def _parse_date(raw: str) -> tuple[str | None, str | None]:
    """(iso_date, precision) — precision is 'day' | 'month' | 'year' | None."""
    s = _clean_cell(raw)
    if not s:
        return None, None
    s = s.replace(",", ", ").replace("  ", " ").strip()
    for fmt in _DATE_FORMATS_DAY:
        try:
            return datetime.strptime(s, fmt).date().isoformat(), "day"
        except ValueError:
            continue
    for fmt, prec in _DATE_FORMATS_COARSE:
        try:
            dt = datetime.strptime(s, fmt)
            if prec == "month":
                return dt.date().replace(day=1).isoformat(), "month"
            return dt.date().replace(month=1, day=1).isoformat(), "year"
        except ValueError:
            continue
    # bare ISO-ish "2025-03" / "March 2025" already covered; give up gracefully
    m = re.search(r"(\d{4})-(\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-01", "month"
    return None, None


# ---------------------------------------------------------------------------
# Header mapping
# ---------------------------------------------------------------------------

def _find_changes_table(tables: list[dict]) -> dict | None:
    """Pick the table whose headers look like the 'Selected changes' table."""
    best, best_rows = None, 0
    for t in tables:
        headers = [_norm(h) for h in t.get("headers", [])]
        has_date = any("date" in h for h in headers)
        has_change = any(("added" in h or "removed" in h or "add" in h or "remov" in h) for h in headers)
        if has_date and has_change and len(t.get("rows", [])) > best_rows:
            best, best_rows = t, len(t["rows"])
    return best


def _map_columns(headers: list[str]) -> dict[str, int | None]:
    h = [_norm(x) for x in headers]
    col: dict[str, int | None] = {
        "date": None, "added_ticker": None, "added_name": None,
        "removed_ticker": None, "removed_name": None, "reason": None,
    }
    for i, name in enumerate(h):
        if col["date"] is None and "date" in name:
            col["date"] = i
        elif "reason" in name:
            col["reason"] = i
        elif "added" in name or name in {"add", "addition"}:
            if "name" in name or "company" in name or "security" in name:
                col["added_name"] = col["added_name"] if col["added_name"] is not None else i
            else:
                col["added_ticker"] = col["added_ticker"] if col["added_ticker"] is not None else i
        elif "removed" in name or "deleted" in name or name in {"remove", "removal"}:
            if "name" in name or "company" in name or "security" in name:
                col["removed_name"] = i
            else:
                col["removed_ticker"] = i
    return col


def _map_columns_positional(width: int) -> dict[str, int | None]:
    """Fallback when headers are merged by rowspan (common on the live page)."""
    if width >= 6:
        return {"date": 0, "added_ticker": 1, "added_name": 2,
                "removed_ticker": 3, "removed_name": 4, "reason": 5}
    if width == 5:
        return {"date": 0, "added_ticker": 1, "added_name": None,
                "removed_ticker": 2, "removed_name": None, "reason": 4}
    if width == 4:
        return {"date": 0, "added_ticker": 1, "added_name": None,
                "removed_ticker": 2, "removed_name": None, "reason": 3}
    return {"date": 0, "added_ticker": 1, "added_name": None,
            "removed_ticker": 2, "removed_name": None, "reason": None}


# ---------------------------------------------------------------------------
# Parse
# ---------------------------------------------------------------------------

def parse_changes(html: str) -> list[dict]:
    parser = _TableParser()
    parser.feed(html)
    table = _find_changes_table(parser.tables)
    if table is None:
        return []
    headers = table["headers"]
    col = _map_columns(headers)
    if col["date"] is None:
        return []
    positional = None

    out: list[dict] = []
    for row in table["rows"]:
        if not isinstance(row, list) or len(row) <= (col["date"] or 0):
            continue

        def cell(key: str) -> str:
            i = col[key]
            if i is None or i >= len(row):
                return ""
            return row[i]

        date_iso, precision = _parse_date(cell("date"))
        if date_iso is None:
            # header repeats / notes rows
            continue

        added_ticker = _extract_ticker(cell("added_ticker")) if col["added_ticker"] is not None else None
        removed_ticker = _extract_ticker(cell("removed_ticker")) if col["removed_ticker"] is not None else None

        # If the header map put both sides in one cell (or rowspan merged the
        # headers), fall back to positional tickers and re-scan.
        if added_ticker is None and removed_ticker is None:
            if positional is None:
                positional = _map_columns_positional(len(row))
            added_ticker = _extract_ticker(row[positional["added_ticker"]]) if positional["added_ticker"] is not None and positional["added_ticker"] < len(row) else None
            removed_ticker = _extract_ticker(row[positional["removed_ticker"]]) if positional["removed_ticker"] is not None and positional["removed_ticker"] < len(row) else None
            added_name = cell("added_name")
            removed_name = cell("removed_name")
        else:
            added_name = _clean_cell(cell("added_name"))
            removed_name = _clean_cell(cell("removed_name"))

        reason = _clean_cell(cell("reason"))

        if added_ticker:
            out.append({
                "date": date_iso, "date_raw": _clean_cell(cell("date")),
                "date_precision": precision,
                "added_ticker": added_ticker, "added_name": added_name or None,
                "removed_ticker": None, "removed_name": None,
                "reason": reason,
            })
        if removed_ticker:
            out.append({
                "date": date_iso, "date_raw": _clean_cell(cell("date")),
                "date_precision": precision,
                "added_ticker": None, "added_name": None,
                "removed_ticker": removed_ticker, "removed_name": removed_name or None,
                "reason": reason,
            })
    out.sort(key=lambda r: (r["date"], r["added_ticker"] or r["removed_ticker"] or ""))
    return out


# ---------------------------------------------------------------------------
# Fetch / probe / self-test
# ---------------------------------------------------------------------------

def _fetch_page_html() -> str:
    data = _api_get({
        "action": "parse",
        "page": _PAGE,
        "prop": "text",
        "format": "json",
    })
    return data["parse"]["text"]["*"]


def _probe() -> None:
    html = _fetch_page_html()
    parser = _TableParser()
    parser.feed(html)
    print(f"{len(parser.tables)} wikitable(s) found on '{_PAGE}'")
    for i, t in enumerate(parser.tables):
        print(f"  [{i}] headers={t['headers']!r} rows={len(t['rows'])}")
        if t["rows"]:
            print(f"        first row={t['rows'][0]!r}")


_SELF_TEST_HTML = """
<table class="wikitable sortable">
<tr><th>Date</th><th>Added</th><th>Removed</th><th>Reason</th></tr>
<tr><td>March 24, 2025</td><td>DASH (DoorDash)</td><td>FMC (FMC Corporation)</td><td>Market cap change</td></tr>
<tr><td>March 3, 2025</td><td>TPG (TPG Inc.)</td><td></td><td>Addition</td></tr>
<tr><td>2025-02-14</td><td></td><td>XYZ</td><td>Acquired</td></tr>
<tr><td>February 2025</td><td>ABC</td><td></td><td>Coarse date row</td></tr>
</table>
"""


def _self_test() -> int:
    recs = parse_changes(_SELF_TEST_HTML)
    for r in recs:
        print(r)
    # a swap yields two records (DASH added + FMC removed) => 5 total
    assert len(recs) == 5, f"expected 5 records, got {len(recs)}"
    by_key = {(r["date"], r["added_ticker"] or r["removed_ticker"]): r for r in recs}
    assert by_key[("2025-02-14", "XYZ")]["removed_ticker"] == "XYZ", recs
    assert ("2025-03-24", "DASH") in by_key, by_key.keys()
    assert ("2025-03-24", "FMC") in by_key, by_key.keys()
    assert by_key[("2025-03-24", "FMC")]["reason"] == "Market cap change"
    assert by_key[("2025-02-01", "ABC")]["date_precision"] == "month"
    print("self-test OK")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", default=str(_OUT_JSON))
    p.add_argument("--probe", action="store_true", help="print wikitable shapes and exit")
    p.add_argument("--self-test", action="store_true", help="offline parser check")
    p.add_argument("--refresh", action="store_true", help="ignore the cached HTML")
    args = p.parse_args(argv)

    if args.self_test:
        return _self_test()
    if args.probe:
        _probe()
        return 0

    if _HTML_CACHE.exists() and not args.refresh:
        html = _HTML_CACHE.read_text(encoding="utf-8")
        print(f"using cached HTML {_HTML_CACHE}")
    else:
        html = _fetch_page_html()
        _OUT_DIR.mkdir(parents=True, exist_ok=True)
        _HTML_CACHE.write_text(html, encoding="utf-8")
        print(f"fetched page HTML -> {_HTML_CACHE}")

    records = parse_changes(html)
    if not records:
        print("ERROR: no change records parsed — table shape may have changed; "
              "run with --probe to inspect", file=sys.stderr)
        return 1

    _OUT_DIR.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(records, indent=1, ensure_ascii=False), encoding="utf-8")

    adds = [r for r in records if r["added_ticker"]]
    rems = [r for r in records if r["removed_ticker"]]
    years = sorted({r["date"][:4] for r in records})
    day_prec = sum(1 for r in records if r["date_precision"] == "day")
    print(f"{len(records)} records ({len(adds)} additions, {len(rems)} removals) -> {args.out}")
    print(f"  years covered: {years[0]}..{years[-1]} ({len(years)} years)")
    print(f"  day-precision: {day_prec}/{len(records)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
