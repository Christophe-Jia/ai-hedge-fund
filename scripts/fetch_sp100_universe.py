#!/usr/bin/env python3
"""Fetch the S&P 100 constituent list from Wikipedia -> data/universe/sp100.json.

Uses only the stdlib (html.parser) — no lxml dependency. Output format:
[{"symbol": "AAPL", "name": "Apple Inc."}, ...]

Usage:
    poetry run python scripts/fetch_sp100_universe.py
"""

from __future__ import annotations

import json
import re
import sys
import urllib.request
from html.parser import HTMLParser
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

_URL = "https://en.wikipedia.org/wiki/S%26P_100"
_OUT = Path(__file__).resolve().parents[1] / "data" / "universe" / "sp100.json"


class _ConstituentParser(HTMLParser):
    """Extract (Symbol, Name) pairs from the constituents wikitable."""

    def __init__(self) -> None:
        super().__init__()
        self.rows: list[list[str]] = []
        self._row: list[str] | None = None
        self._cell: list[str] | None = None
        self._in_table = 0

    def handle_starttag(self, tag, attrs):
        if tag == "table" and any(a == "class" and "wikitable" in (v or "") for a, v in attrs):
            self._in_table += 1
        elif self._in_table and tag == "tr":
            self._row = []
        elif self._in_table and self._row is not None and tag == "td":
            self._cell = []

    def handle_endtag(self, tag):
        if self._in_table and tag == "table":
            self._in_table -= 1
        elif tag == "td" and self._cell is not None and self._row is not None:
            self._row.append("".join(self._cell).strip())
            self._cell = None
        elif tag == "tr" and self._row is not None:
            if self._row:
                self.rows.append(self._row)
            self._row = None

    def handle_data(self, data):
        if self._cell is not None:
            self._cell.append(data)


def parse_constituents(html: str) -> list[dict]:
    parser = _ConstituentParser()
    parser.feed(html)

    out: list[dict] = []
    seen: set[str] = set()
    for row in parser.rows:
        # Symbol is typically the first or second cell; find the first cell
        # that looks like a ticker (1-5 uppercase letters, maybe dots).
        for i, cell in enumerate(row[:3]):
            sym = cell.replace("\u2009", "").strip()
            if re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,9}", sym) and sym not in seen:
                # Name: the cell immediately after the symbol
                name = row[i + 1] if i + 1 < len(row) and len(row[i + 1]) > 2 else ""
                out.append({"symbol": sym, "name": name})
                seen.add(sym)
                break
    return out


def _fetch_html() -> str:
    """Fetch the Wikipedia page, falling back to a previously cached copy.

    Wikipedia throttles plain urllib from some networks (403 Too Many Reqs)
    while curl passes; we try both, then any cached copy.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        req = urllib.request.Request(_URL, headers=headers)
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except Exception:  # noqa: BLE001 — fall through to curl / cache
        pass

    import subprocess

    try:
        out = subprocess.run(
            ["curl", "-s", "-m", "30", _URL,
             "-H", "User-Agent: " + headers["User-Agent"]],
            capture_output=True, text=True, timeout=45,
        )
        if out.returncode == 0 and "wikitable" in out.stdout:
            return out.stdout
    except Exception:  # noqa: BLE001
        pass

    for cache in (_OUT.with_suffix(".html"), Path("/tmp/sp100.html")):
        if cache.exists() and "wikitable" in cache.read_text(errors="replace"):
            print(f"[cache] using {cache}")
            return cache.read_text(errors="replace")

    raise SystemExit("Could not fetch the S&P 100 page (network + cache exhausted)")


def main() -> None:
    html = _fetch_html()

    constituents = parse_constituents(html)
    if len(constituents) < 90:
        raise SystemExit(
            f"Only parsed {len(constituents)} constituents — Wikipedia table "
            f"format likely changed; inspect {_URL}"
        )

    _OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(_OUT, "w") as f:
        json.dump(constituents, f, indent=1, ensure_ascii=False)

    print(f"S&P 100: {len(constituents)} constituents -> {_OUT}")
    syms = [c["symbol"] for c in constituents]
    print("sample:", syms[:10], "...")


if __name__ == "__main__":
    main()
