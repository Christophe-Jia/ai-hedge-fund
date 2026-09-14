#!/usr/bin/env python3
"""Fetch point-in-time S&P 100 constituents from Wikipedia revision history.

For each year, finds the first page revision after Jan 10 and parses its
constituent table -> data/universe/sp100_{year}.json. This is the
survivorship-bias fix: backtests can use the universe as it WAS, not as
it is today.

Usage:
    poetry run python scripts/fetch_sp100_history.py            # 2017..2026
    poetry run python scripts/fetch_sp100_history.py --years 2020,2021
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.fetch_sp100_universe import parse_constituents

_API = "https://en.wikipedia.org/w/api.php"
_UA = "Mozilla/5.0 (research-script, personal quant project)"
_OUT_DIR = Path(__file__).resolve().parents[1] / "data" / "universe"


def _api_get(params: dict) -> dict:
    """Wikipedia API with curl fallback (urllib is throttled on some networks)."""
    url = _API + "?" + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": _UA})
        with urllib.request.urlopen(req, timeout=30) as resp:
            text = resp.read().decode("utf-8")
    except Exception:  # noqa: BLE001 — fall back to curl
        import subprocess
        out = subprocess.run(
            ["curl", "-s", "-m", "45", url, "-H", f"User-Agent: {_UA}"],
            capture_output=True, text=True, timeout=60,
        )
        text = out.stdout
    for attempt in range(3):
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            time.sleep(2 * (attempt + 1))
            import subprocess
            out = subprocess.run(
                ["curl", "-s", "-m", "45", url, "-H", f"User-Agent: {_UA}"],
                capture_output=True, text=True, timeout=60,
            )
            text = out.stdout
    raise RuntimeError(f"Wikipedia API failed: {params}")


def revision_near(year: int) -> tuple[int, str]:
    """First revision id+timestamp on/after Jan 10 of `year`."""
    d = _api_get({
        "action": "query",
        "prop": "revisions",
        "titles": "S&P 100",
        "rvstart": f"{year}-01-10T00:00:00Z",
        "rvlimit": 1,
        "rvdir": "newer",
        "rvprop": "ids|timestamp",
        "format": "json",
    })
    for page in d["query"]["pages"].values():
        revs = page.get("revisions") or []
        if revs:
            return int(revs[0]["revid"]), revs[0]["timestamp"]
    raise RuntimeError(f"no revision found for {year}")


def constituents_of_revision(revid: int) -> list[dict]:
    d = _api_get({
        "action": "parse",
        "oldid": revid,
        "prop": "text",
        "format": "json",
    })
    html = d["parse"]["text"]["*"]
    return parse_constituents(html)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--years", type=str, default=",".join(str(y) for y in range(2017, 2027)))
    args = p.parse_args()

    _OUT_DIR.mkdir(parents=True, exist_ok=True)
    union_syms: set[str] = set()

    for year in [int(y) for y in args.years.split(",")]:
        out = _OUT_DIR / f"sp100_{year}.json"
        if out.exists():
            data = json.loads(out.read_text())
            print(f"  {year}: cached ({len(data)} 成分股)")
            union_syms |= {c["symbol"] for c in data}
            continue
        revid, ts = revision_near(year)
        cs = constituents_of_revision(revid)
        if len(cs) < 90:
            raise SystemExit(f"{year}: only {len(cs)} constituents parsed — table format issue?")
        out.write_text(json.dumps(cs, indent=1, ensure_ascii=False))
        union_syms |= {c["symbol"] for c in cs}
        print(f"  {year}: {len(cs)} 成分股 (rev {revid}, {ts[:10]})")
        time.sleep(1.0)  # be polite to the API

    # union of all years = the full backtest universe incl. removed names
    union_path = _OUT_DIR / "sp100_union.json"
    union_path.write_text(json.dumps(
        sorted(union_syms), indent=0))
    print(f"\n  联合宇宙 (所有年份去重): {len(union_syms)} 只 -> {union_path}")


if __name__ == "__main__":
    main()
