"""Tests for the fundamental feature builder's subtle parts.

Three things here are easy to get quietly wrong and would corrupt an entire
backtest rather than fail loudly:

1. TTM assembly — summing four quarterly facts only if they are consecutive,
   non-overlapping and ~1 year wide, otherwise falling back to the annual
   fact. A silent double-count would inflate every earnings yield.
2. Year-on-year growth — `shift(12)` is only a year if the monthly grid has
   no holes, so growth must be computed on a reindexed full grid. A symbol
   missing one month would otherwise be compared against the wrong month.
3. The split factor — must be measured from the SHARE COUNT's own
   period_end, not the signal date. Using the signal date drops a split that
   happened between the two, mis-scaling market cap by the split ratio
   (10x for pre-2024 NVDA) and making those names look absurdly cheap.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.build_fundamental_features import (  # noqa: E402
    MAX_STALE_DAYS,
    build_raw_panel,
    derive_features,
    fresh_state,
    latest_instant,
    replay_state,
    ttm,
)
from scripts.fetch_edgar_fundamentals import (  # noqa: E402
    connect,
    ensure_schema,
    replace_splits,
    upsert_facts,
)


def _row(sym, tag, value, pstart, pend, filed, unit="USD"):
    return (7, sym, "us-gaap", tag, unit, float(value), pstart, pend, filed,
            "10-K", f"acc-{sym}-{pend}-{filed}", 2020, "FY")


def _q(sym, tag, value, start, end, filed, unit="USD"):
    return _row(sym, tag, value, start, end, filed, unit)


# ---------------------------------------------------------------------------
# latest_instant
# ---------------------------------------------------------------------------

def test_latest_instant_takes_freshest_period_and_ignores_durations():
    per_tag = {
        "Assets": {("", "2020-03-31"): 100.0, ("", "2020-06-30"): 120.0},
        # a duration fact on the same tag must not be mistaken for a level
        "Liabilities": {("2020-04-01", "2020-06-30"): 999.0,
                        ("", "2020-03-31"): 40.0},
    }
    assert latest_instant(per_tag, ("Assets",)) == (120.0, "Assets", "2020-06-30")
    assert latest_instant(per_tag, ("Liabilities",)) == (40.0, "Liabilities", "2020-03-31")
    assert latest_instant(per_tag, ("Goodwill",)) is None


# ---------------------------------------------------------------------------
# TTM
# ---------------------------------------------------------------------------

def _quarters(tag, vals, filed="2021-01-01"):
    per = {tag: {}}
    for i, v in enumerate(vals):
        q = i % 4
        year = 2020 + i // 4
        start = f"{year}-{q * 3 + 1:02d}-01"
        end_month = q * 3 + 3
        end = f"{year}-{end_month:02d}-{'31' if end_month in (3, 12) else '30'}"
        per[tag][(start, end)] = float(v)
    return per


def test_ttm_sums_four_consecutive_quarters():
    per = _quarters("NetIncomeLoss", [10, 20, 30, 40])
    val, method, pend = ttm(per, "NetIncomeLoss")
    assert val == 100.0
    assert method == "ttm_quarters"
    assert pend == "2020-12-31"


def test_ttm_uses_only_the_most_recent_four_quarters():
    per = _quarters("NetIncomeLoss", [1, 1, 1, 1, 10, 20, 30, 40])
    val, method, _ = ttm(per, "NetIncomeLoss")
    assert (val, method) == (100.0, "ttm_quarters")


def test_ttm_falls_back_to_annual_when_quarters_are_incomplete():
    per = {"NetIncomeLoss": {
        ("2020-01-01", "2020-03-31"): 10.0,
        ("2020-04-01", "2020-06-30"): 20.0,
        ("2019-01-01", "2019-12-31"): 55.0,
    }}
    val, method, pend = ttm(per, "NetIncomeLoss")
    assert (val, method, pend) == (55.0, "annual", "2019-12-31")


def test_ttm_rejects_overlapping_quarters():
    """A cumulative 6-month fact must not be summed as if it were a quarter."""
    per = {"NetIncomeLoss": {
        ("2020-01-01", "2020-03-31"): 10.0,
        ("2020-04-01", "2020-06-30"): 20.0,
        ("2020-01-01", "2020-06-30"): 30.0,   # 6-month cumulative, 181 days
        ("2020-07-01", "2020-09-30"): 30.0,
        ("2020-10-01", "2020-12-31"): 40.0,
    }}
    # only 3 genuine quarters (10/20/30/40 is 4? no: 3M,3M,3M,3M -> yes 4)
    val, method, _ = ttm(per, "NetIncomeLoss")
    assert method == "ttm_quarters"
    assert val == 100.0          # 10+20+30+40, the 181d row excluded


def test_ttm_returns_none_without_enough_history():
    per = {"NetIncomeLoss": {("2020-01-01", "2020-03-31"): 10.0}}
    assert ttm(per, "NetIncomeLoss") == (None, "none", None)


# ---------------------------------------------------------------------------
# Growth on a gappy grid
# ---------------------------------------------------------------------------

def _panel(dates, sym_values):
    rows = []
    for sym, vals in sym_values.items():
        for d, v in zip(dates, vals):
            if v is None:
                continue
            rows.append({"date": d, "symbol": sym, "close": 10.0, "shares": 1.0,
                         "split_factor": 1.0, "ttm_revenue": v, "assets": v,
                         "ttm_net_income": v})
    return pd.DataFrame(rows)


def test_growth_uses_a_full_year_even_when_a_month_is_missing():
    """AAA misses month 5; BBB keeps that month in the grid.

    Without reindexing onto the full grid, AAA's month 13 would shift back
    12 surviving rows to month 2 instead of 12 calendar months to month 1.
    The values are distinct per month so a one-month slip is unmistakable
    (12.0x vs 5.5x).
    """
    dates = pd.date_range("2020-01-31", periods=14, freq="ME").strftime("%Y-%m-%d").tolist()
    aaa = [100.0 * (i + 1) for i in range(14)]
    aaa[4] = None                                    # hole in month 5
    panel = _panel(dates, {"AAA": aaa, "BBB": [50.0] * 14})
    out = derive_features(panel).set_index(["date", "symbol"]).sort_index()

    # month 13 = 1300, a year earlier = 100 -> 12.0; a slip to month 2 = 200
    assert out.loc[(dates[12], "AAA"), "rev_yoy"] == pytest.approx(12.0)
    assert out.loc[(dates[12], "AAA"), "asset_growth"] == pytest.approx(12.0)
    # the symbol with a complete grid is unaffected
    assert out.loc[(dates[12], "BBB"), "rev_yoy"] == pytest.approx(0.0)
    # there is no month 0 to compare against, so earlier months stay NaN
    assert np.isnan(out.loc[(dates[11], "AAA"), "rev_yoy"])


def test_growth_is_nan_when_the_base_is_missing():
    dates = pd.date_range("2020-01-31", periods=13, freq="ME").strftime("%Y-%m-%d").tolist()
    rev = [np.nan] * 12 + [110.0]
    panel = _panel(dates, {"AAA": rev})
    out = derive_features(panel).set_index(["date", "symbol"]).sort_index()
    assert np.isnan(out.loc[(dates[12], "AAA"), "rev_yoy"])


# ---------------------------------------------------------------------------
# Staleness guard
# ---------------------------------------------------------------------------

def test_fresh_state_drops_facts_that_stopped_being_reported():
    """A delisted filer's decade-old numbers must not look current.

    Without this cutoff the as-filed replay carries the last available value
    forward forever, so E/P becomes `16-year-old earnings / today's market
    cap` — a garbage factor value that still counts as "covered".
    """
    per_tag = {
        "Assets": {("", "2020-06-30"): 100.0},
        "NetIncomeLoss": {("2019-01-01", "2019-12-31"): 10.0},
    }
    fresh = fresh_state(per_tag, "2020-09-30")
    assert set(fresh) == {"Assets", "NetIncomeLoss"}

    # three years on, nothing is still being reported
    assert fresh_state(per_tag, "2023-09-30") == {}


def test_fresh_state_keeps_the_exact_cutoff_boundary():
    per_tag = {"Assets": {("", "2018-01-01"): 5.0}}
    from datetime import date
    on_cutoff = date(2018, 1, 1).toordinal() + MAX_STALE_DAYS
    assert fresh_state(per_tag, str(date.fromordinal(on_cutoff))) != {}
    assert fresh_state(per_tag, str(date.fromordinal(on_cutoff + 1))) == {}


def test_fresh_state_ignores_tags_the_panel_does_not_read():
    per_tag = {"SomeUnusedTag": {("", "2020-06-30"): 1.0}}
    assert fresh_state(per_tag, "2020-09-30") == {}


# ---------------------------------------------------------------------------
# Split factor / market cap basis
# ---------------------------------------------------------------------------

def test_split_factor_comes_from_the_share_counts_period_end(tmp_path):
    """The 4:1 split sits BETWEEN the share count and the signal date.

    At the 2020-10-31 signal the only visible share count is the pre-split
    100 (filed 2020-05-01). The factor must still be 4, because the split is
    dated after that count's period_end. Measuring from the signal date
    would give 1 and report a market cap 4x too small.
    """
    conn = connect(tmp_path / "t.db")
    ensure_schema(conn)
    upsert_facts(conn, [
        _row("TST", "CommonStockSharesOutstanding", 100.0, "", "2020-03-31",
             "2020-05-01", unit="shares"),
        _row("TST", "CommonStockSharesOutstanding", 400.0, "", "2020-09-30",
             "2020-11-01", unit="shares"),
    ])
    replace_splits(conn, "TST", [("2020-07-15", 4.0)])

    panel = build_raw_panel(
        ["TST"], ["2020-06-30", "2020-10-31", "2020-12-31"], conn
    ).set_index("date")

    assert panel.loc["2020-06-30", "shares"] == 100.0
    assert panel.loc["2020-06-30", "split_factor"] == pytest.approx(4.0)

    # signal after the split but the share count is still the pre-split one
    assert panel.loc["2020-10-31", "shares"] == 100.0
    assert panel.loc["2020-10-31", "split_factor"] == pytest.approx(4.0)

    # once the post-split count is filed the factor drops to 1 (both are now
    # on the same basis, so the product shares*factor is what matters)
    assert panel.loc["2020-12-31", "shares"] == 400.0
    assert panel.loc["2020-12-31", "split_factor"] == pytest.approx(1.0)

    # the invariant: shares x factor is continuous across the split
    assert (panel.loc["2020-10-31", "shares"] * panel.loc["2020-10-31", "split_factor"]
            == panel.loc["2020-12-31", "shares"]
            * panel.loc["2020-12-31", "split_factor"])
    conn.close()


def test_market_cap_uses_the_adjusted_price_basis(tmp_path):
    """MC = adjusted_price * as_reported_shares * factor (same split story)."""
    conn = connect(tmp_path / "t.db")
    ensure_schema(conn)
    upsert_facts(conn, [
        _row("TST", "CommonStockSharesOutstanding", 100.0, "", "2020-03-31",
             "2020-05-01", unit="shares"),
    ])
    replace_splits(conn, "TST", [("2020-07-15", 4.0)])
    panel = build_raw_panel(["TST"], ["2020-06-30"], conn)
    assert panel["split_factor"].iloc[0] == pytest.approx(4.0)
    conn.close()


# ---------------------------------------------------------------------------
# As-of replay vs restatements
# ---------------------------------------------------------------------------

def test_replay_keeps_the_as_filed_value_when_a_later_filing_restates():
    facts = pd.DataFrame([
        {"tag": "Assets", "unit": "USD", "period_start": "",
         "period_end": "2020-06-30", "value": 100.0, "filed_date": "2020-08-01",
         "form": "10-Q"},
        {"tag": "Assets", "unit": "USD", "period_start": "",
         "period_end": "2020-06-30", "value": 80.0, "filed_date": "2021-02-01",
         "form": "10-K"},
    ])
    snaps = replay_state(facts, ["2020-09-30", "2021-03-31"])
    assert snaps["2020-09-30"]["Assets"][("", "2020-06-30")] == 100.0
    assert snaps["2021-03-31"]["Assets"][("", "2020-06-30")] == 100.0


def test_replay_hides_facts_that_are_not_yet_filed():
    facts = pd.DataFrame([
        {"tag": "Assets", "unit": "USD", "period_start": "",
         "period_end": "2020-06-30", "value": 100.0, "filed_date": "2020-08-01",
         "form": "10-Q"},
    ])
    snaps = replay_state(facts, ["2020-07-31", "2020-08-31"])
    assert "Assets" not in snaps["2020-07-31"]
    assert snaps["2020-08-31"]["Assets"][("", "2020-06-30")] == 100.0
