"""Point-in-time correctness tests for the EDGAR ingester.

The whole value of `scripts/fetch_edgar_fundamentals.py` is that a backtest
reading it cannot see the future. `companyfacts` is the LATEST view of every
fact, so the failure mode this file guards against is silent look-ahead: a
number restated in a 2024 filing being served to an `as_of` date in 2021.

Core scenario under test — a restatement:

    FY2020 net income was first reported as 100 on 2021-02-15 (the 10-K
    actually filed then). In 2022-02-15 the company restated it to 80.

    as_of_facts(..., "2021-06-30") MUST return 100  (what was on the tape)
    as_of_facts(..., "2022-06-30") MUST return  80  (the restated view)

Also covers the restatement-is-not-an-overwrite property (both rows live in
the table, which is what makes the as-of query possible), period selection,
future-filing exclusion, unit filtering, tag curation and split detection.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.fetch_edgar_fundamentals import (  # noqa: E402
    as_filed_share_levels,
    as_of_facts,
    connect,
    detect_splits,
    detect_symbol_splits,
    ensure_schema,
    parse_facts,
    restated_share_ratios,
    split_factor_as_of,
    upsert_facts,
)


def _row(sym, tag, value, pstart, pend, filed, unit="USD", form="10-K",
         cik=42, tax="us-gaap"):
    """One `facts` row in the ingester's column order."""
    return (cik, sym, tax, tag, unit, float(value), pstart, pend, filed,
            form, f"acc-{sym}-{filed}", 2020, "FY")


@pytest.fixture()
def db(tmp_path):
    path = tmp_path / "facts.db"
    conn = connect(path)
    ensure_schema(conn)
    yield conn, path
    conn.close()


# ---------------------------------------------------------------------------
# The headline restatement scenario
# ---------------------------------------------------------------------------

def test_restatement_is_not_overwritten_and_as_of_returns_as_filed(db):
    conn, path = db
    upsert_facts(conn, [
        _row("TST", "NetIncomeLoss", 100.0, "2020-01-01", "2020-12-31", "2021-02-15"),
        _row("TST", "NetIncomeLoss", 80.0, "2020-01-01", "2020-12-31", "2022-02-15"),
    ])

    # both observations coexist — a restatement is a new row, not an update
    n = conn.execute("SELECT COUNT(*) FROM facts WHERE symbol='TST'").fetchone()[0]
    assert n == 2

    # before the restatement: the as-filed 100, never the future 80
    before = as_of_facts("TST", "NetIncomeLoss", "2021-06-30", conn=conn)
    assert len(before) == 1
    assert before["value"].iloc[0] == 100.0
    assert before["filed_date"].iloc[0] == "2021-02-15"

    # default ("as-filed"): the original publication keeps winning even once
    # the restatement is public — a period is only ever read as first released
    still = as_of_facts("TST", "NetIncomeLoss", "2022-06-30", conn=conn)
    assert still["value"].iloc[0] == 100.0

    # opt-in alternative: a live reader on 2022-06-30 knows about the
    # restatement and would use 80. Still look-ahead free (80 was filed
    # 2022-02-15 <= 2022-06-30), just a different reading of "available".
    latest = as_of_facts("TST", "NetIncomeLoss", "2022-06-30", conn=conn,
                         prefer_latest=True)
    assert latest["value"].iloc[0] == 80.0
    assert latest["filed_date"].iloc[0] == "2022-02-15"

    # ...and before the restatement both modes agree on 100
    assert as_of_facts("TST", "NetIncomeLoss", "2021-06-30", conn=conn,
                       prefer_latest=True)["value"].iloc[0] == 100.0


def test_future_filings_are_invisible(db):
    conn, _ = db
    upsert_facts(conn, [
        _row("TST", "Assets", 500.0, "", "2020-12-31", "2021-02-15"),
    ])
    # one day before the filing the fact did not exist publicly
    assert as_of_facts("TST", "Assets", "2021-02-14", conn=conn).empty
    # the filing date itself counts as visible (filed_date <= as_of)
    assert as_of_facts("TST", "Assets", "2021-02-15", conn=conn)["value"].iloc[0] == 500.0


def test_as_of_is_strictly_monotonic_in_information(db):
    """Widening as_of can never lose a period, only add or revise it."""
    conn, _ = db
    upsert_facts(conn, [
        _row("TST", "Revenues", 10.0, "2020-01-01", "2020-03-31", "2020-05-01"),
        _row("TST", "Revenues", 30.0, "2020-01-01", "2020-06-30", "2020-08-01"),
        _row("TST", "Revenues", 55.0, "2020-01-01", "2020-09-30", "2020-11-01"),
    ])
    prev_n = 0
    for as_of in ("2020-04-30", "2020-05-01", "2020-07-01", "2020-08-01",
                  "2020-10-01", "2020-11-01", "2021-01-01"):
        got = as_of_facts("TST", "Revenues", as_of, conn=conn)
        assert len(got) >= prev_n
        prev_n = len(got)
    assert prev_n == 3


def test_earliest_filed_wins_within_the_visible_window(db):
    """Three filings carry FY2019: 2019 10-K, 2020 10-K, 2021 10-K.

    Only the original (earliest) may ever be served, at any as_of after it.
    """
    conn, _ = db
    for filed, val in (("2019-11-01", 1.0), ("2020-11-01", 2.0), ("2021-11-01", 3.0)):
        upsert_facts(conn, [_row("TST", "Revenues", val, "2019-01-01", "2019-12-31", filed)])
    for as_of in ("2019-11-01", "2020-06-01", "2020-11-01", "2021-12-31", "2026-01-01"):
        got = as_of_facts("TST", "Revenues", as_of, conn=conn)
        assert got["value"].iloc[0] == 1.0, f"leaked a later filing at {as_of}"


def test_periods_are_selected_independently(db):
    """A restated old quarter must not affect a newer, un-restated quarter."""
    conn, _ = db
    upsert_facts(conn, [
        _row("TST", "NetIncomeLoss", 10.0, "2020-01-01", "2020-03-31", "2020-05-01"),
        _row("TST", "NetIncomeLoss", 12.0, "2020-01-01", "2020-06-30", "2020-08-01"),
        _row("TST", "NetIncomeLoss",  5.0, "2020-01-01", "2020-03-31", "2020-11-01"),
    ])
    got = as_of_facts("TST", "NetIncomeLoss", "2020-09-01", conn=conn)
    by_end = dict(zip(got["period_end"], got["value"]))
    assert by_end == {"2020-03-31": 10.0, "2020-06-30": 12.0}

    # the restatement lands only on Q1; Q2 is untouched in either mode
    as_filed = as_of_facts("TST", "NetIncomeLoss", "2020-12-01", conn=conn)
    assert dict(zip(as_filed["period_end"], as_filed["value"])) == {
        "2020-03-31": 10.0, "2020-06-30": 12.0}

    latest = as_of_facts("TST", "NetIncomeLoss", "2020-12-01", conn=conn,
                         prefer_latest=True)
    assert dict(zip(latest["period_end"], latest["value"])) == {
        "2020-03-31": 5.0, "2020-06-30": 12.0}


def test_duration_and_instant_facts_do_not_collide(db):
    """Same period_end, different period_start → two independent series."""
    conn, _ = db
    upsert_facts(conn, [
        _row("TST", "NetIncomeLoss", 30.0, "2020-01-01", "2020-06-30", "2020-08-01"),
        _row("TST", "NetIncomeLoss", 10.0, "2020-04-01", "2020-06-30", "2020-08-01"),
    ])
    got = as_of_facts("TST", "NetIncomeLoss", "2020-09-01", conn=conn)
    assert sorted(got["value"]) == [10.0, 30.0]


def test_unit_filter_isolates_share_counts(db):
    conn, _ = db
    upsert_facts(conn, [
        _row("TST", "EarningsPerShareDiluted", 3.0, "2020-01-01", "2020-12-31",
             "2021-02-15", unit="USD/shares"),
    ])
    assert as_of_facts("TST", "EarningsPerShareDiluted", "2021-06-30",
                       conn=conn, unit="shares").empty
    assert as_of_facts("TST", "EarningsPerShareDiluted", "2021-06-30",
                       conn=conn, unit="USD/shares")["value"].iloc[0] == 3.0


def test_unknown_symbol_or_tag_returns_empty(db):
    conn, _ = db
    upsert_facts(conn, [_row("TST", "Assets", 1.0, "", "2020-12-31", "2021-02-15")])
    assert as_of_facts("NOPE", "Assets", "2022-01-01", conn=conn).empty
    assert as_of_facts("TST", "Goodwill", "2022-01-01", conn=conn).empty


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------

def test_parse_facts_curates_tags_and_requires_filed_date():
    payload = {"facts": {
        "us-gaap": {
            "NetIncomeLoss": {"units": {"USD": [
                {"start": "2020-01-01", "end": "2020-12-31", "val": 5,
                 "filed": "2021-02-15", "form": "10-K"},
                {"end": "2020-12-31", "val": 9},          # no `filed` → dropped
            ]}},
            "SomeTagWeDoNotWant": {"units": {"USD": [
                {"start": "2020-01-01", "end": "2020-12-31", "val": 7,
                 "filed": "2021-02-15"},
            ]}},
        },
        "dei": {
            "EntityCommonStockSharesOutstanding": {"units": {"shares": [
                {"end": "2021-01-20", "val": 1000, "filed": "2021-02-15"},
            ]}},
        },
    }}
    rows = parse_facts("TST", 42, payload)
    tags = sorted({r[3] for r in rows})
    assert tags == ["EntityCommonStockSharesOutstanding", "NetIncomeLoss"]
    assert all(r[8] for r in rows)          # every row carries a filed_date


# ---------------------------------------------------------------------------
# Splits
# ---------------------------------------------------------------------------

def test_detect_splits_reports_each_split_once():
    """A 7:1 then a 4:1, with the levels drifting between them."""
    levels = [
        ("2013-12-31", 890e6), ("2014-03-31", 900e6),          # pre 7:1
        ("2014-06-30", 6_300e6), ("2014-09-30", 6_400e6),      # post 7:1
        ("2015-06-30", 5_800e6), ("2018-06-30", 4_900e6),      # buybacks
        ("2020-07-17", 4_275e6),                               # pre 4:1
        ("2020-09-26", 17_100e6), ("2020-12-31", 16_900e6),    # post 4:1
        ("2021-06-30", 16_600e6),
    ]
    assert detect_splits(levels) == [("2014-06-30", 7.0), ("2020-09-26", 4.0)]


def test_detect_splits_ignores_gradual_issuance_and_spikes():
    levels = [
        ("2019-03-31", 100e6), ("2019-06-30", 104e6), ("2019-09-30", 108e6),
        ("2019-12-31", 113e6), ("2020-03-31", 1e6),   # mis-scaled XBRL fact
        ("2020-06-30", 118e6), ("2020-09-30", 123e6),
    ]
    assert detect_splits(as_filed_share_levels([
        ("TST", "TST", "us-gaap", "CommonStockSharesOutstanding", "shares", v,
         "", d, "2021-01-01", "10-K", "a", 2020, "FY")
        for d, v in levels
    ])) == []


def test_detect_splits_handles_reverse_split():
    levels = [("2020-03-31", 400e6), ("2020-06-30", 100e6), ("2020-09-30", 99e6)]
    assert detect_splits(levels) == [("2020-06-30", 0.25)]


def test_split_factor_as_of_applies_only_future_splits():
    splits = [("2014-06-28", 7.0), ("2020-09-26", 4.0)]
    assert split_factor_as_of(splits, "2013-01-01") == pytest.approx(28.0)
    assert split_factor_as_of(splits, "2015-01-01") == pytest.approx(4.0)
    assert split_factor_as_of(splits, "2021-01-01") == pytest.approx(1.0)
    assert split_factor_as_of([], "2021-01-01") == pytest.approx(1.0)


def _share_row(sym, value, pend, filed, tag="CommonStockSharesOutstanding"):
    return (42, sym, "us-gaap", tag, "shares", float(value), "", pend, filed,
            "10-K", f"acc-{pend}-{filed}", 2020, "FY")


def test_restated_share_ratios_finds_splits():
    """A 7:1 split rescales the share count of every pre-split period."""
    rows = [
        _share_row("TST", 890e6, "2013-09-28", "2013-10-30"),
        _share_row("TST", 6230e6, "2013-09-28", "2014-10-27"),   # restated 7x
    ]
    assert restated_share_ratios(rows) == {7.0}


def test_restated_share_ratios_ignores_stock_paid_issuance():
    """Paying for an acquisition with stock does NOT restate history.

    This is the case that separates a 3:2 split from Verizon's 2014 Vodafone
    issuance / Marriott's 2016 Starwood issuance, both of which step the
    share count by a locally clean-looking ratio.
    """
    rows = [
        # the same, unrevised historical periods ...
        _share_row("TST", 2_900e6, "2013-12-31", "2014-02-01"),
        _share_row("TST", 2_900e6, "2013-12-31", "2015-02-01"),
        # ... and a much larger count afterwards purely from issuance
        _share_row("TST", 4_100e6, "2014-03-31", "2014-04-30"),
    ]
    assert restated_share_ratios(rows) == set()


def test_detect_splits_requires_corroboration():
    levels = [("2020-03-31", 1000e6), ("2020-06-30", 1500e6), ("2020-09-30", 1500e6)]
    # a clean-looking 1.5x step with no restatement evidence is refused
    assert detect_splits(levels, set()) == []
    # ... and accepted once a restatement confirms the ratio
    assert detect_splits(levels, {1.5}) == [("2020-06-30", 1.5)]
    # None disables the check (used only in tests / diagnostics)
    assert detect_splits(levels) == [("2020-06-30", 1.5)]


def test_detect_symbol_splits_end_to_end():
    rows = [
        # issuance-looking step, unconfirmed -> rejected
        _share_row("ISS", 100e6, "2020-03-31", "2020-05-01"),
        _share_row("ISS", 150e6, "2020-06-30", "2020-08-01"),
        _share_row("ISS", 150e6, "2020-09-30", "2020-11-01"),
    ]
    assert detect_symbol_splits(rows) == []

    split_rows = [
        _share_row("SPL", 100e6, "2020-03-31", "2020-05-01"),
        _share_row("SPL", 200e6, "2020-06-30", "2020-08-01"),
        _share_row("SPL", 200e6, "2020-09-30", "2020-11-01"),
        # ... the 2:1 split restates the pre-split period onto the new basis
        _share_row("SPL", 200e6, "2020-03-31", "2021-02-15"),
    ]
    assert detect_symbol_splits(split_rows) == [("2020-06-30", 2.0)]


def test_shares_are_read_as_filed_not_restated():
    """The share level used for split detection is the as-filed one."""
    rows = [
        ("TST", "TST", "us-gaap", "CommonStockSharesOutstanding", "shares",
         100.0, "", "2020-06-30", "2020-08-01", "10-Q", "a", 2020, "Q2"),
        ("TST", "TST", "us-gaap", "CommonStockSharesOutstanding", "shares",
         400.0, "", "2020-06-30", "2021-02-15", "10-K", "b", 2020, "FY"),
    ]
    assert as_filed_share_levels(rows) == [("2020-06-30", 100.0)]


# ---------------------------------------------------------------------------
# Schema contract
# ---------------------------------------------------------------------------

def test_schema_has_the_documented_indexes(db):
    conn, _ = db
    names = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index'")}
    assert "ix_facts_sym_tag_period" in names
    assert "ix_facts_sym_filed" in names


def test_split_table_roundtrip(db):
    from scripts.fetch_edgar_fundamentals import get_splits, replace_splits

    conn, _ = db
    replace_splits(conn, "TST", [("2020-09-26", 4.0)])
    assert get_splits("TST", conn) == [("2020-09-26", 4.0)]
    replace_splits(conn, "TST", [])
    assert get_splits("TST", conn) == []
