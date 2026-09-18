# Hypothesis registration notes — 2026-09-18

First **prospective** registrations in `hypotheses/registry.jsonl`. Unlike the
27 backfilled families, these were written and frozen *before* any
post-registration data exists: `registered_at_utc` is the real wall-clock time
and `scripts/evaluate_hypotheses.py` will mechanically refuse any window that
starts earlier.

Source spec: `hypotheses/prospective_specs_2026-09-18.json`
(registration via `scripts/register_hypothesis.py --spec …`, never hand-written).

| # | hypothesis_id | status | rubric | band |
|---|---|---|---|---|
| H1 | `merrill_clock_regime_rotation` | registered | 3.333 | NEEDS_STRENGTHENING |
| H2 | `sp500_index_inclusion_effect` | registered | 4.167 | ALLOW_EVALUATION |
| H3 | `cross_border_dual_listing` | **NOT registered** | — | — |

## H1 — Merrill Investment Clock regime rotation (registered)

Mechanism is fully specified (Growth/Inflation composite Z, EWM span=24,
quadrant→sector mapping frozen in the record). Rubric is honest about the weak
dimensions: **no forced counterparty** (`counterparty_arbitrage=1`) and **public
data** (`data_moat=1`) — a widely-published macro framework has no natural
barrier to being arbitraged. It clears the bar only because the mechanism is
stateable, the regime declaration is ex ante and mechanical, and it is
pre-registered.

Data status at registration:

- **FRED macro inputs MISSING locally**: `USALOLITONOSTSAM, INDPRO, ICSA,
  UNRATE, T5YIE, CPILFESL, PPIFIS, CPIAUCSL, TCU`. The platform has cached
  only `DTB3, DTWEXBGS, FEDFUNDS, NASDAQCOM, T10Y2Y, VIXCLS`.
- **ALFRED point-in-time vintages** are required for the as-of rule (INDPRO /
  CPI / PPI / TCU / UNRATE / ICSA are revised — using latest-vintage values
  would leak the future). This is listed in `data_requirements`.
- **Tradables are READY**: all 12 needed ETFs (SPY + 9 sectors + TLT/DBC/BIL)
  were fetched successfully on 2026-09-18 to `data/btc_history.db`
  (`2016-09-19 .. 2026-09-17`, ~2513 rows each) via
  `scripts/check_merrill_clock_data.py --fetch --only-etf`.
- `scripts/check_merrill_clock_data.py` is the data-prep/coverage scaffold; it
  checks cache presence and fetches gaps. It never scores the hypothesis.

## H2 — S&P 500 index inclusion effect (registered)

Strongest of the three: the counterparty is unambiguous — **index funds are
forced, price-insensitive buyers** whose flow is a mandate, not a view
(`counterparty_arbitrage=5`). This is the mirror image of the weekend_gap
failure, where the counterparty question was never asked.

Data status at registration:

- **Effective dates**: `scripts/fetch_sp500_changes.py` parses the Wikipedia
  "List of S&P 500 companies" → "Selected changes" table into
  `data/universe/sp500_changes.json` (one row per added/removed ticker, with
  `date_precision`). The parser is unit-checked offline
  (`--self-test`) and passes.
- **Live verification of the live page was blocked** at registration time:
  Wikimedia returned HTTP 403 "Too Many Reqs" for every endpoint
  (`api.php`, REST, `action=raw`, mobile, `api.wikimedia.org`, Wikidata) from
  this egress IP (see Network caveats). The table's existence and its Date
  column are documented in `scripts/fetch_sp500_history.py`, which already
  reads the same page for the constituent table; the changes-table extractor is
  therefore expected to work once the rate limit clears. **Coverage years and
  record count are not yet recorded here — run
  `poetry run python scripts/fetch_sp500_changes.py` when reachable.**
- **Announcement dates are NOT in the Wikipedia table** — it dates the
  *effective* day. The primary criterion is therefore defined on the
  effective-date window (which is what the mechanism predicts index funds must
  trade around); the announcement→effective decomposition needs S&P DJI
  index-change announcements and is listed as a secondary data requirement.
- **Prices**: added tickers are currently listed, so Nasdaq daily total-return
  closes are fetchable on demand. ~150 of the 736-symbol union are delisted and
  have no series — the criterion drops such events and reports the drop count.

## H3 — Cross-border dual listing (**not registered**)

Mechanism (temporary price divergence between a company's two listings, caused
by cross-market arbitrage frictions) is plausible, but the hypothesis is
**not registerable today: the second market's price data is unavailable at
reasonable cost.**

- The platform stores **US daily OHLCV only** (`data/btc_history.db`:
  584 US stocks + 7 ETFs). There is no Hong Kong / Japan / Europe local-market
  daily feed, and no ADR-vs-ordinary-share pairing table.
- Alternatives considered and rejected:
  - *ADR premium vs ordinary shares* — needs the ordinary-line price in the
    home market, which is exactly the missing data.
  - *Dual-class share pairs (GOOG/GOOGL, FOX/FOXA …)* — same exchange, no
    cross-market friction; the known conversion arbitrage is not the
    mechanism described.
  - *US ETF price vs NAV* — a different (creation/redemption) mechanism, and
    needs holdings/NAV history the platform does not have.
- Decision: **do not register a hypothesis that cannot be settled.** Add it to
  the backlog once a home-market daily feed (e.g. an exchange/EOD vendor or a
  licensed ADR-ordinary pair source) is available; the registry entry can then
  be written with `data_requirements` naming the exact feed.

## Network caveats observed 2026-09-18 (for whoever runs evaluation)

- **FRED** (`fred.stlouisfed.org`) is *flaky* from this environment: TLS
  connects and headers return 200, but the body transfer frequently stalls
  until timeout. Adding `Accept-Encoding: gzip` occasionally succeeds. The
  platform's `FredSeries` already has a curl fallback; a retry loop is needed
  in practice.
- **Wikimedia** was fully rate-limited (403 "Too Many Reqs") — `archive.org`
  (429) and `slickcharts` (403) likewise. `api.nasdaq.com` and
  `stockanalysis.com` were reachable and reliable.
- These are transient egress conditions, not data-availability conclusions;
  re-run the scaffolds rather than treating a failure as "unavailable".
