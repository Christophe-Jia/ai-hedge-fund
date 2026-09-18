# Proposal: record search width (N) in the hypothesis registry

Status: **proposal only** — no code change. Owner decision needed on *when* to
implement and *who* implements. Do not edit `registry.py` /
`register_hypothesis.py` / `registry.jsonl` from this document alone.

Author: deflation-dsr · 2026-09-18 · Context: DSR work (commit `618c04b0`)

## 1. Problem

DSR deflates a champion by the *number of trials* (N) that were available to
select it. The registry — our single source of truth for pre-registration —
records **no search width**. So `scripts/validate_reports.py` has to fall back
to one of:

1. a grid reconstructed **after the fact** from report/scripts
   (e.g. weekend_gap = 6 exits × 3 targets × 7 thresholds = 126, with
   `file:line` evidence), or
2. a **platform constant** (`PLATFORM_HYPOTHESES_SEARCHED=27` /
   `_VARIANTS=60`), which is family-level and either under- or over-penalises.

Both are lossy and, worse, both are **post-hoc**: the analyst sees the winner,
then decides what N to quote. That is precisely the failure mode the registry
exists to prevent (see `src/validation/registry.py` module docstring).

## 2. Proposed fields

| field | type | when filled | required? | by whom |
|---|---|---|---|---|
| `n_trials_planned` | `int >= 1` | **at registration** | yes (new records) | hypothesis author |
| `search_grid` | `object[str, int]` | at registration (recommended) | no | author |
| `n_trials_actual` | `int >= 1 \| null` | at evaluation/resolution | no | evaluator (new revision) |
| `search_grid_evidence` | `string \| null` | with `n_trials_actual` | conditional | evaluator |

- `search_grid` is the machine-checkable decomposition that **must multiply to
  `n_trials_planned`**, e.g. `{"thresholds": 7, "targets": 3, "exit_rules": 6}`
  → 126.
- `n_trials_actual` exists because a pre-registered grid can change once you
  run it; recording both makes the **planned-vs-actual gap itself diagnostic**.

Rejected alternative: a single `variant_count`. It conflates "what we said we'd
search" (the multiplicity *prior*, which must be frozen) with "what we actually
searched" (an outcome). They belong to different moments.

## 3. Why it MUST be recorded at registration (not backfilled)

1. **N directly sets the DSR bar.** The expected-max ceiling is monotone in N,
   so choosing N after seeing the winner is choosing your own pass/fail line —
   the same class of error as choosing the evaluation window after the fact.
2. **The registry already enforces the analogous rule for time.** It refuses an
   `evaluation_window_start` earlier than `registered_at_utc`. Multiplicity is a
   second, independent dimension of the same pre-registration discipline; it
   deserves the same mechanical guard.
3. **External precedent.** Clinical trials pre-register the sample size and the
   number of endpoints; you may not pick them after unblinding. `n_trials_planned`
   is the quant analogue.

Note the incentive is self-correcting in one direction: **over-declaring N only
raises the bar against yourself**, so the risk is *under*-declaring. Hence the
`search_grid` product check + evidence requirement below.

## 4. Minimal validation rules

1. `n_trials_planned` is required for **new** registrations; integer, `>= 1`
   (a single confirmatory test is N=1, not absent).
2. If `search_grid` is present: every value is a positive integer and
   `prod(values) == n_trials_planned`; on mismatch, reject and name the factors.
3. `n_trials_actual`, if present: integer `>= 1`. If
   `n_trials_actual < n_trials_planned`, `search_grid_evidence` becomes
   required (otherwise a smaller N looks like post-hoc narrowing).
4. `search_grid_evidence`: string such as `scripts/backtest_exit_rules.py:74`
   or an artifact path; required whenever `n_trials_actual != n_trials_planned`.
5. **Backward compatibility:** do **not** add the field to `REQUIRED_FIELDS` —
   that would reject the 29 existing ledger lines. Enforce it in `build_record`
   for records created after a declared cutover (e.g. a `schema_version: 2`
   marker on new records), and allow legacy records to carry `null`. Any
   retro-fit of the 27/29 legacy families is *evidence-backed annotation*, marked
   `n_trials_origin: "backfill"`, never silently "as registered".

## 5. How `deflation` will consume it

N-resolution priority (replaces today's "registry > report-countable >
platform constant" with the registry field now actually existing):

1. `n_trials_actual` (evaluation ran a known grid)
2. `n_trials_planned` (pre-registered width)
3. report/script-countable grid, with `file:line` evidence
4. platform constant (`PLATFORM_HYPOTHESES_SEARCHED` / `_VARIANTS`)

The resolved value and its source are written into
`checks.deflation.n_trials_basis` (as today), so every DSR number is
auditable back to a moment in time:

```
deflation_from_stats(sr, n, n_trials=resolve_n_trials(hypothesis_id), ...)
```

`resolve_n_trials` is a thin helper (in `validate_reports.py` or a small util —
**not** in `registry.py`, to keep the registry freeze), reading
`linked_reports`/`hypothesis_id` → registry → priority list above.

Retro-fit example (weekend_gap):

```json
{
  "n_trials_planned": 126,
  "search_grid": {"exit_rules": 6, "targets": 3, "thresholds": 7},
  "n_trials_actual": 126,
  "search_grid_evidence": "scripts/backtest_exit_rules.py:74, scripts/backtest_exit_rules.py:64, scripts/red_team_weekend_gap.py:158",
  "n_trials_origin": "backfill"
}
```

## 6. Known limitations (record, don't hide)

- **Correlated variants.** Raw count over-states independent trials; LdP's
  effective-N (eigenvalue formula `(Σ√λ)²/Σλ`) is the refinement. This proposal
  records the *raw* count and leaves `n_effective` as a future field — better an
  auditable raw number than a fabricated effective one.
- **New overhead at registration.** One integer + an optional decomposition; the
  cost is small and paid at exactly the moment the hypothesis is frozen.
