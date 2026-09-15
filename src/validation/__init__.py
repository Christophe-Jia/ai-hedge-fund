"""Strategy validation framework — noise defense as a first-class citizen.

The existing backtest process defends well against *cheating* (no look-ahead,
locked hyperparameters, a full cost model) but was blind to *noise*: a Sharpe of
0.963 sat next to a mean monthly IC of 0.0082 (t ~ 0.84, indistinguishable from
zero), the IC flipped sign when the test window moved, and the top-10 boundary
was decided by sort ties.  It also never asked "how many things did we try?"
or "does it reproduce?" — this package turns all of that into function calls.

Cross-sectional (monthly) strategies::

    significance(monthly_ic_series)              # PASS / FAIL / NOISE
    multi_window(run_backtest, starts)           # STABLE / UNSTABLE
    boundary_stability(monthly_scores, top_n=10) # STABLE / ARBITRARY
    internal_consistency(ic, sharpe, tn, n)      # CONSISTENT / IMPLAUSIBLE
    neighborhood_stability(run_cfg, grid)        # SMOOTH / OVERFIT

Event-driven strategies (a handful of trades per year)::

    event_significance(trade_returns)            # t + bootstrap CI + Wilson
    event_window_stats(trades, {"2021-23": .., "2024-26": ..})

Every strategy, regardless of type::

    multiple_comparisons(n_searched, best_t, n=..)   # still significant?
    reproducibility_probe(stored_picks, rerun_picks) # REPRODUCIBLE / not
    red_team_checklist(report_dict)                  # PASS / WARN / FAIL
"""

from .boundary import boundary_stability
from .checklist import RED_TEAM_QUESTIONS, red_team_checklist, render_checklist, unanswered
from .consistency import internal_consistency
from .events import event_significance, event_window_stats
from .multiplicity import expected_max_abs_t, multiple_comparisons, required_t
from .neighborhood import neighborhood_stability
from .reproducibility import reproducibility_probe
from .significance import significance, significance_from_stats
from .stats import bootstrap_ci_mean, fdr_bh, proportion_z, two_sided_t_p, wilson_interval, z_two_sided
from .windows import multi_window

__all__ = [
    # cross-sectional
    "significance",
    "significance_from_stats",
    "multi_window",
    "neighborhood_stability",
    "boundary_stability",
    "internal_consistency",
    # event-driven
    "event_significance",
    "event_window_stats",
    # every strategy
    "multiple_comparisons",
    "required_t",
    "expected_max_abs_t",
    "reproducibility_probe",
    "red_team_checklist",
    "render_checklist",
    "unanswered",
    "RED_TEAM_QUESTIONS",
    # statistics helpers
    "wilson_interval",
    "bootstrap_ci_mean",
    "two_sided_t_p",
    "proportion_z",
    "fdr_bh",
    "z_two_sided",
]
