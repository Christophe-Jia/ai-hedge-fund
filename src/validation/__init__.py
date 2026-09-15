"""Strategy validation framework — noise defense as a first-class citizen.

The existing backtest process defends well against *cheating* (no look-ahead,
locked hyperparameters, a full cost model) but was blind to *noise*: a Sharpe of
0.963 sat next to a mean monthly IC of 0.0082 (t ~ 0.84, indistinguishable from
zero), the IC flipped sign when the test window moved, and the top-10 boundary
was decided by sort ties. Every one of those is now a function call.

Typical use::

    from src.validation import (
        significance, multi_window, neighborhood_stability,
        boundary_stability, internal_consistency, red_team_checklist,
    )

    sig = significance(monthly_ic_series)                 # PASS / FAIL / NOISE
    win = multi_window(run_backtest, starts, metric="sharpe")   # STABLE / UNSTABLE
    nb = neighborhood_stability(run_cfg, {"top_n": [5, 10, 15]})
    bd = boundary_stability(monthly_scores, top_n=10)     # STABLE / ARBITRARY
    ic = internal_consistency(0.0082, 0.963, n_months=71) # IMPLAUSIBLE
    cl = red_team_checklist(report_dict)                  # PASS / WARN / FAIL
"""

from .boundary import boundary_stability
from .checklist import RED_TEAM_QUESTIONS, red_team_checklist, render_checklist, unanswered
from .consistency import internal_consistency
from .neighborhood import neighborhood_stability
from .significance import significance, significance_from_stats
from .windows import multi_window

__all__ = [
    "significance",
    "significance_from_stats",
    "multi_window",
    "neighborhood_stability",
    "boundary_stability",
    "internal_consistency",
    "red_team_checklist",
    "render_checklist",
    "unanswered",
    "RED_TEAM_QUESTIONS",
]
