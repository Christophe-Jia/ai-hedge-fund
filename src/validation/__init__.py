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

Perturbation / anti-fragility (event and monthly series)::

    robustness_battery(returns, eras=..)         # ROBUST / FRAGILE / SINGLE_EVENT_DRIVEN
    leave_k_best_out(returns, k=2)               # is it just the best 2 observations?
    leave_k_worst_out(returns, k=2)              # symmetric (loss-driven) check
    drop_fraction_sweep(returns, fractions=..)   # sign stability under random dropping
    leave_one_era_out(returns, eras)             # does one regime carry it?
    concentration_profile(returns)               # top-1/2/3 share, HHI, ex-top-5%

Mechanism-side due diligence (the layer the result-side checks were missing)::

    score(rubric_answers)                            # weighted 1/3/5 rubric
    build_record(..., rubric_answers=..)             # register a hypothesis
    check_no_pre_registration_data(record, start)    # refuse pre-registration data
    sample_sufficiency(n, n_min=..)                  # refuse verdicts below n_min

The result-side framework asks "did you measure this correctly?"; the rubric +
registry ask "is this worth believing, and was it committed to before you
looked?".  Entry points: docs/signal_rubric.md, scripts/register_hypothesis.py,
scripts/evaluate_hypotheses.py, scripts/rubric_attribution.py.
"""

from .boundary import boundary_stability
from .checklist import RED_TEAM_QUESTIONS, red_team_checklist, render_checklist, unanswered
from .consistency import internal_consistency
from .deflation import (
    DSR_THRESHOLD,
    EULER_GAMMA,
    MUTIC_LAMBDA_DEFAULT,
    MUTIC_MAX_CORR,
    NORMAL_KURTOSIS,
    deflated_sharpe_ratio,
    deflation_from_stats,
    deflation_report,
    expected_max_sharpe,
    mutic_adjusted_ic,
    probabilistic_sharpe_ratio,
)
from .events import event_significance, event_window_stats
from .gate import deployment_gate, deployment_gate_from_checks
from .multiplicity import expected_max_abs_t, multiple_comparisons, required_t
from .neighborhood import neighborhood_stability
from .registry import (
    STATUS_EVALUATING,
    STATUS_PROPOSED,
    STATUS_REGISTERED,
    STATUS_REJECTED,
    STATUS_RESOLVED,
    DuplicateHypothesisError,
    PreRegistrationDataError,
    RegistryValidationError,
    append_hypothesis,
    build_record,
    check_no_pre_registration_data,
    get_hypothesis,
    load_registry,
    registry_stats,
    sample_sufficiency,
    survival_label,
    utc_now_iso,
)
from .reproducibility import reproducibility_probe
from .robustness import (
    FLAG_BY_ERA,
    FLAG_FEW_LOSERS,
    FLAG_FEW_WINNERS,
    FLAG_UNSTABLE_RESAMPLE,
    FRAGILE,
    ROBUST,
    SINGLE_EVENT_DRIVEN,
    concentration_profile,
    drop_fraction_sweep,
    leave_k_best_out,
    leave_k_worst_out,
    leave_one_era_out,
    robustness_battery,
)
from .rubric import (
    BAND_ALLOW,
    BAND_REJECT,
    BAND_STRENGTHEN,
    BAND_THRESHOLDS,
    DIMENSION_WEIGHTS,
    DIMENSIONS,
    RubricResult,
    band_for,
    render_rubric_markdown,
    score,
    weakest_dimensions,
)
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
    # statistical deflation (PSR / DSR / MutIC — Bailey & López de Prado)
    "probabilistic_sharpe_ratio",
    "expected_max_sharpe",
    "deflated_sharpe_ratio",
    "mutic_adjusted_ic",
    "deflation_report",
    "deflation_from_stats",
    "DSR_THRESHOLD",
    "EULER_GAMMA",
    "MUTIC_LAMBDA_DEFAULT",
    "MUTIC_MAX_CORR",
    "NORMAL_KURTOSIS",
    "reproducibility_probe",
    "deployment_gate",
    "deployment_gate_from_checks",
    # perturbation / robustness battery (event and monthly series)
    "robustness_battery",
    "leave_k_best_out",
    "leave_k_worst_out",
    "drop_fraction_sweep",
    "leave_one_era_out",
    "concentration_profile",
    "ROBUST",
    "FRAGILE",
    "SINGLE_EVENT_DRIVEN",
    "FLAG_FEW_WINNERS",
    "FLAG_FEW_LOSERS",
    "FLAG_UNSTABLE_RESAMPLE",
    "FLAG_BY_ERA",
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
    # mechanism-side due diligence: rubric + hypothesis registry
    "DIMENSIONS",
    "DIMENSION_WEIGHTS",
    "BAND_ALLOW",
    "BAND_STRENGTHEN",
    "BAND_REJECT",
    "BAND_THRESHOLDS",
    "RubricResult",
    "score",
    "band_for",
    "weakest_dimensions",
    "render_rubric_markdown",
    "STATUS_PROPOSED",
    "STATUS_REGISTERED",
    "STATUS_EVALUATING",
    "STATUS_RESOLVED",
    "STATUS_REJECTED",
    "RegistryValidationError",
    "DuplicateHypothesisError",
    "PreRegistrationDataError",
    "utc_now_iso",
    "build_record",
    "append_hypothesis",
    "load_registry",
    "get_hypothesis",
    "check_no_pre_registration_data",
    "sample_sufficiency",
    "survival_label",
    "registry_stats",
]
