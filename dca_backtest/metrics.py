"""Performance metrics and rolling-window target analysis.

The key question this answers: "If I had started DCA in <any historical month>,
what is the probability my portfolio reaches the target RMB value within N
years?" Each historical start month is simulated independently; the hit rate
across all eligible starts is the empirical probability.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from .config import BacktestConfig, InstrumentSpec
from .engine import contribution_dates, run_backtest
from .strategy import Strategy


def xirr(flows: list[tuple[pd.Timestamp, float]], guesses=(-0.99, 10.0)) -> float | None:
    """Money-weighted annualized return via bisection. Negative = invested."""
    if not flows:
        return None
    signs = {1 if a > 0 else -1 for _, a in flows}
    if len(signs) == 1:
        return None
    t0 = flows[0][0]

    def npv(rate: float) -> float:
        return sum(a / (1.0 + rate) ** ((d - t0).days / 365.25) for d, a in flows)

    lo, hi = guesses
    f_lo, f_hi = npv(lo), npv(hi)
    if f_lo * f_hi > 0:
        return None
    for _ in range(200):
        mid = (lo + hi) / 2.0
        f_mid = npv(mid)
        if abs(f_mid) < 1e-7:
            return mid
        if f_lo * f_mid < 0:
            hi, f_hi = mid, f_mid
        else:
            lo, f_lo = mid, f_mid
    return (lo + hi) / 2.0


def max_drawdown(nav: pd.Series) -> float:
    """Max peak-to-trough decline of the NAV series.

    Note: with ongoing contributions the NAV series is not a pure return index,
    so this is a conservative approximation of the pain an investor would see.
    """
    if len(nav) == 0:
        return 0.0
    dd = nav / nav.cummax() - 1.0
    return float(dd.min())


def rolling_target_analysis(
    levels: pd.DataFrame,
    fx: pd.Series,
    cfg: BacktestConfig,
    strategy: Strategy,
    instruments: dict[str, InstrumentSpec],
    target_rmb: float,
    horizons: tuple[int, ...] = (10, 15, 20, 25, 30),
    progress: callable | None = None,
) -> tuple[pd.DataFrame, dict]:
    """Simulate DCA from every historical month-start; measure target hits.

    Returns (per_start_df, stats):
        per_start_df: start, hit_date, months_to_hit (NaN if never within data)
        stats: per-horizon hit probabilities (conditioned on enough data) plus
        overall completion-time statistics among starts that hit the target.
    """
    starts = contribution_dates(levels.index, 1)
    data_end = levels.index[-1]

    rows = []
    for i, s in enumerate(starts):
        res = run_backtest(
            levels,
            fx,
            cfg,
            strategy,
            instruments,
            start=s,
            target_rmb=target_rmb,
            record_daily=False,
        )
        if res.hit_date is not None:
            months = (res.hit_date.year - s.year) * 12 + (res.hit_date.month - s.month)
        else:
            months = None
        rows.append({"start": s, "hit_date": res.hit_date, "months_to_hit": months})
        if progress is not None and (i + 1) % 50 == 0:
            progress(i + 1, len(starts))

    df = pd.DataFrame(rows)
    end_ts = pd.Timestamp(data_end)
    stats: dict = {"horizons": {}}
    for h in horizons:
        cutoff = end_ts - pd.DateOffset(years=h)
        elig = df[df["start"] <= cutoff]
        if len(elig) == 0:
            continue
        hits = elig["months_to_hit"].notna() & (elig["months_to_hit"] <= h * 12)
        stats["horizons"][h] = {
            "eligible_starts": int(len(elig)),
            "hits": int(hits.sum()),
            "probability": float(hits.mean()),
        }

    hit_months = df["months_to_hit"].dropna()
    stats["overall"] = {
        "total_starts": int(len(df)),
        "ever_hit": int(hit_months.size),
        "ever_hit_rate": float(hit_months.size / len(df)) if len(df) else 0.0,
        "median_years": float(hit_months.median() / 12.0) if hit_months.size else None,
        "min_years": float(hit_months.min() / 12.0) if hit_months.size else None,
        "max_years": float(hit_months.max() / 12.0) if hit_months.size else None,
        "p10_years": float(hit_months.quantile(0.10) / 12.0) if hit_months.size else None,
        "p90_years": float(hit_months.quantile(0.90) / 12.0) if hit_months.size else None,
    }
    return df, stats
