"""DCA backtest engine: daily NAV simulation with monthly RMB contributions.

The simulation iterates over trading days. On contribution days (one per month,
first trading day on/after BacktestConfig.contribution_day):
    1. convert RMB to USD at the historical USD/CNY rate minus an FX spread;
    2. ask the strategy for target weights;
    3. buy each instrument, paying spread + commission;
    4. optionally rebalance the whole portfolio back to the target weights
       (paying spread on traded notional).
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from .config import BacktestConfig, InstrumentSpec
from .strategy import Strategy, StrategyContext

_REBALANCE_MONTHS = {"monthly": 1, "annual": 12}


@dataclass
class BacktestResult:
    strategy_name: str
    config: BacktestConfig
    start: pd.Timestamp
    end: pd.Timestamp
    months: int
    total_contrib_rmb: float
    final_nav_usd: float
    final_nav_rmb: float
    hit_date: pd.Timestamp | None
    daily: pd.DataFrame | None = None
    trades: pd.DataFrame | None = None

    @property
    def multiple(self) -> float:
        return self.final_nav_rmb / self.total_contrib_rmb if self.total_contrib_rmb else float("nan")


def contribution_dates(index: pd.DatetimeIndex, day: int) -> list[pd.Timestamp]:
    """First trading day of each month with day-of-month >= `day`."""
    df = pd.DataFrame({"d": index})
    df["y"] = df["d"].dt.year
    df["m"] = df["d"].dt.month
    df["dom"] = df["d"].dt.day
    out: list[pd.Timestamp] = []
    for _, g in df.groupby(["y", "m"], sort=True):
        cands = g[g["dom"] >= day]
        out.append(cands["d"].iloc[0] if len(cands) else g["d"].iloc[-1])
    return out


def run_backtest(
    levels: pd.DataFrame,
    fx: pd.Series,
    cfg: BacktestConfig,
    strategy: Strategy,
    instruments: dict[str, InstrumentSpec],
    start: pd.Timestamp | str | None = None,
    end: pd.Timestamp | str | None = None,
    target_rmb: float | None = None,
    record_daily: bool = True,
) -> BacktestResult:
    """Run one DCA simulation over [start, end].

    Set target_rmb to stop early once NAV (RMB) crosses the target.
    Set record_daily=False to skip bookkeeping (used by rolling analysis).
    """
    lv = levels.loc[start:end].dropna(how="any")
    if len(lv) < 2:
        raise ValueError("no overlapping price data in the requested window")

    cols = list(lv.columns)
    fx_arr = fx.astype(float).reindex(lv.index).ffill().bfill().to_numpy()
    fac = (lv.pct_change(fill_method=None).fillna(0.0) + 1.0).to_numpy()

    col_idx = {c: i for i, c in enumerate(cols)}
    vals = np.zeros(len(cols))
    cdate_to_month = {d: i for i, d in enumerate(contribution_dates(lv.index, cfg.contribution_day))}

    rebal_every = _REBALANCE_MONTHS.get(cfg.rebalance, 0)
    fx_spread = cfg.fx_spread_bps / 1e4

    strategy.reset()
    cum_rmb = 0.0
    months = 0
    hit_date: pd.Timestamp | None = None
    rows: list[tuple] = []
    trades: list[dict] = []
    last_i = 0

    for i, date in enumerate(lv.index):
        if i:
            vals *= fac[i]

        contrib_today = 0.0
        mi = cdate_to_month.get(date)
        if mi is not None:
            amount_rmb = cfg.monthly_contribution_rmb * (
                1.0 + cfg.contribution_growth_annual
            ) ** (months / 12.0)
            cum_rmb += amount_rmb
            contrib_today = amount_rmb
            usd_gross = amount_rmb / fx_arr[i] * (1.0 - fx_spread)

            ctx = StrategyContext(
                date=date,
                values={c: float(v) for c, v in zip(cols, vals)},
                nav_usd=float(vals.sum()),
                cum_contrib_rmb=cum_rmb,
                month_index=months,
                levels=lv,
                fx=pd.Series(fx_arr, index=lv.index),
            )
            w = strategy.target_weights(ctx)
            w = {k: v for k, v in w.items() if k in col_idx and v > 0}
            wsum = sum(w.values())
            if wsum > 0:
                w = {k: v / wsum for k, v in w.items()}
                for k, wk in w.items():
                    spec = instruments[k]
                    gross = usd_gross * wk
                    cost = gross * spec.spread_bps / 1e4 + (spec.commission_usd if gross > 0 else 0.0)
                    vals[col_idx[k]] += gross - cost
                    if record_daily:
                        trades.append(
                            {
                                "date": date,
                                "type": "buy",
                                "symbol": k,
                                "usd_gross": gross,
                                "usd_cost": cost,
                                "rmb_amount": amount_rmb * wk,
                            }
                        )
                if rebal_every and months > 0 and months % rebal_every == 0:
                    nav = float(vals.sum())
                    for k, wk in w.items():
                        cur = vals[col_idx[k]]
                        tgt = nav * wk
                        traded = abs(tgt - cur)
                        cost = traded * instruments[k].spread_bps / 1e4
                        vals[col_idx[k]] = tgt - np.sign(tgt - cur) * cost
                        if record_daily and traded > 1e-9:
                            trades.append(
                                {
                                    "date": date,
                                    "type": "rebalance",
                                    "symbol": k,
                                    "usd_gross": traded,
                                    "usd_cost": cost,
                                    "rmb_amount": 0.0,
                                }
                            )
            months += 1

        nav_usd = float(vals.sum())
        nav_rmb = nav_usd * fx_arr[i]
        last_i = i
        if record_daily:
            rows.append((date, nav_usd, float(fx_arr[i]), nav_rmb, cum_rmb, contrib_today))

        if target_rmb is not None and nav_rmb >= target_rmb:
            hit_date = date
            break

    daily = None
    trades_df = None
    if record_daily:
        daily = pd.DataFrame(
            rows,
            columns=["date", "nav_usd", "usdcny", "nav_rmb", "cum_contrib_rmb", "contrib_rmb"],
        ).set_index("date")
        trades_df = pd.DataFrame(trades)

    return BacktestResult(
        strategy_name=strategy.name,
        config=cfg,
        start=lv.index[0],
        end=lv.index[last_i],
        months=months,
        total_contrib_rmb=cum_rmb,
        final_nav_usd=float(vals.sum()),
        final_nav_rmb=float(vals.sum()) * float(fx_arr[last_i]),
        hit_date=hit_date,
        daily=daily,
        trades=trades_df,
    )
