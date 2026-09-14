"""Cross-sectional selection backtest engine.

Given a universe of daily close prices, a factor scorer, and a rebalance
schedule, simulates a long-only top-N equal-weight portfolio with IBKR
costs and strict no-look-ahead discipline:

  signal date (e.g. month-end close)
      -> scores computed from data STRICTLY BEFORE the signal date
      -> top-N selected
      -> execution at the close of `execution_lag_bars` trading days later

This is the stock-selection counterpart to the single-ticker agent loop in
src/backtesting/ — deliberately standalone and deterministic (no LLM calls)
so it can be walk-forward optimized later.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

import pandas as pd

from .costs import IbkrCostModel

# Factor function contract: (closes_df, as_of_timestamp) -> Series of scores
FactorFn = Callable[[pd.DataFrame, pd.Timestamp], pd.Series]


@dataclass
class SelectionConfig:
    """Selection parameters.

    `universe` can be:
      - list[str]: fixed universe (backward compatible)
      - dict[int, list[str]]: year -> tickers (point-in-time; the engine
        picks the correct list for each signal date's year)
    """
    universe: list[str] | dict[int, list[str]]
    start_date: str
    end_date: str
    rebalance_freq: str = "ME"        # pandas offset alias (ME = month end)
    top_n: int = 10
    execution_lag_bars: int = 1       # trade this many trading days after signal
    initial_capital: float = 100_000.0
    min_history_bars: int = 260       # required for factor eligibility
    max_weight_per_name: float = 1.0  # cap (1.0 = pure equal weight)
    cost_model: IbkrCostModel = field(default_factory=IbkrCostModel)


@dataclass
class SelectionResult:
    config: SelectionConfig
    equity: pd.Series                  # date -> portfolio value
    holdings: list[tuple[pd.Timestamp, list[str]]]  # (rebalance date, selected)
    turnover_notional: list[float]     # per rebalance, one side (sum |trades|/2)
    total_costs: float
    final_value: float
    metrics: dict = field(default_factory=dict)


class SelectionBacktest:
    """Deterministic cross-sectional top-N selection backtest."""

    def __init__(self, closes: pd.DataFrame, config: SelectionConfig,
                 factor: FactorFn | None = None) -> None:
        """
        Args:
            closes: DataFrame, index = trading dates, columns = tickers,
                    values = daily close. May contain NaN (suspended/new).
            config: selection parameters.
            factor: scorer; defaults to 12-1 momentum.
        """
        self._closes = closes.sort_index()
        self._cfg = config
        if factor is None:
            from .factors import momentum_12_1
            factor = momentum_12_1
        self._factor = factor

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> SelectionResult:
        cfg = self._cfg
        closes = self._closes
        trading_days = closes.index
        window = trading_days[
            (trading_days >= pd.Timestamp(cfg.start_date, tz=trading_days.tz))
            & (trading_days <= pd.Timestamp(cfg.end_date, tz=trading_days.tz))
        ]
        if len(window) < cfg.min_history_bars // 2:
            raise ValueError(f"window too short: {len(window)} trading days")

        # Rebalance signal dates inside the window
        sig_idx = pd.date_range(
            pd.Timestamp(cfg.start_date, tz=trading_days.tz),
            pd.Timestamp(cfg.end_date, tz=trading_days.tz),
            freq=cfg.rebalance_freq,
        )
        signal_set: set[pd.Timestamp] = set()
        for sd in sig_idx:
            pos = trading_days.searchsorted(sd, side="right") - 1
            if pos >= 0 and trading_days[pos] <= window[-1]:
                signal_set.add(trading_days[pos])

        cash = cfg.initial_capital
        shares: dict[str, float] = {}
        equity_values: list[float] = []
        equity_index: list[pd.Timestamp] = []
        holdings: list[tuple[pd.Timestamp, list[str]]] = []
        turnover_list: list[float] = []
        total_costs = 0.0

        pending: list[str] | None = None      # selection awaiting execution
        pending_exec_day: pd.Timestamp | None = None

        for day in window:
            prices_today = closes.loc[day]

            # --- execute pending rebalance at today's close ---------------
            if pending is not None and pending_exec_day is not None and day >= pending_exec_day:
                cash, shares, costs, turnover = self._rebalance(
                    cash, shares, pending, prices_today
                )
                total_costs += costs
                turnover_list.append(turnover)
                holdings.append((day, list(pending)))
                pending = None

            # --- value the book ------------------------------------------
            value = cash + self._book_value(shares, prices_today)
            equity_values.append(value)
            equity_index.append(day)

            # --- generate a new signal at today's close (executes later) --
            if day in signal_set and pending is None:
                selected = self._select(day)
                if selected:
                    exec_pos = trading_days.searchsorted(day) + cfg.execution_lag_bars
                    if exec_pos < len(trading_days):
                        pending = selected
                        pending_exec_day = trading_days[exec_pos]

        equity = pd.Series(equity_values, index=equity_index, name="portfolio")
        result = SelectionResult(
            config=cfg,
            equity=equity,
            holdings=holdings,
            turnover_notional=turnover_list,
            total_costs=total_costs,
            final_value=float(equity.iloc[-1]) if len(equity) else cfg.initial_capital,
        )
        result.metrics = self._compute_metrics(equity)
        return result

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _select(self, signal_day: pd.Timestamp) -> list[str]:
        cfg = self._cfg
        # point-in-time universe: dict year -> tickers
        if isinstance(cfg.universe, dict):
            year = signal_day.year
            # fall back to the nearest available year
            universe = cfg.universe.get(year)
            if universe is None:
                years = sorted(cfg.universe.keys())
                nearby = [y for y in years if y <= year]
                universe = cfg.universe[nearby[-1]] if nearby else cfg.universe[years[0]]
        else:
            universe = cfg.universe

        hist = self._closes[self._closes.index < signal_day]
        eligible = [
            s for s in universe
            if s in self._closes.columns
            and hist[s].dropna().shape[0] >= cfg.min_history_bars
        ]
        if len(eligible) <= cfg.top_n:
            return sorted(eligible)
        scores = self._factor(self._closes, signal_day)
        if scores.empty:
            return []
        scores = scores[scores.index.isin(eligible)].dropna()
        top = scores.sort_values(ascending=False).head(cfg.top_n)
        return sorted(top.index.tolist())

    def _book_value(self, shares: dict[str, float], prices: pd.Series) -> float:
        total = 0.0
        for sym, qty in shares.items():
            px = prices.get(sym)
            if pd.notna(px) and px > 0:
                total += qty * float(px)
        return total

    def _rebalance(
        self,
        cash: float,
        shares: dict[str, float],
        target: list[str],
        prices: pd.Series,
    ) -> tuple[float, dict[str, float], float, float]:
        """Trade current book into equal-weight `target` at today's prices.

        Returns (new_cash, new_shares, costs, one_side_turnover_notional).
        Symbols without a valid price today are carried over untouched.
        """
        cm = self._cfg.cost_model

        def valid_px(sym: str) -> bool:
            px = prices.get(sym)
            return px is not None and pd.notna(px) and px > 0

        # Positions we cannot price today are carried as-is.
        carried = {s: q for s, q in shares.items() if not valid_px(s)}
        tradable_target = [s for s in target if valid_px(s)]
        if not tradable_target:
            return cash, shares, 0.0, 0.0

        # Everything with a valid price gets reallocated to equal weight.
        realloc_value = cash + sum(
            q * float(prices[s]) for s, q in shares.items() if valid_px(s)
        )
        target_value_each = realloc_value / len(tradable_target)

        new_shares: dict[str, float] = dict(carried)
        costs = 0.0
        turnover_notional = 0.0

        # Sells first: exits + overweight trims free up cash.
        for sym, qty in shares.items():
            if not valid_px(sym):
                continue
            px = float(prices[sym])
            desired = target_value_each / px if sym in tradable_target else 0.0
            excess = qty - desired
            if excess > 1e-9:
                costs += cm.trade_cost(excess, px)
                cash += excess * px
                turnover_notional += excess * px
                qty -= excess
            if qty > 1e-9:
                new_shares[sym] = qty

        # Buys: underweight top-ups, capped by available cash.
        for sym in tradable_target:
            px = float(prices[sym])
            desired = target_value_each / px
            current = new_shares.get(sym, 0.0)
            delta = desired - current
            if delta * px <= 1.0:  # ignore dust
                continue
            cost = cm.trade_cost(delta, px)
            if delta * px + cost > cash:
                # scale down to what cash allows (costs already trim slightly)
                affordable = max((cash - cost) / px, 0.0)
                if affordable * px <= 1.0 or affordable <= 0:
                    continue
                delta = affordable
                cost = cm.trade_cost(delta, px)
            cash -= delta * px + cost
            costs += cost
            turnover_notional += delta * px
            new_shares[sym] = current + delta

        return cash, new_shares, costs, turnover_notional / 2.0

    def _compute_metrics(self, equity: pd.Series) -> dict:
        if len(equity) < 3:
            return {}
        from src.backtesting.metrics import PerformanceMetricsCalculator

        values = [
            {"Date": ts, "Portfolio Value": v} for ts, v in equity.items()
        ]
        calc = PerformanceMetricsCalculator(annual_trading_days=252, annual_rf_rate=0.0434)
        metrics: dict = dict(calc.compute_metrics(values) or {})
        calmar = calc.compute_calmar_ratio(values)
        if calmar is not None:
            metrics["calmar_ratio"] = calmar
        metrics["total_return"] = (
            equity.iloc[-1] / equity.iloc[0] - 1.0
        ) * 100.0 if equity.iloc[0] > 0 else None
        return metrics
