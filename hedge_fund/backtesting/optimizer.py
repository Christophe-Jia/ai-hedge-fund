"""
Grid search parameter optimizer for BtcBacktestEngine / BacktestEngine.

Usage:
    from src.backtesting.optimizer import GridSearchOptimizer
    from src.backtesting.btc_engine import BtcBacktestEngine
    from src.strategies.crypto_ema_strategy import make_ema_agent

    optimizer = GridSearchOptimizer(
        engine_class=BtcBacktestEngine,
        strategy_factory=make_ema_agent,
        base_config={
            'tickers': ['BTC/USDT'],
            'start_date': '2024-01-01',
            'end_date': '2024-12-31',
            'initial_capital': 100_000.0,
        },
        param_grid={
            'fast_period': [8, 10, 12],
            'slow_period': [20, 25, 30],
        },
        objective='sharpe_ratio',
        n_workers=4,
        save_to_db=False,
    )
    results_df = optimizer.run()
    print(optimizer.best_params(3))
"""

from __future__ import annotations

import itertools
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Optional

import pandas as pd


# ---------------------------------------------------------------------------
# Standalone worker function (must be at module level for pickling)
# ---------------------------------------------------------------------------

def _run_single_combination(
    engine_class_dotpath: str,
    factory_dotpath: str,
    base_config: dict,
    params: dict,
) -> tuple[dict, dict]:
    """
    Worker function executed in a subprocess.

    Returns (params_dict, metrics_dict).
    """
    import importlib

    # Dynamically import engine class
    module_path, class_name = engine_class_dotpath.rsplit(".", 1)
    engine_cls = getattr(importlib.import_module(module_path), class_name)

    # Dynamically import factory function
    fmod_path, fname = factory_dotpath.rsplit(".", 1)
    factory_fn = getattr(importlib.import_module(fmod_path), fname)

    # Build agent
    agent = factory_fn(**params)

    # Build engine kwargs
    engine_kwargs = dict(base_config)
    engine_kwargs["agent"] = agent
    engine_kwargs.setdefault("model_name", "")
    engine_kwargs.setdefault("model_provider", "")
    engine_kwargs.setdefault("selected_analysts", None)
    engine_kwargs.setdefault("initial_margin_requirement", 0.0)
    engine_kwargs.setdefault("price_only", True)
    engine_kwargs.setdefault("benchmark_ticker", None)

    try:
        engine = engine_cls(**engine_kwargs)
        metrics = engine.run_backtest()
    except Exception as exc:
        tb = traceback.format_exc()
        metrics = {"error": str(exc), "traceback": tb}

    return params, metrics


# ---------------------------------------------------------------------------
# Optimizer
# ---------------------------------------------------------------------------

class GridSearchOptimizer:
    """
    Cartesian-product grid search over strategy parameters.

    Args:
        engine_class:      Engine class (BtcBacktestEngine or BacktestEngine)
        strategy_factory:  Callable(**params) -> agent; e.g. make_ema_agent
        base_config:       Fixed engine parameters (start_date, end_date, etc.)
        param_grid:        Dict of param -> list of values to try
        objective:         Metric to sort by (default "sharpe_ratio")
        n_workers:         ProcessPoolExecutor workers (default 4)
        save_to_db:        If True, save each run to BacktestRunRepository
    """

    def __init__(
        self,
        engine_class,
        strategy_factory: Callable,
        base_config: dict,
        param_grid: dict,
        objective: str = "sharpe_ratio",
        n_workers: int = 4,
        save_to_db: bool = False,
    ):
        self.engine_class = engine_class
        self.strategy_factory = strategy_factory
        self.base_config = base_config
        self.param_grid = param_grid
        self.objective = objective
        self.n_workers = n_workers
        self.save_to_db = save_to_db
        self._results_df: Optional[pd.DataFrame] = None

        # Compute dotpaths for pickling
        self._engine_dotpath = f"{engine_class.__module__}.{engine_class.__name__}"
        self._factory_dotpath = f"{strategy_factory.__module__}.{strategy_factory.__qualname__}"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> pd.DataFrame:
        """Run all parameter combinations and return results DataFrame."""
        combinations = self._build_param_combinations()
        total = len(combinations)
        print(f"[optimizer] Running {total} combinations ({self.n_workers} workers)...")

        rows: list[dict] = []

        with ProcessPoolExecutor(max_workers=self.n_workers) as executor:
            futures = {
                executor.submit(
                    _run_single_combination,
                    self._engine_dotpath,
                    self._factory_dotpath,
                    self.base_config,
                    params,
                ): params
                for params in combinations
            }

            done = 0
            for future in as_completed(futures):
                done += 1
                params_used = futures[future]
                try:
                    params_result, metrics = future.result()
                    row = dict(params_result)
                    if "error" not in metrics:
                        row.update({
                            "sharpe_ratio": metrics.get("sharpe_ratio"),
                            "sortino_ratio": metrics.get("sortino_ratio"),
                            "max_drawdown": metrics.get("max_drawdown"),
                            "calmar_ratio": metrics.get("calmar_ratio"),
                            "total_return": metrics.get("total_return"),
                        })
                    else:
                        row.update({
                            "sharpe_ratio": None,
                            "sortino_ratio": None,
                            "max_drawdown": None,
                            "calmar_ratio": None,
                            "total_return": None,
                            "error": metrics.get("error"),
                        })
                    rows.append(row)
                    pct = int(done / total * 100)
                    bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
                    print(f"\r  [{bar}] {pct}% ({done}/{total})", end="", flush=True)
                except Exception as exc:
                    row = dict(params_used)
                    row["error"] = str(exc)
                    rows.append(row)

        print()  # newline after progress bar

        df = pd.DataFrame(rows)
        if self.objective in df.columns:
            df = df.sort_values(self.objective, ascending=False, na_position="last")
        df = df.reset_index(drop=True)
        self._results_df = df

        if self.save_to_db:
            self._persist_to_db(rows)

        return df

    def best_params(self, n: int = 3) -> list[dict]:
        """Return top-N parameter sets sorted by objective metric."""
        if self._results_df is None:
            raise RuntimeError("Call run() before best_params()")
        df = self._results_df
        if self.objective in df.columns:
            df = df.sort_values(self.objective, ascending=False, na_position="last")
        records = df.head(n).to_dict(orient="records")
        # Return only parameter columns (exclude metric columns)
        metric_cols = {"sharpe_ratio", "sortino_ratio", "max_drawdown", "calmar_ratio", "total_return", "error"}
        param_cols = [c for c in df.columns if c not in metric_cols]
        return [{k: r[k] for k in param_cols if k in r} for r in records]

    def plot_heatmap(
        self,
        x_param: str,
        y_param: str,
        metric: str = "sharpe_ratio",
        save_path: Optional[str] = None,
    ) -> None:
        """
        Plot a 2D heatmap of metric values over a 2-parameter grid.

        Requires matplotlib. Saves to save_path if provided.
        """
        if self._results_df is None:
            raise RuntimeError("Call run() before plot_heatmap()")

        try:
            import matplotlib.pyplot as plt
            import numpy as np
        except ImportError:
            print("[optimizer] matplotlib not installed; skipping heatmap")
            return

        df = self._results_df
        if metric not in df.columns:
            print(f"[optimizer] Metric '{metric}' not in results; available: {list(df.columns)}")
            return

        x_vals = sorted(df[x_param].unique())
        y_vals = sorted(df[y_param].unique())

        # Build matrix
        matrix = np.full((len(y_vals), len(x_vals)), float("nan"))
        for _, row in df.iterrows():
            xi = x_vals.index(row[x_param])
            yi = y_vals.index(row[y_param])
            val = row[metric]
            if pd.notna(val):
                matrix[yi, xi] = val

        fig, ax = plt.subplots(figsize=(max(6, len(x_vals)), max(4, len(y_vals))))
        im = ax.imshow(matrix, aspect="auto", cmap="RdYlGn")
        ax.set_xticks(range(len(x_vals)))
        ax.set_xticklabels(x_vals)
        ax.set_yticks(range(len(y_vals)))
        ax.set_yticklabels(y_vals)
        ax.set_xlabel(x_param)
        ax.set_ylabel(y_param)
        ax.set_title(f"{metric} heatmap")
        fig.colorbar(im, ax=ax, label=metric)

        # Annotate cells
        for yi in range(len(y_vals)):
            for xi in range(len(x_vals)):
                v = matrix[yi, xi]
                if not np.isnan(v):
                    ax.text(xi, yi, f"{v:.2f}", ha="center", va="center", fontsize=8)

        plt.tight_layout()
        if save_path:
            plt.savefig(save_path, dpi=120)
            print(f"[optimizer] Heatmap saved: {save_path}")
        else:
            plt.show()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _build_param_combinations(self) -> list[dict]:
        """Cartesian product of all parameter values."""
        keys = list(self.param_grid.keys())
        values = [self.param_grid[k] for k in keys]
        return [dict(zip(keys, combo)) for combo in itertools.product(*values)]

    def _persist_to_db(self, rows: list[dict]) -> None:
        """Batch-save optimization results to BacktestRun table."""
        try:
            from sqlalchemy.orm import Session
            from app.backend.database.connection import SessionLocal
            from app.backend.database.repositories import BacktestRunRepository

            metric_cols = {"sharpe_ratio", "sortino_ratio", "max_drawdown", "calmar_ratio", "total_return", "error"}
            db: Session = SessionLocal()
            try:
                repo = BacktestRunRepository(db)
                for row in rows:
                    params = {k: v for k, v in row.items() if k not in metric_cols}
                    metrics = {k: v for k, v in row.items() if k in metric_cols and k != "error"}
                    if "error" in row:
                        continue  # Skip failed runs
                    repo.save(
                        engine_type="optimization",
                        tickers=self.base_config.get("tickers", []),
                        start_date=self.base_config.get("start_date", ""),
                        end_date=self.base_config.get("end_date", ""),
                        initial_capital=self.base_config.get("initial_capital", 0),
                        model_name=None,
                        selected_analysts=None,
                        performance_metrics=metrics,
                        portfolio_value_series=[],
                        extra_params=params,
                    )
            finally:
                db.close()
        except Exception as exc:
            print(f"[optimizer] DB persist error: {exc}")
