"""
Backtest history REST API.

GET    /backtests          – list all runs (paginated)
GET    /backtests/{id}     – single run detail (with portfolio_value_series)
DELETE /backtests/{id}     – delete run
PATCH  /backtests/{id}/name – rename run
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session
from typing import Optional

from app.backend.database import get_db
from app.backend.database.repositories import BacktestRunRepository

router = APIRouter(prefix="/backtests")


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class BacktestRunListItem(BaseModel):
    id: int
    name: Optional[str]
    engine_type: str
    created_at: str
    tickers: Optional[list]
    start_date: Optional[str]
    end_date: Optional[str]
    initial_capital: Optional[float]
    model_name: Optional[str]
    sharpe_ratio: Optional[float] = None
    max_drawdown: Optional[float] = None
    total_return: Optional[float] = None

    model_config = {"from_attributes": True}


class BacktestRunDetail(BacktestRunListItem):
    portfolio_value_series: Optional[list] = None
    performance_metrics: Optional[dict] = None
    final_portfolio: Optional[dict] = None
    selected_analysts: Optional[list] = None
    extra_params: Optional[dict] = None

    model_config = {"from_attributes": True}


class RenameRequest(BaseModel):
    name: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_list_item(run) -> BacktestRunListItem:
    metrics = run.performance_metrics or {}
    return BacktestRunListItem(
        id=run.id,
        name=run.name,
        engine_type=run.engine_type,
        created_at=str(run.created_at),
        tickers=run.tickers,
        start_date=run.start_date,
        end_date=run.end_date,
        initial_capital=run.initial_capital,
        model_name=run.model_name,
        sharpe_ratio=metrics.get("sharpe_ratio"),
        max_drawdown=metrics.get("max_drawdown"),
        total_return=metrics.get("total_return"),
    )


def _to_detail(run) -> BacktestRunDetail:
    metrics = run.performance_metrics or {}
    # Serialize portfolio_value_series (dates may be datetime objects)
    pvs = run.portfolio_value_series or []
    pvs_serialized = []
    for pt in pvs:
        d = dict(pt)
        if "Date" in d:
            d["Date"] = str(d["Date"])
        pvs_serialized.append(d)

    return BacktestRunDetail(
        id=run.id,
        name=run.name,
        engine_type=run.engine_type,
        created_at=str(run.created_at),
        tickers=run.tickers,
        start_date=run.start_date,
        end_date=run.end_date,
        initial_capital=run.initial_capital,
        model_name=run.model_name,
        selected_analysts=run.selected_analysts,
        extra_params=run.extra_params,
        sharpe_ratio=metrics.get("sharpe_ratio"),
        max_drawdown=metrics.get("max_drawdown"),
        total_return=metrics.get("total_return"),
        performance_metrics=metrics,
        portfolio_value_series=pvs_serialized,
        final_portfolio=run.final_portfolio,
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("", response_model=list[BacktestRunListItem])
async def list_backtests(
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
):
    """List backtest history (summary, no portfolio_value_series)."""
    repo = BacktestRunRepository(db)
    runs = repo.list(limit=limit, offset=offset)
    return [_to_list_item(r) for r in runs]


@router.get("/{run_id}", response_model=BacktestRunDetail)
async def get_backtest(run_id: int, db: Session = Depends(get_db)):
    """Get full detail of a single backtest run."""
    repo = BacktestRunRepository(db)
    run = repo.get(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Backtest run {run_id} not found")
    return _to_detail(run)


@router.delete("/{run_id}")
async def delete_backtest(run_id: int, db: Session = Depends(get_db)):
    """Delete a backtest run."""
    repo = BacktestRunRepository(db)
    deleted = repo.delete(run_id)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Backtest run {run_id} not found")
    return {"message": "deleted", "id": run_id}


@router.patch("/{run_id}/name")
async def rename_backtest(run_id: int, body: RenameRequest, db: Session = Depends(get_db)):
    """Rename a backtest run."""
    repo = BacktestRunRepository(db)
    run = repo.update_name(run_id, body.name)
    if not run:
        raise HTTPException(status_code=404, detail=f"Backtest run {run_id} not found")
    return {"message": "renamed", "id": run_id, "name": run.name}
