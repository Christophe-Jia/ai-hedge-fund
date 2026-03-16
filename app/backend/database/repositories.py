"""
Database repository layer for BacktestRun and AlertRecord.
"""
from __future__ import annotations

from typing import Optional
from sqlalchemy.orm import Session

from app.backend.database.models import BacktestRun, AlertRecord


class BacktestRunRepository:
    """CRUD for BacktestRun table."""

    def __init__(self, db: Session):
        self.db = db

    def save(
        self,
        engine_type: str,
        tickers: list[str],
        start_date: str,
        end_date: str,
        initial_capital: float,
        model_name: str | None,
        selected_analysts: list[str] | None,
        performance_metrics: dict,
        portfolio_value_series: list[dict],
        final_portfolio: dict | None = None,
        trade_records: list[dict] | None = None,
        extra_params: dict | None = None,
        name: str | None = None,
    ) -> BacktestRun:
        run = BacktestRun(
            name=name,
            engine_type=engine_type,
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
            model_name=model_name,
            selected_analysts=selected_analysts,
            extra_params=extra_params,
            performance_metrics=performance_metrics,
            portfolio_value_series=portfolio_value_series,
            trade_records=trade_records,
            final_portfolio=final_portfolio,
        )
        self.db.add(run)
        self.db.commit()
        self.db.refresh(run)
        return run

    def list(self, limit: int = 50, offset: int = 0) -> list[BacktestRun]:
        return (
            self.db.query(BacktestRun)
            .order_by(BacktestRun.id.desc())
            .offset(offset)
            .limit(limit)
            .all()
        )

    def get(self, run_id: int) -> Optional[BacktestRun]:
        return self.db.query(BacktestRun).filter(BacktestRun.id == run_id).first()

    def delete(self, run_id: int) -> bool:
        run = self.get(run_id)
        if not run:
            return False
        self.db.delete(run)
        self.db.commit()
        return True

    def update_name(self, run_id: int, name: str) -> Optional[BacktestRun]:
        run = self.get(run_id)
        if not run:
            return None
        run.name = name
        self.db.commit()
        self.db.refresh(run)
        return run


class AlertRecordRepository:
    """CRUD for AlertRecord table."""

    def __init__(self, db: Session):
        self.db = db

    def save(
        self,
        rule_id: str,
        severity: str,
        message: str,
        session_id: str | None = None,
        snapshot: dict | None = None,
    ) -> AlertRecord:
        record = AlertRecord(
            session_id=session_id,
            rule_id=rule_id,
            severity=severity,
            message=message,
            snapshot=snapshot,
            acknowledged=False,
        )
        self.db.add(record)
        self.db.commit()
        self.db.refresh(record)
        return record

    def list(
        self,
        severity: str | None = None,
        session_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[AlertRecord]:
        q = self.db.query(AlertRecord)
        if severity:
            q = q.filter(AlertRecord.severity == severity)
        if session_id:
            q = q.filter(AlertRecord.session_id == session_id)
        return q.order_by(AlertRecord.id.desc()).offset(offset).limit(limit).all()

    def acknowledge(self, record_id: int) -> Optional[AlertRecord]:
        record = self.db.query(AlertRecord).filter(AlertRecord.id == record_id).first()
        if not record:
            return None
        record.acknowledged = True
        self.db.commit()
        self.db.refresh(record)
        return record

    def unacknowledged_count(self, session_id: str | None = None) -> int:
        q = self.db.query(AlertRecord).filter(AlertRecord.acknowledged == False)  # noqa: E712
        if session_id:
            q = q.filter(AlertRecord.session_id == session_id)
        return q.count()
