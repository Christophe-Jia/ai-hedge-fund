"""
Alert management REST API.

GET    /alerts               – list alert history
PATCH  /alerts/{id}/acknowledge – mark alert as acknowledged
GET    /alerts/rules         – list current alert rule config
PATCH  /alerts/rules/{rule_id} – update a rule threshold
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session
from typing import Optional

from app.backend.database import get_db
from app.backend.database.repositories import AlertRecordRepository

router = APIRouter(prefix="/alerts")


class AlertListItem(BaseModel):
    id: int
    triggered_at: str
    session_id: Optional[str]
    rule_id: str
    severity: str
    message: Optional[str]
    acknowledged: bool

    model_config = {"from_attributes": True}


class UpdateThresholdRequest(BaseModel):
    threshold: float


def _to_item(record) -> AlertListItem:
    return AlertListItem(
        id=record.id,
        triggered_at=str(record.triggered_at),
        session_id=record.session_id,
        rule_id=record.rule_id,
        severity=record.severity,
        message=record.message,
        acknowledged=record.acknowledged,
    )


@router.get("", response_model=list[AlertListItem])
async def list_alerts(
    severity: Optional[str] = Query(default=None),
    session_id: Optional[str] = Query(default=None),
    limit: int = Query(default=100, le=500),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
):
    """List alert history."""
    repo = AlertRecordRepository(db)
    records = repo.list(severity=severity, session_id=session_id, limit=limit, offset=offset)
    return [_to_item(r) for r in records]


@router.patch("/{alert_id}/acknowledge")
async def acknowledge_alert(alert_id: int, db: Session = Depends(get_db)):
    """Mark an alert as acknowledged."""
    repo = AlertRecordRepository(db)
    record = repo.acknowledge(alert_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Alert {alert_id} not found")
    return _to_item(record)


@router.get("/rules")
async def get_alert_rules():
    """
    Return the current default alert rule configuration.
    Rules are currently defined in-process (src/live/alerting.py) rather than
    persisted in the DB, so this returns a static snapshot.
    """
    from src.live.alerting import make_default_rules
    rules = make_default_rules()
    return {
        "rules": [
            {
                "rule_id": r.rule_id,
                "name": r.name,
                "severity": r.severity,
                "cooldown_seconds": r.cooldown_seconds,
            }
            for r in rules
        ]
    }


@router.get("/unacknowledged-count")
async def get_unacknowledged_count(
    session_id: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
):
    """Get count of unacknowledged alerts (used by UI badge)."""
    repo = AlertRecordRepository(db)
    count = repo.unacknowledged_count(session_id=session_id)
    return {"count": count}
