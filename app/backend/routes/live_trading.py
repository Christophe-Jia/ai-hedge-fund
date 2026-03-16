"""
Live Trading REST + SSE routes.

Endpoints:
    POST   /live-trading/start     – start a trading session
    DELETE /live-trading/stop      – stop the active session
    GET    /live-trading/status    – current session status
    GET    /live-trading/positions – position snapshot
    GET    /live-trading/orders    – order history (last 100)
    GET    /live-trading/stream    – SSE: real-time monitor updates

The default session_id is "main".  A session_id query-param can be added
later for multi-session support.
"""

from __future__ import annotations

import asyncio
import json
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.backend.services.live_trading_manager import (
    LiveTradingConfig,
    LiveTradingManager,
)

router = APIRouter(prefix="/live-trading")

_DEFAULT_SESSION = "main"


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class StartRequest(BaseModel):
    market: str = "crypto"
    tickers: list[str] = ["BTC/USDT"]
    interval_minutes: int = 60
    paper: bool = True
    model_name: str = "gpt-4o"
    model_provider: str = "openai"
    exchange_id: Optional[str] = None
    session_id: str = _DEFAULT_SESSION


class StatusResponse(BaseModel):
    session_id: str
    status: str
    started_at: Optional[str] = None
    error_message: Optional[str] = None
    config: Optional[dict] = None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.post("/start")
async def start_live_trading(body: StartRequest):
    """Start a live trading session."""
    manager = LiveTradingManager.get_instance()
    config = LiveTradingConfig(
        market=body.market,
        tickers=body.tickers,
        interval_minutes=body.interval_minutes,
        paper=body.paper,
        model_name=body.model_name,
        model_provider=body.model_provider,
        exchange_id=body.exchange_id,
    )
    try:
        manager.start(body.session_id, config)
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"message": "started", "session_id": body.session_id}


@router.delete("/stop")
async def stop_live_trading(session_id: str = Query(default=_DEFAULT_SESSION)):
    """Stop the active live trading session."""
    manager = LiveTradingManager.get_instance()
    try:
        manager.stop(session_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"No session '{session_id}'")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"message": "stopped", "session_id": session_id}


@router.get("/status", response_model=StatusResponse)
async def get_status(session_id: str = Query(default=_DEFAULT_SESSION)):
    """Get the current session status."""
    manager = LiveTradingManager.get_instance()
    status = manager.get_status(session_id)
    config_dict = None
    if status.config:
        config_dict = {
            "market": status.config.market,
            "tickers": status.config.tickers,
            "interval_minutes": status.config.interval_minutes,
            "paper": status.config.paper,
            "model_name": status.config.model_name,
        }
    return StatusResponse(
        session_id=status.session_id,
        status=status.status,
        started_at=status.started_at,
        error_message=status.error_message,
        config=config_dict,
    )


@router.get("/positions")
async def get_positions(session_id: str = Query(default=_DEFAULT_SESSION)):
    """Get current position snapshot."""
    manager = LiveTradingManager.get_instance()
    return {"session_id": session_id, "positions": manager.get_positions(session_id)}


@router.get("/orders")
async def get_orders(session_id: str = Query(default=_DEFAULT_SESSION)):
    """Get order history (last 100)."""
    manager = LiveTradingManager.get_instance()
    return {"session_id": session_id, "orders": manager.get_order_history(session_id)}


@router.get("/stream")
async def stream_updates(request: Request, session_id: str = Query(default=_DEFAULT_SESSION)):
    """
    SSE endpoint that pushes monitor snapshots as they arrive.

    Each event is a JSON-encoded snapshot dict:
        data: {"timestamp": ..., "balance": ..., "open_orders": ...}
    """
    manager = LiveTradingManager.get_instance()

    async def event_generator():
        # Subscribe to monitor updates
        queue = manager.subscribe(session_id)
        if queue is None:
            yield f"data: {json.dumps({'error': 'no active session'})}\n\n"
            return

        try:
            while True:
                # Check for client disconnect
                if await request.is_disconnected():
                    break

                try:
                    snapshot = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield f"data: {json.dumps(snapshot, default=str)}\n\n"
                except asyncio.TimeoutError:
                    # Send keepalive comment
                    yield ": keepalive\n\n"
        finally:
            manager.unsubscribe(session_id, queue)

    return StreamingResponse(event_generator(), media_type="text/event-stream")
