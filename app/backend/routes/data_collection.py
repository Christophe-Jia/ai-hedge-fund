import asyncio
import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.backend.services.process_manager import ScriptStatus, process_manager

router = APIRouter(prefix="/data-collection")

PROJECT_ROOT = Path(__file__).resolve().parents[3]  # repo root
DATA_DIR = PROJECT_ROOT / "data"

SCRIPTS: Dict[str, Dict[str, Any]] = {
    "seed_btc_history": {
        "label": "Seed BTC History",
        "description": "Backfill 3 years of BTC OHLCV + funding rate data",
        "cmd": ["poetry", "run", "python", "scripts/seed_btc_history.py", "--years", "3"],
    },
    "backfill_perp_ohlcv": {
        "label": "Backfill Perp OHLCV",
        "description": "Backfill perpetual futures OHLCV data",
        "cmd": ["poetry", "run", "python", "scripts/backfill_perp_ohlcv.py"],
    },
    "backfill_onchain": {
        "label": "Backfill Onchain",
        "description": "Incrementally backfill on-chain metrics",
        "cmd": ["poetry", "run", "python", "scripts/backfill_onchain.py", "--incremental"],
    },
    "collect_macro_data": {
        "label": "Collect Macro Data",
        "description": "Download macro economic indicators (DXY, VIX, etc.)",
        "cmd": ["poetry", "run", "python", "scripts/collect_macro_data.py"],
    },
    "collect_orderbook": {
        "label": "Collect Orderbook",
        "description": "Long-running daemon: stream BTC/USDT order book + trades",
        "cmd": [
            "poetry", "run", "python", "scripts/collect_orderbook.py",
            "--symbols", "BTC/USDT", "--exchange", "gate",
        ],
    },
    "collect_crypto_data": {
        "label": "Collect Crypto Data",
        "description": "Download recent crypto OHLCV from Binance",
        "cmd": ["poetry", "run", "python", "scripts/collect_crypto_data.py"],
    },
    "collect_polymarket_ticks": {
        "label": "Collect Polymarket Ticks",
        "description": "Long-running daemon: stream Polymarket prediction market ticks",
        "cmd": ["poetry", "run", "python", "scripts/collect_polymarket_ticks.py"],
    },
}


# ---------------------------------------------------------------------------
# Helper: read SQLite store stats without blocking event loop
# ---------------------------------------------------------------------------

def _query_sqlite_stats() -> List[Dict[str, Any]]:
    rows = []

    # btc_history.db
    db_path = DATA_DIR / "btc_history.db"
    if db_path.exists():
        size = db_path.stat().st_size
        conn = sqlite3.connect(str(db_path))
        try:
            for table, ts_col in [("ohlcv", "ts"), ("funding_rates", "ts")]:
                try:
                    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    latest = conn.execute(f"SELECT MAX({ts_col}) FROM {table}").fetchone()[0]
                    rows.append({
                        "store": "btc_history",
                        "table": table,
                        "rows": count,
                        "latest_ts": latest,
                        "ts_is_seconds": True,
                        "size_bytes": size,
                    })
                except Exception:
                    pass
        finally:
            conn.close()

    # onchain_metrics.db
    db_path = DATA_DIR / "onchain_metrics.db"
    if db_path.exists():
        size = db_path.stat().st_size
        conn = sqlite3.connect(str(db_path))
        try:
            count = conn.execute("SELECT COUNT(*) FROM onchain_metrics").fetchone()[0]
            latest = conn.execute("SELECT MAX(ts_ms) FROM onchain_metrics").fetchone()[0]
            rows.append({
                "store": "onchain_metrics",
                "table": "onchain_metrics",
                "rows": count,
                "latest_ts": latest,
                "ts_is_seconds": False,
                "size_bytes": size,
            })
        except Exception:
            pass
        finally:
            conn.close()

    # orderbook_trades.db
    db_path = DATA_DIR / "orderbook_trades.db"
    if db_path.exists():
        size = db_path.stat().st_size
        conn = sqlite3.connect(str(db_path))
        try:
            for table, ts_col in [("trades", "ts_ms"), ("order_book_snapshots", "ts_ms")]:
                try:
                    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    latest = conn.execute(f"SELECT MAX({ts_col}) FROM {table}").fetchone()[0]
                    rows.append({
                        "store": "orderbook_trades",
                        "table": table,
                        "rows": count,
                        "latest_ts": latest,
                        "ts_is_seconds": False,
                        "size_bytes": size,
                    })
                except Exception:
                    pass
        finally:
            conn.close()

    # polymarket_ticks.db
    db_path = DATA_DIR / "polymarket_ticks.db"
    if db_path.exists():
        size = db_path.stat().st_size
        conn = sqlite3.connect(str(db_path))
        try:
            count = conn.execute("SELECT COUNT(*) FROM price_ticks").fetchone()[0]
            latest = conn.execute("SELECT MAX(ts) FROM price_ticks").fetchone()[0]
            rows.append({
                "store": "polymarket_ticks",
                "table": "price_ticks",
                "rows": count,
                "latest_ts": latest,
                "ts_is_seconds": True,
                "size_bytes": size,
            })
        except Exception:
            pass
        finally:
            conn.close()

    # macro json files
    macro_dir = DATA_DIR / "macro"
    if macro_dir.exists():
        for json_file in sorted(macro_dir.glob("*.json")):
            rows.append({
                "store": "macro",
                "table": json_file.name,
                "rows": None,
                "latest_ts": None,
                "ts_is_seconds": False,
                "size_bytes": json_file.stat().st_size,
            })

    return rows


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/status")
async def get_status():
    """Return row counts, latest timestamps, and file sizes for all data stores."""
    stores = await asyncio.to_thread(_query_sqlite_stats)
    return {
        "stores": stores,
        "timestamp": datetime.utcnow().isoformat(),
    }


@router.get("/processes")
async def get_processes():
    """Return status of all registered scripts (idle for those never started)."""
    result = []
    for name, info in SCRIPTS.items():
        entry = process_manager.get(name)
        result.append({
            "name": name,
            "label": info["label"],
            "description": info["description"],
            "status": entry.status,
            "started_at": entry.started_at.isoformat() if entry.started_at else None,
            "finished_at": entry.finished_at.isoformat() if entry.finished_at else None,
            "exit_code": entry.exit_code,
        })
    return {"processes": result}


class RunRequest(BaseModel):
    args: List[str] = []


@router.post("/run/{script_name}")
async def run_script(script_name: str, body: RunRequest, request: Request):
    """Start a script and stream its stdout/stderr as SSE events."""
    if script_name not in SCRIPTS:
        raise HTTPException(status_code=404, detail=f"Unknown script: {script_name}")

    if process_manager.is_running(script_name):
        raise HTTPException(status_code=409, detail=f"Script '{script_name}' is already running")

    script_info = SCRIPTS[script_name]
    cmd = script_info["cmd"] + list(body.args)

    async def event_generator():
        process = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=str(PROJECT_ROOT),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env={**os.environ},
        )
        process_manager.register_start(script_name, process)
        yield f"event: start\ndata: {json.dumps({'script': script_name})}\n\n"

        while True:
            if await request.is_disconnected():
                process.terminate()
                break
            try:
                line = await asyncio.wait_for(process.stdout.readline(), timeout=1.0)
            except asyncio.TimeoutError:
                if process.returncode is not None:
                    break
                continue
            if not line:
                break
            yield f"event: log\ndata: {json.dumps({'line': line.decode().rstrip()})}\n\n"

        exit_code = await process.wait()
        process_manager.register_finish(script_name, exit_code)
        status = "done" if exit_code == 0 else "error"
        yield f"event: complete\ndata: {json.dumps({'exit_code': exit_code, 'status': status})}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@router.delete("/run/{script_name}")
async def stop_script(script_name: str):
    """Send SIGTERM (then SIGKILL after 5s) to a running script."""
    if script_name not in SCRIPTS:
        raise HTTPException(status_code=404, detail=f"Unknown script: {script_name}")

    if not process_manager.is_running(script_name):
        raise HTTPException(status_code=409, detail=f"Script '{script_name}' is not running")

    killed = await process_manager.kill(script_name)
    return {"killed": killed, "script": script_name}


# ---------------------------------------------------------------------------
# Polymarket data endpoints
# ---------------------------------------------------------------------------

def _get_polymarket_store():
    """Lazily import and instantiate PolymarketTickStore."""
    from src.data.polymarket_tick_store import PolymarketTickStore
    return PolymarketTickStore()


@router.get("/polymarket/markets")
async def list_polymarket_markets():
    """List all tracked Polymarket prediction markets."""
    try:
        store = await asyncio.to_thread(_get_polymarket_store)
        markets = await asyncio.to_thread(store.list_markets)
        return {"markets": markets}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/polymarket/markets/{token_id}/ticks")
async def get_polymarket_ticks(
    token_id: str,
    start_ts: Optional[int] = None,
    end_ts: Optional[int] = None,
):
    """Get price ticks for a specific Polymarket market (for charting)."""
    try:
        import time
        store = await asyncio.to_thread(_get_polymarket_store)
        s = start_ts or int(time.time()) - 7 * 86400  # default: last 7 days
        e = end_ts or int(time.time())
        ticks = await asyncio.to_thread(store.get_ticks, token_id, s, e)
        return {
            "token_id": token_id,
            "ticks": [{"ts": t[0], "price": t[1]} for t in ticks],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/polymarket/signal")
async def get_polymarket_signal(tickers: str = "BTC/USDT"):
    """
    Compute the current Polymarket-derived macro signal.
    Returns the same structure as polymarket_signal_agent().
    """
    try:
        from src.graph.state import AgentState
        from src.agents.crypto.polymarket_signal_agent import polymarket_signal_agent

        ticker_list = [t.strip() for t in tickers.split(",") if t.strip()]
        state: AgentState = {
            "messages": [],
            "data": {
                "tickers": ticker_list,
                "analyst_signals": {},
                "portfolio": {},
                "start_date": "",
                "end_date": "",
            },
            "metadata": {"show_reasoning": False, "model_name": "", "model_provider": ""},
        }
        result = await asyncio.to_thread(polymarket_signal_agent, state)
        signals = result.get("data", {}).get("analyst_signals", {}).get("polymarket_signal_agent", {})
        return {"signal": signals}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
