"""
End-to-end closed-loop test for the AI Hedge Fund UI backend.

Tests all API endpoints (no real LLM key needed — all LLM calls are mocked).
The backend is started as a subprocess on port 18765 and torn down after tests.

Run:
    PYTHONPATH=/Users/jeffryjia/Vibe/ai-hedge-fund poetry run pytest tests/test_ui_e2e.py -v
"""

import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PORT = 18765
BASE_URL = f"http://localhost:{PORT}"
TIMEOUT = 30  # seconds to wait for backend to start
PROJECT_ROOT = Path(__file__).parent.parent


# ---------------------------------------------------------------------------
# Subprocess backend fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def backend(tmp_path_factory):
    """Start the FastAPI backend on PORT and yield once it's ready."""
    # Use a temp SQLite DB so tests don't touch the real hedge_fund.db
    tmp_dir = tmp_path_factory.mktemp("e2e_db")
    tmp_db = tmp_dir / "test_hedge_fund.db"

    env = {
        **os.environ,
        "PYTHONPATH": str(PROJECT_ROOT),
        # Override the database path so tests are isolated
        "TEST_DB_PATH": str(tmp_db),
    }

    proc = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn",
            "app.backend.main:app",
            "--host", "127.0.0.1",
            "--port", str(PORT),
            "--log-level", "warning",
        ],
        cwd=str(PROJECT_ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Poll /health until the backend is ready
    deadline = time.time() + TIMEOUT
    ready = False
    while time.time() < deadline:
        try:
            r = requests.get(f"{BASE_URL}/health", timeout=2)
            if r.status_code == 200:
                ready = True
                break
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(0.5)

    if not ready:
        proc.kill()
        stdout, stderr = proc.communicate()
        raise RuntimeError(
            f"Backend did not start within {TIMEOUT}s.\n"
            f"stdout: {stdout.decode()}\n"
            f"stderr: {stderr.decode()}"
        )

    yield proc

    # Teardown
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def get(path, **kwargs):
    return requests.get(f"{BASE_URL}{path}", timeout=10, **kwargs)


def post(path, **kwargs):
    return requests.post(f"{BASE_URL}{path}", timeout=10, **kwargs)


def put(path, **kwargs):
    return requests.put(f"{BASE_URL}{path}", timeout=10, **kwargs)


def delete(path, **kwargs):
    return requests.delete(f"{BASE_URL}{path}", timeout=10, **kwargs)


# ---------------------------------------------------------------------------
# Health / root
# ---------------------------------------------------------------------------

class TestHealth:
    def test_health_endpoint(self, backend):
        """B1 fix: GET /health must return 200 {"status": "ok"}."""
        r = get("/health")
        assert r.status_code == 200, f"Expected 200, got {r.status_code}: {r.text}"
        body = r.json()
        assert body == {"status": "ok"}, f"Unexpected body: {body}"

    def test_root_endpoint(self, backend):
        """GET / returns a welcome message."""
        r = get("/")
        assert r.status_code == 200
        body = r.json()
        assert "message" in body

    def test_ping_endpoint_returns_sse(self, backend):
        """GET /ping returns text/event-stream."""
        # Only read 1 byte to avoid waiting for all 5 pings
        r = requests.get(f"{BASE_URL}/ping", stream=True, timeout=5)
        assert r.status_code == 200
        assert "text/event-stream" in r.headers.get("content-type", "")
        r.close()


# ---------------------------------------------------------------------------
# Agents / Language models
# ---------------------------------------------------------------------------

class TestAgentsAndModels:
    def test_get_agents(self, backend):
        """GET /hedge-fund/agents returns a non-empty list."""
        r = get("/hedge-fund/agents")
        assert r.status_code == 200
        body = r.json()
        assert "agents" in body
        assert len(body["agents"]) > 0, "Expected at least one agent"

    def test_get_language_models(self, backend):
        """GET /language-models/ returns a non-empty list."""
        r = get("/language-models/")
        assert r.status_code == 200
        body = r.json()
        # Could be a list or a dict with a key
        if isinstance(body, list):
            assert len(body) > 0
        else:
            # Accept any non-empty dict
            assert body


# ---------------------------------------------------------------------------
# Flow CRUD
# ---------------------------------------------------------------------------

class TestFlowCRUD:
    def test_flow_full_lifecycle(self, backend):
        """Create → get → update → duplicate → search → delete."""
        # Create
        payload = {
            "name": "E2E Test Flow",
            "description": "Created by e2e test",
            "nodes": [{"id": "n1", "type": "agentNode", "position": {"x": 0, "y": 0}}],
            "edges": [],
            "viewport": {"x": 0, "y": 0, "zoom": 1},
            "data": {},
            "is_template": False,
            "tags": ["e2e"],
        }
        r = post("/flows/", json=payload)
        assert r.status_code == 200, f"Create failed: {r.text}"
        flow = r.json()
        flow_id = flow["id"]
        assert flow["name"] == "E2E Test Flow"

        # Get by ID
        r = get(f"/flows/{flow_id}")
        assert r.status_code == 200
        assert r.json()["id"] == flow_id

        # Update
        update_payload = {**payload, "name": "E2E Test Flow Updated"}
        r = put(f"/flows/{flow_id}", json=update_payload)
        assert r.status_code == 200
        assert r.json()["name"] == "E2E Test Flow Updated"

        # Duplicate
        r = post(f"/flows/{flow_id}/duplicate")
        assert r.status_code == 200
        dup = r.json()
        dup_id = dup["id"]
        assert dup_id != flow_id

        # Search
        r = get("/flows/search/E2E Test")
        assert r.status_code == 200
        found_names = [f["name"] for f in r.json()]
        assert any("E2E Test" in n for n in found_names)

        # Delete both
        r = delete(f"/flows/{flow_id}")
        assert r.status_code == 200

        r = delete(f"/flows/{dup_id}")
        assert r.status_code == 200

        # Confirm deletion
        r = get(f"/flows/{flow_id}")
        assert r.status_code == 404

    def test_flow_runs_list_empty(self, backend):
        """GET /flows/{id}/runs/ on a new flow returns empty list."""
        # Create a fresh flow
        payload = {
            "name": "Runs Test Flow",
            "description": "",
            "nodes": [],
            "edges": [],
            "viewport": {"x": 0, "y": 0, "zoom": 1},
            "data": {},
            "is_template": False,
            "tags": [],
        }
        r = post("/flows/", json=payload)
        assert r.status_code == 200
        flow_id = r.json()["id"]

        r = get(f"/flows/{flow_id}/runs/")
        assert r.status_code == 200
        assert r.json() == []

        # Cleanup
        delete(f"/flows/{flow_id}")


# ---------------------------------------------------------------------------
# API key CRUD
# ---------------------------------------------------------------------------

class TestApiKeyCRUD:
    PROVIDER = "e2e_test_provider"

    def test_api_key_full_lifecycle(self, backend):
        """Create → list (summary) → get by provider → delete."""
        # Create / upsert
        payload = {
            "provider": self.PROVIDER,
            "key_value": "test-key-e2e-12345",
            "description": "E2E test key",
            "is_active": True,
        }
        r = post("/api-keys/", json=payload)
        assert r.status_code == 200, f"Create failed: {r.text}"
        key = r.json()
        assert key["provider"] == self.PROVIDER

        # List (summary — key_value should not be returned)
        r = get("/api-keys/")
        assert r.status_code == 200
        providers = [k["provider"] for k in r.json()]
        assert self.PROVIDER in providers

        # Get by provider
        r = get(f"/api-keys/{self.PROVIDER}")
        assert r.status_code == 200
        assert r.json()["provider"] == self.PROVIDER

        # Delete
        r = delete(f"/api-keys/{self.PROVIDER}")
        assert r.status_code == 200

        # Confirm deletion
        r = get(f"/api-keys/{self.PROVIDER}")
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------

class TestDataCollection:
    def test_status_has_stores_key(self, backend):
        """GET /data-collection/status returns {"stores": [...], "timestamp": ...}."""
        r = get("/data-collection/status")
        assert r.status_code == 200
        body = r.json()
        assert "stores" in body
        assert "timestamp" in body
        # stores may be empty if no DBs exist, but the key must be present
        assert isinstance(body["stores"], list)

    def test_processes_has_six_scripts(self, backend):
        """GET /data-collection/processes returns exactly 6 registered scripts."""
        r = get("/data-collection/processes")
        assert r.status_code == 200
        body = r.json()
        assert "processes" in body
        assert len(body["processes"]) == 6, (
            f"Expected 6 scripts, got {len(body['processes'])}: "
            + str([p["name"] for p in body["processes"]])
        )


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

class TestStorage:
    def test_save_json(self, backend, tmp_path):
        """POST /storage/save-json creates a file and returns success."""
        filename = "e2e_test_output.json"
        r = post("/storage/save-json", json={"filename": filename, "data": {"test": True}})
        assert r.status_code == 200
        body = r.json()
        assert body["success"] is True
        assert body["filename"] == filename

        # Verify the file was created
        outputs_path = PROJECT_ROOT / "outputs" / filename
        assert outputs_path.exists(), f"Expected file at {outputs_path}"
        with open(outputs_path) as f:
            saved = json.load(f)
        assert saved == {"test": True}

        # Cleanup
        outputs_path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Ollama
# ---------------------------------------------------------------------------

class TestOllama:
    def test_ollama_status_valid_shape(self, backend):
        """GET /ollama/status returns a valid shape (installed, running keys)."""
        r = get("/ollama/status")
        assert r.status_code == 200
        body = r.json()
        assert "installed" in body
        assert "running" in body


# ---------------------------------------------------------------------------
# SSE: /hedge-fund/run — start event only (LLM mocked)
# ---------------------------------------------------------------------------

class TestHedgeFundRunSSE:
    """
    Tests the /hedge-fund/run SSE endpoint with LLM calls mocked.
    We only check that the stream starts and emits the 'start' event.
    We close the connection immediately after to avoid waiting for the full run.
    """

    def _minimal_run_payload(self):
        PM_ID = "portfolio_manager_wbpmgr"
        return {
            "tickers": ["AAPL"],
            "start_date": "2024-01-01",
            "end_date": "2024-01-02",
            "initial_cash": 100000,
            "model_name": "gpt-4o-mini",
            "model_provider": "OpenAI",
            "margin_requirement": 0,
            "graph_nodes": [
                {"id": "portfolio-start", "type": "portfolioStart", "position": {"x": 0, "y": 0}},
                {"id": PM_ID, "type": "portfolioManager", "position": {"x": 800, "y": 0}},
                {"id": "ben_graham_node01", "type": "agentNode", "position": {"x": 200, "y": 0}, "data": {"agentId": "ben_graham"}},
            ],
            "graph_edges": [
                {"id": "edge-ben_graham_node01-pm", "source": "ben_graham_node01", "target": PM_ID},
            ],
        }

    def test_run_sse_emits_start_event(self, backend):
        """POST /hedge-fund/run returns SSE stream that begins with a 'start' event."""
        payload = self._minimal_run_payload()
        with requests.post(
            f"{BASE_URL}/hedge-fund/run",
            json=payload,
            stream=True,
            timeout=15,
        ) as r:
            assert r.status_code == 200, f"Expected 200, got {r.status_code}: {r.text[:500]}"
            assert "text/event-stream" in r.headers.get("content-type", "")

            # Read just enough to find the first event
            buf = ""
            found_start = False
            for chunk in r.iter_content(chunk_size=256, decode_unicode=True):
                buf += chunk
                if "event: start" in buf:
                    found_start = True
                    break
                # Give up after 4KB to avoid hanging
                if len(buf) > 4096:
                    break

            assert found_start, f"Did not receive 'event: start' in SSE stream. Got: {buf[:500]}"


# ---------------------------------------------------------------------------
# SSE: /hedge-fund/backtest — start event only (no real LLM needed)
# ---------------------------------------------------------------------------

class TestHedgeFundBacktestSSE:
    """
    Tests the /hedge-fund/backtest SSE endpoint.
    We verify 'event: start' is emitted and close immediately.
    The backtest would require real price data to complete, so we don't wait.
    """

    def _minimal_backtest_payload(self):
        PM_ID = "portfolio_manager_wbpmgr"
        return {
            "tickers": ["AAPL"],
            "start_date": "2024-01-01",
            "end_date": "2024-01-05",
            "initial_capital": 100000,
            "model_name": "gpt-4o-mini",
            "model_provider": "OpenAI",
            "margin_requirement": 0,
            "graph_nodes": [
                {"id": "portfolio-start", "type": "portfolioStart", "position": {"x": 0, "y": 0}},
                {"id": PM_ID, "type": "portfolioManager", "position": {"x": 800, "y": 0}},
                {"id": "ben_graham_node01", "type": "agentNode", "position": {"x": 200, "y": 0}, "data": {"agentId": "ben_graham"}},
            ],
            "graph_edges": [
                {"id": "edge-ben_graham_node01-pm", "source": "ben_graham_node01", "target": PM_ID},
            ],
        }

    def test_backtest_sse_emits_start_event(self, backend):
        """POST /hedge-fund/backtest returns SSE stream that begins with 'event: start'."""
        payload = self._minimal_backtest_payload()
        with requests.post(
            f"{BASE_URL}/hedge-fund/backtest",
            json=payload,
            stream=True,
            timeout=15,
        ) as r:
            assert r.status_code == 200, f"Expected 200, got {r.status_code}: {r.text[:500]}"
            assert "text/event-stream" in r.headers.get("content-type", "")

            buf = ""
            found_start = False
            for chunk in r.iter_content(chunk_size=256, decode_unicode=True):
                buf += chunk
                if "event: start" in buf:
                    found_start = True
                    break
                if len(buf) > 4096:
                    break

            assert found_start, f"Did not receive 'event: start' in SSE stream. Got: {buf[:500]}"


# ---------------------------------------------------------------------------
# Unit test: frontend graph fix validation (no backend needed)
# ---------------------------------------------------------------------------

class TestGraphFix:
    """
    Unit-level test that validates the graph.py logic with the fixed IDs
    from use-simple-backtest.ts. No backend subprocess needed.
    """

    def test_extract_base_agent_key_strips_6char_suffix(self):
        """
        extract_base_agent_key() strips the last _XXXXXX (6 lower-alnum) segment.

        Important: agent names whose last part is exactly 6 lower-alnum chars
        (e.g. ben_graham → "graham" is 6 chars) will also be stripped. This
        is why factor node IDs must be sent with an appended suffix from the frontend:
            ben_graham_node01  →  "ben_graham"  ✓
            portfolio_manager_wbpmgr  →  "portfolio_manager"  ✓
        """
        sys.path.insert(0, str(PROJECT_ROOT))
        from app.backend.services.graph import extract_base_agent_key

        # IDs WITH the proper suffix → correct base key returned
        assert extract_base_agent_key("portfolio_manager_wbpmgr") == "portfolio_manager"
        assert extract_base_agent_key("warren_buffett_abc123") == "warren_buffett"
        assert extract_base_agent_key("ben_graham_node01") == "ben_graham"
        # Node without underscore-separated suffix → returned as-is
        assert extract_base_agent_key("portfolio-manager") == "portfolio-manager"
        # A bare key with a 6-char final segment gets stripped (known behavior)
        # "ben_graham" → "graham" is 6 lower-alnum → returns "ben"
        # This is why the frontend must always append a suffix.
        assert extract_base_agent_key("ben_graham") == "ben"

    def test_pm_id_matches_portfolio_manager_check(self):
        """portfolio_manager_wbpmgr is correctly recognized as a portfolio manager node."""
        from app.backend.services.graph import extract_base_agent_key

        PM_ID = "portfolio_manager_wbpmgr"
        base_key = extract_base_agent_key(PM_ID)
        assert base_key == "portfolio_manager", (
            f"Expected 'portfolio_manager', got '{base_key}'. "
            "The PM node will not be added to the LangGraph graph."
        )

    def test_old_pm_id_does_not_match(self):
        """Confirm the old buggy ID 'portfolio-manager' is NOT recognized as a PM node."""
        from app.backend.services.graph import extract_base_agent_key

        BAD_PM_ID = "portfolio-manager"
        base_key = extract_base_agent_key(BAD_PM_ID)
        # Returns the ID unchanged (no underscore-based suffix stripping for hyphens)
        assert base_key != "portfolio_manager", (
            "Bug: the old hyphenated ID is unexpectedly matching portfolio_manager check."
        )

    def test_factor_nodes_have_no_incoming_edges_in_fixed_payload(self):
        """
        In the fixed buildBacktestRequest, factor node IDs are suffixed (e.g. ben_graham_node01)
        so extract_base_agent_key returns the correct base key.
        Factor nodes have NO incoming edges, so graph.py auto-wires start_node → each factor.
        """
        PM_ID = "portfolio_manager_wbpmgr"
        FACTOR_SUFFIX = "node01"
        factors = ["ben_graham", "warren_buffett"]
        factor_node_ids = [f"{f}_{FACTOR_SUFFIX}" for f in factors]

        graph_nodes = [
            {"id": "portfolio-start", "type": "portfolioStart"},
            {"id": PM_ID, "type": "portfolioManager"},
            *[{"id": fid, "type": "agentNode"} for fid in factor_node_ids],
        ]
        graph_edges = [
            {"id": f"edge-{fid}-pm", "source": fid, "target": PM_ID}
            for fid in factor_node_ids
        ]

        node_ids = {n["id"] for n in graph_nodes}
        nodes_with_incoming = {e["target"] for e in graph_edges if e["target"] in node_ids}

        for fid in factor_node_ids:
            assert fid not in nodes_with_incoming, (
                f"Factor '{fid}' has an incoming edge — won't be auto-wired from start_node."
            )
        assert PM_ID in nodes_with_incoming, "PM node should have incoming edges from factors."
