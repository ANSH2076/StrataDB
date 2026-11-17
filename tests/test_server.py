"""Unit tests for Strata HTTP server and REST API (Stage 5).

Tests:
  - Background server start on port 0 with automatic port discovery and shutdown
  - Dashboard index.html served at GET /
  - CRUD over HTTP: PUT, GET, DELETE
  - POST /api/load generates requested keys with correct stats
  - GET /api/state returns all expected keys and metadata
  - GET /api/trace correctly traces keys and stops at tombstone for deleted key
  - trace and get agree for 200 random keys
  - POST /api/compact runs compaction and updates stats
  - POST /api/crash simulates crash and returns mismatches == 0
  - POST /api/settings updates bloom and auto_compact runtime flags
  - Bad input returns HTTP 400 with {"error": ...} JSON
  - POST /api/reset clears files and restarts clean engine
"""

from __future__ import annotations

import json
import os
import random
import threading
from http.server import ThreadingHTTPServer
from typing import Any, Dict, Tuple
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pytest

from strata.server import ServerState, create_handler


@pytest.fixture
def running_server(tmp_path):
    """Fixture that spins up the Strata HTTP server on an ephemeral port in a daemon thread."""
    data_dir = str(tmp_path / "server_data")
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dashboard_path = os.path.join(base_dir, "dashboard", "index.html")

    state = ServerState(data_dir=data_dir, memtable_limit_bytes=1024)
    handler_cls = create_handler(state, dashboard_path)

    # Bind to port 0 for automatic OS port assignment
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    host, port = httpd.server_address

    server_thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    server_thread.start()

    base_url = f"http://{host}:{port}"
    yield base_url, state

    httpd.shutdown()
    httpd.server_close()
    server_thread.join(timeout=2.0)
    state.db.close()


def _request(
    url: str,
    method: str = "GET",
    body: Any = None,
) -> Tuple[int, Dict[str, Any]]:
    """Helper using standard library urllib only."""
    headers = {}
    data_bytes = None
    if body is not None:
        headers["Content-Type"] = "application/json"
        data_bytes = json.dumps(body).encode("utf-8")

    req = Request(url, data=data_bytes, headers=headers, method=method)
    try:
        with urlopen(req, timeout=5.0) as resp:
            status = resp.status
            content = resp.read()
            if resp.headers.get_content_type() == "application/json":
                return status, json.loads(content.decode("utf-8"))
            return status, {"raw": content.decode("utf-8", errors="replace")}
    except HTTPError as e:
        status = e.code
        err_body = e.read()
        try:
            return status, json.loads(err_body.decode("utf-8"))
        except Exception:
            return status, {"error": err_body.decode("utf-8", errors="replace")}


def test_server_serves_dashboard(running_server):
    """GET / serves dashboard/index.html."""
    base_url, _ = running_server
    status, res = _request(f"{base_url}/")
    assert status == 200
    assert "raw" in res
    assert "<title>Strata" in res["raw"]


def test_crud_endpoints(running_server):
    """PUT, GET, and DELETE over HTTP."""
    base_url, _ = running_server

    # PUT
    status, res = _request(f"{base_url}/api/put", method="POST", body={"key": "test_k", "value": "test_v"})
    assert status == 200
    assert res["status"] == "ok"

    # GET
    status, res = _request(f"{base_url}/api/get?key=test_k")
    assert status == 200
    assert res["found"] is True
    assert res["value"] == "test_v"

    # DELETE
    status, res = _request(f"{base_url}/api/delete", method="POST", body={"key": "test_k"})
    assert status == 200
    assert res["status"] == "ok"

    # GET after delete
    status, res = _request(f"{base_url}/api/get?key=test_k")
    assert status == 200
    assert res["found"] is False
    assert res["value"] is None


def test_load_endpoint(running_server):
    """POST /api/load generates records."""
    base_url, _ = running_server
    status, res = _request(f"{base_url}/api/load", method="POST", body={"count": 150})
    assert status == 200
    assert res["loaded"] == 150

    status, state_res = _request(f"{base_url}/api/state")
    assert status == 200
    assert state_res["stats"]["sstable_count"] > 0


def test_state_endpoint_keys(running_server):
    """GET /api/state returns all expected keys."""
    base_url, _ = running_server
    status, res = _request(f"{base_url}/api/state?since=0")
    assert status == 200
    for expected in ["stats", "events", "recovery_info", "memtable_limit_bytes"]:
        assert expected in res
    assert isinstance(res["events"], list)


def test_trace_for_deleted_key(running_server):
    """A trace for a deleted key ends at a tombstone step."""
    base_url, _ = running_server

    # Put and flush
    _request(f"{base_url}/api/put", method="POST", body={"key": "user_del", "value": "active"})
    _request(f"{base_url}/api/delete", method="POST", body={"key": "user_del"})

    status, res = _request(f"{base_url}/api/trace?key=user_del")
    assert status == 200
    assert res["found"] is False
    steps = res["steps"]
    assert len(steps) > 0
    # The terminal action must be a tombstone
    terminal_step = steps[-1]
    assert terminal_step["action"] == "tombstone"


def test_trace_and_get_agreement_200_keys(running_server):
    """trace_get and get return identical results across 200 random keys."""
    base_url, _ = running_server

    # Load keys
    _request(f"{base_url}/api/load", method="POST", body={"count": 200})

    rng = random.Random(1337)
    sample_keys = [f"user_{rng.randint(0, 300):06d}" for _ in range(200)]

    for k in sample_keys:
        _, get_res = _request(f"{base_url}/api/get?key={k}")
        _, trace_res = _request(f"{base_url}/api/trace?key={k}")

        assert trace_res["found"] == get_res["found"]
        assert trace_res["value"] == get_res["value"]


def test_compact_endpoint(running_server):
    """POST /api/compact merges SSTables."""
    base_url, _ = running_server
    _request(f"{base_url}/api/load", method="POST", body={"count": 300})

    status, res = _request(f"{base_url}/api/compact", method="POST")
    assert status == 200
    assert res["status"] == "ok"

    _, state_res = _request(f"{base_url}/api/state")
    assert state_res["stats"]["sstable_count"] <= 1


def test_crash_endpoint(running_server):
    """POST /api/crash returns mismatches == 0."""
    base_url, _ = running_server
    _request(f"{base_url}/api/load", method="POST", body={"count": 150})

    status, res = _request(f"{base_url}/api/crash", method="POST")
    assert status == 200
    assert res["status"] == "ok"
    assert res["mismatches"] == 0
    assert res["checked"] > 0
    assert "recovery_info" in res


def test_settings_endpoint(running_server):
    """POST /api/settings updates bloom and auto_compact flags."""
    base_url, _ = running_server

    status, res = _request(f"{base_url}/api/settings", method="POST", body={"bloom": False, "auto_compact": False})
    assert status == 200
    assert res["bloom"] is False
    assert res["auto_compact"] is False

    _, state_res = _request(f"{base_url}/api/state")
    assert state_res["stats"]["bloom_enabled"] is False
    assert state_res["stats"]["auto_compact"] is False


def test_bad_input_returns_400(running_server):
    """Bad inputs return HTTP 400 with structured JSON errors."""
    base_url, _ = running_server

    # Empty key in GET
    status, res = _request(f"{base_url}/api/get?key=")
    assert status == 400
    assert "error" in res

    # Missing field in PUT
    status, res = _request(f"{base_url}/api/put", method="POST", body={"key": "only_key"})
    assert status == 400
    assert "error" in res

    # Negative count in load
    status, res = _request(f"{base_url}/api/load", method="POST", body={"count": -10})
    assert status == 400
    assert "error" in res

    # Count > 20000 in load
    status, res = _request(f"{base_url}/api/load", method="POST", body={"count": 99999})
    assert status == 400
    assert "error" in res

    # Non-boolean setting
    status, res = _request(f"{base_url}/api/settings", method="POST", body={"bloom": "not_bool"})
    assert status == 400
    assert "error" in res


def test_reset_endpoint(running_server):
    """POST /api/reset clears all data files."""
    base_url, state = running_server
    _request(f"{base_url}/api/load", method="POST", body={"count": 100})

    status, res = _request(f"{base_url}/api/reset", method="POST")
    assert status == 200
    assert res["status"] == "ok"

    _, state_res = _request(f"{base_url}/api/state")
    assert state_res["stats"]["sstable_count"] == 0
    assert state_res["stats"]["memtable_keys"] == 0
    assert state_res["stats"]["wal_records"] == 0
