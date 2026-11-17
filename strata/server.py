"""HTTP API and dashboard server for Strata.

Exposes a REST API and serves the single-file dashboard/index.html interface.

Usage:
    python3 -m strata.server --data ./demo_data --port 8080 [--memtable-limit 2048]

Concurrency & Thread Safety:
    The underlying Strata storage engine is single-threaded (no internal
    locks).  ThreadingHTTPServer dispatches incoming HTTP requests to
    separate worker threads.  To prevent race conditions, corruption, or
    torn state during concurrent requests, EVERY call to the engine is
    guarded by a SINGLE global threading.Lock.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

from strata.engine import DEFAULT_MEMTABLE_LIMIT_BYTES, Strata

MAX_BODY_BYTES = 1024 * 1024  # 1 MB request body limit


class ServerState:
    """Encapsulates the global engine instance and thread lock."""

    def __init__(self, data_dir: str, memtable_limit_bytes: int) -> None:
        self.data_dir = data_dir
        self.memtable_limit_bytes = memtable_limit_bytes
        self.lock = threading.Lock()
        self.db = Strata(
            data_dir=data_dir,
            sync=True,
            memtable_limit_bytes=memtable_limit_bytes,
            use_bloom=True,
            auto_compact=True,
        )


def create_handler(state: ServerState, dashboard_html_path: str):
    """Factory creating the RequestHandler class bound to the server state."""

    class StrataHTTPHandler(BaseHTTPRequestHandler):
        server_version = "StrataHTTP/1.0"

        def log_message(self, format: str, *args: Any) -> None:
            # Suppress default request logging to keep output clean
            pass

        def _send_json(self, status_code: int, data: Dict[str, Any]) -> None:
            """Serialize and send a JSON response."""
            body = json.dumps(data, ensure_ascii=False).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
            self.end_headers()
            self.wfile.write(body)

        def _send_error(self, status_code: int, message: str) -> None:
            """Send a structured JSON error response."""
            self._send_json(status_code, {"error": message})

        def _read_json_body(self) -> Optional[Dict[str, Any]]:
            """Read and decode JSON request body up to MAX_BODY_BYTES."""
            content_length = self.headers.get("Content-Length")
            if not content_length:
                self._send_error(400, "Missing Content-Length header")
                return None

            try:
                length = int(content_length)
            except ValueError:
                self._send_error(400, "Invalid Content-Length header")
                return None

            if length < 0:
                self._send_error(400, "Negative Content-Length header")
                return None

            if length > MAX_BODY_BYTES:
                self._send_error(400, f"Request body too large (max {MAX_BODY_BYTES} bytes)")
                return None

            raw_body = self.rfile.read(length)
            try:
                payload = json.loads(raw_body.decode("utf-8"))
            except Exception as e:
                self._send_error(400, f"Malformed JSON: {e}")
                return None

            if not isinstance(payload, dict):
                self._send_error(400, "JSON body must be an object")
                return None

            return payload

        # -- GET Routing -------------------------------------------------------

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            path = parsed.path
            params = parse_qs(parsed.query)

            if path == "/":
                self._handle_dashboard()
            elif path == "/api/state":
                self._handle_state(params)
            elif path == "/api/get":
                self._handle_get(params)
            elif path == "/api/trace":
                self._handle_trace(params)
            else:
                self._send_error(404, f"Not found: {path}")

        def _handle_dashboard(self) -> None:
            """Serve dashboard/index.html."""
            if not os.path.exists(dashboard_html_path):
                self._send_error(404, "Dashboard template not found")
                return

            with open(dashboard_html_path, "rb") as f:
                content = f.read()

            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

        def _handle_state(self, params: Dict[str, List[str]]) -> None:
            """GET /api/state?since=ID"""
            since_str = params.get("since", ["0"])[0]
            try:
                since_id = int(since_str)
            except ValueError:
                self._send_error(400, "Parameter 'since' must be an integer")
                return

            with state.lock:
                stats = state.db.stats()
                events = state.db.events_since(since_id)
                recovery_info = state.db.recovery_info
                memtable_limit = state.db.memtable_limit_bytes

            self._send_json(200, {
                "stats": stats,
                "events": events,
                "recovery_info": recovery_info,
                "memtable_limit_bytes": memtable_limit,
            })

        def _handle_get(self, params: Dict[str, List[str]]) -> None:
            """GET /api/get?key="""
            keys = params.get("key")
            if not keys or len(keys[0]) == 0:
                self._send_error(400, "Query parameter 'key' is required and must not be empty")
                return

            key = keys[0]
            with state.lock:
                val = state.db.get(key)

            self._send_json(200, {
                "key": key,
                "found": val is not None,
                "value": val,
            })

        def _handle_trace(self, params: Dict[str, List[str]]) -> None:
            """GET /api/trace?key="""
            keys = params.get("key")
            if not keys or len(keys[0]) == 0:
                self._send_error(400, "Query parameter 'key' is required and must not be empty")
                return

            key = keys[0]
            with state.lock:
                res = state.db.trace_get(key)

            self._send_json(200, res)

        # -- POST Routing ------------------------------------------------------

        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            path = parsed.path

            if path == "/api/put":
                self._handle_put()
            elif path == "/api/delete":
                self._handle_delete()
            elif path == "/api/load":
                self._handle_load()
            elif path == "/api/compact":
                self._handle_compact()
            elif path == "/api/settings":
                self._handle_settings()
            elif path == "/api/crash":
                self._handle_crash()
            elif path == "/api/reset":
                self._handle_reset()
            else:
                self._send_error(404, f"Not found: {path}")

        def _handle_put(self) -> None:
            """POST /api/put {"key": "...", "value": "..."}"""
            body = self._read_json_body()
            if body is None:
                return

            key = body.get("key")
            value = body.get("value")

            if not isinstance(key, str) or len(key) == 0:
                self._send_error(400, "Field 'key' must be a non-empty string")
                return
            if not isinstance(value, str) or len(value) == 0:
                self._send_error(400, "Field 'value' must be a non-empty string")
                return

            with state.lock:
                state.db.put(key, value)

            self._send_json(200, {"status": "ok", "key": key})

        def _handle_delete(self) -> None:
            """POST /api/delete {"key": "..."}"""
            body = self._read_json_body()
            if body is None:
                return

            key = body.get("key")
            if not isinstance(key, str) or len(key) == 0:
                self._send_error(400, "Field 'key' must be a non-empty string")
                return

            with state.lock:
                state.db.delete(key)

            self._send_json(200, {"status": "ok", "key": key})

        def _handle_load(self) -> None:
            """POST /api/load {"count": N}"""
            body = self._read_json_body()
            if body is None:
                return

            count = body.get("count")
            if not isinstance(count, int) or count <= 0:
                self._send_error(400, "Field 'count' must be a positive integer")
                return

            if count > 20000:
                self._send_error(400, "Field 'count' cannot exceed 20,000")
                return

            # Generate realistic workload with ~15% overwrites and ~10% deletes
            rng = random.Random()
            with state.lock:
                for _ in range(count):
                    key_id = rng.randint(0, max(50, count // 2))
                    key = f"user_{key_id:06d}"
                    roll = rng.random()

                    if roll < 0.75:
                        val = f"profile_payload_data_v_{rng.randint(1000, 9999)}_{key}"
                        state.db.put(key, val)
                    elif roll < 0.90:
                        # 15% overwrite
                        val = f"updated_payload_data_v_{rng.randint(1000, 9999)}_{key}"
                        state.db.put(key, val)
                    else:
                        # 10% delete
                        state.db.delete(key)

            self._send_json(200, {"status": "ok", "loaded": count})

        def _handle_compact(self) -> None:
            """POST /api/compact"""
            with state.lock:
                res = state.db.compact()

            self._send_json(200, {"status": "ok", "result": res})

        def _handle_settings(self) -> None:
            """POST /api/settings {"bloom": bool, "auto_compact": bool}"""
            body = self._read_json_body()
            if body is None:
                return

            bloom = body.get("bloom")
            auto_compact = body.get("auto_compact")

            if bloom is not None:
                if not isinstance(bloom, bool):
                    self._send_error(400, "Field 'bloom' must be a boolean")
                    return
            if auto_compact is not None:
                if not isinstance(auto_compact, bool):
                    self._send_error(400, "Field 'auto_compact' must be a boolean")
                    return

            with state.lock:
                if bloom is not None:
                    state.db.use_bloom = bloom
                if auto_compact is not None:
                    state.db.auto_compact = auto_compact

                bloom_val = state.db.use_bloom
                auto_val = state.db.auto_compact

            self._send_json(200, {
                "status": "ok",
                "bloom": bloom_val,
                "auto_compact": auto_val,
            })

        def _handle_crash(self) -> None:
            """POST /api/crash

            Simulated crash:
            1. Samples up to 200 existing keys and remembers their values.
            2. Calls state.db._abandon() to close file handles with no flush.
            3. Reopens a fresh Strata instance on the same directory while holding the lock.
            4. Re-reads sampled keys and checks for mismatches.

            NOTE: This is an in-process simulated crash. The true OS process kill-test
            is executed via tests/crash_test.py.
            """
            with state.lock:
                # Sample up to 200 keys
                sampled_keys = [f"user_{i:06d}" for i in range(200)]
                pre_crash_data: Dict[str, Optional[str]] = {}
                for k in sampled_keys:
                    pre_crash_data[k] = state.db.get(k)

                # Abandon the engine instance (no memtable flush)
                state.db._abandon()

                # Reopen on same directory
                new_db = Strata(
                    data_dir=state.data_dir,
                    sync=True,
                    memtable_limit_bytes=state.memtable_limit_bytes,
                    use_bloom=state.db.use_bloom,
                    auto_compact=state.db.auto_compact,
                )
                state.db = new_db

                # Check post-recovery values
                mismatches = 0
                for k, expected_val in pre_crash_data.items():
                    actual_val = state.db.get(k)
                    if actual_val != expected_val:
                        mismatches += 1

                recovery_info = state.db.recovery_info

            self._send_json(200, {
                "status": "ok",
                "recovery_info": recovery_info,
                "checked": len(sampled_keys),
                "mismatches": mismatches,
            })

        def _handle_reset(self) -> None:
            """POST /api/reset

            Deletes only *.sst, *.tmp, wal.log and MANIFEST inside the data dir.
            Refuses to run if data_dir is empty or root '/'.
            """
            norm_dir = os.path.abspath(state.data_dir)
            if not norm_dir or norm_dir == "/" or norm_dir == os.path.abspath("/"):
                self._send_error(400, "Refusing to reset root or empty directory")
                return

            with state.lock:
                state.db.close()

                # Safely delete only Strata database files
                for f in os.listdir(norm_dir):
                    if (
                        f.endswith(".sst")
                        or f.endswith(".tmp")
                        or f == "wal.log"
                        or f == "MANIFEST"
                    ):
                        try:
                            os.remove(os.path.join(norm_dir, f))
                        except OSError:
                            pass

                # Reopen clean engine
                state.db = Strata(
                    data_dir=state.data_dir,
                    sync=True,
                    memtable_limit_bytes=state.memtable_limit_bytes,
                )

            self._send_json(200, {"status": "ok", "message": "Database reset to empty state"})

    return StrataHTTPHandler


def run_server(data_dir: str, port: int, memtable_limit_bytes: int = DEFAULT_MEMTABLE_LIMIT_BYTES) -> None:
    """Start the Strata HTTP server."""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dashboard_html_path = os.path.join(base_dir, "dashboard", "index.html")

    state = ServerState(data_dir, memtable_limit_bytes)
    handler_cls = create_handler(state, dashboard_html_path)

    # Bind strictly to localhost (127.0.0.1) for local safety
    server = ThreadingHTTPServer(("127.0.0.1", port), handler_cls)
    print(f"Strata server running at http://127.0.0.1:{port}/")
    print(f"Data directory: {os.path.abspath(data_dir)}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server...")
    finally:
        server.server_close()
        state.db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Strata HTTP Server and Dashboard")
    parser.add_argument("--data", required=True, help="Path to database storage directory")
    parser.add_argument("--port", type=int, default=8080, help="Port to bind (default 8080)")
    parser.add_argument(
        "--memtable-limit",
        type=int,
        default=DEFAULT_MEMTABLE_LIMIT_BYTES,
        help="MemTable flush threshold in bytes",
    )
    args = parser.parse_args()
    run_server(args.data, args.port, args.memtable_limit)


if __name__ == "__main__":
    main()
