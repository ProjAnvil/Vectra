"""Serverless unit tests: client behaviors pinned without vectra-server.

A stdlib ``http.server`` stub answers canned responses (no go toolchain, no
subprocess, no skip): the ``X-Vectra-Snapshot`` header on error responses,
and the defensive ``conflictTotal`` parse on the non-envelope txn 409. The
``vector_search`` argument validation never reaches the transport at all.
"""

from __future__ import annotations

import http.server
import json
import os
import sys
import threading
import unittest

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_SDK_ROOT = os.path.dirname(_TESTS_DIR)
sys.path.insert(0, os.path.join(_SDK_ROOT, "src"))  # src layout, no install

from vectra_graph import (  # noqa: E402
    Client,
    TxnConflictError,
    VectraNotFoundError,
)


class _StubHandler(http.server.BaseHTTPRequestHandler):
    """Serves the canned (status, headers, body) set by the owning test."""

    canned = (200, {}, b"{}")

    def _answer(self) -> None:
        status, headers, body = self.canned
        self.send_response(status)
        for key, value in headers.items():
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 (http.server naming)
        self._answer()

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", 0))
        if length:
            self.rfile.read(length)
        self._answer()

    def log_message(self, *args) -> None:  # keep test output clean
        pass


class StubServer:
    """Canned-response HTTP server bound to a free loopback port."""

    def __init__(self):
        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _StubHandler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        self.url = "http://127.0.0.1:%d" % self.httpd.server_address[1]

    def set(self, status, headers=None, body=b"{}"):
        _StubHandler.canned = (status, headers or {}, body)

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=5)


class TestStubTransport(unittest.TestCase):
    def setUp(self):
        self.stub = StubServer()
        self.client = Client(base_url=self.stub.url)

    def tearDown(self):
        self.stub.stop()

    def test_snapshot_header_recorded_on_error_response(self):
        # Read endpoints set X-Vectra-Snapshot before writing any error
        # body, so a 404 still refreshes last_snapshot (TS-client parity).
        self.stub.set(
            404,
            {"X-Vectra-Snapshot": "7"},
            json.dumps(
                {"error": {"code": "NODE_NOT_FOUND", "message": "node not found"}}
            ).encode("utf-8"),
        )
        self.client.last_snapshot = None
        with self.assertRaises(VectraNotFoundError) as ctx:
            self.client.node("missing")
        self.assertEqual(ctx.exception.code, "NODE_NOT_FOUND")
        self.assertEqual(self.client.last_snapshot, 7)

    def test_conflict_total_missing_falls_back_to_list(self):
        # non-envelope txn 409 without conflictTotal: len(conflicts), and
        # never a TypeError out of the error parser
        self.stub.set(
            409,
            {},
            json.dumps(
                {"committed": False, "conflicts": ["k1", "k2"], "hint": "refresh"}
            ).encode("utf-8"),
        )
        with self.assertRaises(TxnConflictError) as ctx:
            self.client.txn(nodes=[{"key": "k1", "label": "T"}], snapshot=1)
        self.assertEqual(ctx.exception.conflict_total, 2)

    def test_conflict_total_null_falls_back_to_list(self):
        self.stub.set(
            409,
            {},
            json.dumps(
                {
                    "committed": False,
                    "conflicts": ["k1"],
                    "conflictTotal": None,
                    "hint": "refresh",
                }
            ).encode("utf-8"),
        )
        with self.assertRaises(TxnConflictError) as ctx:
            self.client.txn(nodes=[{"key": "k1", "label": "T"}], snapshot=1)
        self.assertEqual(ctx.exception.conflict_total, 1)


class TestVectorSearchValidation(unittest.TestCase):
    """Client-side argument validation — no server round trip happens."""

    def test_exactly_one_query_mode(self):
        client = Client(base_url="http://127.0.0.1:1")  # nothing is fetched
        with self.assertRaises(ValueError):
            client.vector_search("idx")  # neither
        with self.assertRaises(ValueError):
            client.vector_search("idx", query_vector=[1.0, 0.0], similar_to="a")
        # empty vector == absent (the server's len==0 rule): ValueError, not
        # a request that would 400 server-side
        with self.assertRaises(ValueError):
            client.vector_search("idx", query_vector=[])
        with self.assertRaises(ValueError):
            client.vector_search("idx", similar_to="")


if __name__ == "__main__":
    unittest.main()
