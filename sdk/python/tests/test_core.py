"""End-to-end tests for the vectra_graph client against a real Vectra server.

setUpModule builds the Go server binary (or honors VECTRA_SDK_TEST_BIN),
boots one single-node server on a free port, and shares it across every test
class. Each class writes only its own key/label prefix, so test and class
order never matter. Read-your-writes is made deterministic by awaiting the
published sequence after every write (the same contract SDK users get via
``Client.await_seq``).
"""

from __future__ import annotations

import os
import sys
import unittest

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_SDK_ROOT = os.path.dirname(_TESTS_DIR)
# src-layout package, exercised without installation
sys.path.insert(0, os.path.join(_SDK_ROOT, "src"))

import harness  # noqa: E402  (tests dir is on sys.path via unittest discovery)
from vectra_graph import (  # noqa: E402
    Client,
    VectraConflictError,
    VectraError,
    VectraNotFoundError,
)

FIXTURE = None  # harness.ServerFixture, set in setUpModule
CLIENT = None  # Client bound to the fixture, set in setUpModule


def setUpModule():
    global FIXTURE, CLIENT
    FIXTURE = harness.ServerFixture(harness.build_server())
    FIXTURE.__enter__()
    CLIENT = Client(base_url=FIXTURE.url)


def tearDownModule():
    if FIXTURE is not None:
        FIXTURE.__exit__(None, None, None)
    harness.cleanup_build()


def write_and_sync(**kwargs):
    """Apply one batch, then block until that write is published.

    Returns the batch response dict. Single-node servers publish shortly
    after the raft apply; awaiting ``publishedSeq + 1`` makes every later
    read in the test observe the write deterministically.
    """
    before = CLIENT.stats()["publishedSeq"]
    result = CLIENT.batch(**kwargs)
    aw = CLIENT.await_seq(int(before) + 1)
    assert aw["reached"], "write not published: awaited %d, got %r" % (before + 1, aw)
    return result


class ServerTestCase(unittest.TestCase):
    """Base class: access to the shared module-level client."""

    def setUp(self):
        self.client = CLIENT


class TestHealthStats(ServerTestCase):
    def test_health_ok(self):
        h = self.client.health()
        self.assertEqual(h["status"], "ok")
        self.assertTrue(h["isLeader"])
        self.assertIn("term", h)

    def test_stats_shape(self):
        s = self.client.stats()
        for key in ("nodes", "edges", "sccs", "cyclicNodes", "isLeader",
                    "term", "publishedSeq"):
            self.assertIn(key, s)
        self.assertIsInstance(s["publishedSeq"], int)
        self.assertGreaterEqual(s["publishedSeq"], 1)
        self.assertTrue(s["isLeader"])


class TestNodeLifecycle(ServerTestCase):
    def test_batch_upsert_read_delete(self):
        # upsert
        res = write_and_sync(
            nodes=[{"key": "lc-a", "label": "Service", "props": {"env": "prod", "w": 2}}]
        )
        self.assertEqual(res["nodesApplied"], 1)
        # read back
        n = self.client.node("lc-a")
        self.assertEqual(n["key"], "lc-a")
        self.assertEqual(n["label"], "Service")
        self.assertEqual(n["props"]["env"], "prod")
        # upsert again: same key updates props
        write_and_sync(nodes=[{"key": "lc-a", "label": "Service", "props": {"env": "staging"}}])
        n = self.client.node("lc-a")
        self.assertEqual(n["props"]["env"], "staging")
        # delete, then the read is a typed 404
        res = write_and_sync(node_deletes=["lc-a"])
        self.assertEqual(res["nodesDeleted"], 1)
        with self.assertRaises(VectraNotFoundError) as ctx:
            self.client.node("lc-a")
        self.assertEqual(ctx.exception.status, 404)
        self.assertEqual(ctx.exception.code, "NODE_NOT_FOUND")


class TestTraversals(ServerTestCase):
    @classmethod
    def setUpClass(cls):
        # Dependency graph: api -> auth -> db -> cache, api -> db (direct),
        # auth =METRICS=> db, plus a 2-cycle and a disconnected island.
        write_and_sync(
            nodes=[
                {"key": "tv-api", "label": "Service"},
                {"key": "tv-auth", "label": "Service"},
                {"key": "tv-db", "label": "Service"},
                {"key": "tv-cache", "label": "Service"},
                {"key": "tv-island", "label": "Service"},
                {"key": "tv-ca", "label": "Service"},
                {"key": "tv-cb", "label": "Service"},
            ],
            edges=[
                {"srcKey": "tv-api", "dstKey": "tv-auth", "label": "CALLS"},
                {"srcKey": "tv-api", "dstKey": "tv-db", "label": "CALLS"},
                {"srcKey": "tv-auth", "dstKey": "tv-db", "label": "CALLS"},
                {"srcKey": "tv-auth", "dstKey": "tv-db", "label": "METRICS"},
                {"srcKey": "tv-db", "dstKey": "tv-cache", "label": "CALLS"},
                {"srcKey": "tv-ca", "dstKey": "tv-cb", "label": "CALLS"},
                {"srcKey": "tv-cb", "dstKey": "tv-ca", "label": "CALLS"},
            ],
        )

    def _keys(self, nodes):
        return {n["key"] for n in nodes}

    def test_neighbors_out(self):
        res = self.client.neighbors("tv-api")
        self.assertEqual(self._keys(res["nodes"]), {"tv-auth", "tv-db"})

    def test_neighbors_in(self):
        res = self.client.neighbors("tv-db", direction="in")
        self.assertEqual(self._keys(res["nodes"]), {"tv-api", "tv-auth"})

    def test_neighbors_label_filter(self):
        res = self.client.neighbors("tv-auth", label="METRICS")
        self.assertEqual(self._keys(res["nodes"]), {"tv-db"})

    def test_bfs(self):
        res = self.client.bfs(["tv-api"])
        self.assertEqual(res["count"], 4)
        depths = {n["key"]: n["depth"] for n in res["nodes"]}
        self.assertEqual(depths["tv-api"], 0)
        self.assertEqual(depths["tv-auth"], 1)
        self.assertEqual(depths["tv-db"], 1)
        self.assertEqual(depths["tv-cache"], 2)

    def test_bfs_max_depth(self):
        res = self.client.bfs(["tv-api"], max_depth=1)
        self.assertEqual(res["count"], 3)
        self.assertEqual({n["key"] for n in res["nodes"]}, {"tv-api", "tv-auth", "tv-db"})

    def test_bfs_label_filter(self):
        res = self.client.bfs(["tv-auth"], labels=["METRICS"])
        self.assertEqual(res["count"], 2)
        self.assertEqual({n["key"] for n in res["nodes"]}, {"tv-auth", "tv-db"})

    def test_bfs_direction_in(self):
        res = self.client.bfs(["tv-cache"], direction="in")
        self.assertEqual({n["key"] for n in res["nodes"]},
                         {"tv-cache", "tv-db", "tv-api", "tv-auth"})

    def test_path_found(self):
        res = self.client.path("tv-auth", "tv-cache")
        self.assertTrue(res["found"])
        self.assertEqual(res["hops"], 2)
        self.assertEqual(res["nodes"], ["tv-auth", "tv-db", "tv-cache"])

    def test_path_not_found(self):
        res = self.client.path("tv-api", "tv-island")
        self.assertFalse(res["found"])
        self.assertEqual(res["hops"], 0)

    def test_reachable(self):
        res = self.client.reachable("tv-api")
        keys = set(res["nodes"])
        self.assertTrue({"tv-api", "tv-auth", "tv-db", "tv-cache"} <= keys)
        self.assertNotIn("tv-island", keys)
        self.assertNotIn("tv-ca", keys)

    def test_cycles(self):
        res = self.client.cycles()
        cycles = [set(c["nodes"]) for c in res["cycles"]]
        self.assertIn({"tv-ca", "tv-cb"}, cycles)


class TestCypherRead(ServerTestCase):
    @classmethod
    def setUpClass(cls):
        write_and_sync(
            nodes=[
                {"key": "cy-a", "label": "CySvc", "props": {"w": 5, "tier": "gold"}},
                {"key": "cy-b", "label": "CySvc", "props": {"w": 2, "tier": "silver"}},
                {"key": "cy-x", "label": "CySvc", "props": {"w": 9, "tier": "gold"}},
            ],
            edges=[{"srcKey": "cy-a", "dstKey": "cy-b", "label": "CALLS"}],
        )

    def test_match_return(self):
        res = self.client.cypher("MATCH (n:CySvc {key:'cy-a'}) RETURN n.key, n.tier")
        self.assertEqual(res["columns"], ["n.key", "n.tier"])
        self.assertEqual(res["rows"], [{"n.key": "cy-a", "n.tier": "gold"}])
        self.assertEqual(res["count"], 1)

    def test_where_and_order(self):
        res = self.client.cypher(
            "MATCH (s:CySvc) WHERE s.w > 4 RETURN s.key ORDER BY s.key"
        )
        self.assertEqual(res["count"], 2)
        self.assertEqual([r["s.key"] for r in res["rows"]], ["cy-a", "cy-x"])

    def test_read_has_no_summary(self):
        res = self.client.cypher("MATCH (n:CySvc) RETURN n.key")
        self.assertNotIn("summary", res)


class TestCypherWriteSummary(ServerTestCase):
    def test_create_returns_summary_without_count(self):
        before = CLIENT.stats()["publishedSeq"]
        res = CLIENT.cypher(
            "CREATE (n:CwSvc {key:'cw-x', env:'prod'}) RETURN n.key, n.env"
        )
        # write face: columns/rows from RETURN plus summary — never count
        self.assertNotIn("count", res)
        self.assertEqual(res["columns"], ["n.key", "n.env"])
        self.assertEqual(res["rows"], [{"n.key": "cw-x", "n.env": "prod"}])
        summary = res["summary"]
        self.assertEqual(
            set(summary.keys()),
            {"createdNodes", "createdEdges", "deletedNodes", "deletedEdges",
             "propertiesSet", "indexesCreated", "constraintsCreated"},
        )
        self.assertEqual(summary["createdNodes"], 1)
        self.assertEqual(summary["createdEdges"], 0)
        # propertiesSet counts SET assignments; a pure CREATE map-literal
        # is part of node creation and does not count (wire truth).
        self.assertEqual(summary["propertiesSet"], 0)
        # a SET statement bumps propertiesSet
        res = CLIENT.cypher("MATCH (n:CwSvc {key:'cw-x'}) SET n.env = 'staging'")
        self.assertEqual(res["summary"]["propertiesSet"], 1)
        # the writes really committed: visible to reads once published
        # (CREATE and SET are one raft entry each)
        aw = CLIENT.await_seq(int(before) + 2)
        self.assertTrue(aw["reached"])
        got = CLIENT.cypher("MATCH (n:CwSvc {key:'cw-x'}) RETURN n.env")
        self.assertEqual(got["rows"][0]["n.env"], "staging")


class TestErrorEnvelope(ServerTestCase):
    def test_cypher_parse_error(self):
        with self.assertRaises(VectraError) as ctx:
            self.client.cypher("MATCH (a) RETURN b")  # unbound variable
        e = ctx.exception
        self.assertEqual(e.status, 400)
        self.assertEqual(e.code, "PARSE")
        self.assertTrue(e.message)
        # 400 maps to the base class, not a not-found/conflict subclass
        self.assertNotIsInstance(e, VectraNotFoundError)
        self.assertNotIsInstance(e, VectraConflictError)

    def test_unknown_node_typed_404(self):
        with self.assertRaises(VectraNotFoundError) as ctx:
            self.client.node("no-such-node-404")
        e = ctx.exception
        self.assertEqual(e.status, 404)
        self.assertEqual(e.code, "NODE_NOT_FOUND")

    def test_unknown_neighbors_typed_404(self):
        with self.assertRaises(VectraNotFoundError) as ctx:
            self.client.neighbors("no-such-node-404")
        self.assertEqual(ctx.exception.status, 404)
        self.assertEqual(ctx.exception.code, "NODE_NOT_FOUND")


class TestAwait(ServerTestCase):
    def test_await_reached_after_write(self):
        before = CLIENT.stats()["publishedSeq"]
        res = CLIENT.batch(nodes=[{"key": "aw-1", "label": "Service"}])
        self.assertEqual(res["nodesApplied"], 1)
        aw = CLIENT.await_seq(int(before) + 1, timeout_ms=5000)
        self.assertTrue(aw["reached"])
        self.assertGreaterEqual(aw["publishedSeq"], before + 1)
        # the X-Vectra-Snapshot response header is surfaced by the client
        self.assertIsNotNone(CLIENT.last_snapshot)
        self.assertEqual(CLIENT.last_snapshot, aw["publishedSeq"])
        n = CLIENT.node("aw-1")
        self.assertEqual(n["key"], "aw-1")

    def test_await_timeout_reports_not_reached(self):
        aw = CLIENT.await_seq(1_000_000_000, timeout_ms=200)
        self.assertFalse(aw["reached"])
        self.assertLess(aw["publishedSeq"], 1_000_000_000)


if __name__ == "__main__":
    unittest.main()
