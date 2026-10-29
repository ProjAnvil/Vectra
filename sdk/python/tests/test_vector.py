"""End-to-end tests for the vector index API, Cypher index DDL, and bearer auth.

Vector indexes are declared through raft (create/drop) and searched on the
published clone. Cypher ``CREATE [FULLTEXT] INDEX`` declarations ride the
cypher write lane and stay PENDING until the next Seal/publish — lookups
answer via compensation scan until the declaration merges. The auth class
boots a SECOND server with ``-auth-token-file`` to pin the bearer-token
round trip and the typed 401.
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_SDK_ROOT = os.path.dirname(_TESTS_DIR)
# src-layout package, exercised without installation
sys.path.insert(0, os.path.join(_SDK_ROOT, "src"))

import harness  # noqa: E402  (tests dir is on sys.path via unittest discovery)
from vectra_graph import (  # noqa: E402
    Client,
    TxnConflictError,
    VectraAuthError,
    VectraConflictError,
    VectraError,
    VectraNotFoundError,
)

FIXTURE = None  # harness.ServerFixture, set in setUpModule
CLIENT = None   # Client bound to the fixture, set in setUpModule
BINARY = ""     # server binary path (alive for the whole module)


def setUpModule():
    global FIXTURE, CLIENT, BINARY
    BINARY = harness.build_server()
    FIXTURE = harness.ServerFixture(BINARY)
    FIXTURE.__enter__()
    CLIENT = Client(base_url=FIXTURE.url)


def tearDownModule():
    if FIXTURE is not None:
        FIXTURE.__exit__(None, None, None)
    harness.cleanup_build()


def sync(**kwargs):
    """Apply one batch on the shared client, then await its publish."""
    before = CLIENT.stats()["publishedSeq"]
    result = CLIENT.batch(**kwargs)
    aw = CLIENT.await_seq(int(before) + 1)
    assert aw["reached"], "write not published: awaited %d, got %r" % (before + 1, aw)
    return result


def declare_and_await(create_fn):
    """Run a vector-index declaration (a raft write whose response carries
    the declaration, not the raft index), then await its publication."""
    before = CLIENT.stats()["publishedSeq"]
    result = create_fn()
    aw = CLIENT.await_seq(int(before) + 1)
    assert aw["reached"], "declaration not published: %r" % (aw,)
    return result


class VectorTestCase(unittest.TestCase):
    def setUp(self):
        self.client = CLIENT


class TestVectorLifecycle(VectorTestCase):
    INDEX = "vl-emb"

    @classmethod
    def setUpClass(cls):
        declare_and_await(
            lambda: CLIENT.vector_create_index(cls.INDEX, prop="embedding", dim=3,
                                               metric="cosine")
        )
        sync(nodes=[
            {"key": "vl-a", "label": "VecSvc", "props": {"embedding": [1.0, 0.0, 0.0]}},
            {"key": "vl-b", "label": "VecSvc", "props": {"embedding": [0.9, 0.1, 0.0]}},
            {"key": "vl-c", "label": "VecSvc", "props": {"embedding": [0.0, 1.0, 0.0]}},
            {"key": "vl-d", "label": "Other", "props": {"embedding": [0.0, 0.2, 0.9]}},
        ])

    def test_create_returns_declaration(self):
        index = {"name": self.INDEX, "prop": "embedding", "dim": 3, "metric": "cosine"}
        # pinned by setUpClass; re-check shape via the declaration face
        listed = [d for d in self.client.vector_list_indexes()
                  if d["name"] == self.INDEX]
        self.assertEqual(len(listed), 1)
        for key, want in index.items():
            self.assertEqual(listed[0][key], want)
        for key in ("m", "efConstruction"):
            self.assertIn(key, listed[0])

    def test_list_shape(self):
        decls = self.client.vector_list_indexes()
        self.assertIsInstance(decls, list)
        for d in decls:
            for key in ("name", "prop", "dim", "metric", "m", "efConstruction"):
                self.assertIn(key, d)

    def test_search_by_query_vector(self):
        res = self.client.vector_search(self.INDEX, query_vector=[1.0, 0.0, 0.0], k=10)
        self.assertEqual(res["count"], 4)  # all four nodes carry valid vectors
        self.assertEqual(res["hits"][0]["key"], "vl-a")  # exact self-match first
        keys = {h["key"] for h in res["hits"]}
        self.assertEqual(keys, {"vl-a", "vl-b", "vl-c", "vl-d"})
        for hit in res["hits"]:
            # exact matches score 0, which JSON decodes as int
            self.assertIsInstance(hit["score"], (int, float))

    def test_search_by_similar_to(self):
        res = self.client.vector_search(self.INDEX, similar_to="vl-a", k=2)
        self.assertEqual(res["count"], 2)
        self.assertEqual(res["hits"][0]["key"], "vl-a")

    def test_search_label_filter(self):
        res = self.client.vector_search(self.INDEX, query_vector=[1.0, 0.0, 0.0],
                                        k=10, label="Other")
        self.assertEqual(res["count"], 1)
        self.assertEqual(res["hits"][0]["key"], "vl-d")

    def test_drop_then_typed_404(self):
        # self-contained: uses its own index so test order never matters
        name = "vl-drop"
        declare_and_await(
            lambda: self.client.vector_create_index(name, prop="embedding", dim=3)
        )
        self.assertIn(name, [d["name"] for d in self.client.vector_list_indexes()])
        # the drop is a raft write: read the pre-drop seq, then await the
        # entry so list/search observe the dropped declaration
        before = self.client.stats()["publishedSeq"]
        self.assertIsNone(self.client.vector_drop_index(name))
        aw = self.client.await_seq(int(before) + 1)
        self.assertTrue(aw["reached"])
        self.assertNotIn(name, [d["name"] for d in self.client.vector_list_indexes()])
        with self.assertRaises(VectraNotFoundError) as ctx:
            self.client.vector_search(name, query_vector=[1.0, 0.0, 0.0])
        self.assertEqual(ctx.exception.status, 404)
        self.assertEqual(ctx.exception.code, "VECTOR_INDEX_NOT_FOUND")
        # dropping again answers the same typed 404
        with self.assertRaises(VectraNotFoundError):
            self.client.vector_drop_index(name)


class TestVectorErrors(VectorTestCase):
    INDEX = "ve-idx"

    @classmethod
    def setUpClass(cls):
        declare_and_await(
            lambda: CLIENT.vector_create_index(cls.INDEX, prop="embedding", dim=2,
                                               metric="l2")
        )

    def test_duplicate_create_is_envelope_409(self):
        with self.assertRaises(VectraConflictError) as ctx:
            self.client.vector_create_index(self.INDEX, prop="embedding", dim=2)
        e = ctx.exception
        # the ENVELOPE face: the server's own code, not the txn conflict face
        self.assertEqual(e.status, 409)
        self.assertEqual(e.code, "VECTOR_INDEX_EXISTS")
        self.assertNotIsInstance(e, TxnConflictError)
        self.assertFalse(hasattr(e, "conflicts"))

    def test_bad_metric_is_400_validation(self):
        with self.assertRaises(VectraError) as ctx:
            self.client.vector_create_index("ve-bad", prop="embedding", dim=2,
                                            metric="manhattan")
        e = ctx.exception
        self.assertEqual(e.status, 400)
        self.assertEqual(e.code, "VALIDATION")
        self.assertIn("metric", e.message)

    def test_dim_mismatch_is_400_validation(self):
        with self.assertRaises(VectraError) as ctx:
            self.client.vector_search(self.INDEX, query_vector=[1.0])
        self.assertEqual(ctx.exception.status, 400)
        self.assertEqual(ctx.exception.code, "VALIDATION")

    def test_unknown_index_is_typed_404(self):
        with self.assertRaises(VectraNotFoundError) as ctx:
            self.client.vector_search("ve-no-such-index", query_vector=[0.5, 0.5])
        self.assertEqual(ctx.exception.status, 404)
        self.assertEqual(ctx.exception.code, "VECTOR_INDEX_NOT_FOUND")

    def test_client_requires_exactly_one_query_mode(self):
        with self.assertRaises(ValueError):
            self.client.vector_search(self.INDEX)  # neither
        with self.assertRaises(ValueError):
            self.client.vector_search(self.INDEX, query_vector=[1.0, 0.0],
                                      similar_to="vl-a")  # both


class TestIndexDDL(VectorTestCase):
    @classmethod
    def setUpClass(cls):
        sync(nodes=[
            {"key": "ddl-a", "label": "DdlSvc", "props": {"tier": "gold", "desc": "alpha"}},
            {"key": "ddl-b", "label": "DdlSvc", "props": {"tier": "silver", "desc": "beta"}},
            {"key": "ddl-c", "label": "DdlSvc", "props": {"tier": "gold", "desc": "gamma"}},
        ])

    def test_index_property_summary(self):
        before = self.client.stats()["publishedSeq"]
        res = self.client.index_property("tier")
        # write face: summary counters, never a count key
        self.assertNotIn("count", res)
        self.assertEqual(res["summary"]["indexesCreated"], 1)
        self.assertEqual(res["summary"]["createdNodes"], 0)
        aw = self.client.await_seq(int(before) + 1)
        self.assertTrue(aw["reached"])

    def test_typed_lookup_after_seal(self):
        # the declaration merges at the next Seal (each small-graph raft
        # entry publishes); once published the typed equality anchor works
        res = self.client.cypher("MATCH (n:DdlSvc {tier:'gold'}) RETURN n.key ORDER BY n.key")
        self.assertEqual(res["count"], 2)
        self.assertEqual([r["n.key"] for r in res["rows"]], ["ddl-a", "ddl-c"])

    def test_index_full_text_summary(self):
        res = self.client.index_full_text("desc")
        self.assertEqual(res["summary"]["indexesCreated"], 1)
        # pending declarations are idempotent to re-declare (still counted)
        res = self.client.index_full_text("desc")
        self.assertEqual(res["summary"]["indexesCreated"], 1)


class TestAuthRoundTrip(unittest.TestCase):
    """Boots a SECOND server with -auth-token-file (folded Task-1 item).

    Good token round-trips writes/reads; a wrong token gets the typed 401
    envelope; GET /v1/health stays open as the exempt probe route.
    """

    TOKEN = "sdk-e2e-token-7f3a"

    @classmethod
    def setUpClass(cls):
        fd, cls.token_file = tempfile.mkstemp(prefix="vectra-sdk-token-")
        with os.fdopen(fd, "w") as fh:
            fh.write(cls.TOKEN + "\n")
        cls.addClassCleanup(cls._remove_token_file)
        cls.fixture = harness.ServerFixture(BINARY, auth_token_file=cls.token_file)
        cls.fixture.__enter__()
        cls.addClassCleanup(cls.fixture.__exit__, None, None, None)

    @classmethod
    def _remove_token_file(cls):
        if os.path.exists(cls.token_file):
            os.unlink(cls.token_file)

    def test_good_token_write_and_read(self):
        client = Client(base_url=self.fixture.url, token=self.TOKEN)
        before = client.stats()["publishedSeq"]
        client.batch(nodes=[{"key": "auth-ok", "label": "Auth", "props": {"v": 1}}])
        aw = client.await_seq(int(before) + 1)
        self.assertTrue(aw["reached"])
        node = client.node("auth-ok")
        self.assertEqual(node["label"], "Auth")
        self.assertEqual(node["props"]["v"], 1)

    def test_wrong_token_is_typed_401(self):
        client = Client(base_url=self.fixture.url, token="definitely-wrong")
        with self.assertRaises(VectraAuthError) as ctx:
            client.batch(nodes=[{"key": "auth-bad", "label": "Auth"}])
        e = ctx.exception
        self.assertEqual(e.status, 401)
        self.assertEqual(e.code, "UNAUTHORIZED")
        # every non-exempt route is gated, reads included
        with self.assertRaises(VectraAuthError):
            client.node("auth-ok")

    def test_health_stays_open_without_token(self):
        client = Client(base_url=self.fixture.url, token="")
        self.assertEqual(client.health()["status"], "ok")


if __name__ == "__main__":
    unittest.main()
