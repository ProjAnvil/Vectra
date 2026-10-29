"""End-to-end tests for POST /v1/txn: optimistic (OCC) transactions.

The guard carries the snapshot token the caller's reads were based on — a
raft log index, the same number GET /v1/stats reports as ``publishedSeq``
and reads return as ``X-Vectra-Snapshot`` (kept on ``Client.last_snapshot``).
A guarded commit rejected by validation answers 409 with a NON-envelope
body ``{"committed": false, "conflicts": [...], "conflictTotal": n,
"hint": ...}``; the client raises that as ``TxnConflictError`` (a
``VectraConflictError`` subclass carrying the conflict fields) while every
other 409 keeps the ``{"error": {...}}`` envelope face (pinned in
test_vector.py against VECTOR_INDEX_EXISTS).
"""

from __future__ import annotations

import os
import sys
import unittest
from unittest import mock

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_SDK_ROOT = os.path.dirname(_TESTS_DIR)
# src-layout package, exercised without installation
sys.path.insert(0, os.path.join(_SDK_ROOT, "src"))

import harness  # noqa: E402  (tests dir is on sys.path via unittest discovery)
from vectra_graph import (  # noqa: E402
    Client,
    TxnConflictError,
    VectraConflictError,
    VectraNotFoundError,
)

FIXTURE = None  # harness.ServerFixture, set in setUpModule
CLIENT = None   # primary client, set in setUpModule
CLIENT2 = None  # second client: the concurrent writer in conflict tests


def setUpModule():
    global FIXTURE, CLIENT, CLIENT2
    FIXTURE = harness.ServerFixture(harness.build_server())
    FIXTURE.__enter__()
    CLIENT = Client(base_url=FIXTURE.url)
    CLIENT2 = Client(base_url=FIXTURE.url)


def tearDownModule():
    if FIXTURE is not None:
        FIXTURE.__exit__(None, None, None)
    harness.cleanup_build()


def sync(client, **kwargs):
    """Apply one auto-commit batch on ``client``, then await its publish.

    Awaits the raft entry's publication so later reads (on any client)
    observe the write deterministically; returns the batch response.
    """
    before = client.stats()["publishedSeq"]
    result = client.batch(**kwargs)
    aw = client.await_seq(int(before) + 1)
    assert aw["reached"], "write not published: awaited %d, got %r" % (before + 1, aw)
    return result


class TxnTestCase(unittest.TestCase):
    """Base class: the shared module client plus the concurrent-writer client."""

    def setUp(self):
        self.client = CLIENT
        self.other = CLIENT2


class TestTxnCommit(TxnTestCase):
    def test_guarded_upsert_and_delete_commit(self):
        sync(self.client,
             nodes=[{"key": "tc-seed-a", "label": "Txn", "props": {"v": 1}},
                    {"key": "tc-seed-b", "label": "Txn", "props": {}}])
        # guard snapshot taken from stats (publishedSeq == raft index)
        snapshot = self.client.stats()["publishedSeq"]
        res = self.client.txn(
            nodes=[{"key": "tc-seed-a", "label": "Txn", "props": {"v": 2}}],
            node_deletes=["tc-seed-b"],
            snapshot=snapshot,
        )
        self.assertTrue(res["committed"])
        self.assertEqual(res["nodesApplied"], 1)
        self.assertEqual(res["nodesDeleted"], 1)
        self.assertEqual(res["edgesApplied"], 0)
        self.assertGreaterEqual(res["index"], snapshot)
        # the committed entry publishes like any raft write
        aw = self.client.await_seq(int(res["index"]))
        self.assertTrue(aw["reached"])
        n = self.client.node("tc-seed-a")
        self.assertEqual(n["props"]["v"], 2)
        with self.assertRaises(VectraNotFoundError):
            self.client.node("tc-seed-b")

    def test_unguarded_fresh_keys_commit_at_snapshot_zero(self):
        # snapshot 0 reads "at epoch 0": fresh keys never written before have
        # no version entry, so a zero-snapshot guard on them commits cleanly
        res = self.client.txn(nodes=[{"key": "tc-zero", "label": "Txn", "props": {}}])
        self.assertTrue(res["committed"])
        self.assertEqual(res["nodesApplied"], 1)
        self.assertTrue(self.client.await_seq(int(res["index"]))["reached"])
        self.assertEqual(self.client.node("tc-zero")["key"], "tc-zero")


class TestTxnConflict(TxnTestCase):
    def test_stale_write_set_conflicts(self):
        sync(self.client, nodes=[{"key": "tc-c1", "label": "Txn", "props": {"v": 1}}])
        snapshot = self.client.stats()["publishedSeq"]
        # a second client commits past the snapshot...
        sync(self.other, nodes=[{"key": "tc-c1", "label": "Txn", "props": {"v": 2}}])
        with self.assertRaises(TxnConflictError) as ctx:
            self.client.txn(
                nodes=[{"key": "tc-c1", "label": "Txn", "props": {"v": 3}}],
                snapshot=snapshot,
            )
        e = ctx.exception
        # the conflict face: VectraConflictError subclass, 409, non-envelope
        # body exposed as attributes, code synthesized by the client
        self.assertIsInstance(e, VectraConflictError)
        self.assertEqual(e.status, 409)
        self.assertEqual(e.code, "TXN_CONFLICT")
        self.assertIn("tc-c1", e.conflicts)
        self.assertGreaterEqual(e.conflict_total, 1)
        self.assertTrue(e.hint)
        self.assertFalse(e.committed)
        # nothing partial was applied: the concurrent write is intact
        self.assertEqual(self.client.node("tc-c1")["props"]["v"], 2)

    def test_stale_edge_write_conflicts(self):
        sync(self.client, nodes=[{"key": "tc-e1", "label": "Txn"},
                                 {"key": "tc-e2", "label": "Txn"}])
        snapshot = self.client.stats()["publishedSeq"]
        sync(self.other,
             edges=[{"srcKey": "tc-e1", "dstKey": "tc-e2", "label": "CALLS"}])
        with self.assertRaises(TxnConflictError) as ctx:
            self.client.txn(
                edges=[{"srcKey": "tc-e1", "dstKey": "tc-e2", "label": "CALLS",
                        "props": {"w": 2}}],
                snapshot=snapshot,
            )
        self.assertEqual(ctx.exception.code, "TXN_CONFLICT")
        # edge conflicts report the "src\0dst\0label" key string
        self.assertTrue(any("tc-e1" in c for c in ctx.exception.conflicts))

    def test_refreshed_snapshot_retry_commits(self):
        # the hint's contract: refresh the snapshot, retry, expect success
        sync(self.client, nodes=[{"key": "tc-r1", "label": "Txn", "props": {"v": 1}}])
        stale = self.client.stats()["publishedSeq"]
        sync(self.other, nodes=[{"key": "tc-r1", "label": "Txn", "props": {"v": 2}}])
        with self.assertRaises(TxnConflictError):
            self.client.txn(nodes=[{"key": "tc-r1", "label": "Txn", "props": {"v": 3}}],
                            snapshot=stale)
        fresh = self.client.stats()["publishedSeq"]
        res = self.client.txn(nodes=[{"key": "tc-r1", "label": "Txn", "props": {"v": 3}}],
                              snapshot=fresh)
        self.assertTrue(res["committed"])
        self.assertTrue(self.client.await_seq(int(res["index"]))["reached"])
        self.assertEqual(self.client.node("tc-r1")["props"]["v"], 3)


class TestTxnSER(TxnTestCase):
    def test_clean_read_set_commits(self):
        sync(self.client, nodes=[{"key": "tc-s1", "label": "Txn", "props": {"v": 1}}])
        # the read returns X-Vectra-Snapshot, recorded on Client.last_snapshot
        self.client.node("tc-s1")
        snapshot = self.client.last_snapshot
        self.assertIsNotNone(snapshot)
        res = self.client.txn(
            nodes=[{"key": "tc-s1", "label": "Txn", "props": {"v": 2}}],
            snapshot=snapshot,
            ser=True,
            read_nodes=["tc-s1"],
        )
        self.assertTrue(res["committed"])
        self.assertTrue(self.client.await_seq(int(res["index"]))["reached"])
        self.assertEqual(self.client.node("tc-s1")["props"]["v"], 2)

    def test_stale_read_set_rejects(self):
        sync(self.client, nodes=[{"key": "tc-s2", "label": "Txn", "props": {"v": 1}}])
        self.client.node("tc-s2")
        snapshot = self.client.last_snapshot
        # the read key is rewritten after the snapshot (write set below is a
        # fresh key, so ONLY the SER read set can produce the conflict)
        sync(self.other, nodes=[{"key": "tc-s2", "label": "Txn", "props": {"v": 2}}])
        with self.assertRaises(TxnConflictError) as ctx:
            self.client.txn(
                nodes=[{"key": "tc-unrelated", "label": "Txn", "props": {}}],
                snapshot=snapshot,
                ser=True,
                read_nodes=["tc-s2"],
            )
        e = ctx.exception
        self.assertEqual(e.code, "TXN_CONFLICT")
        self.assertIn("tc-s2", e.conflicts)
        self.assertEqual(e.conflict_total, 1)
        # the guarded write set was not applied either
        with self.assertRaises(VectraNotFoundError):
            self.client.node("tc-unrelated")


class TestClientEnvHandling(unittest.TestCase):
    """Folded Task-1 review minor: VECTRA_URL is whitespace-stripped."""

    def test_vectra_url_and_token_stripped(self):
        env = {
            "VECTRA_URL": "  http://127.0.0.1:9999/  \n",
            "VECTRA_TOKEN": " tok \n",
        }
        with mock.patch.dict(os.environ, env):
            client = Client()
        self.assertEqual(client.base_url, "http://127.0.0.1:9999")
        self.assertEqual(client.token, "tok")


if __name__ == "__main__":
    unittest.main()
