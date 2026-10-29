"""Synchronous, stdlib-only HTTP client for the Vectra REST API (/v1/*).

No third-party dependencies: transport is ``urllib.request``. Every call
carries a timeout. Errors arrive as the typed exceptions from
:mod:`vectra_graph.errors` (HTTP status + server error code).
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Mapping, Optional

from .errors import (
    TxnConflictError,
    VectraAuthError,
    VectraConflictError,
    VectraError,
    VectraNotFoundError,
)


class Client:
    """Vectra REST API client.

    Args:
        base_url: server base URL; defaults to ``$VECTRA_URL`` then
            ``http://127.0.0.1:8080`` (both whitespace-stripped, trailing
            slash dropped).
        token: bearer token; defaults to ``$VECTRA_TOKEN`` (stripped —
            copy-pasted tokens often carry a trailing newline that would
            otherwise guarantee a 401). The Authorization header is only
            attached when the token is non-empty, matching auth-off servers.
        timeout: per-request timeout in seconds, applied to every call.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        token: Optional[str] = None,
        timeout: float = 30.0,
    ) -> None:
        self.base_url = (
            base_url or os.environ.get("VECTRA_URL", "http://127.0.0.1:8080")
        ).strip().rstrip("/")
        if token is None:
            token = os.environ.get("VECTRA_TOKEN", "")
        self.token = token.strip()
        self.timeout = timeout
        #: Sequence number from the most recent ``X-Vectra-Snapshot`` response
        #: header; ``None`` until one is seen. Reads, awaits, and error
        #: responses from read endpoints all refresh it (the server sets the
        #: header before writing any error body). Use it as the snapshot
        #: token for optimistic transactions.
        self.last_snapshot: Optional[int] = None

    # ---- transport -----------------------------------------------------

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Mapping[str, Any]] = None,
        body: Any = None,
        timeout: Optional[float] = None,
    ) -> dict:
        """One request through the shared transport: build URL + headers,
        JSON-encode the body, decode the response, map error envelopes onto
        typed exceptions, and record the snapshot header when present."""
        url = self.base_url + path
        if params:
            url += "?" + urllib.parse.urlencode(params)
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(
                request, timeout=self.timeout if timeout is None else timeout
            ) as response:
                raw = response.read()
                self._record_snapshot(response.headers)
        except urllib.error.HTTPError as exc:
            # Error responses carry X-Vectra-Snapshot too (the header is set
            # before any error write on read endpoints) — record it the same
            # way, matching the TS client.
            self._record_snapshot(getattr(exc, "hdrs", None))
            raise self._typed_error(exc) from None
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))

    def _record_snapshot(self, headers: Any) -> None:
        """Set :attr:`last_snapshot` from a response-headers mapping.

        A ``None`` mapping (or a header value that is not an integer) is a
        no-op — never let snapshot bookkeeping mask the real response.
        """
        if headers is None:
            return
        snapshot = headers.get("X-Vectra-Snapshot")
        if snapshot is not None:
            try:
                self.last_snapshot = int(snapshot)
            except ValueError:
                pass

    @staticmethod
    def _typed_error(exc: urllib.error.HTTPError) -> VectraError:
        """Decode the response body and pick the exception class.

        The ``{"error": {"code", "message"}}`` envelope maps by HTTP status:
        401/403 -> Auth, 404 -> NotFound, 409 -> Conflict, anything else ->
        base VectraError. One documented deviation: a 409 whose body is the
        txn conflict *report* (``committed``/``conflicts`` keys, no
        envelope — server/api.go handleTxn) becomes
        :class:`TxnConflictError` carrying the conflict fields.
        """
        code = ""
        message = str(exc.reason)
        payload = None
        try:
            payload = json.loads(exc.read().decode("utf-8"))
        except (ValueError, OSError):
            pass
        if isinstance(payload, dict):
            envelope = payload.get("error")
            if isinstance(envelope, dict):
                code = str(envelope.get("code", ""))
                server_message = str(envelope.get("message", ""))
                if server_message:
                    message = server_message
        if (
            exc.code == 409
            and not code
            and isinstance(payload, dict)
            and "committed" in payload
            and "conflicts" in payload
        ):
            hint = str(payload.get("hint", ""))
            conflicts = payload.get("conflicts") or []
            # conflictTotal is defensive: fall back to the list length when
            # the field is missing or null rather than raising on int(None).
            raw_total = payload.get("conflictTotal")
            conflict_total = int(raw_total) if raw_total is not None else len(conflicts)
            message = "transaction conflict" + (": " + hint if hint else "")
            return TxnConflictError(
                message,
                conflicts=[str(c) for c in conflicts],
                conflict_total=conflict_total,
                hint=hint,
            )
        if exc.code in (401, 403):
            return VectraAuthError(message, exc.code, code)
        if exc.code == 404:
            return VectraNotFoundError(message, exc.code, code)
        if exc.code == 409:
            return VectraConflictError(message, exc.code, code)
        return VectraError(message, exc.code, code)

    # ---- reads ----------------------------------------------------------

    def health(self) -> dict:
        """GET /v1/health — liveness plus leader/term.

        Gated on the FSM's first raft publish (503 "starting" before it,
        i.e. before the first write); :meth:`await_seq` is the endpoint
        with no readiness gate.
        """
        return self._request("GET", "/v1/health")

    def stats(self) -> dict:
        """GET /v1/stats — graph counters, raft term, publishedSeq."""
        return self._request("GET", "/v1/stats")

    def node(self, key: str) -> dict:
        """GET /v1/nodes/{key} — one node; raises VectraNotFoundError on 404."""
        quoted = urllib.parse.quote(key, safe="")
        return self._request("GET", "/v1/nodes/" + quoted)

    def neighbors(self, key: str, direction: str = "out", label: str = "") -> dict:
        """GET /v1/neighbors — adjacent nodes ("out"/"in", optional label)."""
        params: dict = {"key": key, "dir": direction}
        if label:
            params["label"] = label
        return self._request("GET", "/v1/neighbors", params=params)

    def bfs(
        self,
        from_: list,
        max_depth: int = 0,
        labels: Optional[list] = None,
        direction: str = "out",
    ) -> dict:
        """POST /v1/query — BFS from a set of start keys.

        max_depth 0 means unlimited hops; labels restrict traversal to
        edges carrying one of the given labels.
        """
        body: dict = {"from": from_, "direction": direction}
        if max_depth:
            body["maxDepth"] = max_depth
        if labels:
            body["labels"] = labels
        return self._request("POST", "/v1/query", body=body)

    def path(self, from_: str, to: str) -> dict:
        """GET /v1/path — shortest path ("found", "hops", "nodes")."""
        return self._request("GET", "/v1/path", params={"from": from_, "to": to})

    def reachable(self, from_: str) -> dict:
        """GET /v1/reachable — keys of all nodes reachable from ``from_``."""
        return self._request("GET", "/v1/reachable", params={"from": from_})

    def cycles(self) -> dict:
        """GET /v1/cycles — elementary cycles as key lists."""
        return self._request("GET", "/v1/cycles")

    def cypher(self, query: str) -> dict:
        """POST /v1/cypher — one Cypher-subset statement.

        READ statements return ``{"columns", "rows", "count"}``; WRITE
        statements (CREATE/SET/DELETE/MERGE/CREATE INDEX) return
        ``{"columns", "rows", "summary"}`` and never a ``count`` key. Parse
        and semantic errors surface as VectraError with code ``"PARSE"``.
        """
        return self._request("POST", "/v1/cypher", body={"query": query})

    # ---- writes -----------------------------------------------------------

    def batch(
        self,
        nodes: Optional[list] = None,
        edges: Optional[list] = None,
        edge_deletes: Optional[list] = None,
        node_deletes: Optional[list] = None,
    ) -> dict:
        """POST /v1/batch — auto-commit changeset, applied in fixed order:
        node upserts -> edge upserts -> edge deletes -> node deletes.

        Item shapes (see server/batch.go): node ``{"key", "label", "props"}``;
        edge ``{"srcKey", "dstKey", "label", "props", "allowParallel"}``;
        edge delete ``{"srcKey", "dstKey", "label"}``. Returns applied
        counts (nodesApplied, edgesApplied, edgesSkipped, edgesDeleted,
        nodesDeleted, nodesSkipped).
        """
        return self._request(
            "POST",
            "/v1/batch",
            body={
                "nodes": nodes or [],
                "edges": edges or [],
                "edgeDeletes": edge_deletes or [],
                "nodeDeletes": node_deletes or [],
            },
        )

    def txn(
        self,
        nodes: Optional[list] = None,
        edges: Optional[list] = None,
        edge_deletes: Optional[list] = None,
        node_deletes: Optional[list] = None,
        snapshot: int = 0,
        ser: bool = False,
        read_nodes: Optional[list] = None,
        read_edges: Optional[list] = None,
    ) -> dict:
        """POST /v1/txn — optimistic-concurrency (OCC) transactional commit.

        Item shapes and fixed apply order match :meth:`batch`; the extra
        ``txn`` guard carries the snapshot token the caller's reads were
        based on (a raft log index — take it from
        ``stats()["publishedSeq"]`` or ``Client.last_snapshot``, refreshed
        by every read/await) plus an optional SER read set.

        Validation always covers the batch's own write set: a key written
        by a raft entry newer than ``snapshot`` rejects the whole commit.
        With ``ser=True`` the read set is validated the same way —
        ``read_nodes`` are node keys, ``read_edges`` are
        ``{"src", "dst", "label"}`` dicts. ``snapshot=0`` means "read at
        epoch 0" and conflicts with any prior write to the same keys, so
        always pass a real snapshot (fresh never-written keys commit fine
        at 0).

        A rejected commit raises :class:`TxnConflictError` — the txn 409
        body is a conflict report, not an error envelope — exposing
        ``conflicts``/``conflict_total``/``hint``; refresh the snapshot and
        retry. A commit returns ``{"committed": True, "index",
        "nodesApplied", "edgesApplied", "edgesSkipped", "edgesDeleted",
        "nodesDeleted", "nodesSkipped"}``.
        """
        guard: dict = {"snapshot": int(snapshot), "ser": bool(ser)}
        if read_nodes:
            guard["readNodes"] = list(read_nodes)
        if read_edges:
            guard["readEdges"] = list(read_edges)
        return self._request(
            "POST",
            "/v1/txn",
            body={
                "nodes": nodes or [],
                "edges": edges or [],
                "edgeDeletes": edge_deletes or [],
                "nodeDeletes": node_deletes or [],
                "txn": guard,
            },
        )

    def await_seq(self, seq: int, timeout_ms: int = 5000) -> dict:
        """GET /v1/await — read-your-writes long poll.

        Blocks server-side until ``publishedSeq >= seq`` or ``timeout_ms``
        elapses (server caps the wait at 30s), then returns
        ``{"publishedSeq", "reached"}``. The ``X-Vectra-Snapshot`` response
        header is recorded on ``Client.last_snapshot``. The client-side
        timeout is raised to cover the server wait plus a margin.
        """
        effective_timeout = max(self.timeout, timeout_ms / 1000.0 + 5.0)
        return self._request(
            "GET",
            "/v1/await",
            params={"seq": seq, "timeoutMs": timeout_ms},
            timeout=effective_timeout,
        )

    # ---- vector indexes + index DDL -------------------------------------

    def vector_search(
        self,
        index: str,
        *,
        query_vector: Optional[list] = None,
        similar_to: str = "",
        k: int = 10,
        ef: int = 0,
        label: str = "",
    ) -> dict:
        """POST /v1/vector/search — approximate top-k over a declared index.

        Exactly one of ``query_vector`` (dim must match the declaration;
        enforced client-side as ValueError, matching the server's 400) or
        ``similar_to`` (a node key holding a valid vector on the indexed
        prop). An empty ``query_vector`` counts as absent — the server's
        own rule is ``(len == 0) == (similar_to == "")`` — so it is the
        "neither given" ValueError, not a request. Returns
        ``{"count", "hits"}`` with hits of
        ``{"key", "score"}`` — smaller score is better. ``ef`` 0 takes the
        server's adaptive default; ``label`` filters hits by node label.
        Unknown index raises VectraNotFoundError (VECTOR_INDEX_NOT_FOUND);
        a ``similar_to`` key that does not exist answers 400
        NODE_NOT_FOUND (a bad body reference, not the 404 resource face).
        """
        # bool(): an empty list mirrors the server treating len==0 as absent.
        has_vector = bool(query_vector)
        has_anchor = bool(similar_to)
        if has_vector == has_anchor:
            raise ValueError("provide exactly one of query_vector or similar_to")
        body: dict = {"index": index, "k": int(k)}
        if has_vector:
            body["query_vector"] = [float(x) for x in query_vector]
        else:
            body["similar_to"] = similar_to
        if ef:
            body["ef"] = int(ef)
        if label:
            body["label"] = label
        return self._request("POST", "/v1/vector/search", body=body)

    def vector_create_index(
        self,
        name: str,
        prop: str,
        dim: int,
        metric: str = "cosine",
        m: int = 0,
        ef_construction: int = 0,
    ) -> dict:
        """POST /v1/vector/indexes — declare a vector index (raft write).

        ``metric`` is one of ``cosine``/``l2``/``dot``; ``dim`` 1..4096;
        zero ``m``/``ef_construction`` take the engine defaults (16/200).
        A duplicate name raises VectraConflictError with the server's
        envelope code ``VECTOR_INDEX_EXISTS``; a bad metric/dim raises
        VectraError 400 ``VALIDATION``. Returns ``{"index": {...}}`` —
        the declaration reaches reads once published (await the seq
        bumped by this raft entry when in doubt).
        """
        body: dict = {
            "name": name,
            "prop": prop,
            "dim": int(dim),
            "metric": metric,
        }
        if m:
            body["m"] = int(m)
        if ef_construction:
            body["efConstruction"] = int(ef_construction)
        return self._request("POST", "/v1/vector/indexes", body=body)

    def vector_list_indexes(self) -> list:
        """GET /v1/vector/indexes — declared vector indexes.

        Each entry carries ``name``/``prop``/``dim``/``metric``/``m``/
        ``efConstruction``. Served from the published clone.
        """
        return self._request("GET", "/v1/vector/indexes")["indexes"]

    def vector_drop_index(self, name: str) -> None:
        """DELETE /v1/vector/indexes/{name} — drop a declaration.

        Raft write; returns None on success. Dropping an unknown name
        raises VectraNotFoundError (VECTOR_INDEX_NOT_FOUND).
        """
        quoted = urllib.parse.quote(name, safe="")
        self._request("DELETE", "/v1/vector/indexes/" + quoted)

    def index_property(self, prop: str) -> dict:
        """Cypher ``CREATE INDEX ON (n.prop)`` — declare a typed index.

        Declarations are global per property key (label-scoped spellings
        are rejected by the engine) and PENDING until the next
        Seal/publish — typed lookups answer via compensation scan until
        the declaration merges, then serve from the index. Idempotent:
        re-declaring still counts one ``summary.indexesCreated``. Returns
        the cypher write response; check ``summary.indexesCreated == 1``.
        """
        return self.cypher("CREATE INDEX ON (n.%s)" % prop)

    def index_full_text(self, prop: str) -> dict:
        """Cypher ``CREATE FULLTEXT INDEX ON (n.prop)`` — declare an FTS index.

        Same declaration contract as :meth:`index_property`: pending until
        the next Seal, idempotent, counted in ``summary.indexesCreated``.
        """
        return self.cypher("CREATE FULLTEXT INDEX ON (n.%s)" % prop)
