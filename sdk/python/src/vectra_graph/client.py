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
    VectraAuthError,
    VectraConflictError,
    VectraError,
    VectraNotFoundError,
)


class Client:
    """Vectra REST API client.

    Args:
        base_url: server base URL; defaults to ``$VECTRA_URL`` then
            ``http://127.0.0.1:8080``.
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
        ).rstrip("/")
        if token is None:
            token = os.environ.get("VECTRA_TOKEN", "")
        self.token = token.strip()
        self.timeout = timeout
        #: Sequence number from the most recent ``X-Vectra-Snapshot`` response
        #: header (reads and awaits carry it); ``None`` until one is seen.
        #: Use it as the snapshot token for optimistic transactions.
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
                snapshot = response.headers.get("X-Vectra-Snapshot")
        except urllib.error.HTTPError as exc:
            raise self._typed_error(exc) from None
        if snapshot is not None:
            try:
                self.last_snapshot = int(snapshot)
            except ValueError:
                pass
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))

    @staticmethod
    def _typed_error(exc: urllib.error.HTTPError) -> VectraError:
        """Decode the {"error": {"code", "message"}} envelope and pick the
        exception class by HTTP status: 401/403 -> Auth, 404 -> NotFound,
        409 -> Conflict, anything else -> base VectraError."""
        code = ""
        message = str(exc.reason)
        try:
            payload = json.loads(exc.read().decode("utf-8"))
            envelope = payload.get("error") or {}
            code = str(envelope.get("code", ""))
            server_message = str(envelope.get("message", ""))
            if server_message:
                message = server_message
        except (ValueError, AttributeError):
            pass
        if exc.code in (401, 403):
            return VectraAuthError(message, exc.code, code)
        if exc.code == 404:
            return VectraNotFoundError(message, exc.code, code)
        if exc.code == 409:
            return VectraConflictError(message, exc.code, code)
        return VectraError(message, exc.code, code)

    # ---- reads ----------------------------------------------------------

    def health(self) -> dict:
        """GET /v1/health — liveness plus leader/term (no readiness gate)."""
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
