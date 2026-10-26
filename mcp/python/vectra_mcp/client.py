"""Thin HTTP client for the Vectra REST API (/v1/*)."""

from __future__ import annotations

import os
import urllib.parse
from typing import Any

import httpx


class VectraError(RuntimeError):
    """Vectra API returned a non-OK response."""


class Client:
    def __init__(self, base_url: str | None = None, timeout: float = 60.0):
        self.base_url = (base_url or os.environ.get("VECTRA_URL", "http://127.0.0.1:8080")).rstrip("/")
        # .strip(): copy-pasted tokens often carry a trailing newline, which
        # would otherwise ride into the Authorization header as a guaranteed 401.
        token = os.environ.get("VECTRA_TOKEN", "").strip()
        self._http = httpx.Client(
            base_url=self.base_url,
            timeout=timeout,
            headers={"Authorization": f"Bearer {token}"} if token else {},
        )

    def _check(self, resp: httpx.Response) -> dict[str, Any]:
        if resp.status_code >= 400:
            try:
                err = resp.json().get("error", {})
                raise VectraError(f"{resp.status_code} {err.get('code', '')}: {err.get('message', resp.text)}")
            except ValueError:
                raise VectraError(f"{resp.status_code}: {resp.text}") from None
        return resp.json()

    # write
    def batch(self, nodes: list[dict], edges: list[dict]) -> dict:
        return self._check(self._http.post("/v1/batch", json={"nodes": nodes, "edges": edges}))

    # query
    def stats(self) -> dict:
        return self._check(self._http.get("/v1/stats"))

    def node(self, key: str) -> dict:
        return self._check(self._http.get(f"/v1/nodes/{urllib.parse.quote(key, safe='')}"))

    def neighbors(self, key: str, direction: str = "out", label: str = "") -> dict:
        params = {"key": key, "dir": direction}
        if label:
            params["label"] = label
        return self._check(self._http.get("/v1/neighbors", params=params))

    def bfs(self, frm: list[str], max_depth: int = 0, labels: list[str] | None = None,
            direction: str = "out") -> dict:
        body: dict[str, Any] = {"from": frm, "direction": direction}
        if max_depth:
            body["maxDepth"] = max_depth
        if labels:
            body["labels"] = labels
        return self._check(self._http.post("/v1/query", json=body))

    def path(self, frm: str, to: str) -> dict:
        return self._check(self._http.get("/v1/path", params={"from": frm, "to": to}))

    def reachable(self, frm: str) -> dict:
        return self._check(self._http.get("/v1/reachable", params={"from": frm}))

    def cycles(self) -> dict:
        return self._check(self._http.get("/v1/cycles"))

    def cypher(self, query: str) -> dict:
        return self._check(self._http.post("/v1/cypher", json={"query": query}))

    # vector (indexes declared per prop; search results are approximate)
    def vector_search(self, index: str, query_vector: list[float] | None = None,
                      similar_to: str = "", k: int = 10, ef: int = 0, label: str = "") -> dict:
        body: dict[str, Any] = {"index": index, "k": k}
        if query_vector:
            body["query_vector"] = query_vector
        if similar_to:
            body["similar_to"] = similar_to
        if ef:
            body["ef"] = ef
        if label:
            body["label"] = label
        return self._check(self._http.post("/v1/vector/search", json=body))

    def vector_index_create(self, name: str, prop: str, dim: int, metric: str = "cosine",
                            m: int = 0, ef_construction: int = 0) -> dict:
        body: dict[str, Any] = {"name": name, "prop": prop, "dim": dim, "metric": metric}
        if m:
            body["m"] = m
        if ef_construction:
            body["efConstruction"] = ef_construction
        return self._check(self._http.post("/v1/vector/indexes", json=body))

    def vector_index_drop(self, name: str) -> dict:
        return self._check(self._http.delete(f"/v1/vector/indexes/{urllib.parse.quote(name, safe='')}"))

    def vector_index_list(self) -> dict:
        return self._check(self._http.get("/v1/vector/indexes"))
