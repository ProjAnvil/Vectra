"""Thin HTTP client for the Vectra REST API (/v1/*)."""

from __future__ import annotations

import os
from typing import Any

import httpx


class VectraError(RuntimeError):
    """Vectra API returned a non-OK response."""


class Client:
    def __init__(self, base_url: str | None = None, timeout: float = 60.0):
        self.base_url = (base_url or os.environ.get("VECTRA_URL", "http://127.0.0.1:8080")).rstrip("/")
        self._http = httpx.Client(base_url=self.base_url, timeout=timeout)

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
        return self._check(self._http.get(f"/v1/nodes/{key}"))

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
