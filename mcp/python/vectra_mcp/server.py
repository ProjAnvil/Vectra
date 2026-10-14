"""Vectra MCP server built on FastMCP.

Run modes:
    vectra-mcp-python                  # stdio (local MCP host)
    vectra-mcp-python --http :9000     # Streamable HTTP

Target service is a running Vectra server node; set VECTRA_URL
(default http://127.0.0.1:8080).
"""

from __future__ import annotations

import argparse
import os

from fastmcp import FastMCP

from .client import Client, VectraError

mcp: FastMCP = FastMCP(
    "vectra",
    instructions=(
        "Vectra dependency-graph engine via its HTTP API. Writes go through "
        "inject_batch (raft-replicated by the server); reads are served from "
        "the server's published snapshot. Tools: neighbors (1-hop), bfs "
        "(multi-hop), shortest_path, reachable (blast radius), cycles, stats."
    ),
)

_client = Client()


def _c() -> Client:
    return _client


def _limit(keys: list[str], limit: int) -> dict:
    return {"count": min(len(keys), limit), "total": len(keys), "nodes": keys[:limit]}


@mcp.tool
def inject_batch(nodes: list[dict], edges: list[dict]) -> dict:
    """Upsert nodes/edges (same keys update). nodes: [{key,label,props}],
    edges: [{srcKey,dstKey,label,props,allowParallel?}] — set allowParallel=true
    to append a true parallel edge instead of collapsing onto the existing
    (srcKey,dstKey,label) upsert. Returns applied/skipped counts."""
    try:
        return _c().batch(nodes, edges)
    except VectraError as e:
        return {"error": str(e)}


@mcp.tool
def stats() -> dict:
    """Graph overview: nodes/edges/sccs/cyclicNodes + leader/term."""
    return _c().stats()


@mcp.tool
def get_node(key: str) -> dict:
    """Fetch a node by key (label and properties)."""
    return _c().node(key)


@mcp.tool
def neighbors(key: str, direction: str = "out", label: str = "", limit: int = 1000) -> dict:
    """One-hop neighbors. direction: out|in; optional edge-label filter."""
    res = _c().neighbors(key, direction, label)
    keys = [n["key"] for n in res.get("nodes", [])]
    return _limit(keys, limit)


@mcp.tool
def bfs(frm: list[str], max_depth: int = 0, labels: list[str] | None = None,
        direction: str = "out", limit: int = 1000) -> dict:
    """Multi-hop traversal (1..max_depth hops; 0 = unlimited). Returns each
    reached node's depth. total reflects the true size even when truncated."""
    res = _c().bfs(frm, max_depth, labels, direction)
    nodes = res.get("nodes", [])
    keys = [n["key"] for n in nodes]
    out = _limit(keys, limit)
    out["depths"] = {n["key"]: n["depth"] for n in nodes[:limit]}
    return out


@mcp.tool
def shortest_path(frm: str, to: str) -> dict:
    """Shortest path between two nodes (bidirectional BFS)."""
    return _c().path(frm, to)


@mcp.tool
def reachable(frm: str, limit: int = 1000) -> dict:
    """All nodes reachable from a start (dependency blast radius)."""
    res = _c().reachable(frm)
    return _limit(res.get("nodes", []), limit)


@mcp.tool
def cycles() -> dict:
    """All dependency cycles (SCC member lists, largest first)."""
    return _c().cycles()


def main() -> None:
    parser = argparse.ArgumentParser(prog="vectra-mcp-python")
    parser.add_argument("--http", metavar="ADDR", default=None,
                        help="Streamable HTTP listen address (default: stdio)")
    parser.add_argument("--vectra-url", default=os.environ.get("VECTRA_URL", "http://127.0.0.1:8080"),
                        help="Vectra server HTTP base URL")
    args = parser.parse_args()
    global _client
    _client = Client(args.vectra_url)
    if args.http:
        host, _, port = args.http.partition(":")
        mcp.run(transport="http", host=host or "0.0.0.0", port=int(port or 9000))
    else:
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
