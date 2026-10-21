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
        "(multi-hop), shortest_path, reachable (blast radius), cycles, cypher "
        "(Cypher-subset pattern queries), stats, vector_search (approximate "
        "top-k over a declared vector index)."
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


@mcp.tool
def cypher(query: str, limit: int = 1000) -> dict:
    """Query the graph with a Cypher-subset statement:
    MATCH (a:Label {prop:value})-[e:REL*1..3]->(b) WHERE b.w > 3
    RETURN a.key, b ORDER BY b.w DESC LIMIT 10.
    Node-pair semantics for *hops (shortest distance within the hop window);
    pseudo-props a.key/a.label/a.id; edge vars bind on fixed single hops
    only. total reflects the true row count even when truncated by limit."""
    res = _c().cypher(query)
    rows = res.get("rows", [])
    return {
        "columns": res.get("columns", []),
        "count": min(len(rows), limit),
        "total": len(rows),
        "rows": rows[:limit],
    }


@mcp.tool
def vector_search(index: str, query_vector: list[float] | None = None,
                  similar_to: str = "", k: int = 10, ef: int = 0,
                  label: str = "") -> dict:
    """Approximate top-k nearest-neighbor search over a declared vector
    index. Results are approximate; order across equal scores is not
    stable. Provide exactly one of query_vector or similar_to. Params:
    index, query_vector|similar_to, k (default 10, max 1000), ef, label."""
    return _c().vector_search(index, query_vector, similar_to, k, ef, label)


@mcp.tool
def vector_index_create(name: str, prop: str, dim: int, metric: str = "cosine",
                        m: int = 0, ef_construction: int = 0) -> dict:
    """Declare a vector index over a node property (HNSW). dim is 1..4096;
    metric is cosine|l2|dot (default cosine); m/ef_construction of 0 take
    the engine defaults 16/200. Declarations are raft-replicated;
    re-declaring an existing name is a 409 error."""
    return _c().vector_index_create(name, prop, dim, metric, m, ef_construction)


@mcp.tool
def vector_index_drop(name: str) -> dict:
    """Drop a declared vector index by name. Declarations are
    raft-replicated; dropping an unknown name is a 404 error."""
    return _c().vector_index_drop(name)


@mcp.tool
def vector_index_list() -> dict:
    """List declared vector indexes (name, prop, dim, metric, m, efConstruction)."""
    return _c().vector_index_list()


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
