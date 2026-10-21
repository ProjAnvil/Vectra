#!/usr/bin/env node
/**
 * Vectra MCP server (TypeScript): wraps a running Vectra HTTP service.
 *
 * Target service: set VECTRA_URL (default http://127.0.0.1:8080).
 * Transport: stdio (local MCP hosts, e.g. Claude Desktop / Claude Code).
 *
 * The Go binary `vectra-mcp` embeds the engine directly; this is the
 * polyglot counterpart for JS/TS MCP hosts, talking to the REST API.
 */
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";

const BASE = (process.env.VECTRA_URL ?? "http://127.0.0.1:8080").replace(/\/$/, "");

async function api<T = unknown>(path: string, init?: RequestInit): Promise<T> {
  const resp = await fetch(`${BASE}${path}`, {
    ...init,
    headers: { "Content-Type": "application/json", ...(init?.headers ?? {}) },
  });
  const text = await resp.text();
  if (!resp.ok) {
    throw new Error(`${resp.status} ${path}: ${text}`);
  }
  return JSON.parse(text) as T;
}

interface NodeDTO {
  key: string;
  label?: string;
  props?: Record<string, unknown>;
}

function limit(keys: string[], limit: number) {
  return { count: Math.min(keys.length, limit), total: keys.length, nodes: keys.slice(0, limit) };
}

const server = new McpServer(
  { name: "vectra", version: "0.8.0" },
  {
    instructions:
      "Vectra dependency-graph engine via its HTTP API. Writes go through inject_batch " +
      "(raft-replicated); reads serve from the published snapshot. Tools: neighbors (1-hop), " +
      "bfs (multi-hop), shortest_path, reachable (blast radius), cycles, cypher (Cypher-subset " +
      "pattern queries), stats, vector_search (approximate top-k over a declared vector index).",
  },
);

const nodeSchema = z.object({
  key: z.string().describe("unique node key"),
  label: z.string().optional(),
  props: z.record(z.string(), z.unknown()).optional(),
});
const edgeSchema = z.object({
  srcKey: z.string(),
  dstKey: z.string(),
  label: z.string().optional(),
  props: z.record(z.string(), z.unknown()).optional(),
  allowParallel: z.boolean().optional().describe(
    "append a true parallel edge instead of collapsing onto the existing (srcKey,dstKey,label) upsert",
  ),
});

server.tool(
  "inject_batch",
  "Upsert nodes/edges (same keys update). Returns applied/skipped counts.",
  { nodes: z.array(nodeSchema).optional(), edges: z.array(edgeSchema).optional() },
  async ({ nodes, edges }) => {
    const res = await api("/v1/batch", {
      method: "POST",
      body: JSON.stringify({ nodes: nodes ?? [], edges: edges ?? [] }),
    });
    return { content: [{ type: "text", text: JSON.stringify(res) }] };
  },
);

server.tool("stats", "Graph overview: nodes/edges/sccs/cyclicNodes + leader/term.", {}, async () => {
  const res = await api("/v1/stats");
  return { content: [{ type: "text", text: JSON.stringify(res) }] };
});

server.tool("get_node", "Fetch a node by key (label and properties).", { key: z.string() }, async ({ key }) => {
  const res = await api<NodeDTO>(`/v1/nodes/${encodeURIComponent(key)}`);
  return { content: [{ type: "text", text: JSON.stringify(res) }] };
});

server.tool(
  "neighbors",
  "One-hop neighbors. direction: out|in; optional edge-label filter.",
  {
    key: z.string(),
    direction: z.enum(["out", "in"]).optional().default("out"),
    label: z.string().optional(),
    limit: z.number().int().positive().optional().default(1000),
  },
  async ({ key, direction, label, limit: lim }) => {
    const q = new URLSearchParams({ key, dir: direction });
    if (label) q.set("label", label);
    const res = await api<{ nodes: NodeDTO[] }>(`/v1/neighbors?${q}`);
    return { content: [{ type: "text", text: JSON.stringify(limit(res.nodes.map((n) => n.key), lim)) }] };
  },
);

server.tool(
  "bfs",
  "Multi-hop traversal (1..maxDepth hops; 0 = unlimited). Returns each node's depth. total reflects true size when truncated.",
  {
    from: z.array(z.string()).min(1).describe("one or more start keys"),
    maxDepth: z.number().int().nonnegative().optional().default(0),
    labels: z.array(z.string()).optional(),
    direction: z.enum(["out", "in"]).optional().default("out"),
    limit: z.number().int().positive().optional().default(1000),
  },
  async ({ from, maxDepth, labels, direction, limit: lim }) => {
    const body: Record<string, unknown> = { from, direction };
    if (maxDepth > 0) body.maxDepth = maxDepth;
    if (labels?.length) body.labels = labels;
    const res = await api<{ count: number; nodes: { key: string; depth: number }[] }>("/v1/query", {
      method: "POST",
      body: JSON.stringify(body),
    });
    const keys = res.nodes.map((n) => n.key);
    const depths = Object.fromEntries(res.nodes.slice(0, lim).map((n) => [n.key, n.depth]));
    return { content: [{ type: "text", text: JSON.stringify({ ...limit(keys, lim), depths }) }] };
  },
);

server.tool(
  "shortest_path",
  "Shortest path between two nodes (bidirectional BFS).",
  { from: z.string(), to: z.string() },
  async ({ from, to }) => {
    const q = new URLSearchParams({ from, to });
    const res = await api(`/v1/path?${q}`);
    return { content: [{ type: "text", text: JSON.stringify(res) }] };
  },
);

server.tool(
  "reachable",
  "All nodes reachable from a start (dependency blast radius).",
  { from: z.string(), limit: z.number().int().positive().optional().default(1000) },
  async ({ from, limit: lim }) => {
    const q = new URLSearchParams({ from });
    const res = await api<{ nodes: string[] }>(`/v1/reachable?${q}`);
    return { content: [{ type: "text", text: JSON.stringify(limit(res.nodes, lim)) }] };
  },
);

server.tool("cycles", "All dependency cycles (SCC member lists, largest first).", {}, async () => {
  const res = await api("/v1/cycles");
  return { content: [{ type: "text", text: JSON.stringify(res) }] };
});

server.tool(
  "cypher",
  "Query the graph with a Cypher-subset statement: MATCH (a:Label {prop:value})-[e:REL*1..3]->(b) " +
    "WHERE b.w > 3 RETURN a.key, b ORDER BY b.w DESC LIMIT 10. Node-pair semantics for *hops; " +
    "pseudo-props a.key/a.label/a.id; edge vars bind on fixed single hops only.",
  { query: z.string(), limit: z.number().int().positive().optional().default(1000) },
  async ({ query, limit: lim }) => {
    const res = await api<{ columns: string[]; rows: Record<string, unknown>[] }>("/v1/cypher", {
      method: "POST",
      body: JSON.stringify({ query }),
    });
    return {
      content: [{
        type: "text",
        text: JSON.stringify({ columns: res.columns, count: Math.min(res.rows.length, lim), total: res.rows.length, rows: res.rows.slice(0, lim) }),
      }],
    };
  },
);

server.tool(
  "vector_search",
  "Approximate top-k nearest-neighbor search over a declared vector index. Results are approximate; " +
    "order across equal scores is not stable. Provide exactly one of query_vector or similar_to. " +
    "Params: index, query_vector|similar_to, k (default 10, max 1000), ef, label.",
  {
    index: z.string().describe("vector index name (declare one with vector_index_create)"),
    query_vector: z.array(z.number()).optional().describe("query embedding (exactly one of query_vector/similar_to)"),
    similar_to: z.string().optional().describe("node key whose indexed vector is the query (exactly one of query_vector/similar_to)"),
    k: z.number().int().optional().default(10).describe("neighbors to return, default 10, max 1000"),
    ef: z.number().int().optional().describe("HNSW search beam width; 0 = adaptive default"),
    label: z.string().optional().describe("restrict the search to nodes with this label"),
  },
  async ({ index, query_vector, similar_to, k, ef, label }) => {
    const body: Record<string, unknown> = { index, k };
    if (query_vector?.length) body.query_vector = query_vector;
    if (similar_to) body.similar_to = similar_to;
    if (ef) body.ef = ef;
    if (label) body.label = label;
    const res = await api<{ count: number; hits: { key: string; score: number }[] }>("/v1/vector/search", {
      method: "POST",
      body: JSON.stringify(body),
    });
    return { content: [{ type: "text", text: JSON.stringify(res) }] };
  },
);

server.tool(
  "vector_index_create",
  "Declare a vector index over a node property (HNSW). dim is 1..4096; metric is cosine|l2|dot " +
    "(default cosine); m/efConstruction of 0 take the engine defaults 16/200. Declarations are " +
    "raft-replicated; re-declaring an existing name is a 409 error.",
  {
    name: z.string().describe("index name"),
    prop: z.string().describe("node property holding the vectors"),
    dim: z.number().int().describe("vector dimensionality, 1..4096"),
    metric: z.enum(["cosine", "l2", "dot"]).optional().default("cosine"),
    m: z.number().int().optional().describe("HNSW connectivity; 0 = engine default 16"),
    efConstruction: z.number().int().optional().describe("construction beam width; 0 = engine default 200"),
  },
  async ({ name, prop, dim, metric, m, efConstruction }) => {
    const body: Record<string, unknown> = { name, prop, dim, metric };
    if (m) body.m = m;
    if (efConstruction) body.efConstruction = efConstruction;
    const res = await api("/v1/vector/indexes", { method: "POST", body: JSON.stringify(body) });
    return { content: [{ type: "text", text: JSON.stringify(res) }] };
  },
);

server.tool(
  "vector_index_drop",
  "Drop a declared vector index by name. Declarations are raft-replicated; dropping an unknown name is a 404 error.",
  { name: z.string().describe("index name") },
  async ({ name }) => {
    const res = await api(`/v1/vector/indexes/${encodeURIComponent(name)}`, { method: "DELETE" });
    return { content: [{ type: "text", text: JSON.stringify(res) }] };
  },
);

server.tool("vector_index_list", "List declared vector indexes (name, prop, dim, metric, m, efConstruction).", {}, async () => {
  const res = await api("/v1/vector/indexes");
  return { content: [{ type: "text", text: JSON.stringify(res) }] };
});

async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
