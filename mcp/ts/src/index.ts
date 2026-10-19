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
      "pattern queries), stats.",
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

async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
