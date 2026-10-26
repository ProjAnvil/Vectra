# Vectra MCP

Vectra ships first-class [Model Context Protocol](https://modelcontextprotocol.io) support in three languages, so any MCP host (Claude Desktop, Claude Code, Cursor, custom agents) can query your dependency graph directly.

| Implementation | Location | Engine access | Transports | Best for |
|---|---|---|---|---|
| **Go** (official SDK) | [`mcp/`](.) — `vectra-mcp` binary | **Embedded** (single binary, no network) | stdio + Streamable HTTP | Local use; zero-dependency agents |
| **Python** (FastMCP) | [`python/`](python/) | Wraps Vectra HTTP API | stdio + Streamable HTTP | Python-first MCP hosts |
| **TypeScript** (official SDK) | [`ts/`](ts/) | Wraps Vectra HTTP API | stdio | JS/TS-first MCP hosts |

**Tool surface** (13 tools in the TS/Python proxies, 14 in the embedded Go server; read/write annotated, results capped by default to protect the context window):

| Tool | Kind | Description |
|---|---|---|
| `inject_batch` | write | Upsert nodes/edges (props supported); `allowParallel: true` on an edge appends a parallel edge instead of upsert-collapse |
| `seal` (Go only) | write | Merge micro-layers + checkpoint (auto-indexes injected property keys); maintenance — queries never require it |
| `stats` | read | nodes/edges/SCCs/cyclic-nodes; `sealed` = no unmerged micro-layers |
| `get_node` | read | Node by key |
| `neighbors` | read | One-hop (direction + label filter) |
| `bfs` | read | **Multi-hop** (1..maxDepth, 0 = unlimited), returns per-node depth |
| `shortest_path` | read | Bidirectional BFS path |
| `reachable` | read | Blast radius from a node |
| `cycles` | read | Dependency cycles, largest first, with per-cycle edges (src/dst/label) |
| `lookup_property` (Go only) | read | Property-index lookup (env/team/tier…) |
| `index_fulltext` (Go only) | write | Declare a full-text (substring) index over a string property key (`propKey`); idempotent, answers `lookup_contains` immediately |
| `lookup_contains` (Go only) | read | Case-insensitive substring lookup over an `index_fulltext`-declared key (`propKey` + `contains`) — unlike Cypher `CONTAINS`, which is case-sensitive |
| `cypher` | read | Cypher-subset pattern queries: `[OPTIONAL] MATCH` (repeatable) / `WITH [DISTINCT]` (projection, `ORDER BY`/`SKIP`/`LIMIT`, `WHERE` after) / `UNWIND ... AS x` / `RETURN` (`ORDER BY`/`SKIP`/`LIMIT`) |
| `vector_search` | read | **Approximate** top-k ANN over a declared vector index (`query_vector` or `similar_to`); results are approximate; k default 10, max 1000 |
| `vector_index_create` (TS/Python only) | write | Declare a vector index: `name`, `prop`, `dim`, `metric` (cosine/l2/dot), optional `m`/`efConstruction` |
| `vector_index_drop` (TS/Python only) | write | Drop a declared vector index by name |
| `vector_index_list` (TS/Python only) | read | List declared vector indexes |

The Go server embeds the engine, so vector index lifecycle goes through its Go API (`vectra.VectorIndexDecl` / `CreateVectorIndex`) rather than MCP tools; the proxy clients expose the lifecycle because the REST API does.

## Go (embedded engine)

```bash
cd mcp && go build -o vectra-mcp ./cmd/vectra-mcp
./vectra-mcp                       # stdio
./vectra-mcp -http :8080           # Streamable HTTP at /mcp
./vectra-mcp -snapshot deps.vg     # persist across restarts (snapshot mode)
./vectra-mcp -wal deps.wal         # durable WAL session (see below)
```

Claude Code:

```bash
claude mcp add vectra -- /path/to/vectra-mcp
```

Claude Desktop (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "vectra": { "command": "/path/to/vectra-mcp" }
  }
}
```

v0.7 continuous-write model: every applied write is **immediately visible** to all query tools — no seal required (the sealed gate is gone). `seal` is maintenance: it folds unmerged micro-layers into the sealed base, auto-indexes the property keys seen since the last seal, and checkpoints. The graph stays writable afterwards (writes open a new generation). In `-snapshot` mode a seal also re-saves the file; the store format persists index declarations — typed and, since store v7, full-text — so `lookup_property` and `lookup_contains` work immediately after a restart.

`-wal <dir>` switches to a durable session: every batch is WAL-logged (fsync per batch), `seal` writes the checkpoint (watermark snapshot + manifest + log reset), and a restart on the same directory recovers everything — checkpointed state plus the log tail. `-wal` takes precedence over `-snapshot` (a given `-snapshot` path then names the db file; omitted, it defaults to `<wal>/graph.db`).

**Parameter naming convention** (spelled out because agents guess on first call): a single node is `key`; traversal endpoints are `from`/`to` (`bfs` takes a list in `from`); property lookup uses the `propKey`/`propValue` pair (substring lookup: `propKey` + `contains`); shared controls are `direction` (`"out"` default | `"in"`), `label`/`labels`, `limit` (default 1000), `maxDepth` (0 = unlimited).

## Python (wraps the HTTP API)

Requires a running Vectra service (see `server/cmd/vectra-server` in the repo root):

```bash
cd server && go build -o vectra-server ./cmd/vectra-server
./vectra-server -id n1 -data /tmp/n1 -http :8080 -raft :9000
```

Then:

```bash
cd mcp/python
uv run vectra-mcp-python                    # stdio, VECTRA_URL=http://127.0.0.1:8080
uv run vectra-mcp-python --http :9000       # Streamable HTTP
```

When the service runs with `-auth-token-file`, also set `VECTRA_TOKEN` — the proxy sends it as the bearer token on every request (omit it when auth is off).

Claude Code:

```bash
claude mcp add vectra -- env VECTRA_URL=http://127.0.0.1:8080 uv run --directory /path/to/mcp/python vectra-mcp-python
```

## TypeScript (wraps the HTTP API)

```bash
cd mcp/ts && npm install && npm run build
VECTRA_URL=http://127.0.0.1:8080 node dist/index.js
```

`VECTRA_TOKEN` is the auth counterpart (bearer token on every request when the service runs `-auth-token-file`; omit when auth is off).

Claude Code:

```bash
claude mcp add vectra --env VECTRA_URL=http://127.0.0.1:8080 -- node /path/to/mcp/ts/dist/index.js
```

## Design notes

- The Go server embeds the engine directly (the embedded story); Python/TS servers proxy the HTTP API — they work against a single node or a whole raft cluster (writes are forwarded via leader hints automatically).
- Tool surface follows database-MCP best practice: small (13 tools in the proxies, 14 in Go), read/write annotated, results capped (`limit`, default 1000; vector search `k`, default 10 max 1000) with `total` always reflecting the true size.
- Protocol specs supported: whatever the underlying SDK negotiates (Go SDK v1.8 covers up to 2026-07-28).
