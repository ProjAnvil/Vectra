# Vectra MCP

Vectra ships first-class [Model Context Protocol](https://modelcontextprotocol.io) support in three languages, so any MCP host (Claude Desktop, Claude Code, Cursor, custom agents) can query your dependency graph directly.

| Implementation | Location | Engine access | Transports | Best for |
|---|---|---|---|---|
| **Go** (official SDK) | [`mcp/`](.) — `vectra-mcp` binary | **Embedded** (single binary, no network) | stdio + Streamable HTTP | Local use; zero-dependency agents |
| **Python** (FastMCP) | [`python/`](python/) | Wraps Vectra HTTP API | stdio + Streamable HTTP | Python-first MCP hosts |
| **TypeScript** (official SDK) | [`ts/`](ts/) | Wraps Vectra HTTP API | stdio | JS/TS-first MCP hosts |

**Tool surface** (identical across all three, 8–10 tools, read/write annotated, results capped by default to protect the context window):

| Tool | Kind | Description |
|---|---|---|
| `inject_batch` | write | Upsert nodes/edges (props supported) |
| `seal` (Go only) | write | Build query indexes; graph becomes queryable |
| `stats` | read | nodes/edges/SCCs/cyclic-nodes |
| `get_node` | read | Node by key |
| `neighbors` | read | One-hop (direction + label filter) |
| `bfs` | read | **Multi-hop** (1..maxDepth, 0 = unlimited), returns per-node depth |
| `shortest_path` | read | Bidirectional BFS path |
| `reachable` | read | Blast radius from a node |
| `cycles` | read | Dependency cycles, largest first |
| `lookup_property` (Go only) | read | Property-index lookup (env/team/tier…) |

## Go (embedded engine)

```bash
cd mcp && go build -o vectra-mcp ./cmd/vectra-mcp
./vectra-mcp                       # stdio
./vectra-mcp -http :8080           # Streamable HTTP at /mcp
./vectra-mcp -snapshot deps.vg     # persist across restarts
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

Writes accumulate; call `seal` once after `inject_batch` to make the graph queryable (the tool error message reminds the agent if it forgets). Property keys seen during injection are auto-indexed at seal time.

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

Claude Code:

```bash
claude mcp add vectra -- env VECTRA_URL=http://127.0.0.1:8080 uv run --directory /path/to/mcp/python vectra-mcp-python
```

## TypeScript (wraps the HTTP API)

```bash
cd mcp/ts && npm install && npm run build
VECTRA_URL=http://127.0.0.1:8080 node dist/index.js
```

Claude Code:

```bash
claude mcp add vectra --env VECTRA_URL=http://127.0.0.1:8080 -- node /path/to/mcp/ts/dist/index.js
```

## Design notes

- The Go server embeds the engine directly (the embedded story); Python/TS servers proxy the HTTP API — they work against a single node or a whole raft cluster (writes are forwarded via leader hints automatically).
- Tool surface follows database-MCP best practice: small (10 tools), read/write annotated, results capped (`limit`, default 1000) with `total` always reflecting the true size.
- Protocol specs supported: whatever the underlying SDK negotiates (Go SDK v1.8 covers up to 2026-07-28).
