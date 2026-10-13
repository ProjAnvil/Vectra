# Vectra

**English** | [简体中文](README.zh-CN.md)

An embedded, in-memory graph engine for Go, built for **enterprise dependency graphs** — service call graphs, data lineage, permission inheritance (100K–10M edges). At this scale, the machinery big graph databases carry (buffer managers, disk page caches, WAL transactions) is pure overhead. Vectra bets on the opposite: the graph always fits in memory, reads are lock-free, and the whole graph is one atomic file.

Since v0.2 the same engine upgrades into an optional **multi-replica infrastructure service**: a separate `server` module wraps it with hashicorp/raft (leader election, write replication) plus an HTTP API — the rqlite pattern applied to graphs. And it speaks **MCP** in Go, Python and TypeScript, so AI agents can query your dependency graph directly.

## Why Vectra

| Bet | Payoff |
|---|---|
| Whole graph in memory, CSR with zero pointers (`[]uint32` arrays) | GC cost independent of edge count; lock-free concurrent reads after `Seal()` |
| Layer-synchronized parallel BFS (morsel-driven, atomic-OR bitmap dedup) | 3.3× over naive map adjacency at 10M edges |
| SCC compression at write time (Tarjan) — queries face a DAG | Reachability with one bit per component; `Cycles()` as a free diagnostic |
| Single-file snapshot with CRC32 (v2 format) | Atomic publish/backup; blue-green hot swap in server mode |

## Quickstart (embedded)

```go
import "github.com/projanvil/vectra"

g := vectra.New()
g.AddNode("gateway", "Service", map[string]any{"env": "prod"})
g.AddEdge("gateway", "auth", "CALLS", nil)
g.UpsertEdge("gateway", "auth", "CALLS", map[string]any{"weight": 2}) // replace props on the same (src,dst,label)
g.DeleteNode("ledger")                                               // cascade-deletes incident edges
g.Seal() // immutable from here; concurrent reads are lock-free

// chained traversal
res, _ := g.Query().From("gateway").Out("CALLS").MaxDepth(3).Run()

// reachability (SCC-compressed fast path)
reach, _ := g.Reachable("gateway")

// bidirectional shortest path
path, found, _ := g.ShortestPath("gateway", "kafka")

// dependency-cycle discovery
for _, c := range g.Cycles() { ... }

// atomic single-file snapshot
g.SaveFile("deps.vg")
```

## Server mode (optional module)

All embedded graph products (Kuzu, CozoDB, SQLite) ship without replication; every Raft-backed product is a server. Vectra follows the rqlite precedent — replication wraps the engine, it is not baked in:

```
write: client →POST /v1/batch→ leader ─raft→ majority commit → deterministic FSM replay
publisher (each node): RLock clone → Seal outside the lock → atomic.Pointer swap
queries: GET /v1/* → local published snapshot (lock-free, never interrupted)
```

```bash
cd server && go build -o vectra-server ./cmd/vectra-server
./vectra-server -id n1 -data /data/n1 -http :8080 -raft :9000          # seed node
./vectra-server -id n2 -data /data/n2 -join http://n1:8080              # join
```

Semantics: writes are linearizable (raft commit) — upserts plus logical-key deletes (node deletes cascade incident edges); reads are bounded-stale (≤ SealInterval, immediate for small graphs). Anti-split-brain: nodes with `Join` set never bootstrap. The 3-node in-process chaos suite (leader failover, log replay catch-up, zero query failures during republish) runs under `-race` in CI.

## Transactions

Since v0.4 the server supports **interactive optimistic transactions** on the same raft log — OCC against a per-key version table (each key's last-modified raft index). No locks are held across the conversation; validation is deterministic table lookups inside `fsm.Apply`, so every replica reaches the same commit/conflict decision:

```
read:    any query endpoint → take the X-Vectra-Snapshot response header (publishedSeq) as the token
write:   POST /v1/txn {nodes, edges, edgeDeletes, nodeDeletes,
                       txn: {snapshot: <token>, ser: false, readNodes: [...], readEdges: [...]}}
success: 200 {committed: true, index, nodesApplied, edgesApplied, ...}
conflict: 409 {committed: false, conflicts: [...], conflictTotal, hint}
```

**Isolation.** Default is snapshot isolation (SI): write-set validation, first-committer-wins — any written key modified after the token rejects the whole batch, so blind writes participate too (stronger than badger/FDB defaults, where blind writes sneak through). `"ser": true` adds read-set validation: keys recorded in `readNodes`/`readEdges` that changed after the token also reject (phantom / write-skew protection). Edge writes bump both endpoint versions; node deletes cascade to peer adjacency — all observable through validation. A `txn` field is required (400 `VALIDATION` without it — use `/v1/batch` for auto-commit); readNodes and readEdges are capped at 100,000 keys (400 `TXN_READSET_TOO_LARGE`).

**Read-set coverage** — what to record for `ser: true`:

| Endpoint | Record |
|---|---|
| `GET /v1/nodes/{key}` | `{key}` |
| `GET /v1/neighbors` | `{key}` + all returned neighbor keys |
| `POST /v1/query` | start keys + all reached keys |
| `GET /v1/reachable` | `from` + all returned keys |
| `/v1/path`, `/v1/stats`, `/v1/cycles` | SI-only — full-graph / frontier-dependent reads cannot be SER-validated |

**Retry etiquette.** On 409: full-jitter exponential backoff `sleep = rand(0, min(500ms, 5ms·2^attempt))` (cf. [AWS Exponential Backoff and Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/)), re-read to refresh the token, resubmit. Read-your-writes: poll `GET /v1/stats` until `publishedSeq >= index` from the commit response.

**Overhead.** v0.4.0 acceptance is fsm-level (`BenchmarkFSMAutoCommit` / `TxnSI` / `TxnSER16` in `server/txn_bench_test.go` — direct `fsm.Apply`, `Snapshot = ^uint64(0)` so validation never conflicts): AutoCommit **34.9µs** · TxnSI **28.6µs** · TxnSER16 **38.6µs** (AutoCommit/SER16: 17-node batches, TxnSI: 1-node batch; Apple M5 Pro) — SI/SER within ~25% of auto-commit at a 16-key read set (SER16 ≈ +11%). HTTP-level txnsi/txnser workloads in vectra-bench are future work.

**⚠️ Upgrading to 0.4.0.** Raft snapshots now carry a VCTX trailer (the version table). The incompatibility is bidirectional: old binaries cannot read 0.4.0 snapshots, and 0.4.0 rejects pre-0.4.0 snapshots ("legacy snapshot without VCTX rejected") — mixed-version clusters are unsupported. A node with a pre-0.4.0 raft snapshot on disk refuses to start with that error (raft restore of the on-disk snapshot fails during startup), so there is no in-place conversion from legacy snapshot state. Recovery is re-seed: stop all nodes, remove the raft snapshot state (or the whole data dir) per ops policy, restart and re-ingest — fresh VCTX snapshots are then taken automatically by raft's snapshot policy (SnapshotThreshold / SnapshotInterval).

## MCP (agents)

Three first-class implementations — [Go (embedded engine)](mcp), [Python (FastMCP)](mcp/python), [TypeScript](mcp/ts) — exposing one-hop / multi-hop / shortest-path / reachability / cycles as MCP tools:

```bash
claude mcp add vectra -- /path/to/vectra-mcp
```

See [mcp/README.md](mcp/README.md) for all transports and host configurations.

## Benchmarks (M1 Pro, power-law graphs, full reports in [docs/benchmarks](docs/benchmarks))

| Workload | 1K/10K | 100K/1M | 1M/10M (design ceiling) |
|---|---|---|---|
| one-hop p50 | 0.5µs | 4.4µs | 36µs |
| 3-hop p50 | 7.7µs | 763µs | 7.1ms |
| whole-graph BFS p50 | 16µs | 3.8ms | 70ms |
| shortest path p50 | 3.2µs | 101µs | 1.1ms |
| injection (engine / raft+HTTP) | — | 9.5M / 1.4M edges/s | 9.4M / 1.3M edges/s |

Reproduce locally: `make bench [SIZE=medium] [MODE=http]` — data and report are deleted when the run ends.

Correctness is validated the LDBC way: property tests against independent reference implementations, real-dataset ground truth (SNAP email-Eu-core: 203 SCCs / largest 803, externally published numbers), chaos and degenerate-graph suites, determinism checks. Coverage: root 92.2% · csr/dag 100% · store 84.3% · engine+traverse 98.7% · server 86.9% · mcp 81.5%.

## Repository layout

```
vectra.go            facade: writes, Seal, snapshot, property index
internal/csr         CSR adjacency (pointer-free uint32 arrays)
internal/dag         Tarjan SCC, condensation, topological levels
internal/engine      sealed snapshot + accessors + Seal pipeline
internal/traverse    algorithms: layer-synced BFS, reachability, bidirectional path, cycles
internal/store       snapshot codec (v2: CRC32 trailer, v1 read-compatible)
server/              separate module: raft replication + HTTP API + hot-swap publisher
                     ├─ cmd/vectra-server   service binary
                     ├─ cmd/vectra-bench    stress-test harness
mcp/                 separate module: MCP servers
                     ├─ cmd/vectra-mcp      Go (embedded engine, stdio + HTTP)
                     ├─ python/             FastMCP (wraps HTTP API)
                     └─ ts/                 TypeScript (wraps HTTP API)
testdata/            real dataset fixture (SNAP email-Eu-core, cited)
docs/benchmarks/     stress-test design + results
```

The core module has **zero third-party dependencies** (`go.sum` absent). The `server` module adds hashicorp/raft v1.8.0 + raft-boltdb/v2; the `mcp` module adds the official MCP Go SDK. Choose what you deploy.

## Not in scope (deliberately)

Online writes / delta-CSR (they would forfeit the two core bets), disk spill beyond 10M edges, a full GQL parser in core, sharding. The roadmap holds: k-hop precomputation, a minimal query DSL, property-typed indexes.

## License

[MIT](LICENSE)
