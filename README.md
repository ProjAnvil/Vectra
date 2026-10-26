# Vectra

**English** | [简体中文](README.zh-CN.md)

An embedded, in-memory graph engine for Go, built for **enterprise dependency graphs** — service call graphs, data lineage, permission inheritance (100K–10M edges). At this scale, the machinery big graph databases carry (buffer managers, disk page caches, WAL transactions) is pure overhead. Vectra bets on the opposite: the graph always fits in memory, reads are lock-free, and the whole graph is one atomic file.

Since v0.2 the same engine upgrades into an optional **multi-replica infrastructure service**: a separate `server` module wraps it with hashicorp/raft (leader election, write replication) plus an HTTP API — the rqlite pattern applied to graphs. And it speaks **MCP** in Go, Python and TypeScript, so AI agents can query your dependency graph directly.

## Why Vectra

| Bet | Payoff |
|---|---|
| Whole graph in memory, CSR with zero pointers (`[]uint32` arrays) | GC cost independent of edge count; lock-free concurrent reads — every generation, sealed or not |
| Layer-synchronized parallel BFS (morsel-driven, atomic-OR bitmap dedup) | 3.3× over naive map adjacency at 10M edges |
| SCC compression at write time (Tarjan) — queries face a DAG | Reachability with one bit per component; `Cycles()` as a free diagnostic |
| Single-file snapshot with CRC32 (v3 format) | Atomic publish/backup; blue-green hot swap in server mode; property-index declarations round-trip |
| Immutable micro-layer generations (v0.7) | Continuous writes with zero-lock reads; every applied write immediately visible |

## Quickstart (embedded)

```go
import "github.com/projanvil/vectra"

g := vectra.New()
g.AddNode("gateway", "Service", map[string]any{"env": "prod"})
g.AddEdge("gateway", "auth", "CALLS", nil)
g.UpsertEdge("gateway", "auth", "CALLS", map[string]any{"weight": 2}) // replace props on the same (src,dst,label)
g.DeleteNode("ledger")                                               // cascade-deletes incident edges

// bulk import: g.Batch(func(g *vectra.Graph) { ... }) — one micro-layer per batch; plain
// unbatched writes are absorbed into one open window too (materialized at the next query,
// boundary, or capacity cap), so bulk import no longer requires Batch — but Batch is still
// the fastest shape under WAL (one frame group per boundary instead of a frame per write)

// chained traversal (works with or without a Seal — writes are immediately visible)
res, _ := g.Query().From("gateway").Out("CALLS").MaxDepth(3).Run()

// reachability (SCC-compressed fast path)
reach, _ := g.Reachable("gateway")

// bidirectional shortest path
path, found, _ := g.ShortestPath("gateway", "kafka")

// dependency-cycle discovery (nodes + intra-cycle edges with label/props)
for _, c := range g.Cycles() { ... }

// atomic single-file snapshot
g.SaveFile("deps.vg")
```

Snapshot notes: the v3 format persists `IndexProperty` declarations, so `vectra.Load` restores both the graph and its lookup indexes (typed lookups work immediately after reload; v2/v1 files remain readable but carry no index declarations). Property indexes are **typed** — per key three sorted buckets (string / number / bool): `LookupByProperty` (string eq), `LookupByNumber` / `LookupByNumberRange(min, max *float64)` (numeric eq / range), `LookupByBool`. Numbers normalize to `float64` in the index, so an `int` written via the Go API and the same value read back from a JSON snapshot land in one bucket — the old "integers read back as float64" comparison pitfall disappears for indexed lookups (`int64` beyond 2^53 loses precision, inherent to the JSON number model).

## Continuous writes: micro-layer generations (v0.7)

v0.7 dissolves the sealed-snapshot gate. Every write (or `Batch`) materializes its changes as an immutable **micro-layer** and publishes a new generation `{base, layers}` through one atomic pointer — readers never lock and never block, whatever the writer is doing. A background merger folds `base ∪ layers` into a fresh immutable base (background merge triggers when accumulated layer edge-records reach max(64K, base edge count) — the fold interval stays proportional to the base, 64K being the floor for small graphs; a 2048-layer cap stalls the writer as a backstop until the fold lands), and `Seal()`/`Compact()` run that fold explicitly, plus a checkpoint when a WAL is attached. Sealing no longer freezes anything: writes after `Seal` simply open a new generation, and `Compact` is semantically identical to `Seal` (spec §3.1's "deep compaction renumbering" is absorbed by the merge itself). Both are write-path methods.

**Read visibility by family:**

| Query family | Visibility |
|---|---|
| Point lookups (`NodeByKey` / `NodeByID` / `EdgeByID` / counts / `EdgesOf` / `LookupBy*`) | immediate — three-source read (base ∪ layers, tombstone masking) |
| Traversals (`BFS` / `ShortestPath` / `Cypher` / `Query` / `NeighborsOf`) | immediate — merged row streams over base + layers |
| `Reachable` / SCC-dependent stats | counts immediate; supernode (SCC-compressed) acceleration applies to the merged base, delta-period queries fall back to node-level BFS |
| `Cycles` | folds unmerged layers first — strongly consistent answers, the one query that walks the write path |

**IDs:** node and edge IDs are stable within a generation (deleted IDs burn their numbers); merges renumber densely, so IDs may change across a `Seal`/`Compact`/background fold — the v0.6 cross-merge contract, unchanged.

**Writer discipline:** the zero-lock engine assumes one writer goroutine. In direct mode (calling `AddNode`/`Batch`/... yourself) that is any single goroutine of yours — concurrent bare writes are a data race and interleave WAL frames. Unbatched per-op writes accumulate in an **absorption window** (one micro-layer per flush — the next query, a boundary, or a capacity cap — instead of one layer per op), so bulk import no longer forces `Batch`; `Batch` (or wrapper changesets) remains the fastest shape under WAL because it logs one frame group per boundary instead of a frame per write. The structured form is the single-writer wrapper (`internal/writer.Attach` → `Handle.Submit`): bounded queue, Badger-style group commit (one frame group and one fsync per drained batch), background merges with write-stall at the layer cap, and a volume-triggered checkpoint once the WAL holds ≥64 MiB since the last one. While attached, non-release builds reject bare writes — and off-writer `Seal`/`Compact`/`Cycles` — with `ErrNotWriter`; release builds compile the check away and rely on this contract.

**Write-ahead log (optional):** `Open(path, Options{WALMode, WALDir})` starts a durable session — whole-JSON changeset frames, fsynced per `Batch` boundary in `ModeAlways` (`ModeInterval` fsyncs at explicit boundaries; `ModeOff`, the zero value, is exactly v0.6 snapshot behavior). Checkpoints — on `Seal`/`Compact`/`Close`, or the wrapper's ≥64 MiB volume trigger — write a watermark snapshot `<db>-<lsn>.db`, flip an atomic manifest onto it, and reset the log; recovery loads the watermark and replays the frames above it (a torn tail stops at the first bad frame; a checkpoint failure keeps the WAL for the next attempt). On disk this adds a `wal` sidecar directory next to the snapshot file: older binaries ignore it harmlessly, and a v0.7 binary facing a directory without one just loads the snapshot — no format change to the v3 snapshot itself.

**Memory:** a merge allocates the new base before the old generation becomes garbage, so peak RSS at the 1M/10M tier approaches 2× the ~4.1 GB live set (9–11 GB momentary; spec §9 R11). Budget ≥16 GB for merge-heavy 1M-tier workloads, or enable the wrapper's `GCTune` (drops GOGC to 50 around merges).

**⚠️ Upgrading to 0.5.0.** Store snapshots are now written in the v3 format. The incompatibility is one-directional: v1/v2 files remain readable, but pre-0.5 binaries cannot read v3 files — embedded and MCP users who exchange `.vg` snapshots across versions need both sides on 0.5+. The raft cluster mixed-version policy is unchanged (already unsupported).

## Vector search (v0.9, Phase 1)

Declare an index over a node property and query it with approximate top-k
(embedded HNSW, pure Go, zero new dependencies — full design in
[docs/superpowers/specs/2026-10-19-vector-search-design.md](docs/superpowers/specs/2026-10-19-vector-search-design.md)):

```go
g.CreateVectorIndex(vectra.VectorIndexDecl{Name: "mem", Prop: "embedding", Dim: 768, Metric: "cosine"})
g.AddNode("thought-1", "Memory", map[string]any{"embedding": someVec}) // []float32 prop
hits, _ := g.QueryVector("mem", queryVec, vectra.QueryVectorOptions{K: 10, Label: "Memory"})
```

Committed vectors are immediately queryable (the overlay's exact scan);
background packing builds the segment forest without blocking writes
(writer intake never waits on graph building). Results are approximate
and unordered across ties. Declarations persist through the WAL and
store v4; recovery rebuilds bottom-up with the overlay as the exact
fallback. The MCP server exposes it as `vector_search` (query_vector or
similar_to, k ≤ 1000). Gate 2b numbers:
[docs/benchmarks/2026-10-20-vector-phase1-gate2b.md](docs/benchmarks/2026-10-20-vector-phase1-gate2b.md).

## Full-text property search (v0.12)

Typed indexes answer equality and range; a full-text declaration answers **substring** lookups over a string prop — mixed CJK/Latin included:

```go
g.IndexFullText("desc")                                    // declare (pending until the next Seal/merge)
hits, ok := g.LookupByContains("desc", "payment gateway") // case-insensitive substring, NodeID ascending
```

Semantics: `LookupByContains` is **case-insensitive** (value and needle both fold to lower case) and works rune-pair-wise — at `Seal` time each declared key's values are tokenized into adjacent two-rune **bigrams** ("图数据库" → 图数 数据 据库; a value of m runes posts m-1 deduped grams), and a lookup intersects the needle's gram posting lists, then re-verifies every candidate with a real `Contains` — the bigram index is a prefilter only, so gram collisions never surface as false hits. Declarations follow the `IndexProperty` contract: pending until the next merge (pending keys answer via full scan until then), idempotent, WAL-logged, and persisted in store v7 snapshots; a key may be typed-indexed **and** full-text-indexed. Undeclared keys and empty needles answer `(nil, false)`; needles shorter than two runes take the documented full-scan path. Cross-reference: Cypher's `CONTAINS` is unchanged — a case-sensitive scan with no relation to these buckets.

**Cost (opt-in).** Buckets exist only for declared keys, and every Seal/merge/clone rebuilds them. Budget ~24-30 B per (gram, id) posting: 1M nodes × 100-character values ≈ 3-5 GB. The release gate (`BenchmarkSealFTS100K`: 100K nodes / 1M edges, one key, ~32-rune mixed CJK/Latin values) measures the with-FTS clone+seal cycle at 1.69x the no-FTS line (+162 MB/op, 0.34 s/op). MCP exposes it as `index_fulltext` / `lookup_contains` (Go server).

## Cypher-subset queries

Sealed graphs answer openCypher/GQL-style pattern queries — the DSL compiles onto the same CSR traversal primitives (`bfs` / `neighbors` / `lookup_property` are the physical operators):

```go
res, _ := g.Cypher(`MATCH (b {type:'controller'})-[:CALLS*1..2]->(s)
                    WHERE s.env = 'prod' AND s.w >= 3
                    RETURN b.key, s ORDER BY s.w DESC LIMIT 10`)
// res.Columns = ["b.key", "s"]; res.Rows = [{"b.key": "gw", "s": {key,label,props,...}}, ...]
```

Grammar (v0.13 clause pipeline): clauses chain in any order — `[OPTIONAL] MATCH pattern[, pattern...] [WHERE expr]` (repeatable), `WITH [DISTINCT] items [ORDER BY item [ASC|DESC]...] [SKIP n] [LIMIT n] [WHERE expr]`, `UNWIND list AS alias` — ending in `RETURN *|items [ORDER BY item [ASC|DESC]...] [SKIP n] [LIMIT n]`; node `(var:Label {prop:literal})`, edge `-[var:REL|REL*n..m]->` / `<-[...]-` / `--` (undirected); WHERE supports `= <> < <= > >=`, `STARTS/ENDS WITH`, `CONTAINS`, `IN [...]`, `AND/OR/NOT`, the pseudo-props `var.key / var.label / var.id`, and edge props on fixed single hops (`WHERE e.weight > 3` — parallel edges each bind a row).

Semantics (deliberate, documented): variable-length edges follow **node-pair semantics** — a target matches when its *shortest distance* lies within the hop window (`[:CALLS*1..2]` = 1..2 hops by shortest path; cycles do not create longer matches), intermediate nodes are not filtered by the target pattern, and variable-length edges bind no edge variable. Rows dedup on named bindings + final position; WHERE uses null-propagating logic (a missing property never satisfies, and `NOT` does not rescue it). Anchoring uses the typed property indexes when the pattern allows (`{type:'controller'}`), scanning otherwise. Intermediate rows are capped at 1,000,000 (error asks for a narrower pattern or `LIMIT`).

Clause pipeline and scope: clauses transform one row set stage by stage. Repeated `MATCH` (new in v0.13) cartesian-products disjoint variables and joins shared ones; stages are strict — a stage whose input is zero rows yields zero rows. `OPTIONAL MATCH` keeps an incoming row whose whole clause (comma patterns plus its WHERE) matches nothing, the clause's pattern variables reading null. `WITH` starts a new scope: only projected names survive — bare variables pass their entity binding through (own name or alias), while `var.prop` and aggregation items require `AS` and project scalars — and its optional trailing `WHERE` filters the projected rows. `UNWIND` keeps the scope and declares its alias as a scalar bound to each list element.

Divergences & limits (deliberate): `UNWIND` fan-out and every intermediate stage are capped by the 1,000,000-row budget; `OPTIONAL MATCH` does not dedup across base rows (duplicate inputs stay duplicated); keywords may serve as aliases (`WITH a AS match`); a pattern variable first declared under `OPTIONAL MATCH` is nullable — using it as a pattern variable in a later non-optional `MATCH` is a validation error suggesting the restructures (fold the pattern into the `OPTIONAL MATCH`, or mark the later `MATCH` optional; `IS [NOT] NULL` is not in the grammar); `RETURN *` remains limited to the degenerate single-`MATCH` query; `WITH *`, `range()`, `RETURN DISTINCT`, and general expression items are deferred (the aggregations `count/sum/avg/min/max/collect`, including `DISTINCT` inside count/collect, work in RETURN and WITH items).

Same surface elsewhere: server `POST /v1/cypher` (`{"query": "..."}` → `{columns, rows, count}`; parse errors 400 `PARSE`), MCP tool `cypher` in all three servers (default limit 1000, `total` reports the true size).

## Server mode (optional module)

All embedded graph products (Kuzu, CozoDB, SQLite) ship without replication; every Raft-backed product is a server. Vectra follows the rqlite precedent — replication wraps the engine, it is not baked in:

```
write: client →POST /v1/batch→ leader ─raft→ majority commit → deterministic FSM replay
publisher (each node): RLock clone → Seal outside the lock → atomic.Pointer swap
queries: GET /v1/* and POST /v1/query|/v1/cypher → local published snapshot (lock-free, never interrupted)
```

```bash
cd server && go build -o vectra-server ./cmd/vectra-server
./vectra-server -id n1 -data /data/n1 -http :8080 -raft :9000          # seed node
./vectra-server -id n2 -data /data/n2 -join http://n1:8080              # join
```

Semantics: writes are linearizable (raft commit) — upserts plus logical-key deletes (node deletes cascade incident edges); reads are bounded-stale (≤ SealInterval, immediate for small graphs). Edge identity in batch and transaction writes is the `(srcKey, dstKey, label)` triple — upserts collapse parallels and the last write wins on props; set `"allowParallel": true` on an edge item to append a true parallel edge instead. Anti-split-brain: nodes with `Join` set never bootstrap. The 3-node in-process chaos suite (leader failover, log replay catch-up, zero query failures during republish) runs under `-race` in CI.

**Bearer-token auth (v0.12, optional).** `-auth-token-file <file>` enables static bearer auth: the file holds one token per line — multiple lines are all accepted, which is the rotation story (append the new token to every node, roll restarts, then remove the old one). Blank lines and CRLF endings are tolerated; a line with internal whitespace, an unreadable file, or an empty file aborts startup — auth never degrades into a silently open database. Every route then requires `Authorization: Bearer <token>` (SHA-256 digests, constant-time comparison) except exactly `GET /v1/health`, which stays open for the load-balancer probe. A joining node's `-join` requests carry its own token — one trust domain, every node shares the same token file. **Boundary:** HTTP auth protects HTTP only; the raft channel between nodes stays plaintext and unauthenticated, so bind `-raft` to a private interface and keep cluster traffic off untrusted networks (and terminate TLS in front of the HTTP port if tokens must not cross it in the clear — the server has no built-in TLS).

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

**Retry etiquette.** On 409: full-jitter exponential backoff `sleep = rand(0, min(500ms, 5ms·2^attempt))` (cf. [AWS Exponential Backoff and Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/)), re-read to refresh the token, resubmit. Read-your-writes (v0.5): `GET /v1/await?seq=<index>&timeoutMs=5000` (`timeoutMs` defaults to 5s, capped at 30s) — a server-side long poll that returns `{publishedSeq, reached}` plus the `X-Vectra-Snapshot` header as soon as the local publish covers the commit; re-issue on `reached:false`. Polling `GET /v1/stats` remains a pre-0.5-compatible fallback. S-tier / http / 16 workers / Apple M5 Pro: txnsi ~542 → ~846 commits/s (+56%), txnser ~556 → ~822 (+48%), RYW wait 11.1ms → ~3.1ms, p50 26.5ms → ~18.3ms (docs/benchmarks/2026-10-14-ryw-await.md).

**Overhead.** v0.4.0 acceptance is fsm-level (`BenchmarkFSMAutoCommit` / `TxnSI` / `TxnSER16` in `server/txn_bench_test.go` — direct `fsm.Apply`, `Snapshot = ^uint64(0)` so validation never conflicts): AutoCommit **34.9µs** · TxnSI **28.6µs** · TxnSER16 **38.6µs** (AutoCommit/SER16: 17-node batches, TxnSI: 1-node batch; Apple M5 Pro) — SI/SER within ~25% of auto-commit at a 16-key read set (SER16 ≈ +11%). HTTP-level `txnsi`/`txnser` workloads ship in vectra-bench (`-mode http -workloads txnsi,txnser`): S-tier scales near-linearly (51 → 542 commits/s at 1 → 16 workers, SER conflicts ≤ 4.3%); RYW-respecting throughput on larger graphs is bounded by the publish period (`SealInterval` + clone/Seal) — see `docs/benchmarks/2026-10-13-oltp-txn.md` for the size/interval/worker matrix.

**⚠️ Upgrading to 0.4.0.** Raft snapshots now carry a VCTX trailer (the version table). The incompatibility is bidirectional: old binaries cannot read 0.4.0 snapshots, and 0.4.0 rejects pre-0.4.0 snapshots ("legacy snapshot without VCTX rejected") — mixed-version clusters are unsupported. A node with a pre-0.4.0 raft snapshot on disk refuses to start with that error (raft restore of the on-disk snapshot fails during startup), so there is no in-place conversion from legacy snapshot state. Recovery is re-seed: stop all nodes, remove the raft snapshot state (or the whole data dir) per ops policy, restart and re-ingest — fresh VCTX snapshots are then taken automatically by raft's snapshot policy (SnapshotThreshold / SnapshotInterval).

## MCP (agents)

Three first-class implementations — [Go (embedded engine)](mcp), [Python (FastMCP)](mcp/python), [TypeScript](mcp/ts) — exposing one-hop / multi-hop / shortest-path / reachability / cycles (with intra-cycle edges) / Cypher-subset pattern queries as MCP tools, plus `inject_batch` for writes (`allowParallel` supported; `lookup_property` takes string/number/bool values). In the Go server every applied write is immediately queryable — no seal round-trip; `seal` is optional maintenance (merge + checkpoint), and `-wal <dir>` runs a durable session that recovers across restarts:

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

v0.7 acceptance report — micro-layer generations + WAL against a v0.6.0 baseline (traversal regression, WAL-off import line, merge-in-flight query tail, crash recovery): [docs/benchmarks/2026-10-15-delta-wal.md](docs/benchmarks/2026-10-15-delta-wal.md) (Apple M5 Pro).

Reproduce locally: `make bench [SIZE=medium] [MODE=http]` — data and report are deleted when the run ends.

Correctness is validated the LDBC way: property tests against independent reference implementations, real-dataset ground truth (SNAP email-Eu-core: 203 SCCs / largest 803, externally published numbers), chaos and degenerate-graph suites, determinism checks. Coverage: root 92.3% · csr/dag 100% · store 84.0% · engine+traverse 98.8% · server 87.5% · mcp 85.6%.

## Repository layout

```
vectra.go            facade: writes, Batch, Seal/Compact, snapshot, typed property index
changeset.go         Changeset vocabulary (WAL frames, server raft batches)
options.go           Open options: WALMode / WALDir
merge.go             background-merge protocol: CaptureMerge / PublishMerge / LayerPressure
view.go              three-source row streams (base CSR + micro-layer records)
cypher.go            facade: Cypher-subset query entry (Graph.Cypher)
internal/csr         CSR adjacency (pointer-free uint32 arrays)
internal/dag         Tarjan SCC, condensation, topological levels
internal/engine      sealed snapshot + accessors + Seal pipeline
internal/delta       immutable micro-layer builder and layer records
internal/wal         embedded logical WAL (JSON frames, watermark checkpoints)
internal/writer      single-writer wrapper: queue, group commit, background merger
internal/cypher      Cypher-subset lexer, parser, AST, row executor
internal/traverse    algorithms: layer-synced BFS, reachability, bidirectional path, cycles
internal/store       snapshot codec (v3: indexKey section + CRC32 trailer, v1/v2 read-compatible)
server/              separate module: raft replication + HTTP API + hot-swap publisher
                     ├─ cmd/vectra-server   service binary
                     ├─ cmd/vectra-bench    stress-test harness
mcp/                 separate module: MCP servers
                     ├─ cmd/vectra-mcp      Go (embedded engine, stdio + HTTP, snapshot or WAL session)
                     ├─ python/             FastMCP (wraps HTTP API)
                     └─ ts/                 TypeScript (wraps HTTP API)
testdata/            real dataset fixture (SNAP email-Eu-core, cited)
docs/benchmarks/     stress-test design + results
```

The core module has **zero third-party dependencies** (`go.sum` absent). The `server` module adds hashicorp/raft v1.8.0 + raft-boltdb/v2; the `mcp` module adds the official MCP Go SDK. Choose what you deploy.

## Benchmark & gate commands

Standard gate: `go test . -count=1 && go test ./bench/ -count=1 && go vet .` (the `bench/` acceptance runs are env-gated — e.g. `VECTRA_RESTART=1` — and skip otherwise). The race gate is per package and **pins an explicit timeout**: `internal/vector` measures ~1100s under `-race` (content clean), past Go's 600s default, so the bare `go test -race` form times out spuriously. Use one shape everywhere:

```
go test -race ./internal/vector/ -count=1 -timeout 40m
go test -race ./internal/writer/ -count=1 -timeout 40m   # same shape; finishes well under the cap
go test -race .                -count=1 -timeout 40m     # same shape; finishes well under the cap
```

## Not in scope (deliberately)

Online writes crossing into the server module — raft replication still runs the clone/publish model, because merges renumber IDs and replica replay must stay deterministic (the v0.7 micro-layer engine is embedded-only); disk spill beyond 10M edges; a full GQL parser in core; sharding. The old "online writes / delta-CSR" exclusion is resolved: v0.7 ships them as micro-layer generations without giving up either core bet (in-memory CSR, lock-free reads). The roadmap holds: k-hop precomputation.

## License

[MIT](LICENSE)
