# Vector Phase 1: MVP Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the Phase-0 vector index into Vectra: declarations persisted (WAL + store v4), write-path intake, zero-lock reads, background packing lane, Go API + MCP tool — passing gate 2b.

**Architecture:** Vector state is published as immutable snapshots behind one atomic head per index (per-Graph registry), NOT inherited through the 7 genView construction points — vector state is orthogonal to layer folds, and the 7-point inheritance the spec warns about ("miss one, lose vectors") carries pure risk with no semantic gain. Writers own a mutable builder; every publish (micro-layer rate) and every folder pack swaps in a fresh snapshot. Reads (QueryVector) load the head, never lock.

**Tech Stack:** Go (root module), zero new dependencies, no cgo. MCP Go module for the tool.

**Spec:** docs/superpowers/specs/2026-10-19-vector-search-design.md (v3 + amendments). The spec travels with this plan; executors read both.

## Global Constraints

- Zero third-party dependencies (`go.mod` unchanged); no cgo (spec D2).
- Writer critical path performs no graph operations: normalize-check + JSON encode + WAL append + overlay chunk append only (spec §5.3).
- NaN/Inf vectors must be rejected before `json.Marshal` sees them (spec §5.1 step 2; `NormalizeVec` already rejects).
- Committed vectors are queryable immediately: overlay linear scan (spec D7).
- Wrong-dim / non-numeric vectors: index skips the node, counts a warning, never errors or blocks (spec §13-2).
- Concurrent reads are zero-lock: only immutable snapshots + atomics (spec D1). All read-path state is reader-local after Task 1.
- Same-seed determinism in tests; recall assertions are lower bounds vs brute force, never exact outputs (spec §4.5).
- Store v4 opened by a v3 binary must hard-fail (version dispatch default — already automatic via `store.Decode`'s unsupported-version path; verify, don't reimplement).
- Deviation log: every deviation from the spec gets an entry in the spec's 实现期偏差记录 section, same session it happens.

## Review Focus

1. **Concurrent QueryVector during writes/packs races on shared state** — after Task 1/2, `go test -race` with parallel readers + writer + folder must be clean; the race test is the gate.
2. **Deleted/revived keys leaking old vectors** — delete → search must not return the key; revive → search returns the new vector (Task 6 tests).
3. **WAL-replay divergence** — the same op sequence applied live vs replayed must yield identical index state (Task 6 differential test, per the FoldLayers oracle precedent).
4. **Store v3 files still load (back-compat), v4 rejected by v3 binaries** — Task 4 tests both directions.
5. **Overlay unbounded growth when the folder never runs** (no background lane in tests that use AddNode only) — Search must stay correct with overlay ≫ 2048; the drain API must make progress 2048 at a time (Task 8 tests).

---

### Task 1: Reader-local search state (concurrency prerequisite)

**Files:**
- Modify: `internal/vector/hnsw.go` (HNSW.Search, searchLayer, greedyStep), `internal/vector/state.go` (State.Search)
- Test: `internal/vector/reader_test.go` (new)

**Interfaces:**
- Produces:
  ```go
  type Reader struct { /* visited []uint32; stamp uint32; cands, results []cand — unexported fields, opaque */ }
  func (s *State) NewReader() *Reader
  func (s *State) SearchReader(q []float32, k, ef int, r *Reader) []VectorHit
  // State.Search(q, k, ef) stays: convenience wrapper using a fresh Reader (allocates; correct for non-hot callers).
  ```
- HNSW internal: `searchLayer`/`greedyStep`/`Search` take the reader's buffers; the `h.visited`/`h.stamp`/`h.slCands`/`h.slResults` fields are REMOVED (single-writer build scratch `snVecs/snUsed/prCands` stays on HNSW — build path is folder-only, single-threaded per index).

- [ ] **Step 1: Write the failing race test** — build a 2000×64 State, 8 goroutines × SearchReader concurrently (readers-only race: concurrent read-read is this task's contract; read-during-write needs Task 2's snapshot model and is tested there). Plus a correctness test: SearchReader results equal Search results for the same state/queries.
- [ ] **Step 2: Run it to verify it fails** — `go test -race ./internal/vector/ -run TestStateConcurrentSearch` — expect DATA RACE or FAIL (current Search mutates h.visited).
- [ ] **Step 3: Implement** — move visited/heap buffers into Reader; searchLayer grows `r.visited` to the segment's node count on demand; stamp wraps at 2^32 with full reset (same protocol as today). Keep TestHNSWDeterminism green (same-seed builds unchanged).
- [ ] **Step 4: Full package + race green** — `go test -race ./internal/vector/ -count=1`.
- [ ] **Step 5: Commit** — `perf... no — refactor(vector): reader-local search state` with the race evidence.

### Task 2: Snapshot model — chunked overlay, chunked dead bitmap, key arrays off the read maps

**Files:**
- Modify: `internal/vector/state.go` (major rework), `internal/vector/hnsw.go` (segment key arrays live in state.go side)
- Test: `internal/vector/snapshot_test.go` (new)

**Interfaces:**
- Produces:
  ```go
  type Snapshot struct { /* immutable: metric; segments []segSnap; overlay [][]entry; dead bitmap; dim; total */ }
  type segSnap struct { h *HNSW; keys []string /* seg-local vid -> key, parallel to h.ids */ }
  // entry gains a key field: type entry struct{ key string; vid uint32; seq uint64; vec []float32 }
  func (s *State) Snapshot() *Snapshot          // O(chunks + dirty-dead-chunks), called per publish
  func (s *State) SearchReader(q []float32, k, ef int, r *Reader) []VectorHit  // now reads a Snapshot internally
  func (s *State) OverlayLen() int
  ```
- Internals: overlay becomes `[][]entry` chunked at 2048 (writer appends to the tail chunk; Snapshot copies the chunk-pointer slice + tail chunk). dead set becomes a chunked bitmap (`[1024]uint64` per 65536 vids, map[uint32]*[1024]uint64, COW chunk on mutation — mirror engine.KillSet's shape but vid-keyed, D6). `keyOf`/`vidOf` maps stay writer-side only; read-path rendering uses segSnap.keys + entry.key.

- [ ] **Step 1: Failing tests** — (a) snapshot immutability: take Snapshot, keep querying it while mutating the State (Add/Delete), old snapshot's results unchanged; (b) dead-bitmap semantics equal the old map (delete/revive parity test vs a reference map implementation, randomized 200 ops); (c) SearchReader renders correct keys (it did via keyOf before — now via key arrays).
- [ ] **Step 2: Verify failures**, implement, `go test -race ./internal/vector/` green (Task-1 race test now exercises snapshots).
- [ ] **Step 3: Retention guard** — TestStateNoPhantomRetention must still pass ≤1.6x (chunked structures retain whole chunks; verify no regression beyond noise, adjust if the chunk tail pins vectors — document if the line moves).
- [ ] **Step 4: Commit** — `refactor(vector): immutable snapshot model - chunked overlay/dead, key arrays off read maps`.

### Task 3: WAL IndexDecl extension

**Files:**
- Modify: `internal/wal/wal.go` (IndexDecl, Frame, replayLSN), `changeset.go` (root re-export if needed)
- Test: `internal/wal/wal_test.go` (extend)

**Interfaces:**
- Produces:
  ```go
  type VectorIndexDecl struct { Name, Prop string; Dim int; Metric string; M, EfConstruction int } // in wal (JSON tags lower-camel)
  // IndexDecl gains: VectorIndexes []VectorIndexDecl
  // Frame gains: VectorIndexes []VectorIndexDecl (frameIndexDecl payload, same frame type 3)
  ```

- [ ] **Step 1: Failing test** — AppendIndex with VectorIndexes → Replay yields identical decls (JSON round-trip; empty decl list stays byte-identical to old format for forward compat).
- [ ] **Step 2: Verify fail, implement, green, commit** — `feat(wal): IndexDecl carries vector index declarations`.

### Task 4: Store v4

**Files:**
- Modify: `internal/store/store.go` (versionV4, Data.VectorDecls, Encode/Decode)
- Test: `internal/store/store_test.go` (extend)

**Interfaces:**
- Produces: `Data.VectorDecls []VectorIndexDecl` (store-local mirror struct with JSON tags matching wal's). Encode writes versionV4 + trailing decl section (u32 count + JSON strings, after IndexKeys, before CRC). Decode: v4 reads it; v3 → empty; v3-binary-on-v4 file must hit the existing `unsupported version` hard fail (test that).

- [ ] **Step 1: Failing tests** — v4 round-trip (decls + []float32 props through the JSON props channel), v3 file loads with empty decls, v4-by-v3 rejects.
- [ ] **Step 2: Verify fail, implement, green, commit** — `feat(store): format v4 - vector index declarations`.

### Task 5: Graph API — CreateVectorIndex / DropVectorIndex / VectorIndexes + skip warnings

**Files:**
- Modify: `vectra.go` (API + per-Graph registry), `options.go` (Options.OnVecSkip hook)
- Test: `vectra_vector_test.go` (new)

**Interfaces:**
- Produces:
  ```go
  type VectorIndexDecl struct { Name, Prop string; Dim int; Metric string; M, EfConstruction int } // root type; wal/store convert
  func (g *Graph) CreateVectorIndex(d VectorIndexDecl) error  // validates (dim 1..4096, metric set, name unique, non-empty); WAL-logs log-first (flushPending coalesces opVecIndex pendOps into AppendIndex); builds writer + empty snapshot
  func (g *Graph) DropVectorIndex(name string) error          // WAL-logs (IndexDecl.VectorIndexes re-declares the full set at op position — replay is last-wins per name; tombstone = absence), tears down writer + head
  func (g *Graph) VectorIndexes() []VectorIndexDecl
  // Options.OnVecSkip func(key, index string) — nil-safe; called on NormalizeVec failure at intake (spec §13-2 warning)
  ```
- Registry: `g.vec atomic.Pointer[vecRegistry]`, `vecRegistry map[string]*vecIndex`; each `vecIndex{decl; w *vector.State; head atomic.Pointer[vector.Snapshot]}`. Wrapper-mode safe (mutations happen on the writer goroutine or under pubMu-equivalent sequencing; reads only touch head).

- [ ] **Step 1: Failing tests** — create/drop/list round-trip; duplicate name errors; invalid dim/metric/name errors; decl reaches WAL (Open-replay restores the registry — covered fully in Task 9, here assert the AppendIndex frame content via Replay).
- [ ] **Step 2: Verify fail, implement, green** — direct-mode path first.
- [ ] **Step 3: Deviation-log entry** — spec §6.1 mounts vec per genView (7 inheritance points); this plan mounts per-Graph atomic heads: vector state is orthogonal to layer folds, inheritance carried only risk. Add the entry to the spec's 偏差记录 in the same commit.
- [ ] **Step 4: Commit** — `feat(vectra): vector index declarations - API, registry, WAL logging`.

### Task 6: Write-path intake + WAL replay parity

**Files:**
- Modify: `vectra.go` (AddNode/DeleteNode hooks, applyChangeset replay hook)
- Test: extend `vectra_vector_test.go`

**Interfaces:**
- Consumes: Task 5 registry; `vector.State.Add/Delete/OverlayLen`; `NormalizeVec`.
- Produces: shared intake function used by BOTH live writes and replay:
  ```go
  func (g *Graph) vecIntakeNodes(nodes []wal.NodeUpsert)   // per decl: NormalizeVec(props[decl.Prop]) -> Add; prop absent/invalid where indexed before -> Delete
  func (g *Graph) vecIntakeDeletes(keys []string)          // DeleteNode -> State.Delete per index
  // Called from: AddNode/DeleteNode after recordNode (writer side, post-build), and applyChangeset (replay side).
  // Snapshot publication: after intake at publishLayer time, head.Store(w.Snapshot()) — overlay chunk append cost only.
  ```

- [ ] **Step 1: Failing tests** — (a) AddNode with valid vector → QueryVector-ready (OverlayLen 1, key visible via State path); wrong-dim → skipped + OnVecSkip fired; (b) delete → State.Delete; overwrite prop to non-vector → Delete; revive → new vector wins; (c) **replay parity**: random 300-op sequence (adds/deletes/overwrites/decls) applied live vs replayed from WAL — identical Snapshot search results for 20 queries (differential test, the FoldLayers-oracle precedent).
- [ ] **Step 2: Verify fail, implement, green, `-race` on the parity test.**
- [ ] **Step 3: Commit** — `feat(vectra): vector write-path intake with WAL replay parity`.

### Task 7: QueryVector — Go read API

**Files:**
- Modify: `vectra.go`, `internal/vector` (nothing — consumes SearchReader/AdaptiveEf)
- Test: extend `vectra_vector_test.go`; new `vectra_vector_concurrent_test.go`

**Interfaces:**
- Produces:
  ```go
  type QueryVectorOptions struct { K int; Ef int; Label string }
  type VectorHit struct { Key string; Score float64 } // root-level type — internal/vector cannot appear in the public API
  func (g *Graph) QueryVector(index string, vec []float32, o QueryVectorOptions) ([]VectorHit, error)
  // errors: unknown index; dim mismatch. K defaults 10. Ef 0 -> vector.AdaptiveEf(k, n, DefaultC). Label -> post-filter with 3x oversample (spec §13-1): fetch max(3k, k) candidates, filter by node label via the view (NodeByKey path), truncate k.
  ```
- Path: direct mode `flushPend()` at entry (read-your-write, same as lookupNodes); wrapper mode reads last published (same contract as node queries). Reader via a `sync.Pool` on the Graph (zero-alloc steady state is NOT required in Phase 1; pool is a cheap win, keep simple).

- [ ] **Step 1: Failing tests** — recall lower bound on a 5k×64 clustered build (≥0.95 @k=10 with brute truth, perturbed queries); label filter correctness (filtered result excludes non-matching labels, count preserved); dim mismatch + unknown index errors; read-your-write (Add → QueryVector sees it, direct mode); concurrent test: 8 readers QueryVector while writer Adds — `-race`.
- [ ] **Step 2: Verify fail, implement, green, commit** — `feat(vectra): QueryVector - zero-lock top-k with label post-filter`.

### Task 8: Vector folder lane — packing, drain, recovery rebuild

**Files:**
- Modify: `internal/vector/state.go` (drain API), `vectra.go` (direct-mode lane), `internal/writer/writer.go` + `merger.go` (wrapper lane)
- Test: `internal/vector` drain tests; `vectra_vector_test.go` lane tests; `internal/writer` lane test

**Interfaces:**
- vector produces: `func (s *State) PackNext() bool` — packs at most one 2048-chunk into the L0 tier (cascade + compact as today), returns false when overlay < 2048 and nothing left; callers loop until false.
- vectra.go (direct mode): foldLoop gains a vector stage — after each wake (and once at Load/Open for recovery), for each index drain `PackNext` while it returns true, then `head.Store(w.Snapshot())`. Overlay pressure ≥ 2048 arms the fold wake (afterPublish). Folder time share: vector drain bounded per wake (pack at most N=8 chunks per index per wake — keeps A6-style time share; document).
- writer (wrapper mode): independent `vecSig chan struct{}` (buffered 1) + `vecFlightMu/vecFlight` pair mirroring mergeSig/flight (D10: the stall path `signalMerge` never touches it); merger loop select gains the vec case → drain under the same backoff ladder; failure = backoff only, no writer fallback (spec §5.2-3).
- Recovery (spec §7.1): after Load/Open replay completes, all vectors sit in the overlay; kick the lane (non-blocking signal). Until packed, SearchReader's overlay scan IS the brute-force path — no separate mode needed (overlay linear scan is exact); "底层先建先可用" falls out naturally because PackNext builds L0s first and SearchReader always searches live segments + overlay.

- [ ] **Step 1: Failing tests** — (a) drain: 10k adds → loop PackNext → overlay empty, tier0 holds 4 segments + cascade, SearchReader recall ≥0.95 vs brute; (b) direct lane: SetAutoCompact graph, 6k adds, wait for convergence (poll OverlayLen==0 with timeout), QueryVector recall bound; (c) wrapper lane: writer-mode graph, same convergence poll; vecFlight independent — inject FailMerge-style failure? Keep: assert signalMerge stall path doesn't wait on vecFlight (inspect via a slow-vector test: pack is goroutine-side, writer writes keep flowing).
- [ ] **Step 2: Verify fail, implement, green, `-race` on lane tests, commit** — `feat(vectra,writer): vector folder lane - pack drain, recovery rebuild, D10 independence`.

### Task 9: Persistence — Seal/checkpoint/Load recovery end-to-end

**Files:**
- Modify: `vectra.go` (Save adds decls, loadGraph/Open replay consumes VectorIndexes + rebuilds states from props)
- Test: extend `vectra_vector_test.go` + wal_integration_test.go pattern

**Interfaces:**
- Consumes: Tasks 3/4/6. Save: `store.Data.VectorDecls` from the registry. Load: build registry from decls, walk all base nodes' props through vecIntakeNodes, replay WAL frames (Changesets already route via applyChangeset → Task-6 hooks; IndexDecl.VectorIndexes → registry upsert/drop), then kick the recovery drain (Task 8). Close: drain-on-close is NOT required (WAL is the truth source; deviation note if Seal skips packing — spec §7.1 rebuilds anyway).

- [ ] **Step 1: Failing tests** — (a) crash-style: build 5k vectors + decls, Close, Open — registry present, QueryVector works (overlay brute path immediately), convergence poll reaches packed state, recall bound holds; (b) store v3 file (generated by the v3 test fixture) with vector props loads with zero indexes; (c) seal → snapshot file contains decls (decode directly).
- [ ] **Step 2: Verify fail, implement, green, commit** — `feat(vectra): vector persistence - store v4 declarations, WAL replay, recovery rebuild`.

### Task 10: MCP vector_search tool (Go)

**Files:**
- Modify: `mcp/server.go` (tool #12 + Instructions), `mcp/server_test.go` if tool tests exist (follow neighbors)
- Test: `mcp/vector_test.go` (new)

**Interfaces:**
- Tool: `vector_search` — in: `{index string, query_vector []float64 | similar_to string, k int (default 10, max 100; raised to 1000 on 2026-10-21 per industry comparison)}`; out: `{count, results: [{key, score}]}`; description states results are approximate (D8). Handler: reads via svc.view()-equivalent (svc.live), `similar_to` reads the node's indexed prop then queries.

- [ ] **Step 1: Failing test** — inject graph with vector index + 500 vectors, tool returns top-k with scores; k clamp 100; similar_to path; both-param/none-param errors.
- [ ] **Step 2: Implement, green, Instructions updated (12 tools), commit** — `feat(mcp): vector_search tool`.
- [ ] **Step 3: Deviation note** — TS/Python MCP clients proxy the server/ REST API (already diverged from the Go tool list); vector_search lands Go-only, TS/Python follow with the server REST surface (spec §14 marks server/ as evaluation). Entry in the spec's deviation log.

### Task 11: Gate 2b benchmarks + report + docs

**Files:**
- Create: `docs/benchmarks/2026-10-20-vector-phase1-gate2b.md`; Modify: `README.md` (vector search section, brief)
- Test: `bench/vector_gate2b_test.go` (env-gated, the acceptance-test pattern)

**Interfaces:** Gate lines (spec §10.3): (a) vector write throughput ≥ 20k/s on the writer path (768-dim, WAL on, direct mode — measured via a gated bench test); (b) write-path p99 not degraded (existing mixed-write p99 with vector load ≤ the 5ms throttle line); (c) integrated recall/p99 within 20% of gate-2a results (100k×768 in-distribution, perturbed queries, k=10, adaptive ef — compare vs `2026-10-19-vector-build-optimization.md` final numbers). Plus evaluation notes: overlay COW cost per publish (measured), GC share, server/ raft channel size note (analysis only).

- [ ] **Step 1: Write the gated bench test** (VECTRA_GATE2B=1), run all three lines, write the report with raw tables.
- [ ] **Step 2: If any line fails** — stop, diagnose per systematic-debugging, fix or report the gap honestly (spec: gate 2b fail → back to Phase 0).
- [ ] **Step 3: README section + commit** — `bench(vector): gate 2b - integrated write throughput, recall/p99 parity`.

### Final: whole-branch review + release

- [ ] Fresh code-reviewer subagent over the whole diff (main..HEAD): architecture, concurrency, spec compliance, deviation-log completeness.
- [ ] Address findings; full suite + `-race` on root/internal/mcp; `make vet`.
- [ ] Release: version call per content landed (features → minor v0.10.0 unless everything shipped is fix-shaped); CHANGELOG entry; bump mcp Go/TS/Python strings; annotated tag; `gh release create` with gate-2b numbers.
