# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.11.2] - 2026-10-22

Checkpoint goes O(1): the retained-WAL incremental chain replaces the
full snapshot on every seal, wrapper-mode vector ingestion and Close
stop paying the O(N) snapshot tax, and Close no longer blocks on the
vector drain. Full data: `docs/benchmarks/2026-10-22-v0112-checkpoint.md`.

### Performance

- Interruptible vector drain at Close: `drainVectors` now checks quit
  at chunk boundaries, so shutdown waits at most one in-flight chunk
  (vector convergence stays lazy per spec D10 - a reopen resumes it in
  the background). Wrapper Close 39.2s -> 3.0s on the slow reference
  box [9c8c8fb].
- Cypher copy-on-write row bindings: `row.clone()` shares the parent's
  binding maps and copies exactly once, lazily, at the first write
  (anonymous expansion 243 -> 177 allocs/query; join-branch clones are
  free) [4c7ddd3].

### Added

- O(1) light checkpoints - the retained-WAL incremental chain. A
  checkpoint now rotates the WAL (`wal.Rotate`) and rewrites the
  manifest (v2 `RetainFrom` names the first retained segment) instead
  of writing a snapshot; recovery replays base + retained chain + live
  tail. Compaction (the full-snapshot tier) fires at 256MiB of
  retained bytes, bounding recovery replay volume. Wrapper
  default-config vector ingestion 10.2k/s -> **27.4k/s**, seal stall
  p50 119.8ms -> **10.8ms** (a WAL-segment rotation, not a snapshot),
  Close 7.4-9.8s -> **~1s**; gate-2b lines and the 100k restart
  parity (498ms load vs 7m39.1s rebuild, 922.9x) held.

### Changed

- Store snapshots stamp **v6** (version-only bump; layout identical)
  and the WAL manifest is v2. v0.11.1 and older binaries hard-fail on
  v6 bases - libraries checkpointed by v0.11.2 cannot be downgraded
  (same discipline as v5). v4/v5 libraries migrate in place on first
  open (the pre-migration watermark snapshot is retained as a
  `<watermark>-v<version>.bak` artifact); fresh libraries write a v6
  tripwire from byte one. Downgrade-before-migration is safe; after,
  not.

## [0.11.1] - 2026-10-22

### Performance

- Cypher view-path expansion is allocation-free: the last gap from the
  row-path zero-alloc work (2026-10-19) — the cypher `directions` zip —
  now reuses per-executor scratch via the same rowSink pattern (1-hop
  MATCH at fan 32: 545 → 244 allocs/query, -55%; the expansion accessor
  itself holds a steady-state zero-allocation pin). Two semantics-neutral
  companions landed with it: pseudo-property predicate comparison without
  boxing, and a pairwise fast path for row deduplication under 64 rows.
  Concurrent-cypher tail and M-w16 mixed numbers:
  docs/benchmarks/2026-10-22-cypher-zero-alloc.md.

## [0.11.0] - 2026-10-21

Vector Phase 2: vectors ride a binary WAL/store channel, the HNSW
forest persists across restarts, and the vector surface reaches REST
and MCP. 768-dim frames shrink to a third of their JSON size, vector
write throughput goes 26k/s -> 68.8k/s, and a 100k-vector restart
loads in 455ms where it used to rebuild for 7m19s. Reference docs:
`docs/benchmarks/2026-10-21-vector-phase2-p21.md`,
`docs/benchmarks/2026-10-21-vector-phase2-p22.md`,
`docs/benchmarks/2026-10-21-wrapper-checkpoint-cadence.md`.

### Added

- Binary vector channel: WAL frame type 4 (`VVB2` self-describing
  envelope) and store v5 snapshots (declarations-first, binary float
  section) - 768-dim frames shrink 9.29KB -> 3.07KB, snapshots
  74.1MB -> 24.4MB @8192 vectors; vector write throughput 26k/s ->
  68.8k/s, batch-boundary p50 21ms -> 7.0ms.
- Forest persistence (`<db>.vec`, watermark-gated): 100k-vector
  restart 7m19s rebuild -> 455ms load (964x); WAL tail replays onto
  the loaded forest; mismatched/corrupt caches rebuild transparently.
- Vector REST endpoints: POST /v1/vector/search (query_vector |
  similar_to, k≤1000), POST/GET /v1/vector/indexes, DELETE
  /v1/vector/indexes/{name}; raft-replicated explicit index ops;
  TS/Python MCP clients gain vector_search, vector_index_create/drop/
  list.
- `writer.Handle.DeclareVectorIndex` / `DropVectorIndex` (embedded
  wrapper declaration lane).
- `Graph.Clone()` mounts read-only vector snapshots (server clones
  serve vector queries).

### Changed

- Migration-on-open: legacy (v4) libraries checkpoint to v5
  immediately on first open; fresh libraries write a v5 tripwire from
  byte one. v0.10 binaries hard-fail on v5 snapshots - migrated
  libraries cannot be downgraded (release notes: finish migration
  before any downgrade attempt; a failed migration leaves the library
  v4-readable and retried on next open). The last pre-migration
  watermark snapshot is retained as a `<watermark>-v<version>.bak`
  rollback artifact (e.g. `v-2.db-v4.bak`; copied before the migration
  seal; recovery never reads it, later seals never delete it); the
  migration open transiently costs ~3x snapshot size on disk (old +
  new + copy).
- Default CheckpointBytes 4MiB -> 64MiB (measured cadence:
  `docs/benchmarks/2026-10-21-wrapper-checkpoint-cadence.md`).
- /v1/batch and /v1/txn now accept JSON numeric arrays as vector props
  (previously failed at raft encoding).
- Vector search k cap raised 100 -> 1000 (default 10 unchanged)
  across REST and MCP surfaces, per industry comparison.

### Unsupported

- Mixed-version raft clusters: v0.11 leaders produce v5 raft snapshots
  that v0.10 followers hard-fail to restore. Upgrade all nodes
  together (stop-the-world, or all followers before the leader).

[Unreleased]: https://github.com/projanvil/vectra/compare/v0.11.2...HEAD
[0.11.2]: https://github.com/projanvil/vectra/compare/v0.11.1...v0.11.2
[0.11.1]: https://github.com/projanvil/vectra/compare/v0.11.0...v0.11.1
[0.11.0]: https://github.com/projanvil/vectra/compare/v0.10.0...v0.11.0

## [0.10.0] - 2026-10-20

Vector Phase 1: the index becomes a first-class embedded subsystem.
Declarations persist (WAL + store v4), writes intake through the skip-
and-warn rules, queries run zero-lock over immutable snapshots, and
background folder lanes pack without ever blocking the writer. Gate 2b:
write throughput 23-26k/s against the 20k line, plain-write p50 within
101% of an unindexed graph, integrated recall@10 = 1.0000 at ef
32/64/128. Full data: `docs/benchmarks/2026-10-20-vector-phase1-gate2b.md`.

### Added

- `Graph.CreateVectorIndex` / `DropVectorIndex` / `VectorIndexes` /
  `QueryVector` (+ `QueryVectorOptions`, `VectorHit`,
  `Graph.VectorForNode`, `Options.OnVecSkip`): declaration API and
  zero-lock top-k with adaptive ef and the 3x-oversample label
  post-filter (spec §9.1/§13-1). Mid-batch declarations are rejected
  (their replay semantics would diverge); documentation on the methods.
- WAL `IndexDecl.VectorIndexes`: full-list declaration frames, drop =
  absence; `null` vs empty-array keeps keys-only frames byte-identical
  (old binaries replay them untouched).
- Store format v4: vector declaration section after indexKeys. v3/v2/v1
  files load unchanged; **a v4 file opened by a pre-v0.10 binary hard-
  fails on the version byte** - plan upgrades before downgrades.
- Folder lanes (spec D10): direct mode packs inside the fold loop;
  wrapper mode gets an independent vecSig on the merger goroutine that
  the write-stall path never observes. Draining is two-phase (short
  lock to capture, lock-free HNSW build, short lock to commit) - the
  writer's intake never waits on graph building (spec D5; the
  single-lock first cut measured 601/s, the two-phase cut 26k/s).
- `internal/vector`: per-goroutine search Reader (concurrent-search
  prerequisite), immutable Snapshot model (chunked overlay, chunked
  dead bitmap, key arrays off the read path), recovery drain with the
  overlay as the exact fallback.
- MCP tool #12 `vector_search` (query_vector or similar_to, k ≤ 100,
  approximation note on every response).

### Changed

- Recovery rebuilds vector forests bottom-up after Load/Open (spec
  §7.1); until fully packed, the overlay's linear scan serves queries
  exactly.

### Fixed

- None beyond the above; the audit trail (five code-reviewer audits,
  every must-fix landed with a regression test) is in the commit log.

[Unreleased]: https://github.com/projanvil/vectra/compare/v0.10.0...HEAD
[0.10.0]: https://github.com/projanvil/vectra/compare/v0.9.0...v0.10.0

## [0.9.0] - 2026-10-20

A vector-index and throughput release: the embedded pure-Go HNSW lands
as Phase 0 internals, its build path gets 7.8x faster after a
profile-driven optimization pass, and the traversal row path becomes
allocation-free. 100K x 768 single-threaded builds drop from 1h40m to
12m54.6s with recall@10 = 1.0000 at ef 32/64/128 and query p99 still
28-31% better than the pre-optimization baseline; the 1M verification
builds in 3h38m with gate-2a lines passing directly (recall 0.9960,
p99 7.78ms at the production ef=64). M-w16 mixed-read p99 42.27 ->
21.27/22.05ms, QPS +32-38%. Full data:
`docs/benchmarks/2026-10-19-vector-build-optimization.md` and
`docs/benchmarks/2026-10-19-row-path-zero-alloc.md`.

### Added

- `internal/vector`: embedded pure-Go HNSW (diversity-heuristic
  linking, epoch-stamped visited set), tiered segment State (overlay +
  size-tiered forest + dead set), and the JSON-safe `NormalizeVec`
  intake layer — the Phase 0 spike of the vector-search design. Not
  yet wired into the public API (Phase 1); zero new dependencies, no
  cgo.
- `engine.View.RowsInto`: buffer-reuse form of `View.Rows` — steady
  state allocates nothing, keeping concurrent BFS workers off the
  allocator. `View.Rows` keeps its fresh-allocation contract.
- Tier-merge fan-in raised from 2 to 4 (tier holds <=3 segments; the
  4th arrival merges all four — spec 4.2 amended to match its capacity
  column): write amplification 5.59x -> 2.97x at 100K, 8.19x -> 4.42x
  at 1M. Live-segment upper bound moves 10 -> 15.
- Build-rate gate driver `TestBuildThroughput` (`VECTRA_BUILD_BENCH=1`)
  and kernel micro-bench `BenchmarkDistance`.

### Changed

- Distance kernels unrolled with eight independent accumulators
  (768-dim cosine 577 -> 92 ns, 6.3x); L2 no longer pays the wasted
  dot-product loop. Build-path allocation churn cut 2.77GB -> 507MB
  per 10K build (hand-rolled heaps, per-HNSW scratch, no double
  normalize on the rebuild path).
- Traversal row path is allocation-free: `viewRow` carries
  per-goroutine scratch (parallel expansion forks one per worker) and
  the single-implementation `rower` interface becomes `*viewRow`.
- `pruneTo` sorts its <=33-element candidate lists with a typed
  insertion sort (primitive 4.5x; the reflect-swapper allocation is
  gone).

### Fixed

- The v0.8.0 documented residual "M-w16 read p99 ~44ms from allocator
  contention on the row path": 16 parallel BFS workers allocated 4-6
  slices per expanded node (`View.Rows` append churn plus the
  three-slice zip); with `RowsInto` and per-worker scratch the
  mixed-read p99 halves (42.27 -> 21.27/22.05ms across two samples)
  and QPS rises 32-38%.

[Unreleased]: https://github.com/projanvil/vectra/compare/v0.9.0...HEAD
[0.9.0]: https://github.com/projanvil/vectra/compare/v0.8.0...v0.9.0

## [0.8.0] - 2026-10-19

A performance-focused release: the read path no longer collapses under
sustained writes, point reads are ~20x faster, and GC pressure is halved.
Benchmarks against the 2026-10-16 baseline (M tier, 100K nodes / 1M edges,
8 workers, 200 x 50-edge write batches/s): mixed-read p50 109ms -> ~0.2ms,
p99 1.42s -> ~15ms; writes hold the offered rate with p99 ~5ms;
unbatched import 1.56M edges/s. Full data and methodology:
`docs/benchmarks/2026-10-16-perf-baseline.md` and
`docs/benchmarks/2026-10-17-perf-fixes-retest.md`.

### Added

- `delta.FoldLayers`: semantics-preserving layer-stack compaction primitive
  (winner records keep their IDs, tombstone sets union, differential-tested
  against a random oracle).
- Direct-mode layer-stack governor: `Graph.SetAutoCompact` /
  `Graph.SetAutoSealEdges`. Folds run on a lazily started background
  goroutine (LevelDB/RocksDB/Pebble model adapted to the single-writer
  core: pure computation plus an atomic hand-over, publication stays with
  the writer; no locks added to the core — A8 unchanged).
- Size-tiered fold scheduling: F/T/tail runs, half-size merge rule
  (amortized constant record copies), tail volume gate, layer-count cap,
  and volume-proportional seals at Batch boundaries.
- Per-generation view slots: the writer fills a generation's three-source
  View before publishing it, so readers of a fresh generation always hit.
- Incremental view prebuilding: pure appends extend the previous build
  state at O(new records); layers above 4096 edges (bulk-import windows)
  skip prebuilding — import-shaped workloads pay nothing.
- `engine.KillSet` and `engine.NewViewWithKills`: copy-on-write chunked
  kill-set bitmaps; per-publish view assembly aggregates only new kills.
- vectra-bench: mixed read/write workload with a paced single writer
  (`-writerate`/`-writebatch`), bfsall TEPS (GAP/Graph500 normalization),
  phase-boundary memory/GC sampling, snapshot-restore cold-start phase,
  and a `-profile` pprof attribution mode.

### Changed

- `NeighborsOf` dedups small rows by linear scan and allocates a local
  bitmap for hub-sized rows (was: one whole-graph `[]bool` per call).
- `ShortestPath` pools its parent arrays (`parent[v] = u+1`, zeroed
  acquire replaces the fill loop) and reuses frontier scratch buffers
  (CSR-hot-path allocations 44 -> 14 measured).
- BFS results materialize their depth array lazily on first
  `Depth`/`Reached` use; `Layers`/`Count` consumers allocate nothing
  ID-space-sized.
- Acceptance gate A2 recalibrated: the allocation fixes collapsed the
  idle p99 floor ~45x, mechanically breaking the <=2x ratio line; it now
  also accepts the recorded pre-fix idle floors.

### Performance

- Mixed read/write (M tier): read p50 109ms -> ~0.2ms (~530x), read p99
  1.42s -> ~15ms (~95x), read QPS 31 -> ~4800; write p99 68ms -> ~5ms.
- Point reads: `NeighborsOf` QPS 376K -> 7.8M (p99 250µs -> 4.3µs); at the
  L tier (1M/10M) p50 31.5µs -> ~0.5µs.
- Traversals: bfsall p99 13.2ms -> 6.6ms, bfs3 p99 3.64ms -> 1.94ms,
  reach p99 3.08ms -> 1.68ms (M tier).
- GC CPU during read workloads halved (10.5% -> 5.4% at M, 14.7% -> 6.5%
  at L); cumulative read-path allocations dropped from 1.24TB to a
  fraction over the same runs.
- Unbatched import: 1.56M edges/s (A4 floor 1.0M).

### Fixed

- Read collapse under sustained writes: per-publish view assembly used to
  re-aggregate the whole unmerged stack (and a single-slot cache let
  concurrent readers force full rebuilds on every publish); the
  layer-stack governor plus per-generation slots plus incremental
  prebuilding remove the collapse at any stack depth or volume.
- Writer throttling: folds, snapshot copies, and kill-set re-aggregation
  no longer run on the writer goroutine (write rate held at the offered
  ceiling; p99 60ms -> ~5ms).
- Known residuals (documented in the retest report, not fixed here):
  M-w16 read p99 ~44ms from allocator contention on the row path; mirror
  string-map constant in the write path (B3).

[0.8.0]: https://github.com/projanvil/vectra/compare/v0.7.1...v0.8.0

## [0.7.1] - 2026-10-16

Hotfix: review fixes (WAL rollover fsync, BFS empty-layer guard, bKilled
counting, MCP snapshot openErr) and WAL IndexDecl frames (frame type 3).

[0.7.1]: https://github.com/projanvil/vectra/compare/v0.7.0...v0.7.1

## [0.7.0] - 2026-10-15

Continuous writes: immutable micro-layer generations, three-source
traversal views, single-writer wrapper with write-stall, embedded WAL with
watermark checkpoints, Cypher aggregation.

[0.7.0]: https://github.com/projanvil/vectra/compare/v0.6.0...v0.7.0

## [0.6.0] - 2026-10-13

OLTP transactions: group commit, SI/SER transaction guards, RYW await.

[0.6.0]: https://github.com/projanvil/vectra/compare/v0.5.0...v0.6.0
