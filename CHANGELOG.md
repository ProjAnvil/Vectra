# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.12.0] - 2026-10-26

Auth and full-text search: the HTTP surface gains optional bearer-token
auth, and the engine gains opt-in full-text (substring) property indexes -
case-insensitive bigram buckets built at Seal time, persisted through WAL
frames and store v7. Release gate (`BenchmarkSealFTS100K`, 100K nodes/1M
edges, one key, ~32-rune mixed CJK/Latin values): the with-FTS clone+seal
cycle at 1.685x the without-FTS line, +162 MB/op, 0.339 s/op absolute
(lines: <=2x, <=250 MB, <=1.5 s).

### Added

- HTTP bearer-token auth (`-auth-token-file`): tokens load once at
  startup, one per line, multiple lines all accepted (rotation = append
  the new token to every node, roll restarts, remove the old); blank
  lines and CRLF tolerated, internal whitespace / unreadable / empty
  file aborts startup (fail-fast - auth never degrades into a silently
  open database). Every route requires `Authorization: Bearer <token>`
  (SHA-256 digests, constant-time compare, no early exit) except exactly
  `GET /v1/health`, the load-balancer probe. Join requests carry the
  joining node's token - one trust domain, every node shares the token
  file. The raft channel stays plaintext: HTTP auth never protects
  inter-node raft TCP; bind `-raft` to a private interface.
- Full-text property search: `Graph.IndexFullText(key)` declares a
  string property key for substring lookup, `Graph.LookupByContains(key,
  substr)` answers case-insensitively from seal-built bigram gram
  buckets (a value of m runes posts m-1 deduped two-rune grams - mixed
  CJK/Latin works naturally; every hit re-verifies with a real Contains,
  so the prefilter never returns false positives). The declaration
  contract mirrors IndexProperty (pending until the next merge, pending
  keys answer via scan; idempotent; WAL-logged) and persists in store v7
  snapshots; a key may be both typed- and full-text-indexed. Undeclared
  keys and empty needles answer (nil, false); Cypher `CONTAINS` is
  unchanged (case-sensitive scan). Memory: ~24-30 B per (gram, id) - 1M
  nodes x 100-char values lands around 3-5 GB; strictly opt-in.
- MCP: `index_fulltext` / `lookup_contains` tools (Go server, 14 tools)
  and `VECTRA_TOKEN` env in the Python/TS proxies (sent as the bearer
  token when the server runs auth).
- `BenchmarkSealFTS100K` (bench/clone_test.go): the FTS seal release
  gate - identical 100K-node datasets with and without one FTS
  declaration; the delta isolates the seal-time gram-bucket build.

### Changed

- Store snapshots stamp **v7** (the FTSKey section appended after the
  float32 section; a v6 file is v7 minus that section). Pre-v7 readers -
  every v0.11.x binary - hard-fail on the version byte: libraries
  checkpointed by v0.12.0 cannot be downgraded past v0.11.x (same
  discipline as v5/v6). v6-and-older files load unchanged; legacy v1-v5
  bases keep migrating in place on first open (`.bak` rollback artifact
  retained), and a v6 base upgrades to v7 at its next compact - light
  checkpoints over a v6 base stay legal (tier-gate fix below).

### Fixed

- The light-checkpoint legality probe compared the base's store version
  exactly (`== v6`), so a v7 base would have been misjudged as needing
  the full-snapshot compact tier; the probe now accepts v6-or-newer
  (`>=`), keeping light checkpoints O(1) across store-version bumps
  [d49d11e].

### Performance

- The FTS seal build is allocation-lean: interned gram strings, flat
  extraction with presized postings, counting-sort ordering - the seal
  gate moved from 5.12x / +653 MB (first round, failed) to 1.685x /
  +162 MB / 0.339 s (pass) [8c415ec].

## [0.11.4] - 2026-10-26

The 1M arc: direct measurement, three structural fixes, and the retest
that verifies them at scale. Ingest accept 3.6x, compact-tier stalls
-98%, and the worst-case restart (2h57m30s) eliminated. Measurement
records: `docs/benchmarks/2026-10-23-vector-1m.md`,
`docs/benchmarks/2026-10-25-1m-fixes-retest.md`; design:
`docs/superpowers/specs/2026-10-24-1m-fixes-design.md`.
`docs/benchmarks/2026-10-23-vector-1m.md`; design:
`docs/superpowers/specs/2026-10-24-1m-fixes-design.md`).

### Fixed

- Light Close no longer drops converged pack work. The forest cache
  only advanced at compacts, so a wrapper session that converged its
  vector lane and closed light left the on-disk forest at the last
  compact's state - reopen restored its frozen overlay wholesale and
  re-paid the pack (1M direct measure: 970,751 entries, 2h57m30s).
  Close-shaped checkpoints (`SealForClose`, `Graph.Close`'s light
  branch) now refresh the cache when packs drifted since its last
  write; an idle no-drift close stays O(1) [5b90f6b].
- Recovery no longer re-packs forest-covered frames. The WAL tail
  replay re-intook chain frames the mounted forest already carried -
  vid dedup kept it correct but the copies re-packed (the bench-scale
  repro reopened with overlay=7616 despite a fresh forest). The
  mounted image's watermark now gates the vector intake during replay
  (store frames still apply - props are not in the forest) [5b90f6b].

### Performance

- Volume-triggered compaction goes async. The compact tier's
  snapshot+forest bytes (11.5-26.3s inside the writer's afterBatch at
  1M, 12 occurrences) now run on a background flight over the pinned
  immutable generation; the manifest and WAL mutations complete on the
  writer - quiescent flights Reset exactly as before, busy flights
  (frames appended during the flight) re-anchor v2-style and drop only
  fully-covered segments, so fsynced frames past the pin are never
  deleted. One flight at a time; light seals are suppressed during a
  flight (growth bounded by flight duration x ingest rate, with a
  backpressure valve at CheckpointBytes*4+1GiB) [42eef42, 7efea7b]. The forest
  ENCODE still serializes with submits under the vector index mutex -
  measured at 1M: submit max 11.70s during flights (vs the 11.5-26.3s
  stall every submit used to pay; retest:
  docs/benchmarks/2026-10-25-1m-fixes-retest.md - ingest accept
  5,938 -> 21,459/s, compact-tier submit stall p50 11.47s -> 190ms,
  B1 reopen ready 2h57m30s -> 0s, recall unchanged).

### Changed

- Spec 6.3's "1M x 768 p99 < 10ms" budget line is revised to the
  direct measurement: ef64 (the production adaptive ef) measures
  12.16ms at 1M. The latency-first knee is ef=48 (recall@10 0.968,
  p99 ~8-10ms); closing the 2-3x gap to hnswlib-class engines (SIMD
  dot product, level-0 layout) is a separate optimization arc.

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
