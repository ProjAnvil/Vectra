# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/projanvil/vectra/compare/v0.8.0...HEAD
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
