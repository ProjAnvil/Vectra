# Vectra Benchmarks

This directory holds the dated benchmark reports produced by the `vectra-bench`
harness, plus the harness design document ([DESIGN.md](DESIGN.md)). Most of the
originals are written in Chinese; this page is the English index and headline
summary. Every number below is copied verbatim from the linked report — nothing
is recomputed, averaged, or re-rounded. Where two reports restate the same
metric, the later report's number is used and its source is named. Index
entries marked "(Chinese)" link to Chinese-language originals; unmarked entries
are in English.

## Method

The harness generates deterministic power-law graphs (Barabasi-Albert
preferential attachment with forward time edges), which produce a few
high-fanout hotspot services — deliberately a harder sample set for multi-hop
queries than uniform random graphs — at three tiers aligned with LDBC /
Graphalytics sizing: S (1K nodes / 10K edges), M (100K / 1M), L (1M / 10M).
Workloads follow SNB Interactive query shapes with out-degree-weighted,
pre-sampled parameters (fixed seeds); a `Wmixed` load runs full-speed readers
plus one paced writer so read tails are attributable to a specific sustained
write rate. Metrics are ops, p50/p90/p99/max latency, QPS, TEPS, and GC/heap
counters at stage boundaries, with one warmup round discarded. The same pure-Go
client drives both `engine` mode (in-process function calls) and `http` mode
(the full HTTP + single-node raft path). Full detail:
[DESIGN.md](DESIGN.md).

Hardware: the 2026-10-12 report ran on an Apple M1 Pro (10-core CPU / 18
threads); every report from 2026-10-13 onward ran on an Apple M5 Pro
(18 cores, 64 GB RAM) on APFS dev-class storage. All numbers are single-machine
laptop figures; the two machines are not comparable to each other, and several
reports record +/-20% or more machine/thermal variance between sessions.

## Headline results

| Workload | Report | Headline result |
|---|---|---|
| Engine ingest, L tier (1M nodes / 10M edges) | [2026-10-12-stress](2026-10-12-stress.md) | 3.53M nodes/s, 9.42M edges/s, Seal 1.04s; zero errors across all loads |
| Full-graph BFS, L tier (engine) | [2026-10-12-stress](2026-10-12-stress.md) | p50 ~70ms, p99 103ms |
| OLTP read-your-writes, S tier / http / 16 workers | [2026-10-14-ryw-await](2026-10-14-ryw-await.md) | txnsi TPS +56% (542 → ~846); rywWait 11.1ms → ~3.1ms (-72%) |
| Crash recovery to ready (A7) | [2026-10-15-delta-wal](2026-10-15-delta-wal.md) | 175.6 ms (14.2 MB WAL replayed) |
| WAL group-commit ingest (A5) | [2026-10-15-delta-wal](2026-10-15-delta-wal.md) | 9.29M edges/s, 92.9K batch-fsync/s |
| Pure-read neighbors, M tier / 8 workers | [2026-10-17-perf-fixes-retest](2026-10-17-perf-fixes-retest.md) | QPS 376K → 7.8M (20.7x); p50 4.0µs → 458ns; p99 250µs → 4.3µs (58x) |
| Mixed read tail under sustained writes, M / 8 workers | [2026-10-17-perf-fixes-retest](2026-10-17-perf-fixes-retest.md) | mixed-read p50 109ms → 205-212µs (530x); p99 1.42s → 14.6-15.4ms (95x) |
| Mixed-read p99, M / 16 workers (row path zero-alloc) | [2026-10-19-row-path-zero-alloc](2026-10-19-row-path-zero-alloc.md) | 42.27ms → 21.27 / 22.05ms (-50%); QPS 4,975 → 6,867 / 6,593 (+32~38%) |
| Vector build, 100k x 768 | [2026-10-19-vector-build-optimization](2026-10-19-vector-build-optimization.md) | 1h40m33s → 12m54.6s (7.8x, 129.1 vectors/s); recall@10 1.0000 x3 held |
| Vector write throughput (group commit, 768-dim) | [2026-10-21-vector-phase2-p21](2026-10-21-vector-phase2-p21.md) | 23-26k/s → 68,778/s (2.7x); batch boundary p50 ~21ms → 7.0ms (3.0x) |
| 100k restart, load vs rebuild | [2026-10-22-v0112-checkpoint](2026-10-22-v0112-checkpoint.md) | load 498ms vs rebuild 7m39.1s (922.9x) |
| Wrapper checkpoint cadence, default 4MiB (v0.11.2) | [2026-10-22-v0112-checkpoint](2026-10-22-v0112-checkpoint.md) | throughput 10.2k → 27.4k/s; seal pause p50 119.8ms → 10.8ms (11x); Close 7.4-9.8s → 974ms |
| 1M vector build, wrapper full chain | [2026-10-23-vector-1m](2026-10-23-vector-1m.md) | nominal 83.3/s (3h20m29s) = 1.09x engine-direct (76.4/s) |
| 1M vector query | [2026-10-23-vector-1m](2026-10-23-vector-1m.md) | recall 0.9480 / 0.9900 / 1.0000; graph/state p50 ratio 1.02-1.05x; ef64 p99 12.16ms (22% over the 10ms line) |
| 1M compact-seal pause, after async fix | [2026-10-25-1m-fixes-retest](2026-10-25-1m-fixes-retest.md) | p50 11.47s → 190ms (-98%); ingest accept 5,938 → 21,459/s (3.6x) |

Earlier readings superseded by a later report on the same line: 100k restart
was 964x in [2026-10-21-vector-phase2-p22](2026-10-21-vector-phase2-p22.md)
(v0.11.1: 455ms vs 7m18.65s); the later v0.11.2 reading above is 922.9x.
Vector write throughput was flat at 68,284/s in v0.11.2
([2026-10-22-v0112-checkpoint](2026-10-22-v0112-checkpoint.md)).

## OLTP and engine performance

- [2026-10-12-stress.md](2026-10-12-stress.md) (Chinese) — First full stress
  run. Engine-mode inject at L tier: 3.53M nodes/s, 9.42M edges/s, Seal 1.04s
  (S: 3.38M/10.74M, Seal 1.1ms; M: 3.45M/9.54M, Seal 104ms); 3-hop p50 scales
  7.7µs (S) → 763µs (M) → 7.1ms (L), matching the power-law neighborhood
  growth; L full-graph BFS ~70ms / p99 103ms; raft+HTTP inject 1.3M edges/s;
  zero errors across all loads.
- [2026-10-13-oltp-txn.md](2026-10-13-oltp-txn.md) (Chinese) — OLTP
  transaction layer. Client etiquette decides the conflict rate: an
  immediate-resubmit anti-pattern shows 93.3% txnsi conflicts at 31.6 commit
  QPS, while read-your-writes etiquette zeroes conflicts (0.0%, 4.8 QPS); with
  instant publish on S tier, SER at 16 workers conflicts only 4.3%; single-
  transaction HTTP latency ~20ms is dominated by 2x HTTP + raft consensus.
- [2026-10-14-ryw-await.md](2026-10-14-ryw-await.md) (Chinese) — v0.5.0
  acceptance of the await-based read-your-writes long poll: txnsi +56%
  (542 → ~846), txnser +48% (556 → ~822), rywWait 11.1ms → ~3.1ms (-72%),
  p50 26.5ms → ~18.3ms (-31%).
- [2026-10-15-delta-wal.md](2026-10-15-delta-wal.md) — v0.7 acceptance
  (micro-layer generations + embedded WAL, lines A1-A8): A7 crash recovery
  175.6 ms (14.2 MB replayed); A5 WAL group commit 9.29M edges/s at 92.9K
  batch-fsync/s; initial A4 FAIL (-90.1% WAL-off import) recovered 9x in the
  2026-10-16 hardening retest to 3,710,584 edges/s (0.91x vs the recorded
  baseline — line met), and the A2/A6 misses also passed after hardening.
- [2026-10-16-perf-baseline.md](2026-10-16-perf-baseline.md) (Chinese) —
  Engine-mode baseline and bottleneck hunt. Pure reads healthy and core-scaling:
  M/8 workers neighbors 376K QPS, bfsall TEPS 1.09G → 2.31G edges/s at 16
  workers (2.1x), L bfsall TEPS 721M edges/s (4.6x w1→w8). Bottleneck B1:
  under only ~165 write batches/s, M read p50 collapses ~92µs → 109ms (~27x),
  p99 to 1.4-1.7s. B2: read-path allocations push GC to 10-15%. B3 (light):
  unbatched inject decays 8.6M (S) → 2.0M (L) edges/s. Cold start 2.1-3.0M
  edges/s.
- [2026-10-17-perf-fixes-retest.md](2026-10-17-perf-fixes-retest.md)
  (Chinese) — B1/B2/B3 fix retest in three rounds. B2: neighbors M-w8 QPS
  376K → 7.8M (20.7x), p99 250µs → 4.3µs (58x), read GC 10.5% → 5.4%. B1:
  mixed-read p50 109ms → 205-212µs (530x), p99 1.42s → 14.6-15.4ms (95x),
  writes reach the 166 batches/s pacer ceiling with p99 4.8-5.9ms; round 3
  halves write p50 851µs → 401µs (2.1x); the A4 import regression is restored
  to 1.56M edges/s.

## Zero-allocation row path

- [2026-10-19-row-path-zero-alloc.md](2026-10-19-row-path-zero-alloc.md)
  (Chinese) — Zero-allocation row path on M-w16 mixed: mixed-read p99
  42.27ms → 21.27 / 22.05ms (-50%), max 165.04ms → 104.25ms (-37%), QPS
  4,975 → 6,867 / 6,593 (+32~38%); p50 slightly up (175.3µs → 189.7 / 211.5µs),
  buffer-reset cost, not magnitude-relevant.
- [2026-10-22-cypher-zero-alloc.md](2026-10-22-cypher-zero-alloc.md)
  (Chinese) — The same treatment for Cypher-expanded rows: allocations
  545 → 244 per query (-55%), with the expansion accessor pinned at steady-state
  zero allocations; concurrent tail latency is unchanged within +/-10% run
  variance (the reliable evidence is the allocation side), and the M-w16
  regression check shows mixed-read p99 15.38ms, QPS 9,373 — no regression.

## Vector search arc

- [2026-10-19-vector-spike.md](2026-10-19-vector-spike.md) (Chinese) —
  Phase 0 spike, gate 2a conditional pass at 100k x 768 in-distribution:
  recall@10 1.0000 (saturated, ef 32-384), p99 3.08ms @ ef64, memory
  amplification 1.12-1.46x; build throughput flagged as the independent red
  line: ~17 vectors/s at 100k x 768 (1h40m33s), 10-20x slower than hnswlib
  single-threaded.
- [2026-10-19-vector-build-optimization.md](2026-10-19-vector-build-optimization.md)
  (Chinese) — Build-throughput optimization: 100k x 768 build 1h40m33s →
  12m54.6s (7.8x, 129.1/s); 10k 2m07s → 17.8s (7.1x, 562/s); 768-dim cosine
  kernel 577ns → 92ns (6.3x); write amplification 5.59x / 8.19x → 2.97x /
  4.42x (-47% / -46%); recall@10 1.0000 x3 held.
- [2026-10-20-vector-phase1-gate2b.md](2026-10-20-vector-phase1-gate2b.md)
  (Chinese) — Phase 1 gate 2b integration, all lines pass: vector write
  throughput 23-26k vectors/s (three runs 23.0/24.1/30.7k, line >=20k);
  indexed-vs-unindexed plain-write p50 ratio ~101%; recall@10 1.0000 at all
  efs with integration p50 ratio 0.82/0.87/0.87x. The gate also caught a real
  defect: two-stage drain lifted throughput 601/s → 26k/s (43x).
- [2026-10-21-vector-phase2-p21.md](2026-10-21-vector-phase2-p21.md)
  (Chinese) — Phase 2 binary WAL/store channel: vector write throughput
  23-26k/s → 68,778/s (2.7x); batch boundary p50 ~21ms → 7.0ms (3.0x); WAL
  frame 9.29KB → 3.07KB per vector (3.0x); full snapshot at 8192 vectors
  74.1MB → 24.4MB (3.0x); with the 64MiB default, wrapper ingest 44,849/s with
  0 mid-run seals.
- [2026-10-21-vector-phase2-p22.md](2026-10-21-vector-phase2-p22.md)
  (Chinese) — Forest persistence restart at 100k x 768: load 455ms vs rebuild
  7m18.65s (964x); v.vec 309.4MB at ~680MB/s load bandwidth; gate 2b re-run
  in the same session all pass.
- [2026-10-23-vector-1m.md](2026-10-23-vector-1m.md) (Chinese) — 1M direct
  measurement replacing all extrapolations. Build: wrapper full chain 83.3/s
  nominal (3h20m29s) = 1.09x engine-direct; query recall
  0.9480/0.9900/1.0000 with graph/state p50 ratio 1.02-1.05x, ef64 p99
  12.16ms 22% over the 10ms line; restart: pure forest load open 8.728s,
  ready 0s (red line 10min, ~68x margin), no-forest rebuild ~3-4h; new
  finding: frozen backlog of 970,751 vectors after a lightweight close,
  ready 2h57m30s; memory graph-alone 6.59GB (2.14x raw bytes), GC pauses
  16.3ms total.
- [2026-10-25-1m-fixes-retest.md](2026-10-25-1m-fixes-retest.md) (Chinese) —
  1M retest of the three fixes: compact-seal pause p50 11.47s → 190ms (-98%),
  ingest accept 5,938 → 21,459/s (3.6x); frozen backlog cleared
  (overlay-at-open 970,751 → 2,047, ready 2h57m30s → 0s) at the designed cost
  of Close 315ms → 32.4s; no regression — recall bit-identical, sustained
  ingest 33.7k/s (1.6x).

## Operations

- [2026-10-21-wrapper-checkpoint-cadence.md](2026-10-21-wrapper-checkpoint-cadence.md)
  (Chinese) — Wrapper-mode checkpoint cadence: the default 4MiB budget is
  unusable for vector loads — one full-snapshot seal every ~547 vectors, 84%
  of ingest wall clock, throughput 2835-2919/s (1/9 of the ~26k/s direct
  mode); ruling: raise the default to 64MiB and ship it together with the
  Phase 2 binary frames (which drop the frame cost 9.29KB → 3.07KB per vector
  and lift 4MiB throughput to 10,213/s).
- [2026-10-22-v0112-checkpoint.md](2026-10-22-v0112-checkpoint.md)
  (Chinese) — v0.11.2 turns the O(N) full-snapshot checkpoint into O(1)
  manifest rotation on a retained WAL chain: wrapper default-config
  throughput 10.2k → 27.4k/s (27,380/s), seal pause p50 119.8ms → 10.8ms
  (11x), seal share 76% → 35%, Close 7.4-9.8s → 974ms; gate 2b three lines
  flat vs v0.11.1 (68,284/s, 7.03ms); 100k restart 922.9x.
- [2026-10-22-v0112-scope.md](2026-10-22-v0112-scope.md) (Chinese) — v0.11.2
  hotfix scope, decided by measurement: frame skeleton encoding closed (JSON
  skeleton marshal is 4% of the 6.8-7.5ms batch boundary); wrapper Close drain
  fixed, Close 39.2s → 3.0s; cypher result-row allocation deferred to v0.12;
  incremental checkpoint delivered and measured in the checkpoint report. The
  2026-10-23 addendum records the overlay tail-scan fix (v0.11.3): full-tail
  query 896µs → 342-451µs at ef32 (-50~62%).

## Raw logs

The two 1M reports keep their harness scripts and per-stage logs alongside
them: [2026-10-23-vector-1m/](2026-10-23-vector-1m/) and
[2026-10-25-1m-fixes-retest/](2026-10-25-1m-fixes-retest/).
