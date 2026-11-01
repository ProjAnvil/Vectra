# pendOp WAL recording-buffer A/B (audit F-11) — 2026-11-01

Audit finding F-11 (docs/research/2026-11-01-go-code-audit.md): `pendOp`
(vectra_recovery.go) embeds all five op payloads plus two decl slices in
every recorded op regardless of kind, so plain node/edge batches buffer a
large multiple of the memory their payloads need — the wrapper's 64 MiB
checkpoint-cadence ingest workload buffers thousands of pendOps per drained
group. The audit demands a measured A/B (allocs/op, ns/op on an ingest
benchmark) before adopting any layout change; the measurement can veto.

## Method

- Machine: Apple M5 Pro (darwin/arm64, 18 hw threads), go1.27.3, battery/AC
  quiet desktop window, no other load.
- Benchmarks (added by this change, re-run read-only with the commands
  below):
  - `BenchmarkWrapperIngest512` / `BenchmarkWrapperIngest4K`
    (internal/writer/ingest_bench_test.go): full `Handle.Submit` round-trip
    against a WAL-attached graph (`ModeInterval`, volume checkpoints
    disabled), one pre-built plain changeset (nodes then edges, no vectors)
    per iteration; `-benchtime=1000x` = pure append ingest.
  - `BenchmarkPendOpBufferFootprint` (pendop_bench_test.go): peak
    recording-buffer bytes per recorded op, measured inside one `g.Batch`
    of 4096 node upserts + 2048 edge upserts (2:1 mix), computed
    analytically as sum(len(slice) × unsafe.Sizeof(element)) over the
    buffer's slices at peak. Chosen over a HeapAlloc delta because it is
    deterministic (no GC timing or allocator rounding; the layer builder
    growing beside the buffer does not pollute it) and covers both the old
    single-slice and any per-kind-slice layout with the same formula.
  - `BenchmarkPendOpSizes`: the struct widths behind the numbers.

Re-run (read-only):

    go test ./internal/writer/ -run '^$' -bench 'BenchmarkWrapperIngest' -benchtime=1000x -count=3
    go test . -run '^$' -bench 'BenchmarkPendOp' -benchtime=20x -count=3

## Baseline (before any layout change)

Struct widths (unsafe.Sizeof, arm64):

| type          | bytes |
|---------------|-------|
| pendOp        | 240   |
| NodeUpsert    | 40    |
| EdgeUpsert    | 64    |
| EdgeDelete    | 48    |

Peak recording buffer: **240.0 B/op** — every op, regardless of kind, pays
the full 240-byte struct. Payload actually needed for the 2:1 node:edge
mix: (2×40 + 64)/3 = 48 B/op → measured amplification factor **5.0x** for
plain node/edge batches (higher for single-string ops: 240/16 = 15x for a
node delete; ~10x ballpark of the audit holds for string-op mixes). The
audit premise is confirmed, not stale.

Wrapper ingest (`-benchtime=1000x`, 3 runs):

| benchmark            | ns/op (3 runs)        | B/op (3 runs)         | allocs/op |
|----------------------|-----------------------|-----------------------|-----------|
| WrapperIngest512     | 2.94 / 3.08 / 3.74 ms | 3.12 / 3.31 / 3.31 MB | 2830 / 2836 / 2837 |
| WrapperIngest4K      | 67.4 / 67.6 / 89.7 ms | 41.1 / 44.2 / 47.7 MB | 19998 / 20103 / 20229 |

(The pendOp buffer is ~123 KB per 512-op Submit — ~4% of B/op — so the
ingest benchmarks are the not-worse gate; the footprint benchmark is the
improvement gate.)

## After (per-kind payload slices + compact order log)

TBD — filled in by the A/B commit.
