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
    per iteration; the pre-built pool is sized to b.N, so every iteration
    is a fresh-key append — pure ingest under any -benchtime (the recorded
    runs used -benchtime=1000x on both sides of the A/B).
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

Layout: `pendBuf` = one order log (`pendEntry{kind opKind; idx uint32}`,
16 B/op) plus per-kind payload slices — `nodes []NodeUpsert`,
`edges []EdgeUpsert`, `edgeDels []EdgeDelete`, `strs []string`
(opNodeDelete/opIndex/opFTS), `vecDecls`/`ucDecls` (one full declaration
list per declaration op). An op costs its payload plus one 16-byte log
entry. After a successful flush the backing arrays are zeroed and re-based
to len 0 (capacity retained up to 65,536 entries per slice — above that,
released), so batch flushes stop re-paying append-amortized growth; a
failed Append rebuilds the buffer from the unflushed order-log suffix
(fresh slices), preserving the retry semantics exactly.

Per-op buffer cost by kind (old → new): node 240 → 56, edge 240 → 80,
edge delete 240 → 64, node delete / typed / FTS key 240 → 32,
vec/unique decl op 240 → 40 bytes.

Same machine, same benchmarks, same flags (3 runs):

| benchmark        | ns/op (3 runs)        | B/op (3 runs)         | allocs/op |
|------------------|-----------------------|-----------------------|-----------|
| WrapperIngest512 | 2.65 / 2.63 / 2.56 ms | 3.05 / 3.16 / 3.09 MB | 2827 / 2831 / 2829 |
| WrapperIngest4K  | 53.0 / 47.4 / 47.7 ms | 34.4 / 31.1 / 31.7 MB | 19869 / 19749 / 19768 |

Peak recording buffer: **64.0 B/op** (240.0 → 64.0, −73%, 3.75x). Overhead
beyond the payload itself: 192 → 16 B/op; the amplification factor is
eliminated (5.0x → 1.33x for the 2:1 mix; 15x → 2x for single-string ops).

Adoption verdict against the F-11 criteria:

- allocs/op: 2836 → 2829 (Ingest512 median), 20103 → 19768 (Ingest4K) —
  NOT worse (slightly better: retained capacity removes the per-batch
  pendOps regrowth allocations, ~10 allocs/Submit on the 512 shape).
- ns/op: 3.08 → 2.63 ms (Ingest512 median, −15%), 67.6 → 47.7 ms
  (Ingest4K median, −29%; baseline 4K runs were noisy, 67–90 ms) — NOT
  worse. The B/op drop on Ingest512 (−7%) matches the two removed costs
  (123 KB smaller buffer + ~245 KB/batch growth churn); the larger 4K
  swing includes allocator/GC second-order effects and run variance.
- Peak pendOps memory: 240 → 64 B/op — significantly down; adopted.

Verification: `go test ./...` (root module, includes WAL round-trip and
restart-parity suites), `go test ./internal/writer/...`, restart-parity
tests (`TestChainRestartIdentity`, `TestStaleForestReplayEquivalence`,
`TestOldBinaryTripwireOnChain`, `TestFreshChainForestFastPath`,
`TestWALSessionThreeSourceRoundtrip`) all green; read-path alloc gates
bit-identical (244/177/422/302/350/400, parse pin 48); `go vet ./...`
clean. Frame order is unchanged by construction: flushPending's run
boundaries, decl-run coalescing (last-in-run wins for vec/unique lists)
and the failed-Append suffix semantics are computed from the same kind
sequence, which the order log preserves verbatim.

