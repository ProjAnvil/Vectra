package writer_test

// Wrapper ingest benchmark (audit F-11, 2026-11-01): the single-writer
// Handle.Submit path against a WAL-attached graph, with large consecutive
// changesets — the shape of the wrapper's 64 MiB checkpoint-cadence ingest
// workload, where thousands of WAL pendOps buffer per drained group before
// the one boundary flush. Reports steady-state ns/op, B/op and allocs/op
// for the whole Submit round-trip (queue + apply + record + frame flush).
//
// Protocol notes (kept identical across any A/B re-run):
//   - ModeInterval: frame appends without per-frame fsync, so ns/op
//     measures the recording/encoding path rather than disk sync latency.
//   - CheckpointBytes: -1 disables in-loop volume checkpoints — the ingest
//     path is the measurement target, checkpoint pauses are not.
//   - changesets are pre-built before the timer into a pool sized to b.N:
//     the timed loop only Submits (construction noise stays out of B/op)
//     and every iteration is a fresh-key append — pure ingest under any
//     -benchtime. The pool's memory scales with b.N (the cost of skipping
//     construction inside the loop).
//   - no vector index: this is the PLAIN node/edge batch whose pendOp
//     amplification the audit flagged (~all five payloads embedded per op).

import (
	"path/filepath"
	"testing"

	"github.com/projanvil/vectra"
	"github.com/projanvil/vectra/internal/writer"
)

// ingestCS builds one plain ingest changeset: csSize node upserts followed
// by csSize edge upserts over the batch's own fresh keys (nodes apply
// before edges inside one changeset, so every edge has both endpoints).
func ingestCS(base, csSize int) vectra.Changeset {
	key := func(j int) string {
		if j == csSize {
			j = 0
		}
		return "n" + itoaBench(base) + "-" + itoaBench(j)
	}
	cs := vectra.Changeset{
		Nodes: make([]vectra.NodeUpsert, 0, csSize),
		Edges: make([]vectra.EdgeUpsert, 0, csSize),
	}
	for j := 0; j < csSize; j++ {
		cs.Nodes = append(cs.Nodes, vectra.NodeUpsert{Key: key(j), Label: "Svc"})
	}
	for j := 0; j < csSize; j++ {
		cs.Edges = append(cs.Edges, vectra.EdgeUpsert{
			SrcKey: key(j), DstKey: key(j + 1), Label: "REL",
		})
	}
	return cs
}

func itoaBench(i int) string {
	if i == 0 {
		return "0"
	}
	var b [12]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}

// benchWrapperIngest runs one Submit per iteration against a WAL-attached
// wrapper session and reports the steady-state ingest cost.
func benchWrapperIngest(b *testing.B, csSize int) {
	dir := b.TempDir()
	g, err := vectra.Open(filepath.Join(dir, "ingest.db"), vectra.Options{WALMode: vectra.ModeInterval})
	if err != nil {
		b.Fatal(err)
	}
	h := writer.Attach(g, writer.Config{CheckpointBytes: -1})

	pool := make([]vectra.Changeset, b.N)
	for i := range pool {
		pool[i] = ingestCS(i, csSize)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := h.Submit(pool[i]); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	if err := h.Close(); err != nil {
		b.Fatal(err)
	}
	if err := g.Close(); err != nil {
		b.Fatal(err)
	}
}

// csSize is the per-kind count: ops per Submit = 2*csSize (nodes + edges).
func BenchmarkWrapperIngest512(b *testing.B) { benchWrapperIngest(b, 256) }
func BenchmarkWrapperIngest4K(b *testing.B)  { benchWrapperIngest(b, 2048) }
