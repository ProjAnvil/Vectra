package vectra

// pendOp recording-buffer footprint benchmark (audit F-11, 2026-11-01).
//
// The wrapper's drained groups record every op into g.pendOps before the
// one boundary flush, so the buffer's PEAK size — reached at the end of a
// Batch closure, just before flush — is the memory the audit flagged. This
// white-box benchmark builds one plain ingest batch (node upserts + edge
// upserts, the amplified shapes) inside g.Batch and reports the buffer's
// live bytes per recorded op, measured analytically as
// sum(len(slice) * unsafe.Sizeof(element)) over the buffer's actual
// slices at peak. Chosen over a runtime.ReadMemStats HeapAlloc delta
// because it is deterministic (no GC timing, no allocator rounding, no
// interference from the layer builder growing beside the buffer): the
// same formula covers both the old single-slice pendOp buffer and any
// per-kind-slice layout, so before/after numbers are directly comparable.
//
// BenchmarkPendOpSizes prints the per-kind struct widths it relies on;
// both benchmarks are the read-only re-run surface for the F-11 A/B report.

import (
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/projanvil/vectra/internal/wal"
)

// pendBufBytes returns the live bytes held by the WAL recording buffer at
// its current fill level, per the formula above. Covers the per-kind
// payload-slice layout (the baseline single-slice form measured
// len(pendOps) * unsafe.Sizeof(pendOp) — same formula, one slice).
func pendBufBytes(g *Graph) uint64 {
	p := &g.pendOps
	return uint64(len(p.order))*uint64(unsafe.Sizeof(pendEntry{})) +
		uint64(len(p.nodes))*uint64(unsafe.Sizeof(NodeUpsert{})) +
		uint64(len(p.edges))*uint64(unsafe.Sizeof(EdgeUpsert{})) +
		uint64(len(p.edgeDels))*uint64(unsafe.Sizeof(EdgeDelete{})) +
		uint64(len(p.strs))*uint64(unsafe.Sizeof("")) +
		uint64(len(p.vecDecls))*uint64(unsafe.Sizeof([]wal.VectorIndexDecl(nil))) +
		uint64(len(p.ucDecls))*uint64(unsafe.Sizeof([]wal.UniqueConstraint(nil)))
}

// BenchmarkPendOpBufferFootprint records one plain ingest batch (4096 node
// upserts + 2048 edge upserts — the audit's amplified shapes) inside a
// single Batch and reports the peak recording-buffer bytes per op.
func BenchmarkPendOpBufferFootprint(b *testing.B) {
	dir := b.TempDir()
	g, err := Open(filepath.Join(dir, "pend.db"), Options{WALMode: ModeInterval})
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := g.Close(); err != nil {
			b.Fatal(err)
		}
	}()
	const nodes, edges = 4096, 2048
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf uint64
		var ops int
		if err := g.Batch(func(g *Graph) {
			for n := 0; n < nodes; n++ {
				if _, err := g.AddNode(itoaPend(i)+"-"+itoaPend(n), "Svc", nil); err != nil {
					b.Fatal(err)
				}
			}
			for e := 0; e < edges; e++ {
				dst := e + 1
				if dst == nodes {
					dst = 0
				}
				if _, err := g.UpsertEdge(itoaPend(i)+"-"+itoaPend(e), itoaPend(i)+"-"+itoaPend(dst), "REL", nil); err != nil {
					b.Fatal(err)
				}
			}
			ops = len(g.pendOps.order)
			buf = pendBufBytes(g)
		}); err != nil {
			b.Fatal(err)
		}
		if i == 0 {
			if ops != nodes+edges {
				b.Fatalf("recorded %d ops, want %d", ops, nodes+edges)
			}
			b.ReportMetric(float64(buf)/float64(ops), "pendBuf-B/op")
		}
	}
}

// BenchmarkPendOpSizes prints the struct widths behind the buffer numbers
// so a reviewer can re-derive them without a debugger.
func BenchmarkPendOpSizes(b *testing.B) {
	b.ReportMetric(float64(unsafe.Sizeof(pendEntry{})), "pendEntry-B")
	b.ReportMetric(float64(unsafe.Sizeof(NodeUpsert{})), "NodeUpsert-B")
	b.ReportMetric(float64(unsafe.Sizeof(EdgeUpsert{})), "EdgeUpsert-B")
	b.ReportMetric(float64(unsafe.Sizeof(EdgeDelete{})), "EdgeDelete-B")
	for i := 0; i < b.N; i++ {
	}
}

func itoaPend(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [12]byte
	p := len(buf)
	for i > 0 {
		p--
		buf[p] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[p:])
}
