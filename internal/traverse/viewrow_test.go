package traverse

import (
	"testing"

	"github.com/projanvil/vectra/internal/csr"
)

// sinkFake is a RowSource that also implements the buffer-reuse surface.
type sinkFake struct {
	n    int
	rows map[uint32][]csr.AdjRow
}

func (s *sinkFake) Rows(u uint32, dir int) []csr.AdjRow { return s.rows[u] }
func (s *sinkFake) RowsInto(u uint32, dir int, buf []csr.AdjRow) []csr.AdjRow {
	return append(buf[:0], s.rows[u]...)
}
func (s *sinkFake) NodeCount() int                         { return s.n }
func (s *sinkFake) FastAdjacency(dir int) (csr.Adjacency, bool) { return csr.Adjacency{}, false }

// TestViewRowZeroAllocSteadyState locks the row-path allocation contract:
// after buffer warm-up, Row on a viewRow with a RowsInto-capable source
// allocates nothing. The v0.8 baseline allocated 4+ slices per node
// (View.Rows append churn + the three-slice zip); under 16 parallel BFS
// workers that became mheap span-lock contention and the 42ms mixed-read
// p99 tail (docs/benchmarks/2026-10-17-perf-fixes-retest.md residual).
func TestViewRowZeroAllocSteadyState(t *testing.T) {
	src := &sinkFake{n: 8, rows: map[uint32][]csr.AdjRow{
		1: {
			{Target: 2, EdgeID: 10, LabelID: 0},
			{Target: 3, EdgeID: 11, LabelID: 1},
		},
	}}
	r := viewRow{src: src, dir: 0}
	r.Row(1) // warm the scratch buffers to this node's degree
	var tg, eids, lbs []uint32
	allocs := testing.AllocsPerRun(200, func() {
		tg, eids, lbs = r.Row(1)
	})
	if allocs != 0 {
		t.Fatalf("steady-state viewRow.Row allocates %.0f/op, want 0", allocs)
	}
	if len(tg) != 2 || tg[0] != 2 || eids[1] != 11 || lbs[1] != 1 {
		t.Fatalf("zip contents wrong: tg=%v eids=%v lbs=%v", tg, eids, lbs)
	}
}
