package cypher

import (
	"slices"
	"testing"

	"github.com/projanvil/vectra/internal/csr"
	"github.com/projanvil/vectra/internal/engine"
)

// stubSource is a synthetic three-source Source with static out-rows per
// node; every non-row surface is minimal (directions() touches nothing else).
type stubSource struct {
	n    int
	rows map[uint32][]csr.AdjRow
}

func (s *stubSource) NodeCount() int                         { return s.n }
func (s *stubSource) NodeInfo(id uint32) (engine.NodeRec, bool) { return engine.NodeRec{}, true }
func (s *stubSource) EdgeInfo(id uint32) (uint32, uint32, string, map[string]any, bool) {
	return 0, 0, "", nil, false
}
func (s *stubSource) LabelCount() int                            { return 1 }
func (s *stubSource) LabelID(name string) (uint32, bool)         { return 0, true }
func (s *stubSource) LookupIndex(key, value string) ([]uint32, bool) { return nil, false }
func (s *stubSource) LookupNumber(key string, value float64) ([]uint32, bool) {
	return nil, false
}
func (s *stubSource) LookupBool(key string, value bool) ([]uint32, bool) {
	return nil, false
}
func (s *stubSource) FastAdjacency(dir int) (csr.Adjacency, bool) { return csr.Adjacency{}, false }
func (s *stubSource) Rows(u uint32, dir int) []csr.AdjRow         { return s.rows[u] }

// sinkSource adds the optional rowSink surface to stubSource.
type sinkSource struct{ stubSource }

func (s *sinkSource) RowsInto(u uint32, dir int, buf []csr.AdjRow) []csr.AdjRow {
	return append(buf[:0], s.rows[u]...)
}

// TestDirectionsZeroAllocSteadyState locks the view-path expansion contract:
// after scratch warm-up, the directions() row accessor on a rowSink source
// allocates nothing per expanded node. The pre-fix closure called Rows
// (fresh slice) plus three zip makes per node — the same churn f50d0d0
// removed on the BFS path (root TestCypherViewPathAllocs: 545 allocs/query
// at fan 32 pre-fix family).
func TestDirectionsZeroAllocSteadyState(t *testing.T) {
	if raceEnabled {
		t.Skip("alloc counts differ under -race")
	}
	ex := &executor{src: &sinkSource{stubSource{n: 8, rows: map[uint32][]csr.AdjRow{
		1: {
			{Target: 2, EdgeID: 10, LabelID: 0},
			{Target: 3, EdgeID: 11, LabelID: 1},
		},
		2: {{Target: 9, EdgeID: 12, LabelID: 0}},
	}}}}
	pass := ex.directions(0)[0]
	pass(1) // warm the scratch to the largest degree used below
	pass(2)
	allocs := testing.AllocsPerRun(200, func() {
		pass(1)
		pass(2)
	})
	if allocs != 0 {
		t.Fatalf("steady-state directions accessor allocates %.0f/op, want 0", allocs)
	}
	// Contents after reuse, and the stale-tail guard (the bug class f50d0d0's
	// equivalence test caught): a degree-1 call after a degree-2 call must
	// not leak the previous call's tail through the reused buffers.
	tg, eids, lbs := pass(1)
	if len(tg) != 2 || tg[0] != 2 || tg[1] != 3 || eids[1] != 11 || lbs[1] != 1 {
		t.Fatalf("zip contents wrong: tg=%v eids=%v lbs=%v", tg, eids, lbs)
	}
	if tg, _, _ = pass(2); len(tg) != 1 || tg[0] != 9 {
		t.Fatalf("stale rows leaked through reused scratch: tg=%v", tg)
	}
}

// TestDirectionsFallbackEquivalence: a Source without rowSink keeps the
// copy path and must zip identically to the sink path.
func TestDirectionsFallbackEquivalence(t *testing.T) {
	rows := map[uint32][]csr.AdjRow{
		1: {
			{Target: 2, EdgeID: 10, LabelID: 0},
			{Target: 3, EdgeID: 11, LabelID: 1},
		},
	}
	plain := &executor{src: &stubSource{n: 8, rows: rows}}
	sink := &executor{src: &sinkSource{stubSource{n: 8, rows: rows}}}
	tgP, eidP, lbP := plain.directions(0)[0](1)
	tgS, eidS, lbS := sink.directions(0)[0](1)
	if !slices.Equal(tgP, tgS) || !slices.Equal(eidP, eidS) || !slices.Equal(lbP, lbS) {
		t.Fatalf("fallback and sink zips differ: plain=(%v,%v,%v) sink=(%v,%v,%v)",
			tgP, eidP, lbP, tgS, eidS, lbS)
	}
}
