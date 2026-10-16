package traverse

import (
	"testing"

	"github.com/projanvil/vectra/internal/csr"
)

// stubRows is a Rows-only RowSource (no CSR fast path): a directed path
// 0→1→…→n-1 plus one shortcut 0→n/2, both directions precomputed and the
// row slices cached so the alloc measurement isolates the algorithm itself.
type stubRows struct {
	n        int
	out, in_ map[uint32][]csr.AdjRow
}

func newStubRows(n int) *stubRows {
	s := &stubRows{n: n, out: map[uint32][]csr.AdjRow{}, in_: map[uint32][]csr.AdjRow{}}
	link := func(u, v uint32) {
		s.out[u] = append(s.out[u], csr.AdjRow{Target: v, EdgeID: uint32(len(s.out[u])), LabelID: 0})
		s.in_[v] = append(s.in_[v], csr.AdjRow{Target: u, EdgeID: uint32(len(s.in_[v])), LabelID: 0})
	}
	for u := uint32(0); u+1 < uint32(n); u++ {
		link(u, u+1)
	}
	link(0, uint32(n)/2)
	return s
}

func (s *stubRows) NodeCount() int { return s.n }
func (s *stubRows) FastAdjacency(int) (csr.Adjacency, bool) {
	return csr.Adjacency{}, false
}
func (s *stubRows) Rows(u uint32, dir int) []csr.AdjRow {
	if dir == 0 {
		return s.out[u]
	}
	return s.in_[u]
}

func TestShortestPathStubCorrect(t *testing.T) {
	s := newStubRows(64)
	nodes, edges, found := ShortestPath(s, 0, 63)
	if !found {
		t.Fatal("path not found")
	}
	// shortest route rides the 0→32 shortcut: 0,32,33,…,63 = 33 nodes
	if len(nodes) != 33 {
		t.Fatalf("path len = %d, want 33: %v", len(nodes), nodes)
	}
	if nodes[0] != 0 || nodes[1] != 32 || nodes[len(nodes)-1] != 63 {
		t.Fatalf("path endpoints wrong: %v", nodes)
	}
	if len(edges) != len(nodes)-1 {
		t.Fatalf("edges len = %d, want %d", len(edges), len(nodes)-1)
	}
	// contiguous chain
	for i := 0; i+1 < len(nodes); i++ {
		ok := false
		for _, r := range s.Rows(nodes[i], 0) {
			if r.Target == nodes[i+1] {
				ok = true
			}
		}
		if !ok {
			t.Fatalf("gap at %d: %d → %d not an edge", i, nodes[i], nodes[i+1])
		}
	}
	if _, _, found := ShortestPath(s, 63, 0); found {
		t.Fatal("reverse path found in a directed chain")
	}
}

func TestShortestPathAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("alloc counts differ under -race")
	}
	s := newStubRows(64)
	ShortestPath(s, 0, 63) // warm pools
	if n := testing.AllocsPerRun(200, func() {
		ShortestPath(s, 0, 63)
	}); n > shortestAllocBound {
		t.Fatalf("ShortestPath allocs = %v, want <= %d", n, shortestAllocBound)
	}
}

// shortestAllocBound: row-path measurement. Pre-fix 267; parent pooling +
// frontier ping-pong removed ~30. The residual is viewRow.Row per-node
// three-slice zips (row path only; the production hot path is CSR). Parents
// arrays pooled): measured baseline 10, bound 8.
const shortestAllocBound = 240
