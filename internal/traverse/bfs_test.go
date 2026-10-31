package traverse

import (
	"slices"
	"testing"

	"github.com/projanvil/vectra/internal/csr"
)

// fanRows is a Rows-only RowSource (no CSR fast path): start 0 fans out to
// mids 1..width, every mid converges on sink width+1. The layer-1 frontier
// size is width — the dial for serial vs parallel rows-path expansion — and
// sinkLabel gives the converging edges a maskable label ID.
type fanRows struct {
	n         int
	width     int
	sinkLabel uint32
}

func (s *fanRows) NodeCount() int { return s.n }
func (s *fanRows) FastAdjacency(int) (csr.Adjacency, bool) {
	return csr.Adjacency{}, false
}
func (s *fanRows) Rows(u uint32, dir int) []csr.AdjRow {
	if dir != int(DirOut) {
		return nil
	}
	var out []csr.AdjRow
	switch {
	case u == 0:
		for v := uint32(1); v <= uint32(s.width); v++ {
			out = append(out, csr.AdjRow{Target: v, EdgeID: v})
		}
	case u >= 1 && u <= uint32(s.width):
		out = append(out, csr.AdjRow{Target: uint32(s.width + 1), EdgeID: uint32(s.width + 1), LabelID: s.sinkLabel})
	}
	return out
}

func TestBFSRowsSerialAndDepth(t *testing.T) {
	s := &fanRows{n: 8, width: 4} // nodes 6, 7 unreachable
	res := BFS(s, []uint32{0}, Config{MaxDepth: ^uint32(0), ParallelThreshold: 8})
	want := [][]uint32{{0}, {1, 2, 3, 4}, {5}}
	if !slices.EqualFunc(res.Layers, want, func(a, b []uint32) bool { return slices.Equal(a, b) }) {
		t.Fatalf("layers = %v, want %v", res.Layers, want)
	}
	d := res.EnsureDepth(s.n)
	if d[0] != 0 || d[1] != 1 || d[5] != 2 || d[6] != noDepth {
		t.Fatalf("depths = %v, want start 0, mid 1, sink 2, unreached noDepth", d)
	}
	if res.EnsureDepth(s.n) == nil || len(res.EnsureDepth(s.n)) != s.n {
		t.Fatal("EnsureDepth must memoize the depth array")
	}
	// label mask: converging edges carry label 1, masked out — the sink is
	// never reached and no trailing empty layer is appended
	masked := BFS(&fanRows{n: 8, width: 4, sinkLabel: 1}, []uint32{0},
		Config{MaxDepth: ^uint32(0), ParallelThreshold: 8, LabelMask: []bool{true, false}})
	wantMasked := [][]uint32{{0}, {1, 2, 3, 4}}
	if !slices.EqualFunc(masked.Layers, wantMasked, func(a, b []uint32) bool { return slices.Equal(a, b) }) {
		t.Fatalf("masked layers = %v, want %v", masked.Layers, wantMasked)
	}
}

// TestBFSRowsParallelFallback: the threshold is crossed but the frontier is
// under one parallelChunk, so the parallel entry hires a single worker and
// falls back to the serial loop — same layers either way.
func TestBFSRowsParallelFallback(t *testing.T) {
	s := &fanRows{n: 6, width: 4}
	res := BFS(s, []uint32{0}, Config{MaxDepth: ^uint32(0), ParallelThreshold: 1})
	want := [][]uint32{{0}, {1, 2, 3, 4}, {5}}
	if !slices.EqualFunc(res.Layers, want, func(a, b []uint32) bool { return slices.Equal(a, b) }) {
		t.Fatalf("layers = %v, want %v", res.Layers, want)
	}
}

// TestBFSRowsParallelExpansion: a 600-node frontier spans several chunks and
// workers; the atomic-Or visited bitmap must dedup the 600-way convergence
// on the sink to exactly one layer entry, with NodeFilter applied per node.
func TestBFSRowsParallelExpansion(t *testing.T) {
	const width = 600
	s := &fanRows{n: width + 2, width: width}
	res := BFS(s, []uint32{0}, Config{
		MaxDepth:          ^uint32(0),
		ParallelThreshold: 1,
		NodeFilter:        func(v uint32) bool { return v != 1 },
	})
	if len(res.Layers) != 3 {
		t.Fatalf("layers = %d, want 3: %v", len(res.Layers), res.Layers)
	}
	if len(res.Layers[1]) != width-1 || res.Layers[1][0] != 2 || res.Layers[1][len(res.Layers[1])-1] != width {
		t.Fatalf("layer 1 = %d nodes [%v..%v], want %d filtered [2..%d]",
			len(res.Layers[1]), res.Layers[1][0], res.Layers[1][len(res.Layers[1])-1], width-1, width)
	}
	if len(res.Layers[2]) != 1 || res.Layers[2][0] != width+1 {
		t.Fatalf("layer 2 = %v, want [%d] (concurrent workers dedup the sink)", res.Layers[2], width+1)
	}
	// same wide frontier under a label mask that hides the converging edges:
	// every worker's sink row is masked, expansion exhausts at layer 1
	masked := BFS(&fanRows{n: width + 2, width: width, sinkLabel: 1}, []uint32{0},
		Config{MaxDepth: ^uint32(0), ParallelThreshold: 1, LabelMask: []bool{true, false}})
	if len(masked.Layers) != 2 || len(masked.Layers[1]) != width {
		t.Fatalf("masked parallel layers = %d/%d, want 2 layers with %d mids",
			len(masked.Layers), len(masked.Layers[1]), width)
	}
	// and under a NodeFilter that rejects the sink: the parallel worker's
	// accept branch drops it, expansion again exhausts at layer 1
	filtered := BFS(&fanRows{n: width + 2, width: width}, []uint32{0},
		Config{MaxDepth: ^uint32(0), ParallelThreshold: 1, NodeFilter: func(v uint32) bool { return v != width+1 }})
	if len(filtered.Layers) != 2 || len(filtered.Layers[1]) != width {
		t.Fatalf("filtered parallel layers = %d/%d, want 2 layers with %d mids",
			len(filtered.Layers), len(filtered.Layers[1]), width)
	}
}

// TestVisitedPoolRoundTrip pins the exported pool accessors' contract:
// acquire returns a zeroed bitmap of the requested width, release returns
// it cleared for the next borrower.
func TestVisitedPoolRoundTrip(t *testing.T) {
	b := AcquireVisited(4)
	if len(b) != 4 || b[0] != 0 {
		t.Fatalf("acquire = %v (len %d), want zeroed len 4", b, len(b))
	}
	b[0] = ^uint32(0)
	ReleaseVisited(b)
	b2 := AcquireVisited(2) // the recycled 4-word buffer or a fresh one
	if len(b2) != 2 || b2[0] != 0 || b2[1] != 0 {
		t.Fatalf("acquire after release = %v (len %d), want zeroed len 2", b2, len(b2))
	}
	ReleaseVisited(b2)
}
