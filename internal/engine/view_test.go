package engine

import (
	"testing"

	"github.com/projanvil/vectra/internal/csr"
)

// fakeSource is a minimal DeltaSource for row-path tests: static out/in
// buckets with source-local label IDs.
type fakeSource struct {
	labels []string
	out    map[uint32][]csr.AdjRow
	in     map[uint32][]csr.AdjRow
}

func (s *fakeSource) Rows(u uint32, dir int) []csr.AdjRow {
	if dir == ViewDirIn {
		return s.in[u]
	}
	return s.out[u]
}

func (s *fakeSource) Labels() []string        { return s.labels }
func (s *fakeSource) DeadBaseEdges() []uint32 { return nil }
func (s *fakeSource) KilledEdgeIDs() []uint32 { return nil }
func (s *fakeSource) Empty() bool             { return false }

func testView(t *testing.T) *View {
	t.Helper()
	base, err := Seal(
		[]NodeRec{{Key: "a"}, {Key: "b"}, {Key: "c"}, {Key: "d"}},
		[]EdgeRec{
			{Src: 0, Dst: 1, LabelID: 0},
			{Src: 0, Dst: 2, LabelID: 0},
			{Src: 1, Dst: 3, LabelID: 0},
		},
		[]string{"knows"},
		nil,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	src := &fakeSource{
		labels: []string{"wrote"},
		out: map[uint32][]csr.AdjRow{
			0: {{Target: 3, EdgeID: 100, LabelID: 0}},
			2: {
				{Target: 1, EdgeID: 101, LabelID: 0},
				{Target: 3, EdgeID: 102, LabelID: 0},
			},
		},
		in: map[uint32][]csr.AdjRow{
			1: {{Target: 2, EdgeID: 101, LabelID: 0}},
		},
	}
	return NewView(base, []DeltaSource{src}, 4)
}

// TestViewRowsIntoEquivalence: RowsInto with a reused caller buffer returns
// exactly what a fresh Rows returns, for every node and direction, across
// repeated buffer reuse (stale contents must not leak into results).
func TestViewRowsIntoEquivalence(t *testing.T) {
	v := testView(t)
	var buf []csr.AdjRow
	for round := 0; round < 3; round++ {
		for u := uint32(0); u < 4; u++ {
			for _, dir := range []int{ViewDirOut, ViewDirIn} {
				want := v.Rows(u, dir)
				buf = v.RowsInto(u, dir, buf)
				// length+element compare: RowsInto reuses buf, so an empty
				// result is a non-nil zero-length slice where Rows yields nil.
				if len(buf) != len(want) {
					t.Fatalf("round %d u=%d dir=%d: RowsInto=%v want %v", round, u, dir, buf, want)
				}
				for i := range want {
					if buf[i] != want[i] {
						t.Fatalf("round %d u=%d dir=%d [%d]: RowsInto=%v want %v", round, u, dir, i, buf[i], want[i])
					}
				}
			}
		}
	}
}

// TestViewRowsIntoZeroAlloc locks the allocation contract that makes the
// concurrent row path contention-free: steady-state RowsInto (same node,
// warmed buffers) allocates nothing. The v0.8 baseline allocated 2-3 slices
// per call; 16 BFS workers turned that into mheap span-lock contention and
// a 42ms mixed-read p99 tail.
func TestViewRowsIntoZeroAlloc(t *testing.T) {
	v := testView(t)
	var buf []csr.AdjRow
	buf = v.RowsInto(0, ViewDirOut, buf) // warm the buffers to degree
	allocs := testing.AllocsPerRun(200, func() {
		buf = v.RowsInto(0, ViewDirOut, buf)
	})
	if allocs != 0 {
		t.Fatalf("steady-state RowsInto allocates %.0f times per call, want 0", allocs)
	}
}
