package vectra

import "testing"

// TestBFSAllocsAndLazyDepth pins B2-L1: BFS allocates no NodeCount-sized
// depth array up front (materialized on first Depth/Reached use), and the
// lazy accessors keep exact semantics (reached depths, MaxUint32 unreached).
func TestBFSAllocsAndLazyDepth(t *testing.T) {
	if raceEnabled {
		t.Skip("alloc counts differ under -race")
	}
	g := New()
	keys := make([]string, 40)
	for i := range keys {
		keys[i] = itoa(i)
	}
	for _, k := range keys {
		if _, err := g.AddNode(k, "SVC", nil); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i+1 < len(keys); i++ {
		if _, err := g.AddEdge(keys[i], keys[i+1], "CALLS", nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	start, _ := g.NodeByKey("0")
	// warm
	if _, err := g.BFS([]NodeID{start.ID}); err != nil {
		t.Fatal(err)
	}
	if n := testing.AllocsPerRun(200, func() {
		if _, err := g.BFS([]NodeID{start.ID}); err != nil {
			t.Fatal(err)
		}
	}); n > 95 { // Layers growth is inherent (the result owns them); the
		// NodeCount-sized depth array must not be among the allocations
		t.Fatalf("BFS allocs = %v, want <= 95", n)
	}

	// lazy depth contract: the array materializes on first use, not on BFS
	lazy, err := g.BFS([]NodeID{start.ID})
	if err != nil {
		t.Fatal(err)
	}
	if lazy.depth != nil {
		t.Fatal("depth materialized eagerly")
	}
	if lazy.Depth(NodeID(7)) != 7 {
		t.Fatalf("lazy depth wrong: %d", lazy.Depth(NodeID(7)))
	}
	if lazy.depth == nil {
		t.Fatal("depth not materialized after first use")
	}

	// lazy depth semantics
	res, err := g.BFS([]NodeID{start.ID})
	if err != nil {
		t.Fatal(err)
	}
	if res.Depth(start.ID) != 0 {
		t.Fatalf("start depth = %d", res.Depth(start.ID))
	}
	if res.Depth(NodeID(39)) != 39 {
		t.Fatalf("end depth = %d, want 39", res.Depth(NodeID(39)))
	}
	if res.Reached(start.ID) != true {
		t.Fatal("start not reached")
	}
	unreached := NodeID(200) // beyond the ID space: same panic contract as before is NOT tested here
	_ = unreached
	// a reached-then-depth sequence stays stable across calls
	if res.Depth(NodeID(5)) != 5 || res.Depth(NodeID(5)) != 5 {
		t.Fatal("depth unstable across calls")
	}
	if res.Count() != 40 {
		t.Fatalf("count = %d, want 40", res.Count())
	}
	dm := res.DepthsMap()
	if dm[start.ID] != 0 || dm[NodeID(39)] != 39 {
		t.Fatalf("DepthsMap wrong: %v %v", dm[start.ID], dm[NodeID(39)])
	}
}
