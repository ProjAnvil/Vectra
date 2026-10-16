package vectra

import (
	"testing"
)

// buildAllocGraph returns a sealed graph: 300 nodes "0".."299", node k
// pointing at (k+1..k+8) mod 300, plus a hub ("0") with 200 out-edges that
// exercises the large-row dedup path.
func buildAllocGraph(t *testing.T) (*Graph, string, string) {
	t.Helper()
	keys := make([]string, 300)
	for i := range keys {
		keys[i] = itoa(i)
	}
	g := New()
	for _, k := range keys {
		if _, err := g.AddNode(k, "SVC", nil); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 300 {
		for d := 1; d <= 8; d++ {
			if _, err := g.AddEdge(keys[i], keys[(i+d)%300], "CALLS", nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	for d := 1; d <= 200; d++ {
		if _, err := g.AddEdge(keys[0], keys[d], "HUB", nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	return g, keys[5], keys[0]
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [4]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}

// TestNeighborsOfAllocs pins the B2 fix: no whole-graph dedup allocation per
// call. Small rows dedup linearly over the result; large rows borrow the
// pooled visited bitmap. Bound is result growth (≤3 append allocs at degree
// 8) — the old code spent one extra NodeCount-sized []bool every call.
func TestNeighborsOfAllocs(t *testing.T) {
	g, small, hub := buildAllocGraph(t)
	if _, err := g.NeighborsOf(small, DirOut); err != nil { // warm
		t.Fatal(err)
	}
	if n := testing.AllocsPerRun(100, func() {
		if _, err := g.NeighborsOf(small, DirOut); err != nil {
			t.Fatal(err)
		}
	}); n > 4 {
		t.Fatalf("small-row NeighborsOf allocs = %v, want <= 4", n)
	}
	if _, err := g.NeighborsOf(hub, DirOut); err != nil { // warm the pool
		t.Fatal(err)
	}
	if n := testing.AllocsPerRun(100, func() {
		if _, err := g.NeighborsOf(hub, DirOut); err != nil {
			t.Fatal(err)
		}
	}); n > 12 { // 200-entry result: ~log2 growth allocs, no graph-sized alloc
		t.Fatalf("hub-row NeighborsOf allocs = %v, want <= 12", n)
	}
}

// TestNeighborsOfDedupEquivalence pins behavior: first-occurrence dedup,
// label filtering, both branches.
func TestNeighborsOfDedupEquivalence(t *testing.T) {
	g, _, hub := buildAllocGraph(t)
	all, err := g.NeighborsOf(hub, DirOut)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 200 {
		t.Fatalf("hub neighbors = %d, want 200", len(all))
	}
	seen := map[NodeID]bool{}
	for _, n := range all {
		if seen[n.ID] {
			t.Fatal("duplicate neighbor in result")
		}
		seen[n.ID] = true
	}
	labeled, err := g.NeighborsOf(hub, DirOut, "CALLS")
	if err != nil {
		t.Fatal(err)
	}
	if len(labeled) != 8 { // the ring's 0→1..8 CALLS edges (hub edges are HUB)
		t.Fatalf("hub CALLS neighbors = %d, want 8", len(labeled))
	}
	mixed, err := g.NeighborsOf(hub, DirOut, "HUB")
	if err != nil {
		t.Fatal(err)
	}
	if len(mixed) != 200 {
		t.Fatalf("hub HUB neighbors = %d, want 200", len(mixed))
	}
	// inbound dedup on the small graph: node 1 receives from 0 (HUB) and
	// from 293..299 (CALLS) — dedup to one entry per node
	in, err := g.NeighborsOf("1", DirIn)
	if err != nil {
		t.Fatal(err)
	}
	if len(in) < 2 {
		t.Fatalf("inbound neighbors of 1 = %d, want >= 2", len(in))
	}
	seenIn := map[NodeID]bool{}
	for _, n := range in {
		if seenIn[n.ID] {
			t.Fatal("duplicate inbound neighbor")
		}
		seenIn[n.ID] = true
	}
}
