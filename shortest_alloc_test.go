package vectra

import "testing"

// TestShortestPathAllocsCSR pins the B2 fix on the production hot path
// (sealed graph, CSR fast adjacency): pooled parents (parent[v]=u+1, zeroed
// acquire) and ping-pong frontier buffers leave only the result slices.
func TestShortestPathAllocsCSR(t *testing.T) {
	if raceEnabled {
		t.Skip("alloc counts differ under -race")
	}
	g := New()
	keys := make([]string, 64)
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
	if _, err := g.AddEdge(keys[0], keys[32], "SKIP", nil); err != nil {
		t.Fatal(err)
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	if _, found, err := g.ShortestPath("0", "63"); err != nil || !found {
		t.Fatalf("path: found=%v err=%v", found, err)
	}
	if n := testing.AllocsPerRun(200, func() {
		if _, found, err := g.ShortestPath("0", "63"); err != nil || !found {
			t.Fatalf("path: found=%v err=%v", found, err)
		}
	}); n > 14 {
		t.Fatalf("ShortestPath allocs = %v, want <= 14", n)
	}
}
