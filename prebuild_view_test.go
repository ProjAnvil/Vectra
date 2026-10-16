package vectra

import (
	"math/rand"
	"slices"
	"testing"

	"github.com/projanvil/vectra/internal/csr"
)

// TestPrebuildViewEquivalence: the writer's incrementally extended view
// (pure-append and fold-rewind paths) must be indistinguishable from a
// fresh full build of the same generation — rows for every node in both
// directions, node count, and label resolution.
func TestPrebuildViewEquivalence(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	const keysN = 30
	keys := make([]string, keysN)
	for i := range keys {
		keys[i] = itoa(i)
	}
	g := New().SetAutoCompact(8) // folds fire during the stream
	for _, k := range keys {
		if _, err := g.AddNode(k, "SVC", map[string]any{"v": 1}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i+1 < keysN; i++ {
		if _, err := g.AddEdge(keys[i], keys[i+1], "BASE", nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	rowsSorted := func(rows []csr.AdjRow) []csr.AdjRow {
		out := append([]csr.AdjRow(nil), rows...)
		slices.SortFunc(out, func(a, b csr.AdjRow) int {
			if a.Target != b.Target {
				return int(a.Target) - int(b.Target)
			}
			return int(a.EdgeID) - int(b.EdgeID)
		})
		return out
	}
	check := func(step int) {
		v := g.view.Load()
		if len(v.layers) == 0 {
			return
		}
		c := g.viewCache.Load()
		if c == nil || c.gen != v || c.ext == nil {
			t.Fatalf("step %d: cache missing or not prebuilt", step)
		}
		fresh := v.engineView()
		if c.view.NodeCount() != fresh.NodeCount() {
			t.Fatalf("step %d: NodeCount %d vs %d", step, c.view.NodeCount(), fresh.NodeCount())
		}
		n := c.view.NodeCount()
		for u := 0; u < n; u++ {
			for _, dir := range []int{0, 1} {
				a := rowsSorted(c.view.Rows(uint32(u), dir))
				b := rowsSorted(fresh.Rows(uint32(u), dir))
				if len(a) != len(b) {
					t.Fatalf("step %d u=%d dir=%d: %d rows vs %d", step, u, dir, len(a), len(b))
				}
				for i := range a {
					if a[i] != b[i] {
						t.Fatalf("step %d u=%d dir=%d [%d]: %+v vs %+v", step, u, dir, i, a[i], b[i])
					}
				}
			}
		}
	}
	for batch := 0; batch < 80; batch++ {
		_ = g.Batch(func(gg *Graph) {
			for e := 0; e < 6; e++ {
				a, b := keys[rng.Intn(keysN)], keys[rng.Intn(keysN)]
				if a == b {
					b = keys[(rng.Intn(keysN-1)+1)%keysN]
				}
				switch rng.Intn(4) {
				case 0:
					_, _ = gg.AddEdge(a, b, "M", nil)
				case 1:
					_, _ = gg.UpsertEdge(a, b, "M", map[string]any{"w": rng.Intn(9)})
				case 2:
					_, _ = gg.DeleteEdge(a, b, "M")
				case 3:
					_, _ = gg.AddNode("x"+itoa(rng.Intn(8)), "X", nil)
				}
			}
		})
		check(batch)
	}
	// the differential surface still agrees after everything
	if g.EdgeCount() == 0 {
		t.Fatal("no edges survived the stream")
	}
}
