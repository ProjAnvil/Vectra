package bench

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/projanvil/vectra"
)

// Clone is the cost a write-after-seal re-open (service layer) or a publish
// (server publisher) pays: an O(V+E) replay of raw records through
// AddNode/AddEdge. CloneSeal adds the Seal pipeline on top — the full
// re-publish cycle. Run at the design ceiling with -benchtime=1x.
func benchClone(b *testing.B, nodes, edges int, reseal bool) {
	b.Helper()
	b.ReportAllocs()
	rng := rand.New(rand.NewSource(42))
	g := vectra.New()
	keys := make([]string, nodes)
	for i := range keys {
		keys[i] = fmt.Sprintf("n%08d", i)
		_, _ = g.AddNode(keys[i], "Svc", nil)
	}
	for i := 0; i < edges; i++ {
		_, _ = g.AddEdge(keys[rng.Intn(nodes)], keys[rng.Intn(nodes)], "CALLS", nil)
	}
	if err := g.Seal(); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := g.Clone()
		if reseal {
			if err := c.Seal(); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkCloneSealed100K(b *testing.B) { benchClone(b, 100_000, 1_000_000, false) }
func BenchmarkCloneSeal100K(b *testing.B)   { benchClone(b, 100_000, 1_000_000, true) }
func BenchmarkCloneSealed1M(b *testing.B)   { benchClone(b, benchNodes, benchEdges, false) }
func BenchmarkCloneSeal1MCycle(b *testing.B) {
	benchClone(b, benchNodes, benchEdges, true)
}
