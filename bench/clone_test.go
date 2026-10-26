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

// benchFTSGraph builds the BenchmarkSealFTS100K dataset: the benchClone
// shape (100k "Svc" nodes, power-law "CALLS" edges) where every node also
// carries a fresh ~32-rune mixed CJK/Latin "desc" string — the mixed-script
// service description FTS exists for. The seed is fixed, so every caller
// tokenizes byte-identical values; only the IndexFullText declaration
// differs between the gate's two sub-benchmarks.
func benchFTSGraph(nodes, edges int) *vectra.Graph {
	rng := rand.New(rand.NewSource(42))
	g := vectra.New()
	keys := make([]string, nodes)
	for i := range keys {
		keys[i] = fmt.Sprintf("n%08d", i)
		r := make([]rune, 32)
		for j := range r {
			if rng.Intn(2) == 0 {
				r[j] = rune(0x4E00 + rng.Intn(512)) // CJK unified base-block slice
			} else {
				r[j] = rune('a' + rng.Intn(26))
			}
		}
		_, _ = g.AddNode(keys[i], "Svc", map[string]any{"desc": string(r)})
	}
	for range edges {
		_, _ = g.AddEdge(keys[rng.Intn(nodes)], keys[rng.Intn(nodes)], "CALLS", nil)
	}
	return g
}

// BenchmarkSealFTS100K gates the FTS seal cost (2026-10-26 FTS plan): the
// gram-bucket build the Seal pipeline pays for one declared key over 100k
// ~32-rune values. Both sub-benchmarks run the BenchmarkCloneSeal100K
// clone+seal cycle over the identical dataset; only the IndexFullText
// declaration differs, and Clone propagates FTS declarations into its
// engine.Seal, so every with-fts iteration re-pays the bucket build — the
// delta between the two lines is the FTS seal cost alone. Gate lines
// (release-gate red lines): with-fts <= 2x without-fts ns/op,
// <= 1.5s/op absolute, alloc delta <= 250MB. Run it alone (-run '^$'),
// never beside other work: the A6 lesson.
func BenchmarkSealFTS100K(b *testing.B) {
	for _, sub := range []struct {
		name    string
		withFTS bool
	}{
		{"without-fts", false},
		{"with-fts", true},
	} {
		b.Run(sub.name, func(b *testing.B) {
			b.ReportAllocs()
			g := benchFTSGraph(100_000, 1_000_000)
			if sub.withFTS {
				g.IndexFullText("desc")
			}
			if err := g.Seal(); err != nil {
				b.Fatal(err)
			}
			for b.Loop() {
				c := g.Clone()
				if err := c.Seal(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
