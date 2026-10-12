package vectra_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/projanvil/vectra"
)

// Benchmark size: 1M nodes / 10M edges (a full-scale run at the design
// target's upper bound). For a quick check at smaller scale:
// go test -bench . -benchtime=1x
const benchNodes = 1_000_000
const benchEdges = 10_000_000

func benchGraph(b *testing.B, nodes, edges int) *vectra.Graph {
	b.Helper()
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
	return g
}

// Map adjacency baseline: naive Go (map[int32][]int32 + slice visited).
func bfsMap(adj map[int32][]int32, start int32, maxNodes int32) int {
	visited := make([]bool, maxNodes)
	frontier := []int32{start}
	visited[start] = true
	count := 1
	for len(frontier) > 0 {
		var next []int32
		for _, u := range frontier {
			for _, v := range adj[u] {
				if !visited[v] {
					visited[v] = true
					count++
					next = append(next, v)
				}
			}
		}
		frontier = next
	}
	return count
}

func mapAdj(n, e int) map[int32][]int32 {
	rng := rand.New(rand.NewSource(42))
	adj := map[int32][]int32{}
	for i := 0; i < e; i++ {
		s, d := int32(rng.Intn(n)), int32(rng.Intn(n))
		adj[s] = append(adj[s], d)
	}
	return adj
}

func BenchmarkSeal1M(b *testing.B) {
	for i := 0; i < b.N; i++ {
		g := vectra.New()
		rng := rand.New(rand.NewSource(42))
		keys := make([]string, benchNodes)
		for j := range keys {
			keys[j] = fmt.Sprintf("n%08d", j)
			_, _ = g.AddNode(keys[j], "Svc", nil)
		}
		for j := 0; j < benchEdges; j++ {
			_, _ = g.AddEdge(keys[rng.Intn(benchNodes)], keys[rng.Intn(benchNodes)], "CALLS", nil)
		}
		if err := g.Seal(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBFSMap1M(b *testing.B) {
	adj := mapAdj(benchNodes, benchEdges)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if c := bfsMap(adj, 0, benchNodes); c == 0 {
			b.Fatal("empty")
		}
	}
}

func benchCSR(b *testing.B, thr int) {
	g := benchGraph(b, benchNodes, benchEdges)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := g.BFS([]vectra.NodeID{0}, vectra.WithParallelThreshold(thr))
		if err != nil {
			b.Fatal(err)
		}
		if res.Count() == 0 {
			b.Fatal("empty")
		}
	}
}

func BenchmarkBFSSerial1M(b *testing.B)   { benchCSR(b, 1<<30) }
func BenchmarkBFSParallel1M(b *testing.B) { benchCSR(b, 0) }

func BenchmarkReachable1M(b *testing.B) {
	g := benchGraph(b, benchNodes, benchEdges)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := g.ReachableIDs(0)
		if err != nil {
			b.Fatal(err)
		}
		if len(r) == 0 {
			b.Fatal("empty")
		}
	}
}

// BenchmarkShortestPathBiDirVsUni: bidirectional BFS vs whole-graph
// unidirectional BFS distance queries. In random graphs node pairs are
// close; bidirectional wins on path expansion volume.
func BenchmarkShortestPathBiDir(b *testing.B) {
	g := benchGraph(b, benchNodes, benchEdges)
	pairs := make([][2]vectra.NodeID, 64)
	rng := rand.New(rand.NewSource(7))
	for i := range pairs {
		pairs[i] = [2]vectra.NodeID{vectra.NodeID(rng.Intn(benchNodes)), vectra.NodeID(rng.Intn(benchNodes))}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, p := range pairs {
			if _, _, err := g.ShortestPathIDs(p[0], p[1]); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkShortestPathUniBFS(b *testing.B) {
	g := benchGraph(b, benchNodes, benchEdges)
	pairs := make([][2]vectra.NodeID, 64)
	rng := rand.New(rand.NewSource(7))
	for i := range pairs {
		pairs[i] = [2]vectra.NodeID{vectra.NodeID(rng.Intn(benchNodes)), vectra.NodeID(rng.Intn(benchNodes))}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, p := range pairs {
			res, err := g.BFS([]vectra.NodeID{p[0]}, vectra.WithParallelThreshold(1<<30))
			if err != nil {
				b.Fatal(err)
			}
			if !res.Reached(p[1]) {
				_ = res // unreachable also counts as one distance determination
			}
		}
	}
}
