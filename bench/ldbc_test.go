package bench

import (
	"io"
	"math/rand"
	"strconv"
	"testing"

	"github.com/projanvil/vectra"
	"github.com/projanvil/vectra/internal/fixture"
)

// ============================================================
// LDBC SNB Interactive-style workload benchmarks
//
// Mimics SNB's complex-read query shapes (2-3 hop neighborhoods and
// node-pair paths from one or two start nodes) and adopts its parameter
// curation method: query parameters (start/end nodes) are fixed into a
// representative set outside the benchmark (high fan-out nodes,
// verified-reachable pairs) rather than re-drawn at random each iteration —
// keeping the workload consistent across iterations and the numbers
// comparable.
// Dataset: SNAP email-Eu-core real email graph (1005 nodes / 25571 edges).
// ============================================================

// curateSNBParameters fixes benchmark parameters in advance:
// - kHopSources: top-16 nodes by out-degree (SNB's "active user" start nodes)
// - pathPairs: 64 verified-reachable node pairs (CR14 path query parameters)
func curateSNBParameters(t testing.TB, g *vectra.Graph, el fixture.EdgeList) (kHopSources []vectra.NodeID, pathPairs [][2]vectra.NodeID) {
	t.Helper()
	outDeg := make([]int, el.N)
	for _, s := range el.Src {
		outDeg[s]++
	}
	idx := make([]int, el.N)
	for i := range idx {
		idx[i] = i
	}
	// top 16 by out-degree, descending
	for i := 0; i < len(idx); i++ {
		for j := i + 1; j < len(idx); j++ {
			if outDeg[idx[j]] > outDeg[idx[i]] {
				idx[i], idx[j] = idx[j], idx[i]
			}
		}
	}
	for i := 0; i < 16 && i < el.N; i++ {
		kHopSources = append(kHopSources, vectra.NodeID(idx[i]))
	}
	// draw 64 pairs with a fixed seed, keeping only reachable ones (parameter screening = curation)
	rng := rand.New(rand.NewSource(31))
	for len(pathPairs) < 64 {
		u, v := vectra.NodeID(rng.Intn(el.N)), vectra.NodeID(rng.Intn(el.N))
		if u == v {
			continue
		}
		if _, found, _ := g.ShortestPathIDs(u, v); found {
			pathPairs = append(pathPairs, [2]vectra.NodeID{u, v})
		}
	}
	return kHopSources, pathPairs
}

func benchSNB(b *testing.B) (*vectra.Graph, []vectra.NodeID, [][2]vectra.NodeID) {
	b.Helper()
	g, el := fixture.SNAPEmail(b)
	srcs, pairs := curateSNBParameters(b, g, el)
	return g, srcs, pairs
}

// bytesBuffer is a re-Readable in-memory buffer (a bytes.Buffer cannot be re-read).
type bytesBuffer struct {
	data []byte
	off  int
}

func (b *bytesBuffer) Write(p []byte) (int, error) {
	b.data = append(b.data, p...)
	return len(p), nil
}

func (b *bytesBuffer) Read(p []byte) (int, error) {
	if b.off >= len(b.data) {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.off:])
	b.off += n
	return n, nil
}

// BenchmarkSNB_kHop2/3 correspond to SNB complex-read neighborhood
// discovery queries (CR3/CR9 shapes): 2/3-hop neighborhood counts from
// high fan-out start nodes.
func BenchmarkSNB_kHop2(b *testing.B) {
	g, srcs, _ := benchSNB(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := g.BFS([]vectra.NodeID{srcs[i%len(srcs)]}, vectra.WithMaxDepth(2))
		if err != nil {
			b.Fatal(err)
		}
		if res.Count() < 10 {
			b.Fatalf("2-hop neighborhood of high fan-out start suspiciously small: %d", res.Count())
		}
	}
}

func BenchmarkSNB_kHop3(b *testing.B) {
	g, srcs, _ := benchSNB(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := g.BFS([]vectra.NodeID{srcs[i%len(srcs)]}, vectra.WithMaxDepth(3))
		if err != nil {
			b.Fatal(err)
		}
		if res.Count() < 10 {
			b.Fatalf("3-hop neighborhood of high fan-out start suspiciously small: %d", res.Count())
		}
	}
}

// BenchmarkSNB_ShortestPath corresponds to CR14: shortest path between two nodes (bidirectional BFS).
func BenchmarkSNB_ShortestPath(b *testing.B) {
	g, _, pairs := benchSNB(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := pairs[i%len(pairs)]
		if _, found, err := g.ShortestPathIDs(p[0], p[1]); err != nil || !found {
			b.Fatalf("curated pair unreachable: %v", p)
		}
	}
}

// BenchmarkSNB_Reachability: reachability determination (supernode fast path).
func BenchmarkSNB_Reachability(b *testing.B) {
	g, srcs, _ := benchSNB(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.ReachableIDs(srcs[i%len(srcs)]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSNB_MultiSourceBFS: batched multi-source traversal (the
// call-graph scenario where one requirement spans multiple entry
// services): one BFS over 16 high fan-out start nodes.
func BenchmarkSNB_MultiSourceBFS(b *testing.B) {
	g, srcs, _ := benchSNB(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := g.BFS(srcs, vectra.WithParallelThreshold(0))
		if err != nil {
			b.Fatal(err)
		}
		if res.Count() < 800 {
			b.Fatalf("multi-source neighborhood suspiciously small: %d", res.Count())
		}
	}
}

// BenchmarkSNB_LoadSnapshot: cold-start path — snapshot load + automatic
// Seal throughput (the restart-recovery cost in service mode).
func BenchmarkSNB_LoadSnapshot(b *testing.B) {
	g := vectra.New()
	for i := 0; i < 1005; i++ {
		_, _ = g.AddNode(strconv.Itoa(i), "V", nil)
	}
	for i := 0; i < 25571; i++ {
		_, _ = g.AddEdge(strconv.Itoa(i%1005), strconv.Itoa((i*7+3)%1005), "E", nil)
	}
	if err := g.Seal(); err != nil {
		b.Fatal(err)
	}
	var buf bytesBuffer
	if err := g.Save(&buf); err != nil {
		b.Fatal(err)
	}
	snap := buf.data
	b.SetBytes(int64(len(snap)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := vectra.Load(&bytesBuffer{data: snap}); err != nil {
			b.Fatal(err)
		}
	}
}
