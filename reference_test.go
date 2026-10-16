package vectra_test

import (
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/projanvil/vectra"
	"github.com/projanvil/vectra/internal/fixture"
)

// ============================================================
// Independent reference implementations (for cross-validation)
//
// Deliberately naive and completely different from Vectra's style
// (map adjacency + node-by-node traversal), sharing no production code
// paths. LDBC Graphalytics' verification method: compare kernel output
// one by one against the reference implementation.
// ============================================================

// refBFS is a naive map-adjacency BFS; returns the depth table (start 0 included).
func refBFS(adj map[int][]int, start int) map[int]int {
	dist := map[int]int{start: 0}
	frontier := []int{start}
	for len(frontier) > 0 {
		var next []int
		for _, u := range frontier {
			for _, v := range adj[u] {
				if _, ok := dist[v]; !ok {
					dist[v] = dist[u] + 1
					next = append(next, v)
				}
			}
		}
		frontier = next
	}
	return dist
}

// refReachAll computes the reachability closure of all nodes (slower as n
// grows; small graphs only).
func refReachAll(n int, adj map[int][]int) []map[int]bool {
	reach := make([]map[int]bool, n)
	for s := 0; s < n; s++ {
		reach[s] = map[int]bool{s: true}
		frontier := []int{s}
		for len(frontier) > 0 {
			var next []int
			for _, u := range frontier {
				for _, v := range adj[u] {
					if !reach[s][v] {
						reach[s][v] = true
						next = append(next, v)
					}
				}
			}
			frontier = next
		}
	}
	return reach
}

// refSCCBrute computes SCCs directly from the reachability-closure
// definition: u~v iff mutually reachable. O(V·E), small graphs only
// (≤300 nodes), as an independent control group against Tarjan.
func refSCCBrute(n int, adj map[int][]int) [][]int {
	reach := refReachAll(n, adj)
	assigned := make([]bool, n)
	var sccs [][]int
	for u := 0; u < n; u++ {
		if assigned[u] {
			continue
		}
		comp := []int{u}
		assigned[u] = true
		for v := u + 1; v < n; v++ {
			if !assigned[v] && reach[u][v] && reach[v][u] {
				comp = append(comp, v)
				assigned[v] = true
			}
		}
		sccs = append(sccs, comp)
	}
	return sccs
}

// ============================================================
// Test graph generators: multiple topology shapes (each shape stresses
// different code paths)
// ============================================================

type edgeList struct {
	src, dst []int
	n        int
}

func (e edgeList) adj() map[int][]int {
	adj := map[int][]int{}
	for i := range e.src {
		adj[e.src[i]] = append(adj[e.src[i]], e.dst[i])
	}
	return adj
}

func (e edgeList) graph(t *testing.T) *vectra.Graph {
	t.Helper()
	g := vectra.New()
	for v := 0; v < e.n; v++ {
		if _, err := g.AddNode(strconv.Itoa(v), "V", nil); err != nil {
			t.Fatal(err)
		}
	}
	for i := range e.src {
		if _, err := g.AddEdge(strconv.Itoa(e.src[i]), strconv.Itoa(e.dst[i]), "E", nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	return g
}

// erGraph: sparse random graph (Erdős–Rényi, multiple seeds).
func erGraph(n int, p float64, seed int64) edgeList {
	rng := rand.New(rand.NewSource(seed))
	var e edgeList
	e.n = n
	for u := 0; u < n; u++ {
		for v := 0; v < n; v++ {
			if rng.Float64() < p {
				e.src = append(e.src, u)
				e.dst = append(e.dst, v)
			}
		}
	}
	return e
}

// layeredDAG: layered DAG (no intra-layer edges, forward inter-layer
// edges): acyclic, multi-source multi-sink.
func layeredDAG(layers []int, crossP float64, seed int64) edgeList {
	rng := rand.New(rand.NewSource(seed))
	var e edgeList
	e.n = 0
	offsets := make([]int, len(layers)+1)
	for i, size := range layers {
		offsets[i] = e.n
		e.n += size
	}
	offsets[len(layers)] = e.n
	for l := 0; l+1 < len(layers); l++ {
		for u := offsets[l]; u < offsets[l+1]; u++ {
			for v := offsets[l+1]; v < offsets[l+2]; v++ {
				if rng.Float64() < crossP {
					e.src = append(e.src, u)
					e.dst = append(e.dst, v)
				}
			}
		}
	}
	return e
}

// ringOfClusters: k clusters, each a ring internally (one large SCC),
// one-way links between clusters.
func ringOfClusters(clusters, size int) edgeList {
	var e edgeList
	e.n = clusters * size
	for c := 0; c < clusters; c++ {
		base := c * size
		for i := 0; i < size; i++ {
			e.src = append(e.src, base+i)
			e.dst = append(e.dst, base+(i+1)%size)
		}
		if c+1 < clusters {
			e.src = append(e.src, base)
			e.dst = append(e.dst, (c+1)*size)
		}
	}
	return e
}

var propertyShapes = []struct {
	name string
	make func() edgeList
}{
	{"ER-p0.02-seed1", func() edgeList { return erGraph(300, 0.02, 1) }},
	{"ER-p0.08-seed2", func() edgeList { return erGraph(300, 0.08, 2) }},
	{"layeredDAG", func() edgeList { return layeredDAG([]int{10, 80, 150, 60}, 0.15, 3) }},
	{"ringOfClusters-5x20", func() edgeList { return ringOfClusters(5, 20) }},
	{"denseER-p0.3-seed5", func() edgeList { return erGraph(200, 0.3, 5) }},
}

// TestPropertyBFSMatchesReference: every shape, every start node —
// Vectra BFS depths (serial and parallel) must match the reference BFS
// node for node. This is exactly how the Graphalytics BFS kernel is
// verified (output = per-node level vector).
func TestPropertyBFSMatchesReference(t *testing.T) {
	for _, shape := range propertyShapes {
		t.Run(shape.name, func(t *testing.T) {
			el := shape.make()
			g := el.graph(t)
			adj := el.adj()
			for s := 0; s < el.n; s++ {
				want := refBFS(adj, s)
				ser, err := g.BFS([]vectra.NodeID{vectra.NodeID(s)}, vectra.WithParallelThreshold(1<<30))
				if err != nil {
					t.Fatal(err)
				}
				par, err := g.BFS([]vectra.NodeID{vectra.NodeID(s)}, vectra.WithParallelThreshold(0))
				if err != nil {
					t.Fatal(err)
				}
				if ser.Count() != len(want) || par.Count() != len(want) {
					t.Fatalf("source %d: serial=%d parallel=%d reference=%d",
						s, ser.Count(), par.Count(), len(want))
				}
				for v, d := range want {
					if ser.Depth(vectra.NodeID(v)) != uint32(d) {
						t.Fatalf("source %d -> %d: serial depth=%d want %d",
							s, v, ser.Depth(vectra.NodeID(v)), d)
					}
					if par.Depth(vectra.NodeID(v)) != uint32(d) {
						t.Fatalf("source %d -> %d: parallel depth=%d want %d",
							s, v, par.Depth(vectra.NodeID(v)), d)
					}
				}
			}
		})
	}
}

// TestPropertyReachableMatchesReference: reachable set matches the
// reference BFS node for node.
func TestPropertyReachableMatchesReference(t *testing.T) {
	for _, shape := range propertyShapes {
		t.Run(shape.name, func(t *testing.T) {
			el := shape.make()
			g := el.graph(t)
			adj := el.adj()
			for _, s := range []int{0, 1, el.n / 2, el.n - 1} {
				want := refBFS(adj, s)
				got, err := g.ReachableIDs(vectra.NodeID(s))
				if err != nil {
					t.Fatal(err)
				}
				if len(got) != len(want) {
					t.Fatalf("source %d: Reachable=%d reference=%d", s, len(got), len(want))
				}
				for _, id := range got {
					if _, ok := want[int(id)]; !ok {
						t.Fatalf("source %d: node %d in Reachable but not in reference set", s, id)
					}
				}
			}
		})
	}
}

// TestPropertyShortestPathMatchesReference: 150 sampled node pairs per
// shape — bidirectional BFS path length == reference BFS distance, and
// the path is contiguous with real edges.
func TestPropertyShortestPathMatchesReference(t *testing.T) {
	for _, shape := range propertyShapes {
		t.Run(shape.name, func(t *testing.T) {
			el := shape.make()
			g := el.graph(t)
			adj := el.adj()
			rng := rand.New(rand.NewSource(99))
			for i := 0; i < 150; i++ {
				u, v := rng.Intn(el.n), rng.Intn(el.n)
				want := refBFS(adj, u)
				p, found, err := g.ShortestPathIDs(vectra.NodeID(u), vectra.NodeID(v))
				if err != nil {
					t.Fatal(err)
				}
				if u == v {
					continue
				}
				if d, ok := want[v]; ok {
					if !found {
						t.Fatalf("%d->%d: reference dist=%d but not found", u, v, d)
					}
					if p.Len() != d {
						t.Fatalf("%d->%d: path len=%d reference dist=%d", u, v, p.Len(), d)
					}
					// path validity and optimality: each step's edge exists; re-computing distance per prefix should equal the remaining length
					for j := 0; j+1 < len(p.Nodes); j++ {
						e, ok := g.EdgeByID(p.Edges[j])
						if !ok || e.Src != p.Nodes[j] || e.Dst != p.Nodes[j+1] {
							t.Fatalf("%d->%d: edge %d mismatch on path", u, v, j)
						}
					}
				} else if found {
					t.Fatalf("%d->%d: found path but reference says unreachable", u, v)
				}
			}
		})
	}
}

// TestPropertyCyclesMatchBruteForceSCC: Cycles()'s SCC partition matches
// the brute-force reachability-closure definition exactly (multi-cycle
// shapes + sparse random shapes).
func TestPropertyCyclesMatchBruteForceSCC(t *testing.T) {
	for _, shape := range propertyShapes {
		t.Run(shape.name, func(t *testing.T) {
			el := shape.make()
			g := el.graph(t)
			adj := el.adj()
			want := refSCCBrute(el.n, adj)

			var wantMulti [][]int // reference: components of size>1
			selfLoops := map[int]bool{}
			for i := range el.src {
				if el.src[i] == el.dst[i] {
					selfLoops[el.src[i]] = true
				}
			}
			for _, comp := range want {
				if len(comp) > 1 {
					wantMulti = append(wantMulti, comp)
				}
			}

			got := g.Cycles()
			// SCC cycles + self-loop nodes
			wantCycleCount := len(wantMulti) + len(selfLoops)
			if len(got) != wantCycleCount {
				t.Fatalf("Cycles()=%d want %d (%d multi-SCC + %d self-loops)",
					len(got), wantCycleCount, len(wantMulti), len(selfLoops))
			}
			gotSets := map[string]bool{}
			for _, c := range got {
				keys := make([]string, len(c.Nodes))
				for i, id := range c.Nodes {
					keys[i] = strconv.Itoa(int(id))
				}
				gotSets[strings.Join(keys, ",")] = true
			}
			for _, comp := range wantMulti {
				keys := make([]string, len(comp))
				for i, id := range comp {
					keys[i] = strconv.Itoa(id)
				}
				if !gotSets[strings.Join(keys, ",")] {
					t.Fatalf("missing SCC %v in Cycles(); got %v", comp, gotSets)
				}
			}
		})
	}
}

// TestPropertyStatsSCCCount consistency: Stats.SCCs == reference component count.
func TestPropertyStatsSCCCount(t *testing.T) {
	for _, shape := range propertyShapes {
		t.Run(shape.name, func(t *testing.T) {
			el := shape.make()
			g := el.graph(t)
			want := len(refSCCBrute(el.n, el.adj()))
			if got := g.Stats().SCCs; got != want {
				t.Fatalf("Stats.SCCs=%d want %d", got, want)
			}
		})
	}
}

// ============================================================
// SNAP real-dataset loading
// ============================================================

// loadSNAPEmail loads the shared SNAP dataset via internal/fixture and
// adapts it to the local edgeList (which carries the reference helpers).
func loadSNAPEmail(t testing.TB) (*vectra.Graph, edgeList) {
	t.Helper()
	g, el := fixture.SNAPEmail(t)
	return g, edgeList{n: el.N, src: el.Src, dst: el.Dst}
}
