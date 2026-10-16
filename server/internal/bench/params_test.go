package bench

import (
	"testing"

	"github.com/projanvil/vectra"
)

func buildSealed(t *testing.T, spec Spec) (*vectra.Graph, *Graph) {
	t.Helper()
	bg := Generate(spec)
	g := vectra.New()
	for i, n := range bg.Nodes {
		if _, err := g.AddNode(n.Key, n.Label, n.Props); err != nil {
			t.Fatal(err)
		}
		_ = i
	}
	for _, e := range bg.Edges {
		sk := bg.Nodes[e.Src].Key
		dk := bg.Nodes[e.Dst].Key
		if _, err := g.AddEdge(sk, dk, e.Label, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	return g, bg
}

func TestCurateDeterministic(t *testing.T) {
	g, bg := buildSealed(t, Spec{Nodes: 800, Edges: 8000, Seed: 3})
	a := Curate(g, bg, 32, 99)
	b := Curate(g, bg, 32, 99)
	if len(a.Starts) != 32 || len(a.Pairs) != 32 {
		t.Fatalf("sample counts: %d/%d", len(a.Starts), len(a.Pairs))
	}
	for i := range a.Starts {
		if a.Starts[i] != b.Starts[i] {
			t.Fatalf("start[%d] nondeterministic", i)
		}
	}
	for i := range a.Pairs {
		if a.Pairs[i] != b.Pairs[i] {
			t.Fatalf("pair[%d] nondeterministic", i)
		}
	}
}

func TestCurateStartsDegreeWeighted(t *testing.T) {
	g, bg := buildSealed(t, Spec{Nodes: 2000, Edges: 20000, Seed: 5})
	p := Curate(g, bg, 200, 7)
	outDeg := make(map[string]int)
	for _, e := range bg.Edges {
		outDeg[bg.Nodes[e.Src].Key]++
	}
	// High-fanout nodes (top 1% out-degree) should be significantly oversampled
	hits := map[string]bool{}
	for _, s := range p.Starts {
		hits[s] = true
	}
	top := 0
	for _, d := range outDeg {
		if d >= 20 { // High-fanout threshold for this spec
			top++
		}
	}
	if top == 0 {
		t.Fatal("no high-fanout nodes")
	}
	// At least one high-fanout node hit (extremely likely under degree weighting)
	hitHigh := false
	for s := range hits {
		if outDeg[s] >= 20 {
			hitHigh = true
		}
	}
	if !hitHigh {
		t.Error("degree-weighted sampling hit no high-fanout node")
	}
}

func TestCurateTraversalEdges(t *testing.T) {
	g, bg := buildSealed(t, Spec{Nodes: 800, Edges: 8000, Seed: 11})
	p := Curate(g, bg, 32, 21)
	if len(p.TraversalEdges) == 0 {
		t.Fatal("TraversalEdges empty")
	}
	// Independently recompute the expected scan count for each sampled start:
	// unbounded out-BFS, summing visited nodes' generation out-degrees.
	outDeg := make(map[string]int)
	for _, e := range bg.Edges {
		outDeg[bg.Nodes[e.Src].Key]++
	}
	sampled := 0
	for _, k := range p.Starts {
		edges, ok := p.TraversalEdges[k]
		if !ok {
			continue // only the first tepsSample starts are measured
		}
		sampled++
		if edges <= 0 {
			t.Fatalf("start %s: edges = %d", k, edges)
		}
		node, ok := g.NodeByKey(k)
		if !ok {
			t.Fatal("start key missing")
		}
		res, err := g.BFS([]vectra.NodeID{node.ID})
		if err != nil {
			t.Fatal(err)
		}
		want := 0
		for _, id := range res.Nodes() {
			nd, _ := g.NodeByID(id)
			want += outDeg[nd.Key]
		}
		if edges != want {
			t.Fatalf("start %s: edges = %d, want %d", k, edges, want)
		}
	}
	if sampled == 0 {
		t.Fatal("no starts measured")
	}
	if len(p.TraversalEdges) > tepsSample {
		t.Fatalf("measured %d starts, cap is %d", len(p.TraversalEdges), tepsSample)
	}
}

func TestCuratePairsReachable(t *testing.T) {
	g, bg := buildSealed(t, Spec{Nodes: 1500, Edges: 15000, Seed: 8})
	p := Curate(g, bg, 64, 13)
	for _, pair := range p.Pairs {
		if pair[0] == pair[1] {
			t.Fatalf("self-loop pair: %v", pair)
		}
		path, found, err := g.ShortestPath(pair[0], pair[1])
		if err != nil {
			// Start or end node does not exist
			t.Fatalf("pair references nonexistent node: %v %v", pair, err)
		}
		if !found {
			t.Fatalf("pair unreachable (curation failure): %v", pair)
		}
		if path.Len() < 2 {
			// A few 1-hop pairs are allowed, but most should be ≥2
			t.Logf("short-path pair: %v hops=%d", pair, path.Len())
		}
	}
}
