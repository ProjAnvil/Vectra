package bench

import (
	"fmt"
	"testing"
)

func TestGenerateDeterministic(t *testing.T) {
	a := Generate(Spec{Nodes: 500, Edges: 5000, Seed: 7})
	b := Generate(Spec{Nodes: 500, Edges: 5000, Seed: 7})
	if len(a.Edges) != len(b.Edges) || len(a.Nodes) != len(b.Nodes) {
		t.Fatalf("size mismatch: %d/%d vs %d/%d", len(a.Nodes), len(a.Edges), len(b.Nodes), len(b.Edges))
	}
	for i := range a.Edges {
		if a.Edges[i] != b.Edges[i] {
			t.Fatalf("edge %d mismatch: %v vs %v", i, a.Edges[i], b.Edges[i])
		}
	}
}

func TestGenerateShape(t *testing.T) {
	g := Generate(Spec{Nodes: 2000, Edges: 20000, Seed: 42})
	if len(g.Nodes) != 2000 {
		t.Fatalf("node count = %d", len(g.Nodes))
	}
	if len(g.Edges) != 20000 {
		t.Fatalf("edge count = %d", len(g.Edges))
	}
	// Power-law degree distribution: the top 1% of nodes (by total degree) should
	// account for >= 15% of edge endpoints
	outDeg := make([]int, len(g.Nodes))
	inDeg := make([]int, len(g.Nodes))
	for _, e := range g.Edges {
		outDeg[e.Src]++
		inDeg[e.Dst]++
	}
	type kv struct{ id, deg int }
	var all []kv
	total := 0
	for i := range g.Nodes {
		d := outDeg[i] + inDeg[i]
		all = append(all, kv{i, d})
		total += d
	}
	// Simple sort to take the top 1%
	for i := 1; i < len(all); i++ {
		for j := i; j > 0 && all[j].deg > all[j-1].deg; j-- {
			all[j], all[j-1] = all[j-1], all[j]
		}
	}
	top1 := len(all) / 100
	if top1 == 0 {
		top1 = 1
	}
	topSum := 0
	for _, k := range all[:top1] {
		topSum += k.deg
	}
	if float64(topSum)/float64(total) < 0.15 {
		t.Errorf("insufficient power-law: top 1%% nodes hold only %.1f%% of degrees", 100*float64(topSum)/float64(total))
	}
	// Edge labels come from a fixed set
	for _, e := range g.Edges {
		switch e.Label {
		case "CALLS", "DATA", "EVENT":
		default:
			t.Fatalf("illegal label %q", e.Label)
		}
	}
	// Node props exist and include env
	for i, n := range g.Nodes {
		if n.Key == "" || n.Props["tier"] == nil {
			t.Fatalf("node %d props incomplete: %+v", i, n)
		}
	}
}

func TestGenerateCyclesExist(t *testing.T) {
	// cycleRate creates cycles: with default parameters, cycles with SCC>1 should
	// exist (power-law graph + back edges)
	g := Generate(Spec{Nodes: 3000, Edges: 30000, Seed: 11})
	back := 0 // Edges in the old -> new direction (approximate back edges)
	for _, e := range g.Edges {
		if e.Dst > e.Src {
			back++
		}
	}
	if back == 0 {
		t.Error("no back edges at all — cycleRate not in effect")
	}
	_ = fmt.Sprint(back)
}
