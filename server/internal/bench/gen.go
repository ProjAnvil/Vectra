// Package bench provides graph generation, parameter curation, latency stats,
// and workload execution for Vectra stress tests. See docs/benchmarks/DESIGN.md
// for the design document.
package bench

import (
	"fmt"
	"math/rand"
)

// Spec describes a graph to generate: power-law degree distribution (BA
// preferential attachment, new nodes attach to old nodes), with a cycleRate
// fraction of edges reversed to create cycles (real dependency graphs have
// a few circular dependencies).
type Spec struct {
	Nodes     int
	Edges     int
	Seed      int64
	CycleRate float64 // 0~1, default 0.05
}

// Node / Edge are generation artifacts (aligned with server.Batch's upsert structs).
type Node struct {
	Key   string
	Label string
	Props map[string]any
}

type Edge struct {
	Src   int
	Dst   int
	Label string
}

// Graph is the deterministic generation result.
type Graph struct {
	Nodes []Node
	Edges []Edge
}

var edgeLabels = []string{"CALLS", "DATA", "EVENT"}

// Generate produces a power-law graph per the BA model: nodes carry a tier
// prop, edges take labels in order. New→old is the dominant direction (time
// arrow); a CycleRate fraction reverses to create cycles.
func Generate(spec Spec) *Graph {
	if spec.CycleRate == 0 {
		spec.CycleRate = 0.05
	}
	rng := rand.New(rand.NewSource(spec.Seed))
	g := &Graph{Nodes: make([]Node, 0, spec.Nodes)}

	m := spec.Edges / spec.Nodes // Edges attached per new node (average in-degree)
	if m < 1 {
		m = 1
	}
	// Repeated-target table: a node appears once per degree gained — the classic
	// degree-proportional sampling implementation
	targets := make([]int, 0, spec.Edges*2)

	addNode := func() int {
		id := len(g.Nodes)
		tier := "t" + string(rune('0'+id%4))
		env := "prod"
		if id%5 == 0 {
			env = "staging"
		}
		g.Nodes = append(g.Nodes, Node{
			Key:   fmt.Sprintf("svc-%07d", id),
			Label: "Service",
			Props: map[string]any{"tier": tier, "env": env},
		})
		return id
	}

	// Seed cluster (BA's initial complete graph)
	const seedCluster = 4
	for range seedCluster {
		if len(g.Nodes) >= spec.Nodes {
			break
		}
		u := addNode()
		for v := range u {
			g.Edges = append(g.Edges, Edge{Src: u, Dst: v, Label: edgeLabels[len(g.Edges)%len(edgeLabels)]})
			targets = append(targets, u, v)
		}
	}

	for len(g.Nodes) < spec.Nodes && len(g.Edges) < spec.Edges {
		u := addNode()
		targets = append(targets, u)
		for i := 0; i < m && len(g.Edges) < spec.Edges; i++ {
			var v int
			if len(targets) > 0 {
				v = targets[rng.Intn(len(targets))]
			} else {
				v = rng.Intn(len(g.Nodes))
			}
			if v == u {
				continue
			}
			src, dst := u, v // New -> old
			if rng.Float64() < spec.CycleRate {
				// Back edge creates a cycle: both endpoints are "old-node winners" and both gain selection weight
				src, dst = v, u
				targets = append(targets, src, dst)
			} else {
				// Forward edge only adds weight to the chosen old node (classic BA:
				// rich-get-richer applies only to existing nodes; new nodes start
				// with weight 1 to avoid flattening the distribution)
				targets = append(targets, dst)
			}
			g.Edges = append(g.Edges, Edge{Src: src, Dst: dst, Label: edgeLabels[len(g.Edges)%len(edgeLabels)]})
		}
	}
	// Top up the edge count (when nodes run out first): random node pairs
	for len(g.Edges) < spec.Edges {
		u := rng.Intn(len(g.Nodes))
		v := rng.Intn(len(g.Nodes))
		if u == v {
			continue
		}
		g.Edges = append(g.Edges, Edge{Src: u, Dst: v, Label: edgeLabels[len(g.Edges)%len(edgeLabels)]})
	}
	return g
}
