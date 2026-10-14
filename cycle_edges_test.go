package vectra_test

import (
	"testing"

	"github.com/projanvil/vectra"
)

// TestCyclesCarryMemberEdges: Cycles must expose the intra-cycle edges
// (label + props) so consumers can triage severity (constructor cycle vs
// field cycle) without re-reading raw records and re-joining via EdgesOf.
func TestCyclesCarryMemberEdges(t *testing.T) {
	g := vectra.New()
	_, _ = g.AddNode("a", "Svc", nil)
	_, _ = g.AddNode("b", "Svc", nil)
	_, _ = g.AddNode("c", "Svc", nil)
	_, _ = g.AddNode("d", "Svc", nil) // downstream, not on any cycle
	_, _ = g.AddNode("s", "Svc", nil) // self-loop
	_, _ = g.AddEdge("a", "b", "DEPENDS_ON", map[string]any{"via": "ctor"})
	_, _ = g.AddEdge("b", "c", "DEPENDS_ON", map[string]any{"via": "field"})
	_, _ = g.AddEdge("c", "a", "DEPENDS_ON", nil)
	_, _ = g.AddEdge("b", "a", "INJECT", nil)      // chord: still an intra-SCC edge
	_, _ = g.AddEdge("b", "d", "DEPENDS_ON", nil)  // leaves the cycle: excluded
	_, _ = g.AddEdge("s", "s", "SELF", map[string]any{"kind": "lazy"})
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}

	cs := g.Cycles()
	if len(cs) != 2 {
		t.Fatalf("cycles = %d, want 2 (SCC a-b-c + self-loop s)", len(cs))
	}
	scc, self := cs[0], cs[1]
	if scc.Len() != 3 || self.Len() != 1 {
		t.Fatalf("cycle sizes = %d/%d, want 3/1", scc.Len(), self.Len())
	}

	// intra-SCC edges only: a→b, b→a, b→c, c→a (b→d excluded), ordered by src
	type wantEdge struct{ src, dst, label string }
	want := []wantEdge{
		{"a", "b", "DEPENDS_ON"},
		{"b", "a", "INJECT"},
		{"b", "c", "DEPENDS_ON"},
		{"c", "a", "DEPENDS_ON"},
	}
	if len(scc.Edges) != len(want) {
		t.Fatalf("scc edges = %v", scc.Edges)
	}
	for i, w := range want {
		e := scc.Edges[i]
		src, _ := g.NodeByID(e.Src)
		dst, _ := g.NodeByID(e.Dst)
		if src.Key != w.src || dst.Key != w.dst || e.Label != w.label {
			t.Errorf("edge[%d] = %s-%s-%s, want %s-%s-%s",
				i, src.Key, e.Label, dst.Key, w.src, w.label, w.dst)
		}
	}
	// props survive for triage (index 0 = a→b, index 2 = b→c by ordering)
	if scc.Edges[0].Props["via"] != "ctor" {
		t.Errorf("a→b props = %v, want via=ctor", scc.Edges[0].Props)
	}
	if scc.Edges[2].Props["via"] != "field" {
		t.Errorf("b→c props = %v, want via=field", scc.Edges[2].Props)
	}

	if len(self.Edges) != 1 {
		t.Fatalf("self-loop edges = %d, want 1", len(self.Edges))
	}
	sNode, _ := g.NodeByID(self.Edges[0].Src)
	if sNode.Key != "s" || self.Edges[0].Label != "SELF" || self.Edges[0].Props["kind"] != "lazy" {
		t.Errorf("self-loop edge = %+v", self.Edges[0])
	}
}
