package vectra_test

import (
	"errors"
	"testing"

	"vectra"
)

func mustBuild(t *testing.T) *vectra.Graph {
	t.Helper()
	g := vectra.New()
	for _, n := range []struct{ key, label string }{
		{"gateway", "Service"}, {"auth", "Service"}, {"ledger", "Service"}, {"db", "Storage"},
	} {
		if _, err := g.AddNode(n.key, n.label, map[string]any{"team": "core"}); err != nil {
			t.Fatalf("AddNode(%s): %v", n.key, err)
		}
	}
	for _, e := range []struct{ s, d, l string }{
		{"gateway", "auth", "CALLS"},
		{"gateway", "ledger", "CALLS"},
		{"auth", "db", "QUERY"},
		{"ledger", "db", "QUERY"},
	} {
		if _, err := g.AddEdge(e.s, e.d, e.l, nil); err != nil {
			t.Fatalf("AddEdge(%s->%s): %v", e.s, e.d, err)
		}
	}
	return g
}

func TestAddNodeIdempotent(t *testing.T) {
	g := vectra.New()
	id1, err := g.AddNode("a", "Svc", map[string]any{"k": 1})
	if err != nil {
		t.Fatal(err)
	}
	id2, err := g.AddNode("a", "Svc2", map[string]any{"k": 2})
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Errorf("re-AddNode returned %d, want %d", id2, id1)
	}
	n, ok := g.NodeByKey("a")
	if !ok || n.Label != "Svc2" || n.Props["k"] != 2 {
		t.Errorf("node after update = %+v", n)
	}
}

func TestAddEdgeUnknownNode(t *testing.T) {
	g := vectra.New()
	_, err := g.AddEdge("x", "y", "CALLS", nil)
	if !errors.Is(err, vectra.ErrNodeNotFound) {
		t.Errorf("err = %v, want ErrNodeNotFound", err)
	}
}

func TestSealLocksMutationAndBuildsStats(t *testing.T) {
	g := mustBuild(t)
	if g.Sealed() {
		t.Fatal("graph sealed before Seal()")
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	if !g.Sealed() {
		t.Fatal("Seal() did not seal")
	}
	if _, err := g.AddNode("late", "Svc", nil); !errors.Is(err, vectra.ErrSealed) {
		t.Errorf("AddNode after seal: %v, want ErrSealed", err)
	}
	st := g.Stats()
	if st.Nodes != 4 || st.Edges != 4 {
		t.Errorf("Stats = %+v, want nodes=4 edges=4", st)
	}
	if st.SCCs != 4 {
		t.Errorf("SCCs = %d, want 4 (acyclic)", st.SCCs)
	}
}

func TestSealCyclicGraphStats(t *testing.T) {
	g := vectra.New()
	for _, k := range []string{"a", "b", "c"} {
		if _, err := g.AddNode(k, "Svc", nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, e := range []struct{ s, d string }{{"a", "b"}, {"b", "c"}, {"c", "a"}} {
		if _, err := g.AddEdge(e.s, e.d, "CALLS", nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	st := g.Stats()
	if st.SCCs != 1 {
		t.Errorf("SCCs = %d, want 1", st.SCCs)
	}
	if st.CyclicNodes != 3 {
		t.Errorf("CyclicNodes = %d, want 3", st.CyclicNodes)
	}
}

func TestEdgeByIDAndCounts(t *testing.T) {
	g := mustBuild(t)
	id, err := g.AddEdge("db", "gateway", "EVENT", map[string]any{"qos": "at-least-once"})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	e, ok := g.EdgeByID(id)
	if !ok || e.Src != 3 || e.Dst != 0 || e.Label != "EVENT" || e.Props["qos"] != "at-least-once" {
		t.Errorf("EdgeByID = %+v ok=%v", e, ok)
	}
	if g.EdgeCount() != 5 || g.NodeCount() != 4 {
		t.Errorf("counts = %d/%d, want 5/4", g.EdgeCount(), g.NodeCount())
	}
}

func TestNodeByIDBounds(t *testing.T) {
	g := mustBuild(t)
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	if _, ok := g.NodeByID(vectra.NodeID(99)); ok {
		t.Error("NodeByID(99) should not exist")
	}
	if _, ok := g.EdgeByID(vectra.EdgeID(99)); ok {
		t.Error("EdgeByID(99) should not exist")
	}
}
