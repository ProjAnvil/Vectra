package vectra_test

import (
	"bytes"
	"errors"
	"path/filepath"
	"slices"
	"testing"

	"github.com/projanvil/vectra"
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

func TestGraphSaveLoadRoundtrip(t *testing.T) {
	g := buildCyclic(t) // already Sealed, contains cycles
	var buf bytes.Buffer
	if err := g.Save(&buf); err != nil {
		t.Fatal(err)
	}
	g2, err := vectra.Load(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if !g2.Sealed() {
		t.Fatal("loaded graph not sealed")
	}
	st1, st2 := g.Stats(), g2.Stats()
	if st1 != st2 {
		t.Errorf("stats %v != %v", st1, st2)
	}
	r1, _ := g.Reachable("a")
	r2, _ := g2.Reachable("a")
	if !slices.Equal(r1, r2) {
		t.Errorf("Reachable diverged: %v vs %v", r1, r2)
	}
	p1, ok1, _ := g.ShortestPath("c", "b")
	p2, ok2, _ := g2.ShortestPath("c", "b")
	if ok1 != ok2 || p1.Len() != p2.Len() {
		t.Errorf("ShortestPath diverged")
	}
	if len(g.Cycles()) != len(g2.Cycles()) {
		t.Errorf("Cycles diverged")
	}
	// props preserved
	n, _ := g2.NodeByKey("a")
	_ = n
}

func TestSaveRequiresSealed(t *testing.T) {
	g := vectra.New()
	if _, err := g.AddNode("a", "S", nil); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := g.Save(&buf); !errors.Is(err, vectra.ErrNotSealed) {
		t.Errorf("err = %v, want ErrNotSealed", err)
	}
}

func TestSaveFileLoadFileRoundtrip(t *testing.T) {
	g := mustBuild(t)
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "g.snap")
	if err := g.SaveFile(path); err != nil {
		t.Fatal(err)
	}
	g2, err := vectra.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if g2.Stats() != g.Stats() {
		t.Errorf("stats diverged")
	}
	// node props preserved across the JSON round-trip
	n1, _ := g.NodeByKey("gateway")
	n2, _ := g2.NodeByKey("gateway")
	if n1.Props["team"] != n2.Props["team"] {
		t.Errorf("props diverged: %v vs %v", n1.Props, n2.Props)
	}
	if _, err := vectra.LoadFile(filepath.Join(t.TempDir(), "missing.snap")); err == nil {
		t.Error("expected error for missing file")
	}
}
