package vectra_test

import (
	"errors"
	"testing"

	"github.com/projanvil/vectra"
)

func TestUpsertEdgeReplacesPropsAndKeepsID(t *testing.T) {
	g := mustBuild(t)
	id0, err := g.UpsertEdge("gateway", "auth", "CALLS", map[string]any{"weight": 2})
	if err != nil {
		t.Fatal(err)
	}
	if id0 != 0 { // mustBuild's first edge is gateway->auth CALLS
		t.Errorf("upsert existing edge returned id %d, want 0", id0)
	}
	if g.EdgeCount() != 4 {
		t.Errorf("EdgeCount = %d, want 4 (replace must not add)", g.EdgeCount())
	}
	e, ok := g.EdgeByID(0)
	if !ok || e.Props["weight"] != 2 {
		t.Errorf("edge after upsert = %+v", e)
	}
}

func TestUpsertEdgeCreatesWhenAbsent(t *testing.T) {
	g := mustBuild(t)
	if _, err := g.UpsertEdge("auth", "gateway", "CALLS", nil); err != nil {
		t.Fatal(err)
	}
	if g.EdgeCount() != 5 {
		t.Errorf("EdgeCount = %d, want 5", g.EdgeCount())
	}
}

func TestUpsertEdgeCollapsesParallelEdges(t *testing.T) {
	g := vectra.New()
	for _, k := range []string{"a", "b"} {
		if _, err := g.AddNode(k, "Svc", nil); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		if _, err := g.AddEdge("a", "b", "E", map[string]any{"i": i}); err != nil {
			t.Fatal(err)
		}
	}
	id, err := g.UpsertEdge("a", "b", "E", map[string]any{"i": 9})
	if err != nil {
		t.Fatal(err)
	}
	if id != 2 { // the most recent parallel edge wins
		t.Errorf("kept id = %d, want 2", id)
	}
	if g.EdgeCount() != 1 {
		t.Errorf("EdgeCount = %d, want 1", g.EdgeCount())
	}
	e, _ := g.EdgeByID(2)
	if e.Props["i"] != 9 {
		t.Errorf("props = %v, want i=9", e.Props)
	}
}

func TestUpsertEdgeUnknownNode(t *testing.T) {
	g := vectra.New()
	if _, err := g.UpsertEdge("x", "y", "E", nil); !errors.Is(err, vectra.ErrNodeNotFound) {
		t.Errorf("err = %v, want ErrNodeNotFound", err)
	}
}
