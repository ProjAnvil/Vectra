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

func TestDeleteEdgeIdempotent(t *testing.T) {
	g := mustBuild(t)
	n, err := g.DeleteEdge("gateway", "auth", "CALLS")
	if err != nil || n != 1 {
		t.Fatalf("DeleteEdge = %d, %v; want 1, nil", n, err)
	}
	if g.EdgeCount() != 3 {
		t.Errorf("EdgeCount = %d, want 3", g.EdgeCount())
	}
	if e, ok := g.EdgeByID(0); ok {
		t.Errorf("deleted edge still visible: %+v", e)
	}
	if n, err := g.DeleteEdge("gateway", "auth", "CALLS"); err != nil || n != 0 {
		t.Errorf("re-delete = %d, %v; want 0, nil", n, err)
	}
	if _, err := g.DeleteEdge("x", "auth", "CALLS"); !errors.Is(err, vectra.ErrNodeNotFound) {
		t.Errorf("missing node err = %v, want ErrNodeNotFound", err)
	}
}

func TestDeleteNodeCascadesAndResurrects(t *testing.T) {
	g := mustBuild(t)
	n, err := g.DeleteNode("gateway")
	if err != nil || n != 2 {
		t.Fatalf("DeleteNode = %d, %v; want 2 cascaded edges", n, err)
	}
	if g.NodeCount() != 3 || g.EdgeCount() != 2 {
		t.Fatalf("counts = %d/%d, want 3/2", g.NodeCount(), g.EdgeCount())
	}
	if _, ok := g.NodeByKey("gateway"); ok {
		t.Error("deleted node still visible")
	}
	if _, err := g.AddEdge("gateway", "auth", "CALLS", nil); !errors.Is(err, vectra.ErrNodeNotFound) {
		t.Errorf("AddEdge on deleted node: %v, want ErrNodeNotFound", err)
	}
	id, err := g.AddNode("gateway", "Service", nil) // revive
	if err != nil {
		t.Fatal(err)
	}
	if id != 0 {
		t.Errorf("resurrected id = %d, want 0", id)
	}
	if g.NodeCount() != 4 {
		t.Errorf("NodeCount = %d, want 4", g.NodeCount())
	}
}

func TestMutationAfterSealRejected(t *testing.T) {
	g := mustBuild(t)
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	if _, err := g.UpsertEdge("gateway", "auth", "CALLS", nil); !errors.Is(err, vectra.ErrSealed) {
		t.Errorf("UpsertEdge after seal: %v", err)
	}
	if _, err := g.DeleteEdge("gateway", "auth", "CALLS"); !errors.Is(err, vectra.ErrSealed) {
		t.Errorf("DeleteEdge after seal: %v", err)
	}
	if _, err := g.DeleteNode("gateway"); !errors.Is(err, vectra.ErrSealed) {
		t.Errorf("DeleteNode after seal: %v", err)
	}
}
