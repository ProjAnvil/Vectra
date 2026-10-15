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

// TestMutationAfterSealAccepted: the sealed gate is dissolved — writes after
// Seal succeed, land in a new micro-layer generation, and are immediately
// visible to point queries.
func TestMutationAfterSealAccepted(t *testing.T) {
	g := mustBuild(t)
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	if _, err := g.UpsertEdge("gateway", "auth", "CALLS", map[string]any{"w": 5}); err != nil {
		t.Fatalf("UpsertEdge after seal: %v", err)
	}
	if e, ok := g.EdgeByID(0); !ok || e.Props["w"] != 5 {
		t.Fatalf("fold after seal = %+v ok=%v", e, ok)
	}
	if n, err := g.DeleteEdge("gateway", "ledger", "CALLS"); err != nil || n != 1 {
		t.Fatalf("DeleteEdge after seal = %d, %v", n, err)
	}
	if n, err := g.DeleteNode("db"); err != nil || n != 2 {
		t.Fatalf("DeleteNode after seal = %d, %v; want 2 cascaded", n, err)
	}
	if g.NodeCount() != 3 || g.EdgeCount() != 1 {
		t.Fatalf("counts = %d/%d, want 3/1", g.NodeCount(), g.EdgeCount())
	}
	if err := g.Seal(); err != nil { // merges the post-Seal writes
		t.Fatal(err)
	}
	if st := g.Stats(); st.Nodes != 3 || st.Edges != 1 {
		t.Fatalf("post-merge stats = %+v", st)
	}
}

func TestSealCompactionRenumbersAndRebuildsKeyIndex(t *testing.T) {
	g := vectra.New()
	for _, k := range []string{"a", "b", "c"} {
		if _, err := g.AddNode(k, "Svc", nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, e := range [][3]string{{"a", "b", "E"}, {"b", "c", "E"}} {
		if _, err := g.AddEdge(e[0], e[1], e[2], nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := g.DeleteNode("b"); err != nil { // cascades both edges away
		t.Fatal(err)
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	st := g.Stats()
	if st.Nodes != 2 || st.Edges != 0 {
		t.Fatalf("Stats = %+v, want 2/0", st)
	}
	n, ok := g.NodeByKey("c") // original ID 2 -> 1 after compaction
	if !ok || n.ID != 1 {
		t.Errorf("NodeByKey(c) = %+v, want id 1", n)
	}
	if _, ok := g.NodeByKey("b"); ok {
		t.Error("deleted key still present after seal")
	}
}

func TestExportSnapshotCompactsTombstones(t *testing.T) {
	g := mustBuild(t)
	if _, err := g.DeleteEdge("gateway", "auth", "CALLS"); err != nil {
		t.Fatal(err)
	}
	data, err := g.ExportSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	g2, err := vectra.ImportSnapshot(data)
	if err != nil {
		t.Fatal(err)
	}
	if g2.NodeCount() != 4 || g2.EdgeCount() != 3 {
		t.Fatalf("imported = %d/%d, want 4/3", g2.NodeCount(), g2.EdgeCount())
	}
	if err := g2.Seal(); err != nil {
		t.Fatal(err)
	}
	ns, err := g2.NeighborsOf("gateway", vectra.DirOut, "CALLS")
	if err != nil {
		t.Fatal(err)
	}
	if len(ns) != 1 || ns[0].Key != "ledger" {
		t.Errorf("neighbors = %+v, want only ledger", ns)
	}
}

func TestClonePreservesTombstoneState(t *testing.T) {
	g := mustBuild(t)
	if _, err := g.DeleteNode("gateway"); err != nil {
		t.Fatal(err)
	}
	c := g.Clone()
	if c.NodeCount() != 3 || c.EdgeCount() != 2 {
		t.Fatalf("clone counts = %d/%d, want 3/2", c.NodeCount(), c.EdgeCount())
	}
	if _, err := c.DeleteNode("auth"); err != nil {
		t.Fatal(err) // deletes keep working on the clone (cascades auth->db)
	}
	if err := c.Seal(); err != nil {
		t.Fatal(err)
	}
	st := c.Stats()
	if st.Nodes != 2 || st.Edges != 1 { // ledger->db survives
		t.Errorf("clone stats = %+v, want 2/1", st)
	}
}
