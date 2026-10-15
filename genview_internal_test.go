package vectra

// White-box tests for the micro-layer generation machinery: layer
// granularity per write/batch boundary, WAL-frame ↔ layer alignment, and
// the ratchet/mirror invariants the black-box tests cannot see directly.

import (
	"path/filepath"
	"testing"
)

func layerCount(g *Graph) int { return len(g.view.Load().layers) }

// TestLayerPerBoundary: one micro-layer per single write and per Batch
// boundary; empty batches publish nothing; a merge empties the stack.
func TestLayerPerBoundary(t *testing.T) {
	g := New()
	if layerCount(g) != 0 {
		t.Fatalf("fresh graph layers = %d, want 0", layerCount(g))
	}
	_, _ = g.AddNode("a", "S", nil)
	_, _ = g.AddNode("b", "S", nil)
	if n := layerCount(g); n != 2 {
		t.Fatalf("after 2 single writes layers = %d, want 2", n)
	}
	_ = g.Batch(func(g *Graph) {
		_, _ = g.AddEdge("a", "b", "E", nil)
		_, _ = g.AddEdge("a", "b", "E", nil)
		_, _ = g.UpsertEdge("a", "b", "E", nil)
	})
	if n := layerCount(g); n != 3 {
		t.Fatalf("after Batch layers = %d, want 3 (one layer per boundary)", n)
	}
	if g.EdgeCount() != 1 {
		t.Fatalf("fold inside batch: edges = %d, want 1", g.EdgeCount())
	}
	_ = g.Batch(func(g *Graph) {}) // empty boundary: no junk layer
	if n := layerCount(g); n != 3 {
		t.Fatalf("empty Batch added a layer: %d", n)
	}
	g.IndexProperty("k") // declaration-only layer
	if n := layerCount(g); n != 4 {
		t.Fatalf("IndexProperty must publish a (pending) layer: %d", n)
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	if n := layerCount(g); n != 0 {
		t.Fatalf("after Seal layers = %d, want 0", n)
	}
	if !g.Sealed() {
		t.Fatal("Sealed() must be true after merge")
	}
}

// TestBatchOneFrameOneLayer: with a WAL attached, a kind-ordered Batch is
// exactly one frame AND one layer (spec §2.3: 微层物化与 WAL 帧一对一);
// a single write outside Batch is its own frame and layer.
func TestBatchOneFrameOneLayer(t *testing.T) {
	db := filepath.Join(t.TempDir(), "graph.db")
	g, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	if _, err := g.AddNode("s", "S", nil); err != nil {
		t.Fatal(err)
	}
	if n := layerCount(g); n != 1 {
		t.Fatalf("single write layers = %d, want 1", n)
	}
	if lsn := g.view.Load().lsn; lsn != 1 {
		t.Fatalf("layer lsn = %d, want 1 (frame lsn)", lsn)
	}
	if err := g.Batch(func(g *Graph) {
		_, _ = g.AddNode("x", "S", nil)
		_, _ = g.AddNode("y", "S", nil)
		_, _ = g.UpsertEdge("x", "y", "U", nil)
	}); err != nil {
		t.Fatal(err)
	}
	if n := layerCount(g); n != 2 {
		t.Fatalf("batch layers = %d, want 2 (one layer for the whole batch)", n)
	}
	if lsn := g.view.Load().lsn; lsn != 2 {
		t.Fatalf("layer lsn = %d, want 2 (one frame per batch)", lsn)
	}
}

// TestRatchetMirrorsInvariant: the writer mirrors must always agree with a
// full three-source walk of the published generation (visible counts and
// per-key winner ids).
func TestRatchetMirrorsInvariant(t *testing.T) {
	g := New()
	_, _ = g.AddNode("a", "S", nil)
	_, _ = g.AddNode("b", "S", nil)
	_, _ = g.AddEdge("a", "b", "E", nil)    // id 0
	_, _ = g.UpsertEdge("a", "b", "E", nil) // transfer id 0
	if g.nextEdge != 1 || g.nextNode != 2 {
		t.Fatalf("ratchets = %d/%d, want 1/2 (transfer must not consume)", g.nextEdge, g.nextNode)
	}
	_, _ = g.DeleteEdge("a", "b", "E")
	_, _ = g.UpsertEdge("a", "b", "E", nil) // fresh id after delete
	if g.nextEdge != 2 {
		t.Fatalf("nextEdge = %d, want 2", g.nextEdge)
	}
	if g.nodeTotal != NodeCountOf(g) || g.edgeTotal != g.view.Load().edges {
		t.Fatalf("mirror totals %d/%d vs view %d/%d", g.nodeTotal, g.edgeTotal, g.view.Load().nodes, g.view.Load().edges)
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	if g.nextNode != NodeID(g.view.Load().base.NodeCount()) || g.nextEdge != EdgeID(g.view.Load().base.EdgeCount()) {
		t.Fatalf("ratchets must reset to the merged base size: %d/%d", g.nextNode, g.nextEdge)
	}
}

func NodeCountOf(g *Graph) int { return g.NodeCount() }
