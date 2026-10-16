package vectra

// White-box tests for the micro-layer generation machinery: layer
// granularity per window/batch boundary (the direct-mode absorption window
// of the v0.7 hardening pass), WAL-frame ↔ layer alignment, and the
// ratchet/mirror invariants the black-box tests cannot see directly.

import (
	"path/filepath"
	"testing"
)

func layerCount(g *Graph) int { return len(g.view.Load().layers) }

// TestLayerPerWindow: unbatched direct writes absorb into one open window —
// a burst with no interleaved query materializes as ONE layer at the next
// read (finishOp no longer publishes per op); a Batch boundary publishes its
// own layer; empty batches publish nothing; a merge empties the stack.
func TestLayerPerBoundary(t *testing.T) {
	g := New()
	if layerCount(g) != 0 {
		t.Fatalf("fresh graph layers = %d, want 0", layerCount(g))
	}
	_, _ = g.AddNode("a", "S", nil)
	_, _ = g.AddNode("b", "S", nil)
	if layerCount(g) != 0 {
		t.Fatalf("mid-burst layers = %d, want 0 (absorbed, no query since the writes)", layerCount(g))
	}
	if g.NodeCount() != 2 { // the read materializes the window
		t.Fatalf("NodeCount = %d, want 2", g.NodeCount())
	}
	if n := layerCount(g); n != 1 {
		t.Fatalf("after 2 absorbed writes + read layers = %d, want 1", n)
	}
	_ = g.Batch(func(g *Graph) {
		_, _ = g.AddEdge("a", "b", "E", nil)
		_, _ = g.AddEdge("a", "b", "E", nil)
		_, _ = g.UpsertEdge("a", "b", "E", nil)
	})
	if n := layerCount(g); n != 2 {
		t.Fatalf("after Batch layers = %d, want 2 (one layer per boundary)", n)
	}
	if g.EdgeCount() != 1 {
		t.Fatalf("fold inside batch: edges = %d, want 1", g.EdgeCount())
	}
	_ = g.Batch(func(g *Graph) {}) // empty boundary: no junk layer
	if n := layerCount(g); n != 2 {
		t.Fatalf("empty Batch added a layer: %d", n)
	}
	g.IndexProperty("k") // declaration absorbed with the next window
	if layerCount(g) != 2 {
		t.Fatalf("IndexProperty must absorb (no per-declaration layer): %d", layerCount(g))
	}
	if keys := g.IndexedPropertyKeys(); len(keys) != 1 { // the read materializes it
		t.Fatalf("IndexedPropertyKeys = %v, want [k]", keys)
	}
	if n := layerCount(g); n != 3 {
		t.Fatalf("after IndexProperty + read layers = %d, want 3 (declaration layer)", n)
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
// exactly one frame AND one layer; unbatched writes still record one frame
// per op (SyncAlways durability is per-op) while the layer side absorbs —
// the layer's lsn is the greatest frame lsn it covers.
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
	if _, err := g.AddNode("t", "S", nil); err != nil {
		t.Fatal(err)
	}
	if layerCount(g) != 0 {
		t.Fatalf("absorbed writes must not materialize per op: %d layers", layerCount(g))
	}
	if g.NodeCount() != 2 { // flush
		t.Fatalf("NodeCount = %d, want 2", g.NodeCount())
	}
	if n := layerCount(g); n != 1 {
		t.Fatalf("absorbed 2 per-op frames into %d layers, want 1", n)
	}
	if lsn := g.view.Load().lsn; lsn != 2 {
		t.Fatalf("layer lsn = %d, want 2 (greatest covered frame lsn)", lsn)
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
	if lsn := g.view.Load().lsn; lsn != 3 {
		t.Fatalf("layer lsn = %d, want 3 (one frame per batch)", lsn)
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
