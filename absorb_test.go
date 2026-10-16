package vectra_test

// Black-box tests for the direct-mode write absorption window (v0.7
// hardening, task 7 / acceptance A4): unbatched per-op writes accumulate in
// one open builder ("pend") and materialize as ONE micro-layer per flush.
// Flushes fire at read-API entries (write-then-read stays immediate), at
// Batch/Seal/Close boundaries, and at the capacity cap. Wrapper mode never
// opens a window and wrapper-mode reads skip flushing (the readers are not
// the writer).

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/projanvil/vectra"
	"github.com/projanvil/vectra/internal/writer"
)

// TestAbsorptionOneLayerPerWindow: a burst of unbatched direct writes with no
// interleaved query materializes as a single layer at the next read — the
// per-op layer cost is gone (A4's root cause).
func TestAbsorptionOneLayerPerWindow(t *testing.T) {
	g := vectra.New()
	for i := 0; i < 50; i++ {
		_, _ = g.AddNode(fmt.Sprintf("n%02d", i), "S", nil)
	}
	for i := 0; i < 50; i++ {
		_, _ = g.AddEdge(fmt.Sprintf("n%02d", i), fmt.Sprintf("n%02d", (i+1)%50), "E", nil)
	}
	if n := g.NodeCount(); n != 50 { // the read entry flushes the window
		t.Fatalf("NodeCount = %d, want 50", n)
	}
	layers, layerEdges := g.LayerPressure()
	if layers != 1 {
		t.Fatalf("100 unbatched writes materialized %d layers, want 1 (absorbed into one window)", layers)
	}
	if layerEdges != 50 {
		t.Fatalf("layer edge records = %d, want 50", layerEdges)
	}
}

// TestAbsorptionCapBoundsWindow: bursts beyond a capacity cap flush
// mid-burst — the window's memory stays bounded even with no reads at all.
// Edge-appending bursts are bounded by the 64K-edge cap; every other op by
// the 4096-op cap.
func TestAbsorptionCapBoundsWindow(t *testing.T) {
	t.Run("edge cap", func(t *testing.T) {
		g := vectra.New()
		_, _ = g.AddNode("root", "S", nil)
		const ops = 150_000 // > 2x the 64K-edge cap
		for i := 0; i < ops; i++ {
			_, _ = g.AddEdge("root", "root", "E", nil) // self-loops: no other nodes needed
		}
		if n := g.EdgeCount(); n != ops { // flush
			t.Fatalf("edges = %d, want %d", n, ops)
		}
		layers, _ := g.LayerPressure()
		if layers < 2 || layers > 4 {
			t.Fatalf("%d unbatched AddEdge left %d layers, want 2-4 (cap flush every 64K edges)", ops, layers)
		}
	})
	t.Run("op cap", func(t *testing.T) {
		g := vectra.New()
		const ops = 15000 // > 3x the 4096-op cap
		for i := 0; i < ops; i++ {
			_, _ = g.AddNode(fmt.Sprintf("n%05d", i), "S", nil)
		}
		if n := g.NodeCount(); n != ops { // flush
			t.Fatalf("nodes = %d, want %d", n, ops)
		}
		layers, _ := g.LayerPressure()
		if layers < 3 || layers > 5 {
			t.Fatalf("%d unbatched AddNode left %d layers, want 3-5 (cap flush every 4096 ops)", ops, layers)
		}
	})
}

// TestAbsorptionWriteThenReadEveryFamily: write-then-read stays immediate for
// every query family — the read entry materializes the caller's window
// before answering (direct mode: the reader IS the writer).
func TestAbsorptionWriteThenReadEveryFamily(t *testing.T) {
	g := vectra.New()
	a, _ := g.AddNode("a", "S", map[string]any{"env": "prod"})
	b, _ := g.AddNode("b", "S", nil)
	if n, ok := g.NodeByKey("a"); !ok || n.ID != a || n.Props["env"] != "prod" {
		t.Fatalf("NodeByKey after unbatched write: %v %v", n, ok)
	}
	if n, ok := g.NodeByID(b); !ok || n.Key != "b" {
		t.Fatalf("NodeByID after unbatched write: %v %v", n, ok)
	}
	eid, err := g.AddEdge("a", "b", "E", map[string]any{"w": 1})
	if err != nil {
		t.Fatal(err)
	}
	if e, ok := g.EdgeByID(eid); !ok || e.Src != a || e.Dst != b || e.Props["w"] != 1 {
		t.Fatalf("EdgeByID after unbatched write: %v %v", e, ok)
	}
	if es, err := g.EdgesOf("a", vectra.DirOut); err != nil || len(es) != 1 {
		t.Fatalf("EdgesOf after unbatched write: %v (err %v)", es, err)
	}
	if ns, err := g.NeighborsOf("a", vectra.DirOut); err != nil || len(ns) != 1 || ns[0].Key != "b" {
		t.Fatalf("NeighborsOf after unbatched write: %v (err %v)", ns, err)
	}
	res, err := g.BFS([]vectra.NodeID{a}, vectra.WithMaxDepth(2))
	if err != nil || res.Count() != 2 {
		t.Fatalf("BFS after unbatched write: count=%d err=%v", res.Count(), err)
	}
	if _, found, err := g.ShortestPathIDs(a, b); err != nil || !found {
		t.Fatalf("ShortestPath after unbatched write: found=%v err=%v", found, err)
	}
	if ids, err := g.ReachableIDs(a); err != nil || len(ids) != 2 {
		t.Fatalf("Reachable after unbatched write: %v (err %v)", ids, err)
	}
	if r, err := g.Cypher(`MATCH (x:S)-[:E]->(y) RETURN y.key AS k`); err != nil || len(r.Rows) != 1 {
		t.Fatalf("Cypher after unbatched write: %v (err %v)", r, err)
	}
	if n, e := g.NodeCount(), g.EdgeCount(); n != 2 || e != 1 {
		t.Fatalf("counts after unbatched writes: %d/%d, want 2/1", n, e)
	}
	if st := g.Stats(); st.Nodes != 2 || st.Edges != 1 {
		t.Fatalf("Stats after unbatched writes: %+v", st)
	}
	if g.Sealed() {
		t.Fatal("Sealed() must be false with an unmerged (absorbed) layer")
	}
	// pending index declarations answer via line scan before the merge
	g.IndexProperty("env")
	if ns, ok := g.LookupByProperty("env", "prod"); !ok || len(ns) != 1 || ns[0].Key != "a" {
		t.Fatalf("LookupByProperty over absorbed declaration: %v %v", ns, ok)
	}
	if keys := g.IndexedPropertyKeys(); len(keys) != 1 || keys[0] != "env" {
		t.Fatalf("IndexedPropertyKeys over absorbed declaration: %v", keys)
	}
	// clone/export see the absorbed writes too (same write-then-read contract)
	c := g.Clone()
	if c.NodeCount() != 2 || c.EdgeCount() != 1 {
		t.Fatalf("Clone after unbatched writes: %d/%d", c.NodeCount(), c.EdgeCount())
	}
	var buf bytes.Buffer
	if err := g.Save(&buf); err != nil {
		t.Fatal(err)
	}
	lg, err := vectra.Load(bytes.NewReader(buf.Bytes()))
	if err != nil || lg.EdgeCount() != 1 {
		t.Fatalf("Save/Load after unbatched writes: %v (err %v)", lg, err)
	}
}

// TestAbsorptionSealAndCloseFlush: Seal, Close and revive-style continued
// writing all materialize the window at their boundaries.
func TestAbsorptionSealAndCloseFlush(t *testing.T) {
	g := vectra.New()
	_, _ = g.AddNode("a", "S", nil)
	_, _ = g.AddNode("b", "S", nil)
	_, _ = g.AddEdge("a", "b", "E", nil)
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	if !g.Sealed() || g.EdgeCount() != 1 {
		t.Fatalf("Seal must fold the absorbed window: sealed=%v edges=%d", g.Sealed(), g.EdgeCount())
	}
	// writes after Seal open a new window (new generation), closed by Close
	_, _ = g.AddNode("c", "S", nil)
	_, _ = g.AddEdge("b", "c", "E", nil)
	if err := g.Close(); err != nil { // WAL-off Close: still publishes the window
		t.Fatal(err)
	}
	if n, e := g.NodeCount(), g.EdgeCount(); n != 3 || e != 2 {
		t.Fatalf("post-Close counts %d/%d, want 3/2 (Close must flush the window)", n, e)
	}
}

// TestAbsorptionWALFramesStayPerOp: with a WAL attached, unbatched writes
// still record one frame per op (SyncAlways durability contract — the window
// absorbs layer materialization, never the log), and the absorbed layer's
// lsn covers every frame it contains.
func TestAbsorptionWALFramesStayPerOp(t *testing.T) {
	g, err := vectra.Open(t.TempDir()+"/graph.db", vectra.Options{WALMode: vectra.ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = g.AddNode("a", "S", nil)
	_, _ = g.AddNode("b", "S", nil)
	_, _ = g.AddEdge("a", "b", "E", nil)
	if n := g.NodeCount(); n != 2 { // flush: 3 frames -> 1 layer
		t.Fatalf("NodeCount = %d, want 2", n)
	}
	if layers, _ := g.LayerPressure(); layers != 1 {
		t.Fatalf("absorbed 3 WAL frames into %d layers, want 1", layers)
	}
	if size := g.WALSize(); size == 0 {
		t.Fatal("per-op frames must be logged eagerly (absorption is layer-side only)")
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestAbsorptionWrapperModeSafety: wrapper mode never opens a window (the
// writer's writes all run inside Batch boundaries) and wrapper-mode reads
// skip flushing — concurrent readers observe only published generations
// while the writer batches, merges (MergeDelay widens the window) and
// checkpoints. -race guard for the whole protocol.
func TestAbsorptionWrapperModeSafety(t *testing.T) {
	g := vectra.New()
	_ = g.Batch(func(g *vectra.Graph) {
		for i := 0; i < 8; i++ {
			_, _ = g.AddNode(fmt.Sprintf("n%d", i), "S", nil)
		}
	})
	h := writer.Attach(g, writer.Config{MaxLayers: 4, MergeEdges: 8, MergeDelay: 20 * time.Millisecond})

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				// readers never see a partial changeset: node i and its
				// edge appear together (batch atomicity) — key k only
				// becomes live via a whole Submit
				k := fmt.Sprintf("w%d-%d", r, i%200)
				if n, ok := g.NodeByKey(k); ok {
					if es, err := g.EdgesOf(k, vectra.DirOut); err != nil || len(es) < 1 {
						t.Errorf("EdgesOf(%s) = %v (err %v) — node visible without its edge", k, es, err)
						return
					}
					_ = n
				}
				_, _ = g.BFS([]vectra.NodeID{0}, vectra.WithMaxDepth(1))
				i++
			}
		}(r)
	}
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("w%d-%d", i%3, i)
		cs := vectra.Changeset{
			Nodes: []vectra.NodeUpsert{{Key: k, Label: "W"}},
			Edges: []vectra.EdgeUpsert{{SrcKey: k, DstKey: fmt.Sprintf("n%d", i%8), Label: "E"}},
		}
		if err := h.Submit(cs); err != nil {
			t.Fatalf("Submit: %v", err)
		}
		// write-then-read through the wrapper: visible right after the reply
		if _, ok := g.NodeByKey(k); !ok {
			t.Fatalf("key %s invisible after Submit reply", k)
		}
	}
	close(stop)
	wg.Wait()
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	if n := g.NodeCount(); n != 8+200 {
		t.Fatalf("final nodes = %d, want 208", n)
	}
}

// TestAbsorptionConcurrentReadersSeePublishedOnly: direct-mode writer with
// concurrent reader goroutines — readers never touch the open window (they
// are not the writer), they keep reading the last published generation
// without error, and the writer's own post-write reads see everything
// (-race guard for the owner-token guard).
func TestAbsorptionConcurrentReadersSeePublishedOnly(t *testing.T) {
	g := vectra.New()
	_, _ = g.AddNode("seed", "S", nil)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, _ = g.NodeByKey("seed")
				_, _ = g.BFS([]vectra.NodeID{0}, vectra.WithMaxDepth(1))
				_ = g.NodeCount()
			}
		}()
	}
	for i := 0; i < 500; i++ {
		k := fmt.Sprintf("w%d", i)
		_, _ = g.AddNode(k, "S", nil) // absorbed: no publication per op
		_, _ = g.AddEdge("seed", k, "E", nil)
		if i%50 == 0 {
			// the writer's own read flushes the window mid-burst
			if n := g.NodeCount(); n < i {
				t.Fatalf("NodeCount = %d mid-burst, want >= %d", n, i)
			}
		}
	}
	close(stop)
	wg.Wait()
	if n, e := g.NodeCount(), g.EdgeCount(); n != 501 || e != 500 {
		t.Fatalf("final counts %d/%d, want 501/500", n, e)
	}
}

// TestAbsorptionIDContractStable: write methods keep returning the ID they
// allocated at write time (IDs come from the ratchet at write time; the
// window only defers layer materialization).
func TestAbsorptionIDContractStable(t *testing.T) {
	g := vectra.New()
	a, _ := g.AddNode("a", "S", nil)
	_, _ = g.AddNode("b", "S", nil)
	e1, _ := g.AddEdge("a", "b", "E", nil)
	e2, _ := g.UpsertEdge("a", "b", "E", map[string]any{"w": 2}) // fold keeps e1's ID
	if e2 != e1 {
		t.Fatalf("UpsertEdge folded to %d, want %d (keeps the visible edge's ID)", e2, e1)
	}
	if n, _ := g.NodeByKey("a"); n.ID != a {
		t.Fatalf("AddNode ID drifted: %d vs %d", n.ID, a)
	}
	_, _ = g.DeleteEdge("a", "b", "E")
	e3, _ := g.UpsertEdge("a", "b", "E", nil) // fresh ID after delete
	if e3 == e1 {
		t.Fatal("UpsertEdge after delete must draw a fresh ID")
	}
}

// TestAbsorptionAttachFlushesWindow: attaching the wrapper materializes the
// caller's open window — the writes made just before Attach stay visible to
// reads (wrapper-mode reads never flush, and the writer only publishes at
// batch boundaries).
func TestAbsorptionAttachFlushesWindow(t *testing.T) {
	g := vectra.New()
	_, _ = g.AddNode("a", "S", nil)
	_, _ = g.AddNode("b", "S", nil)
	_, _ = g.AddEdge("a", "b", "E", nil) // absorbed, no read since the writes
	h := writer.Attach(g, writer.Config{})
	if n, e := g.NodeCount(), g.EdgeCount(); n != 2 || e != 1 {
		t.Fatalf("post-Attach counts %d/%d, want 2/1 (Attach must flush the caller's window)", n, e)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
}
