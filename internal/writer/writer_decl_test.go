package writer_test

// Task 11 tests: vector index declaration/drop through the writer lane
// (ruling #8). Attach claims the graph, so the app goroutine has no legal
// direct declaration entry (CreateVectorIndex from it hits ErrNotWriter in
// debug builds); the Handle's declare/drop ride writeCh with changesets and
// the writer goroutine applies them inside its batch step, OUTSIDE g.Batch
// (the decl APIs refuse mid-batch). Same lane = total order: a Submit
// queued after a Declare returns only with the index installed, and one
// queued after a Drop applies with the index already gone.

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/projanvil/vectra"
	"github.com/projanvil/vectra/internal/writer"
)

func vecDecl() vectra.VectorIndexDecl {
	return vectra.VectorIndexDecl{Name: "emb", Prop: "emb", Dim: 4, Metric: "cosine"}
}

func vecCS(key string, v []float32) vectra.Changeset {
	return vectra.Changeset{Nodes: []vectra.NodeUpsert{{
		Key: key, Label: "m", Props: map[string]any{"emb": v},
	}}}
}

func hitKeys(hits []vectra.VectorHit) []string {
	keys := make([]string, 0, len(hits))
	for _, h := range hits {
		keys = append(keys, h.Key)
	}
	return keys
}

func contains(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

// ---- 1. declare via the Handle, then submit vectors and hit ---------------------

func TestDeclareViaHandle(t *testing.T) {
	g := vectra.New()
	h := writer.Attach(g, writer.Config{MaxLayers: 64, MergeEdges: 1 << 30})
	defer func() { _ = h.Close() }()

	// The illegal surface this task closes: direct declaration from the app
	// goroutine while attached (debug builds reject with ErrNotWriter).
	// The Submit first synchronizes on the writer loop being up and the
	// claim recorded (same discipline as TestErrNotWriter).
	if err := h.Submit(nodeCS("w0", "W", nil)); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := g.CreateVectorIndex(vecDecl()); !errors.Is(err, vectra.ErrNotWriter) {
		t.Fatalf("direct CreateVectorIndex while attached = %v, want ErrNotWriter", err)
	}

	if err := h.DeclareVectorIndex(vecDecl()); err != nil {
		t.Fatalf("DeclareVectorIndex: %v", err)
	}
	if got := g.VectorIndexes(); len(got) != 1 || got[0].Name != "emb" || got[0].Dim != 4 {
		t.Fatalf("VectorIndexes = %+v, want one emb dim=4 entry", got)
	}
	// A duplicate name surfaces through the reply, like any Submit error.
	if err := h.DeclareVectorIndex(vecDecl()); !errors.Is(err, vectra.ErrVectorIndexExists) {
		t.Fatalf("duplicate declare = %v, want ErrVectorIndexExists", err)
	}

	// Same-lane ordering: Submits queued after the declare ingest. n00
	// carries the query vector exactly (cosine 1) and the rest are
	// orthogonal, so the top hit must be n00 once Submit returns.
	q := []float32{1, 0, 0, 0}
	var cs vectra.Changeset
	cs.Nodes = append(cs.Nodes, vectra.NodeUpsert{Key: "n00", Label: "m", Props: map[string]any{"emb": q}})
	for i := 1; i < 32; i++ {
		v := []float32{0, float32(i), 0, 0}
		cs.Nodes = append(cs.Nodes, vectra.NodeUpsert{
			Key: fmt.Sprintf("n%02d", i), Label: "m", Props: map[string]any{"emb": v},
		})
	}
	if err := h.Submit(cs); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	hits, err := g.QueryVector("emb", q, vectra.QueryVectorOptions{K: 3})
	if err != nil {
		t.Fatalf("QueryVector: %v", err)
	}
	if len(hits) == 0 || hits[0].Key != "n00" {
		t.Fatalf("top hit = %v, want n00 (declare-then-submit must ingest)", hitKeys(hits))
	}
}

// ---- 2. drop ordering: not-found, no intake, re-declare fresh -------------------

func TestDeclareDropOrdering(t *testing.T) {
	g := vectra.New()
	h := writer.Attach(g, writer.Config{MaxLayers: 64, MergeEdges: 1 << 30})

	if err := h.DeclareVectorIndex(vecDecl()); err != nil {
		t.Fatalf("declare: %v", err)
	}
	q := []float32{1, 0, 0, 0}
	if err := h.Submit(vecCS("first", q)); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	hits, err := g.QueryVector("emb", q, vectra.QueryVectorOptions{K: 1})
	if err != nil {
		t.Fatalf("QueryVector: %v", err)
	}
	if len(hits) == 0 || hits[0].Key != "first" {
		t.Fatalf("top hit = %v, want first", hitKeys(hits))
	}

	// Drop of an unknown name errors through the reply.
	if err := h.DropVectorIndex("nope"); !errors.Is(err, vectra.ErrVectorIndexNotFound) {
		t.Fatalf("drop unknown = %v, want ErrVectorIndexNotFound", err)
	}
	if err := h.DropVectorIndex("emb"); err != nil {
		t.Fatalf("DropVectorIndex: %v", err)
	}
	// Queries report not-found immediately (Submit-order is total, and the
	// drop's own reply already waited for the writer).
	if _, err := g.QueryVector("emb", q, vectra.QueryVectorOptions{K: 1}); !errors.Is(err, vectra.ErrVectorIndexNotFound) {
		t.Fatalf("query after drop = %v, want ErrVectorIndexNotFound", err)
	}
	// Vector Submits queued after the drop still apply — just unindexed.
	if err := h.Submit(vecCS("after", q)); err != nil {
		t.Fatalf("Submit after drop: %v", err)
	}
	if _, ok := g.NodeByKey("after"); !ok {
		t.Fatal("node submitted after the drop is missing")
	}

	// Re-declare rebuilds fresh: the backfill re-ingests existing nodes
	// ("first" and the post-drop "after"), and later Submits keep ingesting.
	if err := h.DeclareVectorIndex(vecDecl()); err != nil {
		t.Fatalf("re-declare: %v", err)
	}
	hits, err = g.QueryVector("emb", q, vectra.QueryVectorOptions{K: 10})
	if err != nil {
		t.Fatalf("QueryVector after re-declare: %v", err)
	}
	keys := hitKeys(hits)
	if !contains(keys, "first") || !contains(keys, "after") {
		t.Fatalf("re-declared index = %v, want first and after backfilled", keys)
	}
	if err := h.Submit(vecCS("fresh", []float32{0, 9, 0, 0})); err != nil {
		t.Fatalf("Submit after re-declare: %v", err)
	}
	hits, err = g.QueryVector("emb", []float32{0, 9, 0, 0}, vectra.QueryVectorOptions{K: 1})
	if err != nil {
		t.Fatalf("QueryVector after re-declare submit: %v", err)
	}
	if len(hits) == 0 || hits[0].Key != "fresh" {
		t.Fatalf("top hit = %v, want fresh", hitKeys(hits))
	}

	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Closed-handle behavior mirrors Submit's: ErrClosed, nothing queued.
	if err := h.DeclareVectorIndex(vecDecl()); !errors.Is(err, vectra.ErrClosed) {
		t.Fatalf("declare after Close = %v, want ErrClosed", err)
	}
	if err := h.DropVectorIndex("emb"); !errors.Is(err, vectra.ErrClosed) {
		t.Fatalf("drop after Close = %v, want ErrClosed", err)
	}
}

// ---- 3. WAL round-trip: the declaration frames logged through the lane ----------

func TestDeclareSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "graph.db")
	g, err := vectra.Open(path, vectra.Options{WALMode: vectra.ModeInterval})
	if err != nil {
		t.Fatal(err)
	}
	h := writer.Attach(g, writer.Config{})

	q := []float32{1, 0, 0, 0}
	if err := h.DeclareVectorIndex(vecDecl()); err != nil {
		t.Fatal(err)
	}
	if err := h.Submit(vecCS("v1", q)); err != nil {
		t.Fatal(err)
	}
	// A full declare/ingest/drop/re-declare cycle must leave the surviving
	// declaration and its WAL frames consistent for replay.
	if err := h.DropVectorIndex("emb"); err != nil {
		t.Fatal(err)
	}
	if err := h.DeclareVectorIndex(vecDecl()); err != nil {
		t.Fatal(err)
	}
	if err := h.Submit(vecCS("v2", []float32{0, 2, 0, 0})); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	g2, err := vectra.Open(path, vectra.Options{WALMode: vectra.ModeInterval})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer g2.Close()
	if got := g2.VectorIndexes(); len(got) != 1 || got[0].Name != "emb" || got[0].Dim != 4 {
		t.Fatalf("reopened VectorIndexes = %+v, want one emb dim=4 entry", got)
	}
	// The re-declared index backfilled v1 at its frame position and v2
	// ingested after it: both must query.
	for _, tc := range []struct {
		key string
		vec []float32
	}{
		{"v1", q},
		{"v2", []float32{0, 2, 0, 0}},
	} {
		hits, err := g2.QueryVector("emb", tc.vec, vectra.QueryVectorOptions{K: 1})
		if err != nil {
			t.Fatalf("reopen QueryVector(%s): %v", tc.key, err)
		}
		if len(hits) == 0 || hits[0].Key != tc.key {
			t.Fatalf("reopen top hit for %s = %v", tc.key, hitKeys(hits))
		}
	}
}

// ---- 4. concurrent Submits + declare/drop: no deadlock under -race --------------

func TestConcurrentDeclareAndSubmit(t *testing.T) {
	g := vectra.New()
	h := writer.Attach(g, writer.Config{MaxLayers: 64, MergeEdges: 1 << 30})
	defer func() { _ = h.Close() }()

	const submitters = 8
	const perSub = 64
	var wg sync.WaitGroup
	wg.Add(submitters + 1)
	for s := 0; s < submitters; s++ {
		s := s
		go func() {
			defer wg.Done()
			for i := 0; i < perSub; i++ {
				v := []float32{float32(s), float32(i), 0, 0}
				if err := h.Submit(vecCS(fmt.Sprintf("n%d-%d", s, i), v)); err != nil {
					t.Errorf("Submit: %v", err)
					return
				}
			}
		}()
	}
	// Declarations race the Submits down the same lane: "keep" survives for
	// the final query, "temp" is declared and dropped while writes land.
	go func() {
		defer wg.Done()
		if err := h.DeclareVectorIndex(vecDecl()); err != nil {
			t.Errorf("declare: %v", err)
		}
		if err := h.DeclareVectorIndex(vectra.VectorIndexDecl{
			Name: "temp", Prop: "emb", Dim: 4, Metric: "l2",
		}); err != nil {
			t.Errorf("declare temp: %v", err)
		}
		if err := h.DropVectorIndex("temp"); err != nil {
			t.Errorf("drop temp: %v", err)
		}
	}()
	wg.Wait()

	if got := len(g.VectorIndexes()); got != 1 {
		t.Fatalf("VectorIndexes after race = %d entries, want 1 (keep)", got)
	}
	// 512 vectors fill overlay chunks (pack boundary), so the search runs
	// the approximate segment path: assert membership in the top-10, not
	// top-1 (spec D8 — results are approximate).
	hits, err := g.QueryVector("emb", []float32{3, 10, 0, 0}, vectra.QueryVectorOptions{K: 10})
	if err != nil {
		t.Fatalf("QueryVector: %v", err)
	}
	if !contains(hitKeys(hits), "n3-10") {
		t.Fatalf("top-10 = %v, want n3-10 among them", hitKeys(hits))
	}
}
