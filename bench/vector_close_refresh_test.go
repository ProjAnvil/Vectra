package bench

// Close-shaped forest refresh (2026-10-24 fix arc, spec item 1): the
// behavior-level pair for the 1M finding B1 - these live in bench because
// wrapper mode needs internal/writer, which package vectra's tests cannot
// import (cycle). The drift counter itself is white-box tested in package
// vectra (TestVecPackDriftTracking).

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/projanvil/vectra"
	"github.com/projanvil/vectra/internal/writer"
)

// closeRefreshVec is injective in i (a seeded generator; the arithmetic
// fixture collides with period 31 and self-query ties broke the probe).
func closeRefreshVec(i int) []float32 {
	rng := rand.New(rand.NewSource(int64(i)))
	v := make([]float32, 8)
	for j := range v {
		v[j] = rng.Float32()
	}
	return v
}

func closeRefreshSubmit(h *writer.Handle, lo, hi int) {
	cs := vectra.Changeset{Nodes: make([]vectra.NodeUpsert, hi-lo)}
	for i := lo; i < hi; i++ {
		cs.Nodes[i-lo] = vectra.NodeUpsert{Key: fmt.Sprintf("n%06d", i), Label: "m", Props: map[string]any{"emb": closeRefreshVec(i)}}
	}
	if err := h.Submit(cs); err != nil {
		panic(err) // test-fixture helper; callers assert the behavior that matters
	}
}

func closeRefreshWaitIdle(t *testing.T, g *vectra.Graph) {
	t.Helper()
	dl := time.Now().Add(30 * time.Second)
	for g.VectorOverlayLen("mem") >= 2048 {
		if time.Now().After(dl) {
			t.Fatalf("vector lane did not converge: overlay=%d", g.VectorOverlayLen("mem"))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestWrapperClosePreservesPackWork is the 1M finding B1 at bench scale:
// a wrapper session that converges its vector lane and ends with a LIGHT
// close (v6 base present, retained under the compaction threshold) must
// still persist the packs done since the last forest write - otherwise
// reopen restores the stale forest's frozen overlay and re-pays the pack.
// Fixed by the close-shaped forest refresh (spec 2026-10-24 item 1).
func TestWrapperClosePreservesPackWork(t *testing.T) {
	dir := t.TempDir()
	g, err := vectra.Open(dir+"/v.db", vectra.Options{WALMode: vectra.ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.CreateVectorIndex(vectra.VectorIndexDecl{Name: "mem", Prop: "emb", Dim: 8, Metric: "cosine"}); err != nil {
		t.Fatal(err)
	}
	h := writer.Attach(g, writer.Config{})
	submit := func(h *writer.Handle, lo, hi int) {
		t.Helper()
		cs := vectra.Changeset{Nodes: make([]vectra.NodeUpsert, hi-lo)}
		for i := lo; i < hi; i++ {
			cs.Nodes[i-lo] = vectra.NodeUpsert{Key: fmt.Sprintf("n%06d", i), Label: "m", Props: map[string]any{"emb": closeRefreshVec(i)}}
		}
		if err := h.Submit(cs); err != nil {
			t.Fatal(err)
		}
	}

	// Session 1: 12k vectors, converge, close. The lineage's first
	// checkpoint compacts, so the forest here is fresh - session 2 is the
	// B1 shape.
	submit(h, 0, 12000)
	closeRefreshWaitIdle(t, g)
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}

	// Session 2: 6k more vectors (packs happen - drift), converge, close.
	// Retained bytes stay far under the threshold, so both checkpoints of
	// this session are LIGHT: without the refresh, the forest on disk is
	// session 1's.
	g2, err := vectra.Open(dir+"/v.db", vectra.Options{WALMode: vectra.ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	h2 := writer.Attach(g2, writer.Config{})
	closeRefreshSubmit(h2, 12000, 18000)
	closeRefreshWaitIdle(t, g2)
	if err := h2.Close(); err != nil {
		t.Fatal(err)
	}
	if err := g2.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: the mounted forest must carry session 2's packs, leaving
	// only the sub-2048 tail (plus at most the chain-replay tail frames
	// deduped by vid) in the overlay.
	g3, err := vectra.Open(dir+"/v.db", vectra.Options{WALMode: vectra.ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer g3.Close()
	if left := g3.VectorOverlayLen("mem"); left >= 2048 {
		t.Fatalf("reopen overlay=%d (>=2048): light Close dropped converged pack work (B1)", left)
	}
	// Correctness anchor: an exact self query ranks its own key #1.
	hits, err := g3.QueryVector("mem", closeRefreshVec(17000), vectra.QueryVectorOptions{K: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) == 0 || hits[0].Key != "n017000" {
		t.Fatalf("self query: top-1=%v", hits)
	}
}

// TestCloseWithoutDriftSkipsForestWrite pins the cost model: a close with
// no pack drift must not rewrite the forest (Close stays O(1) for an
// untouched session).
func TestCloseWithoutDriftSkipsForestWrite(t *testing.T) {
	dir := t.TempDir()
	g, err := vectra.Open(dir+"/v.db", vectra.Options{WALMode: vectra.ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.CreateVectorIndex(vectra.VectorIndexDecl{Name: "mem", Prop: "emb", Dim: 8, Metric: "cosine"}); err != nil {
		t.Fatal(err)
	}
	h := writer.Attach(g, writer.Config{})
	if err := h.Submit(vectra.Changeset{Nodes: []vectra.NodeUpsert{{Key: "a", Label: "m", Props: map[string]any{"emb": closeRefreshVec(1)}}}}); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	fi1, err := os.Stat(dir + "/v.vec")
	if err != nil {
		t.Fatal(err)
	}

	// Reopen and close with no writes, no packs: the forest file must be
	// byte-identical (mtime unchanged).
	g2, err := vectra.Open(dir+"/v.db", vectra.Options{WALMode: vectra.ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	h2 := writer.Attach(g2, writer.Config{})
	if err := h2.Close(); err != nil {
		t.Fatal(err)
	}
	if err := g2.Close(); err != nil {
		t.Fatal(err)
	}
	fi2, err := os.Stat(dir + "/v.vec")
	if err != nil {
		t.Fatal(err)
	}
	if !fi1.ModTime().Equal(fi2.ModTime()) || fi1.Size() != fi2.Size() {
		t.Fatal("forest rewritten on a no-drift close")
	}
}

// TestVectorWritesSurviveCoveredReopen is the C1 regression from the
// branch's final review: g.replayLSN must reset after Open's replay. In
// the B1 shape the refreshed forest covers the whole chain, so replay
// ends at the watermark - a stale replayLSN then silently drops EVERY
// live vector intake of the new session (props still stored, queries
// return stale results). Session 3 here WRITES after the reopen.
func TestVectorWritesSurviveCoveredReopen(t *testing.T) {
	dir := t.TempDir()
	g, err := vectra.Open(dir+"/v.db", vectra.Options{WALMode: vectra.ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.CreateVectorIndex(vectra.VectorIndexDecl{Name: "mem", Prop: "emb", Dim: 8, Metric: "cosine"}); err != nil {
		t.Fatal(err)
	}
	h := writer.Attach(g, writer.Config{})
	closeRefreshSubmit(h, 0, 12000)
	closeRefreshWaitIdle(t, g)
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}

	// Session 2: drift + light close (the refresh writes the forest at
	// the session's LastLSN).
	g2, err := vectra.Open(dir+"/v.db", vectra.Options{WALMode: vectra.ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	h2 := writer.Attach(g2, writer.Config{})
	closeRefreshSubmit(h2, 12000, 18000)
	closeRefreshWaitIdle(t, g2)
	if err := h2.Close(); err != nil {
		t.Fatal(err)
	}
	if err := g2.Close(); err != nil {
		t.Fatal(err)
	}

	// Session 3: WRITE after the covered reopen. The new vectors must
	// reach the index on the writer's own read path (no lane wait).
	g3, err := vectra.Open(dir+"/v.db", vectra.Options{WALMode: vectra.ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	h3 := writer.Attach(g3, writer.Config{})
	closeRefreshSubmit(h3, 18000, 18010)
	if left := g3.VectorOverlayLen("mem"); left < 10 {
		t.Fatalf("session-3 writes never reached the overlay: %d (<10) - stale replay gate", left)
	}
	hits, err := g3.QueryVector("mem", closeRefreshVec(18005), vectra.QueryVectorOptions{K: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) == 0 || hits[0].Key != "n018005" {
		t.Fatalf("session-3 vector invisible to queries: %v", hits)
	}
	if err := h3.Close(); err != nil {
		t.Fatal(err)
	}
	if err := g3.Close(); err != nil {
		t.Fatal(err)
	}
}
