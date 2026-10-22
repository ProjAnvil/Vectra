package writer_test

// v0.11.2 hotfix test: Handle.Close must not wait out an in-flight
// vector-overlay drain. The merger's vector lane packs overlay chunks at
// ~2048 vectors per HNSW build; pre-fix, a drain pass observed quit only
// BETWEEN select cases, so Close's mergerDone join blocked for the whole
// pass (measured 9.3s for an 8192-vector library, A.2 rerun).
//
// The behavioral assertion is timing-independent: Close must ABANDON the
// backlog (spec D10 lazy convergence - queries scan the overlay and
// restart backfills it), not drain it. Evidence: after Handle.Close and
// the graph-level Close (whose Seal persists the vector State as-is - the
// direct close path does not drain either), a reopened library mounts the
// forest written at close time, so its overlay length is exactly what the
// close left behind:
//
//	pre-fix  Close joins the drain to completion -> overlay < 2048
//	post-fix Close abandons at the chunk boundary -> overlay >= 2048
//
// Scale matches the evidence (dim 768, n=8192 = 4 chunks) so a full pass
// costs seconds; the run is env-gated to keep the default suite fast.
// Env: VECTRA_DRAININT=1.

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/projanvil/vectra"
	"github.com/projanvil/vectra/internal/writer"
)

func TestCloseInterruptsVectorDrain(t *testing.T) {
	if os.Getenv("VECTRA_DRAININT") == "" {
		t.Skip("set VECTRA_DRAININT=1")
	}
	if testing.Short() {
		t.Skip("measurement run")
	}
	const n, dim = 8192, 768
	dir := t.TempDir()
	path := filepath.Join(dir, "v.db")
	g, err := vectra.Open(path, vectra.Options{WALMode: vectra.ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	h := writer.Attach(g, writer.Config{})
	if err := h.DeclareVectorIndex(vectra.VectorIndexDecl{
		Name: "emb", Prop: "emb", Dim: dim, Metric: "cosine",
	}); err != nil {
		t.Fatalf("DeclareVectorIndex: %v", err)
	}

	// Batched changesets submitted without pauses: ingestion (one writer
	// group, ~1s) outruns the folder lane (one chunk packs in seconds), so
	// the overlay backlog at Close is the full 4 chunks (8192 entries).
	rng := rand.New(rand.NewSource(7))
	const batch = 2048
	var last <-chan error
	for b := 0; b < n/batch; b++ {
		cs := vectra.Changeset{Nodes: make([]vectra.NodeUpsert, 0, batch)}
		for j := 0; j < batch; j++ {
			v := make([]float32, dim)
			for k := range v {
				v[k] = rng.Float32() - 0.5
			}
			cs.Nodes = append(cs.Nodes, vectra.NodeUpsert{
				Key: fmt.Sprintf("n%05d", b*batch+j), Label: "m",
				Props: map[string]any{"emb": v},
			})
		}
		last = h.SubmitAsync(cs)
	}
	if err := <-last; err != nil {
		t.Fatalf("last SubmitAsync reply: %v", err)
	}

	t0 := time.Now()
	if err := h.Close(); err != nil {
		t.Fatalf("Handle.Close: %v", err)
	}
	closeWall := time.Since(t0)

	// The graph-level close persists what the wrapper session left in
	// memory (this path drains nothing synchronously): its Seal encodes
	// the post-Close overlay, whatever Close abandoned or waited out.
	if err := g.Close(); err != nil {
		t.Fatalf("graph Close: %v", err)
	}

	g2, err := vectra.Open(path, vectra.Options{WALMode: vectra.ModeAlways})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = g2.Close() }()
	// Read immediately: the reopened graph's own folder lane starts
	// draining the overlay at Open, one multi-second chunk at a time.
	overlay := g2.VectorOverlayLen("emb")
	t.Logf("Handle.Close wall: %s; reopened overlay: %d of %d",
		closeWall.Round(time.Millisecond), overlay, n)
	if overlay < 2048 {
		t.Fatalf("reopened overlay = %d, want >= 2048: Close must abandon "+
			"the vector backlog (lazy convergence, spec D10), not wait out "+
			"the drain", overlay)
	}
}
