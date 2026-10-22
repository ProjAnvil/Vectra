package vectra

// v0.11.2 evidence: decompose the ~9.3s Close of an 8192-vector library
// (A.2 rerun: close 9.29-9.84s for a 24.4MB snapshot — disproportionate).
// Times the phases: ingestion+drain state at Close entry, Seal internals
// (flushPending / forceMerge / checkpoint / forest), and whether Close
// blocks on the vector folder lane. Env: VECTRA_CLOSEPROF=1.

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
)

func TestClosePhaseProfile(t *testing.T) {
	if os.Getenv("VECTRA_CLOSEPROF") == "" {
		t.Skip("set VECTRA_CLOSEPROF=1")
	}
	if testing.Short() {
		t.Skip("measurement run")
	}
	const n, dim = 8192, 768
	dir := t.TempDir()
	g, err := Open(dir+"/v.db", Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.CreateVectorIndex(VectorIndexDecl{Name: "mem", Prop: "emb", Dim: dim, Metric: "cosine"}); err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(11))
	vecs := make([][]float32, n)
	for i := range vecs {
		v := make([]float32, dim)
		for j := range v {
			v[j] = rng.Float32() - 0.5
		}
		vecs[i] = v
	}
	t0 := time.Now()
	const batch = 256
	for b := 0; b < n/batch; b++ {
		err := g.Batch(func(g *Graph) {
			for j := 0; j < batch; j++ {
				i := b*batch + j
				if _, err := g.AddNode(mkKey(i), "m", map[string]any{"emb": vecs[i]}); err != nil {
					t.Error(err)
				}
			}
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("ingest: %s (overlay=%d)", time.Since(t0).Round(time.Millisecond), g.VectorOverlayLen("mem"))

	// Let the folder lane run to a natural mid-state, then Close cold-ish:
	// vary the pre-Close drain window to see Close's drain share.
	t1 := time.Now()
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("close(immediate): %s", time.Since(t1).Round(time.Millisecond))

	// Second library, same build, but wait for lane convergence first.
	g2, err := Open(t.TempDir()+"/v.db", Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if err := g2.CreateVectorIndex(VectorIndexDecl{Name: "mem", Prop: "emb", Dim: dim, Metric: "cosine"}); err != nil {
		t.Fatal(err)
	}
	for b := 0; b < n/batch; b++ {
		err := g2.Batch(func(g *Graph) {
			for j := 0; j < batch; j++ {
				i := b*batch + j
				if _, err := g.AddNode(mkKey(i), "m", map[string]any{"emb": vecs[i]}); err != nil {
					t.Error(err)
				}
			}
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(30 * time.Minute)
	for time.Now().Before(deadline) && g2.VectorOverlayLen("mem") >= 2048 {
		time.Sleep(200 * time.Millisecond)
	}
	t.Logf("drained pre-close: overlay=%d (waited %s)", g2.VectorOverlayLen("mem"),
		time.Since(t0).Round(0))
	t2 := time.Now()
	if err := g2.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("close(converged): %s", time.Since(t2).Round(time.Millisecond))
}

func mkKey(i int) string { return fmt.Sprintf("n%08d", i) }
