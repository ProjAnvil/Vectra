package vectra

// Pack-drift tracking (2026-10-24 fix arc, spec 2026-10-24-1m-fixes-design
// item 1): the forest cache only advances at compacts, so pack work done
// after the last compact is in-memory only and a light Close drops it - the
// 1M B1 finding (reopen re-paid 2h57m of converged packs). The refresh
// trigger needs a signal for "packs happened since the last forest write":
// CommitPack successes (drain) are the tree's only overlay->segment work;
// publish() fires per batch and must NOT count.

import (
	"fmt"
	"testing"
	"time"
)

func driftFixtureVec(i int) []float32 {
	v := make([]float32, 8)
	for j := range v {
		v[j] = float32(((i*13+j*7)%31)-15) / 31.0
	}
	return v
}

// waitForLaneIdle blocks until no full chunk is pending, with a deadline:
// packs are lazy (D10), the merger lane converges in the background.
func waitForLaneIdle(t *testing.T, g *Graph, idx string) {
	t.Helper()
	dl := time.Now().Add(30 * time.Second)
	for g.VectorOverlayLen(idx) >= 2048 {
		if time.Now().After(dl) {
			t.Fatalf("vector lane did not converge: overlay=%d", g.VectorOverlayLen(idx))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestVecPackDriftTracking(t *testing.T) {
	dir := t.TempDir()
	g, err := Open(dir+"/v.db", Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if err := g.CreateVectorIndex(VectorIndexDecl{Name: "mem", Prop: "emb", Dim: 8, Metric: "cosine"}); err != nil {
		t.Fatal(err)
	}
	reg := g.vecReg()

	// No writes, no packs: no drift.
	if vecForestDrift(reg) {
		t.Fatal("drift before any pack (fresh index)")
	}

	// Sub-packSize writes alone (no CommitPack) must not drift: the close
	// refresh's cost model depends on pack work, not any write. flushPend
	// publishes the pend (pendMaxOps=4096 caps would otherwise hold it),
	// which is what arms the direct-mode lane kick - publishing without
	// packing must still read as no drift.
	for i := 0; i < 10; i++ {
		if _, err := g.AddNode(fmt.Sprintf("n%06d", i), "m", map[string]any{"emb": driftFixtureVec(i)}); err != nil {
			t.Fatal(err)
		}
	}
	g.flushPend()
	if vecForestDrift(reg) {
		t.Fatal("drift after sub-pack writes with no pack (overlay-only)")
	}

	// Cross packSize and let the lane drain: CommitPack ran, no forest
	// write yet - drift must be true.
	for i := 10; i < 3000; i++ {
		if _, err := g.AddNode(fmt.Sprintf("n%06d", i), "m", map[string]any{"emb": driftFixtureVec(i)}); err != nil {
			t.Fatal(err)
		}
	}
	g.flushPend() // publish + arm the lane kick (direct mode: publish-path trigger)
	waitForLaneIdle(t, g, "mem")
	if !vecForestDrift(reg) {
		t.Fatal("no drift after CommitPack with no forest write")
	}

	// A successful forest write clears the drift at the captured counts.
	if err := g.writeVecForest(uint64(g.wal.LastLSN())); err != nil {
		t.Fatal(err)
	}
	if vecForestDrift(reg) {
		t.Fatal("drift persists after a fresh forest write")
	}

	// More pack work re-arms the drift.
	for i := 3000; i < 5100; i++ {
		if _, err := g.AddNode(fmt.Sprintf("n%06d", i), "m", map[string]any{"emb": driftFixtureVec(i)}); err != nil {
			t.Fatal(err)
		}
	}
	g.flushPend()
	waitForLaneIdle(t, g, "mem")
	if !vecForestDrift(reg) {
		t.Fatal("no drift after a second round of packs")
	}
}
