package vectra

// Async compact flight (2026-10-24 fix arc, spec item 2): the volume-
// triggered compact tier must not run its multi-second snapshot+forest
// writes inside the writer's afterBatch. The writer pins the published
// immutable view and its LSN, a background flight writes the bytes, and a
// writer-side completion lands the manifest and WAL mutations (quiescent:
// Reset exactly as today; busy: v2 manifest re-anchor + segment-granular
// drop of covered segments - fsynced frames past the pin are never
// deleted). These tests pin the two behaviors that define the feature.

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/projanvil/vectra/internal/wal"
)

// asyncCompactVec is injective in i (a seeded generator - the arithmetic
// fixture collides with period 13 and broke the self-query probe).
func asyncCompactVec(i int) []float32 {
	rng := rand.New(rand.NewSource(int64(i)))
	v := make([]float32, 8)
	for j := range v {
		v[j] = rng.Float32()
	}
	return v
}

// withTinyCompact runs fn with the compaction threshold forced to 1 byte
// (every checkpoint takes the compact tier) and restores it after.
func withTinyCompact(t *testing.T, fn func()) {
	t.Helper()
	old := compactBytes
	compactBytes = 1
	defer func() { compactBytes = old }()
	fn()
}

// asyncWriteNodes appends n direct-mode vector nodes.
func asyncWriteNodes(t *testing.T, g *Graph, lo, hi int) {
	t.Helper()
	for i := lo; i < hi; i++ {
		if _, err := g.AddNode(keyN(i), "m", map[string]any{"emb": asyncCompactVec(i)}); err != nil {
			t.Fatal(err)
		}
	}
}

func keyN(i int) string {
	return "n" + string(rune('a'+i/26)) + string(rune('a'+i%26)) // tiny, distinct keys
}

// TestAsyncCompactWriterUnblocked: while a flight is parked inside its
// byte-writing stage, the writer's own write path must complete promptly -
// the 1M measurement's 11.5-26.3s stalls came from exactly this path
// running inline.
func TestAsyncCompactWriterUnblocked(t *testing.T) {
	withTinyCompact(t, func() {
		dir := t.TempDir()
		g, err := Open(dir+"/v.db", Options{WALMode: ModeAlways})
		if err != nil {
			t.Fatal(err)
		}
		if err := g.CreateVectorIndex(VectorIndexDecl{Name: "mem", Prop: "emb", Dim: 8, Metric: "cosine"}); err != nil {
			t.Fatal(err)
		}
		asyncWriteNodes(t, g, 0, 20)

		parked := make(chan struct{})
		release := make(chan struct{})
		g.compactFlightHook = func() {
			close(parked)
			<-release
		}
		if !g.TryAsyncCompact() {
			t.Fatal("flight did not start (compact tier due, none active)")
		}
		<-parked

		// The writer keeps writing while the flight holds the pinned view.
		wrote := make(chan error, 1)
		go func() {
			wrote <- func() error {
				_, err := g.AddNode(keyN(99), "m", map[string]any{"emb": asyncCompactVec(99)})
				return err
			}()
		}()
		select {
		case err := <-wrote:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("writer blocked while the compact flight was in flight")
		}

		close(release)
		if err := g.WaitAsyncCompact(); err != nil {
			t.Fatal(err)
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}

		// Quiescent completion (no frames past the pin): the manifest
		// names the flight's snapshot at the pinned LSN and the chain was
		// Reset - reopen must find every node through it.
		man, ok, err := wal.ReadManifest(dir + "/v.db.wal")
		if err != nil || !ok {
			t.Fatalf("manifest: %v ok=%v", err, ok)
		}
		if man.RetainFrom != 0 || man.LSN == 0 {
			t.Fatalf("quiescent completion left a v2-shaped manifest: %+v", man)
		}
		if _, err := os.Stat(dir + "/" + man.Snapshot); err != nil {
			t.Fatalf("flight snapshot missing: %v", err)
		}
		g2, err := Open(dir+"/v.db", Options{WALMode: ModeAlways})
		if err != nil {
			t.Fatal(err)
		}
		defer g2.Close()
		if _, ok := g2.NodeByKey(keyN(19)); !ok {
			t.Fatal("node lost across the async compact")
		}
	})
}

// TestAsyncCompactBusyCaseKeepsTailFrames: frames fsynced AFTER the pin
// (the writer kept submitting during the flight) must survive the busy-case
// completion - Reset at the pin LSN would delete them; the v2 re-anchor +
// covered-segment drop must keep them.
func TestAsyncCompactBusyCaseKeepsTailFrames(t *testing.T) {
	withTinyCompact(t, func() {
		dir := t.TempDir()
		g, err := Open(dir+"/v.db", Options{WALMode: ModeAlways})
		if err != nil {
			t.Fatal(err)
		}
		if err := g.CreateVectorIndex(VectorIndexDecl{Name: "mem", Prop: "emb", Dim: 8, Metric: "cosine"}); err != nil {
			t.Fatal(err)
		}
		asyncWriteNodes(t, g, 0, 20)

		release := make(chan struct{})
		g.compactFlightHook = func() { <-release }
		if !g.TryAsyncCompact() {
			t.Fatal("flight did not start")
		}
		// Frames past the pin land while the flight is parked. Force them
		// into a ROTATED (retained) segment too, so the drop path sees a
		// mixed chain: covered prefix + frames past the pin.
		asyncWriteNodes(t, g, 20, 30)
		g.flushPend()
		if err := g.Batch(func(g *Graph) {}); err != nil { // batch boundary: a fresh frame group
			t.Fatal(err)
		}
		asyncWriteNodes(t, g, 30, 40)

		close(release)
		if err := g.WaitAsyncCompact(); err != nil {
			t.Fatal(err)
		}

		// The flight's completion manifest: busy shape (v2 re-anchor at the
		// pin, RetainFrom = the covered-prefix survivor). Checked BEFORE
		// Close - with the test threshold at 1 byte, Close legitimately
		// runs one more synchronous compact that replaces this manifest.
		man, ok, err := wal.ReadManifest(dir + "/v.db.wal")
		if err != nil || !ok {
			t.Fatalf("manifest: %v ok=%v", err, ok)
		}
		if man.RetainFrom == 0 || man.LSN == 0 {
			t.Fatalf("busy completion did not re-anchor a retained chain: %+v", man)
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		g2, err := Open(dir+"/v.db", Options{WALMode: ModeAlways})
		if err != nil {
			t.Fatal(err)
		}
		defer g2.Close()
		for _, i := range []int{0, 19, 20, 29, 30, 39} {
			if _, ok := g2.NodeByKey(keyN(i)); !ok {
				t.Fatalf("node %s lost across the busy-case completion (frames past the pin deleted?)", keyN(i))
			}
		}
		// The vector lane serves post-reopen queries off the forest+chain.
		hits, err := g2.QueryVector("mem", asyncCompactVec(35), QueryVectorOptions{K: 1})
		if err != nil {
			t.Fatal(err)
		}
		if len(hits) == 0 || hits[0].Key != keyN(35) {
			t.Fatalf("self query after busy completion: %v", hits)
		}
	})
}
