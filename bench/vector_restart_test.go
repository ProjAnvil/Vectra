package bench

// Restart load-vs-rebuild comparison (P2-2, Task 9): since Task 8, Close's
// checkpoint writes vectors.vec next to the db, and Open mounts the saved
// segment forest when the gates hold (CRC ok, file LSN == manifest LSN,
// declaration set identical) — restart loads the prebuilt HNSW segments
// instead of re-ingesting every snapshot vector. This harness quantifies
// that difference:
//
//	build n x 768 clustered vectors (batched writes, folder lane drained to
//	convergence) -> Close -> restart twice, each on a pristine walk-copy
//	of the library directory:
//	  1. vectors.vec present  -> load path (mount the saved forest)
//	  2. vectors.vec deleted  -> rebuild path (backfillVectorIndex
//	     re-intakes all n snapshot vectors)
//
// What is timed: restart-to-ready, wall clock from the start of Open until
// the folder lane is converged again (overlay < 2048, no full chunk
// pending — the same convergence criterion as the build). This matters
// because State.Add is O(1): the rebuild's backfill only appends n entries
// to the overlay and Open returns almost as fast as the load path; the
// actual HNSW re-construction runs afterwards in the background folder
// lane. Open-only wall time therefore does NOT separate the paths
// (measured 66 vs 68ms at n=10k) — the drain to convergence does: the
// load path mounts prebuilt segments and is converged the moment Open
// returns, while the rebuild path leaves with overlay == n and pays the
// full re-pack after Open. Both the open-only and ready times are logged.
//
// Loaded-vs-rebuilt evidence (limitation): VecForestLoadedForTest lives in
// export_test.go, which is visible only inside the vectra package's own
// test binary — package bench cannot see export_test hooks, and the public
// API is not extended for a bench. The distinction is evidenced instead
// through public behavior: (a) vectors.vec presence before each Open
// (stat), (b) overlay length at Open return — the mounted forest carries
// only the sub-2048 tail, a rebuild re-intakes all n (asserted below when
// n >= 2048), and (c) the ready-time gap, load << rebuild on the same
// machine. Correctness is pinned per path: an exact self query must
// return its own key as hit #1 (cosine distance ~0), legal even while the
// rebuild overlay is still draining (the overlay is exact-scanned).
//
// Timing note: Close resets the WAL at the checkpoint LSN, so neither
// restart replays WAL frames — the two measurements isolate snapshot +
// forest handling and nothing else.
//
// Env-gated like the other acceptance runs: VECTRA_RESTART=1, with n via
// VECTRA_RESTART_N (default 10000 for the smoke run; the recorded 100k
// measurement is controller-executed on a quiet machine).

import (
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/projanvil/vectra"
)

func TestVectorRestartLoadVsRebuild(t *testing.T) {
	if os.Getenv("VECTRA_RESTART") == "" {
		t.Skip("set VECTRA_RESTART=1")
	}
	if testing.Short() {
		t.Skip("measurement run")
	}
	n := 10_000
	if s := os.Getenv("VECTRA_RESTART_N"); s != "" {
		v, err := strconv.Atoi(s)
		if err != nil || v <= 0 {
			t.Fatalf("VECTRA_RESTART_N=%q: not a positive int", s)
		}
		n = v
	}

	const (
		dim   = 768
		batch = 512
		idx   = "mem"
	)
	// Deterministic probes away from the batch edges: an exact self query
	// must rank its own key #1 before and after either restart path.
	probes := []int{n / 3, 2 * n / 3}

	// ---- build (writer path, batched like a real deployment's ingest) ----
	dir := t.TempDir()
	g, err := vectra.Open(dir+"/v.db", vectra.Options{WALMode: vectra.ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.CreateVectorIndex(vectra.VectorIndexDecl{
		Name: idx, Prop: "emb", Dim: dim, Metric: "cosine",
	}); err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(4242))
	vecs := gate2bVecs(rng, n, dim)
	buildStart := time.Now()
	for lo := 0; lo < n; lo += batch {
		hi := lo + batch
		if hi > n {
			hi = n
		}
		err := g.Batch(func(g *vectra.Graph) {
			for i := lo; i < hi; i++ {
				if _, err := g.AddNode(gate2bKey(i), "m", map[string]any{"emb": vecs[i]}); err != nil {
					t.Error(err)
				}
			}
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	// Converged = no full chunk pending (gate-2b pattern): a sub-2048 tail
	// legitimately stays in the overlay forever (read-your-write, D7).
	deadline := time.Now().Add(30 * time.Minute)
	for time.Now().Before(deadline) {
		if g.VectorOverlayLen(idx) < 2048 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	tail := g.VectorOverlayLen(idx)
	if tail >= 2048 {
		t.Fatalf("lane did not converge: overlay=%d (full chunks pending)", tail)
	}
	for _, p := range probes { // baseline: correct before any restart
		restartSelfQuery(t, g, vecs, p)
	}
	if err := g.Close(); err != nil { // checkpoint: snapshot + manifest + vectors.vec
		t.Fatal(err)
	}
	t.Logf("build: n=%d in %s, overlay tail=%d", n,
		time.Since(buildStart).Round(time.Millisecond), tail)

	// restart opens one pristine copy of the library and measures
	// restart-to-ready: Open wall time, then the folder lane's return to
	// convergence (< 2048 pending — already true at Open return on the
	// load path, the whole rebuild cost on the other). Returns the Open
	// wall time, the ready wall time, and the overlay length observed
	// right after Open returned (the which-path evidence).
	restart := func(dir string, dropCache bool) (openDur, readyDur time.Duration, overlayAtOpen int) {
		if dropCache {
			if err := os.Remove(filepath.Join(dir, "vectors.vec")); err != nil {
				t.Fatal(err)
			}
		}
		start := time.Now()
		g, err := vectra.Open(dir+"/v.db", vectra.Options{WALMode: vectra.ModeAlways})
		if err != nil {
			t.Fatal(err)
		}
		openDur = time.Since(start)
		overlayAtOpen = g.VectorOverlayLen(idx)
		dl := time.Now().Add(30 * time.Minute)
		for g.VectorOverlayLen(idx) >= 2048 {
			if time.Now().After(dl) {
				t.Fatalf("folder lane did not converge after restart: overlay=%d", g.VectorOverlayLen(idx))
			}
			time.Sleep(50 * time.Millisecond)
		}
		readyDur = time.Since(start)
		for _, p := range probes { // correct on this path, drained or not
			restartSelfQuery(t, g, vecs, p)
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		return
	}

	// ---- run 1: load path (vectors.vec present) ----
	loadDir := t.TempDir()
	restartCopyTree(t, dir, loadDir)
	fi, err := os.Stat(filepath.Join(loadDir, "vectors.vec"))
	if err != nil {
		t.Fatalf("load-path copy has no vectors.vec (Close must checkpoint it): %v", err)
	}
	loadOpen, loadReady, loadOverlay := restart(loadDir, false)

	// ---- run 2: rebuild path (vectors.vec deleted) ----
	rebuildDir := t.TempDir()
	restartCopyTree(t, dir, rebuildDir)
	rebuildOpen, rebuildReady, rebuildOverlay := restart(rebuildDir, true)

	// Which-path evidence through public behavior: the mounted forest
	// carries only the sub-2048 tail at Open return; a rebuild re-intakes
	// every snapshot vector into the overlay (n entries, then drained).
	if n >= 2048 {
		if loadOverlay >= 2048 {
			t.Errorf("load path opened with overlay=%d (>=2048): the forest cache did not mount — this was a rebuild", loadOverlay)
		}
		if rebuildOverlay < 2048 {
			t.Errorf("rebuild path opened with overlay=%d (<2048): nothing re-intaked — vectors.vec deletion did not force a rebuild", rebuildOverlay)
		}
	}
	// Sanity only: the timing lines carry the verdict (speedup is logged,
	// not asserted — machine variance makes a hard ratio gate flaky).
	if loadReady < 0 {
		t.Errorf("load ready time %s < 0", loadReady)
	}
	if rebuildReady <= 0 {
		t.Errorf("rebuild ready time %s <= 0", rebuildReady)
	}
	t.Logf("vectors.vec: %.1fMB | overlay at open: load=%d rebuild=%d (of n=%d)",
		float64(fi.Size())/(1<<20), loadOverlay, rebuildOverlay, n)
	t.Logf("open-only: load=%s rebuild=%s (Add is O(1); the rebuild cost is the post-open drain)",
		loadOpen.Round(time.Millisecond), rebuildOpen.Round(time.Millisecond))
	t.Logf("n=%d load=%s rebuild=%s speedup=%.1fx", n,
		loadReady.Round(time.Millisecond), rebuildReady.Round(time.Millisecond),
		float64(rebuildReady)/float64(loadReady))
}

// restartSelfQuery asserts the exact-vector invariant: querying vecs[i]
// must return gate2bKey(i) as hit #1 with cosine distance ~0 (scores are
// distances — smaller is better). Holds identically on the live graph,
// the loaded-restart graph, and the rebuilt-restart graph mid-drain (the
// overlay is exact-scanned, so correctness never waits for the pack).
func restartSelfQuery(t *testing.T, g *vectra.Graph, vecs [][]float32, i int) {
	t.Helper()
	hits, err := g.QueryVector("mem", vecs[i], vectra.QueryVectorOptions{K: 10})
	if err != nil {
		t.Fatalf("QueryVector(vec %d): %v", i, err)
	}
	if len(hits) == 0 {
		t.Fatalf("QueryVector(vec %d): no hits", i)
	}
	if hits[0].Key != gate2bKey(i) {
		t.Errorf("self query vec %d: top-1 = %s (score %.4f), want %s",
			i, hits[0].Key, hits[0].Score, gate2bKey(i))
	}
	if hits[0].Score > 1e-4 {
		t.Errorf("self query vec %d: top-1 score %.6f, want cosine distance ~0", i, hits[0].Score)
	}
}

// restartCopyTree copies the whole library directory verbatim (snapshot,
// the .wal dir with manifest and segment, vectors.vec) — the fixture
// consumer test's walk-copy, so each timed restart runs on a pristine
// copy and the two runs cannot pollute each other.
func restartCopyTree(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.Walk(src, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		out := filepath.Join(dst, rel)
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
			return err
		}
		return os.WriteFile(out, b, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
}
