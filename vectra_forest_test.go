package vectra

// vectors.vec forest persistence tests (Phase 2, spec §2.2-2.3): Seal
// writes the cache inside the checkpoint batch, Open mounts it behind the
// watermark gate (CRC + LSN + exact declaration set), and the WAL tail
// replays onto the loaded forest through the ordinary intake. Sizes stay
// small (dim 8, at most one packed chunk) — these are correctness tests,
// not the restart benchmark.

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/projanvil/vectra/internal/wal"
)

func forestKey(i int) string { return fmt.Sprintf("n%06d", i) }

// forestVec derives a deterministic per-i vector: distinct directions, so a
// self-query is unambiguous (score 0, exact key).
func forestVec(i, dim int) []float32 {
	v := make([]float32, dim)
	for j := range v {
		v[j] = float32(math.Sin(float64(i*97+j*13+1)) * (1 + float64(j)))
	}
	return v
}

func forestPathOf(db string) string { return vecForestPath(db) }

// forestBuild opens db, declares one index over prop "v", and adds n nodes
// in a single batch (one WAL frame).
func forestBuild(t *testing.T, db string, n, dim int) (*Graph, [][]float32) {
	t.Helper()
	g, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.CreateVectorIndex(VectorIndexDecl{Name: "emb", Prop: "v", Dim: dim, Metric: "cosine"}); err != nil {
		t.Fatal(err)
	}
	vecs := make([][]float32, n)
	if err := g.Batch(func(g *Graph) {
		for i := 0; i < n; i++ {
			vecs[i] = forestVec(i, dim)
			if _, err := g.AddNode(forestKey(i), "N", map[string]any{"v": vecs[i]}); err != nil {
				t.Fatal(err)
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	return g, vecs
}

// forestQuiesce waits until the folder lane cannot mutate the state any
// further: overlay below one full chunk (< packSize) means no pack can
// start, and a repeated identical reading rules out an in-flight commit
// observed mid-drop. overlayLen only decreases at CommitPack, so a reading
// already below the pack threshold proves the last pack landed.
func forestQuiesce(t *testing.T, g *Graph) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	prev := -1
	for time.Now().Before(deadline) {
		cur := g.VectorOverlayLen("emb")
		if cur < 2048 && cur == prev {
			return
		}
		prev = cur
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("vector overlay never quiesced")
}

// forestLiveSet extracts an index's complete live key set. Valid while the
// state is overlay-only (no packed segments): the search path linearly
// scans every live overlay entry, so K beyond the entry count returns the
// exact set — no approximation, no reachability caveats.
func forestLiveSet(t *testing.T, g *Graph, k int) map[string]float64 {
	return forestLiveSetOf(t, g, "emb", k)
}

func forestLiveSetOf(t *testing.T, g *Graph, index string, k int) map[string]float64 {
	t.Helper()
	hits, err := g.QueryVector(index, forestVec(0, 8), QueryVectorOptions{K: k, Ef: k})
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]float64, len(hits))
	for _, h := range hits {
		out[h.Key] = h.Score
	}
	return out
}

// forestCopyTree duplicates a library directory (db + snapshots + WAL dir +
// forest cache) so the rebuild path can be exercised on the same bytes.
func forestCopyTree(t *testing.T, src, dst string) {
	t.Helper()
	es, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, e := range es {
		s, d := filepath.Join(src, e.Name()), filepath.Join(dst, e.Name())
		if e.IsDir() {
			forestCopyTree(t, s, d)
			continue
		}
		b, err := os.ReadFile(s)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(d, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// TestForestRestartIdentity: Close checkpoints the forest; Open mounts it
// (loaded path) and answers the same queries with the same hits — the
// loaded State is the persisted one, so search runs over the identical
// structure (Task 7's bit-identity carries through the file wrapper).
// 2500 vectors force one packed chunk: the file carries a segment forest
// AND an overlay tail.
func TestForestRestartIdentity(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "graph.db")
	g, _ := forestBuild(t, db, 2500, 8)
	forestQuiesce(t, g)
	queries := [][]float32{forestVec(42, 8), forestVec(1001, 8), forestVec(2499, 8)}
	want := make([]map[string]float64, len(queries))
	for qi, q := range queries {
		hits, err := g.QueryVector("emb", q, QueryVectorOptions{K: 10, Ef: 96})
		if err != nil {
			t.Fatal(err)
		}
		if len(hits) != 10 {
			t.Fatalf("query %d: %d hits, want 10", qi, len(hits))
		}
		want[qi] = make(map[string]float64, len(hits))
		for _, h := range hits {
			want[qi][h.Key] = h.Score
		}
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(forestPathOf(db)); err != nil {
		t.Fatalf("checkpoint wrote no forest cache: %v", err)
	}

	g2, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer g2.Close()
	if !g2.VecForestLoadedForTest() {
		t.Fatal("Open took the rebuild path despite a matching forest cache")
	}
	if n := g2.NodeCount(); n != 2500 {
		t.Fatalf("reopened node count = %d, want 2500", n)
	}
	for qi, q := range queries {
		hits, err := g2.QueryVector("emb", q, QueryVectorOptions{K: 10, Ef: 96})
		if err != nil {
			t.Fatal(err)
		}
		got := make(map[string]float64, len(hits))
		for _, h := range hits {
			got[h.Key] = h.Score
		}
		w := want[qi]
		if len(got) != len(w) {
			t.Fatalf("query %d: %d hits after restart, want %d", qi, len(got), len(w))
		}
		for k, s := range w {
			if gs, ok := got[k]; !ok {
				t.Fatalf("query %d: key %s lost after restart; got %v", qi, k, got)
			} else if gs != s {
				t.Fatalf("query %d: key %s score %v after restart, want %v", qi, k, gs, s)
			}
		}
	}
}

// TestForestTailReplay (ruling #3): a WAL tail written after the forest's
// checkpoint LSN — additions, an overwrite that kills an embedding, and a
// node delete — replays onto the LOADED forest, and the resulting live set
// equals the full-rebuild path's, key for key. The library is left
// un-Closed (crash), so the tail survives as WAL frames.
func TestForestTailReplay(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "graph.db")
	g, _ := forestBuild(t, db, 300, 8)
	if err := g.Seal(); err != nil { // checkpoint: snapshot + manifest + forest @ LSN X
		t.Fatal(err)
	}
	// Tail beyond the watermark.
	if err := g.Batch(func(g *Graph) {
		for i := 300; i < 350; i++ {
			if _, err := g.AddNode(forestKey(i), "N", map[string]any{
				"v": forestVec(i, 8), "w": forestVec(i+1000, 8),
			}); err != nil {
				t.Fatal(err)
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	// Overwrite n7's vector with a non-vector: intake deletes the old embedding.
	if _, err := g.AddNode(forestKey(7), "N", map[string]any{"v": "gone"}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.DeleteNode(forestKey(3)); err != nil {
		t.Fatal(err)
	}
	// A tail declaration frame (full list: emb kept, emb2 added): replay must
	// keep emb's mounted forest — applyVecDecls only rebuilds changed or new
	// names — and backfill emb2 from the nodes written before the frame.
	if err := g.CreateVectorIndex(VectorIndexDecl{Name: "emb2", Prop: "w", Dim: 8, Metric: "cosine"}); err != nil {
		t.Fatal(err)
	}
	// Crash: no Close. Reopen mounts the forest and replays the tail.
	g2, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if !g2.VecForestLoadedForTest() {
		t.Fatal("tail frames must not discard the forest (ruling #3)")
	}
	// Rebuild twin: same bytes, cache removed.
	dir2 := t.TempDir()
	forestCopyTree(t, dir, dir2)
	if err := os.Remove(forestPathOf(filepath.Join(dir2, "graph.db"))); err != nil {
		t.Fatal(err)
	}
	g3, err := Open(filepath.Join(dir2, "graph.db"), Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if g3.VecForestLoadedForTest() {
		t.Fatal("removed cache must force the rebuild path")
	}
	loaded, rebuilt := forestLiveSet(t, g2, 512), forestLiveSet(t, g3, 512)
	if len(loaded) != len(rebuilt) {
		t.Fatalf("live sets differ: loaded %d keys, rebuilt %d keys", len(loaded), len(rebuilt))
	}
	for k, s := range rebuilt {
		ls, ok := loaded[k]
		if !ok {
			t.Fatalf("key %s live on the rebuild path only; loaded set has %d keys", k, len(loaded))
		}
		if ls != s {
			t.Fatalf("key %s: loaded score %v, rebuilt %v", k, ls, s)
		}
	}
	// Analytic expectation: n0..n349 minus the deleted node and the killed embedding.
	if want := 350 - 2; len(loaded) != want {
		t.Fatalf("live set = %d keys, want %d", len(loaded), want)
	}
	if _, ok := loaded[forestKey(3)]; ok {
		t.Fatal("deleted node still live in the index")
	}
	if _, ok := loaded[forestKey(7)]; ok {
		t.Fatal("overwritten embedding still live in the index")
	}
	if _, ok := loaded[forestKey(349)]; !ok {
		t.Fatal("tail vector missing from the loaded forest")
	}
	// The tail's new index: same live set on both paths (the n300..n349
	// nodes carry prop w; the pre-checkpoint nodes do not).
	l2, r2 := forestLiveSetOf(t, g2, "emb2", 512), forestLiveSetOf(t, g3, "emb2", 512)
	if len(l2) != 50 || len(r2) != 50 {
		t.Fatalf("emb2 live sets: loaded %d, rebuilt %d, want 50/50", len(l2), len(r2))
	}
	for k := range r2 {
		if _, ok := l2[k]; !ok {
			t.Fatalf("emb2: key %s live on the rebuild path only", k)
		}
	}
	if err := g2.Close(); err != nil {
		t.Fatal(err)
	}
	if err := g3.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestForestDiscardClasses: every gate failure discards the cache and takes
// the rebuild path with correct results — stale watermark (crash between
// the snapshot rename and the cache rename), corruption, a declaration set
// that no longer matches the snapshot, a missing file, and the last index
// dropped (checkpoint removes the stale cache instead of writing one).
func TestForestDiscardClasses(t *testing.T) {
	build := func(t *testing.T) (db string, vecs [][]float32, lsn uint64) {
		t.Helper()
		db = filepath.Join(t.TempDir(), "graph.db")
		g, vecs := forestBuild(t, db, 64, 8)
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		man, has, err := wal.ReadManifest(db + ".wal")
		if err != nil || !has {
			t.Fatalf("manifest after Close: has=%v err=%v", has, err)
		}
		return db, vecs, man.LSN
	}
	// reopened runs the rebuilt index through an exact self-query (the
	// overlay-only state scans linearly, so top-1 is deterministic).
	reopened := func(t *testing.T, db string, loaded bool, vec []float32) *Graph {
		t.Helper()
		g, err := Open(db, Options{WALMode: ModeAlways})
		if err != nil {
			t.Fatal(err)
		}
		if g.VecForestLoadedForTest() != loaded {
			t.Fatalf("VecForestLoadedForTest = %v, want %v", g.VecForestLoadedForTest(), loaded)
		}
		hits, err := g.QueryVector("emb", vec, QueryVectorOptions{K: 1, Ef: 64})
		if err != nil {
			t.Fatal(err)
		}
		if len(hits) != 1 || hits[0].Score != 0 {
			t.Fatalf("self-query after discard: %+v", hits)
		}
		return g
	}

	t.Run("stale lsn", func(t *testing.T) {
		db, vecs, lsn := build(t)
		man, _, err := wal.ReadManifest(db + ".wal")
		if err != nil {
			t.Fatal(err)
		}
		man.LSN = lsn + 1 // crash window: cache one checkpoint behind the manifest
		if err := wal.WriteManifest(db+".wal", man); err != nil {
			t.Fatal(err)
		}
		g := reopened(t, db, false, vecs[5])
		if _, err := os.Stat(forestPathOf(db)); !os.IsNotExist(err) {
			t.Fatal("stale cache not discarded")
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("corrupt crc", func(t *testing.T) {
		db, vecs, _ := build(t)
		p := forestPathOf(db)
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		b[len(b)/2] ^= 0xff
		if err := os.WriteFile(p, b, 0o644); err != nil {
			t.Fatal(err)
		}
		g := reopened(t, db, false, vecs[5])
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatal("corrupt cache not discarded")
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("decl mismatch", func(t *testing.T) {
		db, vecs, lsn := build(t)
		// A well-formed cache (CRC intact, watermark matching) whose only
		// index is declared dim 16 while the snapshot says dim 8.
		scratch := New()
		if err := scratch.CreateVectorIndex(VectorIndexDecl{Name: "emb", Prop: "v", Dim: 16, Metric: "cosine"}); err != nil {
			t.Fatal(err)
		}
		if _, err := scratch.AddNode("x", "N", map[string]any{"v": forestVec(0, 16)}); err != nil {
			t.Fatal(err)
		}
		b, err := scratch.encodeVecForest(lsn)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(forestPathOf(db), b, 0o644); err != nil {
			t.Fatal(err)
		}
		g := reopened(t, db, false, vecs[5])
		if _, err := os.Stat(forestPathOf(db)); !os.IsNotExist(err) {
			t.Fatal("mismatched cache not discarded")
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("missing file", func(t *testing.T) {
		db, vecs, _ := build(t)
		if err := os.Remove(forestPathOf(db)); err != nil {
			t.Fatal(err)
		}
		g := reopened(t, db, false, vecs[5])
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("last index dropped", func(t *testing.T) {
		db, _, _ := build(t)
		g, err := Open(db, Options{WALMode: ModeAlways})
		if err != nil {
			t.Fatal(err)
		}
		if !g.VecForestLoadedForTest() {
			t.Fatal("baseline reopen must mount the cache")
		}
		if err := g.DropVectorIndex("emb"); err != nil {
			t.Fatal(err)
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(forestPathOf(db)); !os.IsNotExist(err) {
			t.Fatal("checkpoint kept a stale cache for an index-less graph")
		}
		g2, err := Open(db, Options{WALMode: ModeAlways})
		if err != nil {
			t.Fatal(err)
		}
		if n := len(g2.VectorIndexes()); n != 0 {
			t.Fatalf("indexes after drop = %d, want 0", n)
		}
		if err := g2.Close(); err != nil {
			t.Fatal(err)
		}
	})
}
