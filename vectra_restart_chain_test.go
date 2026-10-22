package vectra

// v0.11.2 restart-over-the-retained-chain tests (plan Task 4, review focus
// #4): Open's forest gate is stale-tolerant (file LSN >= manifest LSN, CRC
// and declaration set intact), so a forest written at the last compact
// boundary survives every light checkpoint that follows, and the segment
// chain replays onto it — frames at or below the forest LSN re-applying
// idempotently, frames above it applying as fresh increments. The chained
// library's base snapshot stays a v6 tripwire (old binaries hard-fail).

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/projanvil/vectra/internal/store"
)

// chainLoadedReopen reopens db and requires the forest to have MOUNTED (the
// gate passed and the declaration sets matched).
func chainLoadedReopen(t *testing.T, db string) *Graph {
	t.Helper()
	g, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if !g.VecForestLoadedForTest() {
		t.Fatal("Open took the rebuild path despite a mountable forest")
	}
	return g
}

// chainLiveSetOf is forestLiveSetOf with an explicit query vector.
func chainLiveSetOf(t *testing.T, g *Graph, index string, q []float32, k int) map[string]float64 {
	t.Helper()
	hits, err := g.QueryVector(index, q, QueryVectorOptions{K: k, Ef: k})
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]float64, len(hits))
	for _, h := range hits {
		out[h.Key] = h.Score
	}
	return out
}

// chainCompareSets asserts two live-set maps are equal, keys and scores.
func chainCompareSets(t *testing.T, name string, got, want map[string]float64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d keys, want %d", name, len(got), len(want))
	}
	for k, s := range want {
		gs, ok := got[k]
		if !ok {
			t.Fatalf("%s: key %s missing (have %d keys)", name, k, len(got))
		}
		if gs != s {
			t.Fatalf("%s: key %s score %v, want %v", name, k, gs, s)
		}
	}
}

// TestStaleForestReplayEquivalence pins the stale-tolerant gate (review
// focus #4): a forest whose LSN is AHEAD of the manifest's compact boundary
// must mount, and the whole retained-chain window — frames the forest
// already contains (<= forestLSN, re-applied idempotently through
// State.Add/Delete) and frames above it (fresh increments) — replays onto
// it with results identical to the full-rebuild twin.
//
// The ahead state is crafted the way TestForestDiscardClasses crafts its
// behind state ("stale lsn"): the compact tier's crash window can only
// leave the forest BEHIND its manifest (manifest lands first), so the ahead
// shape — forest landed, manifest rolled back beneath it — is produced here
// by encoding the live registry at the current LSN over the cache file
// while the manifest keeps naming the older compact boundary. The light
// tier never touches the cache, so the crafted file survives Close.
func TestStaleForestReplayEquivalence(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "graph.db")

	// Compact checkpoint: forest and manifest at LSN c, chain empty.
	g, _ := forestBuild(t, db, 300, 8)
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	man1 := readManifest(t, db)
	if man1.LSN == 0 {
		t.Fatal("precondition: first Close must have compacted a real checkpoint")
	}
	if man1.RetainFrom != 0 {
		t.Fatalf("precondition: compact manifest RetainFrom = %d, want 0", man1.RetainFrom)
	}

	// The chain window: additions, an overwrite that kills an embedding,
	// and a node delete, then a light checkpoint (the chain starts here).
	g2, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if err := g2.Batch(func(g *Graph) {
		for i := 300; i < 350; i++ {
			if _, err := g.AddNode(forestKey(i), "N", map[string]any{"v": forestVec(i, 8)}); err != nil {
				t.Fatal(err)
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := g2.AddNode(forestKey(7), "N", map[string]any{"v": "gone"}); err != nil {
		t.Fatal(err)
	}
	if _, err := g2.DeleteNode(forestKey(3)); err != nil {
		t.Fatal(err)
	}
	if err := g2.Seal(); err != nil { // light: manifest stays at LSN c
		t.Fatal(err)
	}
	man2 := readManifest(t, db)
	if man2.LSN != man1.LSN || man2.RetainFrom <= 0 {
		t.Fatalf("precondition: light manifest = %+v, want the compact boundary %d plus a retained chain", man2, man1.LSN)
	}

	// Craft the ahead forest: the live registry (state at the current LSN,
	// window applied once) stamped with that LSN. On disk this is a compact
	// tier whose forest rename landed while its manifest write was lost or
	// rolled back — the state the >= gate must tolerate.
	forestLSN := g2.wal.LastLSN()
	if forestLSN <= man1.LSN {
		t.Fatalf("fixture: forest LSN %d must exceed the manifest boundary %d", forestLSN, man1.LSN)
	}
	crafted, err := g2.encodeVecForest(forestLSN)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(forestPathOf(db), crafted, 0o644); err != nil {
		t.Fatal(err)
	}
	// Frames ABOVE the forest LSN: the fresh-increment half of the replay.
	if err := g2.Batch(func(g *Graph) {
		for i := 350; i < 360; i++ {
			if _, err := g.AddNode(forestKey(i), "N", map[string]any{"v": forestVec(i, 8)}); err != nil {
				t.Fatal(err)
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := g2.Close(); err != nil { // light again: manifest still at LSN c
		t.Fatal(err)
	}
	man3 := readManifest(t, db)
	if man3.LSN != man1.LSN {
		t.Fatalf("fixture: manifest LSN moved to %d, want the compact boundary %d", man3.LSN, man1.LSN)
	}
	if img, err := readVecForest(forestPathOf(db)); err != nil || img == nil || img.lsn != forestLSN {
		t.Fatalf("fixture: crafted forest unreadable or restamped (img=%v err=%v)", img, err)
	}

	// Reopen: the stale-tolerant gate mounts the ahead forest and replays
	// the whole (man.LSN, now] window onto it — re-application included.
	g3 := chainLoadedReopen(t, db)
	defer g3.Close()

	// Rebuild twin: identical bytes, cache removed — backfill from the base
	// snapshot plus the same chain replay.
	dir2 := t.TempDir()
	forestCopyTree(t, dir, dir2)
	if err := os.Remove(forestPathOf(filepath.Join(dir2, "graph.db"))); err != nil {
		t.Fatal(err)
	}
	g4, err := Open(filepath.Join(dir2, "graph.db"), Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer g4.Close()
	if g4.VecForestLoadedForTest() {
		t.Fatal("removed cache must force the rebuild path")
	}

	// Live sets equal, keys and scores (K=512 exceeds the live count, so
	// the overlay-only scan is exact on both paths).
	loaded := chainLiveSetOf(t, g3, "emb", forestVec(0, 8), 512)
	rebuilt := chainLiveSetOf(t, g4, "emb", forestVec(0, 8), 512)
	chainCompareSets(t, "live set", loaded, rebuilt)

	// Analytic expectation: n0..n359 minus the deleted node and the killed
	// embedding. A frame applied twice would duplicate a vid (no effect on
	// the set); a frame dropped or an Add that mutated the wrong state
	// would shift it.
	if want := 360 - 2; len(loaded) != want {
		t.Fatalf("live set = %d keys, want %d", len(loaded), want)
	}
	for _, gone := range []string{forestKey(3), forestKey(7)} {
		if _, ok := loaded[gone]; ok {
			t.Fatalf("key %s must not be live (%s)", gone, "deleted/killed by the window")
		}
	}
	if _, ok := loaded[forestKey(359)]; !ok {
		t.Fatal("fresh-increment vector (above the forest LSN) missing")
	}

	// Declaration lists survive identically on both paths.
	lv, rv := g3.VectorIndexes(), g4.VectorIndexes()
	if len(lv) != 1 || len(rv) != 1 || lv[0] != rv[0] {
		t.Fatalf("VectorIndexes: loaded %v, rebuilt %v", lv, rv)
	}
}

// TestChainRestartIdentity: multiple light checkpoints with interleaved
// writes and deletes, then Close; the reopen (forest from the compact
// boundary + chain replay) answers the SAME queries with the same hits and
// scores as the pre-Close session.
func TestChainRestartIdentity(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "graph.db")

	g, _ := forestBuild(t, db, 380, 8)
	if err := g.Close(); err != nil { // compact
		t.Fatal(err)
	}

	g2, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	add := func(lo, hi int) {
		if err := g2.Batch(func(g *Graph) {
			for i := lo; i < hi; i++ {
				if _, err := g.AddNode(forestKey(i), "N", map[string]any{"v": forestVec(i, 8)}); err != nil {
					t.Fatal(err)
				}
			}
		}); err != nil {
			t.Fatal(err)
		}
	}
	add(380, 411)
	if err := g2.Seal(); err != nil { // light #1
		t.Fatal(err)
	}
	if _, err := g2.DeleteNode(forestKey(5)); err != nil {
		t.Fatal(err)
	}
	if _, err := g2.AddNode(forestKey(6), "N", map[string]any{"v": "gone"}); err != nil {
		t.Fatal(err)
	}
	add(411, 431)
	if err := g2.Seal(); err != nil { // light #2
		t.Fatal(err)
	}
	if _, err := g2.DeleteNode(forestKey(7)); err != nil {
		t.Fatal(err)
	}

	// Pre-Close truth: three probe queries, exact-scan regime (431 - 3
	// live vectors < K).
	probes := [][]float32{forestVec(0, 8), forestVec(200, 8), forestVec(430, 8)}
	want := make([]map[string]float64, len(probes))
	for qi, q := range probes {
		want[qi] = chainLiveSetOf(t, g2, "emb", q, 512)
	}
	wantIdx := g2.VectorIndexes()
	if err := g2.Close(); err != nil { // light #3 (Close)
		t.Fatal(err)
	}
	man := readManifest(t, db)
	if man.RetainFrom <= 0 {
		t.Fatalf("fixture: manifest after light checkpoints = %+v, want a retained chain", man)
	}

	g3 := chainLoadedReopen(t, db)
	defer g3.Close()
	if n := g3.NodeCount(); n != 429 { // 431 adds minus 2 deletes
		t.Fatalf("node count after chain restart = %d, want 429", n)
	}
	for qi, q := range probes {
		got := chainLiveSetOf(t, g3, "emb", q, 512)
		chainCompareSets(t, "restart identity", got, want[qi])
	}
	if got := g3.VectorIndexes(); len(got) != len(wantIdx) || got[0] != wantIdx[0] {
		t.Fatalf("VectorIndexes after restart = %v, want %v", got, wantIdx)
	}
}

// TestOldBinaryTripwireOnChain: a chained library's base snapshot is the v6
// tripwire — this binary probes and decodes it (v0.11.2 reads chained
// libraries), while the version-dispatch branch an older binary (one that
// knows at most v5) takes on a v6 header is exercised through the honest
// in-process proxy: THIS binary facing an unknown version byte
// (TestMigratedLibRejectedByV4Decode's手法, v7 in place of the retired v6).
func TestOldBinaryTripwireOnChain(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "graph.db")

	g, _ := forestBuild(t, db, 64, 8)
	if err := g.Close(); err != nil { // compact: v6 base + forest
		t.Fatal(err)
	}
	g2, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if err := g2.Batch(func(g *Graph) {
		for i := 64; i < 80; i++ {
			if _, err := g.AddNode(forestKey(i), "N", map[string]any{"v": forestVec(i, 8)}); err != nil {
				t.Fatal(err)
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := g2.Seal(); err != nil { // light: the library is now chained
		t.Fatal(err)
	}
	if err := g2.Close(); err != nil {
		t.Fatal(err)
	}
	man := readManifest(t, db)
	if man.RetainFrom <= 0 {
		t.Fatalf("fixture: manifest = %+v, want a retained chain", man)
	}

	// The chained base probes v6 — the downgrade hard-fail an old binary
	// hits BEFORE it could silently ignore RetainFrom and read the base
	// alone (the silent-loss class this plan exists to close).
	snapPath := filepath.Join(filepath.Dir(db), man.Snapshot)
	snapFile, err := os.Open(snapPath)
	if err != nil {
		t.Fatal(err)
	}
	ver, perr := store.ProbeVersion(snapFile)
	snapFile.Close()
	if perr != nil {
		t.Fatalf("probe %s: %v", snapPath, perr)
	}
	if ver != store.VersionV6 {
		t.Fatalf("chained base %s version = %d, want %d", snapPath, ver, store.VersionV6)
	}
	raw, err := os.ReadFile(snapPath)
	if err != nil {
		t.Fatal(err)
	}
	// This binary reads its own v6 product...
	if _, err := store.Decode(bytes.NewReader(raw)); err != nil {
		t.Fatalf("v6 chained base must decode on this binary: %v", err)
	}
	// ...and hard-fails on an unknown version byte instead of misreading
	// it — the identical dispatch branch a v0.11.0/1 binary takes on the
	// v6 header.
	forged := append([]byte(nil), raw...)
	binary.LittleEndian.PutUint16(forged[4:6], store.VersionV6+1)
	if _, err := store.Decode(bytes.NewReader(forged)); err == nil {
		t.Fatal("Decode must hard-fail on an unknown snapshot version (old-binary dispatch proxy)")
	}

	// The chained library itself still recovers completely.
	g3, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer g3.Close()
	if n := g3.NodeCount(); n != 80 {
		t.Fatalf("nodes after chained restart = %d, want 80", n)
	}
}

// TestFreshChainForestFastPath: right after a compact checkpoint the chain
// is empty (Reset deleted it) and the forest LSN equals the manifest LSN —
// the exact-match load must keep working, no discard, no replay needed.
func TestFreshChainForestFastPath(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "graph.db")

	g, vecs := forestBuild(t, db, 64, 8)
	if err := g.Close(); err != nil { // compact
		t.Fatal(err)
	}
	man := readManifest(t, db)
	if man.RetainFrom != 0 {
		t.Fatalf("compact manifest RetainFrom = %d, want 0", man.RetainFrom)
	}
	if segs := listSegments(t, db+".wal"); len(segs) != 1 {
		t.Fatalf("chain after compact = %v, want exactly the fresh segment", segs)
	}
	img, err := readVecForest(forestPathOf(db))
	if err != nil || img == nil || img.lsn != man.LSN {
		t.Fatalf("forest after compact: img=%v err=%v, want LSN %d", img, err, man.LSN)
	}

	g2 := chainLoadedReopen(t, db)
	defer g2.Close()
	if n := g2.NodeCount(); n != 64 {
		t.Fatalf("node count after fast-path reopen = %d, want 64", n)
	}
	hits, err := g2.QueryVector("emb", vecs[30], QueryVectorOptions{K: 1, Ef: 64})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Key != forestKey(30) || hits[0].Score > 1e-6 {
		t.Fatalf("fast-path self query: %+v", hits)
	}
}
