package vectra_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/projanvil/vectra"
	"github.com/projanvil/vectra/internal/wal"
)

// replayStats reopens the WAL directory read-only and counts replayable
// frames (each frame = one Changeset; entries = individual ops).
func replayStats(t *testing.T, walDir string) (frames, entries int, last uint64) {
	t.Helper()
	w, err := wal.Open(walDir, wal.ModeAlways)
	if err != nil {
		t.Fatalf("wal open: %v", err)
	}
	defer w.Close()
	last, err = w.Replay(func(cs vectra.Changeset) error {
		frames++
		entries += len(cs.Nodes) + len(cs.Edges) + len(cs.EdgeDeletes) + len(cs.NodeDeletes)
		return nil
	})
	if err != nil {
		t.Fatalf("wal replay: %v", err)
	}
	return frames, entries, last
}

func mustOpen(t *testing.T, path string, mode vectra.Mode) *vectra.Graph {
	t.Helper()
	g, err := vectra.Open(path, vectra.Options{WALMode: mode})
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	return g
}

func TestOpenWriteCloseReopen(t *testing.T) {
	db := filepath.Join(t.TempDir(), "graph.db")
	g := mustOpen(t, db, vectra.ModeAlways)

	if _, err := g.AddNode("a", "Svc", map[string]any{"w": 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("b", "Svc", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("c", "Db", map[string]any{"x": true}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.UpsertEdge("a", "b", "CALLS", map[string]any{"w": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddEdge("a", "b", "CALLS", map[string]any{"w": 2}); err != nil { // parallel
		t.Fatal(err)
	}
	if _, err := g.UpsertEdge("a", "b", "CALLS", map[string]any{"w": 9}); err != nil { // collapse parallels
		t.Fatal(err)
	}
	if _, err := g.AddEdge("b", "c", "USES", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := g.DeleteEdge("b", "c", "USES"); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("d", "Cache", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := g.DeleteNode("d"); err != nil {
		t.Fatal(err)
	}
	if g.NodeCount() != 3 || g.EdgeCount() != 1 {
		t.Fatalf("live counts: nodes=%d edges=%d; want 3, 1", g.NodeCount(), g.EdgeCount())
	}
	if err := g.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Closed: writes rejected with ErrClosed, queries stay legal.
	if _, err := g.AddNode("z", "Z", nil); err != vectra.ErrClosed {
		t.Fatalf("AddNode after Close: %v, want ErrClosed", err)
	}
	if err := g.Batch(func(g *vectra.Graph) {}); err != vectra.ErrClosed {
		t.Fatalf("Batch after Close: %v, want ErrClosed", err)
	}
	if n, ok := g.NodeByKey("a"); !ok || n.Label != "Svc" {
		t.Fatalf("query after Close: %+v ok=%v", n, ok)
	}
	a, _ := g.NodeByKey("a")
	b, _ := g.NodeByKey("b")
	res, err := g.BFS([]vectra.NodeID{a.ID}) // sealed after checkpoint: traversal legal
	if err != nil || res.Count() != 2 {
		t.Fatalf("BFS after Close: count=%d err=%v; want 2", res.Count(), err)
	}
	_ = b

	// Reopen: everything recovered from snapshot + (empty) WAL tail.
	g2 := mustOpen(t, db, vectra.ModeAlways)
	if g2.NodeCount() != 3 || g2.EdgeCount() != 1 {
		t.Fatalf("reopened counts: nodes=%d edges=%d; want 3, 1", g2.NodeCount(), g2.EdgeCount())
	}
	n, ok := g2.NodeByKey("a")
	if !ok || n.Props["w"] != float64(3) { // JSON roundtrip: int -> float64
		t.Fatalf("reopened node a: %+v", n)
	}
	if es, err := g2.EdgesOf("a", vectra.DirOut); err != nil || len(es) != 1 || es[0].Props["w"] != float64(9) {
		t.Fatalf("reopened edges of a: %+v err=%v", es, err)
	}
	if _, ok := g2.NodeByKey("d"); ok {
		t.Fatal("deleted node d resurrected")
	}
	if err := g2.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAbandonReplaysToLastFrame(t *testing.T) {
	db := filepath.Join(t.TempDir(), "graph.db")
	g := mustOpen(t, db, vectra.ModeInterval)
	g.AddNode("a", "Svc", nil)
	g.AddNode("b", "Svc", nil)
	if err := g.Batch(func(g *vectra.Graph) {
		g.AddEdge("a", "b", "CALLS", map[string]any{"w": 1})
		g.AddEdge("a", "b", "CALLS", map[string]any{"w": 2}) // parallel
	}); err != nil {
		t.Fatal(err)
	}
	// Abandon the session without Close (crash simulation): the WAL is the
	// only durable state; recovery must replay up to the last complete frame.
	g2 := mustOpen(t, db, vectra.ModeInterval)
	if g2.NodeCount() != 2 || g2.EdgeCount() != 2 {
		t.Fatalf("recovered counts: nodes=%d edges=%d; want 2, 2", g2.NodeCount(), g2.EdgeCount())
	}
}

func TestWALOffMatchesV06(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "graph.db")

	// Fresh session, zero-value Options = ModeOff: v0.6 import-style flow.
	g, err := vectra.Open(db, vectra.Options{})
	if err != nil {
		t.Fatal(err)
	}
	g.AddNode("a", "Svc", map[string]any{"w": 1})
	g.AddNode("b", "Svc", nil)
	g.UpsertEdge("a", "b", "CALLS", nil)
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("z", "Z", nil); err != vectra.ErrClosed {
		t.Fatalf("write after Close (ModeOff): %v, want ErrClosed", err)
	}
	if _, err := os.Stat(db); !os.IsNotExist(err) {
		t.Fatal("ModeOff Close must not write a snapshot (v0.6: explicit SaveFile)")
	}
	if _, err := os.Stat(db + ".wal"); !os.IsNotExist(err) {
		t.Fatal("ModeOff must not create a WAL directory")
	}

	// v0.6 durability is explicit: a separate graph goes Seal -> SaveFile
	// (end-to-end roundtrip covered in TestWALOffSnapshotRoundtrip).
	seed := vectra.New()
	seed.AddNode("a", "Svc", nil)
	seed.AddNode("b", "Svc", nil)
	seed.UpsertEdge("a", "b", "CALLS", nil)
	if err := seed.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := seed.SaveFile(db); err != nil {
		t.Fatal(err)
	}

	// Open with ModeOff on an existing snapshot: LoadFile semantics (sealed).
	g3, err := vectra.Open(db, vectra.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !g3.Sealed() {
		t.Fatal("ModeOff Open on existing snapshot must load sealed (v0.6)")
	}
	if g3.NodeCount() != 2 {
		t.Fatalf("loaded nodes = %d, want 2", g3.NodeCount())
	}
	if _, err := g3.AddNode("z", "Z", nil); err != vectra.ErrSealed {
		t.Fatalf("write on sealed v0.6 graph: %v, want ErrSealed", err)
	}
}

func TestWALOffSnapshotRoundtrip(t *testing.T) {
	db := filepath.Join(t.TempDir(), "graph.db")
	g := vectra.New()
	g.AddNode("a", "Svc", nil)
	g.AddNode("b", "Svc", nil)
	g.UpsertEdge("a", "b", "CALLS", nil)
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := g.SaveFile(db); err != nil {
		t.Fatal(err)
	}
	loaded, err := vectra.LoadFile(db)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.NodeCount() != 2 || loaded.EdgeCount() != 1 || !loaded.Sealed() {
		t.Fatalf("v0.6 roundtrip: nodes=%d edges=%d sealed=%v", loaded.NodeCount(), loaded.EdgeCount(), loaded.Sealed())
	}
}

func TestOpenAllModes(t *testing.T) {
	for _, mode := range []vectra.Mode{vectra.ModeAlways, vectra.ModeInterval} {
		db := filepath.Join(t.TempDir(), "graph.db")
		g := mustOpen(t, db, mode)
		g.AddNode("a", "Svc", nil)
		g.AddNode("b", "Svc", nil)
		if err := g.Batch(func(g *vectra.Graph) {
			g.UpsertEdge("a", "b", "CALLS", nil)
		}); err != nil {
			t.Fatal(err)
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		g2 := mustOpen(t, db, mode)
		if g2.NodeCount() != 2 || g2.EdgeCount() != 1 {
			t.Fatalf("mode %d reopen: nodes=%d edges=%d; want 2, 1", mode, g2.NodeCount(), g2.EdgeCount())
		}
		if err := g2.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestBatchGroupsSingleFrame(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "graph.db")
	walDir := db + ".wal"
	g := mustOpen(t, db, vectra.ModeAlways)

	if _, err := g.AddNode("s1", "Svc", nil); err != nil { // single write: own frame
		t.Fatal(err)
	}
	if err := g.Batch(func(g *vectra.Graph) {
		g.AddNode("x", "Svc", nil)
		g.AddNode("y", "Svc", nil)
		g.UpsertEdge("x", "y", "U", nil)
		g.AddEdge("x", "y", "P", nil) // parallel, distinct label
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.DeleteNode("s1"); err != nil { // single write: own frame
		t.Fatal(err)
	}

	// 3 writes groups: single, batch (4 ops -> ONE frame), single.
	frames, entries, _ := replayStats(t, walDir)
	if frames != 3 || entries != 6 {
		t.Fatalf("wal contents: %d frames, %d entries; want 3 frames, 6 entries", frames, entries)
	}

	// Kind-ordered batch content check via reopen.
	g2 := mustOpen(t, db, vectra.ModeAlways)
	if g2.NodeCount() != 2 || g2.EdgeCount() != 2 {
		t.Fatalf("reopened: nodes=%d edges=%d; want 2, 2", g2.NodeCount(), g2.EdgeCount())
	}
}

func TestBatchPreservesOpOrderAcrossKindCuts(t *testing.T) {
	db := filepath.Join(t.TempDir(), "graph.db")
	walDir := db + ".wal"
	g := mustOpen(t, db, vectra.ModeAlways)

	g.AddNode("k", "Old", nil) // single write: frame 1
	// Delete then re-add the same key inside ONE Batch: the kind-partitioned
	// changeset cannot express that order in a single frame, so the recorder
	// must split at the kind regression — replay stays faithful either way.
	if err := g.Batch(func(g *vectra.Graph) {
		g.DeleteNode("k")
		g.AddNode("k", "New", nil)
	}); err != nil {
		t.Fatal(err)
	}
	if n, ok := g.NodeByKey("k"); !ok || n.Label != "New" {
		t.Fatalf("live state: %+v ok=%v; want revived k/New", n, ok)
	}

	frames, _, _ := replayStats(t, walDir)
	if frames != 3 { // single + delete frame + re-add frame
		t.Fatalf("frames = %d, want 3 (kind-regression batch must split)", frames)
	}
	g2 := mustOpen(t, db, vectra.ModeAlways)
	if n, ok := g2.NodeByKey("k"); !ok || n.Label != "New" {
		t.Fatalf("replayed state: %+v ok=%v; want revived k/New", n, ok)
	}
}

func TestManifestReconciliationRoot(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "graph.db")
	walDir := db + ".wal"

	g := mustOpen(t, db, vectra.ModeAlways)
	g.AddNode("a", "Svc", nil)
	g.AddNode("b", "Svc", nil)
	g.AddEdge("a", "b", "P", nil)
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	man, ok, err := wal.ReadManifest(walDir)
	if err != nil || !ok {
		t.Fatalf("manifest after Close: ok=%v err=%v", ok, err)
	}
	if man.Snapshot != filepath.Base(db) {
		t.Fatalf("manifest snapshot = %q, want %q", man.Snapshot, filepath.Base(db))
	}

	// Reopen (replays nothing: all frames are in the checkpoint), add exactly
	// one parallel edge, then abandon — the crash-recovery invariant is
	// first replayed lsn == manifest.lsn + 1.
	g2 := mustOpen(t, db, vectra.ModeAlways)
	if g2.EdgeCount() != 1 {
		t.Fatalf("reopen after checkpoint: edges=%d, want 1 (no re-apply)", g2.EdgeCount())
	}
	if _, err := g2.AddEdge("a", "b", "P", nil); err != nil {
		t.Fatal(err)
	}

	frames, _, last := replayStats(t, walDir)
	if frames != 1 || last != man.LSN+1 {
		t.Fatalf("reconciliation: frames=%d lastLSN=%d manifest.lsn=%d; want 1 frame at lsn %d",
			frames, last, man.LSN, man.LSN+1)
	}

	// And graph-level: exactly +1 edge survives recovery (checkpointed frames
	// are never re-applied — parallels would duplicate otherwise).
	g3 := mustOpen(t, db, vectra.ModeAlways)
	if g3.EdgeCount() != 2 {
		t.Fatalf("recovered edges = %d, want exactly 2", g3.EdgeCount())
	}
}

func TestCloseCheckpointFailureKeepsWAL(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "graph.db")
	walDir := db + ".wal"

	g := mustOpen(t, db, vectra.ModeAlways)
	g.AddNode("a", "Svc", nil)
	g.AddNode("b", "Svc", nil)
	g.UpsertEdge("a", "b", "CALLS", nil)

	// Sabotage the checkpoint's atomic rename: the snapshot target becomes a
	// directory, so SaveFile fails at rename — after Seal and before the
	// manifest/Reset. Close must fail WITHOUT deleting the WAL.
	if err := os.Mkdir(db, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err == nil {
		t.Fatal("Close must fail when the snapshot cannot be persisted")
	}
	if _, err := os.Stat(filepath.Join(walDir, "manifest.json")); !os.IsNotExist(err) {
		t.Fatal("no manifest may be written on failed checkpoint")
	}
	if ok, err := wal.HasSegments(walDir); err != nil || !ok {
		t.Fatalf("WAL segments must survive failed checkpoint: ok=%v err=%v", ok, err)
	}

	// Next startup recovers everything from the WAL alone.
	g2 := mustOpen(t, db, vectra.ModeAlways)
	if g2.NodeCount() != 2 || g2.EdgeCount() != 1 {
		t.Fatalf("recovery after failed checkpoint: nodes=%d edges=%d; want 2, 1", g2.NodeCount(), g2.EdgeCount())
	}
}

func TestOpenManifestSnapshotMissing(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "graph.db")

	g := mustOpen(t, db, vectra.ModeAlways)
	g.AddNode("a", "Svc", nil)
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(db); err != nil {
		t.Fatal(err)
	}
	if _, err := vectra.Open(db, vectra.Options{WALMode: vectra.ModeAlways}); err == nil {
		t.Fatal("Open must fail when the manifest references a missing snapshot")
	}
}

func TestOpenAdoptsV06Snapshot(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "graph.db")

	old := vectra.New()
	old.AddNode("legacy", "Svc", map[string]any{"w": 1})
	if err := old.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := old.SaveFile(db); err != nil {
		t.Fatal(err)
	}

	// First WAL-mode Open next to a v0.6 snapshot with no manifest and no
	// segments: adopt the snapshot (no replay would be correct — the WAL is
	// empty), and stay writable.
	g := mustOpen(t, db, vectra.ModeAlways)
	if g.NodeCount() != 1 {
		t.Fatalf("adopted nodes = %d, want 1", g.NodeCount())
	}
	if _, err := g.AddNode("fresh", "Svc", nil); err != nil {
		t.Fatalf("adopted graph must stay writable: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g2 := mustOpen(t, db, vectra.ModeAlways)
	if g2.NodeCount() != 2 {
		t.Fatalf("post-checkpoint nodes = %d, want 2", g2.NodeCount())
	}
}
