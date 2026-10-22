package vectra

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/projanvil/vectra/internal/store"
	"github.com/projanvil/vectra/internal/wal"
)

// --- helpers -------------------------------------------------------------------

// forceCompactTier shrinks the compaction threshold to 1 byte for the rest of
// the test, forcing the compacting tier on tiny libraries (the var exists
// precisely for this; production code never writes it). Restored on cleanup.
func forceCompactTier(t *testing.T) {
	t.Helper()
	old := compactBytes
	compactBytes = 1
	t.Cleanup(func() { compactBytes = old })
}

// listWatermarks returns the sorted on-disk <base>-<lsn>.db watermark snapshot
// names next to db (the same match cleanupSnapshots uses).
func listWatermarks(t *testing.T, db string) []string {
	t.Helper()
	dir := filepath.Dir(db)
	prefix := strings.TrimSuffix(filepath.Base(db), filepath.Ext(db)) + "-"
	es, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range es {
		name := e.Name()
		if e.IsDir() || !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".db") {
			continue
		}
		mid := strings.TrimSuffix(strings.TrimPrefix(name, prefix), ".db")
		if _, err := strconv.ParseUint(mid, 10, 64); err != nil {
			continue
		}
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// listSegments returns the sorted wal-<N>.log file names in walDir.
func listSegments(t *testing.T, walDir string) []string {
	t.Helper()
	es, err := os.ReadDir(walDir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range es {
		if e.IsDir() || !strings.HasPrefix(e.Name(), "wal-") || !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		out = append(out, e.Name())
	}
	sort.Strings(out)
	return out
}

func readManifest(t *testing.T, db string) wal.Manifest {
	t.Helper()
	man, ok, err := wal.ReadManifest(db + ".wal")
	if err != nil || !ok {
		t.Fatalf("manifest at %s.wal: ok=%v err=%v", db, ok, err)
	}
	return man
}

func snapshotVersion(t *testing.T, db, name string) uint16 {
	t.Helper()
	f, err := os.Open(filepath.Join(filepath.Dir(db), name))
	if err != nil {
		t.Fatalf("open snapshot %s: %v", name, err)
	}
	defer f.Close()
	v, err := store.ProbeVersion(f)
	if err != nil {
		t.Fatalf("probe snapshot %s: %v", name, err)
	}
	return v
}

// --- the tier contract ---------------------------------------------------------

// TestFirstCheckpointCompacts pins review constraint 1: the FIRST checkpoint
// of any lineage compacts. RetainFrom 0 is the v1 sentinel, so a retained
// chain headed at segment 0 is inexpressible — a fresh library's first
// checkpoint (Close here; the byte-one tripwire seal already compacted an
// empty base at Open) must write a real v6 snapshot and a v1-shaped manifest
// (RetainFrom cleared, Format 2).
func TestFirstCheckpointCompacts(t *testing.T) {
	db := filepath.Join(t.TempDir(), "graph.db")
	g, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("a", "Svc", nil); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	man := readManifest(t, db)
	if man.Snapshot == "" {
		t.Fatal("first checkpoint must name a snapshot")
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(db), man.Snapshot)); err != nil {
		t.Fatalf("first checkpoint snapshot missing: %v", err)
	}
	if man.RetainFrom != 0 {
		t.Fatalf("first checkpoint RetainFrom = %d, want 0 (compacted)", man.RetainFrom)
	}
	if man.Format != 2 {
		t.Fatalf("first checkpoint Format = %d, want 2", man.Format)
	}
	if v := snapshotVersion(t, db, man.Snapshot); v != store.VersionV6 {
		t.Fatalf("first checkpoint base version = %d, want v6 tripwire", v)
	}
}

// TestLightCheckpointWritesNoSnapshot pins the O(1) tier: once a real
// checkpoint exists (LSN > 0 base), Seal takes the light path — Rotate plus a
// manifest-v2 rewrite, NO snapshot file, NO forest write, NO cleanup, and the
// manifest's Snapshot/LSN stay at the compact boundary (constraint 3: the LSN
// watermark is the only application gate). The retained segments stay on
// disk: they ARE the increment.
func TestLightCheckpointWritesNoSnapshot(t *testing.T) {
	db := filepath.Join(t.TempDir(), "graph.db")
	g, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Batch(func(g *Graph) {
		for i := 0; i < 8; i++ {
			if _, err := g.AddNode("n"+strconv.Itoa(i), "Svc", nil); err != nil {
				t.Fatal(err)
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil { // first checkpoint: compacts (LSN 0 rule)
		t.Fatal(err)
	}
	man1 := readManifest(t, db)
	if man1.LSN == 0 {
		t.Fatal("precondition: first Close must have compacted a real checkpoint")
	}
	snaps1 := listWatermarks(t, db)

	g2, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if err := g2.Batch(func(g *Graph) {
		for i := 8; i < 16; i++ {
			if _, err := g.AddNode("n"+strconv.Itoa(i), "Svc", nil); err != nil {
				t.Fatal(err)
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	segsBefore := listSegments(t, db+".wal")
	if err := g2.Seal(); err != nil { // second checkpoint: light
		t.Fatal(err)
	}

	if got := listWatermarks(t, db); len(got) != len(snaps1) {
		t.Fatalf("light Seal wrote a snapshot: %v (had %v)", got, snaps1)
	}
	man2 := readManifest(t, db)
	if man2.Snapshot != man1.Snapshot || man2.LSN != man1.LSN {
		t.Fatalf("light manifest moved the boundary: %+v (compact boundary was %+v)", man2, man1)
	}
	if man2.RetainFrom <= 0 {
		t.Fatalf("light manifest RetainFrom = %d, want the retained-chain anchor", man2.RetainFrom)
	}
	if man2.Format != 2 {
		t.Fatalf("light manifest Format = %d, want 2", man2.Format)
	}
	// The retained chain stays on disk (Rotate never deletes), and the
	// accounting split is observable: Size() restarted at the rotation while
	// RetainedBytes() still measures the whole live chain (constraint 2 —
	// the compaction trigger must read the latter).
	segsAfter := listSegments(t, db+".wal")
	if len(segsAfter) < len(segsBefore)+1 {
		t.Fatalf("light Seal must keep old segments and add one: before=%v after=%v", segsBefore, segsAfter)
	}
	for _, s := range segsBefore {
		if !contains(segsAfter, s) {
			t.Fatalf("light Seal deleted retained segment %s: %v", s, segsAfter)
		}
	}
	if g2.wal.Size() != 0 {
		t.Fatalf("Size() after rotation = %d, want 0", g2.wal.Size())
	}
	if g2.wal.RetainedBytes() <= 0 {
		t.Fatal("RetainedBytes() after rotation must still count the retained chain")
	}
	if err := g2.Close(); err != nil {
		t.Fatal(err)
	}
}

func contains(xs []string, v string) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}

// TestCompactionAbsorbsRetained pins the threshold tier: once the retained
// chain reaches the compaction threshold, the checkpoint compacts — a fresh
// v6 base appears, the retained segments are deleted (after the manifest
// lands, by Reset), RetainFrom clears, and recovery still sees every write.
func TestCompactionAbsorbsRetained(t *testing.T) {
	db := filepath.Join(t.TempDir(), "graph.db")
	g, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Batch(func(g *Graph) {
		for i := 0; i < 4; i++ {
			if _, err := g.AddNode("n"+strconv.Itoa(i), "Svc", nil); err != nil {
				t.Fatal(err)
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil { // compact #1
		t.Fatal(err)
	}
	man1 := readManifest(t, db)

	g2, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if err := g2.Batch(func(g *Graph) {
		for i := 4; i < 8; i++ {
			if _, err := g.AddNode("n"+strconv.Itoa(i), "Svc", nil); err != nil {
				t.Fatal(err)
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := g2.Seal(); err != nil { // light: the chain to absorb
		t.Fatal(err)
	}
	// Force the threshold: the very next checkpoint must compact.
	forceCompactTier(t)
	if err := g2.Batch(func(g *Graph) {
		for i := 8; i < 12; i++ {
			if _, err := g.AddNode("n"+strconv.Itoa(i), "Svc", nil); err != nil {
				t.Fatal(err)
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := g2.Seal(); err != nil { // compact #2
		t.Fatal(err)
	}

	man2 := readManifest(t, db)
	if man2.RetainFrom != 0 {
		t.Fatalf("compacted manifest RetainFrom = %d, want 0", man2.RetainFrom)
	}
	if man2.Snapshot == man1.Snapshot {
		t.Fatalf("compaction must write a new base, kept %q", man2.Snapshot)
	}
	if man2.LSN <= man1.LSN {
		t.Fatalf("compacted manifest LSN = %d, want > %d", man2.LSN, man1.LSN)
	}
	if v := snapshotVersion(t, db, man2.Snapshot); v != store.VersionV6 {
		t.Fatalf("compacted base version = %d, want v6", v)
	}
	// Reset deletes everything below the fresh segment: exactly one live
	// segment remains, and the old watermark snapshots are cleaned up.
	if segs := listSegments(t, db+".wal"); len(segs) != 1 {
		t.Fatalf("segments after compaction = %v, want exactly the fresh one", segs)
	}
	if snaps := listWatermarks(t, db); len(snaps) != 1 || snaps[0] != man2.Snapshot {
		t.Fatalf("watermarks after compaction = %v, want only %q", snaps, man2.Snapshot)
	}
	if err := g2.Close(); err != nil {
		t.Fatal(err)
	}
	g3, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if n := g3.NodeCount(); n != 12 {
		t.Fatalf("nodes after compaction recovery = %d, want 12", n)
	}
	if err := g3.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestZeroFirstSegmentForcesCompact pins the structural guard (review
// constraint: the light tier must NEVER emit a manifest with RetainFrom 0).
// A light v2 manifest with RetainFrom 0 makes the next recovery floor at
// man.Segment (0 is the v1 sentinel) and silently skip the entire retained
// chain — the total-loss class. The shape is unreachable through the
// pipeline (every LSN>0 manifest descends from a compact whose Reset
// restarts the chain at ≥1), so this fixture hand-crafts it: compact
// normally, then rename the chain's only segment down to 0 and retarget the
// manifest to match. The next Open parks the head at segment 0
// (FirstSegment()==0) with the v6 base and LSN>0 manifest intact, and Seal
// must answer COMPACT, not light.
func TestZeroFirstSegmentForcesCompact(t *testing.T) {
	db := filepath.Join(t.TempDir(), "graph.db")
	g, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("a", "Svc", nil); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil { // compact #1: v6 base + LSN>0 manifest
		t.Fatal(err)
	}
	man1 := readManifest(t, db)

	// Hand-craft the hazard shape: retarget the manifest at segment 0 and
	// rename the live chain's only segment (fresh from Reset) down to 0.
	// Recovery then walks from 0, parks the head there, and FirstSegment()
	// reports 0 — with every light-eligibility check otherwise green.
	man1.Segment = 0
	if err := wal.WriteManifest(db+".wal", man1); err != nil {
		t.Fatal(err)
	}
	segs := listSegments(t, db+".wal")
	if len(segs) != 1 {
		t.Fatalf("fixture precondition: want exactly one live segment, got %v", segs)
	}
	if err := os.Rename(
		filepath.Join(db+".wal", segs[0]),
		filepath.Join(db+".wal", "wal-0.log"),
	); err != nil {
		t.Fatal(err)
	}

	g2, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if fs := g2.wal.FirstSegment(); fs != 0 {
		t.Fatalf("fixture precondition: FirstSegment = %d, want 0", fs)
	}
	if _, err := g2.AddNode("b", "Svc", nil); err != nil {
		t.Fatal(err)
	}
	if err := g2.Seal(); err != nil {
		t.Fatal(err)
	}

	// COMPACT, not light: a fresh v6 base appeared and the manifest stayed
	// v1-shaped (RetainFrom 0, Format 2) — rather than a light manifest whose
	// RetainFrom 0 would orphan the 0-headed retained chain.
	man2 := readManifest(t, db)
	if man2.RetainFrom != 0 {
		t.Fatalf("Seal over a 0-headed chain emitted RetainFrom = %d, want the compact tier (0)", man2.RetainFrom)
	}
	if man2.Format != 2 {
		t.Fatalf("manifest Format = %d, want 2", man2.Format)
	}
	if man2.Snapshot == man1.Snapshot {
		t.Fatalf("the guard must force a fresh base, kept %q", man2.Snapshot)
	}
	if v := snapshotVersion(t, db, man2.Snapshot); v != store.VersionV6 {
		t.Fatalf("fresh base version = %d, want v6", v)
	}
	if got := listWatermarks(t, db); len(got) != 1 || got[0] != man2.Snapshot {
		t.Fatalf("watermarks after the guard's compact = %v, want only %q", got, man2.Snapshot)
	}
	if err := g2.Close(); err != nil {
		t.Fatal(err)
	}
	g3, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if n := g3.NodeCount(); n != 2 {
		t.Fatalf("recovered nodes = %d, want 2", n)
	}
	if err := g3.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestRestartOverRetainedChain pins replay correctness: after light
// checkpoints the next Open must see writes from BOTH sides of every boundary
// — the compact base, the retained chain, and the active tail all apply.
func TestRestartOverRetainedChain(t *testing.T) {
	db := filepath.Join(t.TempDir(), "graph.db")
	g, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("pre", "Svc", nil); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil { // compact #1
		t.Fatal(err)
	}
	g2, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g2.AddNode("mid", "Svc", nil); err != nil {
		t.Fatal(err)
	}
	if err := g2.Seal(); err != nil { // light #1
		t.Fatal(err)
	}
	if _, err := g2.AddNode("post", "Svc", nil); err != nil {
		t.Fatal(err)
	}
	if err := g2.Seal(); err != nil { // light #2
		t.Fatal(err)
	}
	if err := g2.Close(); err != nil { // light #3 (Close)
		t.Fatal(err)
	}
	man := readManifest(t, db)
	if man.RetainFrom <= 0 {
		t.Fatalf("manifest after light checkpoints = %+v, want a retained chain", man)
	}

	g3, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"pre", "mid", "post"} {
		if _, ok := g3.NodeByKey(key); !ok {
			t.Fatalf("node %q lost across the retained chain", key)
		}
	}
	if n := g3.NodeCount(); n != 3 {
		t.Fatalf("recovered nodes = %d, want 3", n)
	}
	if err := g3.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestCloseIsLightNow pins Close's tier: the first Close of a lineage
// compacts (a real snapshot exists afterwards); every later Close under the
// threshold is light — no new snapshot file appears.
func TestCloseIsLightNow(t *testing.T) {
	db := filepath.Join(t.TempDir(), "graph.db")
	g, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("a", "Svc", nil); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil { // first Close: compacts
		t.Fatal(err)
	}
	snaps1 := listWatermarks(t, db)
	man1 := readManifest(t, db)
	if len(snaps1) == 0 {
		t.Fatal("precondition: first Close must have written a snapshot")
	}

	g2, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g2.AddNode("b", "Svc", nil); err != nil {
		t.Fatal(err)
	}
	if err := g2.Close(); err != nil { // second Close: light
		t.Fatal(err)
	}
	if snaps2 := listWatermarks(t, db); len(snaps2) != len(snaps1) {
		t.Fatalf("second Close wrote a snapshot: %v (had %v)", snaps2, snaps1)
	}
	man2 := readManifest(t, db)
	if man2.Snapshot != man1.Snapshot {
		t.Fatalf("light Close moved the base: %q -> %q", man1.Snapshot, man2.Snapshot)
	}

	// And the light-Close session still recovers everything.
	g3, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if n := g3.NodeCount(); n != 2 {
		t.Fatalf("recovered nodes = %d, want 2", n)
	}
	if err := g3.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestLightCheckpointCrashRotateWithoutManifest pins the light tier's crash
// window (rotate first, manifest second): a crash after the Rotate but before
// the manifest lands leaves the OLD manifest plus one empty extra segment —
// recovery chain-continues into it, parks the head, and loses nothing.
func TestLightCheckpointCrashRotateWithoutManifest(t *testing.T) {
	db := filepath.Join(t.TempDir(), "graph.db")
	g, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("a", "Svc", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("b", "Svc", nil); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil { // compact #1
		t.Fatal(err)
	}
	man1 := readManifest(t, db)

	g2, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g2.AddNode("c", "Svc", nil); err != nil {
		t.Fatal(err)
	}
	// Die between Rotate and WriteManifest: rotate the WAL directly, skip the
	// manifest, abandon the session (no Close).
	segsBefore := listSegments(t, db+".wal")
	if err := g2.wal.Rotate(); err != nil {
		t.Fatal(err)
	}
	man2, ok, err := wal.ReadManifest(db + ".wal")
	if err != nil || !ok || man2.Segment != man1.Segment || man2.RetainFrom != man1.RetainFrom {
		t.Fatalf("crash window must leave the old manifest: ok=%v err=%v man=%+v (was %+v)", ok, err, man2, man1)
	}
	segsAfter := listSegments(t, db+".wal")
	if len(segsAfter) != len(segsBefore)+1 {
		t.Fatalf("crash window must leave exactly one extra empty segment: %v -> %v", segsBefore, segsAfter)
	}

	g3, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if n := g3.NodeCount(); n != 3 {
		t.Fatalf("recovered nodes after rotate-crash = %d, want 3 (chain must continue, no loss)", n)
	}
	if err := g3.Close(); err != nil {
		t.Fatal(err)
	}
}
