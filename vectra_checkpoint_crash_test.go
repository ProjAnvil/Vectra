package vectra

// v0.11.2 final safety sweep (plan Task 6, review focus #1/#2): the crash
// windows the retained-WAL checkpoint pipeline can rest in but that the
// earlier tasks did not pin, plus the silent-loss surface an OLD binary
// (v0.11.1) sees when pointed at a chained library.
//
// Covered here:
//   - compact tier, manifest landed but a Reset delete missed one retained
//     segment (C3): the stale file sits below the manifest's segment floor,
//     recovery never scans it, and the next compact's Reset removes it;
//   - compact tier, new base written but the manifest NOT landed (C1), with
//     a retained chain live (C1 was previously pinned only over an empty
//     chain): the old manifest plus the old chain govern, the orphan v7
//     snapshot is ignored, and a later compact's cleanup deletes it;
//   - the tripwire sweep: EVERY rest state of a chained library — settled
//     compact, settled light (manifest v2), and the mid-session crash shape
//     (rotate landed, manifest not) — names a store-v7 base at the disk
//     level, so a v0.11.1 binary hard-fails at Decode before it can ignore
//     RetainFrom and silently read the base alone;
//   - ModeOff: traced, not assumed. Open's ModeOff branch stats `path` and
//     nothing else — it never reads the WAL dir or the manifest — and a
//     WAL-mode library's `path` never exists (checkpoints write watermark
//     files next to it), so ModeOff cannot see a chained library at all:
//     it returns a fresh empty graph, having read nothing (no silent
//     base-only load is possible). Pointing ModeOff at a v7 snapshot FILE
//     loads it on this binary (Decode knows v7) and hard-fails on an
//     unknown version byte — the exact dispatch branch a v0.11.1 binary
//     takes on the real v7 file.

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/projanvil/vectra/internal/store"
)

// probeManifestBase opens the snapshot the CURRENT manifest names and
// requires its store version — the byte an old binary dispatches on before
// anything else, and therefore the only silent-loss gate that matters.
func probeManifestBase(t *testing.T, db, step string) {
	t.Helper()
	man := readManifest(t, db)
	if v := snapshotVersion(t, db, man.Snapshot); v != store.VersionV7 {
		t.Fatalf("%s: manifest-named base %s probes v%d, want the v7 tripwire", step, man.Snapshot, v)
	}
}

// TestCompactCrashWindowUndeletedRetainedSegment pins C3: Reset deletes the
// retained chain segment by segment AFTER the manifest landed, so a crash
// mid-delete leaves one (or more) stale retained segments on disk BELOW the
// new chain's start. Recovery must never scan them — the manifest's Segment
// floors the walk, and the LSN watermark would refuse their frames anyway —
// and the next compact's Reset must finish the cleanup. The stale segment
// carries an AllowParallel edge append (non-idempotent under re-apply), so a
// broken floor that re-scanned it would surface as a duplicated edge.
func TestCompactCrashWindowUndeletedRetainedSegment(t *testing.T) {
	db := filepath.Join(t.TempDir(), "graph.db")
	walDir := db + ".wal"

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
	if _, err := g.UpsertEdge("a", "b", "U", nil); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil { // first checkpoint: compacts
		t.Fatal(err)
	}

	// Build the retained chain: light #1 leaves wal-S(c, P#1) behind, light
	// #2 leaves wal-S+1(P#2) behind. AddEdge appends true parallel edges —
	// the double-apply canary.
	g2, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g2.AddNode("c", "Svc", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := g2.AddEdge("a", "b", "P", nil); err != nil { // parallel #1
		t.Fatal(err)
	}
	if err := g2.Seal(); err != nil { // light #1
		t.Fatal(err)
	}
	if _, err := g2.AddEdge("a", "b", "P", nil); err != nil { // parallel #2
		t.Fatal(err)
	}
	if err := g2.Seal(); err != nil { // light #2
		t.Fatal(err)
	}
	chain := listSegments(t, walDir)
	if len(chain) < 3 {
		t.Fatalf("fixture: retained chain = %v, want the two retained segments plus the head", chain)
	}
	// Stale-in-waiting: the first retained segment (holds c and P#1).
	staleName := chain[0]
	stalePath := filepath.Join(walDir, staleName)
	staleBytes, err := os.ReadFile(stalePath)
	if err != nil {
		t.Fatal(err)
	}

	// Compact over the chain, then simulate the crash mid-delete: restore
	// one segment Reset already removed.
	forceCompactTier(t)
	if err := g2.Seal(); err != nil { // compact: absorbs the chain, Reset deletes it
		t.Fatal(err)
	}
	if _, err := os.Stat(stalePath); !os.IsNotExist(err) {
		t.Fatalf("fixture: %s must be deleted by the compact's Reset (stat err=%v)", staleName, err)
	}
	if err := os.WriteFile(stalePath, staleBytes, 0o644); err != nil {
		t.Fatal(err)
	}
	man := readManifest(t, db)
	if man.RetainFrom != 0 {
		t.Fatalf("compact manifest RetainFrom = %d, want 0", man.RetainFrom)
	}

	// Reopen: the stale segment is below the manifest's segment floor and
	// must never be scanned — state exact, no duplicated parallel edge.
	g3, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if n := g3.NodeCount(); n != 3 {
		t.Fatalf("nodes after un-deleted-segment recovery = %d, want 3", n)
	}
	if n := g3.EdgeCount(); n != 3 { // U + P#1 + P#2 — a re-scan would say 4
		t.Fatalf("edges after un-deleted-segment recovery = %d, want 3 (no double-apply)", n)
	}
	if _, ok := g3.NodeByKey("c"); !ok {
		t.Fatal("node c lost across the crash window")
	}
	if _, err := os.Stat(stalePath); err != nil {
		t.Fatalf("Open must leave the stale segment for the next compact (it never deletes): %v", err)
	}

	// The next compact finishes the cleanup: Reset deletes everything below
	// the fresh segment, stale file included.
	if _, err := g3.AddNode("e", "Svc", nil); err != nil {
		t.Fatal(err)
	}
	if err := g3.Seal(); err != nil { // compactBytes still forced to 1
		t.Fatal(err)
	}
	if segs := listSegments(t, walDir); len(segs) != 1 || contains(segs, staleName) {
		t.Fatalf("segments after the cleaning compact = %v, want exactly the fresh one (stale %s gone)", segs, staleName)
	}
	if err := g3.Close(); err != nil {
		t.Fatal(err)
	}

	g4, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer g4.Close()
	if n := g4.NodeCount(); n != 4 {
		t.Fatalf("nodes after the cleaning compact = %d, want 4", n)
	}
	if n := g4.EdgeCount(); n != 3 {
		t.Fatalf("edges after the cleaning compact = %d, want 3", n)
	}
}

// TestCompactCrashWindowOrphanBaseOverChain pins C1 WITH a retained chain
// live: the compact tier writes the new v7 base first and the manifest
// second, so a crash in between leaves an orphan watermark snapshot next to
// an OLD manifest whose retained chain still holds every frame above the old
// boundary. Recovery must load the old base and replay the old chain (the
// orphan contains the same writes but is never named), and a later compact's
// cleanupSnapshots must delete the orphan alongside the old base.
func TestCompactCrashWindowOrphanBaseOverChain(t *testing.T) {
	db := filepath.Join(t.TempDir(), "graph.db")
	walDir := db + ".wal"

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
	if _, err := g.UpsertEdge("a", "b", "U", nil); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil { // compact: the chain's base boundary
		t.Fatal(err)
	}

	g2, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g2.AddNode("c", "Svc", nil); err != nil {
		t.Fatal(err)
	}
	if err := g2.Seal(); err != nil { // light: the retained chain is born
		t.Fatal(err)
	}
	if _, err := g2.AddEdge("a", "b", "P", nil); err != nil { // parallel #1
		t.Fatal(err)
	}
	if _, err := g2.AddEdge("a", "b", "P", nil); err != nil { // parallel #2
		t.Fatal(err)
	}
	manLive := readManifest(t, db)
	if manLive.RetainFrom <= 0 {
		t.Fatalf("fixture: manifest = %+v, want a live retained chain", manLive)
	}

	// Crash after SaveFile, before WriteManifest: the orphan v7 watermark is
	// on disk, the manifest still names the old base, and Reset never ran.
	orphan := g2.snapshotFile(g2.wal.LastLSN())
	if err := g2.SaveFile(orphan); err != nil {
		t.Fatal(err)
	}
	if v := snapshotVersion(t, db, filepath.Base(orphan)); v != store.VersionV7 {
		t.Fatalf("orphan watermark probes v%d, want v7", v)
	}
	manCrash := readManifest(t, db)
	if manCrash != manLive {
		t.Fatalf("crash window must leave the old manifest: %+v (was %+v)", manCrash, manLive)
	}

	// Recovery: old base + old chain replay — the orphan is never consulted.
	g3, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if n := g3.NodeCount(); n != 3 {
		t.Fatalf("nodes after orphan-base recovery = %d, want 3", n)
	}
	if n := g3.EdgeCount(); n != 3 { // U + P#1 + P#2, replayed exactly once
		t.Fatalf("edges after orphan-base recovery = %d, want 3 (no double-apply)", n)
	}
	if _, ok := g3.NodeByKey("c"); !ok {
		t.Fatal("node c lost across the crash window")
	}
	if man := readManifest(t, db); man != manLive {
		t.Fatalf("recovery must not rewrite the manifest: %+v (was %+v)", man, manLive)
	}
	if _, err := os.Stat(orphan); err != nil {
		t.Fatalf("the orphan must survive until a compact cleans it: %v", err)
	}

	// The next compact cleans both stale watermarks (old base and orphan).
	if _, err := g3.AddNode("d", "Svc", nil); err != nil {
		t.Fatal(err)
	}
	forceCompactTier(t)
	if err := g3.Close(); err != nil { // compact
		t.Fatal(err)
	}
	manEnd := readManifest(t, db)
	if manEnd.Snapshot == manLive.Snapshot || manEnd.Snapshot == filepath.Base(orphan) {
		t.Fatalf("the cleaning compact must write a fresh base, kept %q", manEnd.Snapshot)
	}
	if snaps := listWatermarks(t, db); len(snaps) != 1 || snaps[0] != manEnd.Snapshot {
		t.Fatalf("watermarks after the cleaning compact = %v, want only %q (orphan cleaned)", snaps, manEnd.Snapshot)
	}
	if segs := listSegments(t, walDir); len(segs) != 1 {
		t.Fatalf("segments after the cleaning compact = %v, want exactly the fresh one", segs)
	}

	g4, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer g4.Close()
	if n := g4.NodeCount(); n != 4 {
		t.Fatalf("nodes after the cleaning compact = %d, want 4", n)
	}
	if n := g4.EdgeCount(); n != 3 {
		t.Fatalf("edges after the cleaning compact = %d, want 3", n)
	}
}

// TestChainLibraryAlwaysV7Backed is the silent-loss sweep (review focus #1):
// whatever rest state a chained library sits in — settled compact (v1-shaped
// manifest), settled light (manifest v2 with RetainFrom, the state a v0.11.1
// binary would misread by ignoring the new field), or the mid-session crash
// shape (rotate landed, manifest not) — the manifest on disk names a store-v7
// base. That byte is what an old binary dispatches on FIRST, so the only
// paths are hard-fail (this test's forged-version proxy) or a full correct
// read; "old binary silently reads the base alone" does not exist.
func TestChainLibraryAlwaysV7Backed(t *testing.T) {
	db := filepath.Join(t.TempDir(), "graph.db")

	g, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("a", "Svc", nil); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil { // compact: the lineage's first base
		t.Fatal(err)
	}
	if man := readManifest(t, db); man.RetainFrom != 0 {
		t.Fatalf("compact manifest RetainFrom = %d, want 0", man.RetainFrom)
	}
	probeManifestBase(t, db, "settled compact")

	g2, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g2.AddNode("b", "Svc", nil); err != nil {
		t.Fatal(err)
	}
	if err := g2.Seal(); err != nil { // light: manifest v2, chain live
		t.Fatal(err)
	}
	manLight := readManifest(t, db)
	if manLight.RetainFrom <= 0 {
		t.Fatalf("fixture: light manifest = %+v, want RetainFrom > 0", manLight)
	}
	probeManifestBase(t, db, "settled light chain")

	// Mid-session crash shape: the rotate landed, the manifest did not. The
	// manifest bytes stay at the light checkpoint while a segment beyond its
	// Segment exists on disk — the exposure window an old binary reopening
	// mid-session faces. Its named base still probes v7.
	segsLive := listSegments(t, db+".wal")
	if _, err := g2.AddNode("c", "Svc", nil); err != nil {
		t.Fatal(err)
	}
	if err := g2.wal.Rotate(); err != nil { // crash: no manifest rewrite
		t.Fatal(err)
	}
	if man := readManifest(t, db); man != manLight {
		t.Fatalf("rotate-without-manifest must leave the manifest untouched: %+v (was %+v)", man, manLight)
	}
	if segs := listSegments(t, db+".wal"); len(segs) != len(segsLive)+1 {
		t.Fatalf("fixture: rotate must leave one extra segment: %v -> %v", segsLive, segs)
	}
	probeManifestBase(t, db, "mid-session crash")

	// The dispatch branch an old binary takes on the v7 header, exercised
	// honestly in-process: this binary decodes its own v7 base, and hard-
	// fails on the same bytes with an unknown version byte.
	man := readManifest(t, db)
	snapPath := filepath.Join(filepath.Dir(db), man.Snapshot)
	raw, err := os.ReadFile(snapPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Decode(bytes.NewReader(raw)); err != nil {
		t.Fatalf("this binary must decode its own v7 base: %v", err)
	}
	forged := append([]byte(nil), raw...)
	binary.LittleEndian.PutUint16(forged[4:6], store.VersionV7+1)
	if _, err := store.Decode(bytes.NewReader(forged)); err == nil {
		t.Fatal("Decode must hard-fail on an unknown snapshot version (old-binary dispatch proxy)")
	}

	// And the chained library itself recovers completely on this binary.
	g3, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer g3.Close()
	if n := g3.NodeCount(); n != 3 {
		t.Fatalf("nodes after chain recovery = %d, want 3", n)
	}
}

// TestModeOffOpenOfChainedLibrary documents and pins the ModeOff trace
// (review focus #1's parenthetical). Open's ModeOff branch stats `path` and
// nothing else: it never reads the WAL directory or the manifest. A WAL-mode
// library's `path` never exists on disk — checkpoints write watermark
// snapshots NEXT TO it and the manifest names those — so:
//
//   - ModeOff Open of a chained library's path returns a fresh empty graph,
//     having read nothing. That is not silent loss: no base was decoded, no
//     increment was skipped; the library is simply invisible to the v0.6
//     snapshot-only contract (and the ModeOff open writes nothing, so the
//     library is untouched);
//   - ModeOff Open of a v7 snapshot FILE loads it on this binary (Decode
//     accepts v1-v7) — the file, not the library: only the compact
//     boundary's content is visible, the retained chain is not;
//   - the same file with an unknown version byte (the dispatch a v0.11.1
//     binary takes on the real v7 file) hard-fails.
//
// There is no ModeOff path that silently reads a chained library's base
// alone: either nothing is read, or the version byte gates the read.
func TestModeOffOpenOfChainedLibrary(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "graph.db")

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
	if _, err := g.UpsertEdge("a", "b", "U", nil); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil { // compact
		t.Fatal(err)
	}
	g2, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g2.AddNode("c", "Svc", nil); err != nil {
		t.Fatal(err)
	}
	if err := g2.Seal(); err != nil { // light: the library is chained
		t.Fatal(err)
	}
	if err := g2.Close(); err != nil {
		t.Fatal(err)
	}
	man := readManifest(t, db)
	if man.RetainFrom <= 0 {
		t.Fatalf("fixture: manifest = %+v, want a retained chain", man)
	}
	chainBefore := listSegments(t, db+".wal")

	// A WAL-mode library's own path never exists: checkpoints write
	// watermarks next to it, never `path` in place.
	if _, err := os.Stat(db); !os.IsNotExist(err) {
		t.Fatalf("library path %s must not exist (stat err=%v); ModeOff has nothing to read", db, err)
	}

	// ModeOff Open of the library: fresh empty graph, nothing read, nothing
	// written — the on-disk chain state is byte-identical afterwards.
	mo, err := Open(db, Options{WALMode: ModeOff})
	if err != nil {
		t.Fatal(err)
	}
	if n := mo.NodeCount(); n != 0 {
		t.Fatalf("ModeOff open of a chained library = %d nodes, want a fresh empty graph (0)", n)
	}
	if err := mo.Close(); err != nil {
		t.Fatal(err)
	}
	if manAfter := readManifest(t, db); manAfter != man {
		t.Fatalf("ModeOff open must not touch the manifest: %+v (was %+v)", manAfter, man)
	}
	if chainAfter := listSegments(t, db+".wal"); len(chainAfter) != len(chainBefore) {
		t.Fatalf("ModeOff open must not touch the WAL dir: %v (was %v)", chainAfter, chainBefore)
	}

	// ModeOff on the manifest-named v7 base FILE: this binary loads it (the
	// compact boundary's content only — the chain is invisible to a file
	// load, by LoadFile semantics), an old binary hard-fails at the byte.
	snap := filepath.Join(dir, man.Snapshot)
	if v := snapshotVersion(t, db, man.Snapshot); v != store.VersionV7 {
		t.Fatalf("chained base probes v%d, want v7", v)
	}
	mof, err := Open(snap, Options{WALMode: ModeOff})
	if err != nil {
		t.Fatalf("this binary must ModeOff-load its own v7 snapshot: %v", err)
	}
	if n := mof.NodeCount(); n != 2 {
		t.Fatalf("ModeOff file load = %d nodes, want the compact boundary's 2 (the chain is not part of the file)", n)
	}
	if n := mof.EdgeCount(); n != 1 {
		t.Fatalf("ModeOff file load = %d edges, want 1", n)
	}
	if err := mof.Close(); err != nil {
		t.Fatal(err)
	}

	// Old-binary dispatch proxy on the same surface: the identical branch a
	// v0.11.1 binary takes on the real v7 file hard-fails through ModeOff.
	raw, err := os.ReadFile(snap)
	if err != nil {
		t.Fatal(err)
	}
	forged := append([]byte(nil), raw...)
	binary.LittleEndian.PutUint16(forged[4:6], store.VersionV7+1)
	forgedPath := filepath.Join(dir, "forged.db")
	if err := os.WriteFile(forgedPath, forged, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(forgedPath, Options{WALMode: ModeOff}); err == nil {
		t.Fatal("ModeOff must hard-fail on an unknown snapshot version (old-binary dispatch proxy)")
	}
}
