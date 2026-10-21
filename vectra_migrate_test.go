package vectra_test

// Migration-on-open to store v5 + fresh-library tripwire (spec
// 2026-10-21 §1.3, rulings #4/#9). The golden v0.10.0 library under
// testdata/legacy-v4 (v4 snapshot + vector-carrying WAL tail) is the
// migration input, consumed read-only: a WAL-mode Open folds it into a v5
// watermark snapshot exactly once, and a brand-new library gets an empty v5
// checkpoint from byte one so a v0.10 downgrade hard-fails at the snapshot
// instead of silently truncating unknown frames.

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/projanvil/vectra"
	"github.com/projanvil/vectra/internal/store"
	"github.com/projanvil/vectra/internal/wal"
)

// copyTree copies a fixture library verbatim into dst: migration tests run
// against exact on-disk bytes, never a regeneration.
func copyTree(t *testing.T, src, dst string) {
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

// headSnapshot resolves the library's current checkpoint head: the
// manifest-named watermark snapshot, its path, and its store version.
func headSnapshot(t *testing.T, db string) (string, uint16) {
	t.Helper()
	man, ok, err := wal.ReadManifest(db + ".wal")
	if err != nil || !ok {
		t.Fatalf("manifest at %s.wal: ok=%v err=%v", db, ok, err)
	}
	p := filepath.Join(filepath.Dir(db), man.Snapshot)
	f, err := os.Open(p)
	if err != nil {
		t.Fatalf("head snapshot %s: %v", p, err)
	}
	defer f.Close()
	ver, err := store.ProbeVersion(f)
	if err != nil {
		t.Fatalf("probe %s: %v", p, err)
	}
	return p, ver
}

func mustStat(t *testing.T, p string) os.FileInfo {
	t.Helper()
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatalf("stat %s: %v", p, err)
	}
	return fi
}

// TestMigrateV4LibraryOnOpen: opening the golden v4 library migrates it in
// place — the manifest's head becomes a v5 watermark snapshot holding the
// folded state of snapshot + WAL tail, reads stay transparent, and a reopen
// of the migrated library answers vector queries (Review Focus #3).
func TestMigrateV4LibraryOnOpen(t *testing.T) {
	dir := t.TempDir()
	copyTree(t, "testdata/legacy-v4", dir)
	db := filepath.Join(dir, "v.db")

	g, err := vectra.Open(db, vectra.Options{WALMode: vectra.ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if _, ver := headSnapshot(t, db); ver != store.VersionV5 {
		t.Fatalf("head snapshot version after Open = %d, want 5", ver)
	}
	// Migration is transparent to reads: the sealed snapshot rows (keys
	// n00..n23 + plain) and the replayed tail (n24..n31) all answer.
	if n := g.Stats().Nodes; n != 33 {
		t.Fatalf("nodes after migration = %d, want 33", n)
	}
	decls := g.VectorIndexes()
	if len(decls) != 1 || decls[0].Name != "mem" || decls[0].Dim != 8 {
		t.Fatalf("vector decls after migration = %+v", decls)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}

	g2, err := vectra.Open(db, vectra.Options{WALMode: vectra.ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer g2.Close()
	q := make([]float32, 8)
	for j := range q {
		q[j] = 5.0 / 8 // residue 4 is nearest
	}
	hits, err := g2.QueryVector("mem", q, vectra.QueryVectorOptions{K: 8})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) == 0 {
		t.Fatal("QueryVector on the migrated library found nothing")
	}
	if n := g2.Stats().Nodes; n != 33 {
		t.Fatalf("nodes after reopen = %d, want 33", n)
	}
}

// TestFreshLibraryHasV5Tripwire (ruling #9): a brand-new WAL-mode library
// carries its v5 checkpoint + manifest from byte one — before any write and
// before Close — so a v0.10 downgrade opening it hard-fails at the snapshot
// version byte instead of silently proceeding.
func TestFreshLibraryHasV5Tripwire(t *testing.T) {
	db := filepath.Join(t.TempDir(), "fresh.db")
	g, err := vectra.Open(db, vectra.Options{WALMode: vectra.ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	p, ver := headSnapshot(t, db)
	if ver != store.VersionV5 {
		t.Fatalf("fresh head %s version = %d, want 5 (tripwire must exist before Close)", p, ver)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ver := headSnapshot(t, db); ver != store.VersionV5 {
		t.Fatalf("post-Close head version = %d, want 5", ver)
	}
}

// TestMigrationIdempotent: reopening a migrated (v5-head) library triggers
// no second seal. The observable is the checkpoint artifacts themselves: a
// seal rewrites both the head snapshot (SaveFile) and the manifest
// (WriteManifest) via temp+rename, so unchanged mtime+size across a reopen
// proves nothing fired.
func TestMigrationIdempotent(t *testing.T) {
	dir := t.TempDir()
	copyTree(t, "testdata/legacy-v4", dir)
	db := filepath.Join(dir, "v.db")

	g, err := vectra.Open(db, vectra.Options{WALMode: vectra.ModeAlways}) // migrates
	if err != nil {
		t.Fatal(err)
	}
	// Precondition: the first Open itself migrated the head (no Close yet).
	if _, ver := headSnapshot(t, db); ver != store.VersionV5 {
		t.Fatalf("head version after first Open = %d, want 5", ver)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	snapPath, _ := headSnapshot(t, db)
	manPath := filepath.Join(db+".wal", "manifest.json")
	names := [2]string{snapPath, manPath}
	before := [2]os.FileInfo{mustStat(t, snapPath), mustStat(t, manPath)}

	g2, err := vectra.Open(db, vectra.Options{WALMode: vectra.ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	after := [2]os.FileInfo{mustStat(t, snapPath), mustStat(t, manPath)}
	for i := range before {
		if !before[i].ModTime().Equal(after[i].ModTime()) || before[i].Size() != after[i].Size() {
			t.Fatalf("reopen rewrote checkpoint artifact %s (mtime %v -> %v): migration is not idempotent",
				names[i], before[i].ModTime(), after[i].ModTime())
		}
	}
	if n := g2.Stats().Nodes; n != 33 {
		t.Fatalf("nodes after reopen = %d, want 33", n)
	}
	if err := g2.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestMigratedLibRejectedByV4Decode pins the downgrade tripwire's mechanism:
// Decode dispatches on the header version and hard-fails on anything it does
// not know BEFORE interpreting the layout. The migration product is v5; an
// old binary is one that knows only older versions — the honest in-process
// proxy for "old reader" is THIS binary facing a hypothetical v6 header,
// which exercises the identical dispatch branch a v0.10 binary takes on the
// v5 product.
func TestMigratedLibRejectedByV4Decode(t *testing.T) {
	dir := t.TempDir()
	copyTree(t, "testdata/legacy-v4", dir)
	db := filepath.Join(dir, "v.db")
	g, err := vectra.Open(db, vectra.Options{WALMode: vectra.ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	// The migration product is the head Open left on disk — before any
	// Close, whose own checkpoint would write v5 regardless.
	snapPath, ver := headSnapshot(t, db)
	if ver != store.VersionV5 {
		t.Fatalf("migrated head version = %d, want 5 (Open alone must produce it)", ver)
	}
	raw, err := os.ReadFile(snapPath)
	if err != nil {
		t.Fatal(err)
	}
	// This binary reads its own v5 product...
	if _, err := store.Decode(bytes.NewReader(raw)); err != nil {
		t.Fatalf("v5 migration product must decode on this binary: %v", err)
	}
	// ...and hard-fails on an unknown version byte instead of misreading it.
	forged := append([]byte(nil), raw...)
	binary.LittleEndian.PutUint16(forged[4:6], store.VersionV5+1)
	if _, err := store.Decode(bytes.NewReader(forged)); err == nil {
		t.Fatal("Decode must hard-fail on an unknown snapshot version (version dispatch)")
	}
}

// TestMigrationRetainsV4Artifact (industry fix: pg_upgrade keeps the old
// cluster, ArangoDB advises a pre-upgrade backup): the migration Seal's
// checkpoint cleanup must not leave the library un-downgradable with no
// rollback bytes — the manifest-named v4 watermark survives as
// <name>-v4.bak, still probing v4. The artifact must also be invisible to
// recovery (reopen is clean) and spare later seals (Close's own checkpoint
// cleanup must not match the .bak).
func TestMigrationRetainsV4Artifact(t *testing.T) {
	dir := t.TempDir()
	copyTree(t, "testdata/legacy-v4", dir)
	db := filepath.Join(dir, "v.db")

	g, err := vectra.Open(db, vectra.Options{WALMode: vectra.ModeAlways}) // migrates
	if err != nil {
		t.Fatal(err)
	}
	if _, ver := headSnapshot(t, db); ver != store.VersionV5 {
		t.Fatalf("head snapshot version after Open = %d, want 5", ver)
	}
	// The pre-migration watermark (fixture manifest names v-2.db) survives as
	// a rollback artifact whose bytes still probe v4.
	bak := filepath.Join(dir, "v-2.db-v4.bak")
	bf, err := os.Open(bak)
	if err != nil {
		t.Fatalf("migration deleted the pre-migration watermark without a rollback artifact: %v", err)
	}
	bakVer, err := store.ProbeVersion(bf)
	bf.Close()
	if err != nil {
		t.Fatal(err)
	}
	if bakVer != 4 {
		t.Fatalf("rollback artifact %s probes v%d, want 4", bak, bakVer)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen (idempotent — no second migration) plus the Close-checkpoint's
	// own cleanupSnapshots must both spare the artifact: unchanged
	// size+mtime across the cycle proves no later seal touched it.
	before := mustStat(t, bak)
	g2, err := vectra.Open(db, vectra.Options{WALMode: vectra.ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if err := g2.Close(); err != nil {
		t.Fatal(err)
	}
	after := mustStat(t, bak)
	if !before.ModTime().Equal(after.ModTime()) || before.Size() != after.Size() {
		t.Fatalf("reopen/Close cycle rewrote rollback artifact %s (mtime %v -> %v): later seals must spare it",
			bak, before.ModTime(), after.ModTime())
	}
}

// TestRetainLegacyArtifactRestore pins the failure-path contract at unit
// level: Open offers no cheap Seal-failure injection (an unwritable directory
// breaks the artifact rename itself), so the rename-aside/restore mechanics
// of retainLegacyArtifact are tested directly.
func TestRetainLegacyArtifactRestore(t *testing.T) {
	t.Run("rename aside and restore", func(t *testing.T) {
		orig := filepath.Join(t.TempDir(), "v-2.db")
		if err := os.WriteFile(orig, []byte("legacy-bytes"), 0o644); err != nil {
			t.Fatal(err)
		}
		bak := orig + "-v4.bak"
		restore := vectra.RetainLegacyArtifactForTest(orig, 4)
		if _, err := os.Stat(orig); !os.IsNotExist(err) {
			t.Fatalf("retain must move the manifest-named file aside: stat err=%v", err)
		}
		if b, err := os.ReadFile(bak); err != nil || string(b) != "legacy-bytes" {
			t.Fatalf("artifact after retain = %q err=%v, want legacy-bytes", b, err)
		}
		restore()
		if b, err := os.ReadFile(orig); err != nil || string(b) != "legacy-bytes" {
			t.Fatalf("restore must put the manifest-named file back for the retry Open: %q err=%v", b, err)
		}
		if _, err := os.Stat(bak); !os.IsNotExist(err) {
			t.Fatalf("restore must consume the artifact: stat err=%v", err)
		}
	})
	t.Run("no-op paths never panic", func(t *testing.T) {
		// Empty path (nothing loaded from a manifest) and a failed rename
		// (missing source) must yield a no-op restore.
		vectra.RetainLegacyArtifactForTest("", 4)()
		vectra.RetainLegacyArtifactForTest(filepath.Join(t.TempDir(), "missing.db"), 4)()
	})
}
