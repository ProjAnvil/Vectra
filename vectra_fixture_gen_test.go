package vectra_test

// Golden fixture for the Phase-2 store-v5 migration (spec
// docs/superpowers/specs/2026-10-21-vector-phase2-design.md §1.3): a
// store-v4 snapshot plus a WAL tail carrying vector frames, captured from
// the code as of v0.10.0. The migration tests treat this directory as
// read-only input; regenerate ONLY from a v0.10.0 checkout:
//
//	VECTRA_GEN_FIXTURE=1 go test . -run TestGenerateLegacyV4Fixture -count=1
//
// Layout on purpose: 24 vectors sealed into the v4 snapshot, 8 more left in
// the WAL tail (the graph is deliberately NOT Closed - Close's final Seal
// would fold the tail into a snapshot), plus one plain node and one
// non-vector prop so migration must discriminate, not blanket-copy.

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/projanvil/vectra"
)

func writeFixtureVecs(t *testing.T, g *vectra.Graph, from, to int) {
	t.Helper()
	err := g.Batch(func(g *vectra.Graph) {
		for i := from; i < to; i++ {
			vec := make([]float32, 8)
			for j := range vec {
				vec[j] = float32(i%8+1) / 8 // distinct per residue, deterministic
			}
			props := map[string]any{"emb": vec, "seq": i}
			if _, err := g.AddNode(fixtureKey(i), "m", props); err != nil {
				t.Error(err)
			}
		}
		// one plain node without any vector prop rides along
		if _, err := g.AddNode("plain", "m", map[string]any{"id": 0}); err != nil {
			t.Error(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
}

func fixtureKey(i int) string { return fmt.Sprintf("n%02d", i) }

func TestGenerateLegacyV4Fixture(t *testing.T) {
	if os.Getenv("VECTRA_GEN_FIXTURE") == "" {
		t.Skip("regeneration is manual, from a v0.10.0 checkout only")
	}
	dir := "testdata/legacy-v4"
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	g, err := vectra.Open(dir+"/v.db", vectra.Options{WALMode: vectra.ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.CreateVectorIndex(vectra.VectorIndexDecl{Name: "mem", Prop: "emb", Dim: 8, Metric: "cosine"}); err != nil {
		t.Fatal(err)
	}
	writeFixtureVecs(t, g, 0, 24)
	if err := g.Seal(); err != nil { // v4 snapshot + manifest + fresh WAL
		t.Fatal(err)
	}
	writeFixtureVecs(t, g, 24, 32) // stays in the WAL tail
	// No Close: the tail must survive on disk exactly as a crash left it.
}

// TestLegacyV4FixtureReplays runs on the committed fixture: the library
// must open, expose the declared index, and answer queries that hit both
// the sealed snapshot (keys < x) and the WAL tail (the rest). It anchors
// the migration work: after store v5 + migration-on-open lands, this test
// must still pass against the same bytes.
func TestLegacyV4FixtureReplays(t *testing.T) {
	src := "testdata/legacy-v4"
	if _, err := os.Stat(src); err != nil {
		t.Skipf("fixture not present: %v", err)
	}
	dir := t.TempDir()
	// copy the whole fixture tree verbatim (snapshot v-*.db + the v.db.wal
	// dir with manifest and segment; note there is no v.db file itself)
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
		dst := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		return os.WriteFile(dst, b, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}

	// ModeOff (the zero Options) keeps v0.6 snapshot-only semantics and
	// never reads the manifest; a legacy WAL library opens with a real
	// mode like any deployment would.
	g, err := vectra.Open(dir+"/v.db", vectra.Options{WALMode: vectra.ModeAlways})
	if err != nil {
		t.Fatalf("open legacy fixture: %v", err)
	}
	defer g.Close()

	decls := g.VectorIndexes()
	if len(decls) != 1 || decls[0].Name != "mem" || decls[0].Dim != 8 {
		t.Fatalf("decls = %+v", decls)
	}
	q := make([]float32, 8)
	for j := range q {
		q[j] = 5.0 / 8 // residue 4 (i%8==4) is nearest
	}
	hits, err := g.QueryVector("mem", q, vectra.QueryVectorOptions{K: 8})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) == 0 {
		t.Fatal("no hits")
	}
	// both sources must contribute: snapshot rows 0..23, tail rows 24..31
	t.Logf("top hit: %s score=%.4f (n=%d)", hits[0].Key, hits[0].Score, len(hits))
	if n := g.Stats().Nodes; n != 33 { // 32 vector nodes + 1 plain
		t.Fatalf("nodes = %d, want 33", n)
	}
}
