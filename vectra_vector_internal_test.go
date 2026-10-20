package vectra

import (
	"testing"

	"github.com/projanvil/vectra/internal/wal"
)

// Internal (white-box) tests for the vector index declaration API. The
// behavioral surface (QueryVector) lands with Task 7; these pin the
// registry, WAL recording, and backfill plumbing.

func vecStateOf(t *testing.T, g *Graph, name string) *vecIndex {
	t.Helper()
	for _, vi := range g.vecReg() {
		if vi.decl.Name == name {
			return vi
		}
	}
	t.Fatalf("index %q not in registry", name)
	return nil
}

func TestVectorDeclAPI(t *testing.T) {
	g := New()
	if err := g.CreateVectorIndex(VectorIndexDecl{Name: "mem", Prop: "emb", Dim: 8, Metric: "cosine"}); err != nil {
		t.Fatal(err)
	}
	if err := g.CreateVectorIndex(VectorIndexDecl{Name: "mem", Prop: "other", Dim: 8, Metric: "cosine"}); err == nil {
		t.Fatal("duplicate name must error")
	}
	for _, bad := range []VectorIndexDecl{
		{Name: "", Prop: "emb", Dim: 8, Metric: "cosine"},
		{Name: "x", Prop: "", Dim: 8, Metric: "cosine"},
		{Name: "x", Prop: "emb", Dim: 0, Metric: "cosine"},
		{Name: "x", Prop: "emb", Dim: 4097, Metric: "cosine"},
		{Name: "x", Prop: "emb", Dim: 8, Metric: "manhattan"},
	} {
		if err := g.CreateVectorIndex(bad); err == nil {
			t.Fatalf("invalid decl %+v must error", bad)
		}
	}
	got := g.VectorIndexes()
	if len(got) != 1 || got[0].Name != "mem" || got[0].Prop != "emb" || got[0].Dim != 8 {
		t.Fatalf("VectorIndexes: %+v", got)
	}
	if err := g.DropVectorIndex("mem"); err != nil {
		t.Fatal(err)
	}
	if err := g.DropVectorIndex("mem"); err == nil {
		t.Fatal("double drop must error")
	}
	if got := g.VectorIndexes(); len(got) != 0 {
		t.Fatalf("after drop: %+v", got)
	}
}

// TestVectorDeclBackfill: creating an index over existing nodes ingests
// their vectors (unionRecords scan), skipping wrong-dim payloads.
func TestVectorDeclBackfill(t *testing.T) {
	g := New()
	g.AddNode("a", "mem", map[string]any{"emb": []float32{1, 0, 0, 0}})
	g.AddNode("b", "mem", map[string]any{"emb": []float32{0, 1, 0, 0}})
	g.AddNode("c", "mem", map[string]any{"emb": []float32{1, 2}})     // wrong dim: skipped
	g.AddNode("d", "mem", map[string]any{"other": 1})                 // no prop: not indexed
	if err := g.CreateVectorIndex(VectorIndexDecl{Name: "mem", Prop: "emb", Dim: 4, Metric: "cosine"}); err != nil {
		t.Fatal(err)
	}
	vi := vecStateOf(t, g, "mem")
	if vi.st.OverlayLen() != 2 {
		t.Fatalf("backfill overlay len = %d, want 2 (a, b)", vi.st.OverlayLen())
	}
	// The published head must see the backfilled state.
	if vi.head.Load() == nil {
		t.Fatal("head not published at create")
	}
}

// TestVectorDeclWALRoundtrip: CreateVectorIndex records a full-list
// IndexDecl frame; Drop re-declares without the name. Replay (Open)
// reconstruction is Task 9's end-to-end; here we pin the frame content.
func TestVectorDeclWALRoundtrip(t *testing.T) {
	dir := t.TempDir()
	db := dir + "/v.db"
	g, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.CreateVectorIndex(VectorIndexDecl{Name: "mem", Prop: "emb", Dim: 4, Metric: "l2", M: 8}); err != nil {
		t.Fatal(err)
	}
	if err := g.CreateVectorIndex(VectorIndexDecl{Name: "two", Prop: "e2", Dim: 4, Metric: "dot"}); err != nil {
		t.Fatal(err)
	}
	if err := g.DropVectorIndex("mem"); err != nil {
		t.Fatal(err)
	}
	g.FlushPend()
	// Replay the live WAL before Close: Close checkpoints and Resets the
	// log (frames move into the v4 snapshot), so post-Close replay is
	// empty by design.
	var lastDecls []wal.VectorIndexDecl
	sawFrame := false
	if _, err := g.wal.Replay(func(fr wal.Frame) error {
		if fr.VectorIndexes != nil {
			sawFrame = true
			lastDecls = fr.VectorIndexes
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if !sawFrame {
		t.Fatal("no vector decl frame in WAL")
	}
	// The final frame re-declares the surviving set: full list semantics,
	// drop = absence.
	if len(lastDecls) != 1 || lastDecls[0].Name != "two" {
		t.Fatalf("last decl frame: %+v, want [two]", lastDecls)
	}
}
