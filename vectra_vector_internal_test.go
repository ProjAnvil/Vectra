package vectra

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/projanvil/vectra/internal/vector"
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
	g.AddNode("c", "mem", map[string]any{"emb": []float32{1, 2}}) // wrong dim: skipped
	g.AddNode("d", "mem", map[string]any{"other": 1})             // no prop: not indexed
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
	// log (decl frames retire into the v4 snapshot's VectorDecls section),
	// so post-Close replay is empty by design.
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

// TestVecIntakeSemantics: live-write intake - add, overwrite to scalar,
// revive, delete - and OnVecSkip reporting (spec 4.4, 13-2).
func TestVecIntakeSemantics(t *testing.T) {
	dir := t.TempDir()
	var skipped []string
	g, err := Open(dir+"/v.db", Options{WALMode: ModeAlways, OnVecSkip: func(key, index string) {
		skipped = append(skipped, key+"/"+index)
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if err := g.CreateVectorIndex(VectorIndexDecl{Name: "mem", Prop: "emb", Dim: 4, Metric: "cosine"}); err != nil {
		t.Fatal(err)
	}
	vi := vecStateOf(t, g, "mem")
	g.AddNode("a", "m", map[string]any{"emb": []float32{1, 0, 0, 0}})
	if vi.st.OverlayLen() != 1 {
		t.Fatalf("after add: overlay=%d", vi.st.OverlayLen())
	}
	// Overwrite to a non-vector: tombstone (search must not return a).
	g.AddNode("a", "m", map[string]any{"emb": "oops"})
	if got := vi.st.Search([]float32{1, 0, 0, 0}, 1, 32); len(got) != 0 {
		t.Fatalf("overwritten-to-scalar key leaked: %+v", got)
	}
	// Revive with a valid vector.
	g.AddNode("a", "m", map[string]any{"emb": []float32{0, 1, 0, 0}})
	got := vi.st.Search([]float32{0, 1, 0, 0}, 1, 32)
	if len(got) != 1 || got[0].Key != "a" {
		t.Fatalf("revive: %+v", got)
	}
	// Delete: gone again.
	g.DeleteNode("a")
	if got := vi.st.Search([]float32{0, 1, 0, 0}, 1, 32); len(got) != 0 {
		t.Fatalf("deleted key leaked: %+v", got)
	}
	// Invalid payload at write: skipped + reported, no error.
	if _, err := g.AddNode("b", "m", map[string]any{"emb": []float32{1, 2, 3}}); err != nil {
		t.Fatal(err)
	}
	// Two skips: the a->"oops" overwrite (invalid payload reports too,
	// spec 13-2) and b's wrong-dim write.
	if len(skipped) != 2 || skipped[0] != "a/mem" || skipped[1] != "b/mem" {
		t.Fatalf("OnVecSkip calls: %v", skipped)
	}
}

// applyVecOps drives one deterministic op stream through a graph.
func applyVecOps(g *Graph) error {
	if err := g.CreateVectorIndex(VectorIndexDecl{Name: "mem", Prop: "emb", Dim: 8, Metric: "cosine"}); err != nil {
		return err
	}
	rng := rand.New(rand.NewSource(7)) // continuous values: tie-free scores
	for i := 0; i < 60; i++ {
		key := fmt.Sprintf("n%d", i)
		vec := make([]float32, 8)
		for j := range vec {
			vec[j] = rng.Float32()
		}
		if _, err := g.AddNode(key, "m", map[string]any{"emb": vec, "id": i}); err != nil {
			return err
		}
	}
	// Declares AFTER nodes exist: backfill path.
	if err := g.CreateVectorIndex(VectorIndexDecl{Name: "late", Prop: "id2", Dim: 4, Metric: "l2"}); err != nil {
		return err
	}
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("m%d", i)
		v2 := []float32{float32(i), 1, 1, 1}
		if _, err := g.AddNode(key, "m2", map[string]any{"id2": v2}); err != nil {
			return err
		}
	}
	// Overwrites, deletes, revives, wrong-dim skips.
	g.AddNode("n0", "m", map[string]any{"emb": []float32{1, 1, 1, 1, 1, 1, 1, 1}})
	g.DeleteNode("n1")
	g.DeleteNode("m2")
	g.AddNode("n2", "m", map[string]any{"emb": []float32{9, 9, 9, 9}})
	g.AddNode("n3", "m", map[string]any{"emb": []float32{1, 2}}) // skip
	if err := g.DropVectorIndex("late"); err != nil {
		return err
	}
	return nil
}

// TestVecReplayParity: the same op stream applied (1) live without WAL,
// (2) live with WAL, (3) replayed from a mid-session copy of that WAL,
// must yield identical per-index query results - the differential oracle
// pattern FoldLayers used.
func TestVecReplayParity(t *testing.T) {
	a := New()
	if err := applyVecOps(a); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	b, err := Open(dir+"/v.db", Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if err := applyVecOps(b); err != nil {
		t.Fatal(err)
	}
	b.FlushPend()
	// Copy the fsynced WAL segments (ModeAlways) and replay into a third
	// graph. No checkpoint exists, so Open takes the segments-only path.
	// Copy only the WAL directory (segment files, no manifest): the copy
	// opens on the segments-only path and must rebuild purely by replay.
	cp := t.TempDir()
	walSrc := filepath.Join(dir, "v.db.wal")
	os.Mkdir(filepath.Join(cp, "v.db.wal"), 0o755)
	files, _ := os.ReadDir(walSrc)
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		in, _ := os.ReadFile(filepath.Join(walSrc, f.Name()))
		os.WriteFile(filepath.Join(cp, "v.db.wal", f.Name()), in, 0o644)
	}
	c, err := Open(cp+"/v.db", Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if len(c.VectorIndexes()) != 1 {
		t.Fatalf("replayed decls: %+v (want only mem; late dropped)", c.VectorIndexes())
	}
	qrng := rand.New(rand.NewSource(9))
	queries := make([][]float32, 12)
	for i := range queries {
		q := make([]float32, 8)
		for j := range q {
			q[j] = qrng.Float32()
		}
		queries[i] = q
	}
	snapA := vecStateOf(t, a, "mem").head.Load()
	snapB := vecStateOf(t, b, "mem").head.Load()
	snapC := vecStateOf(t, c, "mem").head.Load()
	if snapA == nil || snapB == nil || snapC == nil {
		t.Fatal("unpublished head(s)")
	}
	// Compare as (score, key)-sorted sets: equal scores across distinct
	// keys are common in this data, and tie ORDER is not guaranteed (spec
	// D8) - the oracle pins set-level parity, not presentation order.
	norm := func(hs []vector.VectorHit) []vector.VectorHit {
		sort.Slice(hs, func(i, j int) bool {
			if hs[i].Score != hs[j].Score {
				return hs[i].Score < hs[j].Score
			}
			return hs[i].Key < hs[j].Key
		})
		return hs
	}
	r := vecStateOf(t, a, "mem").st.NewReader()
	for qi, q := range queries {
		ha := norm(snapA.Search(q, 5, 64, r))
		hb := norm(snapB.Search(q, 5, 64, r))
		hc := norm(snapC.Search(q, 5, 64, r))
		if len(ha) == 0 || len(hb) == 0 || len(hc) == 0 {
			t.Fatalf("q%d: empty results %d/%d/%d", qi, len(ha), len(hb), len(hc))
		}
		for i := range ha {
			if ha[i] != hb[i] || ha[i] != hc[i] {
				t.Fatalf("q%d [%d]: a=%+v b=%+v c=%+v", qi, i, ha[i], hb[i], hc[i])
			}
		}
	}
}

// TestVecCloseReopenKeepsIndex: Close checkpoints (WAL reset) - the
// declarations and vectors must survive through the v4 snapshot section
// (audit repro: Save ignored VectorDecls and Close silently dropped
// every index).
func TestVecCloseReopenKeepsIndex(t *testing.T) {
	dir := t.TempDir()
	db := dir + "/v.db"
	g, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.CreateVectorIndex(VectorIndexDecl{Name: "mem", Prop: "emb", Dim: 8, Metric: "cosine"}); err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(21))
	for i := 0; i < 30; i++ {
		v := make([]float32, 8)
		for j := range v {
			v[j] = rng.Float32()
		}
		if _, err := g.AddNode(fmt.Sprintf("n%d", i), "m", map[string]any{"emb": v}); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g2, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer g2.Close()
	if got := g2.VectorIndexes(); len(got) != 1 || got[0].Name != "mem" || got[0].Dim != 8 {
		t.Fatalf("reopened decls: %+v", got)
	}
	vi := vecStateOf(t, g2, "mem")
	if vi.st.OverlayLen() != 30 {
		t.Fatalf("reopened overlay = %d, want 30 vectors from the snapshot backfill", vi.st.OverlayLen())
	}
}

// TestVecCreateInBatchRejected: mid-batch declarations would backfill a
// pre-publish view and miss the batch's own nodes (replay applies decl
// frames between changesets) - rejected with an error instead.
func TestVecCreateInBatchRejected(t *testing.T) {
	g := New()
	g.Batch(func(g *Graph) {
		g.AddNode("n", "m", map[string]any{"emb": []float32{1, 0, 0, 0}})
		if err := g.CreateVectorIndex(VectorIndexDecl{Name: "x", Prop: "emb", Dim: 4, Metric: "cosine"}); err == nil {
			t.Error("CreateVectorIndex inside Batch must error")
		}
	})
	if got := g.VectorIndexes(); len(got) != 0 {
		t.Fatalf("rejected create must leave no registry entry: %+v", got)
	}
}

// TestVecRecreateSameNameParity: drop + recreate of one name inside a
// single flush window coalesces into one full-list frame; replay must
// adopt the NEW parameters (audit repro: name-only reconcile kept the
// stale Dim-4 State and diverged all later intake).
func TestVecRecreateSameNameParity(t *testing.T) {
	dir := t.TempDir()
	b, err := Open(dir+"/v.db", Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if err := applyVecOps(b); err != nil {
		t.Fatal(err)
	}
	if err := b.DropVectorIndex("mem"); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateVectorIndex(VectorIndexDecl{Name: "mem", Prop: "emb", Dim: 8, Metric: "cosine"}); err != nil {
		t.Fatal(err)
	}
	b.FlushPend()
	cp := t.TempDir()
	walSrc := filepath.Join(dir, "v.db.wal")
	os.Mkdir(filepath.Join(cp, "v.db.wal"), 0o755)
	files, _ := os.ReadDir(walSrc)
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		in, _ := os.ReadFile(filepath.Join(walSrc, f.Name()))
		os.WriteFile(filepath.Join(cp, "v.db.wal", f.Name()), in, 0o644)
	}
	c, err := Open(cp+"/v.db", Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	bv := vecStateOf(t, b, "mem")
	cv := vecStateOf(t, c, "mem")
	if bv.decl != cv.decl {
		t.Fatalf("recreated decl mismatch: live=%+v replay=%+v", bv.decl, cv.decl)
	}
	if bv.st.OverlayLen() != cv.st.OverlayLen() {
		t.Fatalf("recreated overlay: live=%d replay=%d", bv.st.OverlayLen(), cv.st.OverlayLen())
	}
}
