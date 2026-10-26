package vectra

// FTS declaration pipeline tests (2026-10-26 FTS plan, Task 7): IndexFullText
// declares a property key for full-text search end to end — WAL IndexDecl
// carriage, layer carriage, fold union, merge harvest into the sealed base,
// and persistence through the store v7 snapshot. Query-side behavior
// (LookupByContains) lands in Task 8; these tests assert the declaration
// surfaces themselves (layer FTSKeys / base FTSKeys are package-internal).

import (
	"path/filepath"
	"slices"
	"testing"

	"github.com/projanvil/vectra/internal/delta"
)

// (a) declare -> write -> Seal: the declaration reaches the sealed base, and
// Save + reopen (store v7 roundtrip) restores it. A re-declare over the
// reopened base is a no-op.
func TestIndexFullTextDeclaresAndPersists(t *testing.T) {
	g := New()
	g.IndexFullText("desc")
	if _, err := g.AddNode("a", "Svc", map[string]any{"desc": "hello world"}); err != nil {
		t.Fatal(err)
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	if keys := g.view.Load().base.FTSKeys(); !slices.Equal(keys, []string{"desc"}) {
		t.Fatalf("base FTSKeys after Seal = %v, want [desc]", keys)
	}

	path := filepath.Join(t.TempDir(), "g.db")
	if err := g.SaveFile(path); err != nil {
		t.Fatal(err)
	}
	g2, err := LoadFile(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if keys := g2.view.Load().base.FTSKeys(); !slices.Equal(keys, []string{"desc"}) {
		t.Fatalf("reopened base FTSKeys = %v, want [desc]", keys)
	}

	// idempotent over the base: a re-declare opens no layer
	g2.IndexFullText("desc")
	g2.LookupByProperty("desc", "hello world") // a read materializes any open window
	if n := layerCount(g2); n != 0 {
		t.Fatalf("re-declare over the base opened %d layers, want 0", n)
	}
}

// (b) double declare emits no second declaration layer (the IndexProperty
// absorption/idempotence contract, mirrored — cf. genview_internal_test.go).
func TestIndexFullTextIdempotent(t *testing.T) {
	g := New()
	g.IndexFullText("desc")
	g.IndexFullText("desc")
	if n := layerCount(g); n != 0 { // absorbed like any unbatched write
		t.Fatalf("declaration materialized a layer eagerly: %d", n)
	}
	g.LookupByProperty("x", "y") // the read materializes the window
	if n := layerCount(g); n != 1 {
		t.Fatalf("after double declare + read, layers = %d, want 1 (one declaration layer)", n)
	}
	l := g.view.Load().layers[0]
	if keys := l.FTSKeys(); !slices.Equal(keys, []string{"desc"}) {
		t.Fatalf("layer FTSKeys = %v, want [desc] exactly once", keys)
	}
	if keys := l.IndexKeys(); len(keys) != 0 {
		t.Fatalf("FTS declare leaked into typed index keys: %v", keys)
	}
}

// (c) a key may be BOTH typed-indexed and FTS-indexed: both declarations take
// (MCP auto-declares typed indexes; cross-kind rejection would silently drop
// the FTS half on agent graphs).
func TestIndexFullTextCoexistsWithTyped(t *testing.T) {
	g := New()
	g.IndexProperty("k")
	g.IndexFullText("k")
	if _, err := g.AddNode("a", "Svc", map[string]any{"k": "v"}); err != nil {
		t.Fatal(err)
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	v := g.view.Load()
	if _, ok := v.base.LookupIndex("k", "v"); !ok {
		t.Fatal("typed index for k missing after Seal")
	}
	if keys := v.base.FTSKeys(); !slices.Equal(keys, []string{"k"}) {
		t.Fatalf("base FTSKeys = %v, want [k]", keys)
	}
	// the public enumerations are kind-split (IndexedPropertyKeys typed-only,
	// FullTextIndexedKeys FTS-only): the coexisting key appears once in each
	if keys := g.IndexedPropertyKeys(); !slices.Equal(keys, []string{"k"}) {
		t.Fatalf("IndexedPropertyKeys = %v, want [k] once", keys)
	}
	if keys := g.FullTextIndexedKeys(); !slices.Equal(keys, []string{"k"}) {
		t.Fatalf("FullTextIndexedKeys = %v, want [k] once", keys)
	}
}

// (d) WAL durability: a session abandoned without Close (crash simulation)
// replays the IndexDecl frame — the declaration is active on the reopened
// graph and survives a subsequent Seal. The batched mixed declaration (typed
// + FTS in one boundary) must ride ONE IndexDecl frame.
func TestIndexFullTextWALReplay(t *testing.T) {
	db := filepath.Join(t.TempDir(), "graph.db")
	g, err := Open(db, Options{WALMode: ModeInterval})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Batch(func(g *Graph) {
		g.IndexProperty("team")
		g.IndexFullText("desc") // same declaration frame as the typed key
		if _, err := g.AddNode("a", "Svc", map[string]any{"team": "core", "desc": "text"}); err != nil {
			t.Fatal(err)
		}
	}); err != nil {
		t.Fatal(err)
	}
	// Abandon without Close: the WAL is the only durable state.

	g2, err := Open(db, Options{WALMode: ModeInterval})
	if err != nil {
		t.Fatalf("reopen after abandon: %v", err)
	}
	if _, ok := g2.NodeByKey("a"); !ok {
		t.Fatal("replayed graph lost node a")
	}
	var typed, fts bool
	for _, l := range g2.view.Load().layers {
		if slices.Contains(l.IndexKeys(), "team") {
			typed = true
		}
		if slices.Contains(l.FTSKeys(), "desc") {
			fts = true
		}
	}
	if !typed || !fts {
		t.Fatalf("replayed layers: typed=%v fts=%v, want both declarations active", typed, fts)
	}
	if err := g2.Seal(); err != nil {
		t.Fatal(err)
	}
	v := g2.view.Load()
	if !slices.Contains(v.base.IndexedKeys(), "team") {
		t.Fatalf("base typed keys after replay+Seal = %v, want team", v.base.IndexedKeys())
	}
	if keys := v.base.FTSKeys(); !slices.Equal(keys, []string{"desc"}) {
		t.Fatalf("base FTSKeys after replay+Seal = %v, want [desc]", keys)
	}
	if err := g2.Close(); err != nil {
		t.Fatal(err)
	}
}

// (e) declarations survive folding: the background merger's FoldLayers unions
// per-layer FTS declarations, forceMerge harvests base ∪ layers into the new
// base, and the merged set persists through Save + reopen.
func TestIndexFullTextSurvivesFold(t *testing.T) {
	g := New()
	if err := g.Batch(func(g *Graph) {
		g.IndexFullText("desc")
		if _, err := g.AddNode("a", "Svc", map[string]any{"desc": "x"}); err != nil {
			t.Fatal(err)
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.Batch(func(g *Graph) {
		g.IndexFullText("title")
		if _, err := g.AddNode("b", "Svc", map[string]any{"title": "y"}); err != nil {
			t.Fatal(err)
		}
	}); err != nil {
		t.Fatal(err)
	}

	// the fold the background governor performs: both declarations union
	folded := delta.FoldLayers(g.view.Load().layers)
	if keys := folded.FTSKeys(); !slices.Equal(keys, []string{"desc", "title"}) {
		t.Fatalf("folded layer FTSKeys = %v, want [desc title]", keys)
	}

	if err := g.forceMerge(); err != nil {
		t.Fatal(err)
	}
	if keys := g.view.Load().base.FTSKeys(); !slices.Equal(keys, []string{"desc", "title"}) {
		t.Fatalf("base FTSKeys after forceMerge = %v, want [desc title]", keys)
	}

	path := filepath.Join(t.TempDir(), "fold.db")
	if err := g.SaveFile(path); err != nil {
		t.Fatal(err)
	}
	g2, err := LoadFile(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if keys := g2.view.Load().base.FTSKeys(); !slices.Equal(keys, []string{"desc", "title"}) {
		t.Fatalf("reopened base FTSKeys = %v, want [desc title]", keys)
	}
}
