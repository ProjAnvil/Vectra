package vectra

// LookupByContains tests (2026-10-26 FTS plan, Task 8): the public read side
// of the full-text plan — case-insensitive Contains over FTS-declared string
// properties, answered through the sealed gram buckets (with a mandatory
// verify pass) or the pending full-scan path. Declaration is probed before
// any path choice, so an undeclared key answers (nil, false) regardless of
// needle length (the 1-rune scan path must not answer undeclared keys).

import (
	"bytes"
	"slices"
	"testing"
)

func ftsAdd(t *testing.T, g *Graph, key string, props map[string]any) {
	t.Helper()
	if _, err := g.AddNode(key, "Svc", props); err != nil {
		t.Fatal(err)
	}
}

func hitKeys(ns []Node) []string {
	out := make([]string, len(ns))
	for i, n := range ns {
		out[i] = n.Key
	}
	return out
}

// buildFTSGraph: mixed-case ASCII values that fold to the same needle, a
// space-separated variant, and a CJK value — the shape the bigram tokenizer
// (no padding, rune pairs) has to answer across scripts.
func buildFTSGraph(t *testing.T) *Graph {
	t.Helper()
	g := New()
	g.IndexFullText("title")
	ftsAdd(t, g, "a", map[string]any{"title": "GraphDB"})
	ftsAdd(t, g, "b", map[string]any{"title": "graphdb"})
	ftsAdd(t, g, "c", map[string]any{"title": "GRAPH DB"})
	ftsAdd(t, g, "d", map[string]any{"title": "图数据库"})
	ftsAdd(t, g, "e", map[string]any{"title": "unrelated"})
	ftsAdd(t, g, "f", map[string]any{"title": 42}) // non-string values never hit
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	return g
}

// (a) basics: case folding both ways, a needle crossing a space, CJK, the
// undeclared/empty guards, and declared-empty answering ok=true.
func TestLookupByContainsBasics(t *testing.T) {
	g := buildFTSGraph(t)
	if ns, ok := g.LookupByContains("title", "graphdb"); !ok || !slices.Equal(hitKeys(ns), []string{"a", "b"}) {
		t.Fatalf(`LookupByContains("title","graphdb") = %v ok=%v, want [a b]/true`, hitKeys(ns), ok)
	}
	if ns, ok := g.LookupByContains("title", "GRAPHDB"); !ok || !slices.Equal(hitKeys(ns), []string{"a", "b"}) {
		t.Fatalf("mixed-case needle must fold: %v ok=%v", hitKeys(ns), ok)
	}
	if ns, ok := g.LookupByContains("title", "aph d"); !ok || !slices.Equal(hitKeys(ns), []string{"c"}) {
		t.Fatalf(`LookupByContains("title","aph d") = %v ok=%v, want [c]/true`, hitKeys(ns), ok)
	}
	if ns, ok := g.LookupByContains("title", "数据"); !ok || !slices.Equal(hitKeys(ns), []string{"d"}) {
		t.Fatalf(`LookupByContains("title","数据") = %v ok=%v, want [d]/true`, hitKeys(ns), ok)
	}
	if ns, ok := g.LookupByContains("absent", "graphdb"); ok || ns != nil {
		t.Fatalf("undeclared key must answer (nil,false), got %v ok=%v", ns, ok)
	}
	if ns, ok := g.LookupByContains("title", ""); ok || ns != nil {
		t.Fatalf("empty needle must answer (nil,false), got %v ok=%v", ns, ok)
	}
	// declared key with zero hits keeps ok=true (empty needle is different:
	// that is not a search)
	if ns, ok := g.LookupByContains("title", "zzzz-no-such-text"); !ok || len(ns) != 0 {
		t.Fatalf("declared key with zero hits = %v ok=%v, want empty/true", ns, ok)
	}
	// a declared key that never saw a string value: the empty gram bucket
	// keeps the declared/undeclared distinction
	g.IndexFullText("note")
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	if ns, ok := g.LookupByContains("note", "anything"); !ok || len(ns) != 0 {
		t.Fatalf("declared-empty key = %v ok=%v, want empty/true", ns, ok)
	}
}

// (b) gram collisions must not fabricate hits: needle "abc" splits into grams
// ab+bc, both present in "xxabyy bc zz", but the value does not CONTAIN
// "abc" — the mandatory verify pass filters the gram-prefilter candidate.
func TestLookupByContainsGramCollision(t *testing.T) {
	g := New()
	g.IndexFullText("v")
	ftsAdd(t, g, "solo", map[string]any{"v": "xxabyy bc zz"})
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	if ns, ok := g.LookupByContains("v", "abc"); !ok || len(ns) != 0 {
		t.Fatalf(`needle "abc" over "xxabyy bc zz" = %v ok=%v, want empty/true (verify pass)`, ns, ok)
	}
	if ns, ok := g.LookupByContains("v", "bc z"); !ok || !slices.Equal(hitKeys(ns), []string{"solo"}) {
		t.Fatalf(`needle "bc z" = %v ok=%v, want [solo]/true`, hitKeys(ns), ok)
	}
}

// (c) 1-rune needles produce no grams, so the answer rides the full-scan
// path (documented slower) — and, critically, the declaration pre-probe
// still gates it: an undeclared key with a 1-rune needle answers
// (nil,false). The naive nil-baseIndex implementation would answer true.
func TestLookupByContainsSingleRuneFallback(t *testing.T) {
	g := New()
	g.IndexFullText("v")
	ftsAdd(t, g, "a", map[string]any{"v": "xxabyy bc zz"})
	ftsAdd(t, g, "b", map[string]any{"v": "图数据库"})
	ftsAdd(t, g, "c", map[string]any{"v": "nothing here"})
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	if ns, ok := g.LookupByContains("v", "z"); !ok || !slices.Equal(hitKeys(ns), []string{"a"}) {
		t.Fatalf(`1-rune "z" = %v ok=%v, want [a]/true (scan path)`, hitKeys(ns), ok)
	}
	if ns, ok := g.LookupByContains("v", "Z"); !ok || !slices.Equal(hitKeys(ns), []string{"a"}) {
		t.Fatalf(`1-rune "Z" = %v ok=%v, want [a]/true (needle folds)`, hitKeys(ns), ok)
	}
	if ns, ok := g.LookupByContains("v", "库"); !ok || !slices.Equal(hitKeys(ns), []string{"b"}) {
		t.Fatalf(`1-rune "库" = %v ok=%v, want [b]/true (scan path)`, hitKeys(ns), ok)
	}
	if ns, ok := g.LookupByContains("absent", "z"); ok || ns != nil {
		t.Fatalf("undeclared key + 1-rune needle must answer (nil,false), got %v ok=%v", ns, ok)
	}
}

// (d) pending declarations answer before the merge (R14 parity with the
// typed lookups): declare + write, NO Seal, still ok=true with hits via the
// full-scan path.
func TestLookupByContainsPendingBeforeMerge(t *testing.T) {
	g := New()
	g.IndexFullText("desc")
	ftsAdd(t, g, "a", map[string]any{"desc": "hello world"})
	ns, ok := g.LookupByContains("desc", "ello")
	if !ok || !slices.Equal(hitKeys(ns), []string{"a"}) {
		t.Fatalf("pending lookup = %v ok=%v, want [a]/true", hitKeys(ns), ok)
	}
	if ns, ok := g.LookupByContains("desc", "goodbye"); !ok || len(ns) != 0 {
		t.Fatalf("pending declared key, zero hits = %v ok=%v, want empty/true", ns, ok)
	}
}

// (e) tombstones and overrides carve/replace hits across all paths: delete a
// hit node, override a hit with a non-matching value, override back.
func TestLookupByContainsTombstoneAndOverride(t *testing.T) {
	g := New()
	g.IndexFullText("v")
	ftsAdd(t, g, "a", map[string]any{"v": "alpha beta"})
	ftsAdd(t, g, "b", map[string]any{"v": "beta gamma"})
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	if ns, ok := g.LookupByContains("v", "beta"); !ok || !slices.Equal(hitKeys(ns), []string{"a", "b"}) {
		t.Fatalf("initial hits = %v ok=%v, want [a b]/true", hitKeys(ns), ok)
	}
	if _, err := g.DeleteNode("a"); err != nil {
		t.Fatal(err)
	}
	if ns, ok := g.LookupByContains("v", "beta"); !ok || !slices.Equal(hitKeys(ns), []string{"b"}) {
		t.Fatalf("after delete = %v ok=%v, want [b]/true", hitKeys(ns), ok)
	}
	ftsAdd(t, g, "b", map[string]any{"v": "delta only"}) // override, no match
	if ns, ok := g.LookupByContains("v", "beta"); !ok || len(ns) != 0 {
		t.Fatalf("override with non-matching value = %v ok=%v, want empty/true", ns, ok)
	}
	ftsAdd(t, g, "b", map[string]any{"v": "beta omega"}) // override back
	if ns, ok := g.LookupByContains("v", "beta"); !ok || !slices.Equal(hitKeys(ns), []string{"b"}) {
		t.Fatalf("override with matching value = %v ok=%v, want [b]/true", hitKeys(ns), ok)
	}
}

// (f) end-to-end persistence: FTS declarations and gram buckets ride the
// store v7 round trip, and the reopened graph answers identically.
func TestLookupByContainsJSONRoundTrip(t *testing.T) {
	g := buildFTSGraph(t)
	var buf bytes.Buffer
	if err := g.Save(&buf); err != nil {
		t.Fatal(err)
	}
	g2, err := Load(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if ns, ok := g2.LookupByContains("title", "graphdb"); !ok || !slices.Equal(hitKeys(ns), []string{"a", "b"}) {
		t.Fatalf("round-trip fold query = %v ok=%v, want [a b]/true", hitKeys(ns), ok)
	}
	if ns, ok := g2.LookupByContains("title", "aph d"); !ok || !slices.Equal(hitKeys(ns), []string{"c"}) {
		t.Fatalf("round-trip space-crossing query = %v ok=%v, want [c]/true", hitKeys(ns), ok)
	}
	if ns, ok := g2.LookupByContains("title", "数据"); !ok || !slices.Equal(hitKeys(ns), []string{"d"}) {
		t.Fatalf("round-trip CJK query = %v ok=%v, want [d]/true", hitKeys(ns), ok)
	}
	if ns, ok := g2.LookupByContains("title", "图"); !ok || !slices.Equal(hitKeys(ns), []string{"d"}) {
		t.Fatalf("round-trip 1-rune scan query = %v ok=%v, want [d]/true", hitKeys(ns), ok)
	}
	if ns, ok := g2.LookupByContains("absent", "graphdb"); ok || ns != nil {
		t.Fatalf("round trip must keep the declared/undeclared verdict: %v ok=%v", ns, ok)
	}
}

// (g) results are ascending by node ID (insertion order here, which key
// ordering is not — so only the explicit sort explains the answer).
func TestLookupByContainsSortedByID(t *testing.T) {
	g := New()
	g.IndexFullText("v")
	ftsAdd(t, g, "zeta", map[string]any{"v": "beta one"})
	ftsAdd(t, g, "alpha", map[string]any{"v": "beta two"})
	ftsAdd(t, g, "mid", map[string]any{"v": "beta three"})
	ftsAdd(t, g, "omega", map[string]any{"v": "unrelated"})
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	ns, ok := g.LookupByContains("v", "beta")
	if !ok || !slices.Equal(hitKeys(ns), []string{"zeta", "alpha", "mid"}) {
		t.Fatalf("hits = %v ok=%v, want ID order [zeta alpha mid]", hitKeys(ns), ok)
	}
	for i := 1; i < len(ns); i++ {
		if ns[i-1].ID >= ns[i].ID {
			t.Fatalf("hits not ascending by ID: %v", ns)
		}
	}
}
