package engine

import (
	"slices"
	"testing"
)

func TestBigrams(t *testing.T) {
	// The 8-rule pipeline: lowercase, adjacent rune pairs r[i]r[i+1], no
	// padding, deduped, sorted deterministically; <2 runes => nil.
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{"adjacent pairs", "abc", []string{"ab", "bc"}},
		{"case folded", "AbC", []string{"ab", "bc"}},
		{"dedup", "aaa", []string{"aa"}},
		{"cjk no padding", "图数据库", []string{"图数", "数据", "据库"}},
		{"single rune", "a", nil},
		{"empty", "", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := bigrams(tt.in); !slices.Equal(got, tt.want) {
				t.Fatalf("bigrams(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}

	// Case folding: any casing yields the lowercase gram set.
	if got, want := bigrams("AbC"), bigrams("abc"); !slices.Equal(got, want) {
		t.Fatalf(`bigrams("AbC") = %q, want %q`, got, want)
	}

	// Invalid UTF-8: []rune replaces each bad byte with U+FFFD; the path is
	// deterministic and shared, so index side and query side agree.
	if got, want := bigrams("\xff\xfe"), []string{"\ufffd\ufffd"}; !slices.Equal(got, want) {
		t.Fatalf("bigrams(2 invalid bytes) = %q, want %q", got, want)
	}
	if got := bigrams("\xff"); got != nil {
		t.Fatalf("bigrams(1 invalid byte) = %q, want nil", got)
	}

	// Determinism: repeated extraction of the same text is stable.
	for _, v := range []string{"abc", "图数据库", "\xff\xfe"} {
		if !slices.Equal(bigrams(v), bigrams(v)) {
			t.Fatalf("bigrams(%q) not deterministic", v)
		}
	}
}

func TestSealFTSBuild(t *testing.T) {
	nodes := []NodeRec{
		{Key: "a", Props: map[string]any{"name": "abc"}}, // grams ab, bc
		{Key: "b", Props: map[string]any{"name": 42}},    // int: not indexed
		{Key: "c", Props: map[string]any{"name": nil}},   // nil: not indexed
		{Key: "d", Props: map[string]any{"name": "bcb"}}, // grams bc, cb
		{Key: "e", Props: map[string]any{"other": "zz"}}, // other key: ignored
	}
	s, err := Seal(nodes, nil, nil, nil, []string{"name"})
	if err != nil {
		t.Fatal(err)
	}
	if keys := s.FTSKeys(); !slices.Equal(keys, []string{"name"}) {
		t.Fatalf("FTSKeys() = %q, want [name]", keys)
	}
	// Only string values of declared keys contribute; postings sorted by
	// (gram, id).
	want := []gramEntry{
		{gram: "ab", id: 0},
		{gram: "bc", id: 0},
		{gram: "bc", id: 3},
		{gram: "cb", id: 3},
	}
	if got := s.fts["name"]; !slices.Equal(got, want) {
		t.Fatalf("fts[name] = %v, want %v", got, want)
	}

	// No ftsKeys => no FTS state at all.
	bare, err := Seal(nodes, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if keys := bare.FTSKeys(); keys != nil {
		t.Fatalf("FTSKeys() with no declarations = %q, want nil", keys)
	}
	if ids, ok := bare.LookupFTS("name", []string{"ab"}); ok || ids != nil {
		t.Fatalf("LookupFTS on undeclared = %v, %v; want nil, false", ids, ok)
	}
}

func TestLookupFTS(t *testing.T) {
	nodes := []NodeRec{
		{Key: "a", Props: map[string]any{"name": "xxabyy bc zz"}},
		{Key: "b", Props: map[string]any{"name": "zz tail"}},
		{Key: "c", Props: map[string]any{"name": 7}}, // declared key, non-string
	}
	s, err := Seal(nodes, nil, nil, nil, []string{"name", "ghost"})
	if err != nil {
		t.Fatal(err)
	}
	if keys := s.FTSKeys(); !slices.Equal(keys, []string{"ghost", "name"}) {
		t.Fatalf("FTSKeys() = %q, want [ghost name]", keys)
	}

	// Intersection: one node carrying every query gram.
	if ids, ok := s.LookupFTS("name", []string{"ab", "bc"}); !ok || !slices.Equal(ids, []uint32{0}) {
		t.Fatalf(`LookupFTS(name, [ab bc]) = %v, %v; want [0], true`, ids, ok)
	}
	// Disjoint posting sizes still intersect: ab only on 0, zz on 0 and 1.
	if ids, ok := s.LookupFTS("name", []string{"ab", "zz"}); !ok || !slices.Equal(ids, []uint32{0}) {
		t.Fatalf(`LookupFTS(name, [ab zz]) = %v, %v; want [0], true`, ids, ok)
	}
	// Single gram: full posting list, ascending.
	if ids, ok := s.LookupFTS("name", []string{"zz"}); !ok || !slices.Equal(ids, []uint32{0, 1}) {
		t.Fatalf(`LookupFTS(name, [zz]) = %v, %v; want [0 1], true`, ids, ok)
	}

	// Any empty posting empties the intersection.
	if ids, ok := s.LookupFTS("name", []string{"ab", "qq"}); !ok || len(ids) != 0 {
		t.Fatalf(`LookupFTS(name, [ab qq]) = %v, %v; want empty, true`, ids, ok)
	}
	// Declared but no hits.
	if ids, ok := s.LookupFTS("name", []string{"qq"}); !ok || len(ids) != 0 {
		t.Fatalf(`LookupFTS(name, [qq]) = %v, %v; want empty, true`, ids, ok)
	}
	// Declared key with no string values at all.
	if ids, ok := s.LookupFTS("ghost", []string{"ab"}); !ok || len(ids) != 0 {
		t.Fatalf(`LookupFTS(ghost, [ab]) = %v, %v; want empty, true`, ids, ok)
	}
	// Undeclared key.
	if ids, ok := s.LookupFTS("nope", []string{"ab"}); ok || ids != nil {
		t.Fatalf(`LookupFTS(nope, [ab]) = %v, %v; want nil, false`, ids, ok)
	}

	// Needle==value pipeline identity: query grams come from the same
	// bigrams call that built the index, so a value searched by its own
	// text must hit exactly itself.
	if ids, ok := s.LookupFTS("name", bigrams("zz tail")); !ok || !slices.Equal(ids, []uint32{1}) {
		t.Fatalf(`LookupFTS(name, bigrams("zz tail")) = %v, %v; want [1], true`, ids, ok)
	}
}
