package engine

import (
	"cmp"
	"slices"
	"testing"
)

// The allocation-lean seal build (buildFTS: interned grams, flat extraction,
// presized postings, counting-sort compaction) replaced the old
// bigrams()-per-node pipeline. Two of its invariants were covered only by
// the transient gate-benchmark round; these tests pin them against the
// public Seal entry with exact posting assertions, in the TestSealFTSBuild
// style: (1) in-value gram repeats compact to one posting, and (2) the rune
// path agrees with bigrams() on non-ASCII and invalid-UTF-8 values.

// TestSealFTSDuplicateGramCompaction pins the in-value repeat rule: a value
// posts its gram SET, not its gram multiset. "aaa" contains the pair "aa"
// twice but posts it once; "abab" posts ab and ba once each despite both
// pairs recurring. The counting-sort compaction must land exactly where the
// old per-value dedup did.
func TestSealFTSDuplicateGramCompaction(t *testing.T) {
	nodes := []NodeRec{
		{Key: "a", Props: map[string]any{"name": "aaa"}},  // aa x2 -> once
		{Key: "b", Props: map[string]any{"name": "abab"}}, // ab x2, ba x2 -> once each
	}
	s, err := Seal(nodes, nil, nil, nil, []string{"name"})
	if err != nil {
		t.Fatal(err)
	}
	want := []gramEntry{
		{gram: "aa", id: 0},
		{gram: "ab", id: 1},
		{gram: "ba", id: 1},
	}
	if got := s.fts["name"]; !slices.Equal(got, want) {
		t.Fatalf("fts[name] = %v, want %v", got, want)
	}
	// The compacted postings still answer intersection queries.
	for _, q := range []struct {
		grams []string
		want  []uint32
	}{
		{[]string{"aa"}, []uint32{0}},
		{[]string{"ab", "ba"}, []uint32{1}},
	} {
		if ids, ok := s.LookupFTS("name", q.grams); !ok || !slices.Equal(ids, q.want) {
			t.Fatalf("LookupFTS(name, %v) = %v, %v; want %v, true", q.grams, ids, ok, q.want)
		}
	}
}

// TestSealFTSNonASCIIBuildPath pins the rune path of the seal build against
// bigrams(): CJK text yields its exact adjacent-pair grams with correct IDs,
// and an invalid-UTF-8 value builds without panic to exactly the grams
// Bigrams() yields for the same string (range-over-string and []rune both
// map each bad byte to one U+FFFD, so "\xff\xfeab" grams to
// ["\uFFFD\uFFFD", "\uFFFDa", "ab"]).
func TestSealFTSNonASCIIBuildPath(t *testing.T) {
	t.Run("cjk exact postings", func(t *testing.T) {
		nodes := []NodeRec{
			{Key: "zh", Props: map[string]any{"name": "图数据库"}}, // 图数 数据 据库
			{Key: "skip", Props: map[string]any{"name": 9}},    // non-string: no grams
		}
		s, err := Seal(nodes, nil, nil, nil, []string{"name"})
		if err != nil {
			t.Fatal(err)
		}
		// Byte order: 图数 (E5..) < 据库 (E6 8D..) < 数据 (E6 95..) — the
		// natural reading order differs from the sort order here; every
		// posting at id 0 (the int-valued node contributes nothing).
		want := []gramEntry{
			{gram: "图数", id: 0},
			{gram: "据库", id: 0},
			{gram: "数据", id: 0},
		}
		if got := s.fts["name"]; !slices.Equal(got, want) {
			t.Fatalf("fts[name] = %v, want %v", got, want)
		}
		if ids, ok := s.LookupFTS("name", Bigrams("图数据库")); !ok || !slices.Equal(ids, []uint32{0}) {
			t.Fatalf("LookupFTS(name, CJK grams) = %v, %v; want [0], true", ids, ok)
		}
	})
	t.Run("invalid utf-8 no panic, grams match Bigrams", func(t *testing.T) {
		const bad = "\xff\xfeab" // two invalid bytes, then valid ASCII
		if got := Bigrams(bad); !slices.Equal(got, []string{"\uFFFD\uFFFD", "\uFFFDa", "ab"}) {
			t.Fatalf("Bigrams(%q) = %q; needle side drifted", bad, got)
		}
		s, err := Seal([]NodeRec{{Key: "bad", Props: map[string]any{"name": bad}}}, nil, nil, nil, []string{"name"})
		if err != nil {
			t.Fatal(err)
		}
		var want []gramEntry
		for _, g := range Bigrams(bad) {
			want = append(want, gramEntry{gram: g, id: 0})
		}
		slices.SortFunc(want, func(a, b gramEntry) int {
			if a.gram != b.gram {
				return cmp.Compare(a.gram, b.gram)
			}
			return cmp.Compare(a.id, b.id)
		})
		if got := s.fts["name"]; !slices.Equal(got, want) {
			t.Fatalf("fts[name] = %v, want %v (Bigrams(%q) posted at id 0)", got, want, bad)
		}
	})
}
