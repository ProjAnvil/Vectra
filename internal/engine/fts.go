package engine

import (
	"cmp"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

// gramEntry is one FTS posting entry: a two-rune gram and the node ID whose
// string property carries it.
type gramEntry struct {
	gram string
	id   uint32
}

// bigrams returns the lowercased adjacent rune-pair grams of s — r[i]+r[i+1]
// for 0 <= i < len(r)-1 over r = []rune(s). There is no padding, so a text
// of m runes yields m-1 grams ("图数据库" -> 图数 数据 据库); inputs with fewer
// than two runes (empty, single rune, a lone invalid byte collapsing to one
// U+FFFD) return nil. The result is deduplicated, keeping each gram's first
// occurrence, so the same text always yields the same grams in the order
// they appear. Non-string property values never reach this function (only
// case string values are indexed). The same function extracts needle grams,
// so index side and query side cannot disagree.
func bigrams(s string) []string {
	r := []rune(strings.ToLower(s))
	if len(r) < 2 {
		return nil
	}
	seen := make(map[string]struct{}, len(r)-1)
	grams := make([]string, 0, len(r)-1)
	for i := range len(r) - 1 {
		g := string(r[i : i+2])
		if _, dup := seen[g]; dup {
			continue
		}
		seen[g] = struct{}{}
		grams = append(grams, g)
	}
	return grams
}

// Bigrams is the needle-side entry of bigrams: the root query path
// (LookupByContains) must split search needles with the exact function the
// Seal-time indexer tokenized values with, so index side and query side can
// never disagree.
func Bigrams(s string) []string { return bigrams(s) }

// ---- Seal-side build (allocation-lean form of the bigrams pipeline) ----

// gramID is a posting in build form: the gram's dense index in a gramTable
// plus the node ID. Sorting and counting run on this 8-byte integer form;
// gram strings are only materialized into the final gramEntry slice.
type gramID struct {
	gram uint32
	id   uint32
}

// gramTable interns the grams of one key's build: each unique gram gets one
// backing string and a dense index, so millions of postings share a few
// hundred thousand gram strings instead of allocating per occurrence. The
// table is open-addressed and keyed by mixGram — a seedless hash of the
// rune pair itself, so placement depends only on the grams (builds stay
// deterministic) and every bit of the slot index is fully avalanche-mixed.
// (A plain byte hash like FNV must not be used here: FNV's low bits barely
// diffuse, and gate-shaped grams come from a tiny structured byte alphabet
// — CJK lead/continuation bytes plus a-z — which collapses low-bit masking
// into pathological probe chains.)
type gramTable struct {
	slots  []int32   // hash -> index into grams; -1 = empty (load <= 3/4)
	mask   uint32    // len(slots)-1, a power of two minus one
	grams  []string  // dense index -> gram (encounter order)
	keys   [][2]rune // dense index -> gram rune pair (probe equality)
	hashes []uint64  // dense index -> mixGram hash (O(1) rehome on grow)
	buf    [8]byte   // per-gram UTF-8 encoding scratch (max rune 4B x2)
}

func newGramTable() *gramTable {
	const initial = 1 << 12
	t := &gramTable{slots: make([]int32, initial), mask: uint32(initial - 1)}
	for i := range t.slots {
		t.slots[i] = -1
	}
	return t
}

// mixGram hashes a gram's rune pair: pack into one uint64, then the
// splitmix64 finalizer. The rune pair determines the gram bijectively, so a
// good integer mix of the pair is a good hash of the gram.
func mixGram(a, b rune) uint64 {
	x := uint64(a)<<32 | uint64(b)
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// buildFTS builds one key's gram posting list: the (gram, id)-sorted
// gramEntry slice LookupFTS binary searches. Only a node's case string value
// contributes, and the per-node gram set is exactly bigrams(v). The build
// costs three passes over in-memory data: a rune-count pass presizes the
// postings exactly (no append-doubling), a collection pass emits one
// interned (gram, id) posting per rune pair — in-value repeats included —
// and a stable counting sort orders postings by gram (node IDs arrive
// ascending) while compacting the repeats away, which is cheaper at seal
// scale than a per-value dedup scan.
func buildFTS(nodes []NodeRec, key string) []gramEntry {
	total := 0
	for i := range nodes {
		if v, ok := nodes[i].Props[key].(string); ok {
			if n := utf8.RuneCountInString(v) - 1; n > 0 {
				total += n
			}
		}
	}
	if total == 0 {
		return nil
	}
	tab := newGramTable()
	pairs := make([]gramID, 0, total)
	for id := range nodes {
		if v, ok := nodes[id].Props[key].(string); ok {
			pairs = tab.appendGrams(pairs, v, uint32(id))
		}
	}
	return tab.finish(pairs)
}

// appendGrams appends one (gram index, id) posting per lowercased rune pair
// of v, in-value repeats included — the bigrams() gram multiset. Rune
// iteration with per-rune lowering avoids the per-value ToLower/[]rune
// allocations; finish's stable counting sort groups the repeats adjacently
// and compacts them, so extraction needs no dedup scan of its own.
func (t *gramTable) appendGrams(dst []gramID, v string, id uint32) []gramID {
	var prev rune
	have := false
	for _, r := range v {
		r = unicode.ToLower(r)
		if have {
			dst = append(dst, gramID{gram: t.intern([2]rune{prev, r}), id: id})
		}
		prev, have = r, true
	}
	return dst
}

// intern maps a gram's rune pair to its dense index. Probes compare the
// stored hash and pair — 8-byte loads, no UTF-8 encoding and no gram-string
// dereference on the hot hit path; the backing string is encoded and
// allocated once, on the first miss per unique gram.
func (t *gramTable) intern(p [2]rune) uint32 {
	h := mixGram(p[0], p[1])
	i := uint32(h) & t.mask
	for {
		s := t.slots[i]
		if s < 0 {
			nb := utf8.EncodeRune(t.buf[:], p[0])
			nb += utf8.EncodeRune(t.buf[nb:], p[1])
			idx := uint32(len(t.grams))
			t.grams = append(t.grams, string(t.buf[:nb]))
			t.keys = append(t.keys, p)
			t.hashes = append(t.hashes, h)
			t.slots[i] = int32(idx)
			if len(t.grams)*4 > len(t.slots)*3 {
				t.grow()
			}
			return idx
		}
		if t.hashes[s] == h && t.keys[s] == p {
			return uint32(s)
		}
		i = (i + 1) & t.mask
	}
}

// grow doubles the slot array and rehomes every interned gram via its
// stored hash — no string decoding, O(1) per gram.
func (t *gramTable) grow() {
	slots := make([]int32, len(t.slots)*2)
	for i := range slots {
		slots[i] = -1
	}
	mask := uint32(len(slots) - 1)
	for idx, h := range t.hashes {
		i := uint32(h) & mask
		for slots[i] >= 0 {
			i = (i + 1) & mask
		}
		slots[i] = int32(idx)
	}
	t.slots, t.mask = slots, mask
}

// finish orders postings by (gram, id) and materializes the gramEntry
// slice. Unique grams are renumbered into ascending string order, then a
// stable counting sort over the dense gram indices places every posting;
// stability preserves the ascending node IDs the collection pass emitted,
// so no comparison sort touches the postings at all.
func (t *gramTable) finish(pairs []gramID) []gramEntry {
	order := make([]uint32, len(t.grams))
	for i := range order {
		order[i] = uint32(i)
	}
	slices.SortFunc(order, func(a, b uint32) int {
		return cmp.Compare(t.grams[a], t.grams[b])
	})
	sorted := make([]string, len(order))
	remap := make([]uint32, len(order))
	for new, old := range order {
		sorted[new] = t.grams[old]
		remap[old] = uint32(new)
	}

	// Rewrite posting indices to string order once; the counting passes
	// below then stream sequentially instead of chasing a random remap
	// lookup per posting.
	for i := range pairs {
		pairs[i].gram = remap[pairs[i].gram]
	}
	count := make([]uint32, len(sorted)+1)
	for _, p := range pairs {
		count[p.gram+1]++
	}
	for i := 1; i < len(count); i++ {
		count[i] += count[i-1]
	}
	// Stable scatter by gram (node IDs arrive ascending, so each bucket
	// ends up ascending by ID), then one linear compaction pass: the
	// in-value repeats appendGrams emitted sit adjacently after the sort,
	// and dropping them leaves exactly one posting per (gram, id) — the
	// same set bigrams() deduplication produces.
	out := make([]gramEntry, len(pairs))
	for _, p := range pairs {
		out[count[p.gram]] = gramEntry{gram: sorted[p.gram], id: p.id}
		count[p.gram]++
	}
	w := 0
	for i := range out {
		if w > 0 && out[w-1].id == out[i].id && out[w-1].gram == out[i].gram {
			continue
		}
		out[w] = out[i]
		w++
	}
	return out[:w]
}

// LookupFTS intersects the posting lists of grams under an FTS-declared
// property key, smallest list first; ok=false when the key was not declared
// at Seal time. A gram with an empty posting yields an empty result. The
// posting lists are sorted by (gram, id), so each gram's IDs are located by
// binary search and come out ascending.
func (s *Snapshot) LookupFTS(key string, grams []string) ([]uint32, bool) {
	postings, ok := s.fts[key]
	if !ok {
		return nil, false
	}
	if len(grams) == 0 {
		return nil, true
	}
	lists := make([][]uint32, 0, len(grams))
	for _, g := range grams {
		lo, hi := 0, len(postings)
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			if postings[mid].gram < g {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		var ids []uint32
		for i := lo; i < len(postings) && postings[i].gram == g; i++ {
			ids = append(ids, postings[i].id)
		}
		if len(ids) == 0 {
			return nil, true // an empty posting empties the intersection
		}
		lists = append(lists, ids)
	}
	// Intersect smallest list first: every pass only shrinks the candidate
	// set, so the cheapest filter runs first.
	slices.SortFunc(lists, func(a, b []uint32) int { return cmp.Compare(len(a), len(b)) })
	out := lists[0]
	for _, rest := range lists[1:] {
		out = intersectIDs(out, rest)
		if len(out) == 0 {
			return nil, true
		}
	}
	return out, true
}

// intersectIDs intersects two ascending ID slices; the result is ascending.
func intersectIDs(a, b []uint32) []uint32 {
	var out []uint32
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			i++
		case a[i] > b[j]:
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	return out
}
