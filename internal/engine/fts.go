package engine

import (
	"cmp"
	"slices"
	"strings"
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
