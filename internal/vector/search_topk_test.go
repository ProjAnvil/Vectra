package vector

// v0.11.3 hotfix net for the vecView.search overlay-tail changes:
//   (1) dead-empty fast path - len(v.dead)==0 skips the per-entry alive()
//       closure in the overlay scan (every entry is alive by definition);
//   (2) bounded top-k selection - selectTopK (size-k max-heap, then an
//       ascending sort of the <=k winners) replaces collect-all +
//       sort.Slice(all) + truncate.
//
// Equivalence is a randomized differential test against
// referenceSearchAll, a test-local frozen copy of the OLD merge/collect/
// sort code kept as the oracle. The oracle sorts the FULL candidate list
// by (score,key) and does not truncate, yielding a deterministic
// canonical order; the old production code sorted by score only
// (sort.Slice, unstable) and cut at k. The contract that old code offered
// beyond "the k smallest SCORES" is: the key SET of every tie group the
// cut keeps whole (order inside a group was never stable), and free
// membership inside the one tie group the cut splits. diffTopK enforces
// exactly that - scores exact element-wise, keys compared per score
// group as sets, cut-split group membership checked against the full
// group. Comparing plain sorted (score,key) pairs of both truncated
// results would wrongly pin tie order and membership the old unstable
// sort never guaranteed.

import (
	"fmt"
	"math"
	"sort"
	"testing"
)

// referenceSearchAll is the pre-hotfix vecView.search body (quota merge,
// seq-aware vid dedup, per-entry alive check in the overlay scan), sorted
// by (score,key), NOT truncated. Frozen oracle - do not update it to
// follow production changes; it exists to certify this hotfix only.
func referenceSearchAll(v vecView, q []float32, k, ef int, r *Reader) []VectorHit {
	if k <= 0 || len(q) != v.dim {
		return nil
	}
	if v.metric == Cosine {
		q = Normalize(q)
	} else {
		q = append([]float32(nil), q...)
	}
	if ef < k {
		ef = k
	}
	quota := ef / 4
	if quota < k {
		quota = k
	}
	alive := func(vid uint32) bool {
		ch, ok := v.dead[vid>>16]
		return !ok || ch[(vid&0xffff)>>6]&(1<<(vid&63)) == 0
	}
	type best struct {
		key   string
		score float64
		seq   uint64
	}
	m := map[uint32]best{}
	for _, seg := range v.segs {
		for _, c := range seg.h.searchIdx(q, quota, ef, alive, r) {
			vid := seg.h.ids[c.idx]
			if b, ok := m[vid]; !ok || seg.seq > b.seq {
				m[vid] = best{key: seg.keys[c.idx], score: c.d, seq: seg.seq}
			}
		}
	}
	for _, ch := range v.overlay {
		for _, e := range ch {
			if !alive(e.vid) {
				continue
			}
			m[e.vid] = best{key: e.key, score: Distance(v.metric, q, e.vec), seq: math.MaxUint64}
		}
	}
	all := make([]VectorHit, 0, len(m))
	for _, b := range m {
		all = append(all, VectorHit{Key: b.key, Score: b.score})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].Score != all[j].Score {
			return all[i].Score < all[j].Score
		}
		return all[i].Key < all[j].Key
	})
	return all
}

// diffTopK asserts got (production output: ascending, len<=k) against full
// (oracle canonical full list): equal length min(k,len(full)), scores
// exact element-wise, and keys compared PER SCORE GROUP - as sets for
// groups the cut keeps whole (the old sort.Slice was unstable, so ORDER
// inside a tie group was never contractual), and as membership-subsets
// for the one group the cut splits (which of the tied candidates makes
// the cut was equally free).
func diffTopK(t *testing.T, label string, got, full []VectorHit, k int) {
	t.Helper()
	want := full
	if len(want) > k {
		want = want[:k]
	}
	if len(got) != len(want) {
		t.Fatalf("%s: len got %d want %d", label, len(got), len(want))
	}
	if len(got) == 0 {
		return
	}
	for i := 1; i < len(got); i++ { // ascending is the production contract
		if got[i].Score < got[i-1].Score {
			t.Fatalf("%s: result not ascending at %d", label, i)
		}
	}
	for i := range got {
		if got[i].Score != want[i].Score {
			t.Fatalf("%s: score[%d] got %v want %v", label, i, got[i].Score, want[i].Score)
		}
	}
	fullCount := map[float64]int{}
	for _, h := range full {
		fullCount[h.Score]++
	}
	// Walk maximal score groups (both lists ascending, scores aligned).
	for a := 0; a < len(want); {
		b := a + 1
		for b < len(want) && want[b].Score == want[a].Score {
			b++
		}
		gotKeys := map[string]int{}
		for _, h := range got[a:b] {
			gotKeys[h.Key]++
		}
		wantKeys := map[string]int{}
		for _, h := range want[a:b] {
			wantKeys[h.Key]++
		}
		if fullCount[want[a].Score] > b-a { // cut splits this group
			allow := map[string]bool{}
			for _, h := range full {
				if h.Score == want[a].Score {
					allow[h.Key] = true
				}
			}
			for key := range gotKeys {
				if !allow[key] {
					t.Fatalf("%s: cut-tie group score %v: key %q not in group", label, want[a].Score, key)
				}
			}
		} else {
			if len(gotKeys) != len(wantKeys) {
				t.Fatalf("%s: tie group score %v: key sets differ", label, want[a].Score)
			}
			for key, n := range gotKeys {
				if wantKeys[key] != n {
					t.Fatalf("%s: tie group score %v: key %q not in kept set", label, want[a].Score, key)
				}
			}
		}
		a = b
	}
}

// lcg is a deterministic float generator; the worlds and query batteries
// must be reproducible across runs for failures to be actionable.
type lcg struct{ s uint32 }

func (l *lcg) frac() float32 { l.s = l.s*1664525 + 1013904223; return float32(l.s>>8&0xffff) / 65535 }

func (l *lcg) vec(dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = l.frac()*2 - 1
	}
	return v
}

// buildEquivalenceStages builds one evolving state stressing every merge
// rule the hotfix must preserve: cross-chunk duplicate vids (re-Adds
// spanning pack boundaries), segment-vs-overlay dup vids (overlay wins),
// dead bits on segment-resident and overlay-only vids, revives (unset),
// multiple segments + a live tail, and exact tie groups (identical
// vectors under distinct keys) that k cuts. Returns one snapshot per
// stage plus stored tie vectors for the query battery.
func buildEquivalenceStages() ([]*Snapshot, [][]float32) {
	const dim = 16
	r := lcg{s: 0xC0FFEE}
	s := NewState(Cosine, dim, 8, 64, 42)
	key := func(i int) string { return fmt.Sprintf("k%05d", i) }
	var snaps []*Snapshot
	snap := func() { snaps = append(snaps, s.Snapshot()) }

	// Stage 0: overlay only, dead map empty - the fast path's home case.
	for i := 0; i < 300; i++ {
		s.Add(key(i), r.vec(dim))
	}
	snap()
	// Stage 1: force-pack -> one segment, still no deletes (dead-empty
	// with a segment present); re-Adds of 40 keys span the pack boundary.
	s.Pack(true)
	for i := 0; i < 40; i++ {
		s.Add(key(i), r.vec(dim)) // vid now in segment AND overlay
	}
	snap()
	// Stage 2: deletes, revives, overlay-only tombstones, exact ties.
	for i := 300; i < 700; i++ {
		s.Add(key(i), r.vec(dim))
	}
	for i := 100; i < 180; i++ {
		s.Delete(key(i)) // segment-resident vids die
	}
	for i := 120; i < 140; i++ {
		s.Add(key(i), r.vec(dim)) // revive half: dead bits unset
	}
	for i := 600; i < 660; i++ {
		s.Delete(key(i)) // overlay-only vids die before any pack
	}
	tie1, tie2, tie3 := r.vec(dim), r.vec(dim), r.vec(dim)
	s.Add(key(701), tie1) // identical vectors, distinct vids+keys
	s.Add(key(702), tie1)
	s.Add(key(703), tie2)
	s.Add(key(704), tie2)
	s.Add(key(705), tie3)
	s.Add(key(706), tie3)
	snap()
	// Stage 3: pack the tail (two segments; newest segment wins dup vids
	// via seq), then a fresh tail mixing dup vids, a third tie-vector
	// generation, and deletes against the OLDER segment.
	s.Pack(true)
	for i := 0; i < 60; i++ {
		s.Add(key(i), r.vec(dim)) // vid in both segments AND overlay
	}
	for i := 707; i < 760; i++ {
		s.Add(key(i), tie1) // wide exact-tie group on the overlay side
	}
	for i := 200; i < 230; i++ {
		s.Delete(key(i))
	}
	snap()
	return snaps, [][]float32{tie1, tie2, tie3}
}

// TestSearchEquivalence is the real net: production search vs the frozen
// pre-hotfix oracle over all stages x queries x k x ef. Stages 0-1 run
// with an empty dead map (dead-empty fast path), stages 2-3 with a
// populated one; the oracle always pays the per-entry alive check.
func TestSearchEquivalence(t *testing.T) {
	snaps, stored := buildEquivalenceStages()
	r := lcg{s: 0x5EED}
	qs := make([][]float32, 0, 44)
	for i := 0; i < 40; i++ {
		qs = append(qs, r.vec(16))
	}
	qs = append(qs, stored...) // stored tie vectors: top hits land inside tie groups
	for si, snap := range snaps {
		for qi, q := range qs {
			for _, k := range []int{1, 10, 100} {
				for _, ef := range []int{32, 100} {
					label := fmt.Sprintf("stage%d q%d k%d ef%d", si, qi, k, ef)
					got := snap.Search(q, k, ef, &Reader{})
					full := referenceSearchAll(snap.v, q, k, ef, &Reader{})
					diffTopK(t, label, got, full, k)
				}
			}
		}
	}
}

// TestTopKSelect unit-drives selectTopK (it mutates its input, so each
// case builds fresh) against diffTopK with a synthetic oracle: the same
// hits sorted by (score,key). Tie cases pin the cut-group rule.
func TestTopKSelect(t *testing.T) {
	hits := func(ss ...float64) []VectorHit {
		out := make([]VectorHit, len(ss))
		for i, sc := range ss {
			out[i] = VectorHit{Key: fmt.Sprintf("k%03d", i), Score: sc}
		}
		return out
	}
	distinct := func(n int) []float64 {
		ss := make([]float64, n)
		for i := range ss {
			ss[i] = float64((i*7)%101) + 0.5
		}
		return ss
	}
	tests := []struct {
		name   string
		scores []float64
		k      int
	}{
		{"empty input", nil, 5},
		{"k exceeds len", []float64{3, 1, 2}, 10},
		{"k equals len", []float64{3, 1, 2}, 3},
		{"k one distinct", []float64{5, 1, 9, 3, 7}, 1},
		{"k middle distinct", distinct(100), 5},
		{"k zero-ish len", []float64{4}, 1},
		{"ties away from cut", []float64{1, 2, 2, 2, 3, 4, 4, 5}, 3},
		{"tie at the cut", []float64{1, 2, 2, 2, 3}, 2},
		{"all equal k middle", []float64{7, 7, 7, 7, 7, 7, 7}, 3},
		{"reverse sorted input", []float64{9, 8, 7, 6, 5, 4, 3, 2, 1, 0}, 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := hits(tt.scores...)
			full := make([]VectorHit, len(in))
			copy(full, in)
			sort.Slice(full, func(i, j int) bool {
				if full[i].Score != full[j].Score {
					return full[i].Score < full[j].Score
				}
				return full[i].Key < full[j].Key
			})
			got := selectTopK(in, tt.k)
			diffTopK(t, tt.name, got, full, tt.k)
		})
	}
}
