package vector

import (
	"math"
	"math/rand"
	"sort"
)

// Hit is one nearest-neighbor result from an HNSW search. Score uses the
// index metric; smaller is better for all metrics.
type Hit struct {
	ID    uint32
	Score float64
}

// HNSW is a hierarchical navigable small-world graph over caller-supplied
// uint32 ids (the vector index's vid space, spec §4.1). Phase 0 contract:
// single-threaded for BOTH build and search — the epoch-visited array is
// mutable state on the read path (data race under concurrent Search,
// verified with -race). Phase 1 must localize visited state per reader
// before immutable snapshots publish to concurrent readers; recorded as
// a spec deviation until then.
type HNSW struct {
	metric Metric
	m      int // max links per node above level 0
	m0     int // max links per node at level 0 (2m, standard)
	efc    int // construction search width
	ml     float64
	rng    *rand.Rand

	ids   []uint32     // internal index -> id
	vecs  [][]float32  // internal index -> stored vector
	level []int        // internal index -> top level
	links [][][]uint32 // links[node][level] -> internal neighbor indices
	entry int          // internal index of the entry point, -1 when empty

	// visited is an epoch-stamped array: visited[i] == stamp marks node i
	// as seen by the current searchLayer call. Cheaper than a per-search
	// map; the stamp wraps at 2^32 searches, which resets the array.
	visited []uint32
	stamp   uint32

	// Scratch buffers reused across neighbor selection (alloc profile
	// 2026-10-19: selectNeighbors+pruneTo churned 1.3GB per 10k build on
	// single-use keptVecs/used/cand slices). Valid under the Phase 0
	// single-threaded contract only; Phase 1 concurrency must move these
	// into per-call state.
	snVecs  [][]float32
	snUsed  []bool
	prCands []cand

	// searchLayer heap scratch: the returned result set aliases slResults;
	// callers finish reading it before the next searchLayer invocation
	// (Insert's per-level loop and Search's final loop both qualify).
	slCands   []cand
	slResults []cand
}

func NewHNSW(metric Metric, m, efc int, seed int64) *HNSW {
	if m <= 0 {
		m = 16
	}
	if efc <= 0 {
		efc = 200 // 100 was tried 2026-10-19: -23% build but recall@ef32 drops to 0.9940 at 100k; 200 keeps 1.0000
	}
	return &HNSW{
		metric: metric,
		m:      m,
		m0:     2 * m,
		efc:    efc,
		ml:     1 / math.Log(float64(m)),
		rng:    rand.New(rand.NewSource(seed)),
		entry:  -1,
	}
}

// Len returns the number of inserted nodes.
func (h *HNSW) Len() int { return len(h.ids) }

// All returns every (id, stored-vector) pair. Tier merges and compaction
// rebuilds use it; the slices are fresh copies.
func (h *HNSW) All() ([]uint32, [][]float32) {
	ids := append([]uint32(nil), h.ids...)
	vs := make([][]float32, len(h.vecs))
	copy(vs, h.vecs)
	return ids, vs
}

func (h *HNSW) randomLevel() int {
	u := h.rng.Float64() // [0,1); guard log(0)
	if u < 1e-9 {
		u = 1e-9
	}
	return int(-math.Log(u) * h.ml)
}

// Insert adds id with vector v. The vector is copied (and normalized for
// Cosine); the caller keeps ownership of v.
func (h *HNSW) Insert(id uint32, v []float32) {
	if h.metric == Cosine {
		v = Normalize(v)
	} else {
		v = append([]float32(nil), v...)
	}
	h.insertPrepared(id, v)
}

// insertPrepared inserts v WITHOUT copying: the caller certifies v already
// matches the metric's stored form (unit length for Cosine) and gives up
// ownership. Internal rebuild paths (State.Pack, mergeSegments, compact)
// feed vectors that State.Add or All() already normalized/copied — the
// defensive second copy in Insert was pure churn (0.14GB per 10k build).
// All()-sourced vectors alias the dying source graph; that graph becomes
// unreachable as soon as the rebuild lands, so the alias is retention-free.
func (h *HNSW) insertPrepared(id uint32, v []float32) {
	idx := len(h.ids)
	lvl := h.randomLevel()
	h.ids = append(h.ids, id)
	h.vecs = append(h.vecs, v)
	h.level = append(h.level, lvl)
	h.links = append(h.links, make([][]uint32, lvl+1))
	h.visited = append(h.visited, 0)
	if h.entry < 0 {
		h.entry = idx
		return
	}
	top := h.level[h.entry]
	cur := h.entry
	for lev := top; lev > lvl; lev-- {
		cur = h.greedyStep(lev, cur, v)
	}
	for lev := min(lvl, top); lev >= 0; lev-- {
		maxLinks := h.m
		if lev == 0 {
			maxLinks = h.m0
		}
		cands := h.searchLayer(lev, cur, v, h.efc, nil)
		for _, n := range h.selectNeighbors(maxLinks, cands, v) {
			h.links[idx][lev] = append(h.links[idx][lev], n)
			lst := append(h.links[n][lev], uint32(idx))
			h.links[n][lev] = h.pruneTo(maxLinks, lst, h.vecs[n])
		}
		if len(cands) > 0 {
			cur = cands[0].idx
		}
	}
	if lvl > top {
		h.entry = idx
	}
}

// greedyStep hill-climbs to the local minimum at one level.
func (h *HNSW) greedyStep(lev, cur int, q []float32) int {
	for {
		best := cur
		bd := Distance(h.metric, q, h.vecs[cur])
		moved := false
		for _, n := range h.links[cur][lev] {
			if d := Distance(h.metric, q, h.vecs[n]); d < bd {
				bd, best, moved = d, int(n), true
			}
		}
		if !moved {
			return best
		}
		cur = best
	}
}

// selectNeighbors returns up to maxLinks diverse neighbors from cands
// (sorted ascending by distance to base). A candidate is kept only if it
// is closer to base than to every kept neighbor (Malkov & Yashunin
// Algorithm 4); plain closest-first collapses clustered data into
// disconnected islands. Unused slots fill with pruned candidates
// (keep-pruned) so nodes stay connected.
func (h *HNSW) selectNeighbors(maxLinks int, cands []cand, base []float32) []uint32 {
	// kept is returned and stored in link lists — it must stay a fresh
	// allocation; everything else lives in reusable scratch.
	kept := make([]uint32, 0, maxLinks)
	keptVecs := h.snVecs[:0]
	used := h.snUsed
	if cap(used) < len(cands) {
		used = make([]bool, len(cands))
		h.snUsed = used
	}
	used = used[:len(cands)]
	for i := range used {
		used[i] = false
	}
	for i, c := range cands {
		if len(kept) >= maxLinks {
			break
		}
		diverse := true
		for _, kv := range keptVecs {
			if Distance(h.metric, h.vecs[c.idx], kv) < c.d {
				diverse = false
				break
			}
		}
		if diverse {
			kept = append(kept, uint32(c.idx))
			keptVecs = append(keptVecs, h.vecs[c.idx])
			used[i] = true
		}
	}
	for i := range cands {
		if len(kept) >= maxLinks {
			break
		}
		if !used[i] {
			kept = append(kept, uint32(cands[i].idx))
		}
	}
	h.snVecs = keptVecs
	return kept
}

// pruneTo re-selects owner's link list with the diversity heuristic.
func (h *HNSW) pruneTo(maxLinks int, lst []uint32, owner []float32) []uint32 {
	if len(lst) <= maxLinks {
		return lst
	}
	cs := h.prCands
	if cap(cs) < len(lst) {
		cs = make([]cand, len(lst))
		h.prCands = cs
	}
	cs = cs[:len(lst)]
	for i, n := range lst {
		cs[i] = cand{int(n), Distance(h.metric, owner, h.vecs[n])}
	}
	sort.Slice(cs, func(i, j int) bool { return cs[i].d < cs[j].d })
	return h.selectNeighbors(maxLinks, cs, owner)
}

// Search returns up to k nearest ids, ascending by score. alive may be
// nil; otherwise it filters both graph expansion and final results.
func (h *HNSW) Search(q []float32, k, ef int, alive func(uint32) bool) []Hit {
	if h.entry < 0 || k <= 0 {
		return nil
	}
	if h.metric == Cosine {
		q = Normalize(q)
	} else {
		q = append([]float32(nil), q...)
	}
	if ef < k {
		ef = k
	}
	cur := h.entry
	for lev := h.level[h.entry]; lev > 0; lev-- {
		cur = h.greedyStep(lev, cur, q)
	}
	res := h.searchLayer(0, cur, q, ef, alive)
	out := make([]Hit, 0, k)
	for _, c := range res {
		if alive != nil && !alive(h.ids[c.idx]) {
			continue
		}
		out = append(out, Hit{ID: h.ids[c.idx], Score: c.d})
		if len(out) == k {
			break
		}
	}
	return out
}

type cand struct {
	idx int
	d   float64
}

// searchLayer uses hand-rolled binary heaps instead of container/heap:
// pushing a 16-byte struct through the heap.Interface boxes it into an
// interface value, and that conversion allocated 0.57GB per 10k build
// (alloc profile 2026-10-19). Tie order differs from container/heap, so
// exact-tie graphs are not bit-identical to the old implementation;
// same-seed runs remain deterministic (TestHNSWDeterminism).

func minHeapPush(h *[]cand, c cand) {
	a := append(*h, c)
	i := len(a) - 1
	for i > 0 {
		p := (i - 1) / 2
		if a[p].d <= a[i].d {
			break
		}
		a[p], a[i] = a[i], a[p]
		i = p
	}
	*h = a
}

func minHeapPop(h *[]cand) cand {
	a := *h
	top := a[0]
	last := len(a) - 1
	a[0] = a[last]
	a = a[:last]
	i := 0
	for {
		l, r, m := 2*i+1, 2*i+2, i
		if l < len(a) && a[l].d < a[m].d {
			m = l
		}
		if r < len(a) && a[r].d < a[m].d {
			m = r
		}
		if m == i {
			break
		}
		a[i], a[m] = a[m], a[i]
		i = m
	}
	*h = a
	return top
}

func maxHeapPush(h *[]cand, c cand) {
	a := append(*h, c)
	i := len(a) - 1
	for i > 0 {
		p := (i - 1) / 2
		if a[p].d >= a[i].d {
			break
		}
		a[p], a[i] = a[i], a[p]
		i = p
	}
	*h = a
}

func maxHeapPop(h *[]cand) cand {
	a := *h
	top := a[0]
	last := len(a) - 1
	a[0] = a[last]
	a = a[:last]
	i := 0
	for {
		l, r, m := 2*i+1, 2*i+2, i
		if l < len(a) && a[l].d > a[m].d {
			m = l
		}
		if r < len(a) && a[r].d > a[m].d {
			m = r
		}
		if m == i {
			break
		}
		a[i], a[m] = a[m], a[i]
		i = m
	}
	*h = a
	return top
}

// searchLayer expands up to ef best candidates at one level, starting
// from ep. Returns the result set sorted ascending by distance.
func (h *HNSW) searchLayer(lev, ep int, q []float32, ef int, alive func(uint32) bool) []cand {
	h.stamp++
	if h.stamp == 0 { // wrapped: reset all stamps
		for i := range h.visited {
			h.visited[i] = 0
		}
		h.stamp = 1
	}
	h.visited[ep] = h.stamp
	d0 := Distance(h.metric, q, h.vecs[ep])
	cands := h.slCands[:0]
	results := h.slResults[:0]
	minHeapPush(&cands, cand{idx: ep, d: d0})
	maxHeapPush(&results, cand{idx: ep, d: d0})
	for len(cands) > 0 {
		c := minHeapPop(&cands)
		if len(results) >= ef && c.d >= results[0].d {
			break
		}
		for _, n := range h.links[c.idx][lev] {
			ni := int(n)
			if h.visited[ni] == h.stamp {
				continue
			}
			h.visited[ni] = h.stamp
			if alive != nil && !alive(h.ids[ni]) {
				continue
			}
			nd := Distance(h.metric, q, h.vecs[ni])
			if len(results) < ef || nd < results[0].d {
				minHeapPush(&cands, cand{ni, nd})
				maxHeapPush(&results, cand{ni, nd})
				if len(results) > ef {
					maxHeapPop(&results)
				}
			}
		}
	}
	sort.Slice(results, func(i, j int) bool { return results[i].d < results[j].d })
	h.slCands = cands
	h.slResults = results
	return results
}
