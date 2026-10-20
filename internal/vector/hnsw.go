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

// Reader holds one goroutine's search state: the epoch-stamped visited
// array and the candidate/result heap scratch. Search is race-free when
// every goroutine owns its Reader (State.NewReader); buffers grow on
// demand and are reused across queries and segments — sequential reuse
// only within one goroutine. Phase 1 concurrency prerequisite: these
// used to live on HNSW as read-path mutable state (spec deviation log,
// 2026-10-19 entry, now resolved).
type Reader struct {
	visited []uint32 // visited[i] == stamp marks node i seen this searchLayer call
	stamp   uint32   // wraps at 2^32: full array reset on wrap
	cands   []cand   // searchLayer min-heap scratch
	results []cand   // searchLayer max-heap scratch; returned slice aliases it
}

// HNSW is a hierarchical navigable small-world graph over caller-supplied
// uint32 ids (the vector index's vid space, spec §4.1). Concurrent-read
// contract: Search takes the caller's *Reader (per-goroutine state); the
// graph itself is immutable after build. Build-path scratch (below) is
// single-writer: one folder lane builds a given HNSW at a time.
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

	// Build-path scratch, reused across insertPrepared's searchLayer calls
	// (alloc profile 2026-10-19: 1.3GB per 10k build on single-use slices).
	// Single-writer: the folder lane that packs this segment.
	snVecs  [][]float32
	snUsed  []bool
	prCands []cand
	build   Reader // searchLayer scratch for insertPrepared (build side)
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
		cands := h.searchLayer(lev, cur, v, h.efc, nil, &h.build)
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

// sortCandsAsc sorts cands ascending by distance. pruneTo calls it once
// per neighbor per insert on <=maxLinks+1 (<=33) elements, where
// sort.Slice's reflect swapper cost more than the sort itself; insertion
// sort wins at this size. Oversized inputs (defensive: link lists are
// capped at maxLinks) fall back to pdqsort to avoid quadratic blowup.
func sortCandsAsc(cs []cand) {
	if len(cs) > 64 {
		sort.Slice(cs, func(i, j int) bool { return cs[i].d < cs[j].d })
		return
	}
	for i := 1; i < len(cs); i++ {
		c := cs[i]
		j := i - 1
		for j >= 0 && cs[j].d > c.d {
			cs[j+1] = cs[j]
			j--
		}
		cs[j+1] = c
	}
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
	sortCandsAsc(cs)
	return h.selectNeighbors(maxLinks, cs, owner)
}

// Search returns up to k nearest ids, ascending by score. alive may be
// nil; otherwise it filters both graph expansion and final results. The
// caller's Reader carries per-goroutine scratch; one Reader serves
// sequential queries on one goroutine (State.SearchReader loops segments
// on the same one).
func (h *HNSW) Search(q []float32, k, ef int, alive func(uint32) bool, r *Reader) []Hit {
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
	res := h.searchLayer(0, cur, q, ef, alive, r)
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
// from ep. Returns the result set sorted ascending by distance; the
// returned slice aliases the Reader's results scratch (valid until the
// next searchLayer call on that Reader).
func (h *HNSW) searchLayer(lev, ep int, q []float32, ef int, alive func(uint32) bool, r *Reader) []cand {
	r.stamp++
	if r.stamp == 0 { // wrapped: reset all stamps
		for i := range r.visited {
			r.visited[i] = 0
		}
		r.stamp = 1
	}
	// append-based growth keeps inserts amortized: insertPrepared adds one
	// node per call, and an exact-length copy here made every insert
	// re-copy the whole array (measured 9.7GB of alloc churn on a 65k
	// build, audit 2026-10-20). Zeroed tail entries are safe: stamp >= 1.
	if len(r.visited) < len(h.ids) {
		r.visited = append(r.visited, make([]uint32, len(h.ids)-len(r.visited))...)
	}
	r.visited[ep] = r.stamp
	d0 := Distance(h.metric, q, h.vecs[ep])
	cands := r.cands[:0]
	results := r.results[:0]
	minHeapPush(&cands, cand{idx: ep, d: d0})
	maxHeapPush(&results, cand{idx: ep, d: d0})
	for len(cands) > 0 {
		c := minHeapPop(&cands)
		if len(results) >= ef && c.d >= results[0].d {
			break
		}
		for _, n := range h.links[c.idx][lev] {
			ni := int(n)
			if r.visited[ni] == r.stamp {
				continue
			}
			r.visited[ni] = r.stamp
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
	r.cands = cands
	r.results = results
	return results
}
