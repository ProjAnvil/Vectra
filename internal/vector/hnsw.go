package vector

import (
	"container/heap"
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
}

func NewHNSW(metric Metric, m, efc int, seed int64) *HNSW {
	if m <= 0 {
		m = 16
	}
	if efc <= 0 {
		efc = 200
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
	kept := make([]uint32, 0, maxLinks)
	keptVecs := make([][]float32, 0, maxLinks)
	used := make([]bool, len(cands))
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
	return kept
}

// pruneTo re-selects owner's link list with the diversity heuristic.
func (h *HNSW) pruneTo(maxLinks int, lst []uint32, owner []float32) []uint32 {
	if len(lst) <= maxLinks {
		return lst
	}
	cs := make([]cand, len(lst))
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

type candMin []cand

func (a candMin) Len() int           { return len(a) }
func (a candMin) Less(i, j int) bool { return a[i].d < a[j].d }
func (a candMin) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a *candMin) Push(x any)        { *a = append(*a, x.(cand)) }
func (a *candMin) Pop() any          { old := *a; n := len(old); x := old[n-1]; *a = old[:n-1]; return x }

type candMax []cand

func (a candMax) Len() int           { return len(a) }
func (a candMax) Less(i, j int) bool { return a[i].d > a[j].d }
func (a candMax) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a *candMax) Push(x any)        { *a = append(*a, x.(cand)) }
func (a *candMax) Pop() any          { old := *a; n := len(old); x := old[n-1]; *a = old[:n-1]; return x }

func (a candMax) peek() cand { return a[0] }

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
	cands := &candMin{{idx: ep, d: Distance(h.metric, q, h.vecs[ep])}}
	results := &candMax{{idx: ep, d: (*cands)[0].d}}
	for cands.Len() > 0 {
		c := heap.Pop(cands).(cand)
		if results.Len() >= ef && c.d >= results.peek().d {
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
			if results.Len() < ef || nd < results.peek().d {
				heap.Push(cands, cand{ni, nd})
				heap.Push(results, cand{ni, nd})
				if results.Len() > ef {
					heap.Pop(results)
				}
			}
		}
	}
	out := *results
	sort.Slice(out, func(i, j int) bool { return out[i].d < out[j].d })
	return out
}
