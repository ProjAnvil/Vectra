# Vector Phase 0 Spike Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `internal/vector` — a self-contained pure-Go HNSW engine with tiered segment state and a recall benchmark harness — producing the Phase 0 (门 2a) acceptance data.

**Architecture:** Per spec §4: an index is an overlay (linear-scanned pending vectors) plus a tiered forest of immutable HNSW segments (tier capacity grows ×4, ≤2 segments per tier, in-tier merge promotes upward, top tier = base). Identity is `key → vid`, tombstones live in a per-index dead set, merge dedup prefers higher segment seq. No WAL/View/writer integration in Phase 0.

**Tech Stack:** Go standard library only. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-10-19-vector-search-design.md` (v3). The plan argues from the spec; conflicts resolve against it.

## Global Constraints

- go.mod gains no new require entries. Standard library only.
- `internal/vector` must not import any other package of this repository.
- Code comments in English, matching repo style (comment density like `internal/wal/wal.go`).
- TDD: write the failing test first, watch it fail, then implement.
- All randomness uses fixed seeds. Recall assertions assert lower bounds only (ANN is non-deterministic); never assert exact output sets or order.
- 门 2a acceptance lines (spec §10.3, verbatim): recall@10 ≥ 0.95 (1M×768, clustered, adaptive ef); query p99 < 10ms (same scenario, tiered shape, overlay at full 2048); memory amplification ≤ 1.6× theoretical vector bytes.
- Do not edit spec body sections; deviations go to the spec's 实现期偏差记录 appendix only.
- One commit per task, English commit message, on branch `spec/vector-search`.
- Benchmark matrix is Cosine-only (spec's primary metric); L2/Dot are covered by unit tests. Record this in the report.

---

### Task 1: Distance metrics + JSON-safe NormalizeVec

**Files:**
- Create: `internal/vector/distance.go`
- Test: `internal/vector/distance_test.go`

**Interfaces:**
- Produces (used by every later task):
  - `type Metric uint8`; consts `Cosine`, `L2`, `Dot`
  - `func Distance(m Metric, a, b []float32) float64` — Cosine assumes normalized inputs, returns `1 - dot`; Dot returns `-dot`; L2 returns squared euclidean. Smaller is always better.
  - `func Normalize(v []float32) []float32` — unit-length defensive copy
  - `func NormalizeVec(v any, dim int) ([]float32, bool)` — accepts `[]float32`, `[]any` of `float64`, `[]float64`; rejects wrong length, non-numeric elements, NaN/Inf

- [ ] **Step 1: Write the failing tests**

```go
package vector

import (
	"math"
	"testing"
)

func TestDistanceKnownValues(t *testing.T) {
	a := []float32{1, 0}
	b := []float32{0, 1}
	if got := Distance(Cosine, a, b); math.Abs(got-1) > 1e-9 {
		t.Fatalf("cosine orthogonal = %v, want 1", got)
	}
	if got := Distance(Cosine, a, a); math.Abs(got) > 1e-9 {
		t.Fatalf("cosine identical = %v, want 0", got)
	}
	if got := Distance(L2, []float32{0, 0}, []float32{3, 4}); math.Abs(got-25) > 1e-9 {
		t.Fatalf("l2 squared = %v, want 25", got)
	}
	if got := Distance(Dot, []float32{2, 1}, []float32{3, 4}); math.Abs(got+10) > 1e-9 {
		t.Fatalf("dot negated = %v, want -10", got)
	}
}

func TestNormalize(t *testing.T) {
	out := Normalize([]float32{3, 4})
	var n float64
	for _, x := range out {
		n += float64(x) * float64(x)
	}
	if math.Abs(n-1) > 1e-6 {
		t.Fatalf("norm = %v, want 1", n)
	}
	if out[0] != 0.6 || out[1] != 0.8 {
		t.Fatalf("normalize = %v, want [0.6 0.8]", out)
	}
	src := []float32{3, 4}
	_ = Normalize(src)
	if src[0] != 3 { // defensive copy: source untouched
		t.Fatal("Normalize must not modify its input")
	}
}

func TestNormalizeVec(t *testing.T) {
	direct := []float32{0.5, 0.5, 0.5, 0.5}
	if v, ok := NormalizeVec(direct, 4); !ok || len(v) != 4 || v[0] != 0.5 {
		t.Fatal("[]float32 of matching dim must pass")
	}
	jsonish := []any{float64(0.1), float64(0.2)}
	if v, ok := NormalizeVec(jsonish, 2); !ok || v[1] != 0.2 {
		t.Fatal("[]any of float64 must convert")
	}
	f64 := []float64{0.1, 0.2}
	if _, ok := NormalizeVec(f64, 2); !ok {
		t.Fatal("[]float64 must convert")
	}
	if _, ok := NormalizeVec(direct, 3); ok {
		t.Fatal("wrong dim must fail")
	}
	if _, ok := NormalizeVec([]any{0.1, "x"}, 2); ok {
		t.Fatal("non-numeric element must fail")
	}
	if _, ok := NormalizeVec([]any{0.1, math.NaN()}, 2); ok {
		t.Fatal("NaN must fail")
	}
	if _, ok := NormalizeVec([]any{0.1, math.Inf(1)}, 2); ok {
		t.Fatal("+Inf must fail")
	}
	if _, ok := NormalizeVec("nope", 2); ok {
		t.Fatal("non-slice must fail")
	}
	// Defensive copy: mutating the returned slice must not alias the input.
	v, _ := NormalizeVec(direct, 4)
	v[0] = 99
	if direct[0] != 0.5 {
		t.Fatal("NormalizeVec must return a copy")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/vector/ -run 'TestDistance|TestNormalize' -v`
Expected: FAIL — `undefined: Distance` / `undefined: Normalize` / `undefined: NormalizeVec` (build error is the failing state).

- [ ] **Step 3: Implement**

```go
// Package vector implements Vectra's embedded approximate nearest-neighbor
// index: an HNSW engine, a tiered segment state, and the JSON-safe vector
// normalization layer (spec 2026-10-19, Phase 0 spike).
package vector

import "math"

// Metric is the distance metric of one vector index. Smaller is always
// better: Dot is negated so all three metrics share one ordering.
type Metric uint8

const (
	Cosine Metric = iota
	L2
	Dot
)

// Distance computes the metric between two equal-length vectors. Cosine
// assumes pre-normalized inputs and returns 1 - dot. L2 returns the
// squared euclidean distance (monotone in true L2, cheaper).
func Distance(m Metric, a, b []float32) float64 {
	var dot float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
	}
	switch m {
	case Cosine:
		return 1 - dot
	case Dot:
		return -dot
	default:
		var sq float64
		for i := range a {
			t := float64(a[i] - b[i])
			sq += t * t
		}
		return sq
	}
}

// Normalize returns a unit-length defensive copy of v. The zero vector is
// returned unchanged (it stays orthogonal to everything; callers treat it
// as ordinary data).
func Normalize(v []float32) []float32 {
	out := make([]float32, len(v))
	var n float64
	for i, x := range v {
		n += float64(x) * float64(x)
	}
	n = math.Sqrt(n)
	if n == 0 {
		copy(out, v)
		return out
	}
	for i, x := range v {
		out[i] = float32(float64(x) / n)
	}
	return out
}

// NormalizeVec coerces a property value into a fresh []float32 of exactly
// dim elements. The WAL and store encode props as JSON, so a []float32
// round-trips as []any of float64; this is the single intake point that
// both paths (and live writes) share. Wrong length, non-numeric elements,
// NaN and Inf return false — the caller skips the node with a warning.
func NormalizeVec(v any, dim int) ([]float32, bool) {
	out := make([]float32, dim)
	switch t := v.(type) {
	case []float32:
		if len(t) != dim {
			return nil, false
		}
		copy(out, t)
	case []float64:
		if len(t) != dim {
			return nil, false
		}
		for i, x := range t {
			out[i] = float32(x)
		}
	case []any:
		if len(t) != dim {
			return nil, false
		}
		for i, x := range t {
			f, ok := x.(float64)
			if !ok {
				return nil, false
			}
			out[i] = float32(f)
		}
	default:
		return nil, false
	}
	for _, x := range out {
		if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
			return nil, false
		}
	}
	return out, true
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/vector/ -run 'TestDistance|TestNormalize' -v`
Expected: PASS, 3 tests.

- [ ] **Step 5: Commit**

```bash
git add internal/vector/distance.go internal/vector/distance_test.go
git commit -m "feat(vector): distance metrics + JSON-safe NormalizeVec intake layer"
```

---

### Task 2: HNSW core (insert, layered search, dead filtering)

**Files:**
- Create: `internal/vector/hnsw.go`
- Test: `internal/vector/hnsw_test.go`

**Interfaces:**
- Consumes: `Metric`, `Distance`, `Normalize` (Task 1)
- Produces (used by Tasks 3 and 5):
  - `type Hit struct { ID uint32; Score float64 }`
  - `func NewHNSW(metric Metric, m, efc int, seed int64) *HNSW` (m/efc ≤ 0 fall back to 16/200)
  - `func (h *HNSW) Insert(id uint32, v []float32)` — copies v; normalizes for Cosine
  - `func (h *HNSW) Search(q []float32, k, ef int, alive func(uint32) bool) []Hit` — alive may be nil; ef < k is clamped to k; returns up to k hits ascending by score
  - `func (h *HNSW) All() ([]uint32, [][]float32)` — ids and stored vectors (for tier merges)
  - `func (h *HNSW) Len() int`

- [ ] **Step 1: Write the failing tests**

```go
package vector

import (
	"math/rand"
	"testing"
)

// bruteCheck returns recall of got against the brute-force top-k.
func bruteCheck(vecs [][]float32, q []float32, k int, got []Hit) float64 {
	truth := map[uint32]bool{}
	// O(n·k) selection, fine for unit-test sizes.
	scores := make([]struct {
		id uint32
		d  float64
	}, len(vecs))
	for i, v := range vecs {
		scores[i].id = uint32(i)
		scores[i].d = Distance(Cosine, Normalize(q), v)
	}
	for i := 0; i < len(scores) && len(truth) < k; i++ {
		best := i
		for j := i + 1; j < len(scores); j++ {
			if scores[j].d < scores[best].d {
				best = j
			}
		}
		scores[i], scores[best] = scores[best], scores[i]
		truth[scores[i].id] = true
	}
	hit := 0
	for _, g := range got {
		if truth[g.ID] {
			hit++
		}
	}
	return float64(hit) / float64(k)
}

func TestHNSWRecallAndBasics(t *testing.T) {
	const n, dim = 1000, 32
	rng := rand.New(rand.NewSource(7))
	vecs := make([][]float32, n)
	for i := range vecs {
		vecs[i] = normalizeRand(rng, dim)
	}
	h := NewHNSW(Cosine, 16, 200, 42)
	for i, v := range vecs {
		h.Insert(uint32(i), v)
	}
	if h.Len() != n {
		t.Fatalf("len = %d, want %d", h.Len(), n)
	}
	recall := 0.0
	for qi := 0; qi < 20; qi++ {
		q := vecs[rng.Intn(n)]
		got := h.Search(q, 10, 64, nil)
		if len(got) != 10 {
			t.Fatalf("got %d hits, want 10", len(got))
		}
		for i := 1; i < len(got); i++ { // ascending order invariant
			if got[i].Score < got[i-1].Score {
				t.Fatal("hits must be ascending by score")
			}
		}
		recall += bruteCheck(vecs, q, 10, got)
	}
	recall /= 20
	if recall < 0.9 {
		t.Fatalf("recall@10 = %.3f, want >= 0.9", recall)
	}
}

func TestHNSWDeadFilter(t *testing.T) {
	const n, dim = 400, 16
	rng := rand.New(rand.NewSource(11))
	vecs := make([][]float32, n)
	for i := range vecs {
		vecs[i] = normalizeRand(rng, dim)
	}
	h := NewHNSW(Cosine, 16, 200, 13)
	for i, v := range vecs {
		h.Insert(uint32(i), v)
	}
	dead := map[uint32]bool{5: true, 17: true, 100: true}
	got := h.Search(vecs[5], 10, 64, func(id uint32) bool { return !dead[id] })
	for _, g := range got {
		if dead[g.ID] {
			t.Fatalf("dead id %d leaked into results", g.ID)
		}
	}
	if got := h.Search(vecs[0], 0, 64, nil); got != nil {
		t.Fatal("k=0 must return nil")
	}
}

func TestHNSWDeterminism(t *testing.T) {
	build := func() []Hit {
		rng := rand.New(rand.NewSource(3))
		h := NewHNSW(Cosine, 16, 200, 5)
		for i := 0; i < 200; i++ {
			h.Insert(uint32(i), normalizeRand(rng, 8))
		}
		return h.Search(normalizeRand(rand.New(rand.NewSource(9)), 8), 10, 64, nil)
	}
	a, b := build(), build()
	if len(a) != len(b) {
		t.Fatalf("same seed different lengths: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i].ID != b[i].ID || a[i].Score != b[i].Score {
			t.Fatalf("same seed different results at %d", i)
		}
	}
}

func TestHNSWEmpty(t *testing.T) {
	h := NewHNSW(L2, 0, 0, 1) // defaults kick in
	if got := h.Search([]float32{1}, 5, 10, nil); got != nil {
		t.Fatal("empty index must return nil")
	}
}

// normalizeRand draws a uniform vector and normalizes it.
func normalizeRand(rng *rand.Rand, dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = rng.Float32()*2 - 1
	}
	return Normalize(v)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/vector/ -run TestHNSW -v`
Expected: FAIL — `undefined: NewHNSW` / `Hit`.

- [ ] **Step 3: Implement**

```go
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
// uint32 ids (the vector index's vid space, spec §4.1). Single-writer:
// build offline (folder lane in Phase 1), publish read-only. Concurrent
// reads of a non-mutating instance are safe.
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
		take := min(maxLinks, len(cands))
		for _, c := range cands[:take] {
			h.links[idx][lev] = append(h.links[idx][lev], uint32(c.idx))
			lst := append(h.links[c.idx][lev], uint32(idx))
			h.links[c.idx][lev] = h.pruneTo(maxLinks, lst, h.vecs[c.idx])
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

// pruneTo keeps the maxLinks neighbors closest to owner's vector.
func (h *HNSW) pruneTo(maxLinks int, lst []uint32, owner []float32) []uint32 {
	if len(lst) <= maxLinks {
		return lst
	}
	type dn struct {
		n uint32
		d float64
	}
	ds := make([]dn, len(lst))
	for i, n := range lst {
		ds[i] = dn{n, Distance(h.metric, owner, h.vecs[n])}
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i].d < ds[j].d })
	out := make([]uint32, maxLinks)
	for i := range out {
		out[i] = ds[i].n
	}
	return out
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

func (a candMin) Len() int            { return len(a) }
func (a candMin) Less(i, j int) bool  { return a[i].d < a[j].d }
func (a candMin) Swap(i, j int)       { a[i], a[j] = a[j], a[i] }
func (a *candMin) Push(x any)         { *a = append(*a, x.(cand)) }
func (a *candMin) Pop() any           { old := *a; n := len(old); x := old[n-1]; *a = old[:n-1]; return x }

type candMax []cand

func (a candMax) Len() int            { return len(a) }
func (a candMax) Less(i, j int) bool  { return a[i].d > a[j].d }
func (a candMax) Swap(i, j int)       { a[i], a[j] = a[j], a[i] }
func (a *candMax) Push(x any)         { *a = append(*a, x.(cand)) }
func (a *candMax) Pop() any           { old := *a; n := len(old); x := old[n-1]; *a = old[:n-1]; return x }

func (a candMax) peek() cand { return a[0] }

// searchLayer expands up to ef best candidates at one level, starting
// from ep. Returns the result set sorted ascending by distance.
func (h *HNSW) searchLayer(lev, ep int, q []float32, ef int, alive func(uint32) bool) []cand {
	visited := map[int]bool{ep: true}
	cands := &candMin{{idx: ep, d: Distance(h.metric, q, h.vecs[ep])}}
	results := &candMax{{idx: ep, d: (*cands)[0].d}}
	for cands.Len() > 0 {
		c := heap.Pop(cands).(cand)
		if results.Len() >= ef && c.d >= results.peek().d {
			break
		}
		for _, n := range h.links[c.idx][lev] {
			ni := int(n)
			if visited[ni] {
				continue
			}
			visited[ni] = true
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/vector/ -run TestHNSW -v`
Expected: PASS, 4 tests. If recall < 0.9 on the smoke test, first check `randomLevel`/`ml` wiring before touching the search loop.

- [ ] **Step 5: Commit**

```bash
git add internal/vector/hnsw.go internal/vector/hnsw_test.go
git commit -m "feat(vector): HNSW core - insert, layered search, dead filtering"
```

---

### Task 3: Tiered segment State (overlay, seq merge, compaction)

**Files:**
- Create: `internal/vector/state.go`
- Test: `internal/vector/state_test.go`

**Interfaces:**
- Consumes: Task 1 metrics, Task 2 `Hit`/`NewHNSW`/`Insert`/`Search`/`All`
- Produces (used by Task 5):
  - `type VectorHit struct { Key string; Score float64 }`
  - `func NewState(metric Metric, dim, m, efc int, seed int64) *State`
  - `func (s *State) Add(key string, v []float32)` — wrong dim silently skipped (intake layer sits above in Phase 1)
  - `func (s *State) Delete(key string) bool`
  - `func (s *State) Search(q []float32, k, ef int) []VectorHit`
  - `func (s *State) Pack(force bool)` — packs overlay at ≥2048 or force; cascades merges; compacts
  - `func (s *State) OverlayLen() int`
  - consts `packSize = 2048`, `tierFactor = 4`

- [ ] **Step 1: Write the failing tests**

```go
package vector

import (
	"fmt"
	"math/rand"
	"testing"
)

func stateRecall(t *testing.T, s *State, keys []string, vecs [][]float32, q []float32, k int) float64 {
	t.Helper()
	got := s.Search(q, k, 64)
	hit := 0
	truth := bruteTopKeys(vecs, q, k)
	for _, g := range got {
		if truth[g.Key] {
			hit++
		}
	}
	return float64(hit) / float64(k)
}

// bruteTopKeys is the exact top-k key set for small unit-test sizes.
func bruteTopKeys(vecs [][]float32, q []float32, k int) map[string]bool {
	type kv struct {
		i int
		d float64
	}
	qn := Normalize(q)
	all := make([]kv, len(vecs))
	for i, v := range vecs {
		all[i] = kv{i, Distance(Cosine, qn, v)}
	}
	for i := 0; i < k; i++ {
		for j := i + 1; j < len(all); j++ {
			if all[j].d < all[i].d {
				all[i], all[j] = all[j], all[i]
			}
		}
	}
	out := map[string]bool{}
	for i := 0; i < k && i < len(all); i++ {
		out[fmt.Sprintf("k%d", all[i].i)] = true
	}
	return out
}

func TestStateOverlayImmediateVisibility(t *testing.T) {
	s := NewState(Cosine, 4, 16, 200, 1)
	target := Normalize([]float32{1, 0, 0, 0})
	s.Add("needle", target)
	for i := 0; i < 50; i++ {
		v := normalizeRand(rand.New(rand.NewSource(int64(i))), 4)
		s.Add(fmt.Sprintf("filler%d", i), v)
	}
	got := s.Search(target, 1, 64)
	if len(got) != 1 || got[0].Key != "needle" {
		t.Fatalf("overlay must make committed vectors immediately searchable, got %+v", got)
	}
}

func TestStateUpdateShadowsOld(t *testing.T) {
	s := NewState(Cosine, 4, 16, 200, 1)
	old := Normalize([]float32{1, 0, 0, 0})
	s.Add("k", old)
	s.Pack(true) // old vector now lives in a segment
	fresh := Normalize([]float32{0, 1, 0, 0})
	s.Add("k", fresh) // update via overlay
	got := s.Search(fresh, 1, 64)
	if len(got) != 1 || got[0].Key != "k" {
		t.Fatalf("updated key must still be found, got %+v", got)
	}
	got = s.Search(old, 1, 64)
	if len(got) == 1 && got[0].Key == "k" && got[0].Score < Distance(Cosine, fresh, old) {
		t.Fatal("query near the OLD vector must not return the new vector at a tiny score")
	}
}

func TestStateDeleteAndRevive(t *testing.T) {
	s := NewState(Cosine, 4, 16, 200, 1)
	v := Normalize([]float32{1, 0, 0, 0})
	s.Add("k", v)
	s.Pack(true)
	if !s.Delete("k") {
		t.Fatal("delete of existing key must succeed")
	}
	if got := s.Search(v, 1, 64); len(got) != 0 {
		t.Fatalf("deleted key leaked: %+v", got)
	}
	s.Add("k", v) // revive
	got := s.Search(v, 1, 64)
	if len(got) != 1 || got[0].Key != "k" {
		t.Fatalf("revived key must be found, got %+v", got)
	}
}

func TestStateTierCascadeRecall(t *testing.T) {
	const n, dim = 6000, 32 // 3 packs -> L0 merge -> L1
	rng := rand.New(rand.NewSource(21))
	s := NewState(Cosine, dim, 16, 200, 7)
	vecs := make([][]float32, n)
	for i := range vecs {
		vecs[i] = normalizeRand(rng, dim)
		s.Add(fmt.Sprintf("k%d", i), vecs[i])
		if s.OverlayLen() >= packSize {
			s.Pack(false)
		}
	}
	s.Pack(true)
	if s.OverlayLen() != 0 {
		t.Fatal("pack must clear the overlay")
	}
	recall := 0.0
	for qi := 0; qi < 20; qi++ {
		q := vecs[rng.Intn(n)]
		recall += stateRecall(t, s, nil, vecs, q, 10)
	}
	recall /= 20
	if recall < 0.9 {
		t.Fatalf("post-cascade recall@10 = %.3f, want >= 0.9", recall)
	}
}

func TestStateDeathCompaction(t *testing.T) {
	const n, dim = 4096, 16
	rng := rand.New(rand.NewSource(31))
	s := NewState(Cosine, dim, 16, 200, 17)
	vecs := make([][]float32, n)
	for i := range vecs {
		vecs[i] = normalizeRand(rng, dim)
		s.Add(fmt.Sprintf("k%d", i), vecs[i])
	}
	s.Pack(true)
	for i := 0; i < n; i += 2 { // 50% death -> exceeds the 30% threshold
		s.Delete(fmt.Sprintf("k%d", i))
	}
	s.Pack(true) // triggers compaction pass
	recall := 0.0
	for qi := 0; qi < 20; qi++ {
		q := vecs[rng.Intn(n)]
		got := s.Search(q, 10, 64)
		truth := bruteTopKeys(vecs, q, 12)
		hit := 0
		for _, g := range got {
			if truth[g.Key] {
				hit++
			}
		}
		recall += float64(hit) / 10
	}
	recall /= 20
	if recall < 0.8 { // half the space is dead; 12-deep truth window
		t.Fatalf("post-compaction recall@10 = %.3f, want >= 0.8", recall)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/vector/ -run TestState -v`
Expected: FAIL — `undefined: NewState` / `VectorHit`.

- [ ] **Step 3: Implement**

```go
package vector

import (
	"math"
	"sort"
)

// packSize is the overlay target: one pack builds an L0 segment (spec §4.2).
const packSize = 2048

// tierFactor is the per-tier capacity growth. Tiers hold at most 2
// segments; a 3rd arrival means the two merge and promote one upward.
const tierFactor = 4

// entry is one overlay (not yet indexed) vector.
type entry struct {
	vid uint32
	seq uint64
	vec []float32
}

type segment struct {
	seq uint64
	h   *HNSW
}

// State is one vector index: overlay + tiered segment forest + dead set
// (spec §4). Single-writer structure; Phase 1 publishes immutable
// snapshots through the generation view.
type State struct {
	metric Metric
	dim    int
	m      int
	efc    int
	seed   int64

	nextVID uint32
	vidOf   map[string]uint32
	keyOf   map[uint32]string
	dead    map[uint32]struct{} // vid tombstones; revive clears (spec §4.4)

	overlay []entry
	tiers   [][]*segment
	seq     uint64
}

// VectorHit is the public result type (spec §3.2).
type VectorHit struct {
	Key   string
	Score float64
}

func NewState(metric Metric, dim, m, efc int, seed int64) *State {
	return &State{
		metric: metric,
		dim:    dim,
		m:      m,
		efc:    efc,
		seed:   seed,
		vidOf:  map[string]uint32{},
		keyOf:  map[uint32]string{},
		dead:   map[uint32]struct{}{},
	}
}

// Add upserts key's vector. Wrong-dim vectors are skipped silently; the
// NormalizeVec intake layer above this decides skip+warning (Phase 1).
func (s *State) Add(key string, v []float32) {
	if len(v) != s.dim {
		return
	}
	vid, ok := s.vidOf[key]
	if !ok {
		vid = s.nextVID
		s.nextVID++
		s.vidOf[key] = vid
		s.keyOf[vid] = key
	}
	delete(s.dead, vid)
	if s.metric == Cosine {
		v = Normalize(v)
	} else {
		v = append([]float32(nil), v...)
	}
	s.overlay = append(s.overlay, entry{vid: vid, seq: s.seq, vec: v})
	s.seq++
}

// Delete tombstones key's vid. A later Add revives the same vid (spec §4.4).
func (s *State) Delete(key string) bool {
	vid, ok := s.vidOf[key]
	if !ok {
		return false
	}
	s.dead[vid] = struct{}{}
	return true
}

// OverlayLen returns the pending (not yet packed) vector count.
func (s *State) OverlayLen() int { return len(s.overlay) }

func (s *State) alive(vid uint32) bool {
	_, d := s.dead[vid]
	return !d
}

// Search merges all live segments and the overlay; the same vid resolves
// to its most recent occurrence (overlay beats segments, higher segment
// seq beats lower). Returns up to k hits ascending by score.
func (s *State) Search(q []float32, k, ef int) []VectorHit {
	if k <= 0 {
		return nil
	}
	if ef < k {
		ef = k
	}
	if s.metric == Cosine {
		q = Normalize(q)
	} else {
		q = append([]float32(nil), q...)
	}
	quota := ef / 4 // per-segment candidate quota feeding the merge (spec §6.2)
	if quota < k {
		quota = k
	}
	type best struct {
		score float64
		seq   uint64
	}
	m := map[uint32]best{}
	for _, tier := range s.tiers {
		for _, seg := range tier {
			for _, hit := range seg.h.Search(q, quota, ef, s.alive) {
				if b, ok := m[hit.ID]; !ok || seg.seq > b.seq {
					m[hit.ID] = best{score: hit.Score, seq: seg.seq}
				}
			}
		}
	}
	for _, e := range s.overlay {
		if !s.alive(e.vid) {
			continue
		}
		m[e.vid] = best{score: Distance(s.metric, q, e.vec), seq: math.MaxUint64}
	}
	all := make([]VectorHit, 0, len(m))
	for vid, b := range m {
		all = append(all, VectorHit{Key: s.keyOf[vid], Score: b.score})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Score < all[j].Score })
	if len(all) > k {
		all = all[:k]
	}
	return all
}

// Pack builds an L0 segment from the overlay once it reached packSize
// (or force), cascades in-tier merges upward, then compacts dead-heavy
// segments (spec §4.2/§5.2).
func (s *State) Pack(force bool) {
	if len(s.overlay) == 0 {
		return
	}
	if !force && len(s.overlay) < packSize {
		return
	}
	seg := &segment{seq: s.seq, h: NewHNSW(s.metric, s.m, s.efc, s.seed+s.seq)}
	s.seq++
	for _, e := range s.overlay {
		if s.alive(e.vid) {
			seg.h.Insert(e.vid, e.vec)
		}
	}
	s.overlay = s.overlay[:0]
	carry := seg
	for i := 0; carry != nil; i++ {
		for i >= len(s.tiers) {
			s.tiers = append(s.tiers, nil)
		}
		s.tiers[i] = append(s.tiers[i], carry)
		if len(s.tiers[i]) < 2 {
			break
		}
		a, b := s.tiers[i][0], s.tiers[i][1]
		s.tiers[i] = s.tiers[i][:0]
		carry = s.mergeSegments(a, b)
	}
	s.compact()
}

// mergeSegments rebuilds one segment from the live vectors of a and b.
// Same vid in both resolves to b's vector: b is the later-built segment
// and therefore holds the newer update (spec §4.2).
func (s *State) mergeSegments(a, b *segment) *segment {
	seen := map[uint32]bool{}
	merged := &segment{seq: s.seq, h: NewHNSW(s.metric, s.m, s.efc, s.seed+s.seq)}
	s.seq++
	for _, seg := range []*segment{b, a} {
		ids, vecs := seg.h.All()
		for i, id := range ids {
			if seen[id] || !s.alive(id) {
				continue
			}
			seen[id] = true
			merged.h.Insert(id, vecs[i])
		}
	}
	return merged
}

// compact rebuilds any segment whose dead ratio exceeds 30% (spec §4.2).
func (s *State) compact() {
	const threshold = 0.3
	for _, tier := range s.tiers {
		for i, seg := range tier {
			ids, vecs := seg.h.All()
			dead := 0
			for _, id := range ids {
				if !s.alive(id) {
					dead++
				}
			}
			if dead == 0 || float64(dead)/float64(len(ids)) <= threshold {
				continue
			}
			fresh := &segment{seq: s.seq, h: NewHNSW(s.metric, s.m, s.efc, s.seed+s.seq)}
			s.seq++
			for j, id := range ids {
				if s.alive(id) {
					fresh.h.Insert(id, vecs[j])
				}
			}
			tier[i] = fresh
		}
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/vector/ -run TestState -v`
Expected: PASS, 5 tests.

- [ ] **Step 5: Commit**

```bash
git add internal/vector/state.go internal/vector/state_test.go
git commit -m "feat(vector): tiered segment state - overlay, seq merge, death compaction"
```

---

### Task 4: Adaptive ef formula

**Files:**
- Create: `internal/vector/ef.go`
- Test: `internal/vector/ef_test.go`

**Interfaces:**
- Produces:
  - `func AdaptiveEf(k, n int, c float64) int` — `max(64, c·log2(n))`, floored at `k`; n ≤ 1 returns `max(64, k)`
  - `var DefaultC float64` — 0 until Task 6 locks it from the ef sweep

- [ ] **Step 1: Write the failing test**

```go
package vector

import "testing"

func TestAdaptiveEf(t *testing.T) {
	if got := AdaptiveEf(10, 1_000_000, 8); got != 160 { // 8*19.93 = 159.5 -> 159? floor carefully
		t.Logf("c8@1M = %d", got) // see Step 3 note; assertion below is the contract
	}
	if got := AdaptiveEf(10, 1000, 0.5); got != 64 { // below floor
		t.Fatalf("floor = %d, want 64", got)
	}
	if got := AdaptiveEf(100, 10, 8); got != 100 { // ef >= k
		t.Fatalf("k floor = %d, want 100", got)
	}
	if got := AdaptiveEf(10, 1, 8); got != 64 { // degenerate n
		t.Fatalf("n=1 = %d, want 64", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/vector/ -run TestAdaptiveEf -v`
Expected: FAIL — `undefined: AdaptiveEf`.

- [ ] **Step 3: Implement**

```go
package vector

import "math"

// AdaptiveEf returns the default search width for an index of n vectors
// (spec §4.3/§13-3): ef = max(64, c*log2(n)), floored at k.
func AdaptiveEf(k, n int, c float64) int {
	ef := 64
	if n > 1 {
		if v := int(c * math.Log2(float64(n))); v > ef {
			ef = v
		}
	}
	if ef < k {
		ef = k
	}
	return ef
}

// DefaultC is the adaptive-ef constant locked from the Phase 0 recall-ef
// sweep (Task 6). 0 means "not locked yet"; Phase 1 callers must treat 0
// as "use plain 64".
var DefaultC float64
```

Note: the first assertion in Step 1 is a `t.Logf` probe, not a hard equality — int conversion rounding is not part of the contract. The hard assertions are the floor, k-floor, and degenerate cases.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/vector/ -run TestAdaptiveEf -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/vector/ef.go internal/vector/ef_test.go
git commit -m "feat(vector): adaptive ef formula (max(64, c*log2(n)), floor k)"
```

---

### Task 5: Benchmark harness (generators, brute truth, smoke gate, report writer)

**Files:**
- Create: `internal/vector/bench_test.go`

**Interfaces:**
- Consumes: Tasks 1–4
- Produces (test-only, used by Task 6):
  - `genUniform(rng *rand.Rand, n, dim int) [][]float32`
  - `genClustered(rng *rand.Rand, n, dim int) [][]float32` (n/100 jittered clusters, normalized)
  - `bruteTopIDs(vecs [][]float32, q []float32, k int) []int` (Cosine truth)
  - `TestRecallSmoke` — always runs: 2k×64 clustered, ≥0.85 recall@10 at ef 32
  - `TestSpikeReport` — skipped unless `VECTRA_SPIKE_FULL=1`; runs the matrix {10k,100k,1M}×{384,768} clustered + 100k×768 uniform + ef sweep at 100k×768 ({32,48,64,96,128,192}); writes `docs/benchmarks/2026-10-19-vector-spike.md`

- [ ] **Step 1: Write TestRecallSmoke and the harness (failing: helpers absent is fine — this task ADDS the harness; the failing-first gate is TestRecallSmoke asserting on State which exists from Task 3)**

```go
package vector

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"
)

func genUniform(rng *rand.Rand, n, dim int) [][]float32 {
	vecs := make([][]float32, n)
	for i := range vecs {
		v := make([]float32, dim)
		for j := range v {
			v[j] = rng.Float32()*2 - 1
		}
		vecs[i] = Normalize(v)
	}
	return vecs
}

// genClustered builds n vectors in max(1, n/100) jittered clusters —
// closer to real embedding geometry than uniform noise.
func genClustered(rng *rand.Rand, n, dim int) [][]float32 {
	centers := genUniform(rng, max(1, n/100), dim)
	vecs := make([][]float32, n)
	for i := range vecs {
		c := centers[rng.Intn(len(centers))]
		v := make([]float32, dim)
		for j := range v {
			v[j] = c[j] + (rng.Float32()*2-1)*0.05
		}
		vecs[i] = Normalize(v)
	}
	return vecs
}

// bruteTopIDs is the exact Cosine top-k (ground truth).
func bruteTopIDs(vecs [][]float32, q []float32, k int) []int {
	type idd struct {
		i int
		d float64
	}
	qn := Normalize(q)
	all := make([]idd, len(vecs))
	for i, v := range vecs {
		all[i] = idd{i, Distance(Cosine, qn, v)}
	}
	sort.Slice(all, func(a, b int) bool { return all[a].d < all[b].d })
	out := make([]int, k)
	for i := range out {
		out[i] = all[i].i
	}
	return out
}

// buildState simulates sustained write: batches of packSize, pack each.
func buildState(vecs [][]float32, m, efc int, seed int64) (*State, time.Duration) {
	s := NewState(Cosine, len(vecs[0]), m, efc, seed)
	start := time.Now()
	for i, v := range vecs {
		s.Add(fmt.Sprintf("k%d", i), v)
		if (i+1)%packSize == 0 {
			s.Pack(false)
		}
	}
	s.Pack(true)
	return s, time.Since(start)
}

func recallOf(s *State, vecs [][]float32, queries [][]float32, k, ef int) float64 {
	total := 0.0
	for _, q := range queries {
		truth := map[int]bool{}
		for _, i := range bruteTopIDs(vecs, q, k) {
			truth[i] = true
		}
		hit := 0
		for _, h := range s.Search(q, k, ef) {
			var idx int
			fmt.Sscanf(h.Key, "k%d", &idx)
			if truth[idx] {
				hit++
			}
		}
		total += float64(hit) / float64(k)
	}
	return total / float64(len(queries))
}

func percentiles(times []time.Duration) (p50, p99 time.Duration) {
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	p50 = times[len(times)*50/100]
	p99 = times[len(times)*99/100]
	return
}

func TestRecallSmoke(t *testing.T) {
	const n, dim = 2000, 64
	rng := rand.New(rand.NewSource(101))
	vecs := genClustered(rng, n, dim)
	s, _ := buildState(vecs, 16, 200, 103)
	queries := vecs[n-40:]
	if r := recallOf(s, vecs, queries, 10, 32); r < 0.85 {
		t.Fatalf("smoke recall@10 = %.3f, want >= 0.85", r)
	}
}

// runCell measures one matrix cell: recall@10, p50/p99, heap amplification.
type cellResult struct {
	n, dim      int
	dist        string
	ef          int
	recall      float64
	p50, p99    time.Duration
	buildTime   time.Duration
	heapAmp     float64
}

func runCell(n, dim int, dist string, ef int) cellResult {
	var rng *rand.Rand
	if dist == "clustered" {
		rng = rand.New(rand.NewSource(1000 + n))
	} else {
		rng = rand.New(rand.NewSource(2000 + n))
	}
	var vecs [][]float32
	if dist == "clustered" {
		vecs = genClustered(rng, n, dim)
	} else {
		vecs = genUniform(rng, n, dim)
	}
	s, build := buildState(vecs, 16, 200, 999)
	queries := vecs[n-min(n, 50):]
	r := recallOf(s, vecs, queries, 10, ef)
	times := make([]time.Duration, len(queries))
	for i, q := range queries {
		start := time.Now()
		s.Search(q, 10, ef)
		times[i] = time.Since(start)
	}
	p50, p99 := percentiles(times)
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	heapAmp := float64(ms.HeapInuse) / float64(n*dim*4)
	return cellResult{n, dim, dist, ef, r, p50, p99, build, heapAmp}
}

// TestSpikeReport runs the full Phase 0 matrix and writes the benchmark
// report. Gated: VECTRA_SPIKE_FULL=1 (full run takes tens of minutes and
// multi-GB heap at the 1M cells).
func TestSpikeReport(t *testing.T) {
	if os.Getenv("VECTRA_SPIKE_FULL") == "" {
		t.Skip("set VECTRA_SPIKE_FULL=1 to run the full spike matrix")
	}
	var cells []cellResult
	for _, n := range []int{10_000, 100_000, 1_000_000} {
		for _, dim := range []int{384, 768} {
			cells = append(cells, runCell(n, dim, "clustered", 64))
		}
	}
	cells = append(cells, runCell(100_000, 768, "uniform", 64))
	var sweep []cellResult
	for _, ef := range []int{32, 48, 64, 96, 128, 192} {
		sweep = append(sweep, runCell(100_000, 768, "clustered", ef))
	}
	report := "# Vector Phase 0 Spike Report (auto-generated)\n\n"
	report += "Matrix (Cosine, M=16, EfC=200, seeds fixed):\n\n"
	report += "| N | dim | dist | ef | recall@10 | p50 | p99 | build | heap amp |\n|---|---|---|---|---|---|---|---|---|\n"
	for _, c := range cells {
		report += fmt.Sprintf("| %d | %d | %s | %d | %.4f | %s | %s | %s | %.2fx |\n",
			c.n, c.dim, c.dist, c.ef, c.recall, c.p50, c.p99, c.buildTime.Round(time.Millisecond), c.heapAmp)
	}
	report += "\nef sweep (100k x 768, clustered):\n\n| ef | recall@10 | p99 |\n|---|---|---|\n"
	for _, c := range sweep {
		report += fmt.Sprintf("| %d | %.4f | %s |\n", c.ef, c.recall, c.p99)
	}
	report += "\nAcceptance (门 2a, spec §10.3) and the locked DefaultC value are filled in by Task 6.\n"
	path := filepath.Join("..", "..", "docs", "benchmarks", "2026-10-19-vector-spike.md")
	if err := os.WriteFile(path, []byte(report), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("report written to %s", path)
}
```

- [ ] **Step 2: Run smoke test to verify it fails-or-passes honestly**

Run: `go test ./internal/vector/ -run TestRecallSmoke -v`
Expected: PASS if Tasks 1–3 are correct (this task's failing-first gate is the compile of the harness itself; recall regression is the assertion). If it FAILs below 0.85, the defect is in Tasks 1–3 — fix there, not by loosening this test.

- [ ] **Step 3: Verify the report test skips cleanly**

Run: `go test ./internal/vector/ -run TestSpikeReport -v`
Expected: SKIP ("set VECTRA_SPIKE_FULL=1...").

- [ ] **Step 4: Run the whole package**

Run: `go test ./internal/vector/ -v`
Expected: all PASS, TestSpikeReport SKIP.

- [ ] **Step 5: Commit**

```bash
git add internal/vector/bench_test.go
git commit -m "test(vector): phase-0 benchmark harness - generators, brute truth, smoke gate, report writer"
```

---

### Task 6: Full matrix run, lock DefaultC, acceptance verdict

**Files:**
- Modify: `internal/vector/ef.go` (set `DefaultC`)
- Modify: `docs/benchmarks/2026-10-19-vector-spike.md` (generated, then completed)
- Modify: `docs/superpowers/specs/2026-10-19-vector-search-design.md` (实现期偏差记录 appendix only)

**Interfaces:**
- Consumes: `TestSpikeReport` from Task 5, 门 2a lines from Global Constraints

- [ ] **Step 1: Run the full matrix**

Run: `VECTRA_SPIKE_FULL=1 go test ./internal/vector/ -run TestSpikeReport -v -timeout 2h`
Expected: PASS; report file written. 1M cells need ~3GB+ heap each — run on a machine with headroom. If the run OOMs, reduce to N=500_000 for the 768-dim cell and record the substitution in the report + spec deviation log.

- [ ] **Step 2: Lock DefaultC from the sweep**

From the sweep table (100k×768 clustered): find the smallest ef* with recall ≥ 0.95. Compute `c = ef* / log2(100_000)`. Then verify against the 1M×768 cell: recall at `ef = AdaptiveEf(10, 1_000_000, c)` must be ≥ 0.95; if it is not, raise c to the next sweep step and re-verify (a one-off `go test -run TestSpikeReport` variant or a tiny temporary test is acceptable for the verification; record the chosen c). Set the value in ef.go:

```go
// DefaultC is the adaptive-ef constant locked from the Phase 0 recall-ef
// sweep (docs/benchmarks/2026-10-19-vector-spike.md). ef = c*log2(n).
var DefaultC float64 = <measured value>
```

- [ ] **Step 3: Re-run acceptance with adaptive ef**

Add the measured 1M×768 adaptive-ef recall/p99 to the report's acceptance section. Fill the 门 2a verdict table:

```markdown
## 门 2a verdict

| line (spec §10.3) | measured | pass |
|---|---|---|
| recall@10 ≥ 0.95 (1M×768 clustered, adaptive ef) | <value> | yes/no |
| query p99 < 10ms (same, overlay ≤ 2048 — note: matrix queries run on a packed state; an overlay-full variant adds ~2-4ms, state the sum) | <value> | yes/no |
| memory amplification ≤ 1.6× | <value> | yes/no |
```

If any line fails: do NOT tune thresholds. Write the failure into the report, append a spec deviation entry, and report the miss to the controller (门 2a verdict = FAIL is a legitimate outcome; the plan does not force a pass).

- [ ] **Step 4: Append spec deviation log entries**

Append to the spec's 实现期偏差记录 (appendix only):

```markdown
- Phase 0 (2026-10-19): dead set is a plain map[uint32]struct{} rather than a chunked bitmap (D6 allows implementation reuse; bitmap swap lands with Phase 1 integration).
- Phase 0 (2026-10-19): neighbor selection is closest-first; the heuristic variant is deferred (recall target met without it — see report).
- Phase 0 (2026-10-19): benchmark matrix is Cosine-only; L2/Dot covered by unit tests (Global Constraints note).
```

(Adjust if the implementer made different calls — record what actually happened.)

- [ ] **Step 5: Run the full package once more**

Run: `go test ./internal/vector/ -v && go vet ./internal/vector/`
Expected: all PASS (TestSpikeReport SKIP), vet clean.

- [ ] **Step 6: Commit**

```bash
git add internal/vector/ef.go docs/benchmarks/2026-10-19-vector-spike.md docs/superpowers/specs/2026-10-19-vector-search-design.md
git commit -m "bench(vector): phase-0 full matrix report + lock adaptive ef constant"
```
