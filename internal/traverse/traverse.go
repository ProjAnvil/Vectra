// Package traverse implements the graph query algorithms: multi-source
// level-synchronous BFS (serial + parallel), SCC-supernode reachability,
// bidirectional shortest path, and cycle discovery. Algorithms depend only on
// read-only row accessors and caller-supplied config (label mask, node
// predicate); no write semantics.
//
// Two row surfaces (v0.7 spec §3.2): the plain snapshot (csr slice-zip hot
// path, exposed via FastAdjacency) and the three-source View (Rows over
// base ∪ delta layers). RowSource is traverse's local dependency surface —
// engine's View satisfies it structurally, keeping traverse free of any
// engine import (the layering csr/dag (leaves) -> engine (state), with
// traverse (algorithms) parallel to it).
package traverse

import (
	"cmp"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/projanvil/vectra/internal/csr"
	"github.com/projanvil/vectra/internal/dag"
)

// engineSnapshot is traverse's dependency surface on SCC structures, which
// exist only on a merged base: engine.Snapshot's exported accessors satisfy
// it naturally. Used by the supernode reachability fast path and Cycles
// (which callers run against a merged snapshot).
type engineSnapshot interface {
	AdjacencyF() csr.Adjacency
	AdjacencyR() csr.Adjacency
	CondensedF() dag.Condensed
	CondensedR() dag.Condensed
	NodeCount() int
	SCCOf(v uint32) uint32
	SCCCount() uint32
	SCCMembers(scc uint32) []uint32
	SelfLoop(v uint32) bool
}

// RowSource is the three-source traversal surface: adjacency rows by node ID
// and direction, the union ID-space node count, and the empty-delta fast
// path (the plain base CSR — slice-zip hot loop, A1). Direction codes: 0 =
// out, 1 = in. engine.View implements this interface structurally.
type RowSource interface {
	// Rows returns u's visible adjacency rows (base ∪ layers, tombstones
	// applied, label IDs in the source's joint label table).
	Rows(u uint32, dir int) []csr.AdjRow
	// NodeCount returns the union ID-space size (visited/depth bound).
	NodeCount() int
	// FastAdjacency returns the base CSR and true when no delta source
	// contributes (traversal then runs entirely on the CSR hot path).
	FastAdjacency(dir int) (csr.Adjacency, bool)
}

// rower is the three-source View's direction-fixed row accessor: rows come
// from the RowSource zipped into the parallel slices the View-path algorithm
// bodies consume. The CSR fast path does NOT go through it — the algorithms
// keep the original direct adj.Row loops (tiny leaf functions the compiler
// inlines; an interface hop per node measurably regressed small-hot-graph
// BFS, A1).
type rower interface {
	Row(u uint32) (targets, edgeIDs, labels []uint32)
}

// viewRow adapts a RowSource and direction.
type viewRow struct {
	src RowSource
	dir int
}

func (v viewRow) Row(u uint32) ([]uint32, []uint32, []uint32) {
	rows := v.src.Rows(u, v.dir)
	tg := make([]uint32, len(rows))
	eids := make([]uint32, len(rows))
	lbs := make([]uint32, len(rows))
	for i, r := range rows {
		tg[i], eids[i], lbs[i] = r.Target, r.EdgeID, r.LabelID
	}
	return tg, eids, lbs
}

type Direction uint8

const (
	DirOut Direction = iota
	DirIn
)

const (
	// DefaultParallelThreshold is the frontier size at which expansion
	// switches to parallel.
	DefaultParallelThreshold = 1024
	parallelChunk            = 256
	noDepth                  = ^uint32(0)
	noParent                 = ^uint32(0)
)

// Config holds all tunables of a traversal. LabelMask is built by the caller
// from its label registry (nil = no filtering); NodeFilter is evaluated when a
// new node is marked (start nodes always pass).
type Config struct {
	MaxDepth          uint32
	LabelMask         []bool
	Direction         Direction
	ParallelThreshold int
	NodeFilter        func(uint32) bool
}

// Result is the BFS result: per-layer members + depth array (noDepth =
// unreached).
type Result struct {
	Layers [][]uint32
	depth  []uint32
}

// DepthRaw returns the depth array itself (reused by the facade to avoid a
// whole-graph copy; read-only contract).
func (r *Result) DepthRaw() []uint32 { return r.depth }

// BFS is a multi-source level-synchronous breadth-first search: starts at
// depth 0, each layer expanded as a whole; once the frontier reaches the
// threshold, chunks go to multiple goroutines, deduplicated via an atomic-Or
// bitmap. src carries the whole visible graph: the plain base CSR when no
// delta layers are unmerged (FastAdjacency hot path — the CSR slice-zip loop
// runs unchanged, A1) or the three-source View otherwise.
func BFS(src RowSource, starts []uint32, cfg Config) *Result {
	n := uint32(src.NodeCount())
	for _, st := range starts {
		if st >= n {
			return &Result{depth: filledNoDepth(n)}
		}
	}
	if cfg.ParallelThreshold == 0 {
		cfg.ParallelThreshold = 1 // 0 means force parallel: any non-empty layer goes parallel
	}
	visited := visitedPool.acquire(wordsFor(int(n)))
	defer visitedPool.release(visited)

	res := &Result{depth: filledNoDepth(n)}
	frontier := make([]uint32, 0, len(starts))
	for _, st := range starts {
		if !testAndSet(visited, st) {
			frontier = append(frontier, st)
			res.depth[st] = 0
		}
	}
	res.Layers = append(res.Layers, frontier)
	if adj, ok := src.FastAdjacency(int(cfg.Direction)); ok {
		for depth := uint32(1); depth <= cfg.MaxDepth && len(frontier) > 0; depth++ {
			var next []uint32
			if len(frontier) >= cfg.ParallelThreshold {
				next = expandLayerParallel(adj, cfg, visited, frontier)
			} else {
				next = expandLayerSerial(adj, cfg, visited, frontier)
			}
			for _, v := range next {
				res.depth[v] = depth
			}
			res.Layers = append(res.Layers, next)
			frontier = next
		}
		return res
	}
	r := viewRow{src: src, dir: int(cfg.Direction)}
	for depth := uint32(1); depth <= cfg.MaxDepth && len(frontier) > 0; depth++ {
		var next []uint32
		if len(frontier) >= cfg.ParallelThreshold {
			next = expandRowsParallel(r, cfg, visited, frontier)
		} else {
			next = expandRowsSerial(r, cfg, visited, frontier)
		}
		for _, v := range next {
			res.depth[v] = depth
		}
		res.Layers = append(res.Layers, next)
		frontier = next
	}
	return res
}

func filledNoDepth(n uint32) []uint32 {
	d := make([]uint32, n)
	for i := range d {
		d[i] = noDepth
	}
	return d
}

func testAndSet(words []uint32, v uint32) bool {
	w := &words[v>>5]
	bit := uint32(1) << (v & 31)
	if *w&bit != 0 {
		return true
	}
	*w |= bit
	return false
}

func accept(v uint32, cfg Config) bool {
	return cfg.NodeFilter == nil || cfg.NodeFilter(v)
}

// expandLayerSerial is the CSR fast path (original hot loop: adj.Row is a
// tiny leaf the compiler inlines — keep it direct, no interface).
func expandLayerSerial(adj csr.Adjacency, cfg Config, visited []uint32, frontier []uint32) []uint32 {
	var next []uint32
	for _, u := range frontier {
		tg, _, lb := adj.Row(u)
		for i, v := range tg {
			if cfg.LabelMask != nil && !cfg.LabelMask[lb[i]] {
				continue
			}
			if !testAndSet(visited, v) && accept(v, cfg) {
				next = append(next, v)
			}
		}
	}
	return next
}

func expandLayerParallel(adj csr.Adjacency, cfg Config, visited []uint32, frontier []uint32) []uint32 {
	workers := min(runtime.GOMAXPROCS(0), (len(frontier)+parallelChunk-1)/parallelChunk)
	if workers <= 1 {
		return expandLayerSerial(adj, cfg, visited, frontier)
	}
	chunk := (len(frontier) + workers - 1) / workers
	locals := make([][]uint32, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		lo := w * chunk
		if lo >= len(frontier) {
			break
		}
		hi := min(lo+chunk, len(frontier))
		wg.Add(1)
		go func(w, lo, hi int) {
			defer wg.Done()
			local := make([]uint32, 0, (hi-lo)*2)
			for _, u := range frontier[lo:hi] {
				tg, _, lb := adj.Row(u)
				for i, v := range tg {
					if cfg.LabelMask != nil && !cfg.LabelMask[lb[i]] {
						continue
					}
					if !accept(v, cfg) {
						continue
					}
					word := &visited[v>>5]
					bit := uint32(1) << (v & 31)
					if atomic.OrUint32(word, bit)&bit != 0 {
						continue // already marked by another worker
					}
					local = append(local, v)
				}
			}
			locals[w] = local
		}(w, lo, hi)
	}
	wg.Wait()
	var next []uint32
	for _, l := range locals {
		next = append(next, l...)
	}
	return next
}

// expandRowsSerial/expandRowsParallel are the three-source View path: rows
// come zipped from the rower (View rows per node).
func expandRowsSerial(r rower, cfg Config, visited []uint32, frontier []uint32) []uint32 {
	var next []uint32
	for _, u := range frontier {
		tg, _, lb := r.Row(u)
		for i, v := range tg {
			if cfg.LabelMask != nil && !cfg.LabelMask[lb[i]] {
				continue
			}
			if !testAndSet(visited, v) && accept(v, cfg) {
				next = append(next, v)
			}
		}
	}
	return next
}

func expandRowsParallel(r rower, cfg Config, visited []uint32, frontier []uint32) []uint32 {
	workers := min(runtime.GOMAXPROCS(0), (len(frontier)+parallelChunk-1)/parallelChunk)
	if workers <= 1 {
		return expandRowsSerial(r, cfg, visited, frontier)
	}
	chunk := (len(frontier) + workers - 1) / workers
	locals := make([][]uint32, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		lo := w * chunk
		if lo >= len(frontier) {
			break
		}
		hi := min(lo+chunk, len(frontier))
		wg.Add(1)
		go func(w, lo, hi int) {
			defer wg.Done()
			local := make([]uint32, 0, (hi-lo)*2)
			for _, u := range frontier[lo:hi] {
				tg, _, lb := r.Row(u)
				for i, v := range tg {
					if cfg.LabelMask != nil && !cfg.LabelMask[lb[i]] {
						continue
					}
					if !accept(v, cfg) {
						continue
					}
					word := &visited[v>>5]
					bit := uint32(1) << (v & 31)
					if atomic.OrUint32(word, bit)&bit != 0 {
						continue // already marked by another worker
					}
					local = append(local, v)
				}
			}
			locals[w] = local
		}(w, lo, hi)
	}
	wg.Wait()
	var next []uint32
	for _, l := range locals {
		next = append(next, l...)
	}
	return next
}

// Reachable returns all nodes reachable from start (including itself),
// ascending by ID, via strict node-level traversal over the row source —
// always correct, and the only option when a label filter is set or delta
// layers are unmerged (supernode structures exist only on a merged base).
// The SCC-supernode fast path is ReachableSupernode; the caller picks it
// (no NodeFilter support there — the predicate conflicts with component
// semantics).
func Reachable(src RowSource, start uint32, cfg Config) []uint32 {
	n := uint32(src.NodeCount())
	visited := visitedPool.acquire(wordsFor(int(n)))
	defer visitedPool.release(visited)
	testAndSet(visited, start)
	frontier := []uint32{start}
	if adj, ok := src.FastAdjacency(int(cfg.Direction)); ok {
		for len(frontier) > 0 {
			var next []uint32
			for _, u := range frontier {
				tg, _, lb := adj.Row(u)
				for i, v := range tg {
					if cfg.LabelMask != nil && !cfg.LabelMask[lb[i]] {
						continue
					}
					if !testAndSet(visited, v) {
						next = append(next, v)
					}
				}
			}
			frontier = next
		}
	} else {
		r := viewRow{src: src, dir: int(cfg.Direction)}
		for len(frontier) > 0 {
			var next []uint32
			for _, u := range frontier {
				tg, _, lb := r.Row(u)
				for i, v := range tg {
					if cfg.LabelMask != nil && !cfg.LabelMask[lb[i]] {
						continue
					}
					if !testAndSet(visited, v) {
						next = append(next, v)
					}
				}
			}
			frontier = next
		}
	}
	out := make([]uint32, 0, 64)
	for v := uint32(0); v < n; v++ {
		if visited[v>>5]&(1<<(v&31)) != 0 {
			out = append(out, v)
		}
	}
	return out
}

// ReachableSupernode is the SCC-condensation fast path: BFS over the
// condensation DAG of a MERGED snapshot (each visited bit is one component),
// members collected per component. Callers must only use it without a label
// filter or NodeFilter and with no unmerged delta.
func ReachableSupernode(s engineSnapshot, start uint32, dir Direction) []uint32 {
	cond := s.CondensedF()
	if dir == DirIn {
		cond = s.CondensedR()
	}
	seen := make([]bool, s.SCCCount())
	startSCC := s.SCCOf(start)
	seen[startSCC] = true
	frontier := []uint32{startSCC}
	for len(frontier) > 0 {
		var next []uint32
		for _, c := range frontier {
			tg, _ := cond.Row(c)
			for _, d := range tg {
				if !seen[d] {
					seen[d] = true
					next = append(next, d)
				}
			}
		}
		frontier = next
	}
	var out []uint32
	for c := uint32(0); c < s.SCCCount(); c++ {
		if !seen[c] {
			continue
		}
		out = append(out, s.SCCMembers(c)...)
	}
	slices.Sort(out)
	return out
}

// ShortestPath is a bidirectional BFS: the smaller frontier side expands a
// full layer first, checking the opposite side's visited bitmap when marking;
// the first meeting point is optimal. Returns node and edge sequences.
// src carries the whole visible graph (CSR fast path or three-source View).
func ShortestPath(src RowSource, from, to uint32) (nodes, edges []uint32, found bool) {
	if from == to {
		return []uint32{from}, nil, true
	}
	n := int(src.NodeCount())
	fVis := visitedPool.acquire(wordsFor(n))
	bVis := visitedPool.acquire(wordsFor(n))
	defer visitedPool.release(fVis)
	defer visitedPool.release(bVis)
	fParent := make([]uint32, n)
	bParent := make([]uint32, n)
	for i := range fParent {
		fParent[i], bParent[i] = noParent, noParent
	}

	fFront := []uint32{from}
	bFront := []uint32{to}
	fVis[from>>5] |= 1 << (from & 31)
	bVis[to>>5] |= 1 << (to & 31)

	if fAdj, ok := src.FastAdjacency(int(DirOut)); ok {
		bAdj, _ := src.FastAdjacency(int(DirIn))
		for len(fFront) > 0 && len(bFront) > 0 {
			if len(fFront) <= len(bFront) {
				next, meet, ok := expandMeeting(fFront, fAdj, fVis, bVis, fParent)
				if ok {
					nodes, edges := reconstructCSR(fAdj, meet, fParent, bParent)
					return nodes, edges, true
				}
				fFront = next
			} else {
				next, meet, ok := expandMeeting(bFront, bAdj, bVis, fVis, bParent)
				if ok {
					nodes, edges := reconstructCSR(fAdj, meet, fParent, bParent)
					return nodes, edges, true
				}
				bFront = next
			}
		}
		return nil, nil, false
	}
	fRow, bRow := viewRow{src: src, dir: int(DirOut)}, viewRow{src: src, dir: int(DirIn)}
	for len(fFront) > 0 && len(bFront) > 0 {
		if len(fFront) <= len(bFront) {
			next, meet, ok := expandMeetingRows(fFront, fRow, fVis, bVis, fParent)
			if ok {
				nodes, edges := reconstructRows(fRow, meet, fParent, bParent)
				return nodes, edges, true
			}
			fFront = next
		} else {
			next, meet, ok := expandMeetingRows(bFront, bRow, bVis, fVis, bParent)
			if ok {
				nodes, edges := reconstructRows(fRow, meet, fParent, bParent)
				return nodes, edges, true
			}
			bFront = next
		}
	}
	return nil, nil, false
}

// expandMeeting expands one full layer and returns the new frontier; checks
// the opposite side's visited bitmap when marking. CSR fast path (original
// hot loop: Neighbors is a slice view, no interface).
func expandMeeting(frontier []uint32, adj csr.Adjacency, mine, theirs []uint32, parent []uint32) (next []uint32, meet uint32, found bool) {
	for _, u := range frontier {
		for _, v := range adj.Neighbors(u) {
			w := &mine[v>>5]
			bit := uint32(1) << (v & 31)
			if *w&bit != 0 {
				continue
			}
			*w |= bit
			parent[v] = u
			next = append(next, v)
			if !found && theirs[v>>5]&bit != 0 {
				meet = v
				found = true
			}
		}
	}
	return next, meet, found
}

func expandMeetingRows(frontier []uint32, r rower, mine, theirs []uint32, parent []uint32) (next []uint32, meet uint32, found bool) {
	for _, u := range frontier {
		for _, v := range neighborsOf(r, u) {
			w := &mine[v>>5]
			bit := uint32(1) << (v & 31)
			if *w&bit != 0 {
				continue
			}
			*w |= bit
			parent[v] = u
			next = append(next, v)
			if !found && theirs[v>>5]&bit != 0 {
				meet = v
				found = true
			}
		}
	}
	return next, meet, found
}

// neighborsOf is the rower's neighbor view (ShortestPath never needs edge
// IDs during expansion — only for reconstruction).
func neighborsOf(r rower, u uint32) []uint32 {
	tg, _, _ := r.Row(u)
	return tg
}

func reconstructCSR(adj csr.Adjacency, meet uint32, fParent, bParent []uint32) ([]uint32, []uint32) {
	fwd := []uint32{meet} // meet -> from, reversed afterwards
	for v := fParent[meet]; v != noParent; v = fParent[v] {
		fwd = append(fwd, v)
	}
	for i, j := 0, len(fwd)-1; i < j; i, j = i+1, j-1 {
		fwd[i], fwd[j] = fwd[j], fwd[i]
	}
	var bwd []uint32 // meet -> to
	for v := bParent[meet]; v != noParent; v = bParent[v] {
		bwd = append(bwd, v)
	}
	nodes := append(fwd, bwd...)
	edges := make([]uint32, 0, len(nodes)-1)
	for i := 0; i+1 < len(nodes); i++ {
		edges = append(edges, anyEdgeCSR(adj, nodes[i], nodes[i+1]))
	}
	return nodes, edges
}

func anyEdgeCSR(adj csr.Adjacency, u, v uint32) uint32 {
	tg, eids, _ := adj.Row(u)
	for i, t := range tg {
		if t == v {
			return eids[i]
		}
	}
	return 0 // path is contiguous by construction; unreachable in practice
}

func reconstructRows(r rower, meet uint32, fParent, bParent []uint32) ([]uint32, []uint32) {
	fwd := []uint32{meet} // meet -> from, reversed afterwards
	for v := fParent[meet]; v != noParent; v = fParent[v] {
		fwd = append(fwd, v)
	}
	for i, j := 0, len(fwd)-1; i < j; i, j = i+1, j-1 {
		fwd[i], fwd[j] = fwd[j], fwd[i]
	}
	var bwd []uint32 // meet -> to
	for v := bParent[meet]; v != noParent; v = bParent[v] {
		bwd = append(bwd, v)
	}
	nodes := append(fwd, bwd...)
	edges := make([]uint32, 0, len(nodes)-1)
	for i := 0; i+1 < len(nodes); i++ {
		tg, eids, _ := r.Row(nodes[i])
		for x, t := range tg {
			if t == nodes[i+1] {
				edges = append(edges, eids[x])
				break
			}
		}
	}
	return nodes, edges
}

// CycleEdge is one edge with both endpoints on the cycle: an intra-SCC edge
// of a multi-node cycle, or the self-loop of a singleton cycle.
type CycleEdge struct {
	Src    uint32
	Dst    uint32
	EdgeID uint32
}

// Cycle is one circular dependency: the member list of a multi-node SCC plus
// its intra-member edges, or a single self-loop node and its loop edge.
type Cycle struct {
	Nodes []uint32
	Edges []CycleEdge
}

func (c Cycle) Len() int { return len(c.Nodes) }

// Cycles returns all circular dependencies, descending by member count, then
// ascending by smallest member ID. Edges includes every intra-SCC edge — the
// perimeter plus chords — because both determine the cycle's severity.
func Cycles(s engineSnapshot) []Cycle {
	var cycles []Cycle
	for c := uint32(0); c < s.SCCCount(); c++ {
		if mem := s.SCCMembers(c); len(mem) > 1 {
			cycles = append(cycles, Cycle{Nodes: mem, Edges: intraSCCEdges(s, mem)})
		}
	}
	n := s.NodeCount()
	for v := 0; v < n; v++ {
		if s.SelfLoop(uint32(v)) {
			cycles = append(cycles, Cycle{
				Nodes: []uint32{uint32(v)},
				Edges: selfLoopEdges(s, uint32(v)),
			})
		}
	}
	slices.SortFunc(cycles, func(a, b Cycle) int {
		if a.Len() != b.Len() {
			return b.Len() - a.Len()
		}
		return int(a.Nodes[0]) - int(b.Nodes[0])
	})
	return cycles
}

// intraSCCEdges collects the edges whose endpoints are both component
// members. CSR rows keep insertion order, so the result is explicitly sorted
// by (src, dst, edgeID) — a canonical order independent of write sequence.
func intraSCCEdges(s engineSnapshot, mem []uint32) []CycleEdge {
	in := make(map[uint32]struct{}, len(mem))
	for _, m := range mem {
		in[m] = struct{}{}
	}
	var out []CycleEdge
	for _, u := range mem { // SCCMembers are ascending
		tg, eids, _ := s.AdjacencyF().Row(u)
		for i, v := range tg {
			if _, ok := in[v]; ok {
				out = append(out, CycleEdge{Src: u, Dst: v, EdgeID: eids[i]})
			}
		}
	}
	slices.SortFunc(out, func(a, b CycleEdge) int {
		return cmp.Or(
			cmp.Compare(a.Src, b.Src),
			cmp.Compare(a.Dst, b.Dst),
			cmp.Compare(a.EdgeID, b.EdgeID),
		)
	})
	return out
}

func selfLoopEdges(s engineSnapshot, v uint32) []CycleEdge {
	tg, eids, _ := s.AdjacencyF().Row(v)
	var out []CycleEdge
	for i, w := range tg {
		if w == v {
			out = append(out, CycleEdge{Src: v, Dst: v, EdgeID: eids[i]})
		}
	}
	return out
}

// ---- visited bitmap buffer pool ----

type bitmapPool struct {
	p sync.Pool
}

func (bp *bitmapPool) acquire(words int) []uint32 {
	if v, ok := bp.p.Get().(*[]uint32); ok && len(*v) >= words {
		b := (*v)[:words]
		clear(b)
		return b
	}
	return make([]uint32, words)
}

func (bp *bitmapPool) release(b []uint32) {
	clear(b)
	bp.p.Put(&b)
}

var visitedPool bitmapPool

func wordsFor(n int) int { return (n + 31) / 32 }
