// Package traverse implements the graph query algorithms: multi-source
// level-synchronous BFS (serial + parallel), SCC-supernode reachability,
// bidirectional shortest path, and cycle discovery. Algorithms depend only on
// engine.Snapshot's read-only accessors and caller-supplied config (label
// mask, node predicate); no write semantics.
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

// engineSnapshot is traverse's minimal dependency surface on the sealed
// snapshot: engine.Snapshot's exported accessors satisfy it naturally —
// traverse does not import engine, keeping the layering csr/dag (leaves) ->
// engine (state), with traverse (algorithms) parallel to it.
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
// bitmap.
func BFS(s engineSnapshot, starts []uint32, cfg Config) *Result {
	n := uint32(s.NodeCount())
	for _, st := range starts {
		if st >= n {
			return &Result{depth: filledNoDepth(n)}
		}
	}
	if cfg.ParallelThreshold == 0 {
		cfg.ParallelThreshold = 1 // 0 means force parallel: any non-empty layer goes parallel
	}
	adj := s.AdjacencyF()
	if cfg.Direction == DirIn {
		adj = s.AdjacencyR()
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

// Reachable returns all nodes reachable from start (including itself),
// ascending by ID. Without a label filter it takes the SCC-supernode fast
// path (queries face a DAG, zero cycle detection); with a filter, members of
// a component may be joined only by filtered-out edges, so it falls back to
// strict node-level traversal. Note: the supernode fast path does not support
// NodeFilter (the predicate conflicts with component semantics); callers must
// not take the fast path with a Config carrying a NodeFilter.
func Reachable(s engineSnapshot, start uint32, cfg Config) []uint32 {
	adj := s.AdjacencyF()
	if cfg.Direction == DirIn {
		adj = s.AdjacencyR()
	}
	if cfg.LabelMask == nil {
		return reachableSupernode(s, start, cfg.Direction)
	}
	n := uint32(s.NodeCount())
	visited := visitedPool.acquire(wordsFor(int(n)))
	defer visitedPool.release(visited)
	testAndSet(visited, start)
	frontier := []uint32{start}
	for len(frontier) > 0 {
		var next []uint32
		for _, u := range frontier {
			tg, _, lb := adj.Row(u)
			for i, v := range tg {
				if !cfg.LabelMask[lb[i]] {
					continue
				}
				if !testAndSet(visited, v) {
					next = append(next, v)
				}
			}
		}
		frontier = next
	}
	out := make([]uint32, 0, 64)
	for v := uint32(0); v < n; v++ {
		if visited[v>>5]&(1<<(v&31)) != 0 {
			out = append(out, v)
		}
	}
	return out
}

func reachableSupernode(s engineSnapshot, start uint32, dir Direction) []uint32 {
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
func ShortestPath(s engineSnapshot, from, to uint32) (nodes, edges []uint32, found bool) {
	if from == to {
		return []uint32{from}, nil, true
	}
	n := int(s.NodeCount())
	fAdj, bAdj := s.AdjacencyF(), s.AdjacencyR()
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

	for len(fFront) > 0 && len(bFront) > 0 {
		if len(fFront) <= len(bFront) {
			next, meet, ok := expandMeeting(fFront, fAdj, fVis, bVis, fParent)
			if ok {
				nodes, edges := reconstruct(s, meet, fParent, bParent)
				return nodes, edges, true
			}
			fFront = next
		} else {
			next, meet, ok := expandMeeting(bFront, bAdj, bVis, fVis, bParent)
			if ok {
				nodes, edges := reconstruct(s, meet, fParent, bParent)
				return nodes, edges, true
			}
			bFront = next
		}
	}
	return nil, nil, false
}

// expandMeeting expands one full layer and returns the new frontier; checks
// the opposite side's visited bitmap when marking.
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

func reconstruct(s engineSnapshot, meet uint32, fParent, bParent []uint32) ([]uint32, []uint32) {
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
		edges = append(edges, anyEdge(s, nodes[i], nodes[i+1]))
	}
	return nodes, edges
}

func anyEdge(s engineSnapshot, u, v uint32) uint32 {
	tg, eids, _ := s.AdjacencyF().Row(u)
	for i, t := range tg {
		if t == v {
			return eids[i]
		}
	}
	return 0 // path is contiguous by construction; unreachable in practice
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
