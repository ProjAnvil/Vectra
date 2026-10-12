// Package traverse 实现图的查询算法：多源层同步 BFS（串行+并行）、
// SCC 超节点可达性、双向最短路径、循环发现。算法只依赖 engine.Snapshot
// 的只读访问器与调用方传入的配置（标签掩码、节点谓词），不含写入语义。
package traverse

import (
	"runtime"
	"slices"
	"sync"
	"sync/atomic"

	"vectra/internal/csr"
	"vectra/internal/dag"
)

// engineSnapshot 是 traverse 对密封快照的最小依赖面：engine.Snapshot
// 的导出访问器天然满足此接口——traverse 不 import engine，层级保持
// csr/dag（叶）→ engine（状态）与 traverse（算法）平行。
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
	// DefaultParallelThreshold 是 frontier 达到该规模时切换并行展开。
	DefaultParallelThreshold = 1024
	parallelChunk            = 256
	noDepth                  = ^uint32(0)
	noParent                 = ^uint32(0)
)

// Config 是一次遍历的全部可调项。LabelMask 由调用方按其标签注册表
// 构建（nil = 不过滤）；NodeFilter 在标记新节点时求值（起点恒入选）。
type Config struct {
	MaxDepth          uint32
	LabelMask         []bool
	Direction         Direction
	ParallelThreshold int
	NodeFilter        func(uint32) bool
}

// Result 是 BFS 结果：按层成员 + 深度数组（noDepth = 未到达）。
type Result struct {
	Layers [][]uint32
	depth  []uint32
}

// DepthRaw 返回深度数组本体（门面复用，避免整图拷贝；只读契约）。
func (r *Result) DepthRaw() []uint32 { return r.depth }

// BFS 多源层同步广度优先：起点深度 0，每层整体展开；frontier 达到
// 阈值时切块交给多 goroutine，去重用原子 Or 位图。
func BFS(s engineSnapshot, starts []uint32, cfg Config) *Result {
	n := uint32(s.NodeCount())
	for _, st := range starts {
		if st >= n {
			return &Result{depth: filledNoDepth(n)}
		}
	}
	if cfg.ParallelThreshold == 0 {
		cfg.ParallelThreshold = 1 // 0 视为强制并行：任何非空层都并行
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
						continue // 已被其他 worker 标记
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

// Reachable 返回从 start 出发（含自身）可达的全部节点，按 ID 升序。
// 无标签过滤时走 SCC 超节点快路径（查询面对 DAG，零环检测）；
// 有过滤时分量成员间可能只有被过滤的边，退回严格节点级遍历。
// 注意：超节点快路径不支持 NodeFilter（谓词与分量语义冲突），
// 调用方不得在带 NodeFilter 的 Config 上走快路径。
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

// ShortestPath 双向 BFS：小前沿一侧优先整层展开，标记时检测对侧
// visited，首个相遇点即最优。返回节点与边序列。
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

// expandMeeting 完整展开一层并返回新前沿；标记时检测对侧 visited。
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
	fwd := []uint32{meet} // meet -> from，逆序
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
	return 0 // 路径保证连续，正常不可达
}

// Cycle 是一个循环依赖：多节点 SCC 的成员列表，或一个自环节点。
type Cycle struct {
	Nodes []uint32
}

func (c Cycle) Len() int { return len(c.Nodes) }

// Cycles 返回全部循环依赖，按成员数降序、再按最小成员 ID 升序。
func Cycles(s engineSnapshot) []Cycle {
	var cycles []Cycle
	for c := uint32(0); c < s.SCCCount(); c++ {
		if mem := s.SCCMembers(c); len(mem) > 1 {
			cycles = append(cycles, Cycle{Nodes: mem})
		}
	}
	n := s.NodeCount()
	for v := 0; v < n; v++ {
		if s.SelfLoop(uint32(v)) {
			cycles = append(cycles, Cycle{Nodes: []uint32{uint32(v)}})
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

// ---- visited 位图缓冲池 ----

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
