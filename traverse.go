package vectra

import (
	"runtime"
	"sync"
	"sync/atomic"
)

type Direction uint8

const (
	DirOut Direction = iota
	DirIn
)

// BFSResult 是一次 BFS 的完整结果：按层分组的成员 + 每节点深度。
type BFSResult struct {
	Layers [][]NodeID
	Depths map[NodeID]uint32
}

// Nodes 返回全部到达节点（按层拼接）。
func (r *BFSResult) Nodes() []NodeID {
	total := 0
	for _, l := range r.Layers {
		total += len(l)
	}
	out := make([]NodeID, 0, total)
	for _, l := range r.Layers {
		out = append(out, l...)
	}
	return out
}

// Count 返回到达节点总数（含起点）。
func (r *BFSResult) Count() int {
	total := 0
	for _, l := range r.Layers {
		total += len(l)
	}
	return total
}

type bfsConfig struct {
	maxDepth    uint32
	labelNames  []string
	direction   Direction
	parallelThr int
}

type BFSOption func(*bfsConfig)

func WithMaxDepth(d uint32) BFSOption       { return func(c *bfsConfig) { c.maxDepth = d } }
func WithDirection(d Direction) BFSOption   { return func(c *bfsConfig) { c.direction = d } }
func WithParallelThreshold(n int) BFSOption { return func(c *bfsConfig) { c.parallelThr = n } }

// WithLabels 只沿给定标签的边遍历。空列表 = 不过滤；标签不存在时
// 结果只剩起点。
func WithLabels(names ...string) BFSOption {
	return func(c *bfsConfig) { c.labelNames = names }
}

// BFS 多源层同步广度优先：起点深度 0，每层整体展开。层同步是并行化
// 的基础——frontier 是扁平数组，达到阈值时切块交给多个 goroutine。
func (g *Graph) BFS(starts []NodeID, opts ...BFSOption) (*BFSResult, error) {
	if !g.sealed {
		return nil, ErrNotSealed
	}
	cfg := bfsConfig{maxDepth: ^uint32(0), parallelThr: defaultParallelThreshold}
	for _, o := range opts {
		o(&cfg)
	}
	n := uint32(len(g.nodes))
	for _, s := range starts {
		if uint32(s) >= n {
			return nil, ErrNodeNotFound
		}
	}
	adj := g.view(cfg.direction)
	mask := g.labelMask(cfg.labelNames)

	visited := visitedPool.acquire(wordsFor(int(n)))
	defer visitedPool.release(visited)

	res := &BFSResult{Depths: make(map[NodeID]uint32, 16)}
	frontier := make([]NodeID, 0, len(starts))
	for _, s := range starts {
		if !testAndSet(visited, uint32(s)) {
			frontier = append(frontier, s)
			res.Depths[s] = 0
		}
	}
	res.Layers = append(res.Layers, frontier)
	for depth := uint32(1); depth <= cfg.maxDepth && len(frontier) > 0; depth++ {
		var next []NodeID
		if len(frontier) >= cfg.parallelThr {
			next = g.expandLayerParallel(adj, mask, visited, frontier)
		} else {
			next = g.expandLayerSerial(adj, mask, visited, frontier)
		}
		for _, v := range next {
			res.Depths[v] = depth
		}
		res.Layers = append(res.Layers, next)
		frontier = next
	}
	return res, nil
}

// labelMask 把标签名集合转成按 labelID 索引的布尔掩码；
// names 为空返回 nil（不过滤）。
func (g *Graph) labelMask(names []string) []bool {
	if len(names) == 0 {
		return nil
	}
	mask := make([]bool, len(g.labels))
	for _, n := range names {
		if id, ok := g.labIDs[n]; ok {
			mask[id] = true
		}
	}
	return mask
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

func (g *Graph) expandLayerSerial(adj adjacencyView, mask []bool, visited []uint32, frontier []NodeID) []NodeID {
	var next []NodeID
	for _, u := range frontier {
		lo, hi := adj.row(uint32(u))
		for s := lo; s < hi; s++ {
			if mask != nil && !mask[adj.label(s)] {
				continue
			}
			v := adj.target(s)
			if !testAndSet(visited, v) {
				next = append(next, NodeID(v))
			}
		}
	}
	return next
}

// adjacencyView 把 CSR 的三个平行数组压成一个可拷贝的值类型，
// 避免 traverse 层持有内部包类型的指针。
type adjacencyView struct {
	off, tgt, lab []uint32
}

func (a adjacencyView) row(u uint32) (uint32, uint32) { return a.off[u], a.off[u+1] }
func (a adjacencyView) target(s uint32) uint32        { return a.tgt[s] }
func (a adjacencyView) label(s uint32) uint32         { return a.lab[s] }

func (g *Graph) view(dir Direction) adjacencyView {
	if dir == DirIn {
		return adjacencyView{off: g.csrR.Offsets, tgt: g.csrR.Targets, lab: g.csrR.Labels}
	}
	return adjacencyView{off: g.csrF.Offsets, tgt: g.csrF.Targets, lab: g.csrF.Labels}
}

const (
	defaultParallelThreshold = 1024
	parallelChunk            = 256 // frontier 切块的最小粒度
)

// expandLayerParallel 将 frontier 切块分给多个 goroutine 并行展开。
// 去重靠原子 Or 位图：worker 置位前该位为 0 才拥有该节点，
// 无锁也无 CAS 重试。这是层同步 morsel 驱动并行的串行退路。
func (g *Graph) expandLayerParallel(adj adjacencyView, mask []bool, visited []uint32, frontier []NodeID) []NodeID {
	workers := runtime.GOMAXPROCS(0)
	if m := (len(frontier) + parallelChunk - 1) / parallelChunk; m < workers {
		workers = m
	}
	if workers <= 1 {
		return g.expandLayerSerial(adj, mask, visited, frontier)
	}
	chunk := (len(frontier) + workers - 1) / workers
	locals := make([][]NodeID, workers)
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
			local := make([]NodeID, 0, (hi-lo)*2)
			for _, u := range frontier[lo:hi] {
				start, end := adj.row(uint32(u))
				for s := start; s < end; s++ {
					if mask != nil && !mask[adj.label(s)] {
						continue
					}
					v := adj.target(s)
					w := &visited[v>>5]
					bit := uint32(1) << (v & 31)
					if atomic.OrUint32(w, bit)&bit != 0 {
						continue // 已被其他 worker 标记
					}
					local = append(local, NodeID(v))
				}
			}
			locals[w] = local
		}(w, lo, hi)
	}
	wg.Wait()
	var next []NodeID
	for _, l := range locals {
		next = append(next, l...)
	}
	return next
}
