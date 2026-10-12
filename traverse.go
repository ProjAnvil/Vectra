package vectra

import (
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
)

type Direction uint8

const (
	DirOut Direction = iota
	DirIn
)

// noDepth 是"未到达"哨兵。
const noDepth = ^uint32(0)

// BFSResult 是一次 BFS 的完整结果：按层分组的成员 + 每节点深度。
// 深度用平坦 uint32 数组而非 map——百万级结果集上 map 插入成本
// 会吞掉 CSR 的访存优势（基准验证过），数组的 GC 成本也趋近于零。
type BFSResult struct {
	Layers [][]NodeID
	depth  []uint32 // 未到达 = noDepth
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

// Depth 返回节点深度；未到达返回 math.MaxUint32。
func (r *BFSResult) Depth(id NodeID) uint32 { return r.depth[id] }

// Reached 报告节点是否到达。
func (r *BFSResult) Reached(id NodeID) bool { return r.depth[id] != noDepth }

// DepthsMap 按需构建"节点->深度"映射（小结果集的便捷接口；
// 大结果集请用 Depth/Reached/Nodes）。
func (r *BFSResult) DepthsMap() map[NodeID]uint32 {
	m := make(map[NodeID]uint32, r.Count())
	for d, l := range r.Layers {
		for _, n := range l {
			m[n] = uint32(d)
		}
	}
	return m
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

	res := &BFSResult{depth: make([]uint32, n)}
	for i := range res.depth {
		res.depth[i] = noDepth
	}
	frontier := make([]NodeID, 0, len(starts))
	for _, s := range starts {
		if !testAndSet(visited, uint32(s)) {
			frontier = append(frontier, s)
			res.depth[s] = 0
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
			res.depth[v] = depth
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

// adjacencyView 把 CSR 的四个平行数组压成一个可拷贝的值类型，
// 避免 traverse 层持有内部包类型的指针。
type adjacencyView struct {
	off, tgt, lab, eid []uint32
}

func (a adjacencyView) row(u uint32) (uint32, uint32) { return a.off[u], a.off[u+1] }
func (a adjacencyView) target(s uint32) uint32        { return a.tgt[s] }
func (a adjacencyView) label(s uint32) uint32         { return a.lab[s] }
func (a adjacencyView) edge(s uint32) uint32          { return a.eid[s] }

func (g *Graph) view(dir Direction) adjacencyView {
	if dir == DirIn {
		a := g.csrR
		return adjacencyView{off: a.Offsets, tgt: a.Targets, lab: a.Labels, eid: a.EdgeIDs}
	}
	a := g.csrF
	return adjacencyView{off: a.Offsets, tgt: a.Targets, lab: a.Labels, eid: a.EdgeIDs}
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

type reachConfig struct {
	labelNames []string
	direction  Direction
}

type ReachOption func(*reachConfig)

func ReachWithLabels(names ...string) ReachOption {
	return func(c *reachConfig) { c.labelNames = names }
}
func ReachWithDirection(d Direction) ReachOption {
	return func(c *reachConfig) { c.direction = d }
}

// ReachableIDs 返回从 start 出发（含自身）可达的全部节点，按 NodeID 升序。
// 在 SCC 凝聚 DAG 上做超节点 BFS：查询面对的图无环，visited 位图每位对应
// 一个分量——这是写入侧压环换来的查询侧红利（环内互达由分量成员表
// 一次性展开，无需节点级环检测）。
func (g *Graph) ReachableIDs(start NodeID, opts ...ReachOption) ([]NodeID, error) {
	if !g.sealed {
		return nil, ErrNotSealed
	}
	if uint32(start) >= uint32(len(g.nodes)) {
		return nil, ErrNodeNotFound
	}
	cfg := reachConfig{direction: DirOut}
	for _, o := range opts {
		o(&cfg)
	}
	mask := g.labelMask(cfg.labelNames)

	// 无标签过滤：超节点快路径——进入分量即互达（分量成员经任意边
	// 互达），visited 位图每位对应一个分量。
	// 有标签过滤：分量成员间可能只有被过滤的边（进入 a 不代表经
	// DATA 边能到同分量的 b），退回节点级遍历保证严格语义。
	if mask == nil {
		return g.reachableSupernode(start, g.condensedView(cfg.direction)), nil
	}
	return g.reachableNodeLevel(start, cfg.direction, mask), nil
}

// reachableSupernode 输出前按 NodeID 排序（分量编号是 Tarjan 发现序，
// 与节点序无关）。
func (g *Graph) reachableSupernode(start NodeID, cond dagCondensed) []NodeID {
	seen := make([]bool, g.sccCount)
	startSCC := g.sccOf[start]
	seen[startSCC] = true
	frontier := []uint32{startSCC}
	for len(frontier) > 0 {
		var next []uint32
		for _, s := range frontier {
			for _, d := range cond.targets(s) {
				if !seen[d] {
					seen[d] = true
					next = append(next, d)
				}
			}
		}
		frontier = next
	}
	out := make([]NodeID, 0, len(g.sccNodes))
	for s := uint32(0); s < g.sccCount; s++ {
		if !seen[s] {
			continue
		}
		for _, v := range g.sccNodes[g.sccNodeOff[s]:g.sccNodeOff[s+1]] {
			out = append(out, NodeID(v))
		}
	}
	slices.Sort(out)
	return out
}

// reachableNodeLevel 是标签过滤时的严格可达性：节点级遍历，
// 每条边都要命中掩码。结果按 NodeID 升序。
func (g *Graph) reachableNodeLevel(start NodeID, dir Direction, mask []bool) []NodeID {
	adj := g.view(dir)
	n := uint32(len(g.nodes))
	visited := visitedPool.acquire(wordsFor(int(n)))
	defer visitedPool.release(visited)
	testAndSet(visited, uint32(start))
	frontier := []NodeID{start}
	for len(frontier) > 0 {
		var next []NodeID
		for _, u := range frontier {
			lo, hi := adj.row(uint32(u))
			for s := lo; s < hi; s++ {
				if !mask[adj.label(s)] {
					continue
				}
				v := adj.target(s)
				if !testAndSet(visited, v) {
					next = append(next, NodeID(v))
				}
			}
		}
		frontier = next
	}
	out := make([]NodeID, 0, 64)
	for v := uint32(0); v < n; v++ {
		if visited[v>>5]&(1<<(v&31)) != 0 {
			out = append(out, NodeID(v))
		}
	}
	return out
}

// dagCondensed 是凝聚 DAG 的值类型视图（不依赖内部包类型导出）。
type dagCondensed struct {
	off, tgt []uint32
}

func (c dagCondensed) targets(s uint32) []uint32 { return c.tgt[c.off[s]:c.off[s+1]] }

func (g *Graph) condensedView(dir Direction) dagCondensed {
	c := g.cond
	if dir == DirIn {
		c = g.condR
	}
	return dagCondensed{off: c.Offsets, tgt: c.Targets}
}

// Reachable 是 ReachableIDs 的字符串 key 便捷版。
func (g *Graph) Reachable(startKey string, opts ...ReachOption) ([]string, error) {
	node, ok := g.NodeByKey(startKey)
	if !ok {
		return nil, ErrNodeNotFound
	}
	ids, err := g.ReachableIDs(node.ID, opts...)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, g.nodes[id].Key)
	}
	return out, nil
}

// Path 是一条节点-边交替的具体路径：Nodes[i] --Edges[i]--> Nodes[i+1]。
type Path struct {
	Nodes []NodeID
	Edges []EdgeID
}

// Len 返回跳数（边数）。
func (p Path) Len() int { return len(p.Edges) }

const noParent = ^uint32(0)

// ShortestPathIDs 双向 BFS 最短路径：始终整层展开较小一侧的前沿，
// 标记时检测对侧 visited，首个相遇点的距离即最优（两侧距离均为
// 精确 BFS 距离，且相遇前无更短路径存在的状态可达）。
func (g *Graph) ShortestPathIDs(from, to NodeID) (Path, bool, error) {
	if !g.sealed {
		return Path{}, false, ErrNotSealed
	}
	n := uint32(len(g.nodes))
	if uint32(from) >= n || uint32(to) >= n {
		return Path{}, false, ErrNodeNotFound
	}
	if from == to {
		return Path{Nodes: []NodeID{from}}, true, nil
	}
	nInt := int(n)
	fVis := visitedPool.acquire(wordsFor(nInt))
	bVis := visitedPool.acquire(wordsFor(nInt))
	defer visitedPool.release(fVis)
	defer visitedPool.release(bVis)
	fParent := make([]uint32, nInt)
	bParent := make([]uint32, nInt)
	for i := range fParent {
		fParent[i], bParent[i] = noParent, noParent
	}

	fAdj, bAdj := g.view(DirOut), g.view(DirIn)
	fFront := []NodeID{from}
	bFront := []NodeID{to}
	fVis[from>>5] |= 1 << (from & 31)
	bVis[to>>5] |= 1 << (to & 31)

	for len(fFront) > 0 && len(bFront) > 0 {
		if len(fFront) <= len(bFront) {
			next, meet, ok := expandMeeting(fFront, fAdj, fVis, bVis, fParent)
			if ok {
				return g.reconstruct(meet, fParent, bParent), true, nil
			}
			fFront = next
		} else {
			next, meet, ok := expandMeeting(bFront, bAdj, bVis, fVis, bParent)
			if ok {
				return g.reconstruct(meet, fParent, bParent), true, nil
			}
			bFront = next
		}
	}
	return Path{}, false, nil
}

// ShortestPath 是 ShortestPathIDs 的字符串 key 便捷版。
func (g *Graph) ShortestPath(fromKey, toKey string) (Path, bool, error) {
	fn, ok := g.NodeByKey(fromKey)
	if !ok {
		return Path{}, false, ErrNodeNotFound
	}
	tn, ok := g.NodeByKey(toKey)
	if !ok {
		return Path{}, false, ErrNodeNotFound
	}
	return g.ShortestPathIDs(fn.ID, tn.ID)
}

// expandMeeting 完整展开一层并返回新前沿；标记时检测对侧 visited，
// 新标记 v 已在对侧集合中即为相遇点。
func expandMeeting(frontier []NodeID, adj adjacencyView, mine, theirs []uint32, parent []uint32) (next []NodeID, meet uint32, found bool) {
	for _, u := range frontier {
		lo, hi := adj.row(uint32(u))
		for s := lo; s < hi; s++ {
			v := adj.target(s)
			w := &mine[v>>5]
			bit := uint32(1) << (v & 31)
			if *w&bit != 0 {
				continue
			}
			*w |= bit
			parent[v] = uint32(u)
			next = append(next, NodeID(v))
			if !found && theirs[v>>5]&bit != 0 {
				meet = v
				found = true
			}
		}
	}
	return next, meet, found
}

// reconstruct 从相遇点向两侧回溯父指针拼接路径。
// bParent 在反向图上的语义：bParent[v]=u 对应原图边 v->u，
// 因此从 bParent[meet] 起步逐步回溯即得 meet->to 的正向节点序列。
func (g *Graph) reconstruct(meet uint32, fParent, bParent []uint32) Path {
	fwd := []NodeID{NodeID(meet)} // meet -> from，逆序
	for v := fParent[meet]; v != noParent; v = fParent[v] {
		fwd = append(fwd, NodeID(v))
	}
	// 反转得 from -> meet
	for i, j := 0, len(fwd)-1; i < j; i, j = i+1, j-1 {
		fwd[i], fwd[j] = fwd[j], fwd[i]
	}
	var bwd []NodeID // meet -> to
	for v := bParent[meet]; v != noParent; v = bParent[v] {
		bwd = append(bwd, NodeID(v))
	}
	nodes := append(fwd, bwd...)
	edges := make([]EdgeID, 0, len(nodes)-1)
	for i := 0; i+1 < len(nodes); i++ {
		edges = append(edges, g.anyEdge(nodes[i], nodes[i+1]))
	}
	return Path{Nodes: nodes, Edges: edges}
}

// anyEdge 返回 u->v 的任意一条边（多边时任取首条）。
func (g *Graph) anyEdge(u, v NodeID) EdgeID {
	lo, hi := g.csrF.Offsets[u], g.csrF.Offsets[u+1]
	for s := lo; s < hi; s++ {
		if g.csrF.Targets[s] == uint32(v) {
			return EdgeID(g.csrF.EdgeIDs[s])
		}
	}
	return 0 // 路径保证连续，正常不可达
}

// Cycle 是一个循环依赖：大小>1 的强连通分量的成员列表，
// 或一个自环节点。
type Cycle struct {
	Nodes []NodeID
}

// Len 返回环上节点数。
func (c Cycle) Len() int { return len(c.Nodes) }

// Cycles 返回图中所有循环依赖，按成员数降序、再按最小成员 ID 升序。
// 这是写入侧 SCC 压缩的副产品："循环依赖阻断"的防御性机制
// 变成"循环依赖发现"的诊断性能力。
func (g *Graph) Cycles() []Cycle {
	if !g.sealed {
		return nil
	}
	var cycles []Cycle
	for s := uint32(0); s < g.sccCount; s++ {
		if size := g.sccNodeOff[s+1] - g.sccNodeOff[s]; size > 1 {
			mem := g.sccNodes[g.sccNodeOff[s]:g.sccNodeOff[s+1]]
			ids := make([]NodeID, size)
			for i, v := range mem {
				ids[i] = NodeID(v)
			}
			cycles = append(cycles, Cycle{Nodes: ids})
		}
	}
	for u, self := range g.selfLoop {
		if self {
			cycles = append(cycles, Cycle{Nodes: []NodeID{NodeID(u)}})
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
