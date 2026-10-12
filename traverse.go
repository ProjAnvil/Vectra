package vectra

import (
	tr "vectra/internal/traverse"
)

type Direction uint8

const (
	DirOut Direction = iota
	DirIn
)

// BFSResult 是一次 BFS 的完整结果：按层分组的成员 + 每节点深度。
// 深度用平坦 uint32 数组而非 map——百万级结果集上 map 插入成本
// 会吞掉 CSR 的访存优势（基准验证过），数组的 GC 成本也趋近于零。
type BFSResult struct {
	Layers [][]NodeID
	depth  []uint32
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
func (r *BFSResult) Reached(id NodeID) bool { return r.depth[id] != ^uint32(0) }

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
	nodeFilter  func(Node) bool
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
	n := uint32(g.snap.NodeCount())
	for _, s := range starts {
		if uint32(s) >= n {
			return nil, ErrNodeNotFound
		}
	}
	res := tr.BFS(g.snap, toU32s(starts), g.traverseConfig(opts...))
	return &BFSResult{Layers: toNodeIDLayers(res.Layers), depth: res.DepthRaw()}, nil
}

func (g *Graph) traverseConfig(opts ...BFSOption) tr.Config {
	cfg := bfsConfig{maxDepth: ^uint32(0), parallelThr: tr.DefaultParallelThreshold, direction: DirOut}
	for _, o := range opts {
		o(&cfg)
	}
	tc := tr.Config{
		MaxDepth:          cfg.maxDepth,
		Direction:         tr.Direction(cfg.direction),
		ParallelThreshold: cfg.parallelThr,
	}
	if len(cfg.labelNames) > 0 {
		tc.LabelMask = g.labelMask(cfg.labelNames)
	}
	if cfg.nodeFilter != nil {
		snap := g.snap
		tc.NodeFilter = func(id uint32) bool {
			rec, _ := snap.NodeInfo(id)
			return cfg.nodeFilter(Node{ID: NodeID(id), Key: rec.Key, Label: rec.Label, Props: rec.Props})
		}
	}
	return tc
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

func toU32s(ids []NodeID) []uint32 {
	out := make([]uint32, len(ids))
	for i, id := range ids {
		out[i] = uint32(id)
	}
	return out
}

func toNodeIDLayers(layers [][]uint32) [][]NodeID {
	out := make([][]NodeID, len(layers))
	for i, l := range layers {
		out[i] = make([]NodeID, len(l))
		for j, v := range l {
			out[i][j] = NodeID(v)
		}
	}
	return out
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
// 无标签过滤时在 SCC 凝聚 DAG 上做超节点 BFS（查询面对的图无环，
// visited 每位对应一个分量）；有过滤时退回严格节点级遍历。
func (g *Graph) ReachableIDs(start NodeID, opts ...ReachOption) ([]NodeID, error) {
	if !g.sealed {
		return nil, ErrNotSealed
	}
	if uint32(start) >= uint32(g.snap.NodeCount()) {
		return nil, ErrNodeNotFound
	}
	cfg := reachConfig{direction: DirOut}
	for _, o := range opts {
		o(&cfg)
	}
	tc := tr.Config{Direction: tr.Direction(cfg.direction)}
	if len(cfg.labelNames) > 0 {
		tc.LabelMask = g.labelMask(cfg.labelNames)
	}
	ids := tr.Reachable(g.snap, uint32(start), tc)
	out := make([]NodeID, len(ids))
	for i, id := range ids {
		out[i] = NodeID(id)
	}
	return out, nil
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
		n, _ := g.NodeByID(id)
		out = append(out, n.Key)
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

// ShortestPathIDs 双向 BFS 最短路径：始终整层展开较小一侧的前沿，
// 标记时检测对侧 visited，首个相遇点的距离即最优。
func (g *Graph) ShortestPathIDs(from, to NodeID) (Path, bool, error) {
	if !g.sealed {
		return Path{}, false, ErrNotSealed
	}
	if uint32(from) >= uint32(g.snap.NodeCount()) || uint32(to) >= uint32(g.snap.NodeCount()) {
		return Path{}, false, ErrNodeNotFound
	}
	nodes, edges, found := tr.ShortestPath(g.snap, uint32(from), uint32(to))
	if !found {
		return Path{}, false, nil
	}
	p := Path{Nodes: make([]NodeID, len(nodes)), Edges: make([]EdgeID, len(edges))}
	for i, v := range nodes {
		p.Nodes[i] = NodeID(v)
	}
	for i, e := range edges {
		p.Edges[i] = EdgeID(e)
	}
	return p, true, nil
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
	raw := tr.Cycles(g.snap)
	out := make([]Cycle, len(raw))
	for i, c := range raw {
		out[i] = Cycle{Nodes: make([]NodeID, len(c.Nodes))}
		for j, v := range c.Nodes {
			out[i].Nodes[j] = NodeID(v)
		}
	}
	return out
}

// WithNodeFilter 对到达节点施加谓词（标记新节点时求值；起点恒入选，
// 谓词不满足的节点不进入结果也不被展开）。
func WithNodeFilter(pred func(Node) bool) BFSOption {
	return func(c *bfsConfig) { c.nodeFilter = pred }
}
