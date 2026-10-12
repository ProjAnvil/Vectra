package vectra

// Query 是链式遍历构造器：
//
//	g.Query().From("gateway").Out("CALLS").MaxDepth(3).Run()
//
// From 支持多起点（批量多源 BFS）；Out/In 指定方向并可选按标签过滤；
// MaxDepth 限制跳数（默认不限）。Where 对到达节点施加谓词。
// Run 执行层同步 BFS（前沿达到阈值自动并行）。
type Query struct {
	g        *Graph
	fromKeys []string
	labels   []string
	dir      Direction
	maxDepth uint32
	nodePred func(Node) bool
}

func (g *Graph) Query() *Query {
	return &Query{g: g, maxDepth: ^uint32(0), dir: DirOut}
}

func (q *Query) From(keys ...string) *Query  { q.fromKeys = keys; return q }
func (q *Query) Out(labels ...string) *Query { q.labels, q.dir = labels, DirOut; return q }
func (q *Query) In(labels ...string) *Query  { q.labels, q.dir = labels, DirIn; return q }
func (q *Query) MaxDepth(d uint32) *Query    { q.maxDepth = d; return q }

// Where 对到达节点施加谓词（等价 WithNodeFilter；起点不受限）。
func (q *Query) Where(pred func(Node) bool) *Query { q.nodePred = pred; return q }

func (q *Query) Run() (*BFSResult, error) {
	if len(q.fromKeys) == 0 {
		return nil, ErrNodeNotFound
	}
	starts := make([]NodeID, 0, len(q.fromKeys))
	for _, k := range q.fromKeys {
		n, ok := q.g.NodeByKey(k)
		if !ok {
			return nil, ErrNodeNotFound
		}
		starts = append(starts, n.ID)
	}
	opts := []BFSOption{WithDirection(q.dir), WithMaxDepth(q.maxDepth)}
	if len(q.labels) > 0 {
		opts = append(opts, WithLabels(q.labels...))
	}
	if q.nodePred != nil {
		opts = append(opts, WithNodeFilter(q.nodePred))
	}
	return q.g.BFS(starts, opts...)
}

// NeighborsOf 返回 key 节点的一跳邻居（按标签过滤，空 = 全部，去重）。
func (g *Graph) NeighborsOf(key string, dir Direction, labels ...string) ([]Node, error) {
	n, ok := g.NodeByKey(key)
	if !ok {
		return nil, ErrNodeNotFound
	}
	if !g.sealed {
		return nil, ErrNotSealed
	}
	adj := g.snap.AdjacencyF()
	if dir == DirIn {
		adj = g.snap.AdjacencyR()
	}
	mask := g.labelMask(labels)
	seen := make([]bool, g.snap.NodeCount())
	var out []Node
	tg, _, lb := adj.Row(uint32(n.ID))
	for i, v := range tg {
		if mask != nil && !mask[lb[i]] {
			continue
		}
		if !seen[v] {
			seen[v] = true
			node, _ := g.NodeByID(NodeID(v))
			out = append(out, node)
		}
	}
	return out, nil
}
