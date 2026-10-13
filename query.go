package vectra

// Query is the chained traversal builder:
//
//	g.Query().From("gateway").Out("CALLS").MaxDepth(3).Run()
//
// From supports multiple start nodes (batched multi-source BFS); Out/In
// pick the direction with optional label filtering; MaxDepth caps hops
// (unbounded by default). Where applies a predicate to reached nodes.
// Run executes a level-synchronous BFS (goes parallel automatically once
// the frontier crosses a threshold).
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

// Where applies a predicate to reached nodes (equivalent to WithNodeFilter;
// start nodes are exempt).
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

// NeighborsOf returns the one-hop neighbors of the node key (label-filtered,
// empty = all, deduplicated).
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

// EdgesOf returns the live edges incident to key, filtered by direction and
// optional labels. Unlike NeighborsOf (deduplicated nodes), every parallel
// edge is returned with its own props — this is the entry point for reading
// weight-like attributes, e.g. read-modify-write updates via UpsertEdge
// (safe under the embedded single-writer model). Works both before and
// after Seal; a deleted node key yields ErrNodeNotFound.
func (g *Graph) EdgesOf(key string, dir Direction, labels ...string) ([]Edge, error) {
	n, ok := g.NodeByKey(key)
	if !ok {
		return nil, ErrNodeNotFound
	}
	mask := g.labelMask(labels)
	var out []Edge
	if g.sealed {
		adj := g.snap.AdjacencyF()
		if dir == DirIn {
			adj = g.snap.AdjacencyR()
		}
		_, eids, lb := adj.Row(uint32(n.ID))
		for i := range eids {
			if mask != nil && !mask[lb[i]] {
				continue
			}
			if e, ok := g.EdgeByID(EdgeID(eids[i])); ok {
				out = append(out, e)
			}
		}
		return out, nil
	}
	for i, e := range g.edges {
		if g.tombE[i] {
			continue
		}
		if dir == DirOut && e.src != n.ID {
			continue
		}
		if dir == DirIn && e.dst != n.ID {
			continue
		}
		if mask != nil && !mask[e.label] {
			continue
		}
		out = append(out, Edge{ID: EdgeID(i), Src: e.src, Dst: e.dst, Label: g.labels[e.label], Props: e.props})
	}
	return out, nil
}
