package vectra

import (
	"github.com/projanvil/vectra/internal/engine"
	tr "github.com/projanvil/vectra/internal/traverse"
)

type Direction uint8

const (
	DirOut Direction = iota
	DirIn
)

// BFSResult is the complete result of one BFS: members grouped by layer +
// per-node depth. Depth uses a flat uint32 array instead of a map — on
// million-scale result sets map insertion cost would swallow the CSR's
// memory-access advantage (verified by benchmarks), and the array's GC cost
// is near zero.
type BFSResult struct {
	Layers [][]NodeID
	depth  []uint32
}

// Nodes returns all reached nodes (layers concatenated).
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

// Count returns the total number of reached nodes (start nodes included).
func (r *BFSResult) Count() int {
	total := 0
	for _, l := range r.Layers {
		total += len(l)
	}
	return total
}

// Depth returns the node depth; unreached nodes return math.MaxUint32.
func (r *BFSResult) Depth(id NodeID) uint32 { return r.depth[id] }

// Reached reports whether the node was reached.
func (r *BFSResult) Reached(id NodeID) bool { return r.depth[id] != ^uint32(0) }

// DepthsMap builds a "node->depth" map on demand (a convenience for small
// result sets; prefer Depth/Reached/Nodes for large ones).
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

// WithLabels traverses only edges with the given labels. Empty list = no
// filtering; if no label exists the result is just the start nodes.
func WithLabels(names ...string) BFSOption {
	return func(c *bfsConfig) { c.labelNames = names }
}

// BFS is multi-source level-synchronous breadth-first search: start nodes
// at depth 0, each layer expanded wholesale. Level synchronization is the
// basis for parallelism — the frontier is a flat array, chunked across
// goroutines once it crosses a threshold.
//
// v0.7 traversal family (spec §3.2): BFS runs against the generation's
// three-source View (base ∪ unmerged layers) — writes are visible
// immediately. With nothing unmerged the View IS the base CSR and the
// slice-zip hot path runs untouched (FastAdjacency).
func (g *Graph) BFS(starts []NodeID, opts ...BFSOption) (*BFSResult, error) {
	v := g.view.Load()
	src := v.rows()
	n := uint32(src.NodeCount())
	for _, s := range starts {
		if uint32(s) >= n {
			return nil, ErrNodeNotFound
		}
	}
	res := tr.BFS(src, toU32s(starts), g.traverseConfig(v, src, opts...))
	return &BFSResult{Layers: toNodeIDLayers(res.Layers), depth: res.DepthRaw()}, nil
}

// traverseConfig builds the traversal config for one generation: the label
// mask indexes the source's label table (the base's, or the View's joint
// base ∪ pending-label table), and NodeFilter resolves node records through
// the generation's winner resolution (layer overrides win).
func (g *Graph) traverseConfig(v *genView, src tr.RowSource, opts ...BFSOption) tr.Config {
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
		lr := labelResolver(v.base)
		if view, ok := src.(*engine.View); ok { // three-source: the joint label table
			lr = view
		}
		tc.LabelMask = labelMaskOf(lr, cfg.labelNames)
	}
	if cfg.nodeFilter != nil {
		tc.NodeFilter = func(id uint32) bool {
			rec, _ := v.nodeByID(NodeID(id))
			return cfg.nodeFilter(Node{ID: NodeID(id), Key: rec.Key, Label: rec.Label, Props: rec.Props})
		}
	}
	return tc
}

// labelResolver is the label-table surface shared by the base snapshot and
// the three-source View (whose table is the joint base ∪ pending-labels).
type labelResolver interface {
	LabelCount() int
	LabelID(name string) (uint32, bool)
}

// labelMaskOf turns a set of label names into a boolean mask indexed by the
// resolver's label table; empty names returns nil (no filtering).
func labelMaskOf(lr labelResolver, names []string) []bool {
	if len(names) == 0 {
		return nil
	}
	mask := make([]bool, lr.LabelCount())
	for _, n := range names {
		if id, ok := lr.LabelID(n); ok {
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

// ReachableIDs returns all nodes reachable from start (itself included),
// ascending by NodeID. With no label filter and nothing unmerged it runs
// supernode BFS on the SCC condensation DAG (the query-facing graph is
// acyclic; each visited bit corresponds to one component) — the supernode
// acceleration exists only at merge boundaries (spec §3.2); a label filter
// or unmerged delta layers fall back to strict node-level traversal over
// the three-source View.
func (g *Graph) ReachableIDs(start NodeID, opts ...ReachOption) ([]NodeID, error) {
	v := g.view.Load()
	cfg := reachConfig{direction: DirOut}
	for _, o := range opts {
		o(&cfg)
	}
	tc := tr.Config{Direction: tr.Direction(cfg.direction)}
	if len(cfg.labelNames) > 0 {
		tc.LabelMask = labelMaskOf(v.base, cfg.labelNames)
	}
	if tc.LabelMask == nil && len(v.layers) == 0 {
		if uint32(start) >= uint32(v.base.NodeCount()) {
			return nil, ErrNodeNotFound
		}
		ids := tr.ReachableSupernode(v.base, uint32(start), tr.Direction(cfg.direction))
		out := make([]NodeID, len(ids))
		for i, id := range ids {
			out[i] = NodeID(id)
		}
		return out, nil
	}
	src := v.rows()
	if uint32(start) >= uint32(src.NodeCount()) {
		return nil, ErrNodeNotFound
	}
	if tc.LabelMask != nil {
		if view, ok := src.(*engine.View); ok { // joint label table
			tc.LabelMask = labelMaskOf(view, cfg.labelNames)
		}
	}
	ids := tr.Reachable(src, uint32(start), tc)
	out := make([]NodeID, len(ids))
	for i, id := range ids {
		out[i] = NodeID(id)
	}
	return out, nil
}

// Reachable is the string-key convenience version of ReachableIDs.
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

// Path is one concrete node-edge alternating path: Nodes[i] --Edges[i]--> Nodes[i+1].
type Path struct {
	Nodes []NodeID
	Edges []EdgeID
}

// Len returns the hop count (number of edges).
func (p Path) Len() int { return len(p.Edges) }

// ShortestPathIDs is bidirectional BFS shortest path: always expands the
// whole layer of the smaller side's frontier, checks the opposite side's
// visited when marking; the first meeting point yields the optimal distance.
//
// v0.7 traversal family (spec §3.2): runs over the generation's three-source
// View — paths may cross base and layer edges alike; with nothing unmerged
// it is the plain CSR bidirectional BFS (FastAdjacency hot path).
func (g *Graph) ShortestPathIDs(from, to NodeID) (Path, bool, error) {
	v := g.view.Load()
	src := v.rows()
	if uint32(from) >= uint32(src.NodeCount()) || uint32(to) >= uint32(src.NodeCount()) {
		return Path{}, false, ErrNodeNotFound
	}
	nodes, edges, found := tr.ShortestPath(src, uint32(from), uint32(to))
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

// ShortestPath is the string-key convenience version of ShortestPathIDs.
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

// Cycle is one circular dependency: the member list of a strongly connected
// component of size>1, or a single self-loop node. Edges carries the
// intra-member edges with full label/props (perimeter plus chords), so
// consumers can triage cycle severity without re-joining raw records via
// EdgesOf.
type Cycle struct {
	Nodes []NodeID
	Edges []Edge
}

// Len returns the number of nodes on the cycle.
func (c Cycle) Len() int { return len(c.Nodes) }

// Cycles returns all circular dependencies in the graph, sorted by member
// count descending, then by smallest member ID ascending. It is a byproduct
// of write-side SCC compression: the defensive "cycle blocking" mechanism
// becomes the diagnostic "cycle discovery" capability.
//
// v0.7 (spec §3.2): Cycles is the traversal-family exception — unmerged
// layers are force-merged first (SCC structures only exist on a merged
// base), so the answer is always strongly consistent. The forced merge
// rewrites writer-private state, so although Cycles answers a query it
// walks the write path: concurrent-safe only via the writer handle (Submit
// sessions) or single-goroutine discipline. In wrapper mode, debug builds
// panic with ErrNotWriter when called off the writer goroutine; release
// builds rely on this documented contract. Every other query is lock-free.
func (g *Graph) Cycles() []Cycle {
	g.assertWriter()
	v := g.view.Load()
	if len(v.layers) > 0 {
		if g.forceMerge() != nil {
			return nil
		}
		v = g.view.Load()
	}
	raw := tr.Cycles(v.base)
	out := make([]Cycle, len(raw))
	for i, c := range raw {
		out[i] = Cycle{Nodes: make([]NodeID, len(c.Nodes))}
		for j, v := range c.Nodes {
			out[i].Nodes[j] = NodeID(v)
		}
		if len(c.Edges) > 0 {
			out[i].Edges = make([]Edge, len(c.Edges))
			for j, ce := range c.Edges {
				if e, ok := g.EdgeByID(EdgeID(ce.EdgeID)); ok {
					out[i].Edges[j] = e
				}
			}
		}
	}
	return out
}

// WithNodeFilter applies a predicate to reached nodes (evaluated when a new
// node is marked; start nodes are always included; nodes failing the
// predicate enter neither the result nor the expansion).
func WithNodeFilter(pred func(Node) bool) BFSOption {
	return func(c *bfsConfig) { c.nodeFilter = pred }
}
