package vectra

import (
	"cmp"
	"slices"

	"github.com/projanvil/vectra/internal/delta"
	"github.com/projanvil/vectra/internal/engine"
	tr "github.com/projanvil/vectra/internal/traverse"
)

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
//
// v0.7 traversal family (spec §3.2, Task 4): row iteration over the
// generation's three-source View — base rows tombstone-filtered plus layer
// rows, so writes are visible immediately. With nothing unmerged the plain
// CSR row zip runs (FastAdjacency hot path).
// neighborsDedupLimit is the row size below which NeighborsOf dedups by
// linear scan over the result instead of allocating a visited bitmap: point
// queries touch ~degree-sized rows, and a whole-graph allocation per call
// was the top B2 allocation source. Rows above the limit allocate a LOCAL
// bitmap (hub queries are rare) — deliberately NOT the traverse shared
// pool: 7M+ QPS of small pool churn evicts the big BFS/ShortestPath
// bitmaps and forced their fresh allocation per call (the bfsall
// regression the retest matrix caught).
const neighborsDedupLimit = 64

// dedupAdd appends id's node record to out unless already present (linear
// scan — bounded by neighborsDedupLimit callers).
func dedupAppend(out []Node, materialize func(uint32) (Node, bool), id uint32) []Node {
	for i := range out {
		if out[i].ID == NodeID(id) {
			return out
		}
	}
	if rec, ok := materialize(id); ok {
		return append(out, rec)
	}
	return out
}

// dedupSet dedups via a locally allocated bitmap for rows above the linear
// limit (hub queries are rare; see neighborsDedupLimit for why this is not
// pooled).
type dedupSet struct {
	words []uint32
}

func newDedupSet(nodeCount int) dedupSet {
	return dedupSet{words: make([]uint32, (nodeCount+31)/32)}
}

func (d *dedupSet) seen(v uint32) bool {
	w := &d.words[v>>5]
	bit := uint32(1) << (v & 31)
	if *w&bit != 0 {
		return true
	}
	*w |= bit
	return false
}

func (g *Graph) NeighborsOf(key string, dir Direction, labels ...string) ([]Node, error) {
	n, ok := g.NodeByKey(key) // materializes the caller's absorption window
	if !ok {
		return nil, ErrNodeNotFound
	}
	v := g.view.Load()
	src := g.rowsOf(v)
	lr := labelResolver(v.base)
	if view, ok := src.(*engine.View); ok { // joint label table
		lr = view
	}
	mask := labelMaskOf(lr, labels)
	dirCode := int(tr.DirOut)
	if dir == DirIn {
		dirCode = int(tr.DirIn)
	}
	materialize := func(id uint32) (Node, bool) {
		return v.nodeByID(NodeID(id))
	}
	var out []Node
	if adj, fast := src.FastAdjacency(dirCode); fast {
		tg, _, lb := adj.Row(uint32(n.ID))
		if len(tg) <= neighborsDedupLimit {
			for i, t := range tg {
				if mask != nil && !mask[lb[i]] {
					continue
				}
				out = dedupAppend(out, materialize, t)
			}
			return out, nil
		}
		dedup := newDedupSet(src.NodeCount()) // local bitmap: see neighborsDedupLimit
		for i, t := range tg {
			if mask != nil && !mask[lb[i]] {
				continue
			}
			if !dedup.seen(t) {
				if rec, ok := materialize(t); ok {
					out = append(out, rec)
				}
			}
		}
		return out, nil
	}
	rows := src.Rows(uint32(n.ID), dirCode)
	if len(rows) <= neighborsDedupLimit {
		for _, r := range rows {
			if mask != nil && !mask[r.LabelID] {
				continue
			}
			out = dedupAppend(out, materialize, r.Target)
		}
		return out, nil
	}
	dedup := newDedupSet(src.NodeCount())
	for _, r := range rows {
		if mask != nil && !mask[r.LabelID] {
			continue
		}
		if !dedup.seen(r.Target) {
			if rec, ok := materialize(r.Target); ok {
				out = append(out, rec)
			}
		}
	}
	return out, nil
}

// EdgesOf returns the live edges incident to key, filtered by direction and
// optional labels. Unlike NeighborsOf (deduplicated nodes), every parallel
// edge is returned with its own props — this is the entry point for reading
// weight-like attributes, e.g. read-modify-write updates via UpsertEdge
// (safe under the embedded single-writer model).
//
// v0.7 point-query family (spec §3.2): three-source read, immediately
// consistent — base incidents come from the CSR rows (tombstone-filtered),
// layer incidents from a record scan (newer-tombstone-filtered); results are
// sorted by EdgeID ascending. A deleted node key yields ErrNodeNotFound.
func (g *Graph) EdgesOf(key string, dir Direction, labels ...string) ([]Edge, error) {
	g.flushPend()
	v := g.view.Load()
	_, ok := v.nodeWinner(key)
	if !ok {
		return nil, ErrNodeNotFound
	}
	var labSet map[string]bool
	if len(labels) > 0 {
		labSet = make(map[string]bool, len(labels))
		for _, l := range labels {
			labSet[l] = true
		}
	}
	var out []Edge
	if bid, isBase := v.baseKeys[key]; isBase {
		adj := v.base.AdjacencyF()
		if dir == DirIn {
			adj = v.base.AdjacencyR()
		}
		_, eids, _ := adj.Row(uint32(bid))
		for _, eid := range eids {
			src, dst, label, props, ok := v.base.EdgeInfo(eid)
			if !ok {
				continue
			}
			k := delta.EdgeKey{SrcKey: v.baseKeyOf(src), DstKey: v.baseKeyOf(dst), Label: label}
			if !v.baseEdgeLive(k) {
				continue
			}
			if labSet != nil && !labSet[label] {
				continue
			}
			out = append(out, Edge{ID: EdgeID(eid), Src: NodeID(src), Dst: NodeID(dst), Label: label, Props: props})
		}
	}
	for i, l := range v.layers {
		for j, rec := range l.Edges() {
			if dir == DirOut && rec.SrcKey != key {
				continue
			}
			if dir == DirIn && rec.DstKey != key {
				continue
			}
			if labSet != nil && !labSet[rec.Label] {
				continue
			}
			k := delta.EdgeKey{SrcKey: rec.SrcKey, DstKey: rec.DstKey, Label: rec.Label}
			if !v.layerEdgeLive(i, k) {
				continue
			}
			out = append(out, Edge{
				ID:  EdgeID(l.EdgeIDAt(j)),
				Src: v.nodeIDOf(rec.SrcKey), Dst: v.nodeIDOf(rec.DstKey),
				Label: rec.Label, Props: rec.Props,
			})
		}
	}
	slices.SortFunc(out, func(a, b Edge) int { return cmp.Compare(a.ID, b.ID) })
	return out, nil
}
