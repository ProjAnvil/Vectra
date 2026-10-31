package vectra

import (
	"maps"
	"strconv"

	"github.com/projanvil/vectra/internal/cypher"
)

// CypherWriteSummary is the statement mutation counter set (the Neo4j
// counters analogue) of a Cypher write statement.
type CypherWriteSummary = cypher.WriteSummary

// CypherIsWrite reports whether the Cypher statement mutates (write
// clauses or index DDL). Server routing uses this to pick the apply path.
// The parse error is returned unchanged.
func CypherIsWrite(query string) (bool, error) { return cypher.IsWrite(query) }

// writeGraph is the root facade's cypher.Writer: the mutation surface the
// write-clause executor drives. Every method is key- or triple-addressed
// over the existing root write APIs — the executor resolves bound IDs to
// keys once, before the first call. Not safe for concurrent use: it must
// run on the graph's writer goroutine (the caller's goroutine in direct
// mode, the FSM apply goroutine in server mode), the same discipline as
// AddNode/AddEdge.
type writeGraph struct{ g *Graph }

// newWriteGraph adapts g for the write-clause executor.
func newWriteGraph(g *Graph) *writeGraph { return &writeGraph{g: g} }

var _ cypher.Writer = (*writeGraph)(nil)

// NewNodeKey draws "_c<id>" from the same node-ID ratchet AddNode uses.
// The draw advances the ratchet even when the key never becomes a node:
// IDs may skip, ratchets only grow, and the key stays a pure function of
// graph state + call sequence (raft FSM re-execution determinism — never
// time/UUID/random). A key already live (a user squatted "_c5" ahead of
// the ratchet) is skipped.
func (w *writeGraph) NewNodeKey() string {
	for {
		id := w.g.nextNode
		w.g.nextNode++
		k := "_c" + strconv.FormatUint(uint64(id), 10)
		if !w.g.nodeLive(k) {
			return k
		}
	}
}

// NodeIDByKey resolves the winning (newest live) record for key.
func (w *writeGraph) NodeIDByKey(key string) (uint32, bool) {
	n, ok := w.g.NodeByKey(key)
	if !ok {
		return 0, false
	}
	return uint32(n.ID), true
}

// CreateNode applies AddNode upsert semantics: an existing key keeps its
// NodeID and has label and props replaced wholesale (created=false).
func (w *writeGraph) CreateNode(key, label string, props map[string]any) (uint32, bool, error) {
	created := !w.g.nodeLive(key)
	id, err := w.g.AddNode(key, label, props)
	if err != nil {
		return 0, false, err
	}
	return uint32(id), created, nil
}

// SetNodeProps merges props onto the live node record; a nil value deletes
// the key. Whole-record rewrite through AddNode because no per-property
// primitive exists (gap-fill E8). The read comes from the live generation
// and the write goes back directly — never inside g.Batch: batch queries
// see a pre-batch generation, and statement atomicity is first-error-stops,
// not transactional. Returns the number of property keys written
// (sets + deletes).
func (w *writeGraph) SetNodeProps(key string, props map[string]any) (int, error) {
	n, ok := w.g.NodeByKey(key)
	if !ok {
		return 0, ErrNodeNotFound
	}
	if len(props) == 0 {
		return 0, nil // nothing to write: skip the record rewrite and its WAL frame
	}
	merged := make(map[string]any, len(n.Props)+len(props))
	maps.Copy(merged, n.Props)
	for k, v := range props {
		if v == nil {
			delete(merged, k)
			continue
		}
		merged[k] = v
	}
	if _, err := w.g.AddNode(key, n.Label, merged); err != nil {
		return 0, err
	}
	return len(props), nil
}

// NodeHasEdges reports whether the node has any live incident edge (the
// DETACH requirement check). Direction has no "both" constant, so the two
// orientations are probed separately; a self-loop shows in both.
func (w *writeGraph) NodeHasEdges(key string) bool {
	if es, err := w.g.EdgesOf(key, DirOut); err == nil && len(es) > 0 {
		return true
	}
	es, err := w.g.EdgesOf(key, DirIn)
	return err == nil && len(es) > 0
}

// DeleteNodeCascade is DETACH DELETE: the node plus every incident edge;
// the count is the cascade's edge kills (root DeleteNode's second return).
func (w *writeGraph) DeleteNodeCascade(key string) (int, error) {
	return w.g.DeleteNode(key)
}

// edgesOnTriple lists the live parallel edges of (src, dst, label) in
// EdgeID order (EdgesOf output is EdgeID-ascending). Edge records carry
// NodeIDs, so the dst filter resolves through NodeByID.
func (w *writeGraph) edgesOnTriple(srcKey, dstKey, label string) ([]Edge, error) {
	es, err := w.g.EdgesOf(srcKey, DirOut, label)
	if err != nil {
		return nil, err
	}
	var out []Edge
	for _, e := range es {
		if dn, ok := w.g.NodeByID(e.Dst); ok && dn.Key == dstKey {
			out = append(out, e)
		}
	}
	return out, nil
}

// DeleteEdgeTriple deletes every parallel edge on the triple (E6
// divergence: root DeleteEdge is triple-addressed, not per-edge).
func (w *writeGraph) DeleteEdgeTriple(srcKey, dstKey, label string) (int, error) {
	return w.g.DeleteEdge(srcKey, dstKey, label)
}

// UpsertEdgeTriple folds by triple: existing parallels collapse onto the
// winner (EdgeID kept, props replaced wholesale); otherwise a new edge is
// created. created reports which.
func (w *writeGraph) UpsertEdgeTriple(srcKey, dstKey, label string, props map[string]any) (bool, error) {
	live, err := w.edgesOnTriple(srcKey, dstKey, label)
	if err != nil {
		return false, err
	}
	if _, err := w.g.UpsertEdge(srcKey, dstKey, label, props); err != nil {
		return false, err
	}
	return len(live) == 0, nil
}

// CreateEdge appends a NEW parallel edge (CREATE semantics: duplicates
// allowed, contrast UpsertEdgeTriple's fold). A missing endpoint is a
// statement error (first-error-stops).
func (w *writeGraph) CreateEdge(srcKey, dstKey, label string, props map[string]any) (uint32, error) {
	id, err := w.g.AddEdge(srcKey, dstKey, label, props)
	if err != nil {
		return 0, err
	}
	return uint32(id), nil
}

// SetEdgeProps writes props onto every live parallel edge of the triple
// (merge overlay, nil deletes — the SetNodeProps convention) and returns
// the number of edges touched.
//
// Mechanism: root UpsertEdge FOLDS a multi-parallel triple onto one edge,
// so it cannot update parallels in place. Instead the parallels are
// snapshotted, each one's props merged, the whole triple deleted, and each
// parallel re-added. The parallels survive with their props; EdgeIDs
// change (the ID ratchet never reuses — EdgeIDByTriple rebinds RETURN'd
// edge variables). This diverges from the plan's "UpsertEdge per parallel
// edge", which taken literally collapses the triple on the first call.
func (w *writeGraph) SetEdgeProps(srcKey, dstKey, label string, props map[string]any) (int, error) {
	if len(props) == 0 {
		return 0, nil // nothing to write: an empty overlay must not churn EdgeIDs
	}
	es, err := w.edgesOnTriple(srcKey, dstKey, label)
	if err != nil {
		return 0, err
	}
	if len(es) == 0 {
		return 0, nil
	}
	merged := make([]map[string]any, len(es))
	for i, e := range es {
		m := make(map[string]any, len(e.Props)+len(props))
		maps.Copy(m, e.Props)
		for k, v := range props {
			if v == nil {
				delete(m, k)
				continue
			}
			m[k] = v
		}
		merged[i] = m
	}
	if _, err := w.g.DeleteEdge(srcKey, dstKey, label); err != nil {
		return 0, err
	}
	for _, m := range merged {
		if _, err := w.g.AddEdge(srcKey, dstKey, label, m); err != nil {
			return 0, err
		}
	}
	return len(es), nil
}

// EdgeIDByTriple returns the first (lowest-ID) live parallel edge of the
// triple; EdgesOf output is EdgeID-ascending.
func (w *writeGraph) EdgeIDByTriple(srcKey, dstKey, label string) (uint32, bool) {
	es, err := w.edgesOnTriple(srcKey, dstKey, label)
	if err != nil || len(es) == 0 {
		return 0, false
	}
	return uint32(es[0].ID), true
}

// IndexProperty declares a typed index. Root IndexProperty is chainable
// and silently drops the declaration after Close; the Writer surface
// reports that as ErrClosed (parity with the other write methods).
func (w *writeGraph) IndexProperty(key string) error {
	if w.g.closed.Load() {
		return ErrClosed
	}
	w.g.IndexProperty(key)
	return nil
}

// IndexFullText declares a full-text index; same declaration and closed
// contract as IndexProperty.
func (w *writeGraph) IndexFullText(key string) error {
	if w.g.closed.Load() {
		return ErrClosed
	}
	w.g.IndexFullText(key)
	return nil
}

// DeclareUnique registers a (label, prop) uniqueness constraint, and
// DropUnique removes one, through the root API (the Task 2 engine
// surface). Cypher statements never run inside g.Batch, so the engine's
// mid-Batch rejection is unreachable on this path — the Go API keeps the
// guard. Both errors (duplicate declaration, declaration-time duplicate
// data, unknown constraint, a write-time violation surfacing through the
// declaration scan) propagate RAW to the statement caller: the Cypher
// layer never wraps writer errors, so the payload stays reachable for
// errors.Is/errors.As and the server's wire mapping (unique-constraints
// spec §2.6).
func (w *writeGraph) DeclareUnique(label, prop string) error {
	return w.g.DeclareUnique(label, prop)
}

// DropUnique removes the constraint; same raw-error contract as
// DeclareUnique.
func (w *writeGraph) DropUnique(label, prop string) error {
	return w.g.DropUnique(label, prop)
}
