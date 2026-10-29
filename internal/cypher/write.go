package cypher

import (
	"fmt"

	"github.com/projanvil/vectra/internal/engine"
)

// WriteSummary is the statement's mutation counters (the Neo4j counters
// analogue). Result carries it when the statement wrote.
type WriteSummary struct {
	CreatedNodes, CreatedEdges    int
	DeletedNodes, DeletedEdges    int
	PropertiesSet, IndexesCreated int
}

// Writer is the mutation surface the write clauses drive. Every method is
// key- or triple-addressed: the executor resolves bound IDs to keys once,
// against the pinned pre-write source, before the first write call. All
// calls arrive sequentially from one goroutine (the writer goroutine in
// direct mode, the FSM apply goroutine in server mode).
type Writer interface {
	// NewNodeKey returns a fresh unique node key for CREATE patterns
	// without an explicit key: "_c<id>" drawn from the node-ID ratchet.
	// Deterministic: replaying the same statement sequence on identical
	// state yields identical keys (required for raft FSM re-execution).
	NewNodeKey() string
	NodeIDByKey(key string) (uint32, bool)
	// CreateNode applies AddNode upsert semantics; created=false when the
	// key already existed (props replaced, per AddNode).
	CreateNode(key, label string, props map[string]any) (id uint32, created bool, err error)
	// SetNodeProps merges props; a nil value deletes the key; returns the
	// number of property keys written (sets + deletes).
	SetNodeProps(key string, props map[string]any) (int, error)
	NodeHasEdges(key string) bool
	DeleteNodeCascade(key string) error
	// DeleteEdgeTriple deletes every parallel edge on the triple (E6
	// divergence, pinned by test).
	DeleteEdgeTriple(srcKey, dstKey, label string) (int, error)
	UpsertEdgeTriple(srcKey, dstKey, label string, props map[string]any) (created bool, err error)
	// CreateEdge adds a NEW parallel edge (CREATE semantics — duplicates
	// allowed; contrast UpsertEdgeTriple's fold). Fails when an endpoint
	// is missing (statement error, first-error-stops).
	CreateEdge(srcKey, dstKey, label string, props map[string]any) (id uint32, err error)
	// SetEdgeProps writes props onto every live parallel edge of the
	// triple; returns the number of edges touched.
	SetEdgeProps(srcKey, dstKey, label string, props map[string]any) (int, error)
	// EdgeIDByTriple returns the first live parallel edge (used to rebind
	// RETURN'd edge variables after writes).
	EdgeIDByTriple(srcKey, dstKey, label string) (uint32, bool)
	IndexProperty(key string) error
	IndexFullText(key string) error
}

// IsWrite reports whether the statement mutates (write clauses or DDL).
// The parse error is returned unchanged: classification never invents
// errors, so an unparseable statement is the caller's parse failure, not
// a classification verdict.
func IsWrite(q string) (bool, error) {
	ast, err := Parse(q)
	if err != nil {
		return false, err
	}
	for _, c := range ast.Clauses {
		switch c.(type) {
		case *CreateClause, *SetClause, *DeleteClause, *MergeClause, *IndexClause:
			return true, nil
		}
	}
	return false, nil
}

// ---- write-path execution (RunWrite) -----------------------------------------
//
// The write statement's shape: an optional READ PREFIX (MATCH/WITH/UNWIND)
// runs over the pinned pre-write source through the ordinary stage
// machinery; then each write clause transforms the row set per row; an
// optional trailing RETURN projects rows whose entity bindings have been
// REBOUND through the writer against the post-write state.

// nodeRef / edgeRef are the key/triple identity of one bound entity: the
// write path never addresses entities by ID (IDs are generation-local),
// so every bound entity is resolved to its identity exactly once, memoized
// in the executor's nodeRefs/edgeRefs sidecars (ID-keyed: one var names a
// different entity per row of a multi-row statement).
type nodeRef struct{ key string }

type edgeRef struct{ srcKey, dstKey, label string }

// RunWrite executes a write statement. The read prefix runs over src
// (pinned generation, existing stage machinery); write stages drive w
// per input row, left to right; a trailing RETURN projects rows whose
// entity variables have been REBOUND through keys via w against the
// post-write state (Review Focus 4). The single implicit empty row
// anchors a bare CREATE/MERGE statement (same seeding matchRows uses).
func RunWrite(src Source, w Writer, q string) (*Result, error) {
	ast, err := Parse(q)
	if err != nil {
		return nil, err
	}
	ex := &executor{
		src: src, w: w,
		nodeRefs:     make(map[uint32]nodeRef),
		edgeRefs:     make(map[uint32]edgeRef),
		writtenNodes: make(map[uint32]engine.NodeRec),
		writtenEdges: make(map[uint32]writtenEdge),
	}
	// The CSR fast path requires BOTH directions (RunView's gating rule).
	if adjF, okF := src.FastAdjacency(0); okF {
		if adjR, okR := src.FastAdjacency(1); okR {
			ex.adjF, ex.adjR, ex.fastAdj = adjF, adjR, true
		}
	}
	return runStatement(ex, ast)
}

// createStage executes one CREATE clause: per input row, per pattern,
// nodes then edges, patterns left to right. New bindings land in the row
// (own* COW discipline — input rows may share maps with siblings), so a
// node created here is renderable in a later stage or the final RETURN.
// First error stops the statement; earlier rows' writes remain.
func (ex *executor) createStage(c *CreateClause, rows []row) ([]row, error) {
	if rows == nil {
		rows = []row{{}} // bare write statement: one implicit empty incoming row
	}
	out := make([]row, 0, len(rows))
	for _, r := range rows {
		nr := r.clone() // COW: mutations below copy shared maps exactly once
		nr.pos = 0      // stage-constructed row: no frontier to carry
		for _, pat := range c.Patterns {
			if err := ex.createPattern(pat, &nr); err != nil {
				return nil, err
			}
		}
		out = append(out, nr)
	}
	return out, nil
}

// createPattern writes one path pattern into r: all nodes first (left to
// right — an endpoint exists before any edge references it, and property
// expressions may read an earlier endpoint's creation-time state), then
// the edges. keys/ids memo the pattern's resolved endpoints for the edge
// pass; an endpoint is either REBOUND (var already bound in the row; its
// key resolved through the pinned source or this statement's writes) or
// CREATED fresh.
func (ex *executor) createPattern(pat *PathPattern, r *row) error {
	keys := make([]string, len(pat.Nodes))
	ids := make([]uint32, len(pat.Nodes))
	for i, np := range pat.Nodes {
		if np.Var != "" {
			if id, ok := r.nodes[np.Var]; ok { // rebind: the bound node is the identity
				key, ok := ex.boundNodeKey(id)
				if !ok {
					return &Error{np.Line, np.Col, fmt.Sprintf("bound node %q does not resolve to a key", np.Var)}
				}
				keys[i], ids[i] = key, id
				continue
			}
			// Bare form with the var unbound in THIS row: the rebind
			// spelling (validation allows only bare rebinds) met an
			// OPTIONAL MATCH null. Creating a nameless fresh node instead
			// would silently invent data — error.
			if np.Label == "" && np.Exprs == nil {
				line, col := np.Line, np.Col
				if line == 0 {
					line, col = 1, 1
				}
				return &Error{line, col, fmt.Sprintf("variable %q is not bound in this row (an OPTIONAL MATCH null); CREATE cannot rebind it", np.Var)}
			}
		}
		key, id, err := ex.createNode(np, *r)
		if err != nil {
			return err
		}
		keys[i], ids[i] = key, id
		if np.Var != "" {
			r.ownNodes()
			r.nodes[np.Var] = id
		}
	}
	for i, ep := range pat.Edges {
		if len(ep.Labels) != 1 { // nil or multi-label: no single triple to write
			line, col := ep.Line, ep.Col
			if line == 0 {
				line, col = 1, 1
			}
			return &Error{line, col, "write edge patterns require exactly one relationship label"}
		}
		props := make(map[string]any, len(ep.Exprs))
		for name, e := range ep.Exprs {
			if v, ok := ex.eval(e, *r); ok && v != nil {
				props[name] = v // null-valued props are skipped, never stored
			}
		}
		eid, err := ex.w.CreateEdge(keys[i], keys[i+1], ep.Labels[0], props)
		if err != nil {
			return err // missing endpoints are impossible here: nodes ran first
		}
		ex.sum.CreatedEdges++
		ex.writtenEdges[eid] = writtenEdge{src: ids[i], dst: ids[i+1], label: ep.Labels[0], props: props}
		if ep.Var != "" {
			r.ownEdges()
			r.edges[ep.Var] = eid
		}
	}
	return nil
}

// createNode evaluates one fresh node pattern against the row and writes
// it: the "key" expression (when present) must evaluate to a string —
// positioned runtime error otherwise — the evaluated property map MINUS
// key goes to CreateNode, and the record lands in writtenNodes so later
// patterns and the RETURN projection read the created state.
func (ex *executor) createNode(np *NodePattern, r row) (string, uint32, error) {
	var key string
	if kexpr, has := np.Exprs["key"]; has {
		v, ok := ex.eval(kexpr, r)
		if !ok {
			v = nil
		}
		if s, isStr := v.(string); isStr {
			key = s
		} else {
			line, col := kexpr.pos()
			if line == 0 {
				line, col = 1, 1
			}
			return "", 0, &Error{line, col, fmt.Sprintf("key property must be a string (the node's logical key); got %s", litTypeName(v))}
		}
	} else {
		key = ex.w.NewNodeKey()
	}
	props := make(map[string]any, len(np.Exprs))
	for name, e := range np.Exprs {
		if name == "key" {
			continue // identity, not a stored property
		}
		if v, ok := ex.eval(e, r); ok && v != nil {
			props[name] = v
		}
	}
	id, created, err := ex.w.CreateNode(key, np.Label, props)
	if err != nil {
		return "", 0, err
	}
	if created {
		ex.sum.CreatedNodes++
	}
	ex.writtenNodes[id] = engine.NodeRec{Key: key, Label: np.Label, Props: props}
	return key, id, nil
}

// boundNodeKey resolves a bound node ID to its logical key: the refs memo
// first (each entity dereferences at most once), then writtenNodes (nodes
// this statement created), then the PINNED pre-write source. ok=false only
// for an ID resolvable nowhere — impossible through the parse-time rules.
func (ex *executor) boundNodeKey(id uint32) (string, bool) {
	if ref, ok := ex.nodeRefs[id]; ok {
		return ref.key, true
	}
	rec, ok := ex.nodeInfo(id)
	if !ok {
		return "", false
	}
	ex.nodeRefs[id] = nodeRef{key: rec.Key}
	return rec.Key, true
}

// rebindRows remaps every row's entity bindings to the POST-write state:
// each binding's memoized key/triple resolves through w (NodeIDByKey /
// EdgeIDByTriple); a miss means the entity no longer exists and the var is
// DROPPED from the row — itemValue's unbound arm then renders null (the
// Review Focus 4 null-safety). Bindings this statement created are already
// post-write IDs and pass through unchanged.
func (ex *executor) rebindRows(rows []row) []row {
	for i := range rows {
		r := &rows[i]
		if len(r.nodes) > 0 {
			r.ownNodes() // COW: sibling rows may share this map
			for v, id := range r.nodes {
				if nid, ok := ex.rebindNode(id); ok {
					r.nodes[v] = nid
				} else {
					delete(r.nodes, v)
				}
			}
		}
		if len(r.edges) > 0 {
			r.ownEdges()
			for v, id := range r.edges {
				if nid, ok := ex.rebindEdge(id); ok {
					r.edges[v] = nid
				} else {
					delete(r.edges, v)
				}
			}
		}
	}
	return rows
}

func (ex *executor) rebindNode(id uint32) (uint32, bool) {
	ref, ok := ex.nodeRefs[id]
	if !ok {
		rec, resolved := ex.nodeInfo(id)
		if !resolved {
			return id, true // bound by this statement's writes: already post-write
		}
		ref = nodeRef{key: rec.Key}
		ex.nodeRefs[id] = ref
	}
	return ex.w.NodeIDByKey(ref.key) // miss = deleted: the caller drops the var
}

func (ex *executor) rebindEdge(id uint32) (uint32, bool) {
	ref, ok := ex.edgeRefs[id]
	if !ok {
		src, dst, label, _, resolved := ex.edgeInfo(id)
		if !resolved {
			return id, true // created by this statement
		}
		srck, okS := ex.boundNodeKey(src) // endpoint IDs -> keys, pinned/memoized
		dstk, okD := ex.boundNodeKey(dst)
		if !okS || !okD {
			return id, true
		}
		ref = edgeRef{srcKey: srck, dstKey: dstk, label: label}
		ex.edgeRefs[id] = ref
	}
	return ex.w.EdgeIDByTriple(ref.srcKey, ref.dstKey, ref.label)
}
