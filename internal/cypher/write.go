package cypher

import (
	"fmt"
	"maps"

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
	// No guard: behavior on a closed graph is undefined, matching
	// AddNode — the consuming CreateNode call is what fails closed.
	NewNodeKey() string
	NodeIDByKey(key string) (uint32, bool)
	// CreateNode applies AddNode upsert semantics; created=false when the
	// key already existed (props replaced, per AddNode).
	CreateNode(key, label string, props map[string]any) (id uint32, created bool, err error)
	// SetNodeProps merges props; a nil value deletes the key; returns the
	// number of property keys written (sets + deletes).
	SetNodeProps(key string, props map[string]any) (int, error)
	NodeHasEdges(key string) bool
	// DeleteNodeCascade removes the node and every incident edge (DETACH
	// DELETE), returning the number of edges the cascade killed (the
	// summary's DeletedEdges counter adds them).
	DeleteNodeCascade(key string) (edgesDeleted int, err error)
	// DeleteEdgeTriple deletes every parallel edge on the triple (E6
	// divergence, pinned by test).
	DeleteEdgeTriple(srcKey, dstKey, label string) (int, error)
	UpsertEdgeTriple(srcKey, dstKey, label string, props map[string]any) (created bool, err error)
	// CreateEdge adds a NEW parallel edge (CREATE semantics — duplicates
	// allowed; contrast UpsertEdgeTriple's fold). Fails when an endpoint
	// is missing (statement error, first-error-stops).
	CreateEdge(srcKey, dstKey, label string, props map[string]any) (id uint32, err error)
	// SetEdgeProps writes props onto every live parallel edge of the
	// triple; returns the number of edges touched. The rewrite is
	// delete-then-re-add of the whole triple: a mid-loop failure leaves
	// it partially re-added (first-error-stops, no rollback).
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
		setEdgeFolds: make(map[edgeRef]writtenEdge),
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
					line, col := np.Line, np.Col
					if line == 0 { // hand-built pattern: no token positions
						line, col = 1, 1
					}
					return &Error{line, col, fmt.Sprintf("bound node %q does not resolve to a key", np.Var)}
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
		// Exactly one label per edge is a static checkWritePattern rule;
		// Labels[0] below is safe by validation.
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

// setStage executes one SET clause: per input row, items left to right
// through the shared SetItem machinery (applySetItems — MERGE's ON CREATE
// SET / ON MATCH SET trailers run the identical loop against their
// just-bound/created vars). First error stops the statement; earlier rows'
// writes remain. Rows pass through unchanged — SET mutates properties,
// never bindings.
func (ex *executor) setStage(c *SetClause, rows []row) ([]row, error) {
	if rows == nil {
		rows = []row{{}} // defensive: validation requires a binding clause before SET
	}
	for i := range rows {
		if err := ex.applySetItems(c.Items, &rows[i]); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

// applySetItems applies one ordered SetItem list to the row's bound
// entities: each item resolves its target through the row's bindings (node
// key via boundNodeKey, edge triple via boundEdgeRef — both memoized in
// the refs sidecars), evaluates the RHS against the row plus a per-list
// pending overlay of the values EARLIER items staged (SET a.x = 1, a.y =
// a.x must see the 1), and writes exactly one property: a nil value
// DELETES the key. PropertiesSet accumulates the writer's counts (sets +
// deletes for nodes; edges touched for the triple-addressed edge form).
// First error stops the list — and with it the statement.
func (ex *executor) applySetItems(items []*SetItem, r *row) error {
	var pending map[string]map[string]any // var -> prop -> staged value (this list only)
	for _, it := range items {
		val := any(nil) // the null outcome stages a delete
		if v, ok := ex.evalPending(it.Value, *r, pending); ok {
			val = v
		}
		if id, ok := r.nodes[it.Var]; ok { // nil-map lookup: safe miss
			key, okK := ex.boundNodeKey(id)
			if !okK {
				return &Error{1, 1, fmt.Sprintf("bound node %q does not resolve to a key", it.Var)}
			}
			n, err := ex.w.SetNodeProps(key, map[string]any{it.Prop: val})
			if err != nil {
				return err
			}
			ex.sum.PropertiesSet += n
			ex.overlayNodeProp(id, it.Prop, val)
		} else if id, ok := r.edges[it.Var]; ok {
			ref, okR := ex.boundEdgeRef(id)
			if !okR {
				return &Error{1, 1, fmt.Sprintf("bound edge %q does not resolve to a triple", it.Var)}
			}
			n, err := ex.w.SetEdgeProps(ref.srcKey, ref.dstKey, ref.label, map[string]any{it.Prop: val})
			if err != nil {
				return err
			}
			ex.sum.PropertiesSet += n
			ex.foldEdgeProp(ref, id, it.Prop, val)
		} else {
			// An OPTIONAL MATCH miss leaves the var unbound in the kept
			// row; Neo4j parity is the explicit null error.
			return &Error{1, 1, fmt.Sprintf("SET requires a bound node or edge variable; %s is null", it.Var)}
		}
		if pending == nil {
			pending = make(map[string]map[string]any)
		}
		props, ok := pending[it.Var]
		if !ok {
			props = make(map[string]any)
			pending[it.Var] = props
		}
		props[it.Prop] = val
	}
	return nil
}

// deleteStage executes one [DETACH] DELETE clause: per input row, vars
// left to right. A node var resolves its key through the refs sidecar
// (pinned pre-write identity, memoized once — the rebind after the stage
// reuses the memo), then: a node an earlier row already deleted resolves
// nowhere (NodeIDByKey miss) and deletes as a SILENT no-op — no count, no
// error, so DeletedNodes counts actual deletions; without DETACH a node
// that still has live incident edges errors the statement (first-error-
// stops fires BEFORE any write: the node and its edges survive); with DETACH
// or no edges, DeleteNodeCascade removes the node and its incident edges
// and DeletedEdges accumulates the cascade's kill count. An edge var
// resolves its triple, skipping SILENTLY when either endpoint is no longer
// live (a DETACH DELETE a, r whose a-cascade already killed r — the edge
// arm's mirror of the node arm's not-found no-op, never a leaked root
// error), and DeleteEdgeTriple removes every parallel at once: DeletedEdges
// accumulates the returned count, so one DELETE e of a two-parallel triple
// counts 2 (the E6 divergence). An unbound var (OPTIONAL MATCH miss)
// errors, the SET message shape. Rows pass through unchanged; the trailing
// RETURN's rebind drops deleted bindings.
func (ex *executor) deleteStage(c *DeleteClause, rows []row) ([]row, error) {
	if rows == nil {
		rows = []row{{}} // defensive: validation requires a binding clause before DELETE
	}
	for _, r := range rows {
		for _, v := range c.Vars {
			if id, ok := r.nodes[v]; ok { // nil-map lookup: safe miss
				key, okK := ex.boundNodeKey(id)
				if !okK {
					return nil, &Error{1, 1, fmt.Sprintf("bound node %q does not resolve to a key", v)}
				}
				if _, live := ex.w.NodeIDByKey(key); !live {
					continue // an earlier row's delete removed it: no-op, no count
				}
				if !c.Detach && ex.w.NodeHasEdges(key) {
					return nil, &Error{1, 1, fmt.Sprintf("cannot delete node %s: it still has edges (use DETACH DELETE)", key)}
				}
				killed, err := ex.w.DeleteNodeCascade(key)
				if err != nil {
					return nil, err
				}
				ex.sum.DeletedNodes++
				ex.sum.DeletedEdges += killed
			} else if id, ok := r.edges[v]; ok {
				ref, okR := ex.boundEdgeRef(id)
				if !okR {
					return nil, &Error{1, 1, fmt.Sprintf("bound edge %q does not resolve to a triple", v)}
				}
				// A triple whose endpoint died earlier in this statement
				// (the DETACH DELETE a, r case: a's cascade killed r) is
				// already gone — skip, no count, no error.
				if _, live := ex.w.NodeIDByKey(ref.srcKey); !live {
					continue
				}
				if _, live := ex.w.NodeIDByKey(ref.dstKey); !live {
					continue
				}
				n, err := ex.w.DeleteEdgeTriple(ref.srcKey, ref.dstKey, ref.label)
				if err != nil {
					return nil, err
				}
				ex.sum.DeletedEdges += n
			} else {
				// An OPTIONAL MATCH miss leaves the var unbound in the kept
				// row; Neo4j parity is the explicit null error (SET shape).
				return nil, &Error{1, 1, fmt.Sprintf("DELETE requires a bound node or edge variable; %s is null", v)}
			}
		}
	}
	return rows, nil
}

// mergeStage executes one MERGE clause: per input row, the clause's single
// pattern (validation guarantees exactly one — a lone node, or one directed
// edge with both endpoints resolvable) is matched by IDENTITY, not by full
// prop map (the documented divergence: a node by its logical key, an edge
// by its (srcKey, dstKey, label) triple). The node arm and the edge arm
// share the shape: probe the identity, bind on hit (created=false, ON
// MATCH SET items apply), create on miss (created=true, ON CREATE SET
// items apply); in both arms the pattern's own props are CREATION-TIME
// ONLY — a hit never rewrites them (ON MATCH SET is the update path).
// Summary counters move only on actual creates. First error stops the
// statement; earlier rows' writes remain.
func (ex *executor) mergeStage(c *MergeClause, rows []row) ([]row, error) {
	if rows == nil {
		rows = []row{{}} // bare MERGE statement: one implicit empty incoming row
	}
	out := make([]row, 0, len(rows))
	for _, r := range rows {
		nr := r.clone() // COW: new bindings below copy shared maps exactly once
		nr.pos = 0      // stage-constructed row: no frontier to carry
		if err := ex.mergePattern(c.Pattern, &nr, c.OnCreate, c.OnMatch); err != nil {
			return nil, err
		}
		out = append(out, nr)
	}
	return out, nil
}

// mergePattern dispatches the MERGE pattern to its arm (the validation
// shape: one node, or one edge between two nodes).
func (ex *executor) mergePattern(pat *PathPattern, r *row, onCreate, onMatch []*SetItem) error {
	if len(pat.Edges) == 0 {
		return ex.mergeNode(pat.Nodes[0], r, onCreate, onMatch)
	}
	return ex.mergeEdge(pat, r, onCreate, onMatch)
}

// mergeNode merges one node pattern: the identity key comes from the row's
// binding when the var is bound (the bare rebind spelling) or from the
// pattern's evaluated "key" expression (validator-guaranteed on fresh node
// patterns; the createStage runtime string check applies). A live key
// binds the existing ID and fires the ON MATCH items; a dead key runs the
// createStage create path and fires the ON CREATE items. A bound var whose
// node died earlier in this statement (DELETE then MERGE) recreates by the
// SAME key — the bare pattern contributes no label or props (validation),
// so the resurrection is nameless but identity-stable.
func (ex *executor) mergeNode(np *NodePattern, r *row, onCreate, onMatch []*SetItem) error {
	key, bound := "", false
	if np.Var != "" {
		if id, ok := r.nodes[np.Var]; ok {
			k, okK := ex.boundNodeKey(id)
			if !okK {
				line, col := np.Line, np.Col
				if line == 0 {
					line, col = 1, 1
				}
				return &Error{line, col, fmt.Sprintf("bound node %q does not resolve to a key", np.Var)}
			}
			key, bound = k, true
		}
	}
	if !bound {
		if kexpr, has := np.Exprs["key"]; has {
			v, ok := ex.eval(kexpr, *r)
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
				return &Error{line, col, fmt.Sprintf("key property must be a string (the node's logical key); got %s", litTypeName(v))}
			}
		} else {
			key = ex.w.NewNodeKey() // defensive: the grammar requires a key here; a keyless fresh merge can never hit
		}
	}
	if id, live := ex.w.NodeIDByKey(key); live {
		// hit: bind, never rewrite — the pattern's props are creation-time only
		if np.Var != "" {
			r.ownNodes()
			r.nodes[np.Var] = id
		}
		return ex.applySetItems(onMatch, r)
	}
	if bound {
		// the bound binding died earlier in this statement: recreate by the
		// same key (createNode cannot be reused — a bare rebind pattern
		// carries no key expression, so it would draw a ratchet key)
		id, created, err := ex.w.CreateNode(key, np.Label, nil)
		if err != nil {
			return err
		}
		if created {
			ex.sum.CreatedNodes++
		}
		// Non-nil props, exactly as createNode records: overlayNodeProp
		// assigns straight into an existing record's map.
		ex.writtenNodes[id] = engine.NodeRec{Key: key, Label: np.Label, Props: map[string]any{}}
		r.ownNodes()
		r.nodes[np.Var] = id
		return ex.applySetItems(onCreate, r)
	}
	_, id, err := ex.createNode(np, *r) // the createStage path: props MINUS key, CreatedNodes on created
	if err != nil {
		return err
	}
	if np.Var != "" {
		r.ownNodes()
		r.nodes[np.Var] = id
	}
	return ex.applySetItems(onCreate, r)
}

// mergeEdge merges one directed edge pattern (the validation shape: two
// nodes, one labeled fixed-hop outgoing edge). Endpoints resolve to
// IDENTITIES first, before any write: a bound var through the refs memo
// (the bare rebind spelling), a fresh pattern through its evaluated "key"
// expression (a live key binds the existing node; a dead or keyless
// pattern would create — keyless per CREATE's ratchet, so its triple can
// never pre-exist). EdgeIDByTriple then decides the arm BEFORE any
// endpoint is created (Neo4j full-pattern-match semantics: an existing
// edge binds the whole pattern — endpoints included — and suppresses
// endpoint creation even when the pattern spells them as fresh). The miss
// arm creates only the endpoints that need it (createStage rules) and
// writes the edge with UpsertEdgeTriple (fold semantics: same-row
// duplicates of the triple collapse onto one). The edge var binds for
// RETURN in both arms; the pattern's edge props are creation-time only.
func (ex *executor) mergeEdge(pat *PathPattern, r *row, onCreate, onMatch []*SetItem) error {
	ep := pat.Edges[0]
	label := ep.Labels[0] // exactly one label per write edge: static checkWritePattern rule
	keys, ids := make([]string, 2), make([]uint32, 2)
	create := [2]bool{} // endpoint needs creation on the miss arm
	known := true       // both endpoint keys resolved without the ratchet?
	for i, np := range pat.Nodes {
		if np.Var != "" {
			if id, ok := r.nodes[np.Var]; ok { // rebind: the bound node is the identity
				key, okK := ex.boundNodeKey(id)
				if !okK {
					line, col := np.Line, np.Col
					if line == 0 {
						line, col = 1, 1
					}
					return &Error{line, col, fmt.Sprintf("bound node %q does not resolve to a key", np.Var)}
				}
				keys[i], ids[i] = key, id
				continue
			}
			if np.Label == "" && np.Exprs == nil {
				// Bare form unbound in THIS row: the rebind spelling met an
				// OPTIONAL MATCH null (the createStage rule) — merging a
				// nameless fresh node would invent identity. Error.
				line, col := np.Line, np.Col
				if line == 0 {
					line, col = 1, 1
				}
				return &Error{line, col, fmt.Sprintf("variable %q is not bound in this row (an OPTIONAL MATCH null); MERGE cannot rebind it", np.Var)}
			}
		}
		if kexpr, has := np.Exprs["key"]; has {
			v, ok := ex.eval(kexpr, *r)
			if !ok {
				v = nil
			}
			if s, isStr := v.(string); isStr {
				keys[i] = s
			} else {
				line, col := kexpr.pos()
				if line == 0 {
					line, col = 1, 1
				}
				return &Error{line, col, fmt.Sprintf("key property must be a string (the node's logical key); got %s", litTypeName(v))}
			}
			if id, live := ex.w.NodeIDByKey(keys[i]); live {
				ids[i] = id // the key names an existing endpoint: no creation
			} else {
				create[i] = true
			}
		} else {
			// keyless fresh endpoint: CREATE's ratchet path — a generated
			// key is unique, so this endpoint's triple cannot pre-exist.
			create[i], known = true, false
		}
	}
	if known {
		if eid, hit := ex.w.EdgeIDByTriple(keys[0], keys[1], label); hit {
			// hit: the whole pattern exists — endpoints are live (a live
			// edge needs live endpoints) and bind by key; no writes at all.
			for i, np := range pat.Nodes {
				if np.Var != "" {
					id, _ := ex.w.NodeIDByKey(keys[i])
					r.ownNodes()
					r.nodes[np.Var] = id
				}
			}
			if ep.Var != "" {
				r.ownEdges()
				r.edges[ep.Var] = eid
			}
			return ex.applySetItems(onMatch, r)
		}
	}
	// miss: create the endpoints that need it (fresh patterns only — a
	// bound var or an existing key never creates), then the edge.
	for i, np := range pat.Nodes {
		if np.Var != "" && !create[i] {
			r.ownNodes()
			r.nodes[np.Var] = ids[i]
		}
		if !create[i] {
			continue
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
	props := make(map[string]any, len(ep.Exprs))
	for name, e := range ep.Exprs {
		if v, ok := ex.eval(e, *r); ok && v != nil {
			props[name] = v // null-valued props are skipped, never stored
		}
	}
	created, err := ex.w.UpsertEdgeTriple(keys[0], keys[1], label, props)
	if err != nil {
		return err
	}
	if created {
		ex.sum.CreatedEdges++
	}
	// Bind the edge var for RETURN: the write's canonical ID (the fold's
	// winner), recorded in writtenEdges so the projection reads post-write
	// truth — the pinned source does not know this edge.
	if eid, ok := ex.w.EdgeIDByTriple(keys[0], keys[1], label); ok {
		ex.writtenEdges[eid] = writtenEdge{src: ids[0], dst: ids[1], label: label, props: props}
		if ep.Var != "" {
			r.ownEdges()
			r.edges[ep.Var] = eid
		}
	}
	return ex.applySetItems(onCreate, r)
}

// indexStage applies one IndexClause: CREATE [FULLTEXT] INDEX declares the
// property key on the writer — a statement-level effect with no data
// touched. A no-rows statement by grammar (the parser admits an IndexClause
// only standalone: no read prefix, no RETURN may accompany it), so it
// applies exactly once per statement, never through the per-row stage
// machinery. The declaration itself is idempotent and PENDING until the
// next Seal (the Writer's root contract: lookups answer via the
// compensation scan until the merge seals the index) — and every
// successfully-applied statement counts one IndexesCreated, an idempotent
// re-declaration included (it is still a creation request).
func (ex *executor) indexStage(c *IndexClause) error {
	if c.FullText {
		if err := ex.w.IndexFullText(c.Prop); err != nil {
			return err
		}
	} else {
		if err := ex.w.IndexProperty(c.Prop); err != nil {
			return err
		}
	}
	ex.sum.IndexesCreated++
	return nil
}

// evalPending evaluates one SET item's RHS: a property read the row's
// pending overlay serves FIRST (values staged by earlier items of this
// row), everything else falls through to the ordinary evaluator — reads of
// other entities resolve through the pinned pre-write source, exactly as
// CREATE's property expressions do. PropRef-only by design: a nested or
// comparison RHS falls through whole (see foldEdgeProp for the edge-var
// consequence).
func (ex *executor) evalPending(e Expr, r row, pending map[string]map[string]any) (any, bool) {
	if pending != nil {
		if p, ok := e.(*PropRef); ok {
			if props := pending[p.Var]; props != nil {
				if v, seen := props[p.Name]; seen {
					return v, v != nil // a staged nil is a delete: the null outcome
				}
			}
		}
	}
	return ex.eval(e, r)
}

// overlayNodeProp folds one staged property into writtenNodes[id] so later
// items, later stages, and the RETURN projection read post-SET truth — the
// pinned source keeps serving the pre-write props. The record's props map
// is copied on first touch (the source's maps are shared read-only); every
// map ever stored into writtenNodes is executor-private, so later touches
// mutate in place. A nil value deletes.
func (ex *executor) overlayNodeProp(id uint32, prop string, val any) {
	rec, ok := ex.writtenNodes[id]
	if !ok {
		rec, _ = ex.nodeInfo(id) // writtenNodes miss: base is the pinned record
		p := make(map[string]any, len(rec.Props)+1)
		maps.Copy(p, rec.Props)
		rec.Props = p
	}
	if val == nil {
		delete(rec.Props, prop)
	} else {
		rec.Props[prop] = val
	}
	ex.writtenNodes[id] = rec
}

// foldEdgeProp stages one edge property write into the statement's
// triple-keyed fold (setEdgeFolds) and publishes it under the CURRENT
// canonical ID of the triple (EdgeIDByTriple — the same ID rebindRows
// remaps RETURN bindings to). The fold seeds from the PRE-WRITE record of
// the bound parallel (edgeInfo(boundID): writtenEdges first — an edge this
// statement created — then the pinned source), NEVER from the canonical ID
// itself: SetEdgeProps rewrites the triple with fresh EdgeIDs on every
// call, so a canonical ID resolves nowhere pre-write and moves again on
// every later item — seeding from it would empty the fold's base and strand
// earlier items' folds on stale IDs (Review F1).
//
// Known limitation (the pending overlay is PropRef-only, see evalPending):
// the fold publishes under the canonical ID while RHS reads resolve
// through the BOUND ID, so a nested or comparison RHS on an edge var
// (e.g. r.w + 1 after SET r.w = 1) reads the pinned pre-write value; only
// a direct property read sees the staged value. Node targets have no such
// gap — their overlay publishes under the bound ID itself.
func (ex *executor) foldEdgeProp(ref edgeRef, boundID uint32, prop string, val any) {
	fold, ok := ex.setEdgeFolds[ref]
	if !ok {
		src, dst, label, props, _ := ex.edgeInfo(boundID) // pre-write truth
		p := make(map[string]any, len(props)+1)
		maps.Copy(p, props)
		fold = writtenEdge{src: src, dst: dst, label: label, props: p}
		ex.setEdgeFolds[ref] = fold
	}
	if val == nil {
		delete(fold.props, prop)
	} else {
		fold.props[prop] = val
	}
	if canon, okC := ex.w.EdgeIDByTriple(ref.srcKey, ref.dstKey, ref.label); okC {
		ex.writtenEdges[canon] = fold // publish; fold.props is shared, so always current
	}
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

// boundEdgeRef resolves a bound edge ID to its (srcKey, dstKey, label)
// triple, memoized in edgeRefs (each entity dereferences at most once).
//
// Known limitation — the documented v1 contract: the triple-addressed
// writer methods (SetEdgeProps among them) and EdgeIDByTriple all address
// the lowest-ID live parallel, so a MATCH-bound edge variable addresses
// its WHOLE triple under SET: with parallel edges, SET r.p writes p onto
// every parallel, never just the one edge r happened to bind.
func (ex *executor) boundEdgeRef(id uint32) (edgeRef, bool) {
	if ref, ok := ex.edgeRefs[id]; ok {
		return ref, true
	}
	src, dst, label, _, resolved := ex.edgeInfo(id)
	if !resolved {
		return edgeRef{}, false
	}
	srck, okS := ex.boundNodeKey(src) // endpoint IDs -> keys, pinned/memoized
	dstk, okD := ex.boundNodeKey(dst)
	if !okS || !okD {
		return edgeRef{}, false
	}
	ref := edgeRef{srcKey: srck, dstKey: dstk, label: label}
	ex.edgeRefs[id] = ref
	return ref, true
}

func (ex *executor) rebindEdge(id uint32) (uint32, bool) {
	ref, ok := ex.boundEdgeRef(id)
	if !ok {
		return id, true // created by this statement: already post-write
	}
	return ex.w.EdgeIDByTriple(ref.srcKey, ref.dstKey, ref.label)
}
