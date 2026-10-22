package cypher

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/projanvil/vectra/internal/csr"
	"github.com/projanvil/vectra/internal/engine"
)

// maxRows caps intermediate pattern rows (binding tuples) per expansion
// step. Exceeding it is a query-shape problem, not a data problem: narrow
// the pattern or add LIMIT. Package variable so tests can lower it.
var maxRows = 1_000_000

// NodeVal / EdgeVal are the node/edge values projected into result rows.
type NodeVal struct {
	ID    uint32         `json:"id"`
	Key   string         `json:"key"`
	Label string         `json:"label"`
	Props map[string]any `json:"props,omitempty"`
}

type EdgeVal struct {
	ID    uint32         `json:"id"`
	Src   uint32         `json:"src"`
	Dst   uint32         `json:"dst"`
	Label string         `json:"label"`
	Props map[string]any `json:"props,omitempty"`
}

// Result is the outcome of one statement: ordered columns and one map per
// matching row. Cell values are NodeVal, EdgeVal, string, float64, bool or
// nil.
type Result struct {
	Columns []string
	Rows    []map[string]any
}

// Source is the three-source execution surface (v0.7 spec §3.2): node/edge
// records by ID across base ∪ delta layers, the joint label table, the
// three-source property lookups for pattern anchoring, and adjacency rows
// (with the empty-delta CSR fast path). The root facade implements it over
// one immutable generation; engine.View supplies the row half structurally.
// Sources may optionally also implement rowSink (below): the buffer-reuse
// form of Rows that keeps the view-path expansion allocation-free.
type Source interface {
	NodeCount() int
	NodeInfo(id uint32) (engine.NodeRec, bool)
	EdgeInfo(id uint32) (src, dst uint32, label string, props map[string]any, ok bool)
	LabelCount() int
	LabelID(name string) (uint32, bool)
	LookupIndex(key, value string) ([]uint32, bool)
	LookupNumber(key string, value float64) ([]uint32, bool)
	LookupBool(key string, value bool) ([]uint32, bool)
	FastAdjacency(dir int) (csr.Adjacency, bool)
	Rows(u uint32, dir int) []csr.AdjRow
}

// rowSink is Source's optional buffer-reuse surface: sources that can
// append their adjacency rows into a caller-owned buffer implement it (the
// root facade does, over engine.View.RowsInto). directions() fills its
// executor scratch through it; sources without it keep the Rows copy path.
type rowSink interface {
	RowsInto(u uint32, dir int, buf []csr.AdjRow) []csr.AdjRow
}

// Run parses and executes q against the sealed snapshot (the merged-base
// hot path: no delta indirection anywhere).
func Run(snap *engine.Snapshot, q string) (*Result, error) {
	ast, err := Parse(q)
	if err != nil {
		return nil, err
	}
	ex := &executor{snap: snap, adjF: snap.AdjacencyF(), adjR: snap.AdjacencyR(), fastAdj: true}
	return runStatement(ex, ast)
}

// RunView parses and executes q against a three-source Source. When the
// source's delta is empty it takes the same CSR hot path as Run (via
// FastAdjacency); otherwise adjacency rows come from the View.
func RunView(src Source, q string) (*Result, error) {
	ast, err := Parse(q)
	if err != nil {
		return nil, err
	}
	ex := &executor{src: src}
	// The CSR fast path requires BOTH directions: taking it on adjF alone
	// would leave a zero-value adjR readable with fastAdj set (expansion
	// would silently read empty reverse rows). Current sources decide
	// symmetrically, but the interface does not promise it — gate on both.
	if adjF, okF := src.FastAdjacency(0); okF {
		if adjR, okR := src.FastAdjacency(1); okR {
			ex.adjF, ex.adjR, ex.fastAdj = adjF, adjR, true
		}
	}
	return runStatement(ex, ast)
}

func runStatement(ex *executor, ast *Query) (*Result, error) {
	rows, err := ex.matchRows(ast.Match)
	if err != nil {
		return nil, err
	}
	if ast.Match.Where != nil {
		filtered := make([]row, 0, len(rows))
		for _, r := range rows {
			if v, ok := ex.eval(ast.Match.Where, r); ok && v == true {
				filtered = append(filtered, r)
			}
		}
		rows = filtered
	}
	return ex.project(ast, rows)
}

// row is one binding tuple plus the pattern's frontier position (the node
// the next edge segment expands from). Binding maps are copy-on-write:
// clone() shares them and both sides are flagged, and a map is copied
// exactly once, lazily, by the ownNodes/ownEdges helpers below. Hard
// constraint: a row's maps are mutated ONLY through those helpers, and
// never after the row has been stored into a row set or cloned from —
// every mutation site (extendNode, expand's edge binding, runPath's
// cartesian anchor) writes only freshly cloned rows.
type row struct {
	pos   uint32
	nodes map[string]uint32 // node var -> node ID
	edges map[string]uint32 // edge var -> edge ID
	// shared flags: the corresponding map is aliased by another row (set on
	// BOTH sides by clone). Zero value false = exclusively owned, which is
	// the state of every freshly constructed row.
	nodesShared bool
	edgesShared bool
}

// clone returns a row sharing r's binding maps copy-on-write. The eager
// deep copy cost 2 map allocs per row on every expand/branch (fan-32
// 1-hop queries paid it 33x per query); sharing removes that, and the
// own* helpers restore isolation exactly where a write actually happens.
// Both the source and the child are marked shared so a later write on
// either side copies first — siblings and parent never observe each
// other's bindings.
func (r *row) clone() row {
	if r.nodes != nil {
		r.nodesShared = true
	}
	if r.edges != nil {
		r.edgesShared = true
	}
	return row{
		pos:         r.pos,
		nodes:       r.nodes,
		edges:       r.edges,
		nodesShared: r.nodes != nil,
		edgesShared: r.edges != nil,
	}
}

// ownNodes makes r's node map safe to mutate: a shared map is copied once
// (this row keeps the private copy; every other holder keeps the original,
// unmodified), a nil map is created fresh. Allocation-free when the row
// already owns a non-nil map.
func (r *row) ownNodes() {
	if r.nodesShared {
		m := make(map[string]uint32, len(r.nodes)+1)
		maps.Copy(m, r.nodes)
		r.nodes, r.nodesShared = m, false
	} else if r.nodes == nil {
		r.nodes = make(map[string]uint32, 1)
	}
}

// ownEdges is ownNodes for the edge map.
func (r *row) ownEdges() {
	if r.edgesShared {
		m := make(map[string]uint32, len(r.edges)+1)
		maps.Copy(m, r.edges)
		r.edges, r.edgesShared = m, false
	} else if r.edges == nil {
		r.edges = make(map[string]uint32, 1)
	}
}

// dedupKey identifies a row: named bindings plus the frontier position, so
// rows that differ only in anonymous end nodes stay distinct (Cypher
// semantics) while true duplicates from parallel expansion paths collapse.
func (r row) dedupKey() string {
	parts := make([]string, 0, len(r.nodes)+len(r.edges)+1)
	parts = append(parts, fmt.Sprintf("p=%d", r.pos))
	for k, v := range r.nodes {
		parts = append(parts, fmt.Sprintf("n:%s=%d", k, v))
	}
	for k, v := range r.edges {
		parts = append(parts, fmt.Sprintf("e:%s=%d", k, v))
	}
	slices.Sort(parts[1:]) // position first, bindings in stable order
	return strings.Join(parts, ";")
}

type executor struct {
	snap *engine.Snapshot // merged-base execution (Run); nil in RunView
	src  Source           // three-source execution (RunView); nil in Run
	// adjacency closures for expansion: the CSR slices on both entries when
	// the delta is empty (A1 fast path), nil rows otherwise.
	adjF    csr.Adjacency
	adjR    csr.Adjacency
	fastAdj bool // adjF/adjR carry the whole graph (expansion uses them)
	// labelIDCache: name -> (id, found)
	labelIDCache map[string]uint32
	// BFS scratch shared across variable-length expansions: depths[i] is
	// valid when gen[i] == current generation.
	depths []uint32
	gen    []uint32
	genNow uint32
	// view-path expansion scratch (directions' row accessors): rowBuf feeds
	// RowsInto (or the Rows copy for sink-less sources); the zip slices are
	// what the accessors return, valid only until the executor's next
	// accessor call — both call sites (expand's fixed-hop loop,
	// reachableWithin's BFS) consume them inline. One executor per
	// Run/RunView call keeps this per-query, per-goroutine: no locks.
	rowBuf  []csr.AdjRow
	rowTg   []uint32
	rowEids []uint32
	rowLbs  []uint32
}

// The three-source dispatch helpers: RunView's source answers, Run's
// snapshot otherwise. Exactly one of snap/src is set per executor.

func (ex *executor) nodeCount() int {
	if ex.src != nil {
		return ex.src.NodeCount()
	}
	return ex.snap.NodeCount()
}

func (ex *executor) nodeInfo(id uint32) (engine.NodeRec, bool) {
	if ex.src != nil {
		return ex.src.NodeInfo(id)
	}
	return ex.snap.NodeInfo(id)
}

func (ex *executor) edgeInfo(id uint32) (uint32, uint32, string, map[string]any, bool) {
	if ex.src != nil {
		return ex.src.EdgeInfo(id)
	}
	return ex.snap.EdgeInfo(id)
}

func (ex *executor) labelCount() int {
	if ex.src != nil {
		return ex.src.LabelCount()
	}
	return ex.snap.LabelCount()
}

func (ex *executor) labelID(name string) (uint32, bool) {
	if ex.src != nil {
		return ex.src.LabelID(name)
	}
	return ex.snap.LabelID(name)
}

func (ex *executor) lookupIndex(key, value string) ([]uint32, bool) {
	if ex.src != nil {
		return ex.src.LookupIndex(key, value)
	}
	return ex.snap.LookupIndex(key, value)
}

func (ex *executor) lookupNumber(key string, value float64) ([]uint32, bool) {
	if ex.src != nil {
		return ex.src.LookupNumber(key, value)
	}
	return ex.snap.LookupNumber(key, value)
}

func (ex *executor) lookupBool(key string, value bool) ([]uint32, bool) {
	if ex.src != nil {
		return ex.src.LookupBool(key, value)
	}
	return ex.snap.LookupBool(key, value)
}

func (ex *executor) labelMask(labels []string) []bool {
	if len(labels) == 0 {
		return nil
	}
	if ex.labelIDCache == nil {
		ex.labelIDCache = map[string]uint32{}
	}
	mask := make([]bool, ex.labelCount())
	for _, name := range labels {
		id, cached := ex.labelIDCache[name]
		if !cached {
			var ok bool
			id, ok = ex.labelID(name)
			if !ok {
				continue // label never interned: no edge can match it
			}
			ex.labelIDCache[name] = id
		}
		mask[id] = true
	}
	return mask
}

// matchRows executes every comma-separated path pattern in sequence,
// joining on shared variables (cartesian product when none are shared).
func (ex *executor) matchRows(m *MatchClause) ([]row, error) {
	rows := []row{}
	for _, pat := range m.Patterns {
		var err error
		rows, err = ex.runPath(pat, rows)
		if err != nil {
			return nil, err
		}
		rows = dedupRows(rows)
		if len(rows) > maxRows {
			return nil, &Error{1, 1, fmt.Sprintf("result too large: more than %d intermediate rows; narrow the pattern or add LIMIT", maxRows)}
		}
	}
	return rows, nil
}

// dedupPairwiseMax is the batch size below which dedupRows compares rows
// pairwise instead of building string keys: small expansion batches (the
// common shape) dedup with zero allocations — the key path spent ~9 allocs
// per row on Sprintf/Join (3 strings per binding), which dominated the
// view-path query budget. Above it, the map path's O(n) beats O(n²).
const dedupPairwiseMax = 64

// dedupRows drops exact duplicates (same frontier position and identical
// node/edge bindings), keeping first occurrences in order. Small batches
// compare pairwise against the kept prefix — allocation-free and exactly
// the seen-key semantics, since a row IS its bindings (dedupKey encodes
// nothing else); large batches keep the string-key map.
func dedupRows(rows []row) []row {
	if len(rows) <= dedupPairwiseMax {
		out := rows[:0]
		for _, r := range rows {
			dup := false
			for j := range out {
				if sameRow(out[j], r) {
					dup = true
					break
				}
			}
			if !dup {
				out = append(out, r)
			}
		}
		return out
	}
	seen := make(map[string]bool, len(rows))
	out := rows[:0]
	for _, r := range rows {
		k := r.dedupKey()
		if !seen[k] {
			seen[k] = true
			out = append(out, r)
		}
	}
	return out
}

// sameRow is full binding-tuple equality (the pairwise form of dedupKey:
// same position, same node bindings, same edge bindings).
func sameRow(a, b row) bool {
	if a.pos != b.pos || len(a.nodes) != len(b.nodes) || len(a.edges) != len(b.edges) {
		return false
	}
	for k, v := range a.nodes {
		if bv, ok := b.nodes[k]; !ok || bv != v {
			return false
		}
	}
	for k, v := range a.edges {
		if bv, ok := b.edges[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

// runPath evaluates one path pattern against the incoming rows (empty for
// the first pattern: the path's first node is the anchor).
func (ex *executor) runPath(pat *PathPattern, base []row) ([]row, error) {
	first := pat.Nodes[0]
	var rows []row
	if len(base) == 0 {
		for _, id := range ex.anchorCandidates(first) {
			r := row{pos: id}
			if first.Var != "" {
				r.nodes = map[string]uint32{first.Var: id}
			}
			rows = append(rows, r)
		}
	} else {
		for _, b := range base {
			if first.Var != "" && b.nodes != nil {
				if id, ok := b.nodes[first.Var]; ok { // shared variable: join on the binding
					if !ex.nodePred(first, id) {
						continue
					}
					r := b.clone()
					r.pos = id
					rows = append(rows, r)
					continue
				}
			}
			// not bound: cartesian over anchor candidates
			for _, id := range ex.anchorCandidates(first) {
				r := b.clone()
				r.pos = id
				if first.Var != "" {
					r.ownNodes() // COW: siblings share b's map until first write
					r.nodes[first.Var] = id
				}
				rows = append(rows, r)
			}
		}
	}
	if len(rows) > maxRows {
		return nil, &Error{1, 1, fmt.Sprintf("result too large: more than %d intermediate rows; narrow the pattern or add LIMIT", maxRows)}
	}
	for i, ep := range pat.Edges {
		var err error
		rows, err = ex.expand(rows, ep, pat.Nodes[i+1])
		if err != nil {
			return nil, err
		}
		if len(rows) > maxRows {
			return nil, &Error{1, 1, fmt.Sprintf("result too large: more than %d intermediate rows; narrow the pattern or add LIMIT", maxRows)}
		}
	}
	return rows, nil
}

// anchorCandidates resolves a node pattern to start IDs: property-map equalities
// on indexed keys narrow via the typed indexes (intersecting when several
// apply); anything left falls back to a full scan with the node predicate.
func (ex *executor) anchorCandidates(np *NodePattern) []uint32 {
	n := uint32(ex.nodeCount())
	var cand []uint32 // nil = unconstrained yet
	for k, v := range np.Props {
		var ids []uint32
		var ok bool
		switch tv := v.(type) {
		case string:
			ids, ok = ex.lookupIndex(k, tv)
		case bool:
			ids, ok = ex.lookupBool(k, tv)
		default:
			if f, isNum := numVal(v); isNum {
				ids, ok = ex.lookupNumber(k, f)
			}
		}
		if !ok {
			continue // unindexed key: checked by the node predicate instead
		}
		if cand == nil {
			cand = ids
		} else {
			cand = intersectIDs(cand, ids)
		}
	}
	if cand == nil {
		cand = make([]uint32, 0, n)
		for id := uint32(0); id < n; id++ {
			cand = append(cand, id)
		}
	}
	out := cand[:0]
	for _, id := range cand {
		if ex.nodePred(np, id) {
			out = append(out, id)
		}
	}
	return out
}

func intersectIDs(a, b []uint32) []uint32 {
	set := make(map[uint32]bool, len(b))
	for _, v := range b {
		set[v] = true
	}
	out := a[:0]
	for _, v := range a {
		if set[v] {
			out = append(out, v)
		}
	}
	return out
}

// nodePred is a node pattern's own predicate: label equality plus property-map
// equality (numbers compared after normalization). The pseudo-props "key" and
// "label" read the node's identity fields, shadowing any same-named property.
// The identity fields compare WITHOUT boxing into any: the anchor scan runs
// nodePred once per node per pattern, and string boxing there was ~100
// allocs/query on the view path (zero-row MATCH baseline).
func (ex *executor) nodePred(np *NodePattern, id uint32) bool {
	rec, ok := ex.nodeInfo(id)
	if !ok {
		return false
	}
	if np.Label != "" && rec.Label != np.Label {
		return false
	}
	for k, want := range np.Props {
		var match bool
		switch k {
		case "key":
			// litEq on a string reduces to want being the same string; the
			// unboxed form must stay equivalent (a numeric want is a
			// definite mismatch).
			ws, isStr := want.(string)
			match = isStr && rec.Key == ws
		case "label":
			ws, isStr := want.(string)
			match = isStr && rec.Label == ws
		default:
			got, present := rec.Props[k]
			match = present && litEq(got, want)
		}
		if !match {
			return false
		}
	}
	return true
}

// litEq compares a stored property against a pattern literal with numeric
// normalization (int 3 == float64 3).
func litEq(a, b any) bool {
	if af, aok := numVal(a); aok {
		if bf, bok := numVal(b); bok {
			return af == bf
		}
		return false
	}
	return a == b
}

// numVal normalizes any numeric value to float64 (query literals arrive as
// float64; stored props may be any int/uint/float width or json.Number).
func numVal(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	case jsonNumber:
		f, err := n.Float64()
		return f, err == nil
	}
	return 0, false
}

// jsonNumber avoids importing encoding/json just for this one type.
type jsonNumber interface{ Float64() (float64, error) }

// expand advances every row across one edge segment onto the next node pattern.
// Fixed single hop enumerates adjacency slots (parallel edges yield distinct
// rows when the edge is named); variable-length segments run a per-row BFS
// (node-pair semantics: intermediate nodes are not filtered by the target
// pattern, and no edges are bound).
func (ex *executor) expand(rows []row, ep *EdgePattern, np *NodePattern) ([]row, error) {
	mask := ex.labelMask(ep.Labels)
	var out []row
	if ep.MinHops == 1 && ep.MaxHops == 1 {
		for _, r := range rows {
			for _, pass := range ex.directions(ep.Dir) {
				targets, eids, labs := pass(r.pos)
				for i := range targets {
					if mask != nil && !mask[labs[i]] {
						continue
					}
					if !ex.nodePred(np, targets[i]) {
						continue
					}
					nr, ok := r.extendNode(np, targets[i])
					if !ok {
						continue
					}
					if ep.Var != "" {
						if prev, dup := nr.edges[ep.Var]; dup && prev != eids[i] {
							continue // repeated edge var must agree
						}
						nr.ownEdges() // COW: siblings share r's map until first write
						nr.edges[ep.Var] = eids[i]
					}
					out = append(out, nr)
				}
			}
		}
		return out, nil
	}
	for _, r := range rows {
		for id, d := range ex.reachableWithin(r.pos, mask, ep.Dir, ep.MaxHops) {
			if int(d) < ep.MinHops {
				continue // below the hop window (e.g. the start node itself)
			}
			nr, ok := r.extendNode(np, id)
			if !ok {
				continue
			}
			out = append(out, nr)
		}
	}
	return out, nil
}

// directions returns the adjacency-row accessors for the edge direction.
// On the CSR fast path (adjF/adjR set) these are the plain slice-zip Row
// closures; on the three-source path each accessor zips the View's AdjRow
// slice into the parallel arrays the expansion loops consume — through
// executor scratch filled via the source's optional rowSink (a Rows copy
// for sources without it), so steady state allocates nothing per expanded
// node (TestDirectionsZeroAllocSteadyState). The returned slices are that
// scratch: valid only until the executor's next accessor call, which both
// call sites honor by consuming rows inline before the next pass.
func (ex *executor) directions(dir int) []func(uint32) ([]uint32, []uint32, []uint32) {
	viewRow := func(d int) func(uint32) ([]uint32, []uint32, []uint32) {
		sink, canSink := ex.src.(rowSink) // resolved once per accessor, not per node
		return func(u uint32) ([]uint32, []uint32, []uint32) {
			if canSink {
				ex.rowBuf = sink.RowsInto(u, d, ex.rowBuf)
			} else {
				ex.rowBuf = append(ex.rowBuf[:0], ex.src.Rows(u, d)...)
			}
			ex.rowTg, ex.rowEids, ex.rowLbs = ex.rowTg[:0], ex.rowEids[:0], ex.rowLbs[:0]
			for _, r := range ex.rowBuf {
				ex.rowTg = append(ex.rowTg, r.Target)
				ex.rowEids = append(ex.rowEids, r.EdgeID)
				ex.rowLbs = append(ex.rowLbs, r.LabelID)
			}
			return ex.rowTg, ex.rowEids, ex.rowLbs
		}
	}
	if ex.src != nil && !ex.fastAdj {
		switch dir {
		case DirIn:
			return []func(uint32) ([]uint32, []uint32, []uint32){viewRow(1)}
		case DirBoth:
			return []func(uint32) ([]uint32, []uint32, []uint32){viewRow(0), viewRow(1)}
		default:
			return []func(uint32) ([]uint32, []uint32, []uint32){viewRow(0)}
		}
	}
	switch dir {
	case DirIn:
		return []func(uint32) ([]uint32, []uint32, []uint32){ex.adjR.Row}
	case DirBoth:
		return []func(uint32) ([]uint32, []uint32, []uint32){ex.adjF.Row, ex.adjR.Row}
	default:
		return []func(uint32) ([]uint32, []uint32, []uint32){ex.adjF.Row}
	}
}

// extendNode returns the row advanced to node id, enforcing repeated-variable
// agreement (cycle patterns). ok=false when a repeated var binds differently.
func (r row) extendNode(np *NodePattern, id uint32) (row, bool) {
	if np.Var != "" && r.nodes != nil {
		if prev, dup := r.nodes[np.Var]; dup && prev != id {
			return row{}, false
		}
	}
	nr := r.clone()
	nr.pos = id
	if np.Var != "" {
		nr.ownNodes() // COW: sibling extensions share r's map until first write
		nr.nodes[np.Var] = id
	}
	return nr, true
}

// reachableWithin computes shortest BFS depths from start (bounded by maxHops,
// unbounded when max<0) with an edge-label mask and direction, using
// generation-stamped scratch arrays. The returned map holds every visited
// node with its depth; the caller applies the hop window's lower bound and
// the target node pattern.
func (ex *executor) reachableWithin(start uint32, mask []bool, dir, maxHops int) map[uint32]uint32 {
	n := uint32(ex.nodeCount())
	if int(start) >= int(n) {
		return nil
	}
	if ex.depths == nil {
		ex.depths = make([]uint32, n)
		ex.gen = make([]uint32, n)
		ex.genNow = 0
	}
	ex.genNow++
	gen := ex.genNow
	passes := ex.directions(dir)
	frontier := []uint32{start}
	ex.depths[start], ex.gen[start] = 0, gen
	if maxHops < 0 {
		maxHops = int(n) // simple-path upper bound; longer revisiting walks are not tracked (node-pair semantics)
	}
	for d := 1; d <= maxHops && len(frontier) > 0; d++ {
		var next []uint32
		for _, u := range frontier {
			for _, pass := range passes {
				targets, _, labs := pass(u)
				for i, v := range targets {
					if mask != nil && !mask[labs[i]] {
						continue
					}
					if ex.gen[v] == gen {
						continue
					}
					ex.gen[v], ex.depths[v] = gen, uint32(d)
					next = append(next, v)
				}
			}
		}
		frontier = next
	}
	out := map[uint32]uint32{}
	for id := uint32(0); id < n; id++ {
		if ex.gen[id] == gen {
			out[id] = ex.depths[id]
		}
	}
	return out
}
