package cypher

import (
	"fmt"
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

// Run parses and executes q against the sealed snapshot.
func Run(snap *engine.Snapshot, q string) (*Result, error) {
	ast, err := Parse(q)
	if err != nil {
		return nil, err
	}
	ex := &executor{snap: snap, adjF: snap.AdjacencyF(), adjR: snap.AdjacencyR()}
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
// the next edge segment expands from).
type row struct {
	pos   uint32
	nodes map[string]uint32 // node var -> node ID
	edges map[string]uint32 // edge var -> edge ID
}

func (r row) clone() row {
	nr := row{pos: r.pos}
	if r.nodes != nil {
		nr.nodes = make(map[string]uint32, len(r.nodes)+1)
		for k, v := range r.nodes {
			nr.nodes[k] = v
		}
	}
	if r.edges != nil {
		nr.edges = make(map[string]uint32, len(r.edges)+1)
		for k, v := range r.edges {
			nr.edges[k] = v
		}
	}
	return nr
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
	snap *engine.Snapshot
	adjF csr.Adjacency
	adjR csr.Adjacency
	// labelIDCache: name -> (id, found)
	labelIDCache map[string]uint32
	// BFS scratch shared across variable-length expansions: depths[i] is
	// valid when gen[i] == current generation.
	depths []uint32
	gen    []uint32
	genNow uint32
}

func (ex *executor) labelMask(labels []string) []bool {
	if len(labels) == 0 {
		return nil
	}
	if ex.labelIDCache == nil {
		ex.labelIDCache = map[string]uint32{}
	}
	mask := make([]bool, ex.snap.LabelCount())
	for _, name := range labels {
		id, cached := ex.labelIDCache[name]
		if !cached {
			var ok bool
			id, ok = ex.snap.LabelID(name)
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

func dedupRows(rows []row) []row {
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
					if r.nodes == nil {
						r.nodes = map[string]uint32{}
					}
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
	n := uint32(ex.snap.NodeCount())
	var cand []uint32 // nil = unconstrained yet
	for k, v := range np.Props {
		var ids []uint32
		var ok bool
		switch tv := v.(type) {
		case string:
			ids, ok = ex.snap.LookupIndex(k, tv)
		case bool:
			ids, ok = ex.snap.LookupBool(k, tv)
		default:
			if f, isNum := numVal(v); isNum {
				ids, ok = ex.snap.LookupNumber(k, f)
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
func (ex *executor) nodePred(np *NodePattern, id uint32) bool {
	rec, ok := ex.snap.NodeInfo(id)
	if !ok {
		return false
	}
	if np.Label != "" && rec.Label != np.Label {
		return false
	}
	for k, want := range np.Props {
		var got any
		var present bool
		switch k {
		case "key":
			got, present = rec.Key, true
		case "label":
			got, present = rec.Label, true
		default:
			got, present = rec.Props[k]
		}
		if !present || !litEq(got, want) {
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
						if nr.edges == nil {
							nr.edges = map[string]uint32{}
						}
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
func (ex *executor) directions(dir int) []func(uint32) ([]uint32, []uint32, []uint32) {
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
		if nr.nodes == nil {
			nr.nodes = map[string]uint32{}
		}
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
	n := uint32(ex.snap.NodeCount())
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
