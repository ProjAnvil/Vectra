package cypher

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
)

// maxGroups caps the number of distinct groups an aggregation may build
// (a query-shape problem, like maxRows). Package variable so tests can
// lower it.
var maxGroups = 1_000_000

// project applies the terminal RETURN clause's items / ORDER BY / SKIP /
// LIMIT to the stage loop's final row set (ret is the RETURN clause
// itself; m is the MATCH clause RETURN * expands, tracked by the stage
// loop). ORDER BY keys evaluate var[.prop] on the binding itself, so they
// may reference properties not present in RETURN (Cypher-compatible); a
// key that names a projected column (aliases included) uses that value.
func (ex *executor) project(ret *ReturnClause, m *MatchClause, rows []row) (*Result, error) {
	items := ret.Items
	if ret.Star {
		items = starItems(m)
	}
	for _, it := range items {
		if it.Agg.Func != "" {
			return ex.projectAgg(ret, items, rows)
		}
	}
	res := &Result{Columns: make([]string, len(items))}
	for i, it := range items {
		res.Columns[i] = it.Name
	}
	inCols := make(map[string]bool, len(items))
	for _, it := range items {
		inCols[it.Name] = true
	}
	projRows := make([]map[string]any, len(rows))
	for i, r := range rows {
		m := make(map[string]any, len(items))
		for _, it := range items {
			m[it.Name] = ex.itemValue(it, r)
		}
		projRows[i] = m
	}
	if len(ret.OrderBy) > 0 {
		keys := ret.OrderBy
		sortVals := make([][]any, len(rows))
		for i, r := range rows {
			vals := make([]any, len(keys))
			for j, k := range keys {
				if inCols[k.Name] {
					vals[j] = projRows[i][k.Name] // projected column (alias) value
				} else {
					vals[j] = ex.orderValue(k.Name, r)
				}
			}
			sortVals[i] = vals
		}
		projRows = sortRows(projRows, keys, sortVals)
	}
	projRows = applySkipLimit(projRows, ret.Skip, ret.Limit)
	res.Rows = projRows
	return res, nil
}

// projectAgg is the aggregation path: every non-aggregating RETURN item is a
// grouping key, aggregations accumulate per group in one pass over rows, and
// ORDER BY / SKIP / LIMIT apply afterwards (LIMIT never short-circuits the
// pass).
func (ex *executor) projectAgg(ret *ReturnClause, items []*ReturnItem, rows []row) (*Result, error) {
	res := &Result{Columns: make([]string, len(items))}
	for i, it := range items {
		res.Columns[i] = it.Name
	}
	var keyIdx, aggIdx []int
	for i, it := range items {
		if it.Agg.Func == "" {
			keyIdx = append(keyIdx, i)
		} else {
			aggIdx = append(aggIdx, i)
		}
	}

	groups := make(map[string]*accState)
	var order []*accState // first-seen group order
	for _, r := range rows {
		var kb []byte
		keys := make([]any, len(keyIdx))
		for j, i := range keyIdx {
			v := ex.itemValue(items[i], r)
			keys[j] = v
			kb = encodeVal(kb, v)
		}
		ks := string(kb)
		st := groups[ks]
		if st == nil {
			if len(groups) >= maxGroups {
				return nil, &Error{1, 1, fmt.Sprintf("too many groups: aggregation exceeded %d groups; add grouping keys or filters", maxGroups)}
			}
			st = &accState{keys: keys, aggs: make([]*aggAcc, len(aggIdx))}
			groups[ks] = st
			order = append(order, st)
		}
		for j, i := range aggIdx {
			if st.aggs[j] == nil {
				st.aggs[j] = &aggAcc{}
			}
			if err := ex.accum(items[i], st.aggs[j], r); err != nil {
				return nil, err
			}
		}
	}
	// No grouping keys and no input rows: still exactly one output row
	// (count=0, sum=0, avg/min/max=null, collect=[]).
	if len(keyIdx) == 0 && len(order) == 0 {
		st := &accState{aggs: make([]*aggAcc, len(aggIdx))}
		for j := range st.aggs {
			st.aggs[j] = &aggAcc{}
		}
		order = append(order, st)
	}

	projRows := make([]map[string]any, len(order))
	for gi, st := range order {
		m := make(map[string]any, len(items))
		for j, i := range keyIdx {
			m[items[i].Name] = st.keys[j]
		}
		for j, i := range aggIdx {
			m[items[i].Name] = st.aggs[j].result(items[i].Agg)
		}
		projRows[gi] = m
	}
	if len(ret.OrderBy) > 0 {
		keys := ret.OrderBy
		sortVals := make([][]any, len(projRows))
		for i, m := range projRows {
			vals := make([]any, len(keys))
			for j, k := range keys {
				vals[j] = m[k.Name] // post-aggregation rows only have projected columns
			}
			sortVals[i] = vals
		}
		projRows = sortRows(projRows, keys, sortVals)
	}
	projRows = applySkipLimit(projRows, ret.Skip, ret.Limit)
	res.Rows = projRows
	return res, nil
}

// applySkipLimit slices the sorted rows by SKIP then LIMIT (either < 0 =
// absent): rows[skip : skip+limit] with both bounds clamped — SKIP past the
// end yields the empty slice.
func applySkipLimit(rows []map[string]any, skip, limit int) []map[string]any {
	lo := 0
	if skip > 0 {
		lo = min(skip, len(rows))
	}
	hi := len(rows)
	if limit >= 0 && hi-lo > limit {
		hi = lo + limit
	}
	return rows[lo:hi]
}

// accState is one aggregation group: the key values and one accumulator per
// aggregating RETURN item.
type accState struct {
	keys []any
	aggs []*aggAcc
}

// aggAcc accumulates one aggregation call over a group. Null inputs are
// skipped by every function (count(*) counts rows instead of values).
type aggAcc struct {
	n            int     // rows for count(*), non-null inputs otherwise
	sum          float64 // sum/avg
	sumN         int     // non-null numeric inputs (avg divisor)
	minV, maxV   any
	hasMin       bool
	hasMax       bool
	list         []any // collect
	seenDistinct map[string]bool
}

// accum folds one binding row into the accumulator of aggregating item it.
func (ex *executor) accum(it *ReturnItem, ac *aggAcc, r row) error {
	call := it.Agg
	if call.Star { // count(*): every row counts
		ac.n++
		return nil
	}
	v := ex.itemValue(call.Arg, r)
	if v == nil {
		return nil // null inputs never aggregate
	}
	if f, ok := numVal(v); ok {
		v = f // numeric inputs normalize to float64
	}
	if call.Distinct {
		enc := string(encodeVal(nil, v))
		if ac.seenDistinct[enc] {
			return nil
		}
		if ac.seenDistinct == nil {
			ac.seenDistinct = map[string]bool{}
		}
		ac.seenDistinct[enc] = true
	}
	switch call.Func {
	case "count":
		ac.n++
	case "sum", "avg":
		f, ok := v.(float64) // numerics were normalized above
		if !ok {
			return &Error{1, 1, fmt.Sprintf("%s(): non-numeric input %T", call.Func, v)}
		}
		ac.sum += f
		ac.sumN++
	case "min":
		if !ac.hasMin || cmpVals(v, ac.minV) < 0 {
			ac.minV, ac.hasMin = v, true
		}
	case "max":
		if !ac.hasMax || cmpVals(v, ac.maxV) > 0 {
			ac.maxV = v
			ac.hasMax = true
		}
	case "collect":
		ac.list = append(ac.list, v)
	}
	return nil
}

// result finalizes the accumulator: count/sum/avg as float64, min/max as the
// stored scalars, collect as []any.
func (ac *aggAcc) result(call AggCall) any {
	switch call.Func {
	case "count":
		return float64(ac.n)
	case "sum":
		return ac.sum // 0 over an empty/all-null group
	case "avg":
		if ac.sumN == 0 {
			return nil
		}
		return ac.sum / float64(ac.sumN)
	case "min":
		if !ac.hasMin {
			return nil
		}
		return ac.minV
	case "max":
		if !ac.hasMax {
			return nil
		}
		return ac.maxV
	case "collect":
		if ac.list == nil {
			return []any{}
		}
		return ac.list
	}
	return nil
}

// encodeVal appends a canonical, self-delimiting encoding of v (group keys
// and DISTINCT seen-sets). The encoding is injective — distinct values
// encode to distinct byte strings; the tag numbers are arbitrary and
// unrelated to cmpVals' type ranking. Numbers are numVal-normalized IEEE
// bits; null has its own tag.
func encodeVal(b []byte, v any) []byte {
	switch x := v.(type) {
	case nil:
		return append(b, 0)
	case bool:
		if x {
			return append(b, 1, 1)
		}
		return append(b, 1, 0)
	case string:
		b = append(b, 2)
		b = strconv.AppendInt(b, int64(len(x)), 10)
		b = append(b, ' ')
		return append(b, x...)
	case NodeVal:
		b = append(b, 3)
		return binary.BigEndian.AppendUint32(b, x.ID)
	case EdgeVal:
		b = append(b, 4)
		return binary.BigEndian.AppendUint32(b, x.ID)
	}
	if f, ok := numVal(v); ok {
		b = append(b, 5)
		return binary.BigEndian.AppendUint64(b, math.Float64bits(f))
	}
	s := fmt.Sprintf("%#v", v)
	b = append(b, 6)
	b = strconv.AppendInt(b, int64(len(s)), 10)
	b = append(b, ' ')
	return append(b, s...)
}

// sortRows stable-sorts projected rows by precomputed ORDER BY key values
// (sortVals[i][j] belongs to projRows[i] and key j).
func sortRows(projRows []map[string]any, keys []OrderKey, sortVals [][]any) []map[string]any {
	idx := make([]int, len(projRows))
	for i := range idx {
		idx[i] = i
	}
	slices.SortStableFunc(idx, func(a, b int) int {
		for j, k := range keys {
			c := cmpVals(sortVals[a][j], sortVals[b][j])
			if k.Desc {
				c = -c
			}
			if c != 0 {
				return c
			}
		}
		return 0
	})
	sorted := make([]map[string]any, len(projRows))
	for i, ix := range idx {
		sorted[i] = projRows[ix]
	}
	return sorted
}

// starItems expands RETURN * to every named variable in declaration order
// (node, edge, node, edge ... per path).
func starItems(m *MatchClause) []*ReturnItem {
	var items []*ReturnItem
	for _, pat := range m.Patterns {
		for i, n := range pat.Nodes {
			if n.Var != "" {
				items = append(items, &ReturnItem{Var: n.Var, Name: n.Var})
			}
			if i < len(pat.Edges) && pat.Edges[i].Var != "" {
				items = append(items, &ReturnItem{Var: pat.Edges[i].Var, Name: pat.Edges[i].Var})
			}
		}
	}
	return items
}

func (ex *executor) itemValue(it *ReturnItem, r row) any {
	if it.Prop == "" {
		if id, ok := r.nodes[it.Var]; ok {
			rec, _ := ex.nodeInfo(id)
			return NodeVal{ID: id, Key: rec.Key, Label: rec.Label, Props: rec.Props}
		}
		if id, ok := r.edges[it.Var]; ok {
			return ex.edgeVal(id)
		}
		if v, ok := r.vals[it.Var]; ok { // scalar binding (WITH/UNWIND alias): the value itself
			return v
		}
		return nil
	}
	v, ok := ex.scalarOf(r, it.Var, it.Prop)
	if !ok {
		return nil
	}
	return v
}

func (ex *executor) edgeVal(id uint32) EdgeVal {
	src, dst, label, props, _ := ex.edgeInfo(id)
	return EdgeVal{ID: id, Src: src, Dst: dst, Label: label, Props: props}
}

// orderValue evaluates an ORDER BY name ("b" or "b.w") on the binding.
func (ex *executor) orderValue(name string, r row) any {
	if i := strings.IndexByte(name, '.'); i >= 0 {
		v, ok := ex.scalarOf(r, name[:i], name[i+1:])
		if !ok {
			return nil
		}
		return v
	}
	return ex.itemValue(&ReturnItem{Var: name, Name: name}, r)
}

// scalarOf reads var.name: the pseudo-props key/label/id (nodes) and
// label/id (edges), or the node/edge's own property. ok=false when the
// variable is unbound or the property is absent (evaluates to null). A
// scalar binding (vals) carries no properties — property access on one
// evaluates to null; scope validation rejects the spelling anyway, this
// keeps hand-built expression trees honest.
func (ex *executor) scalarOf(r row, v, name string) (any, bool) {
	if id, ok := r.nodes[v]; ok {
		rec, _ := ex.nodeInfo(id)
		switch name {
		case "key":
			return rec.Key, true
		case "label":
			return rec.Label, true
		case "id":
			return float64(id), true
		}
		val, present := rec.Props[name]
		return val, present
	}
	if id, ok := r.edges[v]; ok {
		_, _, label, props, _ := ex.edgeInfo(id)
		switch name {
		case "label":
			return label, true
		case "id":
			return float64(id), true
		}
		val, present := props[name]
		return val, present
	}
	// v may still be a scalar binding (r.vals): scalars carry no properties,
	// so var.name on one takes the same null outcome as an unbound variable
	// (scope validation rejects the spelling in parsed queries).
	return nil, false
}

// eval evaluates a WHERE expression to (value, ok); ok=false is the null
// outcome (absent property, NULL literal, incomparable operands), which
// never satisfies the WHERE and is not rescued by NOT.
func (ex *executor) eval(e Expr, r row) (any, bool) {
	switch x := e.(type) {
	case *Lit:
		if x.Val == nil {
			return nil, false
		}
		return x.Val, true
	case *PropRef:
		return ex.scalarOf(r, x.Var, x.Name)
	case *Not:
		v, ok := ex.eval(x.X, r)
		if !ok {
			return nil, false
		}
		b, isBool := v.(bool)
		if !isBool {
			return nil, false
		}
		return !b, true
	case *ListLit:
		return nil, false // lists only appear as the right side of IN
	case *Binary:
		return ex.evalBinary(x, r)
	}
	return nil, false
}

func (ex *executor) evalBinary(x *Binary, r row) (any, bool) {
	switch x.Op {
	case "AND":
		lb, lok := ex.boolOf(x.L, r)
		rb, rok := ex.boolOf(x.R, r)
		if (lok && !lb) || (rok && !rb) {
			return false, true // F AND x = F
		}
		if lok && rok {
			return lb && rb, true
		}
		return nil, false
	case "OR":
		lb, lok := ex.boolOf(x.L, r)
		rb, rok := ex.boolOf(x.R, r)
		if (lok && lb) || (rok && rb) {
			return true, true // T OR x = T
		}
		if lok && rok {
			return lb || rb, true
		}
		return nil, false
	case "STARTS WITH", "ENDS WITH", "CONTAINS":
		lv, lok := ex.eval(x.L, r)
		rv, rok := ex.eval(x.R, r)
		ls, lIsStr := lv.(string)
		rs, rIsStr := rv.(string)
		if !lok || !rok || !lIsStr || !rIsStr {
			return nil, false
		}
		switch x.Op {
		case "STARTS WITH":
			return strings.HasPrefix(ls, rs), true
		case "ENDS WITH":
			return strings.HasSuffix(ls, rs), true
		default:
			return strings.Contains(ls, rs), true
		}
	case "IN":
		lv, lok := ex.eval(x.L, r)
		if !lok {
			return nil, false
		}
		lst, isList := x.R.(*ListLit)
		if !isList {
			return nil, false
		}
		for _, want := range lst.Vals {
			if litEq(lv, want) {
				return true, true
			}
		}
		return false, true
	}
	return ex.evalCompare(x, r)
}

func (ex *executor) evalCompare(x *Binary, r row) (any, bool) {
	lv, lok := ex.eval(x.L, r)
	rv, rok := ex.eval(x.R, r)
	if !lok || !rok {
		return nil, false
	}
	if lf, ok := numVal(lv); ok {
		if rf, ok2 := numVal(rv); ok2 {
			return cmpOp(x.Op, cmp.Compare(lf, rf)), true
		}
		return typeMismatch(x.Op), false2(x.Op)
	}
	if ls, ok := lv.(string); ok {
		if rs, ok2 := rv.(string); ok2 {
			return cmpOp(x.Op, strings.Compare(ls, rs)), true
		}
		return typeMismatch(x.Op), false2(x.Op)
	}
	if lb, ok := lv.(bool); ok {
		if rb, ok2 := rv.(bool); ok2 {
			c := 0
			if lb != rb {
				c = 1
				if !lb {
					c = -1
				}
			}
			return cmpOp(x.Op, c), true
		}
	}
	return typeMismatch(x.Op), false2(x.Op)
}

// typeMismatch/false2: equality between different types is a definite false
// (1 = 'a' is false); orderings are null.
func typeMismatch(op string) any {
	switch op {
	case "=":
		return false
	case "<>":
		return true
	}
	return nil
}

func false2(op string) bool {
	switch op {
	case "=", "<>":
		return true
	}
	return false
}

func cmpOp(op string, c int) bool {
	switch op {
	case "=":
		return c == 0
	case "<>":
		return c != 0
	case "<":
		return c < 0
	case "<=":
		return c <= 0
	case ">":
		return c > 0
	case ">=":
		return c >= 0
	}
	return false
}

func (ex *executor) boolOf(e Expr, r row) (bool, bool) {
	v, ok := ex.eval(e, r)
	if !ok {
		return false, false
	}
	b, isBool := v.(bool)
	return b, isBool
}

// cmpVals is the ORDER BY total order: null < bool < number < string <
// node/edge (by key/label then ID); numbers compare numerically.
func cmpVals(a, b any) int {
	ra, rb := rankOf(a), rankOf(b)
	if ra != rb {
		return cmp.Compare(ra, rb)
	}
	switch av := a.(type) {
	case nil:
		return 0
	case bool:
		bv := b.(bool)
		if av == bv {
			return 0
		}
		if !av {
			return -1
		}
		return 1
	case string:
		return strings.Compare(av, b.(string))
	case NodeVal:
		bv := b.(NodeVal)
		if c := strings.Compare(av.Key, bv.Key); c != 0 {
			return c
		}
		return cmp.Compare(av.ID, bv.ID)
	case EdgeVal:
		bv := b.(EdgeVal)
		if c := strings.Compare(av.Label, bv.Label); c != 0 {
			return c
		}
		return cmp.Compare(av.ID, bv.ID)
	}
	af, aok := numVal(a)
	bf, bok := numVal(b)
	if aok && bok {
		return cmp.Compare(af, bf)
	}
	return 0
}

func rankOf(v any) int {
	switch v.(type) {
	case nil:
		return 0
	case bool:
		return 1
	case string:
		return 3
	case NodeVal:
		return 4
	case EdgeVal:
		return 5
	}
	if _, ok := numVal(v); ok {
		return 2
	}
	return 6
}
