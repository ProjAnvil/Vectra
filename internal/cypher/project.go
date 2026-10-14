package cypher

import (
	"cmp"
	"slices"
	"strings"
)

// project applies RETURN / ORDER BY / LIMIT to the WHERE-filtered rows.
// ORDER BY keys evaluate var[.prop] on the binding itself, so they may
// reference properties not present in RETURN (Cypher-compatible).
func (ex *executor) project(q *Query, rows []row) (*Result, error) {
	items := q.Return.Items
	if q.Return.Star {
		items = starItems(q.Match)
	}
	res := &Result{Columns: make([]string, len(items))}
	for i, it := range items {
		res.Columns[i] = it.Name
	}
	projRows := make([]map[string]any, len(rows))
	for i, r := range rows {
		m := make(map[string]any, len(items))
		for _, it := range items {
			m[it.Name] = ex.itemValue(it, r)
		}
		projRows[i] = m
	}
	if len(q.Return.OrderBy) > 0 {
		keys := q.Return.OrderBy
		sortVals := make([][]any, len(rows))
		for i, r := range rows {
			vals := make([]any, len(keys))
			for j, k := range keys {
				vals[j] = ex.orderValue(k.Name, r)
			}
			sortVals[i] = vals
		}
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
		projRows = sorted
	}
	if q.Return.Limit >= 0 && len(projRows) > q.Return.Limit {
		projRows = projRows[:q.Return.Limit]
	}
	res.Rows = projRows
	return res, nil
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
			rec, _ := ex.snap.NodeInfo(id)
			return NodeVal{ID: id, Key: rec.Key, Label: rec.Label, Props: rec.Props}
		}
		if id, ok := r.edges[it.Var]; ok {
			return ex.edgeVal(id)
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
	src, dst, label, props, _ := ex.snap.EdgeInfo(id)
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
// variable is unbound or the property is absent (evaluates to null).
func (ex *executor) scalarOf(r row, v, name string) (any, bool) {
	if id, ok := r.nodes[v]; ok {
		rec, _ := ex.snap.NodeInfo(id)
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
		_, _, label, props, _ := ex.snap.EdgeInfo(id)
		switch name {
		case "label":
			return label, true
		case "id":
			return float64(id), true
		}
		val, present := props[name]
		return val, present
	}
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
