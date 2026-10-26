package cypher

import (
	"slices"
	"strings"
	"testing"

	"github.com/projanvil/vectra/internal/engine"
)

// unwindSnap is the UNWIND fixture: three SVC nodes, two carrying list
// properties and one without — the mixed-null shape UNWIND must survive
// (a null source drops that input row without touching the others).
func unwindSnap(t *testing.T) *engine.Snapshot {
	t.Helper()
	nodes := []engine.NodeRec{
		{Key: "one", Label: "SVC", Props: map[string]any{"tags": []any{"a", "b"}}},
		{Key: "two", Label: "SVC", Props: map[string]any{"tags": []any{"c"}}},
		{Key: "none", Label: "SVC"},
	}
	s, err := engine.Seal(nodes, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// floatsOf extracts one column's numeric values (list elements parse as
// float64 literals; stored numerics normalize through numVal).
func floatsOf(t *testing.T, res *Result, name string) []float64 {
	t.Helper()
	out := make([]float64, 0, len(res.Rows))
	for _, r := range res.Rows {
		f, ok := numVal(r[name])
		if !ok {
			t.Fatalf("column %s: non-numeric cell %#v", name, r[name])
		}
		out = append(out, f)
	}
	return out
}

// ---- parser arm ----

// TestUnwindParse pins the UNWIND grammar: expr is a list literal (the
// UNWIND-only operand spelling), a bare variable, or var.prop, and the AS
// alias is mandatory. It replaces T2's `expected RETURN, got "UNWIND"` pin.
func TestUnwindParse(t *testing.T) {
	q, err := Parse(`MATCH (a) UNWIND [1, 2, 3] AS x RETURN x`)
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Clauses) != 3 {
		t.Fatalf("clauses: %d", len(q.Clauses))
	}
	u, ok := q.Clauses[1].(*UnwindClause)
	if !ok {
		t.Fatalf("clause 1: %T", q.Clauses[1])
	}
	lit, ok := u.Expr.(*ListLit)
	if !ok || len(lit.Vals) != 3 || lit.Vals[0] != float64(1) || lit.Vals[2] != float64(3) {
		t.Fatalf("list expr: %+v", u.Expr)
	}
	if u.Alias != "x" {
		t.Fatalf("alias: %q", u.Alias)
	}

	// bare variable (a scalar holding a list — the collect() alias form)
	q, err = Parse(`MATCH (a) WITH collect(a.key) AS ks UNWIND ks AS k RETURN k`)
	if err != nil {
		t.Fatal(err)
	}
	u = q.Clauses[2].(*UnwindClause)
	if v, ok := u.Expr.(*VarRef); !ok || v.Var != "ks" {
		t.Fatalf("var expr: %+v", u.Expr)
	}
	// var.prop (a list-valued property)
	q, err = Parse(`MATCH (a) UNWIND a.tags AS t RETURN t`)
	if err != nil {
		t.Fatal(err)
	}
	u = q.Clauses[1].(*UnwindClause)
	if p, ok := u.Expr.(*PropRef); !ok || p.Var != "a" || p.Name != "tags" {
		t.Fatalf("prop expr: %+v", u.Expr)
	}

	// UNWIND chains (the clause is repeatable, like MATCH)
	if _, err := Parse(`MATCH (a) UNWIND [1] AS x UNWIND x AS y RETURN y`); err != nil {
		t.Fatalf("chained UNWIND: %v", err)
	}

	// alias is mandatory
	withErr(t, `MATCH (a) UNWIND [1, 2] RETURN a`, `expected AS, got "RETURN"`)
	// UNWIND takes no trailer: WHERE belongs to a following WITH
	withErr(t, `MATCH (a) UNWIND [1] AS k WHERE k = 1 RETURN k`, `expected RETURN, got "WHERE"`)
	// operands outside the three spellings are rejected
	withErr(t, `MATCH (a) UNWIND 5 AS x RETURN x`, `expected variable, property reference or list literal, got "5"`)
	withErr(t, `MATCH (a) UNWIND a AS x RETURN x`, "bare variable reference requires a scalar")
	// UNWIND sets sawReset before its own expression is checked: the
	// scope-chain undefined family applies even in a pure-MATCH query
	withErr(t, `MATCH (a) UNWIND q AS k RETURN k`, `variable "q" is not defined in this scope`)
	// parseListLit is reused verbatim: the empty list literal is not in the
	// grammar (same as IN today); empty sources come from bindings
	withErr(t, `MATCH (a) UNWIND [] AS x RETURN x`, "expected literal")
}

// TestUnwindAliasShadow pins the alias-shadowing rule: UNWIND binds into
// vals, but itemValue reads nodes/edges first, so an alias reusing a
// node/edge name would return the stale entity — validate rejects it
// (Neo4j also rejects the rebind). Rebinding a SCALAR is the legal case:
// vals overwrite vals, so the self-rebind UNWIND ks AS ks is exact.
func TestUnwindAliasShadow(t *testing.T) {
	// node shadow — the I1 repro: without the guard, RETURN a yields the
	// node twice instead of the list elements
	withErr(t, `MATCH (a) UNWIND [1, 2] AS a RETURN a`,
		`variable "a" is already declared as a node or edge; the UNWIND alias must be a fresh name`)
	// edge shadow
	withErr(t, `MATCH (a)-[e:CALLS]->(b) UNWIND [1] AS e RETURN e`,
		`variable "e" is already declared as a node or edge; the UNWIND alias must be a fresh name`)
	// OPTIONAL-declared nodes are still nodes: the shadow rule fires
	// regardless of nullability
	withErr(t, `MATCH (a) OPTIONAL MATCH (a)-[:KNOWS]->(b) UNWIND [1] AS b RETURN b`,
		`variable "b" is already declared as a node or edge; the UNWIND alias must be a fresh name`)
	// a node renamed by a bare WITH item keeps its kind under the alias,
	// so the alias is shadow-protected too
	withErr(t, `MATCH (a) WITH a AS x UNWIND [1] AS x RETURN x`,
		`variable "x" is already declared as a node or edge; the UNWIND alias must be a fresh name`)

	// scalar self-rebind stays legal and is exact end-to-end: each output
	// row's ks is one list element, not the whole list
	res := run(t, `MATCH (n) WITH collect(n.key) AS ks UNWIND ks AS ks RETURN ks`)
	if k := keysOf(res, "ks"); !slices.Equal(k, []string{"gw", "order", "pay", "store"}) {
		t.Fatalf("scalar self-rebind: %v", k)
	}
	// rebinding a scalar under a fresh name (the normal idiom) is untouched
	res = run(t, `MATCH (n) WITH collect(n.key) AS ks UNWIND ks AS k RETURN k`)
	if k := keysOf(res, "k"); !slices.Equal(k, []string{"gw", "order", "pay", "store"}) {
		t.Fatalf("scalar rebind: %v", k)
	}
}

// ---- sources ----

// TestUnwindLiteral: a list literal fans each input row out to one output
// row per element, in element order, input row after input row.
func TestUnwindLiteral(t *testing.T) {
	res := run(t, `MATCH (a {key:'gw'}) UNWIND [1, 2, 3] AS x RETURN x`)
	if f := floatsOf(t, res, "x"); !slices.Equal(f, []float64{1, 2, 3}) {
		t.Fatalf("single row fan-out: %v", f)
	}
	// three input rows × two elements each, row-major
	res = run(t, `MATCH (a:Service) UNWIND [1, 2] AS x RETURN x`)
	if f := floatsOf(t, res, "x"); !slices.Equal(f, []float64{1, 2, 1, 2, 1, 2}) {
		t.Fatalf("per-row fan-out: %v", f)
	}
	// UNWIND with no MATCH-prefixed variables: the input bindings survive
	res = run(t, `MATCH (a:Service) UNWIND [1] AS x RETURN a.key, x`)
	if k := keysOf(res, "a.key"); !sameSet(k, "gw", "order", "pay") {
		t.Fatalf("bindings survive UNWIND: %v", k)
	}
}

// TestUnwindVarCollect: the killer idiom — collect() builds the list, the
// alias holds it, UNWIND fans it back out (no implicit DISTINCT).
func TestUnwindVarCollect(t *testing.T) {
	res := run(t, `MATCH (n) WITH collect(n.key) AS ks UNWIND ks AS k RETURN k`)
	if k := keysOf(res, "k"); !slices.Equal(k, []string{"gw", "order", "pay", "store"}) {
		t.Fatalf("killer idiom: %v", k)
	}
	res = run(t, `MATCH (n:Service) WITH collect(n.env) AS es UNWIND es AS e RETURN e`)
	if k := keysOf(res, "e"); !slices.Equal(k, []string{"prod", "prod", "dev"}) {
		t.Fatalf("no implicit distinct: %v", k)
	}
}

// TestUnwindProp: a list-valued property as the source; the node without
// the property (null) drops its row while the others keep fanning out.
func TestUnwindProp(t *testing.T) {
	s := unwindSnap(t)
	res := runOn(t, s, `MATCH (n) UNWIND n.tags AS t RETURN t`)
	if k := keysOf(res, "t"); !slices.Equal(k, []string{"a", "b", "c"}) {
		t.Fatalf("prop source with mixed null: %v", k)
	}
}

// ---- null / empty / non-list ----

// TestUnwindNullEmptyDrop: null and empty-list sources drop their input
// rows; an all-null pipeline yields zero rows without error.
func TestUnwindNullEmptyDrop(t *testing.T) {
	// null vals binding (a.missing is absent on every row)
	if res := run(t, `MATCH (a:Service) WITH a, a.missing AS m UNWIND m AS x RETURN x`); len(res.Rows) != 0 {
		t.Fatalf("null binding must drop every row: %d rows", len(res.Rows))
	}
	// empty list from an empty collect: the single agg row is dropped
	s := unwindSnap(t)
	if res := runOn(t, s, `MATCH (n {key:'nope'}) WITH collect(n.tags) AS ts UNWIND ts AS t RETURN t`); len(res.Rows) != 0 {
		t.Fatalf("empty list must drop the row: %d rows", len(res.Rows))
	}
	// strict empty stage: zero input rows stay zero through UNWIND and a
	// later MATCH never re-anchors
	if res := run(t, `MATCH (a {nosuch:1}) UNWIND [1] AS x RETURN x`); len(res.Rows) != 0 {
		t.Fatalf("empty UNWIND: %d rows", len(res.Rows))
	}
	if res := run(t, `MATCH (a {nosuch:1}) UNWIND [1] AS x MATCH (b) RETURN b.key`); len(res.Rows) != 0 {
		t.Fatalf("empty UNWIND into MATCH must stay empty: %d rows", len(res.Rows))
	}
}

// TestUnwindNonListError: a non-list non-null source is a positioned
// *Error naming the offending type.
func TestUnwindNonListError(t *testing.T) {
	for _, c := range []struct{ q, want string }{
		{`MATCH (a:Service) UNWIND a.env AS e RETURN e`, "UNWIND requires a list, got string"},
		// stored props keep their dynamic type in the message (w is an int
		// in the execSnap fixture; parsed literals are float64)
		{`MATCH (a:Service) WITH a, a.w AS w UNWIND w AS x RETURN x`, "UNWIND requires a list, got int"},
	} {
		res, err := Run(execSnap(t), c.q)
		if err == nil {
			t.Fatalf("Run(%q): expected error %q, got rows %+v", c.q, c.want, res.Rows)
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Fatalf("Run(%q): got %q, want substring %q", c.q, err.Error(), c.want)
		}
		e, ok := err.(*Error)
		if !ok {
			t.Fatalf("Run(%q): error must be *Error, got %T", c.q, err)
		}
		if e.Line == 0 || e.Col == 0 {
			t.Fatalf("Run(%q): error must carry the expression position, got %d:%d", c.q, e.Line, e.Col)
		}
	}
}

// ---- one flattening level ----

// TestUnwindOneLevelFlatten: exactly one level — an element that is itself
// a list binds as a list value, and a chained UNWIND is what flattens it
// further. (Nested list literals are not in the grammar; the shape comes
// from collect() over list-valued properties.)
func TestUnwindOneLevelFlatten(t *testing.T) {
	s := unwindSnap(t)
	res := runOn(t, s, `MATCH (n) WITH collect(n.tags) AS ts UNWIND ts AS t RETURN t`)
	if len(res.Rows) != 2 {
		t.Fatalf("one row per list element: %d rows %+v", len(res.Rows), res.Rows)
	}
	first, ok := res.Rows[0]["t"].([]any)
	if !ok || len(first) != 2 || first[0] != "a" || first[1] != "b" {
		t.Fatalf("nested element must bind as a list value: %#v", res.Rows[0]["t"])
	}
	if second := res.Rows[1]["t"].([]any); len(second) != 1 || second[0] != "c" {
		t.Fatalf("second element: %#v", res.Rows[1]["t"])
	}
	// a second UNWIND flattens exactly one more level
	res = runOn(t, s, `MATCH (n) WITH collect(n.tags) AS ts UNWIND ts AS t UNWIND t AS x RETURN x`)
	if k := keysOf(res, "x"); !slices.Equal(k, []string{"a", "b", "c"}) {
		t.Fatalf("chained flatten: %v", k)
	}
}

// TestUnwindEntityElement: elements bind as scalars — an entity element
// binds as its NodeVal value and renders through RETURN's vals arm. (The
// alias is typed scalar at validation, so entity spelling downstream —
// n2.key, MATCH (n2) — is a scope error; this pins the value side.)
func TestUnwindEntityElement(t *testing.T) {
	res := run(t, `MATCH (n) WITH collect(n) AS ns UNWIND ns AS n2 RETURN n2`)
	if len(res.Rows) != 4 {
		t.Fatalf("entity elements: %d rows", len(res.Rows))
	}
	for i, want := range []string{"gw", "order", "pay", "store"} {
		nv, ok := res.Rows[i]["n2"].(NodeVal)
		if !ok || nv.Key != want {
			t.Fatalf("row %d: got %#v, want node %q", i, res.Rows[i]["n2"], want)
		}
	}
}

// ---- DISTINCT after UNWIND ----

// TestUnwindDistinct: WITH DISTINCT over UNWIND output collapses duplicate
// elements. The load-bearing case is the stale-pos discriminator: the two
// 2-hop rows share every binding UNWIND keeps (a, b) and differ only in
// the frontier position the anonymous c left behind — the same element
// reached from rows with different frontier pos must collapse.
func TestUnwindDistinct(t *testing.T) {
	s := withSnap(t)
	if res := runOn(t, s, `MATCH (a {key:'0'})-[:CALLS]->(b)-[:CALLS]->(c) UNWIND ['x'] AS k WITH DISTINCT k RETURN k`); len(res.Rows) != 1 || res.Rows[0]["k"] != "x" {
		t.Fatalf("stale-pos discriminator: %+v", res.Rows)
	}
	// without DISTINCT the row multiplicity survives
	if res := runOn(t, s, `MATCH (a {key:'0'})-[:CALLS]->(b)-[:CALLS]->(c) UNWIND ['x'] AS k WITH k RETURN k`); len(res.Rows) != 2 {
		t.Fatalf("no DISTINCT keeps both rows: %d rows", len(res.Rows))
	}
	// duplicate elements within one list collapse too
	if res := runOn(t, s, `MATCH (a {key:'0'}) UNWIND ['x', 'x'] AS k WITH DISTINCT k RETURN k`); len(res.Rows) != 1 {
		t.Fatalf("duplicate elements: %d rows", len(res.Rows))
	}
	// distinct elements stay distinct
	if res := runOn(t, s, `MATCH (a {key:'0'}) UNWIND ['x', 'y'] AS k WITH DISTINCT k RETURN k`); len(res.Rows) != 2 {
		t.Fatalf("distinct elements: %d rows", len(res.Rows))
	}
}

// ---- maxRows ----

// TestUnwindMaxRows: the fan-out is capped by maxRows with the standard
// result-too-large error; the cap is inclusive (maxRows rows are fine).
func TestUnwindMaxRows(t *testing.T) {
	saved := maxRows
	t.Cleanup(func() { maxRows = saved })
	maxRows = 2
	_, err := Run(execSnap(t), `MATCH (a {key:'gw'}) UNWIND [1, 2, 3] AS x RETURN x`)
	if err == nil || !strings.Contains(err.Error(), "result too large: more than 2 intermediate rows; narrow the pattern or add LIMIT") {
		t.Fatalf("maxRows exceeded: got %v", err)
	}
	maxRows = 3
	if res := run(t, `MATCH (a {key:'gw'}) UNWIND [1, 2, 3] AS x RETURN x`); len(res.Rows) != 3 {
		t.Fatalf("at the cap: %d rows", len(res.Rows))
	}
}

// ---- scope: parse-path variants of the T2 pins ----

// TestUnwindScopeParsePath: the prior scope SURVIVES UNWIND (only WITH
// resets it), the alias is a scalar, and the UNWIND alias feeds the
// sawReset error families.
func TestUnwindScopeParsePath(t *testing.T) {
	// survival: b declared before UNWIND is still visible after it
	if res := run(t, `MATCH (a {key:'gw'}), (b:Store) UNWIND [1] AS k RETURN b.key`); len(res.Rows) != 1 || res.Rows[0]["b.key"] != "store" {
		t.Fatalf("scope survives UNWIND: %+v", res.Rows)
	}
	// WITH-dropped variables stay dropped through UNWIND
	withErr(t, `MATCH (a), (b) WITH a UNWIND [1] AS k RETURN b`, `variable "b" is not defined in this scope`)
	// the UNWIND alias is a scalar: no property access on it
	withErr(t, `MATCH (a) UNWIND [1] AS k RETURN k.x`, `variable "k" is a scalar; property access requires a node or edge`)
	withErr(t, `MATCH (a) UNWIND [1] AS k WITH k WHERE k.x = 1 RETURN k`, `variable "k" is a scalar; property access requires a node or edge`)
	// a scalar alias cannot be rebound by a pattern
	withErr(t, `MATCH (a) UNWIND [1] AS k MATCH (k) RETURN k`, `variable "k" is a scalar; pattern variables must be nodes or edges`)
	// property access on a scalar projection feeding UNWIND (rule-8 family)
	withErr(t, `MATCH (a:Service) WITH a.w AS w UNWIND w.x AS y RETURN y`, `variable "w" is a scalar; property access requires a node or edge`)
	// RETURN * after UNWIND: the T2-pinned rejection, parse path
	withErr(t, `MATCH (a) UNWIND [1] AS k RETURN *`, "RETURN * is not supported after WITH/UNWIND or multiple MATCH clauses")
}
