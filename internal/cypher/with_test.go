package cypher

import (
	"slices"
	"strings"
	"testing"

	"github.com/projanvil/vectra/internal/engine"
)

// withSnap is the WITH-clause fixture: a CALLS fork chain
//
//	'0' --CALLS--> '1' --CALLS--> '2'
//	                └--CALLS-----> '3'
//
// The 2-hop pattern (a {key:'0'})-[:CALLS]->(b)-[:CALLS]->(c) yields exactly
// two rows sharing a and b and differing only in c — rows that are identical
// in every binding WITH could project once c is dropped. They are the WITH
// DISTINCT discriminator: they must collapse when (and only when) the
// stage-constructed rows carry the normalized pos instead of the stale
// frontier position c left behind.
func withSnap(t *testing.T) *engine.Snapshot {
	t.Helper()
	nodes := []engine.NodeRec{
		{Key: "0", Label: "SVC"},
		{Key: "1", Label: "SVC"},
		{Key: "2", Label: "SVC"},
		{Key: "3", Label: "SVC"},
	}
	edges := []engine.EdgeRec{
		{Src: 0, Dst: 1, LabelID: 0},
		{Src: 1, Dst: 2, LabelID: 0},
		{Src: 1, Dst: 3, LabelID: 0},
	}
	s, err := engine.Seal(nodes, edges, []string{"CALLS"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// runOn runs q against an explicit snapshot (run() is welded to execSnap).
func runOn(t *testing.T, snap *engine.Snapshot, q string) *Result {
	t.Helper()
	res, err := Run(snap, q)
	if err != nil {
		t.Fatalf("Run(%q): %v", q, err)
	}
	return res
}

// withErr pins a query error (parse or validate path) through Run.
func withErr(t *testing.T, q, want string) {
	t.Helper()
	_, err := Run(execSnap(t), q)
	if err == nil {
		t.Fatalf("Run(%q): expected error %q, got nil", q, want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("Run(%q): got %q, want substring %q", q, err.Error(), want)
	}
	if _, ok := err.(*Error); !ok {
		t.Fatalf("Run(%q): error must be *Error, got %T", q, err)
	}
}

// ---- parser arm ----

// TestWithParse pins the WITH grammar: [DISTINCT] items, ORDER BY/SKIP/LIMIT
// trailer, trailing WHERE — and the absent-field defaults.
func TestWithParse(t *testing.T) {
	q, err := Parse(`MATCH (a) WITH DISTINCT a AS x, a.w AS k ORDER BY k DESC SKIP 1 LIMIT 2 WHERE k = 1 RETURN x, k`)
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Clauses) != 3 {
		t.Fatalf("clauses: %d", len(q.Clauses))
	}
	w, ok := q.Clauses[1].(*WithClause)
	if !ok {
		t.Fatalf("clause 1: %T", q.Clauses[1])
	}
	if !w.Distinct || len(w.Items) != 2 || w.Items[0].Alias != "x" || w.Items[1].Alias != "k" {
		t.Fatalf("distinct/items: distinct=%v items=%+v", w.Distinct, w.Items)
	}
	if len(w.OrderBy) != 1 || w.OrderBy[0].Name != "k" || !w.OrderBy[0].Desc {
		t.Fatalf("order: %+v", w.OrderBy)
	}
	if w.Skip != 1 || w.Limit != 2 {
		t.Fatalf("skip/limit: %d/%d", w.Skip, w.Limit)
	}
	b, isBin := w.Where.(*Binary)
	if !isBin || b.Op != "=" {
		t.Fatalf("where: %+v", w.Where)
	}
	if _, isRef := b.L.(*VarRef); !isRef || b.L.(*VarRef).Var != "k" {
		t.Fatalf("where lhs must be the bare scalar reference k: %+v", b.L)
	}

	// defaults: no DISTINCT, no trailer, no WHERE
	q, err = Parse(`MATCH (a) WITH a RETURN a`)
	if err != nil {
		t.Fatal(err)
	}
	w = q.Clauses[1].(*WithClause)
	if w.Distinct || w.Skip != -1 || w.Limit != -1 || w.Where != nil || len(w.OrderBy) != 0 {
		t.Fatalf("defaults: %+v", w)
	}
	// trailer pieces are independently optional
	if _, err := Parse(`MATCH (a) WITH a SKIP 1 RETURN a`); err != nil {
		t.Fatalf("skip-only: %v", err)
	}
	if _, err := Parse(`MATCH (a) WITH a LIMIT 2 RETURN a`); err != nil {
		t.Fatalf("limit-only: %v", err)
	}
	// LIMIT must follow SKIP (the RETURN trailer contract); WITH is not
	// terminal, so a trailing SKIP lands on the clause loop's error
	withErr(t, `MATCH (a) WITH a LIMIT 1 SKIP 2 RETURN a`, `expected RETURN, got "SKIP"`)
	// WITH * has no star form
	withErr(t, `MATCH (a) WITH * RETURN a`, "expected variable")
}

// ---- non-aggregating projection ----

// TestWithPassthrough: entity pass-through (re-seeded by ID), scalar alias
// into vals, mixed items — one output row per input row, input order
// preserved (pinned against the no-WITH twin query).
func TestWithPassthrough(t *testing.T) {
	s := execSnap(t)
	res := runOn(t, s, `MATCH (a {type:'controller'})-[:CALLS]->(b) WITH a, b.key AS k RETURN k`)
	if len(res.Rows) != 2 {
		t.Fatalf("rows: %d %+v", len(res.Rows), res.Rows)
	}
	if len(res.Columns) != 1 || res.Columns[0] != "k" {
		t.Fatalf("columns: %v", res.Columns)
	}
	twin := runOn(t, s, `MATCH (a {type:'controller'})-[:CALLS]->(b) RETURN b.key`)
	if !slices.Equal(keysOf(res, "k"), keysOf(twin, "b.key")) {
		t.Fatalf("WITH must preserve input row order: %v vs %v", keysOf(res, "k"), keysOf(twin, "b.key"))
	}

	// mixed entity + scalar items keep node identity AND bind vals
	res = runOn(t, s, `MATCH (a {type:'controller'})-[:CALLS]->(b) WITH a, b, b.key AS k RETURN a.key, b.key, k`)
	if got := rowSet(renderRows(res)); len(got) != 2 {
		t.Fatalf("mixed rows: %v", got)
	}
	for _, r := range res.Rows {
		if r["a.key"] != "gw" || r["k"] != r["b.key"] {
			t.Fatalf("mixed row: %+v", r)
		}
	}

	// alias renaming of an entity keeps it a node binding
	res = runOn(t, s, `MATCH (a) WITH a AS x RETURN x.key`)
	if k := keysOf(res, "x.key"); !sameSet(k, "gw", "order", "pay", "store") {
		t.Fatalf("renamed entity: %v", k)
	}

	// edge pass-through keeps edge identity (parallel edges stay distinct)
	res = runOn(t, s, `MATCH (a {key:'order'})-[e:CALLS]->(b) WITH e RETURN e`)
	if len(res.Rows) != 2 {
		t.Fatalf("edge rows: %d", len(res.Rows))
	}
	ids := map[any]bool{}
	for _, r := range res.Rows {
		ev, ok := r["e"].(EdgeVal)
		if !ok {
			t.Fatalf("edge value: %+v", r["e"])
		}
		ids[ev.ID] = true
	}
	if len(ids) != 2 {
		t.Fatalf("parallel edges must stay distinct: %v", ids)
	}

	// scalar-only projection duplicates survive (no implicit DISTINCT)
	res = runOn(t, s, `MATCH (a:Service) WITH a.env AS e RETURN e`)
	if len(res.Rows) != 3 {
		t.Fatalf("no implicit distinct: %d rows", len(res.Rows))
	}
}

// TestWithWhereAfterWith: WHERE over the projected rows — bare-ident
// operands read the vals binding, entity props read the re-seeded binding,
// and the filter is three-valued (null never satisfies; NOT does not
// rescue). WHERE runs AFTER the trailer.
func TestWithWhereAfterWith(t *testing.T) {
	res := run(t, `MATCH (a:Service) WITH a, a.w AS w WHERE w > 3 RETURN a.key`)
	if k := keysOf(res, "a.key"); !sameSet(k, "gw", "pay") {
		t.Fatalf("bare-ident operand: %v", k)
	}
	// entity property through the re-seeded binding
	res = run(t, `MATCH (a:Service) WITH a WHERE a.env = 'prod' RETURN a.key`)
	if k := keysOf(res, "a.key"); !sameSet(k, "gw", "order") {
		t.Fatalf("entity prop after WITH: %v", k)
	}
	// bare scalar in a later MATCH's WHERE (bound by the earlier WITH)
	res = run(t, `MATCH (a:Service) WITH a, a.w AS w MATCH (b:Service) WHERE w > 6 RETURN a.key, b.key`)
	if got := rowSet(renderRows(res)); len(got) != 3 {
		t.Fatalf("scalar in MATCH WHERE: %v", got) // w=7 (pay) × 3 services
	}
	// null semantics: m is null on every row
	if res := run(t, `MATCH (a:Service) WITH a, a.missing AS m WHERE m = 1 RETURN a.key`); len(res.Rows) != 0 {
		t.Fatalf("null comparison must not satisfy WHERE: %d rows", len(res.Rows))
	}
	if res := run(t, `MATCH (a:Service) WITH a, a.missing AS m WHERE NOT m = 1 RETURN a.key`); len(res.Rows) != 0 {
		t.Fatalf("NOT must not rescue null: %d rows", len(res.Rows))
	}
	// WHERE applies after sort → skip → limit
	res = run(t, `MATCH (a:Service) WITH a, a.w AS w ORDER BY w DESC LIMIT 2 WHERE w > 4 RETURN a.key`)
	if k := keysOf(res, "a.key"); !slices.Equal(k, []string{"pay", "gw"}) {
		t.Fatalf("WHERE after trailer: %v", k)
	}
}

// TestWithTrailerOrderSkipLimit: the WITH trailer is sort → skip → limit.
// The SKIP-without-sorted-input case discriminates the order: sorting first
// drops pay(7), leaving [order, gw]; skipping first would drop gw and sort
// the rest to [pay, order].
func TestWithTrailerOrderSkipLimit(t *testing.T) {
	res := run(t, `MATCH (a:Service) WITH a.key AS k ORDER BY k DESC SKIP 1 RETURN k`)
	if k := keysOf(res, "k"); !slices.Equal(k, []string{"order", "gw"}) {
		t.Fatalf("sort before skip: %v", k)
	}
	res = run(t, `MATCH (a:Service) WITH a.key AS k ORDER BY k DESC SKIP 1 LIMIT 1 RETURN k`)
	if k := keysOf(res, "k"); !slices.Equal(k, []string{"order"}) {
		t.Fatalf("skip+limit: %v", k)
	}
	if res := run(t, `MATCH (a:Service) WITH a.key AS k SKIP 5 RETURN k`); len(res.Rows) != 0 {
		t.Fatalf("skip past end: %d rows", len(res.Rows))
	}
	res = run(t, `MATCH (a:Service) WITH a.key AS k LIMIT 2 RETURN k`)
	if k := keysOf(res, "k"); !slices.Equal(k, []string{"gw", "order"}) {
		t.Fatalf("limit keeps natural order: %v", k)
	}
	// ORDER BY over a projected ENTITY's property (binding re-seeded)
	res = run(t, `MATCH (a:Service) WITH a ORDER BY a.w ASC RETURN a.key`)
	if k := keysOf(res, "a.key"); !slices.Equal(k, []string{"order", "gw", "pay"}) {
		t.Fatalf("order by entity prop: %v", k)
	}
}

// ---- DISTINCT ----

// TestWithDistinct: WITH DISTINCT dedups the projected rows via the
// vals-aware row identity. The multi-hop case is the load-bearing one: two
// rows identical in every binding WITH projects but differing in the stale
// frontier pos MUST collapse — pos normalization at stage construction is
// what makes the identity see them as equal.
func TestWithDistinct(t *testing.T) {
	s := withSnap(t)
	if res := runOn(t, s, `MATCH (a {key:'0'})-[:CALLS]->(b)-[:CALLS]->(c) WITH DISTINCT b RETURN b`); len(res.Rows) != 1 {
		t.Fatalf("DISTINCT must collapse rows differing only in stale pos: %d rows %+v", len(res.Rows), res.Rows)
	}
	// without DISTINCT the two rows survive (row semantics)
	if res := runOn(t, s, `MATCH (a {key:'0'})-[:CALLS]->(b)-[:CALLS]->(c) WITH b RETURN b`); len(res.Rows) != 2 {
		t.Fatalf("no DISTINCT must keep both rows: %d", len(res.Rows))
	}
	// a differing projected binding keeps both rows
	if res := runOn(t, s, `MATCH (a {key:'0'})-[:CALLS]->(b)-[:CALLS]->(c) WITH DISTINCT b, c RETURN b.key, c.key`); len(res.Rows) != 2 {
		t.Fatalf("distinct entity pairs must both survive: %d", len(res.Rows))
	}
	// entity + scalar mixed identity: equal pairs collapse (parallel edges)
	if res := run(t, `MATCH (a {key:'order'})-[e:CALLS]->(b) WITH DISTINCT b, b.key AS k RETURN k`); len(res.Rows) != 1 {
		t.Fatalf("mixed entity+scalar identity: %d rows", len(res.Rows))
	}
	// scalar-only distinct (undirected so both labels occur as b)
	if res := run(t, `MATCH (a)-[:CALLS]-(b) WITH DISTINCT b.label AS l RETURN l`); !sameSet(keysOf(res, "l"), "Service", "Store") {
		t.Fatalf("scalar distinct: %v", keysOf(res, "l"))
	}
}

// ---- aggregating WITH ----

// TestWithAggregating: group keys = non-agg items (entity keys re-seed on
// output), one pass accumulation, first-seen group order, and the
// empty-input rules carried over from RETURN aggregation.
func TestWithAggregating(t *testing.T) {
	// grouping by a projected scalar
	res := run(t, `MATCH (a:Service) WITH a.env AS e, count(*) AS c RETURN e, c`)
	if len(res.Rows) != 2 {
		t.Fatalf("groups: %d %+v", len(res.Rows), res.Rows)
	}
	// first-seen order over the scan order gw(prod), order(prod), pay(dev)
	if res.Rows[0]["e"] != "prod" || res.Rows[0]["c"] != float64(2) ||
		res.Rows[1]["e"] != "dev" || res.Rows[1]["c"] != float64(1) {
		t.Fatalf("group rows: %+v", res.Rows)
	}
	// multi-key grouping
	res = run(t, `MATCH (a)-[:USES]->(b) WITH a.key AS ak, b.key AS bk, count(*) AS c RETURN ak, bk, c`)
	if got := rowSet(renderRows(res)); !slices.Equal(got, []string{
		"ak=order bk=store c=1 ", "ak=pay bk=store c=1 ",
	}) {
		t.Fatalf("multi-key: %v", got)
	}
	// entity group key re-seeds the binding for RETURN
	res = run(t, `MATCH (a {type:'controller'})-[:CALLS]->(b) WITH a, count(*) AS c RETURN a.key, c`)
	if len(res.Rows) != 1 || res.Rows[0]["a.key"] != "gw" || res.Rows[0]["c"] != float64(2) {
		t.Fatalf("entity key: %+v", res.Rows)
	}
	// collect
	res = run(t, `MATCH (a:Service) WITH a.env AS e, collect(a.w) AS ws RETURN e, ws`)
	if len(res.Rows) != 2 {
		t.Fatalf("collect groups: %d", len(res.Rows))
	}
	if !slices.Equal(res.Rows[0]["ws"].([]any), []any{float64(5), float64(3)}) ||
		!slices.Equal(res.Rows[1]["ws"].([]any), []any{float64(7)}) {
		t.Fatalf("collect lists: %+v", res.Rows)
	}
	// aggregation over scalars projected by an earlier WITH
	res = run(t, `MATCH (a:Service) WITH a.env AS e, count(*) AS c RETURN sum(c) AS total`)
	if len(res.Rows) != 1 || res.Rows[0]["total"] != float64(3) {
		t.Fatalf("sum over WITH scalars: %+v", res.Rows)
	}
	// trailer over aggregated output (ORDER BY the agg alias)
	res = run(t, `MATCH (a:Service) WITH a.env AS e, count(*) AS c ORDER BY c DESC, e ASC RETURN e`)
	if k := keysOf(res, "e"); !slices.Equal(k, []string{"prod", "dev"}) {
		t.Fatalf("agg trailer: %v", k)
	}
	// empty input, no group keys: the single zero row carries over
	if res := run(t, `MATCH (a {nosuch:1}) WITH count(*) AS c RETURN c`); len(res.Rows) != 1 || res.Rows[0]["c"] != float64(0) {
		t.Fatalf("empty-input one-row: %+v", res.Rows)
	}
	if res := run(t, `MATCH (a {nosuch:1}) WITH collect(a.key) AS ws RETURN ws`); len(res.Rows) != 1 || len(res.Rows[0]["ws"].([]any)) != 0 {
		t.Fatalf("empty collect: %+v", res.Rows)
	}
	// empty input WITH group keys: zero rows
	if res := run(t, `MATCH (a {nosuch:1}) WITH a.key AS k, count(*) AS c RETURN k, c`); len(res.Rows) != 0 {
		t.Fatalf("empty keyed groups: %d rows", len(res.Rows))
	}
	// DISTINCT on aggregating WITH is a no-op (groups are already unique)
	if res := run(t, `MATCH (a:Service) WITH DISTINCT a.env AS e, count(*) AS c RETURN e, c`); len(res.Rows) != 2 {
		t.Fatalf("distinct agg: %d rows", len(res.Rows))
	}
}

// ---- entity re-seed through MATCH ----

// TestWithEntityReseed: a projected node keeps identity for a later MATCH's
// join — through the existing matchRows(c, rows) threading — under its own
// name or an alias; unprojected variables are gone.
func TestWithEntityReseed(t *testing.T) {
	s := withSnap(t)
	if res := runOn(t, s, `MATCH (a {key:'0'})-[:CALLS]->(b) WITH a, b.key AS k MATCH (a)-[:CALLS]->(c) RETURN c.key`); !slices.Equal(keysOf(res, "c.key"), []string{"1"}) {
		t.Fatalf("join through WITH: %v", keysOf(res, "c.key"))
	}
	// renamed entity joins under the alias
	if res := runOn(t, s, `MATCH (a {key:'0'})-[:CALLS]->(b) WITH a AS x MATCH (x)-[:CALLS]->(c) RETURN c.key`); !slices.Equal(keysOf(res, "c.key"), []string{"1"}) {
		t.Fatalf("renamed join: %v", keysOf(res, "c.key"))
	}
	// execSnap shape: b passed through and joined by the second MATCH
	res := run(t, `MATCH (a {type:'controller'})-[:CALLS]->(b) WITH a, b MATCH (b)-[:USES]->(c) RETURN a.key, c.key`)
	if got := rowSet(renderRows(res)); !slices.Equal(got, []string{
		"a.key=gw c.key=store ", "a.key=gw c.key=store ",
	}) {
		t.Fatalf("b join through WITH: %v", got)
	}
	// unprojected vars are gone: referencing b after WITH (without
	// redeclaring it in a pattern) is a scope error — a fresh MATCH (b)
	// would re-DECLARE b as a new pattern variable (a cartesian anchor)
	res = runOn(t, s, `MATCH (a {key:'0'})-[:CALLS]->(b) WITH a MATCH (b)-[:CALLS]->(c) RETURN c.key`)
	if len(res.Rows) == 0 {
		t.Fatalf("redeclared b must re-anchor (cartesian): %d rows", len(res.Rows))
	}
	withErr(t, `MATCH (a {key:'0'})-[:CALLS]->(b) WITH a MATCH (x)-[:CALLS]->(c) WHERE b.key = '1' RETURN c.key`,
		`variable "b" is not defined in this scope`)
}

// ---- scope: parse-path variants of the T2 pins ----

// TestWithScopeSurvivalParsePath replaces T2's unexpected-token pin for the
// WITH keyword: the parser arm exists, so the scope rules are exercised via
// real query strings.
func TestWithScopeSurvivalParsePath(t *testing.T) {
	// alias-required (expressions and aggregations must be aliased)
	withErr(t, `MATCH (a) WITH a.w RETURN a`, "expression in WITH must be aliased (use AS)")
	withErr(t, `MATCH (a) WITH count(*) RETURN a`, "expression in WITH must be aliased (use AS)")
	// survival set: only projected names live past WITH
	withErr(t, `MATCH (a), (b) WITH a RETURN b`, `variable "b" is not defined in this scope`)
	withErr(t, `MATCH (a), (b) WITH a WHERE b.w = 1 RETURN a`, `variable "b" is not defined in this scope`)
	withErr(t, `MATCH (a), (b) WITH a ORDER BY b.w RETURN a`, `ORDER BY variable "b" is not defined in this scope`)
	// dropped from a later clause entirely (x was never projected)
	withErr(t, `MATCH (a), (x) WITH a MATCH (b) WHERE b.missing = 1 AND x.w > 2 RETURN b`, `variable "x" is not defined in this scope`)
	// aliased bare var: the old name is gone
	withErr(t, `MATCH (a) WITH a AS x RETURN a`, `variable "a" is not defined in this scope`)
	// scalar alias usable downstream (stored prop is an int in this fixture)
	if res := run(t, `MATCH (a {key:'gw'}) WITH a.w AS w RETURN w`); len(res.Rows) != 1 {
		t.Fatalf("scalar alias downstream: %+v", res.Rows)
	} else if v, ok := numVal(res.Rows[0]["w"]); !ok || v != 5 {
		t.Fatalf("scalar alias value: %+v", res.Rows[0])
	}
	// RETURN * after WITH/multi-MATCH: the T2-pinned rejection, parse path
	withErr(t, `MATCH (a) WITH a RETURN *`, "RETURN * is not supported after WITH/UNWIND or multiple MATCH clauses")
	withErr(t, `MATCH (a) MATCH (b) RETURN *`, "RETURN * is not supported after WITH/UNWIND or multiple MATCH clauses")
}

// TestWithBareVarRefRules: bare-ident operands (the WHERE-after-WITH
// extension) resolve to scalar variables only — node/edge references are
// rejected at validation in both MATCH WHERE and WITH WHERE.
func TestWithBareVarRefRules(t *testing.T) {
	withErr(t, `MATCH (a) WHERE a RETURN a`, "bare variable reference requires a scalar")
	withErr(t, `MATCH (a)-[e:CALLS]->(b) WHERE e RETURN a`, "bare variable reference requires a scalar")
	withErr(t, `MATCH (a) WITH a WHERE a RETURN a`, "bare variable reference requires a scalar")
	withErr(t, `MATCH (a) WHERE missingvar RETURN a`, `variable "missingvar" is not declared in the pattern`)
}

// TestWithWhereAfterTrailerASC is the ASC-side discriminator T4's
// DESC-side pin could not tell apart: with w = gw:5, order:3, pay:7 in scan
// order, ASC+LIMIT 2 keeps [order(3), gw(5)] and the WHERE w > 4 then drops
// order — only gw survives. Filtering BEFORE the trailer would keep
// [gw(5), pay(7)] and return both, so the test pins trailer-then-filter.
func TestWithWhereAfterTrailerASC(t *testing.T) {
	res := run(t, `MATCH (a:Service) WITH a, a.w AS w ORDER BY w ASC LIMIT 2 WHERE w > 4 RETURN a.key`)
	if k := keysOf(res, "a.key"); !slices.Equal(k, []string{"gw"}) {
		t.Fatalf("ASC trailer-then-filter: %v", k)
	}
}

// TestWithChainedAggEmpty: an all-filtered first WITH stage feeds a second,
// aggregating WITH — the empty-input one-row rule carries through the chain
// (count(*) over zero rows is the single zero row).
func TestWithChainedAggEmpty(t *testing.T) {
	res := run(t, `MATCH (a:Service) WITH a.env AS e WHERE e = 'x' WITH count(*) AS c RETURN c`)
	if len(res.Rows) != 1 || res.Rows[0]["c"] != float64(0) {
		t.Fatalf("chained WITH over empty stage: %+v", res.Rows)
	}
}

// TestWithOrderByScalarKindCheck: ORDER BY scalar.prop in WITH is rejected
// (the projected name is a scalar; property access requires an entity)
// instead of silently sorting all-null keys. The entity form stays legal.
// The T6 rider mirrors the check on the RETURN path: a WITH/UNWIND-scoped
// scalar property-accessed by RETURN's ORDER BY is rejected the same way.
func TestWithOrderByScalarKindCheck(t *testing.T) {
	withErr(t, `MATCH (a:Service) WITH a.w AS w ORDER BY w.x RETURN w`, `variable "w" is a scalar; property access requires a node or edge`)
	withErr(t, `MATCH (a:Service) WITH count(*) AS c ORDER BY c.x RETURN c`, `variable "c" is a scalar; property access requires a node or edge`)
	// RETURN-path mirror (T6 rider): without it these silently sorted
	// all-null keys
	withErr(t, `MATCH (a:Service) WITH a.w AS w RETURN w ORDER BY w.x`, `variable "w" is a scalar; property access requires a node or edge`)
	withErr(t, `MATCH (a:Service) WITH count(*) AS c RETURN c ORDER BY c.x`, `variable "c" is a scalar; property access requires a node or edge`)
	withErr(t, `MATCH (a:Service) UNWIND [1, 2] AS k RETURN k ORDER BY k.x`, `variable "k" is a scalar; property access requires a node or edge`)
	// entity forms stay legal on both paths
	if res := run(t, `MATCH (a:Service) WITH a ORDER BY a.w ASC RETURN a.key`); len(res.Rows) != 3 {
		t.Fatalf("entity WITH ORDER BY must stay legal: %d rows", len(res.Rows))
	}
	if res := run(t, `MATCH (a:Service) RETURN a.key ORDER BY a.w`); len(res.Rows) != 3 {
		t.Fatalf("entity RETURN ORDER BY must stay legal: %d rows", len(res.Rows))
	}
}

// ---- strict empty stages through WITH ----

// TestWithStrictEmpty: a WITH stage over zero rows yields zero rows and
// never re-anchors a later MATCH (the T3 strict stage semantics).
func TestWithStrictEmpty(t *testing.T) {
	if res := run(t, `MATCH (a {nosuch:1}) WITH a RETURN a.key`); len(res.Rows) != 0 {
		t.Fatalf("empty WITH: %d rows", len(res.Rows))
	}
	if res := run(t, `MATCH (a {nosuch:1}) WITH a MATCH (b) RETURN b.key`); len(res.Rows) != 0 {
		t.Fatalf("empty WITH into MATCH must stay empty: %d rows", len(res.Rows))
	}
	// aggregation still reports its zero row over the strict-empty pipeline
	if res := run(t, `MATCH (a {nosuch:1}) WITH count(*) AS c RETURN c`); len(res.Rows) != 1 || res.Rows[0]["c"] != float64(0) {
		t.Fatalf("count over empty WITH: %+v", res.Rows)
	}
}
