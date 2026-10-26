package cypher

import (
	"strings"
	"testing"

	"github.com/projanvil/vectra/internal/engine"
)

// optSnap is the OPTIONAL-MATCH fixture:
//
//	alice ──KNOWS──> bob        Persons: alice, bob, carol
//	                             Ghosts: g1, g2 (no edges at all)
//
// alice alone has a KNOWS out-edge — the mixed matched/unmatched shape
// every optional test needs; the two isolated Ghost nodes give a disjoint
// optional pattern a non-empty cartesian target, and the absent Void
// label the empty one.
func optSnap(t *testing.T) *engine.Snapshot {
	t.Helper()
	nodes := []engine.NodeRec{
		{Key: "alice", Label: "Person", Props: map[string]any{"name": "alice"}},
		{Key: "bob", Label: "Person", Props: map[string]any{"name": "bob"}},
		{Key: "carol", Label: "Person", Props: map[string]any{"name": "carol"}},
		{Key: "g1", Label: "Ghost", Props: map[string]any{"name": "g1"}},
		{Key: "g2", Label: "Ghost", Props: map[string]any{"name": "g2"}},
	}
	edges := []engine.EdgeRec{{Src: 0, Dst: 1, LabelID: 0}}
	s, err := engine.Seal(nodes, edges, []string{"KNOWS"}, []string{"name"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// optRun runs q against the optional fixture (run() is welded to execSnap).
func optRun(t *testing.T, q string) *Result {
	t.Helper()
	res, err := Run(optSnap(t), q)
	if err != nil {
		t.Fatalf("Run(%q): %v", q, err)
	}
	return res
}

// optErr pins a query error (parse or validate path) through Run.
func optErr(t *testing.T, q, want string) {
	t.Helper()
	_, err := Run(optSnap(t), q)
	if err == nil {
		t.Fatalf("Run(%q): expected error %q, got nil", q, want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("Run(%q): got %q, want substring %q", q, err.Error(), want)
	}
}

// ---- parser arm ----

// TestOptionalParse pins the OPTIONAL MATCH grammar: the two-keyword arm
// at any clause position (leading included), the Optional flag on the
// clause, WHERE inside the optional clause — and the T2 pin replacement:
// a lone OPTIONAL at clause position (anything but MATCH following it)
// stays the generic unexpected token.
func TestOptionalParse(t *testing.T) {
	q, err := Parse(`MATCH (a) OPTIONAL MATCH (b) RETURN a`)
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Clauses) != 3 {
		t.Fatalf("clauses: %d", len(q.Clauses))
	}
	if m0, ok := q.Clauses[0].(*MatchClause); !ok || m0.Optional {
		t.Fatalf("clause 0: %T optional=%v", q.Clauses[0], m0.Optional)
	}
	m1, ok := q.Clauses[1].(*MatchClause)
	if !ok || !m1.Optional {
		t.Fatalf("clause 1: %T optional=%v", q.Clauses[1], m1.Optional)
	}

	// leading position: OPTIONAL MATCH may open the query (execution
	// anchors it from the one implicit empty incoming row)
	q, err = Parse(`OPTIONAL MATCH (a) RETURN a`)
	if err != nil {
		t.Fatalf("leading OPTIONAL MATCH: %v", err)
	}
	if m := q.Clauses[0].(*MatchClause); !m.Optional {
		t.Fatalf("leading flag: %v", m.Optional)
	}

	// clause keywords are case-insensitive like every other
	if _, err := Parse(`match (a) optional match (b) return a`); err != nil {
		t.Fatalf("case-insensitive: %v", err)
	}

	// WHERE inside the optional clause belongs to it
	q, err = Parse(`MATCH (a) OPTIONAL MATCH (b) WHERE b.name = 'x' RETURN a`)
	if err != nil {
		t.Fatal(err)
	}
	if m := q.Clauses[1].(*MatchClause); m.Where == nil {
		t.Fatal("optional WHERE: nil")
	}

	// T2 pin replacement: OPTIONAL at clause position NOT followed by
	// MATCH is still the generic unexpected token (the pre-T6 pin rejected
	// every OPTIONAL at clause position; this replaces parser_test.go's
	// `MATCH (a) OPTIONAL MATCH (b) RETURN a` row)
	optErr(t, `MATCH (a) OPTIONAL RETURN a`, `expected RETURN, got "OPTIONAL"`)

	// T2 guard unchanged: a scalar var as a pattern var inside the
	// optional clause is still a validate error
	optErr(t, `MATCH (a) WITH a.name AS k OPTIONAL MATCH (k) RETURN k`,
		`variable "k" is a scalar; pattern variables must be nodes or edges`)

	// optional vars are ordinary scope members after the clause — no new
	// validation errors for referencing them
	if _, err := Parse(`MATCH (a) OPTIONAL MATCH (b) RETURN b`); err != nil {
		t.Fatalf("optional var referenced after clause: %v", err)
	}
}

// ---- semantics ----

// TestOptionalMatchedUnmatched: one query mixing rows whose optional
// pattern matches (alice) with rows whose does not (bob, carol) — matched
// rows extend with the pattern bindings, unmatched rows survive with the
// pattern var reading null.
func TestOptionalMatchedUnmatched(t *testing.T) {
	res := optRun(t, `MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(f) RETURN p.name, f.name`)
	if len(res.Rows) != 3 {
		t.Fatalf("rows: %d (%+v)", len(res.Rows), res.Rows)
	}
	got := map[string]any{}
	for _, r := range res.Rows {
		p, ok := r["p.name"].(string)
		if !ok {
			t.Fatalf("p.name cell: %#v", r["p.name"])
		}
		if _, dup := got[p]; dup {
			t.Fatalf("duplicate base row survived as two: %+v", res.Rows)
		}
		got[p] = r["f.name"]
	}
	if got["alice"] != "bob" {
		t.Fatalf("alice f.name = %#v, want \"bob\"", got["alice"])
	}
	if got["bob"] != nil || got["carol"] != nil {
		t.Fatalf("unmatched rows must read null: %+v", got)
	}

	// the unbound var projects as a null NODE cell too (itemValue's
	// unbound arm), not just a null property
	res = optRun(t, `MATCH (p {name:'bob'}) OPTIONAL MATCH (p)-[:KNOWS]->(f) RETURN f`)
	if len(res.Rows) != 1 || res.Rows[0]["f"] != nil {
		t.Fatalf("null node read: %+v", res.Rows)
	}
}

// TestOptionalCommaAllOrNothing: optionality covers the WHOLE keyword —
// with comma patterns every pattern must match for the row to extend, and
// the row survives with every pattern var null otherwise. The
// discriminator: alice's first pattern matches (f=bob) but the second
// empties, while bob/carol's first pattern empties — an implementation
// that re-anchors a later pattern from scratch after an empty earlier one
// would strand rows that lost the p binding entirely.
func TestOptionalCommaAllOrNothing(t *testing.T) {
	res := optRun(t, `MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(f), (f)-[:KNOWS]->(g) RETURN p.name, f.name, g.name`)
	if len(res.Rows) != 3 {
		t.Fatalf("rows: %d (%+v)", len(res.Rows), res.Rows)
	}
	for _, r := range res.Rows {
		if r["p.name"] == nil {
			t.Fatalf("base binding lost: %+v", r)
		}
		if r["f.name"] != nil || r["g.name"] != nil {
			t.Fatalf("no row may extend under all-or-nothing: %+v", r)
		}
	}
}

// TestOptionalWhereEmptyKeepsRow: the optional clause's WHERE filters the
// sub-rows; a WHERE that filters the expansion to empty still keeps the
// base row (contrast: the same WHERE on a plain MATCH yields zero rows).
func TestOptionalWhereEmptyKeepsRow(t *testing.T) {
	res := optRun(t, `MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(f) WHERE f.name = 'zed' RETURN p.name, f.name`)
	if len(res.Rows) != 3 {
		t.Fatalf("rows: %d (%+v)", len(res.Rows), res.Rows)
	}
	for _, r := range res.Rows {
		if r["p.name"] == nil || r["f.name"] != nil {
			t.Fatalf("kept row must carry base bindings and null pattern vars: %+v", r)
		}
	}
	if res := run(t, `MATCH (p)-[:KNOWS]->(f) WHERE f.name = 'zed' RETURN p.key`); len(res.Rows) != 0 {
		t.Fatalf("plain MATCH twin must yield zero rows: %d", len(res.Rows))
	}

	// WHERE over the sub-rows sees base and pattern bindings together:
	// only alice's cartesian ghosts survive p.name = 'alice'; bob and
	// carol are kept with null x
	res = optRun(t, `MATCH (p:Person) OPTIONAL MATCH (x:Ghost) WHERE p.name = 'alice' RETURN p.name, x.name`)
	if len(res.Rows) != 4 {
		t.Fatalf("rows: %d (%+v)", len(res.Rows), res.Rows)
	}
	paired := 0
	for _, r := range res.Rows {
		if r["p.name"] == "alice" {
			paired++
			if r["x.name"] != "g1" && r["x.name"] != "g2" {
				t.Fatalf("alice row: %+v", r)
			}
		} else if r["x.name"] != nil {
			t.Fatalf("kept row must have null x: %+v", r)
		}
	}
	if paired != 2 {
		t.Fatalf("alice pairs: %d", paired)
	}
}

// TestOptionalNullThroughWith: an unbound pattern var projected through
// WITH yields a null scalar and MUST NOT re-seed the node bindings (a
// zero-value NodeVal would resurrect node 0 and read as "alice").
func TestOptionalNullThroughWith(t *testing.T) {
	res := optRun(t, `MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(f) WITH p, f RETURN p.name, f.name`)
	if len(res.Rows) != 3 {
		t.Fatalf("rows: %d (%+v)", len(res.Rows), res.Rows)
	}
	for _, r := range res.Rows {
		if r["p.name"] == nil {
			t.Fatalf("p lost through WITH: %+v", r)
		}
		// the matched row keeps its binding; the two kept-unchanged rows
		// must project f as null (a zero-value NodeVal re-seed would
		// resurrect node 0 and read as "alice")
		if r["p.name"] == "alice" {
			if r["f.name"] != "bob" {
				t.Fatalf("matched f lost through WITH: %+v", r)
			}
		} else if r["f.name"] != nil {
			t.Fatalf("null f must survive WITH as null (no re-seed): %+v", r)
		}
	}

	// the entity/null cells survive an aliased projection round-trip
	res = optRun(t, `MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(f) WITH f AS x RETURN x`)
	if len(res.Rows) != 3 {
		t.Fatalf("rows: %d", len(res.Rows))
	}
	nulls, node := 0, 0
	for _, r := range res.Rows {
		switch r["x"].(type) {
		case nil:
			nulls++
		case NodeVal:
			node++
		default:
			t.Fatalf("x cell: %#v", r["x"])
		}
	}
	if nulls != 2 || node != 1 {
		t.Fatalf("x cells: %d null, %d node", nulls, node)
	}
}

// TestOptionalCartesianNull: a disjoint optional pattern cartesian-products
// per base row against a NON-empty candidate set and survives an EMPTY
// one (the null extension); the leading form anchors from the implicit
// empty row on both sides.
func TestOptionalCartesianNull(t *testing.T) {
	// empty candidate set: every base row kept, x null
	res := optRun(t, `MATCH (p:Person) OPTIONAL MATCH (x:Void) RETURN p.name, x.name`)
	if len(res.Rows) != 3 {
		t.Fatalf("null extension rows: %d (%+v)", len(res.Rows), res.Rows)
	}
	for _, r := range res.Rows {
		if r["x.name"] != nil {
			t.Fatalf("x must read null: %+v", r)
		}
	}

	// non-empty disjoint set: full cartesian 3 x 2
	res = optRun(t, `MATCH (p:Person) OPTIONAL MATCH (x:Ghost) RETURN p.name, x.name`)
	if len(res.Rows) != 6 {
		t.Fatalf("cartesian rows: %d (%+v)", len(res.Rows), res.Rows)
	}

	// leading form, matched: one row per ghost
	res = optRun(t, `OPTIONAL MATCH (x:Ghost) RETURN x.name`)
	if len(res.Rows) != 2 {
		t.Fatalf("leading matched rows: %d (%+v)", len(res.Rows), res.Rows)
	}

	// leading form, empty: the one implicit row survives all-null
	res = optRun(t, `OPTIONAL MATCH (x:Void) RETURN x.name`)
	if len(res.Rows) != 1 || res.Rows[0]["x.name"] != nil {
		t.Fatalf("leading null extension: %+v", res.Rows)
	}
}

// TestOptionalNoDedupMerge: the merge does not dedup across base rows —
// duplicate incoming rows (UNWIND fans [1,1] over one matched base) stay
// duplicated whether the optional pattern matches or not.
func TestOptionalNoDedupMerge(t *testing.T) {
	// unmatched duplicates: both kept (a cross-row dedup would collapse
	// them into one)
	res := optRun(t, `MATCH (p:Person) WHERE p.name = 'bob' UNWIND [1, 1] AS i OPTIONAL MATCH (p)-[:KNOWS]->(f) RETURN count(*) AS c`)
	if len(res.Rows) != 1 || res.Rows[0]["c"] != float64(2) {
		t.Fatalf("unmatched duplicates: %+v", res.Rows)
	}

	// matched duplicates: both expand
	res = optRun(t, `MATCH (p:Person) WHERE p.name = 'alice' UNWIND [1, 1] AS i OPTIONAL MATCH (p)-[:KNOWS]->(f) RETURN count(*) AS c`)
	if len(res.Rows) != 1 || res.Rows[0]["c"] != float64(2) {
		t.Fatalf("matched duplicates: %+v", res.Rows)
	}
}

// TestOptionalMaxRows: the per-base expansion is capped by the standard
// per-pattern check, and the MERGED output cumulatively — the cartesian
// shape never exceeds maxRows inside one base row but does across them.
func TestOptionalMaxRows(t *testing.T) {
	saved := maxRows
	t.Cleanup(func() { maxRows = saved })

	// cumulative: 3 persons x 2 ghosts = 6 merged; cap 3 trips mid-merge
	// (the single-row expansions of 2 never exceed it)
	maxRows = 3
	optErr(t, `MATCH (p:Person) OPTIONAL MATCH (x:Ghost) RETURN p.name`,
		"result too large: more than 3 intermediate rows")

	// within one base row: the per-pattern check trips first
	maxRows = 1
	optErr(t, `MATCH (p {name:'alice'}) OPTIONAL MATCH (x:Ghost) RETURN p.name`,
		"result too large: more than 1 intermediate rows")
}

// TestOptionalKeptRowPosNormalized: kept-unchanged rows normalize pos to
// 0 — the stage-constructed-row invariant WITH/UNWIND share — so no
// downstream consumer ever sees the stale frontier position. Checked at
// the stage level: the public stages construct fresh rows (pos 0 anyway)
// before any pos-sensitive consumer could observe a kept row.
func TestOptionalKeptRowPosNormalized(t *testing.T) {
	s := optSnap(t)
	ex := &executor{snap: s, adjF: s.AdjacencyF(), adjR: s.AdjacencyR(), fastAdj: true}
	m := &MatchClause{
		Optional: true,
		Patterns: []*PathPattern{{Nodes: []*NodePattern{{Var: "x", Label: "Void"}}}},
	}
	base := []row{{pos: 7}, {pos: 9, nodes: map[string]uint32{"p": 0}}}
	out, err := ex.optionalRows(m, base)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 || out[0].pos != 0 || out[1].pos != 0 || out[1].nodes["p"] != 0 {
		t.Fatalf("kept rows: %+v", out)
	}

	// a nil base (first stage) yields the one implicit empty row
	out, err = ex.optionalRows(m, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0].pos != 0 || out[0].nodes != nil {
		t.Fatalf("implicit first row: %+v", out)
	}
}

// ---- baseline ----

// TestOptionalBaselinePlainPaths: the plain-MATCH twins of the optional
// shapes keep their behavior — the optional arm must not disturb the
// matched path.
func TestOptionalBaselinePlainPaths(t *testing.T) {
	res := optRun(t, `MATCH (p:Person)-[:KNOWS]->(f) RETURN p.name, f.name`)
	if len(res.Rows) != 1 || res.Rows[0]["p.name"] != "alice" || res.Rows[0]["f.name"] != "bob" {
		t.Fatalf("plain twin: %+v", res.Rows)
	}
	res = optRun(t, `MATCH (p:Person), (x:Ghost) RETURN count(*) AS c`)
	if len(res.Rows) != 1 || res.Rows[0]["c"] != float64(6) {
		t.Fatalf("plain comma cartesian: %+v", res.Rows)
	}
}
