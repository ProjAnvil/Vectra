package cypher

import (
	"strings"
	"testing"
)

// Write-clause parse/validate pins: happy paths assert the parsed AST
// shape (the hand-built-field style of parser_test.go), the error table
// asserts one substring plus the error position per validation rule.

func TestParseCreateNode(t *testing.T) {
	q, err := Parse(`CREATE (n:Service {key:'gateway', env:'prod'}) RETURN n`)
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Clauses) != 2 {
		t.Fatalf("clauses: %d", len(q.Clauses))
	}
	cc := q.Clauses[0].(*CreateClause)
	if len(cc.Patterns) != 1 {
		t.Fatalf("patterns: %d", len(cc.Patterns))
	}
	n := cc.Patterns[0].Nodes[0]
	if n.Var != "n" || n.Label != "Service" || len(cc.Patterns[0].Edges) != 0 {
		t.Fatalf("node: %+v", n)
	}
	if n.Props != nil {
		t.Fatalf("write pattern must not fill literal Props: %+v", n.Props)
	}
	if key := n.Exprs["key"].(*Lit); key.Val != "gateway" {
		t.Fatalf("key expr: %+v", key)
	}
	if env := n.Exprs["env"].(*Lit); env.Val != "prod" {
		t.Fatalf("env expr: %+v", env)
	}
	if _, ok := q.Clauses[1].(*ReturnClause); !ok {
		t.Fatalf("clause 1: %T", q.Clauses[1])
	}

	// comma-separated patterns, no RETURN (write statements may omit it)
	q, err = Parse(`CREATE (a:P {key:'x'}), (b:Q {key:'y'})`)
	if err != nil {
		t.Fatal(err)
	}
	cc = q.Clauses[0].(*CreateClause)
	if len(cc.Patterns) != 2 || cc.Patterns[0].Nodes[0].Var != "a" || cc.Patterns[1].Nodes[0].Var != "b" {
		t.Fatalf("patterns: %+v", cc.Patterns)
	}
	if len(q.Clauses) != 1 {
		t.Fatalf("clauses: %d", len(q.Clauses))
	}
}

func TestParseCreateEdge(t *testing.T) {
	q, err := Parse(`CREATE (a:P {key:'x'})-[r:CALLS]->(b:Q {key:'y'}) RETURN r`)
	if err != nil {
		t.Fatal(err)
	}
	cc := q.Clauses[0].(*CreateClause)
	pat := cc.Patterns[0]
	if len(pat.Nodes) != 2 || len(pat.Edges) != 1 {
		t.Fatalf("shape: %d nodes %d edges", len(pat.Nodes), len(pat.Edges))
	}
	if pat.Nodes[0].Var != "a" || pat.Nodes[0].Label != "P" || pat.Nodes[1].Var != "b" || pat.Nodes[1].Label != "Q" {
		t.Fatalf("nodes: %+v", pat.Nodes)
	}
	e := pat.Edges[0]
	if e.Var != "r" || len(e.Labels) != 1 || e.Labels[0] != "CALLS" || e.Dir != DirOut || e.MinHops != 1 || e.MaxHops != 1 {
		t.Fatalf("edge: %+v", e)
	}
}

func TestParseCreateRebind(t *testing.T) {
	q, err := Parse(`MATCH (a {key:'x'}) CREATE (a)-[:CALLS]->(b {key:'y'}) RETURN b`)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := q.Clauses[0].(*MatchClause); !ok {
		t.Fatalf("clause 0: %T", q.Clauses[0])
	}
	cc := q.Clauses[1].(*CreateClause)
	n := cc.Patterns[0].Nodes[0]
	if n.Var != "a" || n.Label != "" || n.Props != nil || n.Exprs != nil {
		t.Fatalf("rebind endpoint must stay bare: %+v", n)
	}
	if e := cc.Patterns[0].Edges[0]; e.Var != "" || len(e.Labels) != 1 || e.Labels[0] != "CALLS" {
		t.Fatalf("edge: %+v", e)
	}
}

func TestParseCreateExprProps(t *testing.T) {
	// the bulk-import shape: write-pattern prop values are EXPRESSIONS
	q, err := Parse(`UNWIND ['a','b'] AS k CREATE (n:P {key:k, weight:2}) RETURN n`)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := q.Clauses[0].(*UnwindClause); !ok {
		t.Fatalf("clause 0: %T", q.Clauses[0])
	}
	n := q.Clauses[1].(*CreateClause).Patterns[0].Nodes[0]
	if v, ok := n.Exprs["key"].(*VarRef); !ok || v.Var != "k" {
		t.Fatalf("key expr: %+v", n.Exprs["key"])
	}
	if l, ok := n.Exprs["weight"].(*Lit); !ok || l.Val != float64(2) {
		t.Fatalf("weight expr: %+v", n.Exprs["weight"])
	}
	if n.Props != nil {
		t.Fatalf("write pattern Props must stay nil: %+v", n.Props)
	}

	// MATCH patterns keep LITERAL props (index anchoring depends on it)
	qm, err := Parse(`MATCH (a:P {key:'x'}) RETURN a`)
	if err != nil {
		t.Fatal(err)
	}
	mp := qm.Clauses[0].(*MatchClause).Patterns[0].Nodes[0]
	if mp.Props["key"] != "x" || mp.Exprs != nil {
		t.Fatalf("match pattern props: %+v / exprs %+v", mp.Props, mp.Exprs)
	}
}

func TestParseSetItems(t *testing.T) {
	q, err := Parse(`MATCH (a) SET a.env = 'staging', a.weight = 3 RETURN a`)
	if err != nil {
		t.Fatal(err)
	}
	sc := q.Clauses[1].(*SetClause)
	if len(sc.Items) != 2 {
		t.Fatalf("items: %d", len(sc.Items))
	}
	it := sc.Items[0]
	if it.Var != "a" || it.Prop != "env" || it.Value.(*Lit).Val != "staging" {
		t.Fatalf("item 0: %+v", it)
	}
	it = sc.Items[1]
	if it.Var != "a" || it.Prop != "weight" || it.Value.(*Lit).Val != float64(3) {
		t.Fatalf("item 1: %+v", it)
	}

	// RHS may reference other entity properties
	if _, err := Parse(`MATCH (a), (b) SET a.env = b.env`); err != nil {
		t.Fatalf("prop RHS: %v", err)
	}
}

func TestParseDeleteDetached(t *testing.T) {
	q, err := Parse(`MATCH (a)-[r:CALLS]->(b) DETACH DELETE a, r`)
	if err != nil {
		t.Fatal(err)
	}
	d := q.Clauses[1].(*DeleteClause)
	if !d.Detach || len(d.Vars) != 2 || d.Vars[0] != "a" || d.Vars[1] != "r" {
		t.Fatalf("delete: %+v", d)
	}

	q, err = Parse(`MATCH (a) DELETE a`)
	if err != nil {
		t.Fatal(err)
	}
	d = q.Clauses[1].(*DeleteClause)
	if d.Detach || len(d.Vars) != 1 || d.Vars[0] != "a" {
		t.Fatalf("plain delete: %+v", d)
	}

	// RETURN after DELETE parses (deleted vars render null at runtime)
	if _, err := Parse(`MATCH (a) DELETE a RETURN a.key`); err != nil {
		t.Fatalf("delete + RETURN: %v", err)
	}
}

func TestParseMergeWithOnClauses(t *testing.T) {
	q, err := Parse(`MERGE (n:P {key:'x'}) ON CREATE SET n.created = true, n.ts = 1 ON MATCH SET n.seen = 2 RETURN n`)
	if err != nil {
		t.Fatal(err)
	}
	mc := q.Clauses[0].(*MergeClause)
	n := mc.Pattern.Nodes[0]
	if n.Var != "n" || n.Label != "P" || len(mc.Pattern.Edges) != 0 {
		t.Fatalf("pattern: %+v", mc.Pattern)
	}
	if key := n.Exprs["key"].(*Lit); key.Val != "x" {
		t.Fatalf("key expr: %+v", n.Exprs["key"])
	}
	if len(mc.OnCreate) != 2 || len(mc.OnMatch) != 1 {
		t.Fatalf("trailers: %d/%d", len(mc.OnCreate), len(mc.OnMatch))
	}
	oc := mc.OnCreate[0]
	if oc.Var != "n" || oc.Prop != "created" || oc.Value.(*Lit).Val != true {
		t.Fatalf("on create 0: %+v", oc)
	}
	if oc = mc.OnCreate[1]; oc.Prop != "ts" || oc.Value.(*Lit).Val != float64(1) {
		t.Fatalf("on create 1: %+v", oc)
	}
	if om := mc.OnMatch[0]; om.Prop != "seen" || om.Value.(*Lit).Val != float64(2) {
		t.Fatalf("on match: %+v", om)
	}

	// edge MERGE, no trailers
	q, err = Parse(`MERGE (a:P {key:'x'})-[r:CALLS]->(b:Q {key:'y'})`)
	if err != nil {
		t.Fatal(err)
	}
	mc = q.Clauses[0].(*MergeClause)
	if len(mc.Pattern.Nodes) != 2 || len(mc.Pattern.Edges) != 1 || mc.Pattern.Edges[0].Var != "r" {
		t.Fatalf("edge pattern: %+v", mc.Pattern)
	}
	if mc.OnCreate != nil || mc.OnMatch != nil {
		t.Fatalf("absent trailers must stay nil: %+v %+v", mc.OnCreate, mc.OnMatch)
	}
}

func TestParseIndexDDL(t *testing.T) {
	q, err := Parse(`CREATE INDEX ON (n.env)`)
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Clauses) != 1 {
		t.Fatalf("clauses: %d", len(q.Clauses))
	}
	ic := q.Clauses[0].(*IndexClause)
	if ic.Prop != "env" || ic.FullText {
		t.Fatalf("index: %+v", ic)
	}

	q, err = Parse(`CREATE FULLTEXT INDEX ON (n.body)`)
	if err != nil {
		t.Fatal(err)
	}
	ic = q.Clauses[0].(*IndexClause)
	if ic.Prop != "body" || !ic.FullText {
		t.Fatalf("fulltext index: %+v", ic)
	}
}

// TestParseConstraintDDL pins the constraint DDL grammar (spec §2.1): both
// the CREATE and DROP REQUIRE-form statements parse to one standalone
// ConstraintClause carrying the (label, prop) identity; there is no name
// form — the pair IS the identity.
func TestParseConstraintDDL(t *testing.T) {
	q, err := Parse(`CREATE CONSTRAINT FOR (n:Person) REQUIRE n.email IS UNIQUE`)
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Clauses) != 1 {
		t.Fatalf("clauses: %d", len(q.Clauses))
	}
	cc := q.Clauses[0].(*ConstraintClause)
	if cc.Label != "Person" || cc.Prop != "email" || cc.Drop {
		t.Fatalf("constraint: %+v", cc)
	}

	q, err = Parse(`DROP CONSTRAINT FOR (n:Person) REQUIRE n.email IS UNIQUE`)
	if err != nil {
		t.Fatal(err)
	}
	cc = q.Clauses[0].(*ConstraintClause)
	if cc.Label != "Person" || cc.Prop != "email" || !cc.Drop {
		t.Fatalf("drop constraint: %+v", cc)
	}

	// keywords are case-insensitive like every keyword in the grammar
	q, err = Parse(`create constraint for (n:P) require n.p is unique`)
	if err != nil {
		t.Fatal(err)
	}
	if cc := q.Clauses[0].(*ConstraintClause); cc.Label != "P" || cc.Prop != "p" || cc.Drop {
		t.Fatalf("lowercase constraint: %+v", cc)
	}
}

// TestConstraintClauseClassification pins BOTH classification lists (spec
// §5.2 layer 3): isWriteClause drives the parser's RETURN-optional logic,
// IsWrite drives the root/server routing — a ConstraintClause (either form)
// must register as a write in both or the statement misroutes.
func TestConstraintClauseClassification(t *testing.T) {
	if !isWriteClause(&ConstraintClause{Label: "P", Prop: "email"}) {
		t.Fatal("isWriteClause(&ConstraintClause{create}) = false")
	}
	if !isWriteClause(&ConstraintClause{Label: "P", Prop: "email", Drop: true}) {
		t.Fatal("isWriteClause(&ConstraintClause{drop}) = false")
	}
	for _, q := range []string{
		`CREATE CONSTRAINT FOR (n:Person) REQUIRE n.email IS UNIQUE`,
		`DROP CONSTRAINT FOR (n:Person) REQUIRE n.email IS UNIQUE`,
	} {
		got, err := IsWrite(q)
		if err != nil {
			t.Fatalf("IsWrite(%q): %v", q, err)
		}
		if !got {
			t.Fatalf("IsWrite(%q) = false, want true", q)
		}
	}
}

func TestParseWriteNoReturnOptional(t *testing.T) {
	for _, src := range []string{
		`CREATE (n:P {key:'x'})`,
		`MATCH (a) SET a.x = 1`,
		`MATCH (a) DELETE a`,
		`MERGE (n:P {key:'x'})`,
		`CREATE FULLTEXT INDEX ON (n.body)`,
		`CREATE CONSTRAINT FOR (n:Person) REQUIRE n.email IS UNIQUE`,
		`DROP CONSTRAINT FOR (n:Person) REQUIRE n.email IS UNIQUE`,
	} {
		if _, err := Parse(src); err != nil {
			t.Fatalf("Parse(%q): %v", src, err)
		}
	}
	// a pure-read statement still requires RETURN (existing error unchanged)
	_, err := Parse(`MATCH (a)`)
	if err == nil || !strings.Contains(err.Error(), `expected RETURN, got end of input`) {
		t.Fatalf("missing RETURN error: %v", err)
	}
}

func TestParseWriteErrors(t *testing.T) {
	cases := []struct {
		src       string
		want      string
		line, col int
	}{
		{"SET n.x = 1", "no scope to act on", 1, 1},
		{"DELETE n", "no scope to act on", 1, 1},
		{"DETACH DELETE n", "no scope to act on", 1, 1},
		{`MATCH (a) SET b.x = 1`, `variable "b" is not declared in the pattern`, 1, 1},
		{`UNWIND [1] AS k SET k.x = 1`, "SET/DELETE requires a node or edge variable", 1, 1},
		{`MATCH (a) DELETE b`, `variable "b" is not declared`, 1, 1},
		{"MATCH (a) CREATE (b) MATCH (c)", "read clauses after a write clause are not supported in this version", 1, 22},
		{"MATCH (a) CREATE (b) WITH a", "read clauses after a write clause", 1, 22},
		{"MATCH (a) CREATE (b) UNWIND [1] AS k", "read clauses after a write clause", 1, 22},
		{"MATCH (a) CREATE (b) OPTIONAL MATCH (c)", "read clauses after a write clause", 1, 22},
		{"CREATE (a)--(b)", "must be outgoing", 1, 1},
		{"CREATE (a)<--(b)", "must be outgoing", 1, 1},
		{"CREATE (a)-[r:X*2]->(b)", "variable-length edges are not supported", 1, 13},
		{"CREATE (a)-[r]->(b)", "write edge patterns require exactly one relationship label", 1, 13},
		{`MATCH (a) CREATE (a:P {key:'y'})`, `cannot re-specify label/props when rebinding "a"`, 1, 19},
		{"CREATE (n:P {key:1})", "key property must be a string", 1, 18},
		{"MERGE (n:P)", "MERGE node requires a {key: ...} property", 1, 8},
		{"MERGE (a)-[r:X]->(b)-[s:Y]->(c)", "MERGE takes a single node or a single edge pattern", 1, 1},
		{"MERGE (a)--(b)", "must be outgoing", 1, 1},
		{"CREATE INDEX ON (n:Label.env)", "indexes are global per property key; label-scoped indexes are not supported", 1, 19},
		{"CREATE INDEX ON (n:Label)", "label-scoped indexes are not supported", 1, 19},
		{"MATCH (a) CREATE INDEX ON (a.x)", "standalone statement", 1, 11},
		{"CREATE INDEX ON (n.x) RETURN n", "standalone statement", 1, 1},
		{"MATCH (a)-[r:X*]->(b) DELETE r", "variable-length edge", 1, 1},
		{`MATCH (a)-[r:X]->(b) CREATE (r)-[:P]->(c)`, `variable "r" is an edge`, 1, 30},
		{`MATCH (a) CREATE (b)-[a:X]->(c)`, `edge variable "a" is already declared`, 1, 23},
		// scalar colliding as an edge var rides the same `bound` branch
		// (edge pattern vars must be fresh names of any kind) — pinned
		// explicitly so the scalar arm cannot silently change message.
		{`UNWIND [1] AS k CREATE (a)-[k:X]->(b)`, `edge variable "k" is already declared`, 1, 29},
		{"MERGE (a), (b)", "MERGE takes exactly one pattern", 1, 10},
		// constraint DDL: standalone rule (both forms), grammar shape
		{"CREATE CONSTRAINT FOR (n:P) REQUIRE n.email IS UNIQUE RETURN n", "standalone statement", 1, 1},
		{"MATCH (a) CREATE CONSTRAINT FOR (n:P) REQUIRE n.email IS UNIQUE", "CREATE CONSTRAINT is a standalone statement", 1, 11},
		{"MATCH (a) DROP CONSTRAINT FOR (n:P) REQUIRE n.email IS UNIQUE", "DROP CONSTRAINT is a standalone statement", 1, 11},
		{"DROP CONSTRAINT FOR (n:P) REQUIRE n.email IS UNIQUE RETURN n", "standalone statement", 1, 1},
		{"CREATE CONSTRAINT ON (n:P) REQUIRE n.email IS UNIQUE", "expected FOR", 1, 19},
		{"CREATE CONSTRAINT FOR n:P REQUIRE n.email IS UNIQUE", `expected "("`, 1, 23},
		{"CREATE CONSTRAINT FOR (n) REQUIRE n.email IS UNIQUE", `expected ":"`, 1, 25},
		{"CREATE CONSTRAINT FOR (n:P) REQUIRE m.email IS UNIQUE", `REQUIRE variable "m" must repeat the FOR variable "n"`, 1, 37},
		{"DROP CONSTRAINT FOR (n:P)", "expected REQUIRE", 1, 26},
		{`CREATE CONSTRAINT FOR (n:P) REQUIRE n.email IS NOT UNIQUE`, "expected UNIQUE", 1, 48},
	}
	for _, c := range cases {
		_, err := Parse(c.src)
		if err == nil {
			t.Fatalf("Parse(%q): expected error", c.src)
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Fatalf("Parse(%q): got %q, want substring %q", c.src, err.Error(), c.want)
		}
		e, ok := err.(*Error)
		if !ok {
			t.Fatalf("Parse(%q): error must be *Error, got %T", c.src, err)
		}
		if e.Line != c.line || e.Col != c.col {
			t.Fatalf("Parse(%q): position %d:%d, want %d:%d", c.src, e.Line, e.Col, c.line, c.col)
		}
	}
}

// TestValidateReadAfterWriteMirrored pins the validate-side mirror of the
// read-after-write sequencing rule: parseQuery enforces it with token
// positions, validate() re-enforces it for hand-built Queries (same
// message, {1,1} — hand-built clauses carry no positions).
func TestValidateReadAfterWriteMirrored(t *testing.T) {
	q := &Query{Clauses: []Clause{
		&MatchClause{},
		&CreateClause{Patterns: []*PathPattern{{}}},
		&MatchClause{},
	}}
	err := q.validate()
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "read clauses after a write clause are not supported in this version") {
		t.Fatalf("got %q", err)
	}
	e, ok := err.(*Error)
	if !ok {
		t.Fatalf("error must be *Error, got %T", err)
	}
	if e.Line != 1 || e.Col != 1 {
		t.Fatalf("position %d:%d, want 1:1", e.Line, e.Col)
	}
	// The legal ordering (write clauses only after the read prefix) passes.
	q = &Query{Clauses: []Clause{
		&MatchClause{Patterns: []*PathPattern{{Nodes: []*NodePattern{{Var: "a"}}}}},
		&CreateClause{Patterns: []*PathPattern{{}}},
	}}
	if err := q.validate(); err != nil {
		t.Fatalf("write-after-read must validate: %v", err)
	}
}

// TestParseWriteEdgeProps pins the write-pattern edge property map (Task 3
// gap-fill: CREATE/MERGE edges carry expression-valued props, MATCH edges
// keep rejecting the brace form) and its reference validation.
func TestParseWriteEdgeProps(t *testing.T) {
	ast, err := Parse(`CREATE (a)-[:CALLS {w:1, tag:'x'}]->(b:P {key:'b'})`)
	if err != nil {
		t.Fatal(err)
	}
	cc := ast.Clauses[0].(*CreateClause)
	ep := cc.Patterns[0].Edges[0]
	if len(ep.Exprs) != 2 {
		t.Fatalf("edge Exprs = %v want two entries", ep.Exprs)
	}
	if w := ep.Exprs["w"].(*Lit); w.Val != 1.0 {
		t.Errorf("w = %v want 1", w.Val)
	}
	if tag := ep.Exprs["tag"].(*Lit); tag.Val != "x" {
		t.Errorf("tag = %v want x", tag.Val)
	}
	// props referencing declared variables parse (the bulk-import shape)
	if _, err := Parse(`UNWIND ['a'] AS k CREATE (x:P {key:k})-[:E {src:k}]->(y:P {key:'y'})`); err != nil {
		t.Fatalf("expression-valued edge prop: %v", err)
	}
	// undeclared references are positioned errors
	if _, err := Parse(`CREATE (a)-[:E {w: missing.w}]->(b)`); err == nil {
		t.Fatal("undeclared reference in edge props must fail validation")
	}
	// MATCH keeps its grammar: the brace on a read-pattern edge is a parse
	// error exactly as before the write path existed
	if _, err := Parse(`MATCH (a)-[:E {w:1}]->(b) RETURN a`); err == nil {
		t.Fatal("MATCH edge property map must not parse")
	}
}
