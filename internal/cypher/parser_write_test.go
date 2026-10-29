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

func TestParseWriteNoReturnOptional(t *testing.T) {
	for _, src := range []string{
		`CREATE (n:P {key:'x'})`,
		`MATCH (a) SET a.x = 1`,
		`MATCH (a) DELETE a`,
		`MERGE (n:P {key:'x'})`,
		`CREATE FULLTEXT INDEX ON (n.body)`,
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
