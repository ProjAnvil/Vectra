package cypher

import (
	"strings"
	"testing"
)

// Assertions read the clause list directly (q.Clauses[i] type assertions),
// never through the temporary accessors — the accessors die with the T3
// stage loop and these tests must survive that.

func TestParseCoreStatement(t *testing.T) {
	q, err := Parse(`MATCH (a:Bean)-[:CALLS*1..2]->(b) WHERE a.type='controller' AND b.w>=3 RETURN a.key, b ORDER BY b.w DESC LIMIT 5`)
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Clauses) != 2 {
		t.Fatalf("clauses: %d", len(q.Clauses))
	}
	m := q.Clauses[0].(*MatchClause)
	if len(m.Patterns) != 1 {
		t.Fatalf("patterns: %d", len(m.Patterns))
	}
	p := m.Patterns[0]
	if len(p.Nodes) != 2 || p.Nodes[0].Var != "a" || p.Nodes[0].Label != "Bean" || p.Nodes[1].Var != "b" {
		t.Fatalf("nodes: %+v", p.Nodes)
	}
	e := p.Edges[0]
	if len(e.Labels) != 1 || e.Labels[0] != "CALLS" || e.MinHops != 1 || e.MaxHops != 2 || e.Dir != 0 {
		t.Fatalf("edge: %+v", e)
	}
	w := m.Where.(*Binary)
	if w.Op != "AND" {
		t.Fatalf("where root: %v", w.Op)
	}
	lhs := w.L.(*Binary)
	if lhs.Op != "=" || lhs.L.(*PropRef).Var != "a" || lhs.L.(*PropRef).Name != "type" || lhs.R.(*Lit).Val != "controller" {
		t.Fatalf("where lhs: %+v", lhs)
	}
	rhs := w.R.(*Binary)
	if rhs.Op != ">=" || rhs.R.(*Lit).Val != float64(3) {
		t.Fatalf("where rhs: %+v", rhs)
	}
	ret := q.Clauses[1].(*ReturnClause)
	if len(ret.Items) != 2 || ret.Items[0].Name != "a.key" || ret.Items[1].Name != "b" {
		t.Fatalf("items: %+v", ret.Items)
	}
	if len(ret.OrderBy) != 1 || ret.OrderBy[0].Name != "b.w" || !ret.OrderBy[0].Desc || ret.Limit != 5 {
		t.Fatalf("order/limit: %+v %d", ret.OrderBy, ret.Limit)
	}
	if ret.Skip != -1 {
		t.Fatalf("skip: %d, want -1 (absent)", ret.Skip)
	}
}

func TestParseEdgeForms(t *testing.T) {
	cases := []struct {
		src        string
		dir        int
		labels     []string
		minH, maxH int
	}{
		{"MATCH (a)-[e:X|Y]->(b) RETURN e", 0, []string{"X", "Y"}, 1, 1},
		{"MATCH (a)<-[z]-(b) RETURN z", 1, nil, 1, 1},
		{"MATCH (a)--(b) RETURN a", 2, nil, 1, 1},
		{"MATCH (a)-->(b) RETURN a", 0, nil, 1, 1},
		{"MATCH (a)<--(b) RETURN a", 1, nil, 1, 1},
		{"MATCH (a)-[:X*]->(b) RETURN a", 0, []string{"X"}, 1, -1},
		{"MATCH (a)-[:X*2]->(b) RETURN a", 0, []string{"X"}, 2, 2},
		{"MATCH (a)-[:X*..3]->(b) RETURN a", 0, []string{"X"}, 1, 3},
		{"MATCH (a)-[:X*2..]->(b) RETURN a", 0, []string{"X"}, 2, -1},
		{"MATCH (a)-[:X*2..4]->(b) RETURN a", 0, []string{"X"}, 2, 4},
		{"MATCH (a)<-[:X]-(b) RETURN a", 1, []string{"X"}, 1, 1},
	}
	for _, c := range cases {
		q, err := Parse(c.src)
		if err != nil {
			t.Fatalf("Parse(%q): %v", c.src, err)
		}
		e := q.Clauses[0].(*MatchClause).Patterns[0].Edges[0]
		if e.Dir != c.dir || e.MinHops != c.minH || e.MaxHops != c.maxH {
			t.Fatalf("Parse(%q): got dir=%d hops=%d..%d, want dir=%d hops=%d..%d", c.src, e.Dir, e.MinHops, e.MaxHops, c.dir, c.minH, c.maxH)
		}
		if len(e.Labels) != len(c.labels) {
			t.Fatalf("Parse(%q): labels %v want %v", c.src, e.Labels, c.labels)
		}
	}
}

func TestParsePropsCommaPathsAndOps(t *testing.T) {
	q, err := Parse(`MATCH (a {type:'controller', n:3}), (c)-[:Y]->(b) WHERE NOT (a.x = 1 OR a.y <> 2) AND a.name STARTS WITH 'Or' AND a.env IN ['prod','stg'] AND b.e CONTAINS 'x' AND a.z ENDS WITH 's' RETURN * LIMIT 2`)
	if err != nil {
		t.Fatal(err)
	}
	m := q.Clauses[0].(*MatchClause)
	if len(m.Patterns) != 2 {
		t.Fatalf("patterns: %d", len(m.Patterns))
	}
	np := m.Patterns[0].Nodes[0]
	if np.Props["type"] != "controller" || np.Props["n"] != float64(3) {
		t.Fatalf("props: %+v", np.Props)
	}
	if _, ok := m.Where.(*Binary); !ok {
		t.Fatal("where should be Binary")
	}
	ret := q.Clauses[1].(*ReturnClause)
	if !ret.Star || ret.Limit != 2 {
		t.Fatalf("return: %+v", ret)
	}
	// 大小写不敏感关键词
	if _, err := Parse(`match (a) where a.x = 1 return a limit 3`); err != nil {
		t.Fatalf("lowercase keywords: %v", err)
	}
}

func TestParseErrors(t *testing.T) {
	cases := []struct {
		src  string
		want string // 错误信息子串
	}{
		{"MATCH (a RETURN a", `expected ")"`},
		{"MATCH (a) RETURN b", `variable "b" is not declared`},
		{"MATCH (a) RETURN a WHERE b.x = 1", "expected end of input"}, // WHERE 在 RETURN 后
		{"MATCH (a)-[r:X*1..2]->(b) RETURN r", "variable-length edge"},
		{"MATCH (a)-[r:X*1..2]->(b) WHERE r.y = 1 RETURN b", "variable-length edge"},
		{"MATCH (a) RETURN a ORDER BY b.x", "is not declared"}, // ORDER BY 引用未声明变量
		{"MATCH () RETURN *", "no named variables"},
		{"MATCH (a) WHERE a.x = RETURN a", "expected"}, // 右操作数缺失
		{"MATCH (a) WHERE = 1 RETURN a", "expected"},   // 左操作数非法
		{"MATCH (a) RETURN a LIMIT -", "expected number"},
		{"MATCH (a)", `expected RETURN, got end of input`}, // RETURN 缺失
		{"MATCH (a) WHERE a.x = 1", `expected RETURN, got end of input`},
		{"MATCH (a) LIMIT 1", `expected RETURN, got "LIMIT"`}, // 子句位置的非 RETURN 关键字
	}
	for _, c := range cases {
		_, err := Parse(c.src)
		if err == nil {
			t.Fatalf("Parse(%q): expected error", c.src)
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Fatalf("Parse(%q): got %q, want substring %q", c.src, err.Error(), c.want)
		}
		if _, ok := err.(*Error); !ok {
			t.Fatalf("Parse(%q): error must be *Error, got %T", c.src, err)
		}
	}
}

// TestParseSkip pins the SKIP trailer: after ORDER BY, before LIMIT.
func TestParseSkip(t *testing.T) {
	q, err := Parse(`MATCH (a) RETURN a ORDER BY a.key DESC SKIP 2 LIMIT 3`)
	if err != nil {
		t.Fatal(err)
	}
	ret := q.Clauses[1].(*ReturnClause)
	if ret.Skip != 2 || ret.Limit != 3 {
		t.Fatalf("skip/limit: %d/%d", ret.Skip, ret.Limit)
	}
	if len(ret.OrderBy) != 1 || ret.OrderBy[0].Name != "a.key" || !ret.OrderBy[0].Desc {
		t.Fatalf("order: %+v", ret.OrderBy)
	}

	// SKIP without LIMIT, and without ORDER BY
	q, err = Parse(`MATCH (a) RETURN a SKIP 1`)
	if err != nil {
		t.Fatal(err)
	}
	ret = q.Clauses[1].(*ReturnClause)
	if ret.Skip != 1 || ret.Limit != -1 {
		t.Fatalf("skip/limit: %d/%d", ret.Skip, ret.Limit)
	}

	// negative / non-integer SKIP
	for _, c := range []struct{ src, want string }{
		{"MATCH (a) RETURN a SKIP 1.5", `bad SKIP "1.5"`},
		{"MATCH (a) RETURN a SKIP -1", "expected number"},
		{"MATCH (a) RETURN a LIMIT 1 SKIP 2", `expected end of input`}, // SKIP must precede LIMIT
	} {
		_, err := Parse(c.src)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("Parse(%q): got %v, want substring %q", c.src, err, c.want)
		}
	}
}

// TestParseClauseCardinality pins the grammar cardinality: MATCH may
// repeat (cartesian/join semantics arrive with the T3 stage loop), RETURN
// is terminal, and WITH/UNWIND/OPTIONAL are not clause keywords yet —
// their arms land in T4/T5/T6, which own these three pins' replacement.
func TestParseClauseCardinality(t *testing.T) {
	// repeated MATCH parses and validates (T2 executes only the first)
	q, err := Parse(`MATCH (a) MATCH (b) RETURN a`)
	if err != nil {
		t.Fatalf("repeated MATCH: %v", err)
	}
	if len(q.Clauses) != 3 {
		t.Fatalf("clauses: %d", len(q.Clauses))
	}
	if _, ok := q.Clauses[0].(*MatchClause); !ok {
		t.Fatalf("clause 0: %T", q.Clauses[0])
	}
	if _, ok := q.Clauses[1].(*MatchClause); !ok {
		t.Fatalf("clause 1: %T", q.Clauses[1])
	}
	if _, ok := q.Clauses[2].(*ReturnClause); !ok {
		t.Fatalf("clause 2: %T", q.Clauses[2])
	}

	// RETURN is terminal: any clause keyword after it errors
	for _, c := range []struct{ src, want string }{
		{"MATCH (a) RETURN a MATCH (b)", `expected end of input, got "MATCH"`},
		{"MATCH (a) WITH a AS x RETURN x", `expected RETURN, got "WITH"`},
		{"MATCH (a) UNWIND [1] AS k RETURN k", `expected RETURN, got "UNWIND"`},
		{"MATCH (a) OPTIONAL MATCH (b) RETURN a", `expected RETURN, got "OPTIONAL"`},
	} {
		_, err := Parse(c.src)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("Parse(%q): got %v, want substring %q", c.src, err, c.want)
		}
	}

	// keyword-as-alias: keywords lex as identifiers, so an alias may spell
	// like one (divergence from Cypher noted for T7)
	q, err = Parse(`MATCH (a) RETURN a AS match`)
	if err != nil {
		t.Fatalf("keyword alias: %v", err)
	}
	if got := q.Clauses[1].(*ReturnClause).Items[0].Name; got != "match" {
		t.Fatalf("alias name: %q", got)
	}
}

// TestParseClauseStorageInline pins the zero-alloc clause storage: a
// parsed query of up to four clauses carries its clause list in the Query
// struct's inline array (no heap slice at all — the T1 bench table's
// allocs/op include zero clause-slice allocations), and the fifth clause
// spills to a heap slice naturally. The backing identity is the contract:
// append over clauses[:0] with cap 4 cannot allocate below the spill.
func TestParseClauseStorageInline(t *testing.T) {
	two, err := Parse(`MATCH (a)-[:CALLS]->(b) RETURN b.key`)
	if err != nil {
		t.Fatal(err)
	}
	if len(two.Clauses) != 2 || &two.Clauses[0] != &two.clauses[0] {
		t.Fatalf("2-clause query must live in the inline clause storage: %d clauses", len(two.Clauses))
	}
	four, err := Parse(`MATCH (a) MATCH (b) MATCH (c) RETURN a.key`)
	if err != nil {
		t.Fatal(err)
	}
	if len(four.Clauses) != 4 || &four.Clauses[0] != &four.clauses[0] {
		t.Fatalf("4-clause query must live in the inline clause storage: %d clauses", len(four.Clauses))
	}
	five, err := Parse(`MATCH (a) MATCH (b) MATCH (c) MATCH (d) RETURN a.key`)
	if err != nil {
		t.Fatal(err)
	}
	if len(five.Clauses) != 5 || &five.Clauses[0] == &five.clauses[0] {
		t.Fatalf("5-clause query must spill off the inline storage: %d clauses", len(five.Clauses))
	}
}
