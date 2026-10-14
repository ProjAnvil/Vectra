package cypher

import (
	"strings"
	"testing"
)

func TestParseCoreStatement(t *testing.T) {
	q, err := Parse(`MATCH (a:Bean)-[:CALLS*1..2]->(b) WHERE a.type='controller' AND b.w>=3 RETURN a.key, b ORDER BY b.w DESC LIMIT 5`)
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Match.Patterns) != 1 {
		t.Fatalf("patterns: %d", len(q.Match.Patterns))
	}
	p := q.Match.Patterns[0]
	if len(p.Nodes) != 2 || p.Nodes[0].Var != "a" || p.Nodes[0].Label != "Bean" || p.Nodes[1].Var != "b" {
		t.Fatalf("nodes: %+v", p.Nodes)
	}
	e := p.Edges[0]
	if len(e.Labels) != 1 || e.Labels[0] != "CALLS" || e.MinHops != 1 || e.MaxHops != 2 || e.Dir != 0 {
		t.Fatalf("edge: %+v", e)
	}
	w := q.Match.Where.(*Binary)
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
	if len(q.Return.Items) != 2 || q.Return.Items[0].Name != "a.key" || q.Return.Items[1].Name != "b" {
		t.Fatalf("items: %+v", q.Return.Items)
	}
	if len(q.Return.OrderBy) != 1 || q.Return.OrderBy[0].Name != "b.w" || !q.Return.OrderBy[0].Desc || q.Return.Limit != 5 {
		t.Fatalf("order/limit: %+v %d", q.Return.OrderBy, q.Return.Limit)
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
		e := q.Match.Patterns[0].Edges[0]
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
	if len(q.Match.Patterns) != 2 {
		t.Fatalf("patterns: %d", len(q.Match.Patterns))
	}
	np := q.Match.Patterns[0].Nodes[0]
	if np.Props["type"] != "controller" || np.Props["n"] != float64(3) {
		t.Fatalf("props: %+v", np.Props)
	}
	if _, ok := q.Match.Where.(*Binary); !ok {
		t.Fatal("where should be Binary")
	}
	if !q.Return.Star || q.Return.Limit != 2 {
		t.Fatalf("return: %+v", q.Return)
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
