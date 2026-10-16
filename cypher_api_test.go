package vectra

import (
	"errors"
	"testing"
)

func TestCypherFacadeEndToEnd(t *testing.T) {
	g := New()
	g.IndexProperty("type")
	_, _ = g.AddNode("gw", "Service", map[string]any{"type": "controller"})
	_, _ = g.AddNode("order", "Service", map[string]any{"type": "service", "w": 3})
	_, _ = g.AddNode("pay", "Service", map[string]any{"type": "service", "w": 7})
	_, _ = g.AddEdge("gw", "order", "CALLS", nil)
	_, _ = g.AddEdge("gw", "pay", "CALLS", nil)
	_, _ = g.AddEdge("order", "pay", "CALLS", nil)
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	res, err := g.Cypher(`MATCH (b {type:'controller'})-[:CALLS*1..2]->(s) RETURN b.key, s.key ORDER BY s.key`)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Columns) != 2 || res.Columns[0] != "b.key" || res.Columns[1] != "s.key" {
		t.Fatalf("columns: %v", res.Columns)
	}
	if len(res.Rows) != 2 || res.Rows[0]["s.key"] != "order" || res.Rows[1]["s.key"] != "pay" {
		t.Fatalf("rows: %+v", res.Rows)
	}

	// var 值投影为公有 Node/Edge 类型
	res, err = g.Cypher(`MATCH (a {type:'controller'}) RETURN a`)
	if err != nil {
		t.Fatal(err)
	}
	n, ok := res.Rows[0]["a"].(Node)
	if !ok || n.Key != "gw" || n.Label != "Service" || n.Props["type"] != "controller" {
		t.Fatalf("node value: %#v", res.Rows[0]["a"])
	}

	// 数值谓词 + 伪属性
	res, err = g.Cypher(`MATCH (s:Service) WHERE s.w > 4 AND s.label = 'Service' RETURN s.key`)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 || res.Rows[0]["s.key"] != "pay" {
		t.Fatalf("where: %+v", res.Rows)
	}

	// 未 Seal 合法（门已溶解）：对空 base 作答（遍历族 base-only，Task 4 收口）
	res2, err := New().Cypher(`MATCH (a) RETURN a`)
	if err != nil {
		t.Fatalf("unsealed: %v", err)
	}
	if len(res2.Rows) != 0 {
		t.Fatalf("unsealed rows = %d, want 0 (empty base)", len(res2.Rows))
	}

	// 解析/语义错误是定位的 CypherError
	_, err = g.Cypher(`MATCH (a) RETURN b`)
	var ce *CypherError
	if !errors.As(err, &ce) || ce.Line != 1 {
		t.Fatalf("cypher error: %v", err)
	}
}
