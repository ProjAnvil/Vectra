package mcpserver

import (
	"testing"
)

func TestLookupPropertyTypedValues(t *testing.T) {
	cs := newTestSession(t)
	call[any](t, cs, "inject_batch", map[string]any{
		"nodes": []map[string]any{
			{"key": "a", "label": "Service", "props": map[string]any{"env": "prod", "w": 3, "on": true}},
			{"key": "b", "label": "Service", "props": map[string]any{"env": "dev", "w": 7, "on": false}},
		},
		"edges": []map[string]any{{"srcKey": "a", "dstKey": "b"}},
	})
	call[any](t, cs, "seal", nil)

	nums := call[struct {
		Nodes []string `json:"nodes"`
	}](t, cs, "lookup_property", map[string]any{"propKey": "w", "propValue": 7})
	if len(nums.Nodes) != 1 || nums.Nodes[0] != "b" {
		t.Fatalf("numeric lookup: %+v", nums.Nodes)
	}
	bools := call[struct {
		Nodes []string `json:"nodes"`
	}](t, cs, "lookup_property", map[string]any{"propKey": "on", "propValue": true})
	if len(bools.Nodes) != 1 || bools.Nodes[0] != "a" {
		t.Fatalf("bool lookup: %+v", bools.Nodes)
	}
	strs := call[struct {
		Nodes []string `json:"nodes"`
	}](t, cs, "lookup_property", map[string]any{"propKey": "env", "propValue": "dev"})
	if len(strs.Nodes) != 1 || strs.Nodes[0] != "b" {
		t.Fatalf("string lookup: %+v", strs.Nodes)
	}
}

func TestCypherTool(t *testing.T) {
	cs := newTestSession(t)
	call[any](t, cs, "inject_batch", map[string]any{
		"nodes": []map[string]any{
			{"key": "gw", "label": "Service", "props": map[string]any{"type": "controller"}},
			{"key": "order", "label": "Service", "props": map[string]any{"type": "service", "w": 3}},
			{"key": "store", "label": "Store"},
		},
		"edges": []map[string]any{
			{"srcKey": "gw", "dstKey": "order", "label": "CALLS"},
			{"srcKey": "order", "dstKey": "store", "label": "USES"},
		},
	})
	call[any](t, cs, "seal", nil)

	out := call[struct {
		Columns []string         `json:"columns"`
		Rows    []map[string]any `json:"rows"`
		Count   int              `json:"count"`
		Total   int              `json:"total"`
	}](t, cs, "cypher", map[string]any{
		"query": `MATCH (b {type:'controller'})-[:CALLS]->(s)-[:USES]->(d) RETURN b.key, d.key`,
	})
	if out.Count != 1 || out.Total != 1 {
		t.Fatalf("count/total: %d/%d", out.Count, out.Total)
	}
	if len(out.Columns) != 2 || out.Columns[0] != "b.key" || out.Columns[1] != "d.key" {
		t.Fatalf("columns: %v", out.Columns)
	}
	if out.Rows[0]["b.key"] != "gw" || out.Rows[0]["d.key"] != "store" {
		t.Fatalf("row: %v", out.Rows[0])
	}

	// limit 截断：total 报真实值
	out = call[struct {
		Columns []string         `json:"columns"`
		Rows    []map[string]any `json:"rows"`
		Count   int              `json:"count"`
		Total   int              `json:"total"`
	}](t, cs, "cypher", map[string]any{
		"query": `MATCH (n) RETURN n.key`,
		"limit": 2,
	})
	if out.Count != 2 || out.Total != 3 {
		t.Fatalf("limit: count=%d total=%d", out.Count, out.Total)
	}
}
