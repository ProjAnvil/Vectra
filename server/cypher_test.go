package server

import (
	"bytes"
	"net/http"
	"testing"
	"time"
)

func TestAPICypherEndpoint(t *testing.T) {
	tc := newTestCluster(t, 1)
	leader := tc.leaderWithin(10 * time.Second)
	postJSON(t, leader.URL()+"/v1/batch", map[string]any{
		"nodes": []map[string]any{
			{"key": "gw", "label": "Service", "props": map[string]any{"type": "controller"}},
			{"key": "order", "label": "Service", "props": map[string]any{"type": "service"}},
			{"key": "store", "label": "Store"},
		},
		"edges": []map[string]any{
			{"srcKey": "gw", "dstKey": "order", "label": "CALLS"},
			{"srcKey": "order", "dstKey": "store", "label": "USES"},
		},
	})
	tc.waitAllPublished(3, 2, 5*time.Second)

	resp, out := postJSON(t, leader.URL()+"/v1/cypher", map[string]any{
		"query": `MATCH (b {type:'controller'})-[:CALLS]->(s)-[:USES]->(d) RETURN b.key, d.key`,
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cypher: %d %v", resp.StatusCode, out)
	}
	if out["count"] != float64(1) {
		t.Fatalf("count = %v, want 1", out["count"])
	}
	cols := out["columns"].([]any)
	if len(cols) != 2 || cols[0] != "b.key" || cols[1] != "d.key" {
		t.Fatalf("columns: %v", cols)
	}
	row := out["rows"].([]any)[0].(map[string]any)
	if row["b.key"] != "gw" || row["d.key"] != "store" {
		t.Fatalf("row: %v", row)
	}
	if resp.Header.Get("X-Vectra-Snapshot") == "" {
		t.Fatal("snapshot header missing")
	}

	// 节点值投影（json 序列化的 Node）
	resp, out = postJSON(t, leader.URL()+"/v1/cypher", map[string]any{
		"query": `MATCH (s {type:'service'}) RETURN s LIMIT 1`,
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("node value: %d %v", resp.StatusCode, out)
	}
	row = out["rows"].([]any)[0].(map[string]any)
	node := row["s"].(map[string]any)
	if node["key"] != "order" || node["props"].(map[string]any)["type"] != "service" {
		t.Fatalf("node: %v", node)
	}

	// 解析错 → 400 PARSE
	resp, out = postJSON(t, leader.URL()+"/v1/cypher", map[string]any{"query": `MATCH (a) RETURN b`})
	if resp.StatusCode != http.StatusBadRequest || out["error"].(map[string]any)["code"] != "PARSE" {
		t.Fatalf("parse error: %d %v", resp.StatusCode, out)
	}

	// 缺 query → 400 VALIDATION
	resp, out = postJSON(t, leader.URL()+"/v1/cypher", map[string]any{})
	if resp.StatusCode != http.StatusBadRequest || out["error"].(map[string]any)["code"] != "VALIDATION" {
		t.Fatalf("missing query: %d %v", resp.StatusCode, out)
	}

	// 坏 JSON → 400
	resp2, err := http.Post(leader.URL()+"/v1/cypher", "application/json", bytes.NewReader([]byte("{")))
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad json: %d", resp2.StatusCode)
	}
}
