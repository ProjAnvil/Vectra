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

	// limit truncates: total reports the true value
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

// TestMCPCypherSummary pins the cypher tool's write-result surface: the
// embedded engine executes the statement and the structured output carries
// the summary counters; reads keep the summary key absent.
func TestMCPCypherSummary(t *testing.T) {
	cs := newTestSession(t)
	out := call[struct {
		Columns []string         `json:"columns"`
		Rows    []map[string]any `json:"rows"`
		Summary *struct {
			CreatedNodes   int `json:"createdNodes"`
			CreatedEdges   int `json:"createdEdges"`
			DeletedNodes   int `json:"deletedNodes"`
			DeletedEdges   int `json:"deletedEdges"`
			PropertiesSet  int `json:"propertiesSet"`
			IndexesCreated int `json:"indexesCreated"`
		} `json:"summary"`
	}](t, cs, "cypher", map[string]any{
		"query": `CREATE (n:Service {key:'gw', env:'prod'})`,
	})
	if out.Summary == nil || out.Summary.CreatedNodes != 1 || out.Summary.CreatedEdges != 0 {
		t.Fatalf("create summary: %+v", out.Summary)
	}

	// The write is visible to the next read, which carries no summary.
	read := call[struct {
		Columns []string         `json:"columns"`
		Rows    []map[string]any `json:"rows"`
		Count   int              `json:"count"`
		Summary *struct {
			CreatedNodes int `json:"createdNodes"`
		} `json:"summary"`
	}](t, cs, "cypher", map[string]any{"query": `MATCH (n {key:'gw'}) RETURN n.env`})
	if read.Count != 1 || len(read.Rows) != 1 || read.Rows[0]["n.env"] != "prod" {
		t.Fatalf("read after write: %+v", read)
	}
	if read.Summary != nil {
		t.Fatalf("read response must not carry summary: %+v", read.Summary)
	}

	// SET surfaces propertiesSet.
	set := call[struct {
		Summary *struct {
			PropertiesSet int `json:"propertiesSet"`
		} `json:"summary"`
	}](t, cs, "cypher", map[string]any{"query": `MATCH (n {key:'gw'}) SET n.env = 'staging'`})
	if set.Summary == nil || set.Summary.PropertiesSet != 1 {
		t.Fatalf("set summary: %+v", set.Summary)
	}
}
