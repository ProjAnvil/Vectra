package mcpserver

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/projanvil/vectra"
)

// newTestSession starts an in-process server and connects an MCP client to it (real protocol path).
func newTestSession(t *testing.T) *mcp.ClientSession {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	s := New(vectra.New(), Options{})
	ct, st := mcp.NewInMemoryTransports()
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0"}, nil)
	go func() { _ = s.Run(ctx, st) }() // server must be up first so the handshake can complete
	cs, err := client.Connect(ctx, ct, nil)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cs.Close()
		cancel()
	})
	return cs
}

func call[Out any](t *testing.T, cs *mcp.ClientSession, tool string, args map[string]any) Out {
	t.Helper()
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      tool,
		Arguments: args,
	})
	if err != nil {
		t.Fatalf("CallTool(%s): %v", tool, err)
	}
	if res.IsError {
		t.Fatalf("CallTool(%s) returned tool error: %+v", tool, res.Content)
	}
	// structured output is in res.StructuredContent / res.RawOutput
	raw, _ := json.Marshal(res.StructuredContent)
	var out Out
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode structured output: %v (%s)", err, raw)
	}
	return out
}

func TestInjectSealStats(t *testing.T) {
	cs := newTestSession(t)

	inj := call[struct {
		NodesApplied int `json:"nodesApplied"`
		EdgesApplied int `json:"edgesApplied"`
	}](t, cs, "inject_batch", map[string]any{
		"nodes": []map[string]any{
			{"key": "gateway", "label": "Service"},
			{"key": "auth", "label": "Service"},
			{"key": "db", "label": "Storage"},
		},
		"edges": []map[string]any{
			{"srcKey": "gateway", "dstKey": "auth", "label": "CALLS"},
			{"srcKey": "auth", "dstKey": "db", "label": "QUERY"},
		},
	})
	if inj.NodesApplied != 3 || inj.EdgesApplied != 2 {
		t.Fatalf("inject: %+v", inj)
	}

	st := call[struct {
		Nodes int `json:"nodes"`
		Edges int `json:"edges"`
		SCCs  int `json:"sccs"`
	}](t, cs, "seal", nil)
	if st.Nodes != 3 || st.Edges != 2 || st.SCCs != 3 {
		t.Fatalf("seal: %+v", st)
	}

	st2 := call[struct {
		Nodes int `json:"nodes"`
	}](t, cs, "stats", nil)
	if st2.Nodes != 3 {
		t.Fatalf("stats: %+v", st2)
	}
}

func TestNeighborsAndBFS(t *testing.T) {
	cs := newTestSession(t)
	call[any](t, cs, "inject_batch", map[string]any{
		"nodes": []map[string]any{
			{"key": "a"}, {"key": "b"}, {"key": "c"}, {"key": "d"},
		},
		"edges": []map[string]any{
			{"srcKey": "a", "dstKey": "b", "label": "CALLS"},
			{"srcKey": "b", "dstKey": "c", "label": "CALLS"},
			{"srcKey": "c", "dstKey": "d", "label": "DATA"},
		},
	})
	call[any](t, cs, "seal", nil)

	nb := call[struct {
		Nodes []string `json:"nodes"`
	}](t, cs, "neighbors", map[string]any{"key": "a", "direction": "out"})
	if len(nb.Nodes) != 1 || nb.Nodes[0] != "b" {
		t.Fatalf("neighbors: %+v", nb)
	}

	// 2 hops: a->b->c; with labels restricted to CALLS, d (DATA) is unreachable
	bfs := call[struct {
		Count int      `json:"count"`
		Nodes []string `json:"nodes"`
	}](t, cs, "bfs", map[string]any{
		"from":     []string{"a"},
		"maxDepth": 2,
		"labels":   []string{"CALLS"},
	})
	if bfs.Count != 3 { // a, b, c
		t.Fatalf("bfs count = %d: %+v", bfs.Count, bfs.Nodes)
	}

	// result capping: limit=2 returns only 2 nodes but total reflects the true count
	lim := call[struct {
		Count int      `json:"count"`
		Total int      `json:"total"`
		Nodes []string `json:"nodes"`
	}](t, cs, "bfs", map[string]any{"from": []string{"a"}, "limit": 2})
	if lim.Total != 4 || len(lim.Nodes) != 2 {
		t.Fatalf("limit: total=%d returned=%d", lim.Total, len(lim.Nodes))
	}
}

func TestShortestPathReachableCycles(t *testing.T) {
	cs := newTestSession(t)
	call[any](t, cs, "inject_batch", map[string]any{
		"nodes": []map[string]any{{"key": "x"}, {"key": "y"}, {"key": "z"}, {"key": "w"}},
		"edges": []map[string]any{
			{"srcKey": "x", "dstKey": "y"},
			{"srcKey": "y", "dstKey": "z"},
			{"srcKey": "z", "dstKey": "y"}, // cycle y<->z
			{"srcKey": "w", "dstKey": "x"},
		},
	})
	call[any](t, cs, "seal", nil)

	p := call[struct {
		Found bool     `json:"found"`
		Hops  int      `json:"hops"`
		Nodes []string `json:"nodes"`
	}](t, cs, "shortest_path", map[string]any{"from": "x", "to": "z"})
	if !p.Found || p.Hops != 2 {
		t.Fatalf("path: %+v", p)
	}

	r := call[struct {
		Count int      `json:"count"`
		Nodes []string `json:"nodes"`
	}](t, cs, "reachable", map[string]any{"from": "w"})
	if r.Count != 4 {
		t.Fatalf("reachable: %+v", r)
	}

	cy := call[struct {
		Cycles [][]string `json:"cycles"`
	}](t, cs, "cycles", nil)
	if len(cy.Cycles) != 1 || len(cy.Cycles[0]) != 2 {
		t.Fatalf("cycles: %+v", cy.Cycles)
	}
}

func TestQueryBeforeSealIsHelpfulError(t *testing.T) {
	cs := newTestSession(t)
	call[any](t, cs, "inject_batch", map[string]any{
		"nodes": []map[string]any{{"key": "a"}},
	})
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "neighbors", Arguments: map[string]any{"key": "a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError {
		t.Fatal("query before seal should return a tool error")
	}
	// the error message should hint at seal
	hint := ""
	if len(res.Content) > 0 {
		if tc, ok := res.Content[0].(*mcp.TextContent); ok {
			hint = tc.Text
		}
	}
	if !strings.Contains(hint, "seal") {
		t.Errorf("error hint should contain seal: %q", hint)
	}
}

func TestLookupProperty(t *testing.T) {
	cs := newTestSession(t)
	call[any](t, cs, "inject_batch", map[string]any{
		"nodes": []map[string]any{
			{"key": "a", "label": "Service", "props": map[string]any{"env": "prod"}},
			{"key": "b", "label": "Service", "props": map[string]any{"env": "dev"}},
		},
		"edges": []map[string]any{{"srcKey": "a", "dstKey": "b"}},
	})
	call[any](t, cs, "seal", nil)
	nodes := call[struct {
		Nodes []string `json:"nodes"`
	}](t, cs, "lookup_property", map[string]any{"propKey": "env", "value": "prod"})
	// the MCP service auto-registers property keys seen during injection when sealing
	if len(nodes.Nodes) != 1 || nodes.Nodes[0] != "a" {
		t.Fatalf("lookup_property: %+v", nodes.Nodes)
	}
}
