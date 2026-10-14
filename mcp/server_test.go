package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
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
	}](t, cs, "lookup_property", map[string]any{"propKey": "env", "propValue": "prod"})
	// the MCP service auto-registers property keys seen during injection when sealing
	if len(nodes.Nodes) != 1 || nodes.Nodes[0] != "a" {
		t.Fatalf("lookup_property: %+v", nodes.Nodes)
	}
}

func TestInjectDeleteSeal(t *testing.T) {
	cs := newTestSession(t)
	call[struct{}](t, cs, "inject_batch", map[string]any{
		"nodes": []map[string]any{{"key": "a"}, {"key": "b"}},
		"edges": []map[string]any{{"srcKey": "a", "dstKey": "b", "label": "E"}},
	})
	out := call[struct {
		EdgesDeleted int `json:"edgesDeleted"`
		NodesDeleted int `json:"nodesDeleted"`
	}](t, cs, "inject_batch", map[string]any{
		"deleteEdges": []map[string]any{{"srcKey": "a", "dstKey": "b", "label": "E"}},
		"deleteNodes": []string{"b"},
	})
	if out.EdgesDeleted != 1 || out.NodesDeleted != 1 {
		t.Fatalf("out = %+v", out)
	}
	call[struct{}](t, cs, "seal", map[string]any{})
	st := call[struct {
		Nodes int `json:"nodes"`
		Edges int `json:"edges"`
	}](t, cs, "stats", map[string]any{})
	if st.Nodes != 1 || st.Edges != 0 {
		t.Fatalf("stats = %+v, want 1/0", st)
	}
}

// newSnapshotSession starts a server in -snapshot mode: the README's
// bulk-import → SaveFile → vectra-mcp -snapshot flow. The snapshot is built
// with an explicit property index so lookup_property should work without any
// re-injection.
func newSnapshotSession(t *testing.T) (*mcp.ClientSession, string) {
	t.Helper()
	g := vectra.New()
	_, _ = g.AddNode("orderctl", "Class", map[string]any{"stereotype": "controller"})
	_, _ = g.AddNode("orderrepo", "Class", map[string]any{"stereotype": "repository"})
	_, _ = g.AddEdge("orderctl", "orderrepo", "DEPENDS_ON", nil)
	g.IndexProperty("stereotype")
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "deps.vg")
	if err := g.SaveFile(path); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := New(vectra.New(), Options{SnapshotPath: path})
	ct, st := mcp.NewInMemoryTransports()
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0"}, nil)
	go func() { _ = s.Run(ctx, st) }()
	cs, err := client.Connect(ctx, ct, nil)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cs.Close()
		cancel()
	})
	return cs, path
}

func TestSnapshotModeQueryAndLookup(t *testing.T) {
	cs, _ := newSnapshotSession(t)

	st := call[struct {
		Nodes int `json:"nodes"`
		Edges int `json:"edges"`
	}](t, cs, "stats", nil)
	if st.Nodes != 2 || st.Edges != 1 {
		t.Fatalf("stats from snapshot: %+v, want 2/1", st)
	}

	// lookup_property must work from the snapshot's persisted index
	// declarations without any re-injection (P1 regression test).
	nodes := call[struct {
		Nodes []string `json:"nodes"`
	}](t, cs, "lookup_property", map[string]any{"propKey": "stereotype", "propValue": "controller"})
	if len(nodes.Nodes) != 1 || nodes.Nodes[0] != "orderctl" {
		t.Fatalf("lookup_property from snapshot: %+v", nodes.Nodes)
	}
}

func TestInjectUpsertEdgeCollapses(t *testing.T) {
	cs := newTestSession(t)
	call[struct{}](t, cs, "inject_batch", map[string]any{
		"nodes": []map[string]any{{"key": "a"}, {"key": "b"}, {"key": "c"}},
		"edges": []map[string]any{
			{"srcKey": "a", "dstKey": "b", "label": "E", "props": map[string]any{"w": 1}},
			{"srcKey": "a", "dstKey": "b", "label": "E", "props": map[string]any{"w": 2}},
			{"srcKey": "b", "dstKey": "c", "label": "E"},
		},
	})
	call[struct{}](t, cs, "seal", map[string]any{})
	st := call[struct {
		Nodes int `json:"nodes"`
		Edges int `json:"edges"`
	}](t, cs, "stats", map[string]any{})
	if st.Nodes != 3 || st.Edges != 2 { // a->b collapsed to one
		t.Fatalf("stats = %+v, want 3/2", st)
	}
}

func TestInjectParallelEdges(t *testing.T) {
	cs := newTestSession(t)
	call[struct{}](t, cs, "inject_batch", map[string]any{
		"nodes": []map[string]any{{"key": "a"}, {"key": "b"}},
		"edges": []map[string]any{
			{"srcKey": "a", "dstKey": "b", "label": "DEPENDS_ON",
				"props": map[string]any{"via": "constructor"}, "allowParallel": true},
			{"srcKey": "a", "dstKey": "b", "label": "DEPENDS_ON",
				"props": map[string]any{"via": "setter"}, "allowParallel": true},
		},
	})
	call[struct{}](t, cs, "seal", map[string]any{})
	st := call[struct {
		Edges int `json:"edges"`
	}](t, cs, "stats", map[string]any{})
	if st.Edges != 2 { // both parallel edges survive
		t.Fatalf("stats = %+v, want 2 edges", st)
	}
}

// TestSealThenReinjectContinues: "re-inject to write again" (the seal tool's
// promise). After a seal, the first inject_batch re-opens a writable copy of
// the published state; until the next seal, queries keep serving the old
// published view (bounded-stale, same read model as the server module).
func TestSealThenReinjectContinues(t *testing.T) {
	cs := newTestSession(t)
	stats := func() struct {
		Nodes int `json:"nodes"`
		Edges int `json:"edges"`
	} {
		return call[struct {
			Nodes int `json:"nodes"`
			Edges int `json:"edges"`
		}](t, cs, "stats", nil)
	}

	call[any](t, cs, "inject_batch", map[string]any{
		"nodes": []map[string]any{{"key": "a"}, {"key": "b"}},
		"edges": []map[string]any{{"srcKey": "a", "dstKey": "b", "label": "E"}},
	})
	call[any](t, cs, "seal", nil)
	if st := stats(); st.Nodes != 2 || st.Edges != 1 {
		t.Fatalf("first seal: %+v, want 2/1", st)
	}

	inj := call[struct {
		NodesApplied int `json:"nodesApplied"`
	}](t, cs, "inject_batch", map[string]any{
		"nodes": []map[string]any{{"key": "c"}},
		"edges": []map[string]any{{"srcKey": "b", "dstKey": "c", "label": "E"}},
	})
	if inj.NodesApplied != 1 {
		t.Fatalf("re-inject after seal applied %d nodes, want 1 (writes must not be silently dropped)", inj.NodesApplied)
	}
	if st := stats(); st.Nodes != 2 {
		t.Fatalf("pre-reseal stats = %+v, want the old published view (2 nodes, bounded-stale reads)", st)
	}

	call[any](t, cs, "seal", nil)
	if st := stats(); st.Nodes != 3 || st.Edges != 2 {
		t.Fatalf("post-reseal stats = %+v, want 3/2 (re-open must carry the published state forward)", st)
	}
}

// ---- Streamable HTTP mode (Handler) ----

// newHTTPSession connects one MCP client session to an httptest-served Handler.
func newHTTPSession(t *testing.T, url string) *mcp.ClientSession {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0"}, nil)
	cs, err := client.Connect(ctx, &mcp.StreamableClientTransport{
		Endpoint: url, DisableStandaloneSSE: true,
	}, nil)
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

// TestHTTPHandlerSessionsShareState: the Handler must serve every session
// from ONE embedded service — a session sealed by client A must be visible to
// client B. Per-session services share the live graph without sharing the
// mutex (the old bug): B would race A's writes and never see A's sealed view.
func TestHTTPHandlerSessionsShareState(t *testing.T) {
	ts := httptest.NewServer(Handler(vectra.New(), Options{}))
	defer ts.Close()
	url := ts.URL + "/mcp"

	a := newHTTPSession(t, url)
	call[any](t, a, "inject_batch", map[string]any{
		"nodes": []map[string]any{{"key": "a"}, {"key": "b"}},
	})
	call[any](t, a, "seal", nil)

	b := newHTTPSession(t, url)
	st := call[struct {
		Nodes int `json:"nodes"`
		Edges int `json:"edges"`
	}](t, b, "stats", nil)
	if st.Nodes != 2 {
		t.Fatalf("session B sees %d nodes, want 2 (sessions must share the sealed view)", st.Nodes)
	}
}

// TestHTTPHandlerConcurrentInjects: two sessions injecting simultaneously
// must serialize through the shared service's mutex (run with -race to catch
// the per-session-mutex regression: both wrote the shared live graph).
func TestHTTPHandlerConcurrentInjects(t *testing.T) {
	ts := httptest.NewServer(Handler(vectra.New(), Options{}))
	defer ts.Close()
	url := ts.URL + "/mcp"

	a := newHTTPSession(t, url)
	b := newHTTPSession(t, url)

	const batches = 25
	var wg sync.WaitGroup
	inject := func(cs *mcp.ClientSession, prefix string) {
		defer wg.Done()
		for i := 0; i < batches; i++ {
			res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{
				Name: "inject_batch",
				Arguments: map[string]any{
					"nodes": []map[string]any{{"key": fmt.Sprintf("%s-%d", prefix, i)}},
				},
			})
			if err != nil || res.IsError {
				t.Errorf("inject %s-%d failed: err=%v isError=%v", prefix, i, err, res.IsError)
				return
			}
		}
	}
	wg.Add(2)
	go inject(a, "a")
	go inject(b, "b")
	wg.Wait()

	call[any](t, a, "seal", nil)
	st := call[struct {
		Nodes int `json:"nodes"`
	}](t, b, "stats", nil)
	if st.Nodes != 2*batches {
		t.Fatalf("nodes = %d, want %d (concurrent injects must all land)", st.Nodes, 2*batches)
	}
}
