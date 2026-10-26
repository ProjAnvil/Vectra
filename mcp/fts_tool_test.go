package mcpserver

import (
	"context"
	"path/filepath"
	"slices"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/projanvil/vectra"
)

// newFTSSessionAt starts a server in -snapshot mode over an existing file
// (newSnapshotSession's shape, parameterized by path).
func newFTSSessionAt(t *testing.T, path string) *mcp.ClientSession {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	s := New(vectra.New(), Options{SnapshotPath: path})
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

// TestLookupContainsTool: declare a full-text index through the tool, inject
// nodes through inject_batch, and read them back with lookup_contains —
// before seal (the pending declaration answers via scan, R14) and after it
// (the gram buckets answer). The needle "AUTH" vs value "Authentication
// boundary" pins the case-insensitive contract.
func TestLookupContainsTool(t *testing.T) {
	cs := newTestSession(t)

	decl := call[struct {
		PropKey string `json:"propKey"`
	}](t, cs, "index_fulltext", map[string]any{"propKey": "desc"})
	if decl.PropKey != "desc" {
		t.Fatalf("index_fulltext echo: %+v", decl)
	}
	call[struct{}](t, cs, "inject_batch", map[string]any{
		"nodes": []map[string]any{
			{"key": "gateway", "props": map[string]any{"desc": "API Gateway for ingress"}},
			{"key": "auth", "props": map[string]any{"desc": "Authentication boundary"}},
			{"key": "db", "props": map[string]any{"desc": "Postgres primary"}},
		},
	})

	// pre-seal: the declaration is pending, the lookup still answers
	out := call[struct {
		Count int      `json:"count"`
		Total int      `json:"total"`
		Nodes []string `json:"nodes"`
	}](t, cs, "lookup_contains", map[string]any{"propKey": "desc", "contains": "AUTH"})
	if out.Total != 1 || out.Count != 1 || out.Nodes[0] != "auth" {
		t.Fatalf("lookup_contains (pre-seal): %+v", out)
	}

	// post-seal: the gram buckets answer the same query
	call[struct{}](t, cs, "seal", nil)
	out2 := call[struct {
		Count int      `json:"count"`
		Total int      `json:"total"`
		Nodes []string `json:"nodes"`
	}](t, cs, "lookup_contains", map[string]any{"propKey": "desc", "contains": "gateway"})
	if out2.Total != 1 || out2.Nodes[0] != "gateway" {
		t.Fatalf("lookup_contains (post-seal): %+v", out2)
	}
}

// TestLookupContainsLimit pins the limit contract: count is what this call
// returned, total is the true match size, and the truncation keeps the
// first hits by node id.
func TestLookupContainsLimit(t *testing.T) {
	cs := newTestSession(t)
	call[struct{}](t, cs, "index_fulltext", map[string]any{"propKey": "desc"})
	var nodes []map[string]any
	for i := 0; i < 5; i++ {
		nodes = append(nodes, map[string]any{
			"key":   "f" + string(rune('0'+i)),
			"props": map[string]any{"desc": "sidecar proxy " + string(rune('0'+i))},
		})
	}
	call[struct{}](t, cs, "inject_batch", map[string]any{"nodes": nodes})
	call[struct{}](t, cs, "seal", nil)

	out := call[struct {
		Count int      `json:"count"`
		Total int      `json:"total"`
		Nodes []string `json:"nodes"`
	}](t, cs, "lookup_contains", map[string]any{"propKey": "desc", "contains": "SIDECAR", "limit": 2})
	if out.Count != 2 || out.Total != 5 {
		t.Fatalf("limit: count=%d total=%d, want 2/5", out.Count, out.Total)
	}
	if !slices.Equal(out.Nodes, []string{"f0", "f1"}) {
		t.Fatalf("limit: nodes=%v, want the first two by node id", out.Nodes)
	}
}

// TestLookupContainsValidation: an undeclared key is a loud error (never a
// silent empty success), an empty needle is not a search, and an empty
// propKey on the declaration side is a client error — the same mustErr
// shape as the vector tool's validation test.
func TestLookupContainsValidation(t *testing.T) {
	cs := newTestSession(t)
	call[struct{}](t, cs, "inject_batch", map[string]any{
		"nodes": []map[string]any{{"key": "a", "props": map[string]any{"desc": "alpha"}}},
	})
	ctx := context.Background()
	mustErr := func(tool string, args map[string]any) {
		res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: args})
		if err == nil && !res.IsError {
			t.Fatalf("expected error for %s %+v", tool, args)
		}
	}
	mustErr("lookup_contains", map[string]any{"propKey": "desc", "contains": "alp"}) // undeclared key
	call[struct{}](t, cs, "index_fulltext", map[string]any{"propKey": "desc"})
	mustErr("lookup_contains", map[string]any{"propKey": "desc", "contains": ""}) // empty needle
	mustErr("index_fulltext", map[string]any{"propKey": ""})                      // empty propKey
}

// TestSnapshotRestoresFTSDeclarations pins the reload ruling: a snapshot's
// FTS declarations come back as FTS (re-declared through IndexFullText),
// never as typed auto-indexes — and they survive the next seal's save.
func TestSnapshotRestoresFTSDeclarations(t *testing.T) {
	g := vectra.New()
	_, _ = g.AddNode("orderctl", "Class", map[string]any{"stereotype": "controller", "desc": "orchestrates orders"})
	_, _ = g.AddNode("orderrepo", "Class", map[string]any{"stereotype": "repository", "desc": "persists order rows"})
	g.IndexProperty("stereotype")
	g.IndexFullText("desc")
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "deps.vg")
	if err := g.SaveFile(path); err != nil {
		t.Fatal(err)
	}

	// service level: FTS keys restore as FTS; typed keys keep feeding the
	// (typed-only) seal-time auto-index — FTS keys never join propKeys.
	svc := newService(vectra.New(), Options{SnapshotPath: path})
	if got := svc.live.FullTextIndexedKeys(); len(got) != 1 || got[0] != "desc" {
		t.Fatalf("FullTextIndexedKeys = %v, want [desc]", got)
	}
	if got := svc.live.IndexedPropertyKeys(); len(got) != 1 || got[0] != "stereotype" {
		t.Fatalf("IndexedPropertyKeys = %v, want [stereotype] (no typed auto-declare over FTS keys)", got)
	}
	if svc.propKeys["desc"] || !svc.propKeys["stereotype"] {
		t.Fatalf("propKeys = %v: FTS keys must not feed typed auto-indexing", svc.propKeys)
	}

	// behavioral, through a real session: lookup_contains works right after
	// load, while the typed kind was NOT auto-declared over the FTS key.
	cs := newFTSSessionAt(t, path)
	out := call[struct {
		Count int      `json:"count"`
		Nodes []string `json:"nodes"`
	}](t, cs, "lookup_contains", map[string]any{"propKey": "desc", "contains": "ORDER"})
	if out.Count != 2 || !slices.Contains(out.Nodes, "orderctl") || !slices.Contains(out.Nodes, "orderrepo") {
		t.Fatalf("lookup_contains from snapshot: %+v", out)
	}
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "lookup_property",
		Arguments: map[string]any{"propKey": "desc", "propValue": "orchestrates orders"},
	})
	if err == nil && !res.IsError {
		t.Fatal("lookup_property on an FTS-only key must stay not-indexed")
	}

	// seal re-saves the snapshot; the FTS declarations survive the round-trip
	call[struct{}](t, cs, "seal", nil)
	cs2 := newFTSSessionAt(t, path)
	out2 := call[struct {
		Count int      `json:"count"`
		Nodes []string `json:"nodes"`
	}](t, cs2, "lookup_contains", map[string]any{"propKey": "desc", "contains": "persists"})
	if out2.Count != 1 || out2.Nodes[0] != "orderrepo" {
		t.Fatalf("lookup_contains after seal round-trip: %+v", out2)
	}
}
