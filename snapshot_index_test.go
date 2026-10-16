package vectra_test

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/projanvil/vectra"
)

// buildIndexedGraph is the SCV-style bulk-import flow: nodes with string
// props, an explicit index declaration, Seal.
func buildIndexedGraph(t *testing.T) *vectra.Graph {
	t.Helper()
	g := vectra.New()
	_, _ = g.AddNode("orderctl", "Class", map[string]any{"stereotype": "controller"})
	_, _ = g.AddNode("orderrepo", "Class", map[string]any{"stereotype": "repository"})
	_, _ = g.AddNode("web", "Class", map[string]any{"stereotype": "controller"})
	_, _ = g.AddEdge("orderctl", "orderrepo", "DEPENDS_ON", nil)
	g.IndexProperty("stereotype")
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	return g
}

// TestSaveLoadPreservesIndexDeclarations: SaveFile → LoadFile must round-trip
// the property-index declarations, so LookupByProperty works on the reloaded
// graph (regression: store v2 lost the declarations and every Load-based
// service — e.g. vectra-mcp -snapshot — lost lookup_property).
func TestSaveLoadPreservesIndexDeclarations(t *testing.T) {
	g := buildIndexedGraph(t)
	path := filepath.Join(t.TempDir(), "deps.vg")
	if err := g.SaveFile(path); err != nil {
		t.Fatal(err)
	}

	g2, err := vectra.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if keys := g2.IndexedPropertyKeys(); len(keys) != 1 || keys[0] != "stereotype" {
		t.Fatalf("reloaded index keys = %v, want [stereotype]", keys)
	}
	nodes, ok := g2.LookupByProperty("stereotype", "controller")
	if !ok || len(nodes) != 2 {
		t.Fatalf("LookupByProperty after reload: %d hits, ok=%v, want 2/true", len(nodes), ok)
	}
	if nodes[0].Key != "orderctl" || nodes[1].Key != "web" {
		t.Fatalf("hits = [%s %s], want [orderctl web]", nodes[0].Key, nodes[1].Key)
	}
}

// TestLoadV2SnapshotHasNoIndexDeclarations: v2 files never persisted index
// declarations — loading one yields a working graph with no indexes (the
// documented fallback, not an error).
func TestLoadV2SnapshotHasNoIndexDeclarations(t *testing.T) {
	// handcraft a minimal v2 snapshot: 1 node, 0 edges, no indexKey section
	body := []byte{'V', 'C', 'T', 'R', 2, 0}
	body = binary.LittleEndian.AppendUint32(body, 1) // 1 node
	body = binary.LittleEndian.AppendUint32(body, 0) // 0 edges
	body = binary.LittleEndian.AppendUint32(body, 4)
	body = append(body, 'n', 'o', 'd', 'e') // key "node"
	body = binary.LittleEndian.AppendUint32(body, 1)
	body = append(body, 'L')                         // label "L"
	body = binary.LittleEndian.AppendUint32(body, 0) // props nil
	body = binary.LittleEndian.AppendUint32(body, crc32.ChecksumIEEE(body))

	path := filepath.Join(t.TempDir(), "v2.vg")
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatal(err)
	}
	g, err := vectra.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if n, ok := g.NodeByKey("node"); !ok || n.Key != "node" {
		t.Fatalf("v2 graph content wrong: %+v ok=%v", n, ok)
	}
	if _, ok := g.LookupByProperty("any", "x"); ok {
		t.Fatal("v2 snapshot cannot restore indexes; LookupByProperty should report ok=false")
	}
}

// TestExportImportPreservesIndexDeclarations: the unsealed export path (the
// server FSM's snapshot/restore flow) must carry declarations too.
func TestExportImportPreservesIndexDeclarations(t *testing.T) {
	g := vectra.New()
	_, _ = g.AddNode("a", "Svc", map[string]any{"env": "prod"})
	_, _ = g.AddNode("b", "Svc", map[string]any{"env": "dev"})
	g.IndexProperty("env")
	data, err := g.ExportSnapshot() // unsealed export
	if err != nil {
		t.Fatal(err)
	}
	g2, err := vectra.ImportSnapshot(data)
	if err != nil {
		t.Fatal(err)
	}
	if keys := g2.IndexedPropertyKeys(); len(keys) != 1 || keys[0] != "env" {
		t.Fatalf("imported index keys = %v, want [env]", keys)
	}
	if err := g2.Seal(); err != nil {
		t.Fatal(err)
	}
	if nodes, ok := g2.LookupByProperty("env", "prod"); !ok || len(nodes) != 1 || nodes[0].Key != "a" {
		t.Fatalf("lookup after import+seal: %v ok=%v", nodes, ok)
	}
}
