package store_test

import (
	"bytes"
	"testing"

	"vectra/internal/store"
)

func TestEncodeDecodeRoundtrip(t *testing.T) {
	var buf bytes.Buffer
	nodes := []store.Node{
		{Key: "gw", Label: "Service", Props: map[string]any{"team": "core", "qps": float64(1200)}},
		{Key: "db", Label: "Storage"},
	}
	edges := []store.Edge{
		{SrcKey: "gw", DstKey: "db", Label: "QUERY", Props: map[string]any{"mode": "read"}},
		{SrcKey: "db", DstKey: "gw", Label: "EVENT"},
	}
	if err := store.Encode(&buf, nodes, edges); err != nil {
		t.Fatal(err)
	}
	gotNodes, gotEdges, err := store.Decode(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if len(gotNodes) != 2 || len(gotEdges) != 2 {
		t.Fatalf("counts = %d/%d, want 2/2", len(gotNodes), len(gotEdges))
	}
	if gotNodes[0].Key != "gw" || gotNodes[0].Label != "Service" || gotNodes[0].Props["team"] != "core" || gotNodes[0].Props["qps"] != float64(1200) {
		t.Errorf("node0 = %+v", gotNodes[0])
	}
	if gotNodes[1].Props != nil {
		t.Errorf("node1 props = %v, want nil", gotNodes[1].Props)
	}
	if gotEdges[0].SrcKey != "gw" || gotEdges[0].DstKey != "db" || gotEdges[0].Label != "QUERY" || gotEdges[0].Props["mode"] != "read" {
		t.Errorf("edge0 = %+v", gotEdges[0])
	}
	if gotEdges[1].Props != nil {
		t.Errorf("edge1 props = %v, want nil", gotEdges[1].Props)
	}
}

func TestDecodeRejectsBadMagic(t *testing.T) {
	if _, _, err := store.Decode(bytes.NewReader([]byte("NOPE"))); err == nil {
		t.Fatal("expected error for bad magic")
	}
}

func TestDecodeRejectsTruncated(t *testing.T) {
	var buf bytes.Buffer
	_ = store.Encode(&buf, []store.Node{{Key: "a", Label: "L"}}, nil)
	if _, _, err := store.Decode(bytes.NewReader(buf.Bytes()[:5])); err == nil {
		t.Fatal("expected error for truncated input")
	}
}

func TestUnicodeKeysRoundtrip(t *testing.T) {
	var buf bytes.Buffer
	nodes := []store.Node{{Key: "服务-总账", Label: "Svc"}}
	if err := store.Encode(&buf, nodes, nil); err != nil {
		t.Fatal(err)
	}
	got, _, err := store.Decode(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Key != "服务-总账" {
		t.Errorf("key = %q", got[0].Key)
	}
}

func TestEmptyGraphRoundtrip(t *testing.T) {
	var buf bytes.Buffer
	if err := store.Encode(&buf, nil, nil); err != nil {
		t.Fatal(err)
	}
	nodes, edges, err := store.Decode(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 0 || len(edges) != 0 {
		t.Errorf("counts = %d/%d, want 0/0", len(nodes), len(edges))
	}
}
