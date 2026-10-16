package server

import (
	"bytes"
	"encoding/gob"
	"testing"

	"github.com/projanvil/vectra"
)

// flatNodeUpsert & co mirror the pre-0.7 server item types exactly (same
// field names, locally defined types). gob matches structurally by field
// name, so raft logs written by old binaries decode into the new Batch
// (whose item types are aliases of the core vectra ones) and vice versa —
// the two-directional wire-compatibility contract of the changeset uplift.
type flatNodeUpsert struct {
	Key   string
	Label string
	Props map[string]any
}

type flatEdgeUpsert struct {
	SrcKey        string
	DstKey        string
	Label         string
	Props         map[string]any
	AllowParallel bool
}

type flatEdgeDelete struct {
	SrcKey string
	DstKey string
	Label  string
}

type flatBatch struct {
	Version     int
	Nodes       []flatNodeUpsert
	Edges       []flatEdgeUpsert
	EdgeDeletes []flatEdgeDelete
	NodeDeletes []string
	Txn         *TxnGuard
}

// fullBatch fills every changeset field plus Txn.
func fullBatch() Batch {
	return Batch{
		Version: 3,
		Nodes:   []NodeUpsert{{Key: "a", Label: "Svc", Props: map[string]any{"w": 2}}},
		Edges: []EdgeUpsert{
			{SrcKey: "a", DstKey: "b", Label: "CALLS", Props: map[string]any{"w": 1}},
			{SrcKey: "a", DstKey: "b", Label: "CALLS", AllowParallel: true},
		},
		EdgeDeletes: []EdgeDelete{{SrcKey: "b", DstKey: "c", Label: "USES"}},
		NodeDeletes: []string{"gone"},
		Txn: &TxnGuard{
			Snapshot:  7,
			SER:       true,
			ReadNodes: []string{"a"},
			ReadEdges: []EdgeRef{{Src: "a", Dst: "b", Label: "CALLS"}},
		},
	}
}

// TestBatchGobLegacyRoundtrip asserts the wire-compatibility contract of the
// changeset uplift in both directions against the pre-0.7 flat shape:
// old-encoded frames decode into the new Batch with every field (incl. Txn)
// intact, and new-encoded frames decode into the old flat struct.
func TestBatchGobLegacyRoundtrip(t *testing.T) {
	// Old writer -> new reader.
	var old bytes.Buffer
	oldB := flatBatch{
		Version: 3,
		Nodes:   []flatNodeUpsert{{Key: "a", Label: "Svc", Props: map[string]any{"w": 2}}},
		Edges: []flatEdgeUpsert{
			{SrcKey: "a", DstKey: "b", Label: "CALLS", Props: map[string]any{"w": 1}},
			{SrcKey: "a", DstKey: "b", Label: "CALLS", AllowParallel: true},
		},
		EdgeDeletes: []flatEdgeDelete{{SrcKey: "b", DstKey: "c", Label: "USES"}},
		NodeDeletes: []string{"gone"},
		Txn: &TxnGuard{
			Snapshot:  7,
			SER:       true,
			ReadNodes: []string{"a"},
			ReadEdges: []EdgeRef{{Src: "a", Dst: "b", Label: "CALLS"}},
		},
	}
	if err := gob.NewEncoder(&old).Encode(oldB); err != nil {
		t.Fatal(err)
	}
	got, err := decodeBatch(old.Bytes())
	if err != nil {
		t.Fatalf("decode legacy flat frame into new Batch: %v", err)
	}
	want := fullBatch()
	if got.Version != want.Version || len(got.Nodes) != 1 || got.Nodes[0].Key != "a" || got.Nodes[0].Props["w"] != 2 {
		t.Fatalf("nodes = %+v", got.Nodes)
	}
	if len(got.Edges) != 2 || !got.Edges[1].AllowParallel || got.Edges[0].Props["w"] != 1 {
		t.Fatalf("edges = %+v", got.Edges)
	}
	if len(got.EdgeDeletes) != 1 || got.EdgeDeletes[0].Label != "USES" {
		t.Fatalf("edgeDeletes = %+v", got.EdgeDeletes)
	}
	if len(got.NodeDeletes) != 1 || got.NodeDeletes[0] != "gone" {
		t.Fatalf("nodeDeletes = %+v", got.NodeDeletes)
	}
	if got.Txn == nil || got.Txn.Snapshot != 7 || !got.Txn.SER || len(got.Txn.ReadEdges) != 1 {
		t.Fatalf("txn = %+v", got.Txn)
	}

	// New writer -> old reader.
	data, err := encodeBatch(want)
	if err != nil {
		t.Fatal(err)
	}
	var back flatBatch
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&back); err != nil {
		t.Fatalf("decode new frame into flat legacy struct: %v", err)
	}
	if back.Version != 3 || len(back.Nodes) != 1 || back.Nodes[0].Key != "a" ||
		len(back.Edges) != 2 || back.Edges[1].AllowParallel != true ||
		len(back.EdgeDeletes) != 1 || len(back.NodeDeletes) != 1 || back.Txn.Snapshot != 7 {
		t.Fatalf("legacy decode of new batch = %+v", back)
	}
}

// TestBatchChangesetOf pins the compile-time identity between Batch fields
// and the core changeset: the item types are aliases, so the projection is
// assignment-only (no per-field copying or conversion).
func TestBatchChangesetOf(t *testing.T) {
	b := fullBatch()
	cs := b.ChangesetOf()
	var nodes []vectra.NodeUpsert = cs.Nodes
	var edges []vectra.EdgeUpsert = cs.Edges
	var dels []vectra.EdgeDelete = cs.EdgeDeletes
	if cs.Version != 3 || len(nodes) != 1 || len(edges) != 2 || len(dels) != 1 || len(cs.NodeDeletes) != 1 {
		t.Fatalf("ChangesetOf = %+v", cs)
	}
	b2 := Batch{Version: cs.Version, Nodes: nodes, Edges: edges, EdgeDeletes: dels, NodeDeletes: cs.NodeDeletes}
	if b2.Txn != nil || len(b2.Nodes) != 1 || !b2.Edges[1].AllowParallel {
		t.Fatalf("Batch rebuilt from Changeset = %+v", b2)
	}
}
