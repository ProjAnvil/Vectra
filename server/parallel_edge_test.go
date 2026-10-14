package server

import (
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/projanvil/vectra"
)

// TestFSMApplyParallelEdges: AllowParallel=true appends true parallel edges
// (embedded AddEdge semantics) instead of collapsing onto the (src,dst,label)
// upsert key — multiple semantically distinct edges between the same pair
// (e.g. several injection points) must all survive.
func TestFSMApplyParallelEdges(t *testing.T) {
	f := newFSM(50 * time.Millisecond)
	defer f.Close()

	base := Batch{Version: batchVersion, Nodes: []NodeUpsert{
		{Key: "a", Label: "Svc"}, {Key: "b", Label: "Svc"},
	}}
	f.Apply(&raft.Log{Data: mustGob(t, base)})

	par := Batch{Version: batchVersion, Edges: []EdgeUpsert{
		{SrcKey: "a", DstKey: "b", Label: "DEPENDS_ON", Props: map[string]any{"via": "constructor"}, AllowParallel: true},
		{SrcKey: "a", DstKey: "b", Label: "DEPENDS_ON", Props: map[string]any{"via": "setter"}, AllowParallel: true},
	}}
	r := f.Apply(&raft.Log{Data: mustGob(t, par)}).(ApplyResult)
	if r.EdgesApplied != 2 {
		t.Fatalf("edges applied = %d, want 2", r.EdgesApplied)
	}
	f.mu.RLock()
	es, err := f.live.EdgesOf("a", vectra.DirOut)
	f.mu.RUnlock()
	if err != nil {
		t.Fatal(err)
	}
	if len(es) != 2 {
		t.Fatalf("live parallel edges = %d, want 2", len(es))
	}
	vias := map[string]bool{}
	for _, e := range es {
		vias[e.Props["via"].(string)] = true
	}
	if !vias["constructor"] || !vias["setter"] {
		t.Fatalf("parallel edge props merged: %v", vias)
	}

	// default (AllowParallel=false) keeps last-write-wins collapse
	collapse := Batch{Version: batchVersion, Edges: []EdgeUpsert{
		{SrcKey: "a", DstKey: "b", Label: "DEPENDS_ON", Props: map[string]any{"via": "ctor"}},
		{SrcKey: "a", DstKey: "b", Label: "DEPENDS_ON", Props: map[string]any{"via": "setter"}},
	}}
	f.Apply(&raft.Log{Data: mustGob(t, collapse)})
	f.mu.RLock()
	es, _ = f.live.EdgesOf("a", vectra.DirOut)
	f.mu.RUnlock()
	if len(es) != 1 || es[0].Props["via"] != "setter" {
		t.Fatalf("default upsert should collapse to last write: %d edges, via=%v", len(es), es[0].Props["via"])
	}
}

// TestBatchGobCarriesAllowParallel: the raft-log gob encoding must round-trip
// the new field (additive gob evolution).
func TestBatchGobCarriesAllowParallel(t *testing.T) {
	b := Batch{Version: batchVersion, Edges: []EdgeUpsert{
		{SrcKey: "a", DstKey: "b", Label: "E", AllowParallel: true},
	}}
	got, err := decodeBatch(mustGob(t, b))
	if err != nil {
		t.Fatal(err)
	}
	if !got.Edges[0].AllowParallel {
		t.Fatal("AllowParallel lost in gob round-trip")
	}
}
