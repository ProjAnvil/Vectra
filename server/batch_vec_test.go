package server

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/projanvil/vectra"
)

// vecOpDecl is the canonical test declaration (Dim 4 matches the fixtures).
func vecOpDecl(name string) vectra.VectorIndexDecl {
	return vectra.VectorIndexDecl{Name: name, Prop: "emb", Dim: 4, Metric: "cosine"}
}

// vecDeclsOf reads the live graph's declarations under the fsm lock (the
// harness's read discipline around f.live).
func vecDeclsOf(f *fsm) []vectra.VectorIndexDecl {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.live.VectorIndexes()
}

// TestVectorIndexOpsApply exercises the raft replay path for explicit vector
// index declaration ops: create (with same-batch node ingestion via the
// ops-first order), drop (index gone, queries fail), and the exactly-one-set
// op shape validation (rejected before any mutation).
func TestVectorIndexOpsApply(t *testing.T) {
	f := newFSM(time.Hour)

	// Create + changeset in one entry: the op applies before the changeset
	// (ops-first, the writer-lane precedent), so the batch's own nodes
	// ingest through live intake and are queryable immediately.
	create := vecOpDecl("mem")
	res := f.Apply(&raft.Log{Index: 1, Data: mustGob(t, Batch{
		Version:        1,
		VectorIndexOps: []VectorIndexOp{{Create: &create}},
		Nodes: []NodeUpsert{
			{Key: "doc1", Label: "Doc", Props: map[string]any{"emb": []float32{1, 0, 0, 0}}},
			{Key: "doc2", Label: "Doc", Props: map[string]any{"emb": []float32{0, 1, 0, 0}}},
		},
	})})
	if r, ok := res.(ApplyResult); !ok || !r.Committed {
		t.Fatalf("create apply = %+v (%T), want committed ApplyResult", res, res)
	}
	decls := vecDeclsOf(f)
	if len(decls) != 1 || decls[0] != create {
		t.Fatalf("decls after create = %+v, want [mem]", decls)
	}
	f.mu.RLock()
	hits, err := f.live.QueryVector("mem", []float32{1, 0, 0, 0}, vectra.QueryVectorOptions{K: 2})
	f.mu.RUnlock()
	if err != nil {
		t.Fatalf("QueryVector after create: %v", err)
	}
	if len(hits) != 2 || hits[0].Key != "doc1" {
		t.Fatalf("hits = %+v, want doc1 first, 2 total (same-batch nodes ingested)", hits)
	}

	// Drop in its own entry: the declaration disappears and queries fail
	// with the engine's not-found error.
	res = f.Apply(&raft.Log{Index: 2, Data: mustGob(t, Batch{
		Version:        1,
		VectorIndexOps: []VectorIndexOp{{Drop: "mem"}},
	})})
	if r, ok := res.(ApplyResult); !ok || !r.Committed {
		t.Fatalf("drop apply = %+v (%T), want committed ApplyResult", res, res)
	}
	if decls := vecDeclsOf(f); len(decls) != 0 {
		t.Fatalf("decls after drop = %+v, want none", decls)
	}
	f.mu.RLock()
	_, err = f.live.QueryVector("mem", []float32{1, 0, 0, 0}, vectra.QueryVectorOptions{})
	f.mu.RUnlock()
	if !errors.Is(err, vectra.ErrVectorIndexNotFound) {
		t.Fatalf("QueryVector after drop = %v, want ErrVectorIndexNotFound", err)
	}

	// Malformed ops (neither set / both set) fail the entry with a clear
	// error BEFORE any mutation — the declaration set is untouched.
	for i, ops := range [][]VectorIndexOp{
		{{}},                             // neither set
		{{Create: &create, Drop: "mem"}}, // both set
	} {
		res := f.Apply(&raft.Log{Index: uint64(3 + i), Data: mustGob(t, Batch{Version: 1, VectorIndexOps: ops})})
		err, ok := res.(error)
		if !ok {
			t.Fatalf("invalid op %d: apply = %+v (%T), want error", i, res, res)
		}
		if !strings.Contains(err.Error(), "vectorIndexOps") || !strings.Contains(err.Error(), "exactly one of create/drop") {
			t.Fatalf("invalid op %d: error %q lacks the expected context", i, err)
		}
		if decls := vecDeclsOf(f); len(decls) != 0 {
			t.Fatalf("invalid op %d mutated decls: %+v", i, decls)
		}
	}
}

// TestVectorIndexOpExactlyOneSet pins the op shape rule at the unit level:
// exactly one of Create/Drop must be set — nil Create + empty Drop is
// invalid (NOT a no-op), and both set is invalid.
func TestVectorIndexOpExactlyOneSet(t *testing.T) {
	create := vecOpDecl("mem")
	tests := []struct {
		name string
		op   VectorIndexOp
		want bool
	}{
		{"create only", VectorIndexOp{Create: &create}, true},
		{"drop only", VectorIndexOp{Drop: "mem"}, true},
		{"neither", VectorIndexOp{}, false},
		{"both", VectorIndexOp{Create: &create, Drop: "mem"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.op.exactlyOneSet(); got != tt.want {
				t.Errorf("exactlyOneSet(%+v) = %v, want %v", tt.op, got, tt.want)
			}
		})
	}
}
