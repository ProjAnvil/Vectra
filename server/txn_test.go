package server

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func txnBatch() Batch {
	return Batch{
		Version: 3,
		Nodes:   []NodeUpsert{{Key: "a", Label: "Svc"}},
		Txn: &TxnGuard{
			Snapshot:  7,
			SER:       true,
			ReadNodes: []string{"a", "b"},
			ReadEdges: []EdgeRef{{Src: "a", Dst: "b", Label: "CALLS"}},
		},
	}
}

func TestBatchTxnGobRoundtrip(t *testing.T) {
	data, err := encodeBatch(txnBatch())
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeBatch(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.Txn == nil {
		t.Fatal("Txn lost in roundtrip")
	}
	if got.Txn.Snapshot != 7 || !got.Txn.SER || len(got.Txn.ReadNodes) != 2 {
		t.Fatalf("TxnGuard = %+v", got.Txn)
	}
	if got.Txn.ReadEdges[0] != (EdgeRef{Src: "a", Dst: "b", Label: "CALLS"}) {
		t.Fatalf("ReadEdges = %+v", got.Txn.ReadEdges)
	}
}

// legacyBatch mirrors the pre-0.4.0 Batch field set: gob must skip the new
// Txn field when an old binary (or a test-local legacy struct) decodes.
type legacyBatch struct {
	Version     int
	Nodes       []NodeUpsert
	Edges       []EdgeUpsert
	EdgeDeletes []EdgeDelete
	NodeDeletes []string
}

func TestBatchGobLegacyCompat(t *testing.T) {
	// New encoder -> legacy struct: unknown Txn field must be ignored.
	data, err := encodeBatch(txnBatch())
	if err != nil {
		t.Fatal(err)
	}
	var old legacyBatch
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&old); err != nil {
		t.Fatalf("legacy decode of new batch: %v", err)
	}
	if len(old.Nodes) != 1 || old.Nodes[0].Key != "a" {
		t.Fatalf("legacy struct = %+v", old)
	}

	// Legacy encoder -> new struct: Txn must stay nil (auto-commit).
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(legacyBatch{Version: 2, Nodes: []NodeUpsert{{Key: "x", Label: "L"}}}); err != nil {
		t.Fatal(err)
	}
	got, err := decodeBatch(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if got.Txn != nil {
		t.Fatalf("Txn = %+v, want nil for legacy payload", got.Txn)
	}
}

// applyAt applies a batch under a chosen raft log index (validation compares
// against these indexes).
func applyAt(f *fsm, b Batch, idx uint64) interface{} {
	data, err := encodeBatch(b)
	if err != nil {
		panic(err) // test helper: gob encode of a valid Batch cannot fail
	}
	return f.Apply(&raft.Log{Data: data, Index: idx})
}

func TestFSMTxnSIWriteConflict(t *testing.T) {
	f := newFSM(time.Hour)
	// T1 @ index 5 writes node "a" (auto-commit, establishes lastMod[a]=5).
	applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "Svc"}}}, 5)
	// T2 read at token 3 (< 5) then writes "a" with SI guard: rejected.
	r := applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "Svc"}},
		Txn: &TxnGuard{Snapshot: 3}}, 6).(ApplyResult)
	if r.Committed {
		t.Fatal("SI write-write conflict must be rejected")
	}
	if len(r.Conflicts) == 0 || r.Conflicts[0] != "a" {
		t.Fatalf("conflicts = %v, want [a]", r.Conflicts)
	}
	// Fresh token (>= 5) commits.
	r = applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "Svc"}},
		Txn: &TxnGuard{Snapshot: 5}}, 7).(ApplyResult)
	if !r.Committed || r.Index != 7 {
		t.Fatalf("fresh token must commit: %+v", r)
	}
}

func TestFSMTxnDifferentKeysCommit(t *testing.T) {
	f := newFSM(time.Hour)
	applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}}, 5)
	r := applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "b", Label: "S"}},
		Txn: &TxnGuard{Snapshot: 3}}, 6).(ApplyResult)
	if !r.Committed {
		t.Fatal("disjoint write sets must both commit under SI")
	}
}

func TestFSMTxnSERReadWriteConflict(t *testing.T) {
	f := newFSM(time.Hour)
	applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}, {Key: "b", Label: "S"}}}, 5)
	// T reads "a" at token 5; concurrent edge write bumps nodeVer[a] @6.
	applyAt(f, Batch{Version: 3, Edges: []EdgeUpsert{{SrcKey: "a", DstKey: "b", Label: "E"}}}, 6)
	// SER: writing untouched key "c" while having read "a" (modified after
	// snapshot) -> rejected on the read set alone.
	r := applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "c", Label: "S"}},
		Txn: &TxnGuard{Snapshot: 5, SER: true, ReadNodes: []string{"a"}}}, 7).(ApplyResult)
	if r.Committed {
		t.Fatal("SER must reject read-write anti-dependency")
	}
	if len(r.Conflicts) == 0 || r.Conflicts[0] != "a" {
		t.Fatalf("conflicts = %v, want [a]", r.Conflicts)
	}
	// Same scenario under SI (no read validation): commits — the two-level split.
	r = applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "c", Label: "S"}},
		Txn: &TxnGuard{Snapshot: 5, ReadNodes: []string{"a"}}}, 8).(ApplyResult)
	if !r.Committed {
		t.Fatal("SI does not validate read sets")
	}
}

func TestFSMAutoCommitBlindWriteBumps(t *testing.T) {
	f := newFSM(time.Hour)
	// Auto-commit (nil Txn) blind write must still enter the version table,
	// otherwise concurrent guarded readers would never see it (false negative).
	applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}}, 5)
	r := applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "b", Label: "S"}},
		Txn: &TxnGuard{Snapshot: 4, SER: true, ReadNodes: []string{"a"}}}, 6).(ApplyResult)
	if r.Committed {
		t.Fatal("blind auto-commit write must be visible to SER validation")
	}
}

func TestFSMTxnReplayBumpIdempotent(t *testing.T) {
	f := newFSM(time.Hour)
	b := Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}}
	applyAt(f, b, 5)
	applyAt(f, b, 5) // same entry replayed: same index, bump must not advance
	f.mu.RLock()
	got := f.nodeVer["a"]
	f.mu.RUnlock()
	if got != 5 {
		t.Fatalf("nodeVer[a] = %d, want 5 (replay bumps to the same index)", got)
	}
}

func TestFSMTxnConcurrentMixedLoad(t *testing.T) {
	f := newFSM(5 * time.Millisecond)
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				b := Batch{Version: 3, Nodes: []NodeUpsert{{Key: fmt.Sprintf("w%d-%d", w, i), Label: "S"}}}
				if i%2 == 0 {
					b.Txn = &TxnGuard{Snapshot: 0}
				}
				applyAt(f, b, uint64(w*25+i+1))
			}
		}(w)
	}
	wg.Wait()
	f.sealNow()
	if st := f.Published().Stats(); st.Nodes != 100 {
		t.Fatalf("nodes = %d, want 100", st.Nodes)
	}
}

func TestFSMTxnCascadeKeys(t *testing.T) {
	f := newFSM(time.Hour)
	applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "x", Label: "S"}, {Key: "y", Label: "S"}},
		Edges: []EdgeUpsert{{SrcKey: "x", DstKey: "y", Label: "E"}}}, 5)
	// NodeDeletes["x"] cascades the x->y edge: peer y's adjacency changes.
	applyAt(f, Batch{Version: 3, NodeDeletes: []string{"x"}}, 6)
	// SER txn that read y's adjacency at token 5 must now be rejected.
	r := applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "z", Label: "S"}},
		Txn: &TxnGuard{Snapshot: 5, SER: true, ReadNodes: []string{"y"}}}, 7).(ApplyResult)
	if r.Committed {
		t.Fatal("cascade delete must bump peer adjacency version (audit P1-3)")
	}
}

func TestFSMTxnRejectedNoSideEffects(t *testing.T) {
	f := newFSM(time.Hour)
	applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}}, 5)
	before := f.metrics.AppliedBatches.Load()
	r := applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}},
		Txn: &TxnGuard{Snapshot: 3}}, 6).(ApplyResult)
	if r.Committed {
		t.Fatal("expected rejection")
	}
	f.mu.RLock()
	n := f.live.NodeCount()
	f.mu.RUnlock()
	if n != 1 {
		t.Fatalf("live nodes = %d, want 1 (rejected txn must not touch live)", n)
	}
	if got := f.metrics.AppliedBatches.Load(); got != before {
		t.Fatalf("AppliedBatches = %d, want unchanged %d", got, before)
	}
	if got := f.metrics.TxnConflicts.Load(); got != 1 {
		t.Fatalf("TxnConflicts = %d, want 1", got)
	}
	// Replay of a rejected entry stays rejected and never bumps.
	r2 := applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}},
		Txn: &TxnGuard{Snapshot: 3}}, 6).(ApplyResult)
	if r2.Committed {
		t.Fatal("rejected entry must stay rejected on replay")
	}
}

func TestFSMTxnDeterminismAcrossReplicas(t *testing.T) {
	run := func() []bool {
		f := newFSM(time.Hour)
		var out []bool
		for i, b := range []Batch{
			{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}},
			{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}, Txn: &TxnGuard{Snapshot: 0}},
			{Version: 3, Nodes: []NodeUpsert{{Key: "b", Label: "S"}}, Txn: &TxnGuard{Snapshot: 0}},
		} {
			out = append(out, applyAt(f, b, uint64(i+1)).(ApplyResult).Committed)
		}
		return out
	}
	a, b := run(), run()
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("replica divergence at %d: %v vs %v", i, a, b)
		}
	}
	// Expected: [true, false, true] — entry 2 reads at token 0 < lastMod[a]=1.
	if a[0] != true || a[1] != false || a[2] != true {
		t.Fatalf("commit sequence = %v, want [true false true]", a)
	}
}

func TestFSMTxnConflictsTruncated(t *testing.T) {
	f := newFSM(time.Hour)
	nodes := make([]NodeUpsert, 150)
	reads := make([]string, 150)
	for i := range nodes {
		nodes[i] = NodeUpsert{Key: fmt.Sprintf("k%03d", i), Label: "S"}
		reads[i] = fmt.Sprintf("k%03d", i)
	}
	applyAt(f, Batch{Version: 3, Nodes: nodes}, 5)
	r := applyAt(f, Batch{Version: 3, Nodes: nodes,
		Txn: &TxnGuard{Snapshot: 3, SER: true, ReadNodes: reads}}, 6).(ApplyResult)
	if r.Committed {
		t.Fatal("expected rejection")
	}
	if len(r.Conflicts) != maxConflicts || r.ConflictTotal != 150 {
		t.Fatalf("len(Conflicts)=%d ConflictTotal=%d, want %d/150", len(r.Conflicts), r.ConflictTotal, maxConflicts)
	}
}
