package server

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
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

func persistSnap(t *testing.T, f *fsm) []byte {
	t.Helper()
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&memSink{buf: &buf}); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestFSMSnapshotVCTXPersistRestore(t *testing.T) {
	f := newFSM(time.Hour)
	applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}}, 5)
	applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}},
		Txn: &TxnGuard{Snapshot: 5}}, 6) // committed -> nodeVer[a]=6
	// Edge-key round-trip: a separate edge (c->d) so node "a"'s version stays
	// 6 and the existing assertions keep their meaning.
	applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "c", Label: "S"}, {Key: "d", Label: "S"}},
		Edges: []EdgeUpsert{{SrcKey: "c", DstKey: "d", Label: "CALLS"}}}, 7)
	raw := persistSnap(t, f)

	f2 := newFSM(time.Hour)
	if err := f2.Restore(io.NopCloser(bytes.NewReader(raw))); err != nil {
		t.Fatal(err)
	}
	if got := f2.nodeVer["a"]; got != 6 {
		t.Fatalf("nodeVer[a] = %d, want 6", got)
	}
	if got := f2.edgeVer[edgeVerKey{Src: "c", Dst: "d", Label: "CALLS"}]; got != 7 {
		t.Fatalf("edgeVer[c->d CALLS] = %d, want 7", got)
	}
	if got := f2.appliedIndex.Load(); got != 7 {
		t.Fatalf("appliedIndex = %d, want 7", got)
	}
	// Post-restore log replay continues from restored table state.
	r := applyAt(f2, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}},
		Txn: &TxnGuard{Snapshot: 5}}, 8).(ApplyResult)
	if r.Committed {
		t.Fatal("token 5 < lastMod 6 must reject after restore")
	}
}

func TestFSMRestoreRejectsLegacy(t *testing.T) {
	f := newFSM(time.Hour)
	applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}}, 5)
	legacy, err := f.live.ExportSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	f2 := newFSM(time.Hour)
	if err := f2.Restore(io.NopCloser(bytes.NewReader(legacy))); err == nil {
		t.Fatal("legacy snapshot (no VCTX) must be rejected")
	}
}

func TestFSMRestoreReplayEquivalence(t *testing.T) {
	// The core audit-P1-4 property: a replica restored mid-stream and replaying
	// the same suffix must reach identical accept/reject decisions (and table).
	build := func() []Batch {
		return []Batch{
			{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}, {Key: "b", Label: "S"}}},
			{Version: 3, Edges: []EdgeUpsert{{SrcKey: "a", DstKey: "b", Label: "E"}}},
			{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}, Txn: &TxnGuard{Snapshot: 1}},
			{Version: 3, Nodes: []NodeUpsert{{Key: "c", Label: "S"}}, Txn: &TxnGuard{Snapshot: 2, SER: true, ReadNodes: []string{"a"}}},
		}
	}
	cont := func(f *fsm, from int) []bool {
		var out []bool
		for i, b := range build()[from:] {
			out = append(out, applyAt(f, b, uint64(from+i+1)).(ApplyResult).Committed)
		}
		return out
	}
	// Uninterrupted.
	full := newFSM(time.Hour)
	want := cont(full, 0)
	// Restored after entry 2.
	part := newFSM(time.Hour)
	cont(part, 0)
	// persist WITHOUT letting further applies leak in: snapshot now.
	raw := persistSnap(t, part)
	res := newFSM(time.Hour)
	if err := res.Restore(io.NopCloser(bytes.NewReader(raw))); err != nil {
		t.Fatal(err)
	}
	got := cont(res, 2)
	wantTail := want[2:]
	for i := range wantTail {
		if got[i] != wantTail[i] {
			t.Fatalf("divergence at suffix %d: restored=%v uninterrupted=%v", i, got, wantTail)
		}
	}
	if res.nodeVer["a"] != full.nodeVer["a"] {
		t.Fatalf("nodeVer[a]: restored=%d uninterrupted=%d", res.nodeVer["a"], full.nodeVer["a"])
	}
}

func TestFSMSnapshotFrozenDuringConcurrentApply(t *testing.T) {
	f := newFSM(time.Hour)
	applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}}, 5)
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	// Applies after Snapshot() must not appear in what Persist writes.
	applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "b", Label: "S"}}}, 6)
	var buf bytes.Buffer
	if err := snap.Persist(&memSink{buf: &buf}); err != nil {
		t.Fatal(err)
	}
	f2 := newFSM(time.Hour)
	if err := f2.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))); err != nil {
		t.Fatal(err)
	}
	if _, ok := f2.nodeVer["b"]; ok {
		t.Fatal("post-Snapshot apply leaked into persisted table")
	}
}

func TestPublishedBundleSeqInvariant(t *testing.T) {
	f := newFSM(5 * time.Millisecond)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var observed atomic.Int32
	wg.Add(1)
	go func() { // Reader: every observed (graph, seq) must contain all writes ≤ seq.
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if ps := f.published.Load(); ps != nil {
				observed.Add(1)
				// Write k lives at raft index k; the graph must contain every
				// node whose index is ≤ the published seq.
				for k := 1; uint64(k) <= ps.seq; k++ {
					if _, ok := ps.graph.NodeByKey(fmt.Sprintf("n%03d", k)); !ok {
						t.Errorf("seq=%d but node n%03d (index %d) missing", ps.seq, k, k)
						return
					}
				}
			}
		}
	}()
	for k := 1; k <= 50; k++ {
		applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{
			{Key: fmt.Sprintf("n%03d", k), Label: "S"}}}, uint64(k))
		time.Sleep(time.Millisecond)
	}
	close(stop)
	wg.Wait()
	if observed.Load() < 2 {
		t.Fatalf("reader only observed %d bundles; invariant never exercised", observed.Load())
	}
}

func TestSplitSnapshotBadInputs(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{"too short", []byte("ab")},
		{"length overflow", append([]byte{0xFF, 0xFF, 0xFF, 0xFF}, []byte("VCTX...")...)},
		{"graph only (legacy shape)", []byte("VCTR")},
	}
	for _, c := range cases {
		if _, _, err := splitSnapshot(c.data); err == nil {
			t.Errorf("%s: expected error", c.name)
		}
	}
}

// ============================================================
// POST /v1/txn — HTTP contract tests (real raft cluster).
// ============================================================

// currentLeader returns nil when no leader is elected (failover window).
func (tc *testCluster) currentLeader() *testNode {
	for _, tn := range tc.nodes {
		if tn.node.IsLeader() {
			return tn
		}
	}
	return nil
}

func (tc *testCluster) leader(t *testing.T) *testNode {
	t.Helper()
	if n := tc.currentLeader(); n != nil {
		return n
	}
	t.Fatal("no leader")
	return nil
}

func (tc *testCluster) closeAll() {
	for _, tn := range tc.nodes {
		tn.srv.Close()
		tn.node.Close()
	}
}

func TestTxnEndpointCommitThenConflict(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.closeAll()
	lead := tc.leader(t)
	body := func(snapshot uint64, key, val string) []byte {
		b := map[string]any{
			"nodes": []map[string]any{{"key": key, "label": "Svc", "props": map[string]any{"v": val}}},
			"txn":   map[string]any{"snapshot": snapshot},
		}
		raw, _ := json.Marshal(b)
		return raw
	}
	post := func(raw []byte) (int, map[string]any) {
		resp, err := http.Post(lead.URL()+"/v1/txn", "application/json", bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}
	// Seed one write so the first publish completes (query gate opens), then
	// read the token from a stats response header (RYW pattern).
	seed, _ := json.Marshal(map[string]any{"nodes": []map[string]any{{"key": "seed", "label": "S"}}})
	sresp0, err := http.Post(lead.URL()+"/v1/batch", "application/json", bytes.NewReader(seed))
	if err != nil {
		t.Fatal(err)
	}
	sresp0.Body.Close()
	hresp, err := http.Get(lead.URL() + "/v1/stats")
	if err != nil {
		t.Fatal(err)
	}
	hresp.Body.Close()
	if hresp.StatusCode != 200 {
		t.Fatalf("stats after seed: %d", hresp.StatusCode)
	}
	tok, _ := strconv.ParseUint(hresp.Header.Get("X-Vectra-Snapshot"), 10, 64)
	code, out := post(body(tok, "a", "1"))
	if code != 200 || out["committed"] != true {
		t.Fatalf("first txn: %d %v", code, out)
	}
	idx := uint64(out["index"].(float64))
	// Stale token (< idx) writing the same key must conflict.
	code, out = post(body(idx-1, "a", "2"))
	if code != 409 || out["committed"] != false {
		t.Fatalf("stale txn: %d %v", code, out)
	}
	if out["conflictTotal"].(float64) < 1 {
		t.Fatalf("conflictTotal = %v", out["conflictTotal"])
	}
	// Fresh token commits.
	code, out = post(body(idx, "a", "3"))
	if code != 200 {
		t.Fatalf("fresh txn: %d %v", code, out)
	}
	// seed + a (the 409 wrote nothing): the whole cluster must settle on 2 nodes.
	tc.waitAllPublished(2, 0, 5*time.Second)
}

func TestTxnEndpointNotLeaderAndStats(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.closeAll()
	lead := tc.leader(t)
	var follower *testNode
	for _, tn := range tc.nodes {
		if tn != lead {
			follower = tn
			break
		}
	}
	raw, _ := json.Marshal(map[string]any{
		"nodes": []map[string]any{{"key": "a", "label": "S"}},
		"txn":   map[string]any{"snapshot": 0}})
	resp, err := http.Post(follower.URL()+"/v1/txn", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 503 {
		t.Fatalf("follower /v1/txn: status = %d, want 503", resp.StatusCode)
	}
	// Seed one write so the first publish completes (query gate opens) —
	// the stats endpoint is gated on readiness, and this cluster has no
	// committed entries yet (the follower POST above never reached raft).
	seed, _ := json.Marshal(map[string]any{"nodes": []map[string]any{{"key": "s", "label": "S"}}})
	sresp0, err := http.Post(lead.URL()+"/v1/batch", "application/json", bytes.NewReader(seed))
	if err != nil {
		t.Fatal(err)
	}
	sresp0.Body.Close()
	// Stats exposes both indexes, and the invariant publishedSeq <= appliedIndex.
	sresp, err := http.Get(lead.URL() + "/v1/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer sresp.Body.Close()
	var st map[string]any
	_ = json.NewDecoder(sresp.Body).Decode(&st)
	applied := st["appliedIndex"].(float64)
	published := st["publishedSeq"].(float64)
	if published > applied {
		t.Fatalf("publishedSeq(%v) > appliedIndex(%v)", published, applied)
	}
}

func TestTxnEndpointReadsetCap(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.closeAll()
	lead := tc.leader(t)
	big := make([]string, 100_001)
	raw, _ := json.Marshal(map[string]any{
		"txn": map[string]any{"snapshot": 0, "readNodes": big},
	})
	resp, err := http.Post(lead.URL()+"/v1/txn", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	var out struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Error.Code != "TXN_READSET_TOO_LARGE" {
		t.Fatalf("error code = %q, want TXN_READSET_TOO_LARGE", out.Error.Code)
	}
}

func TestTxnEndpointNilTxnRejected(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.closeAll()
	lead := tc.leader(t)
	// Auto-commit shaped body (no "txn" field): /v1/txn must reject it with a
	// VALIDATION 400 rather than silently treating it as a zero-snapshot txn.
	raw, _ := json.Marshal(map[string]any{
		"nodes": []map[string]any{{"key": "a", "label": "S"}}})
	resp, err := http.Post(lead.URL()+"/v1/txn", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	var out struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Error.Code != "VALIDATION" {
		t.Fatalf("error code = %q, want VALIDATION", out.Error.Code)
	}
}

func TestTxnSERPhantomOverHTTP(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.closeAll()
	lead := tc.leader(t)
	seed := map[string]any{"nodes": []map[string]any{
		{"key": "u", "label": "S"}, {"key": "v", "label": "S"}}}
	raw, _ := json.Marshal(seed)
	resp, _ := http.Post(lead.URL()+"/v1/batch", "application/json", bytes.NewReader(raw))
	resp.Body.Close()
	tc.waitAllPublished(2, 0, 5*time.Second)

	// Token from a query (add node w + edge u->w concurrently first? No:
	// establish base, then read u's neighbors at token T, then bump u).
	nresp, _ := http.Get(lead.URL() + "/v1/neighbors?key=u")
	nresp.Body.Close()
	tok64, _ := strconv.ParseUint(nresp.Header.Get("X-Vectra-Snapshot"), 10, 64)

	// Concurrent auto-commit write bumps nodeVer[u].
	bump, _ := json.Marshal(map[string]any{"edges": []map[string]any{
		{"srcKey": "u", "dstKey": "v", "label": "E"}}})
	bresp, _ := http.Post(lead.URL()+"/v1/batch", "application/json", bytes.NewReader(bump))
	bresp.Body.Close()

	ser, _ := json.Marshal(map[string]any{
		"nodes": []map[string]any{{"key": "w", "label": "S"}},
		"txn":   map[string]any{"snapshot": tok64, "ser": true, "readNodes": []string{"u"}}})
	resp2, _ := http.Post(lead.URL()+"/v1/txn", "application/json", bytes.NewReader(ser))
	resp2.Body.Close()
	if resp2.StatusCode != 409 {
		t.Fatalf("SER phantom: status = %d, want 409", resp2.StatusCode)
	}
	// SI with the same read set commits (write-skew split).
	si, _ := json.Marshal(map[string]any{
		"nodes": []map[string]any{{"key": "w", "label": "S"}},
		"txn":   map[string]any{"snapshot": tok64, "readNodes": []string{"u"}}})
	resp3, _ := http.Post(lead.URL()+"/v1/txn", "application/json", bytes.NewReader(si))
	resp3.Body.Close()
	if resp3.StatusCode != 200 {
		t.Fatalf("SI same read set: status = %d, want 200", resp3.StatusCode)
	}
}

// ============================================================
// Cluster determinism, concurrent conflicts, failover drift.
// ============================================================

func TestTxnClusterDeterminism(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.closeAll()
	lead := tc.leader(t)
	// Interleave commits and conflicts over HTTP; then compare all replicas'
	// version tables (same package access).
	send := func(snapshot uint64, key string) bool {
		raw, _ := json.Marshal(map[string]any{
			"nodes": []map[string]any{{"key": key, "label": "S"}},
			"txn":   map[string]any{"snapshot": snapshot}})
		resp, err := http.Post(lead.URL()+"/v1/txn", "application/json", bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode == 200 && out["committed"] == true
	}
	send(0, "a") // commits at index 1
	if send(0, "a") {
		t.Fatal("second write to a with stale token must conflict")
	}
	tc.waitAllPublished(1, 0, 5*time.Second)
	// The absolute index depends on preceding config entries; the invariant is
	// that every replica has the SAME nodeVer[a] (>= 1: the write landed).
	var first uint64
	for i, tn := range tc.nodes {
		tn.node.fsm.mu.RLock()
		got := tn.node.fsm.nodeVer["a"]
		tn.node.fsm.mu.RUnlock()
		if got < 1 {
			t.Fatalf("node %s: nodeVer[a]=%d, want >=1", tn.node.cfg.NodeID, got)
		}
		if i == 0 {
			first = got
		} else if got != first {
			t.Fatalf("replica divergence: nodeVer[a] = %d vs %d", first, got)
		}
	}
}

func TestTxnFailoverNoDrift(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.closeAll()
	lead := tc.leader(t)
	raw, _ := json.Marshal(map[string]any{
		"nodes": []map[string]any{{"key": "a", "label": "S"}},
		"txn":   map[string]any{"snapshot": 0}})
	resp, _ := http.Post(lead.URL()+"/v1/txn", "application/json", bytes.NewReader(raw))
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("seed txn status = %d", resp.StatusCode)
	}
	// Failover: stop the leader, wait for a new one (poll the fatal-free
	// variant — the no-leader window is expected), retry with a stale token.
	lead.srv.Close()
	lead.node.Close()
	deadline := time.Now().Add(10 * time.Second)
	var next *testNode
	for time.Now().Before(deadline) {
		if n := tc.currentLeader(); n != nil && n != lead {
			next = n
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if next == nil {
		t.Fatal("no new leader elected")
	}
	stale, _ := json.Marshal(map[string]any{
		"nodes": []map[string]any{{"key": "a", "label": "S"}},
		"txn":   map[string]any{"snapshot": 0}})
	resp2, err := http.Post(next.URL()+"/v1/txn", "application/json", bytes.NewReader(stale))
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != 409 {
		t.Fatalf("post-failover stale token: status = %d, want 409", resp2.StatusCode)
	}
}

func TestTxnConcurrentExactlyOneCommits(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.closeAll()
	lead := tc.leader(t)
	seed, _ := json.Marshal(map[string]any{"nodes": []map[string]any{{"key": "a", "label": "S"}}})
	sr, _ := http.Post(lead.URL()+"/v1/batch", "application/json", bytes.NewReader(seed))
	sr.Body.Close()
	// RYW token: a literal snapshot 1 assumes the seed lands at raft index 1,
	// but cluster bootstrap + AddVoter config entries occupy the earlier
	// indexes (the seed lands at index 5 here), so a literal 1 is already
	// stale and all four contenders would 409. The token a client holds right
	// after the seed is the seed's own applied index: the first contender to
	// apply sees lastMod == token (fresh) and commits, while every later one
	// sees the winner's higher index (lastMod > token) and conflicts.
	tok := lead.node.fsm.appliedIndex.Load()

	var wg sync.WaitGroup
	results := make([]int, 4) // HTTP status per contender
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			raw, _ := json.Marshal(map[string]any{
				"nodes": []map[string]any{{"key": "a", "label": "S", "props": map[string]any{"w": i}}},
				"txn":   map[string]any{"snapshot": tok}})
			resp, err := http.Post(lead.URL()+"/v1/txn", "application/json", bytes.NewReader(raw))
			if err != nil {
				t.Error(err)
				return
			}
			resp.Body.Close()
			results[i] = resp.StatusCode
		}(i)
	}
	wg.Wait()
	ok := 0
	for _, c := range results {
		if c == 200 {
			ok++
		} else if c != 409 {
			t.Fatalf("unexpected status %d", c)
		}
	}
	if ok != 1 {
		t.Fatalf("committed = %d, want exactly 1 (statuses: %v)", ok, results)
	}
}

func TestFSMRestoreReplayRealisticVariant(t *testing.T) {
	// The realistic raft restore shape: the snapshot is taken after only the
	// FIRST TWO entries, so the restored table does NOT contain the suffix's
	// own effects. Replaying entries 3-4 is then a fresh application — no
	// table entry ever equals the replay index, so the same-index exclusion in
	// validateTxn is never exercised and the decisions rest purely on the
	// persisted prefix state (exactly what a follower restored mid-stream sees).
	build := []Batch{
		{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}, {Key: "b", Label: "S"}}},
		{Version: 3, Edges: []EdgeUpsert{{SrcKey: "a", DstKey: "b", Label: "E"}}},
		{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}, Txn: &TxnGuard{Snapshot: 1}},
		{Version: 3, Nodes: []NodeUpsert{{Key: "c", Label: "S"}}, Txn: &TxnGuard{Snapshot: 2, SER: true, ReadNodes: []string{"a"}}},
	}
	suffix := func(f *fsm, from int) []bool {
		var out []bool
		for i, b := range build[from:] {
			out = append(out, applyAt(f, b, uint64(from+i+1)).(ApplyResult).Committed)
		}
		return out
	}
	// Uninterrupted reference run: [commit, commit, reject, commit].
	full := newFSM(time.Hour)
	want := suffix(full, 0)
	if want[2] != false || want[3] != true {
		t.Fatalf("uninterrupted decisions = %v, want entry 3 rejected / entry 4 committed", want)
	}
	// Snapshot after exactly the first two entries, then restore + replay.
	part := newFSM(time.Hour)
	for i, b := range build[:2] {
		applyAt(part, b, uint64(i+1))
	}
	raw := persistSnap(t, part)
	res := newFSM(time.Hour)
	if err := res.Restore(io.NopCloser(bytes.NewReader(raw))); err != nil {
		t.Fatal(err)
	}
	got := suffix(res, 2)
	wantTail := want[2:]
	for i := range wantTail {
		if got[i] != wantTail[i] {
			t.Fatalf("suffix divergence at %d: restored=%v uninterrupted=%v", i, got, wantTail)
		}
	}
	if res.nodeVer["a"] != full.nodeVer["a"] {
		t.Fatalf("nodeVer[a]: restored=%d uninterrupted=%d", res.nodeVer["a"], full.nodeVer["a"])
	}
}

// ============================================================
// Task 7 coverage additions — defensive branches of the txn path.
// ============================================================

// frameSnapshot builds a VCTX-framed snapshot: u32 LE graph length + graph
// payload + trailer bytes (starting where "VCTX" would sit).
func frameSnapshot(graph, trailer []byte) []byte {
	out := make([]byte, 4, 4+len(graph)+len(trailer))
	binary.LittleEndian.PutUint32(out, uint32(len(graph)))
	out = append(out, graph...)
	return append(out, trailer...)
}

func TestSplitSnapshotFramingVariants(t *testing.T) {
	graph := []byte("GRAPHPAYLOAD")
	verBytes := func(v any) []byte {
		var b bytes.Buffer
		if err := gob.NewEncoder(&b).Encode(v); err != nil {
			t.Fatal(err)
		}
		return append([]byte{1, 0}, b.Bytes()...) // u16 LE format version 1 + gob body
	}
	errCases := []struct {
		name    string
		trailer []byte
	}{
		{"wrong magic after graph (legacy shape)", []byte("LEGX" + "\x01\x00" + "junk")},
		{"VCTX magic but version truncated", []byte("VCTX\x01")},
		{"unsupported VCTX version 2", []byte("VCTX\x02\x00" + "junk")},
		{"valid framing, corrupt gob body", append([]byte("VCTX"), verBytes("not-a-verSnapshot")...)},
	}
	for _, c := range errCases {
		if _, _, err := splitSnapshot(frameSnapshot(graph, c.trailer)); err == nil {
			t.Errorf("%s: expected error", c.name)
		}
	}
	// Valid framing with an all-zero verSnapshot: nil maps must be defaulted
	// to empty maps (Restore assigns them verbatim, nil maps would panic on
	// later writes).
	gr, vs, err := splitSnapshot(frameSnapshot(graph, append([]byte("VCTX"), verBytes(verSnapshot{})...)))
	if err != nil {
		t.Fatal(err)
	}
	if string(gr) != "GRAPHPAYLOAD" {
		t.Fatalf("graph = %q", gr)
	}
	if vs.NodeVer == nil || vs.EdgeVer == nil {
		t.Fatalf("nil maps not defaulted: %+v", vs)
	}
}

func TestFSMTxnEdgeKeyConflicts(t *testing.T) {
	f := newFSM(time.Hour)
	applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "a", Label: "S"}, {Key: "b", Label: "S"}},
		Edges: []EdgeUpsert{{SrcKey: "a", DstKey: "b", Label: "E"}}}, 5) // edgeVer[a,b,E]=5
	// SI: re-writing the same edge at a stale token conflicts. An edge write
	// also bumps its endpoint nodes' versions, so the conflict list reports
	// the endpoint node keys AND the edge key (src NUL dst NUL label).
	r := applyAt(f, Batch{Version: 3, Edges: []EdgeUpsert{{SrcKey: "a", DstKey: "b", Label: "E"}},
		Txn: &TxnGuard{Snapshot: 3}}, 6).(ApplyResult)
	if r.Committed {
		t.Fatal("SI edge write-write conflict must be rejected")
	}
	want := edgeKeyString(edgeVerKey{Src: "a", Dst: "b", Label: "E"})
	found := false
	for _, k := range r.Conflicts {
		if k == want {
			found = true
		}
	}
	if !found {
		t.Fatalf("conflicts = %v, want to contain edge key %q", r.Conflicts, want)
	}
	// SER: a read-edge recorded in the guard is validated too — here nothing
	// else is touched, so the edge key is the sole conflict.
	r = applyAt(f, Batch{Version: 3, Nodes: []NodeUpsert{{Key: "c", Label: "S"}},
		Txn: &TxnGuard{Snapshot: 3, SER: true, ReadEdges: []EdgeRef{{Src: "a", Dst: "b", Label: "E"}}}}, 7).(ApplyResult)
	if r.Committed {
		t.Fatal("SER read-edge anti-dependency must be rejected")
	}
	if len(r.Conflicts) != 1 || r.Conflicts[0] != want {
		t.Fatalf("conflicts = %v, want [%s]", r.Conflicts, want)
	}
}

func TestTxnEndpointBadBody(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.closeAll()
	lead := tc.leader(t)
	resp, err := http.Post(lead.URL()+"/v1/txn", "application/json", bytes.NewReader([]byte("{not json")))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	var out struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Error.Code != "VALIDATION" {
		t.Fatalf("error code = %q, want VALIDATION", out.Error.Code)
	}
}

// failingReader simulates a truncated snapshot stream (disk error / short read).
type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (failingReader) Close() error             { return nil }

func TestFSMRestoreReaderError(t *testing.T) {
	f := newFSM(time.Hour)
	if err := f.Restore(failingReader{}); err == nil {
		t.Fatal("short snapshot stream must error")
	}
}

func TestFSMApplyCorruptLogRejected(t *testing.T) {
	f := newFSM(time.Hour)
	if got := f.PublishedSeq(); got != 0 {
		t.Fatalf("PublishedSeq before first publish = %d, want 0", got)
	}
	// A corrupt raft log entry must surface the decode error, never a
	// half-applied ApplyResult (deterministic-skip discipline does not
	// extend to undecodable batches).
	if _, ok := f.Apply(&raft.Log{Data: []byte{0xFF, 0xFE, 0xFD}, Index: 1}).(ApplyResult); ok {
		t.Fatal("corrupt log data must not produce an ApplyResult")
	}
}

func TestFSMNodeDeleteMissingSkipped(t *testing.T) {
	f := newFSM(time.Hour)
	r := applyAt(f, Batch{Version: 3, NodeDeletes: []string{"ghost"}}, 5).(ApplyResult)
	if !r.Committed || r.NodesSkipped != 1 || r.NodesDeleted != 0 {
		t.Fatalf("deleting a nonexistent key: %+v", r)
	}
}
