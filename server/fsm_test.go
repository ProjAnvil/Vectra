package server

import (
	"bytes"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/projanvil/vectra"
)

func sampleBatch() Batch {
	return Batch{
		Version: 1,
		Nodes: []NodeUpsert{
			{Key: "gateway", Label: "Service", Props: map[string]any{"env": "prod"}},
			{Key: "auth", Label: "Service", Props: map[string]any{"env": "prod"}},
			{Key: "db", Label: "Storage", Props: nil},
		},
		Edges: []EdgeUpsert{
			{SrcKey: "gateway", DstKey: "auth", Label: "CALLS"},
			{SrcKey: "auth", DstKey: "db", Label: "QUERY"},
			{SrcKey: "ghost", DstKey: "db", Label: "QUERY"}, // References a nonexistent node → skipped
		},
	}
}

func TestBatchGobRoundtrip(t *testing.T) {
	b := sampleBatch()
	data, err := encodeBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeBatch(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Nodes) != 3 || got.Nodes[0].Key != "gateway" || got.Nodes[0].Props["env"] != "prod" {
		t.Errorf("nodes roundtrip mismatch: %+v", got.Nodes)
	}
	if len(got.Edges) != 3 || got.Edges[2].SrcKey != "ghost" {
		t.Errorf("edges roundtrip mismatch: %+v", got.Edges)
	}
	if got.Version != 1 {
		t.Errorf("version = %d", got.Version)
	}
}

func TestFSMApplyUpsertIdempotent(t *testing.T) {
	f := newFSM(50 * time.Millisecond)
	data, _ := encodeBatch(sampleBatch())

	r1 := f.Apply(&raft.Log{Data: data}).(ApplyResult)
	if r1.NodesApplied != 3 || r1.EdgesApplied != 2 || r1.EdgesSkipped != 1 {
		t.Fatalf("first Apply: %+v, want 3/2/1", r1)
	}
	f.mu.RLock()
	n, e := f.live.NodeCount(), f.live.EdgeCount()
	f.mu.RUnlock()
	if n != 3 || e != 2 {
		t.Fatalf("live = %d/%d, want 3/2", n, e)
	}

	// Repeated Apply (idempotent upsert): node count unchanged
	r2 := f.Apply(&raft.Log{Data: data}).(ApplyResult)
	if r2.NodesApplied != 3 {
		t.Errorf("repeated Apply nodes = %d, want 3 (idempotent)", r2.NodesApplied)
	}
	f.mu.RLock()
	n = f.live.NodeCount()
	f.mu.RUnlock()
	if n != 3 {
		t.Errorf("nodes after repeat = %d, want 3", n)
	}

	// Prop update semantics: upsert the same key again with a different env
	upd, _ := encodeBatch(Batch{Version: 1, Nodes: []NodeUpsert{{Key: "db", Label: "Storage", Props: map[string]any{"env": "dr"}}}})
	f.Apply(&raft.Log{Data: upd})
	f.mu.RLock()
	node, _ := f.live.NodeByKey("db")
	f.mu.RUnlock()
	if node.Props["env"] != "dr" {
		t.Errorf("props update failed: %v", node.Props)
	}
}

func TestFSMPersistRestore(t *testing.T) {
	f := newFSM(50 * time.Millisecond)
	f.Apply(&raft.Log{Data: mustGob(t, sampleBatch())})
	waitPublished(t, f, 3, 2, 2*time.Second)

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	sink := &memSink{buf: &buf}
	if err := snap.Persist(sink); err != nil {
		t.Fatal(err)
	}

	f2 := newFSM(50 * time.Millisecond)
	if err := f2.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))); err != nil {
		t.Fatal(err)
	}
	waitPublished(t, f2, 3, 2, 2*time.Second)
	if f2.metrics.SkippedEdges.Load() != 0 {
		t.Errorf("restore should not produce skipped edges: %d", f2.metrics.SkippedEdges.Load())
	}
	// After restore, Apply still works (live is in the unsealed state)
	ext, _ := encodeBatch(Batch{Version: 1, Nodes: []NodeUpsert{{Key: "kafka", Label: "Bus"}}})
	f2.Apply(&raft.Log{Data: ext})
	waitPublished(t, f2, 4, 2, 2*time.Second)
}

func TestFSMPublisherFreshness(t *testing.T) {
	// Small graph (<100k edges): publish immediately after each batch
	f := newFSM(time.Hour) // Even a huge throttle must not block immediate publish for small graphs
	f.Apply(&raft.Log{Data: mustGob(t, sampleBatch())})
	waitPublished(t, f, 3, 2, 2*time.Second)

	// Large-graph throttle path: lower the limit to simulate a large graph; no publish before the interval elapses
	f2 := newFSM(time.Hour)
	f2.smallGraphLimit = 0 // Force the throttled branch
	f2.Apply(&raft.Log{Data: mustGob(t, sampleBatch())})
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if p := f2.Published(); p != nil && p.NodeCount() == 3 {
			t.Fatal("should not publish during the throttle interval")
		}
		time.Sleep(20 * time.Millisecond)
	}
	f2.sealNow() // Manual trigger (simulating interval expiry)
	waitPublished(t, f2, 3, 2, 2*time.Second)
}

func TestFSMConcurrentApplyAndRead(t *testing.T) {
	f := newFSM(10 * time.Millisecond)
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				b := Batch{Version: 1, Nodes: []NodeUpsert{
					{Key: keyOf(w, i), Label: "Svc"},
				}, Edges: []EdgeUpsert{
					{SrcKey: keyOf(w, i), DstKey: keyOf(w, 0), Label: "E"},
				}}
				f.Apply(&raft.Log{Data: mustGob(t, b)})
			}
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			if p := f.Published(); p != nil {
				_, _ = p.ReachableIDs(0)
			}
			time.Sleep(time.Millisecond)
		}
	}()
	wg.Wait()
	waitPublished(t, f, 80, 80, 3*time.Second)
}

func TestFSMReadyGate(t *testing.T) {
	f := newFSM(10 * time.Millisecond)
	select {
	case <-f.Ready():
		t.Fatal("should not be ready before first publish")
	default:
	}
	f.Apply(&raft.Log{Data: mustGob(t, sampleBatch())})
	select {
	case <-f.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("should be ready after first publish")
	}
	// Ready stays readable once closed
	select {
	case <-f.Ready():
	default:
		t.Fatal("Ready should stay readable once closed")
	}
}

// ---- Test helpers ----

func mustGob(t *testing.T, b Batch) []byte {
	t.Helper()
	data, err := encodeBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func keyOf(w, i int) string {
	return "n" + string(rune('a'+w)) + "-" + string(rune('0'+i%10)) + "-" + string(rune('0'+i/10))
}

func waitPublished(t *testing.T, f *fsm, nodes, edges int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if p := f.Published(); p != nil {
			st := p.Stats()
			if st.Nodes == nodes && st.Edges == edges {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	p := f.Published()
	if p == nil {
		t.Fatalf("publish timeout: nil (want %d/%d)", nodes, edges)
	}
	st := p.Stats()
	t.Fatalf("publish timeout: published=%d/%d, want %d/%d", st.Nodes, st.Edges, nodes, edges)
}

// memSink is an in-memory raft.SnapshotSink implementation (for tests).
type memSink struct {
	buf      *bytes.Buffer
	closed   bool
	canceled bool
}

func (m *memSink) Write(p []byte) (int, error) { return m.buf.Write(p) }
func (m *memSink) Close() error                { m.closed = true; return nil }
func (m *memSink) ID() string                  { return "mem-sink" }
func (m *memSink) Cancel() error               { m.canceled = true; return nil }

var _ raft.SnapshotSink = (*memSink)(nil)

var _ = vectra.New // Keep the engine import (Published returns *vectra.Graph)
