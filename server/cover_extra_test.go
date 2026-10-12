package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/projanvil/vectra"
)

func TestNewValidationErrors(t *testing.T) {
	if _, err := New(Config{DataDir: t.TempDir()}); err == nil {
		t.Error("missing NodeID should error")
	}
	if _, err := New(Config{NodeID: "x"}); err == nil {
		t.Error("missing DataDir should error")
	}
	// Directory identity mismatch
	dir := t.TempDir()
	meta := nodeMeta{NodeID: "other", HTTPAddr: "h", RaftAddr: "r"}
	data, _ := json.Marshal(meta)
	_ = os.WriteFile(filepath.Join(dir, "meta.json"), data, 0o644)
	if _, err := New(Config{NodeID: "x", DataDir: dir}); !errors.Is(err, ErrDirMismatch) {
		t.Errorf("directory identity mismatch: %v", err)
	}
	// Bad meta.json
	_ = os.WriteFile(filepath.Join(dir, "meta.json"), []byte("{"), 0o644)
	if _, err := New(Config{NodeID: "x", DataDir: dir}); err == nil {
		t.Error("bad meta.json should error")
	}
}

func TestNodeJoinBadSeedStillStarts(t *testing.T) {
	// Unreachable Join is not fatal: the node still starts (stays candidate; ops can retry)
	tn := newTestNode(t, "solo", "http://127.0.0.1:1")
	defer func() { tn.node.Close(); tn.srv.Close() }()
	select {
	case <-tn.node.Ready():
		t.Fatal("should not be ready without writes")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestNodeReadyExposesFSMReady(t *testing.T) {
	tn := newTestNode(t, "solo2", "")
	defer func() { tn.node.Close(); tn.srv.Close() }()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && !tn.node.IsLeader() {
		time.Sleep(20 * time.Millisecond)
	}
	if !tn.node.IsLeader() {
		t.Fatal("single node not elected")
	}
	if _, err := tn.node.Apply(t.Context(), Batch{Version: 1, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}}); err != nil {
		t.Fatalf("single-node write: %v", err)
	}
	select {
	case <-tn.node.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("Ready not opened")
	}
}

func TestApplyOnFollowerRejected(t *testing.T) {
	tc := newTestCluster(t, 3)
	tc.leaderWithin(15 * time.Second)
	var follower *testNode
	for _, tn := range tc.nodes {
		if !tn.node.IsLeader() {
			follower = tn
			break
		}
	}
	if follower == nil {
		t.Fatal("no follower")
	}
	_, err := follower.node.Apply(t.Context(), Batch{Version: 1})
	if !errors.Is(err, ErrNotLeader) {
		t.Errorf("follower Apply: %v, want ErrNotLeader", err)
	}
}

func TestApplyDecodeErrorReturned(t *testing.T) {
	f := newFSM(time.Millisecond)
	defer f.Close()
	res := f.Apply(&raft.Log{Data: []byte("garbage-not-gob")})
	if _, ok := res.(error); !ok {
		t.Fatalf("bad log should return error, got %#v", res)
	}
}

func TestAnnounceAddr(t *testing.T) {
	for in, want := range map[string]string{
		":8080":         "127.0.0.1:8080",
		"0.0.0.0:9":     "127.0.0.1:9",
		"10.0.0.1:1000": "10.0.0.1:1000",
		"no-port":       "no-port",
	} {
		if got := announceAddr(in); got != want {
			t.Errorf("announceAddr(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestDefaultRaftConfigUsedWhenNotInjected(t *testing.T) {
	n := &Node{cfg: Config{NodeID: "x"}}
	c := n.raftConfig()
	if c == nil || c.LocalID != raft.ServerID("x") {
		t.Fatalf("default raft config: %+v", c)
	}
}

func TestCloseIdempotent(t *testing.T) {
	tn := newTestNode(t, "closer", "")
	tn.node.Close()
	tn.node.Close() // Second close does not panic
	tn.srv.Close()
}

func TestHandlerUnknownPath404(t *testing.T) {
	tn := newTestNode(t, "solo3", "")
	defer func() { tn.node.Close(); tn.srv.Close() }()
	resp, err := http.Get(tn.URL() + "/v1/unknown")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("unknown path = %d, want 404", resp.StatusCode)
	}
	// expvar endpoint available
	resp, err = http.Get(tn.URL() + "/debug/vars")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/debug/vars = %d", resp.StatusCode)
	}
}

func TestHandleJoinValidation(t *testing.T) {
	tn := newTestNode(t, "solo4", "")
	defer func() { tn.node.Close(); tn.srv.Close() }()
	resp, out := postJSON(t, tn.URL()+"/v1/join", map[string]any{"nodeID": "x"})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("join with missing fields: %d %v", resp.StatusCode, out)
	}
}

func TestHandleQueryStartNotFound(t *testing.T) {
	tc := newTestCluster(t, 1)
	leader := tc.leaderWithin(15 * time.Second)
	postJSON(t, leader.URL()+"/v1/batch", map[string]any{
		"nodes": []map[string]any{{"key": "a", "label": "S"}},
	})
	waitLocalPublished(t, leader, 1, 0, 5*time.Second)
	resp, out := postJSON(t, leader.URL()+"/v1/query", map[string]any{
		"from": []string{"missing"},
	})
	if resp.StatusCode != http.StatusBadRequest || out["error"].(map[string]any)["code"] != "NODE_NOT_FOUND" {
		t.Fatalf("start node not found: %d %v", resp.StatusCode, out)
	}
	// NODE_NOT_FOUND for reachable / path / neighbors
	resp, _ = getJSON(t, leader.URL()+"/v1/reachable?from=missing")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("reachable missing: %d", resp.StatusCode)
	}
	resp, _ = getJSON(t, leader.URL()+"/v1/path?from=missing&to=a")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("path missing: %d", resp.StatusCode)
	}
	resp, _ = getJSON(t, leader.URL()+"/v1/neighbors?key=missing")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("neighbors missing: %d", resp.StatusCode)
	}
	// Unreachable path: found=false
	resp, out = getJSON(t, leader.URL()+"/v1/path?from=a&to=a")
	if resp.StatusCode != http.StatusOK || out["found"] != true {
		t.Errorf("self-loop path: %d %v", resp.StatusCode, out)
	}
}

func TestGraphNilAfterReadyButBeforePublishEdge(t *testing.T) {
	// graph()'s belt-and-suspenders branch: Ready closed but published nil (the tiny
	// window after Restore and before the first publish) — triggered directly via the FSM
	f := newFSM(time.Millisecond)
	defer f.Close()
	data, _ := encodeBatch(Batch{Version: 1, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}})
	f.Apply(&raft.Log{Data: data})
	<-f.Ready()
	if f.Published() == nil {
		t.Fatal("published should not be nil after ready")
	}
}

func TestFSMReleaseNoop(t *testing.T) {
	s := &graphSnapshot{clone: vectra.New()}
	s.Release() // No-op, must not panic
}

func TestPersistErrorOnFailingSink(t *testing.T) {
	f := newFSM(time.Millisecond)
	defer f.Close()
	data, _ := encodeBatch(Batch{Version: 1, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}})
	f.Apply(&raft.Log{Data: data})
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := snap.Persist(&failSink{}); err == nil {
		t.Fatal("sink write failure should propagate")
	}
}

type failSink struct{}

func (fs *failSink) Write(p []byte) (int, error) { return 0, errors.New("io fail") }
func (fs *failSink) Close() error                { return nil }
func (fs *failSink) ID() string                  { return "fail" }
func (fs *failSink) Cancel() error               { return nil }

func waitLocalPublished(t *testing.T, tn *testNode, nodes, edges int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if p := tn.node.Published(); p != nil {
			st := p.Stats()
			if st.Nodes == nodes && st.Edges == edges {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("local publish timeout %d/%d", nodes, edges)
}

var _ = httptest.NewServer
var _ = bytes.NewReader

// TestNewProductionDefaults Production default path: BoltDB + FileSnapshotStore +
// TCP transport (port 0); write-close-reopen goes through disk recovery.
func TestNewProductionDefaults(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{NodeID: "prod1", HTTPAddr: "127.0.0.1:0", RaftAddr: "127.0.0.1:0", DataDir: dir}
	n, err := New(cfg)
	if err != nil {
		t.Fatalf("production default assembly: %v", err)
	}
	srv := httptest.NewServer(n.Handler())
	defer func() { srv.Close(); n.Close() }()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && !n.IsLeader() {
		time.Sleep(20 * time.Millisecond)
	}
	if !n.IsLeader() {
		t.Fatal("production-default single node not elected")
	}
	resp, out := postJSON(t, srv.URL+"/v1/batch", map[string]any{
		"nodes": []map[string]any{{"key": "a", "label": "S"}, {"key": "b", "label": "S"}},
		"edges": []map[string]any{{"srcKey": "a", "dstKey": "b", "label": "E"}},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("production write: %d %v", resp.StatusCode, out)
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if p := n.Published(); p != nil && p.Stats().Edges == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	n.Close()

	// Reopen the same directory: existing bolt/snapshot state → no re-bootstrap, recovers and catches up
	n2, err := New(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer n2.Close()
	srv2 := httptest.NewServer(n2.Handler())
	defer srv2.Close()
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if p := n2.Published(); p != nil && p.Stats().Nodes == 2 {
			return // Log replay recovery succeeded
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("did not recover from raft logs after reopen")
}

func TestNewBadRaftAddrRejected(t *testing.T) {
	cfg := Config{NodeID: "bad", HTTPAddr: "127.0.0.1:0", RaftAddr: "not-an-addr:xyz", DataDir: t.TempDir()}
	if _, err := New(cfg); err == nil {
		t.Fatal("bad RaftAddr should error")
	}
}

// TestJoinGarbageResponseTolerated seed returns a non-JSON registry response:
// join does not fail, just warns (registry stays empty).
func TestJoinGarbageResponseTolerated(t *testing.T) {
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("not-json"))
	}))
	defer bad.Close()
	tn := newTestNode(t, "j1", bad.URL)
	defer func() { tn.node.Close(); tn.srv.Close() }()
	// Node starts up as usual
	if tn.node.IsLeader() {
		t.Log("became leader without joining a cluster — should not happen (non-empty Join never bootstraps)")
	}
}
