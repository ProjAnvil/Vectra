package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/projanvil/vectra"
)

// ============================================================
// In-process test cluster (InmemTransport + httptest) — shared by
// T4 API tests and T5 chaos tests.
// ============================================================

type testNode struct {
	node   *Node
	srv    *httptest.Server
	trans  *raft.InmemTransport
	logs   raft.LogStore
	stable raft.StableStore
	snaps  raft.SnapshotStore
}

func (tn *testNode) URL() string { return tn.srv.URL }

type testCluster struct {
	t     *testing.T
	nodes []*testNode
}

// newTestCluster builds a cluster: n1 bootstraps from an empty dir and gets
// elected; the other nodes are created with Join (New runs the real /v1/join
// protocol). Transports are interconnected before New so AddVoter can wait
// for a majority commit.
func newTestCluster(t *testing.T, n int) *testCluster {
	t.Helper()
	tc := &testCluster{t: t}
	first := newTestNode(t, "n1", "")
	tc.nodes = append(tc.nodes, first)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && !first.node.IsLeader() {
		time.Sleep(20 * time.Millisecond)
	}
	if !first.node.IsLeader() {
		t.Fatal("n1 not elected leader")
	}
	for i := 2; i <= n; i++ {
		tn := newTestNode(t, fmt.Sprintf("n%d", i), first.URL(), tc.nodes...)
		tc.nodes = append(tc.nodes, tn)
	}
	return tc
}

func newTestNode(t *testing.T, id, join string, existing ...*testNode) *testNode {
	t.Helper()
	transAddr, trans := raft.NewInmemTransport(raft.NewInmemAddr())
	logs := raft.NewInmemStore()
	stable := raft.NewInmemStore()
	snaps := raft.NewInmemSnapshotStore()

	// Interconnect with existing nodes first (join's AddVoter needs a reachable majority)
	for _, ex := range existing {
		trans.Connect(raft.ServerAddress(ex.node.cfg.RaftAddr), ex.trans)
		ex.trans.Connect(raft.ServerAddress(string(transAddr)), trans)
	}

	cfg := Config{
		NodeID:        id,
		HTTPAddr:      "127.0.0.1:0",
		RaftAddr:      string(transAddr),
		DataDir:       t.TempDir(),
		Join:          join,
		SealInterval:  50 * time.Millisecond,
		LogStore:      logs,
		StableStore:   stable,
		SnapshotStore: snaps,
		Transport:     trans,
		RaftConfig:    testRaftConfig(id),
	}
	n, err := New(cfg)
	if err != nil {
		t.Fatalf("New(%s): %v", id, err)
	}
	tn := &testNode{node: n, trans: trans, logs: logs, stable: stable, snaps: snaps}
	tn.srv = httptest.NewServer(n.Handler())
	t.Cleanup(func() {
		tn.srv.Close()
		n.Close()
	})
	return tn
}

// testRaftConfig for chaos tests: tick 20ms, ElectionTick 12, HeartbeatTick 2.
func testRaftConfig(id string) *raft.Config {
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(id)
	c.HeartbeatTimeout = 240 * time.Millisecond
	c.ElectionTimeout = 240 * time.Millisecond
	c.LeaderLeaseTimeout = 180 * time.Millisecond
	c.CommitTimeout = 20 * time.Millisecond
	c.SnapshotThreshold = 8192
	c.TrailingLogs = 8192
	c.Logger = nil
	return c
}

func (tc *testCluster) leaderWithin(within time.Duration) *testNode {
	tc.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		for _, tn := range tc.nodes {
			if tn.node.IsLeader() {
				return tn
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	tc.t.Fatal("no node became leader within the deadline")
	return nil
}

func (tc *testCluster) waitAllPublished(nodes, edges int, within time.Duration) {
	tc.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		ok := true
		for _, tn := range tc.nodes {
			p := tn.node.Published()
			if p == nil {
				ok = false
				break
			}
			st := p.Stats()
			if st.Nodes != nodes || st.Edges != edges {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	tc.t.Fatalf("cluster did not fully publish %d/%d within the deadline", nodes, edges)
}

// ============================================================
// HTTP API contract tests
// ============================================================

func postJSON(t *testing.T, url string, body any) (*http.Response, map[string]any) {
	t.Helper()
	data, _ := json.Marshal(body)
	resp, err := http.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return resp, out
}

func getJSON(t *testing.T, url string) (*http.Response, map[string]any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return resp, out
}

func TestAPIBatchWriteAndQuery(t *testing.T) {
	tc := newTestCluster(t, 3)
	leader := tc.leaderWithin(10 * time.Second)

	resp, out := postJSON(t, leader.URL()+"/v1/batch", map[string]any{
		"nodes": []map[string]any{
			{"key": "gateway", "label": "Service", "props": map[string]any{"env": "prod"}},
			{"key": "auth", "label": "Service"},
			{"key": "db", "label": "Storage"},
		},
		"edges": []map[string]any{
			{"srcKey": "gateway", "dstKey": "auth", "label": "CALLS"},
			{"srcKey": "auth", "dstKey": "db", "label": "QUERY"},
			{"srcKey": "ghost", "dstKey": "db", "label": "QUERY"},
		},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("batch write: %d %v", resp.StatusCode, out)
	}
	if out["nodesApplied"] != float64(3) || out["edgesApplied"] != float64(2) || out["edgesSkipped"] != float64(1) {
		t.Fatalf("applied result wrong: %v", out)
	}

	tc.waitAllPublished(3, 2, 10*time.Second)

	// Query endpoints (on the leader)
	resp, out = getJSON(t, leader.URL()+"/v1/reachable?from=gateway")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("reachable: %d", resp.StatusCode)
	}
	nodes, _ := out["nodes"].([]any)
	if len(nodes) != 3 {
		t.Fatalf("reachable nodes = %v, want 3", nodes)
	}

	resp, out = getJSON(t, leader.URL()+"/v1/path?from=gateway&to=db")
	if resp.StatusCode != http.StatusOK || out["found"] != true || out["hops"] != float64(2) {
		t.Fatalf("path: %d %v", resp.StatusCode, out)
	}

	resp, out = getJSON(t, leader.URL()+"/v1/stats")
	if resp.StatusCode != http.StatusOK || out["nodes"] != float64(3) || out["edges"] != float64(2) {
		t.Fatalf("stats: %d %v", resp.StatusCode, out)
	}
	if out["isLeader"] != true {
		t.Fatalf("stats.isLeader = %v", out["isLeader"])
	}

	// Reads on a follower (eventually consistent): also served
	var follower *testNode
	for _, tn := range tc.nodes {
		if tn != leader {
			follower = tn
			break
		}
	}
	resp, out = getJSON(t, follower.URL()+"/v1/stats")
	if resp.StatusCode != http.StatusOK || out["nodes"] != float64(3) {
		t.Fatalf("follower stats: %d %v", resp.StatusCode, out)
	}
}

func TestAPINonLeaderWriteRedirectHint(t *testing.T) {
	tc := newTestCluster(t, 3)
	leader := tc.leaderWithin(10 * time.Second)
	var follower *testNode
	for _, tn := range tc.nodes {
		if tn != leader {
			follower = tn
			break
		}
	}
	resp, out := postJSON(t, follower.URL()+"/v1/batch", map[string]any{
		"nodes": []map[string]any{{"key": "x", "label": "S"}},
	})
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("non-leader write should be 503: %d %v", resp.StatusCode, out)
	}
	if errBody, ok := out["error"].(map[string]any); !ok || errBody["code"] != "NOT_LEADER" {
		t.Fatalf("error code: %v", out)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Error("missing Retry-After header")
	}
	// Leader hint header is optional (registry is best effort), not enforced; but if present it must be resolvable
	if h := resp.Header.Get("X-Vectra-Leader"); h != "" && h == follower.URL() {
		t.Error("leader hint points to itself")
	}
}

func TestAPIQueryValidation(t *testing.T) {
	tc := newTestCluster(t, 1)
	leader := tc.leaderWithin(10 * time.Second)
	postJSON(t, leader.URL()+"/v1/batch", map[string]any{
		"nodes": []map[string]any{{"key": "a", "label": "S"}, {"key": "b", "label": "S"}},
		"edges": []map[string]any{{"srcKey": "a", "dstKey": "b", "label": "E"}},
	})
	tc.waitAllPublished(2, 1, 5*time.Second)

	// Missing from
	resp, out := postJSON(t, leader.URL()+"/v1/query", map[string]any{"maxDepth": 2})
	if resp.StatusCode != http.StatusBadRequest || out["error"].(map[string]any)["code"] != "VALIDATION" {
		t.Fatalf("missing from: %d %v", resp.StatusCode, out)
	}
	// Nonexistent node
	resp, out = getJSON(t, leader.URL()+"/v1/nodes/missing")
	if resp.StatusCode != http.StatusNotFound || out["error"].(map[string]any)["code"] != "NODE_NOT_FOUND" {
		t.Fatalf("missing node: %d %v", resp.StatusCode, out)
	}
	// Bad JSON
	resp2, err := http.Post(leader.URL()+"/v1/query", "application/json", bytes.NewReader([]byte("{")))
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad JSON: %d", resp2.StatusCode)
	}
}

func TestAPIQueryEndpoint(t *testing.T) {
	tc := newTestCluster(t, 1)
	leader := tc.leaderWithin(10 * time.Second)
	postJSON(t, leader.URL()+"/v1/batch", map[string]any{
		"nodes": []map[string]any{
			{"key": "a", "label": "S"}, {"key": "b", "label": "S"}, {"key": "c", "label": "S"},
		},
		"edges": []map[string]any{
			{"srcKey": "a", "dstKey": "b", "label": "CALLS"},
			{"srcKey": "b", "dstKey": "c", "label": "DATA"},
			{"srcKey": "a", "dstKey": "c", "label": "DATA"},
		},
	})
	tc.waitAllPublished(3, 3, 5*time.Second)

	resp, out := postJSON(t, leader.URL()+"/v1/query", map[string]any{
		"from":     []string{"a"},
		"labels":   []string{"CALLS"},
		"maxDepth": 2,
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("query: %d %v", resp.StatusCode, out)
	}
	if out["count"] != float64(2) { // a, b
		t.Fatalf("query count = %v, want 2", out["count"])
	}

	// direction in
	resp, out = postJSON(t, leader.URL()+"/v1/query", map[string]any{
		"from":      []string{"c"},
		"direction": "in",
	})
	if resp.StatusCode != http.StatusOK || out["count"] != float64(3) {
		t.Fatalf("query in: %d %v", resp.StatusCode, out)
	}
}

func TestAPIHealthAndReadyGate(t *testing.T) {
	tc := newTestCluster(t, 1)
	leader := tc.leaderWithin(10 * time.Second)
	// No writes → not ready (empty graph unpublished) → 503
	resp, _ := getJSON(t, leader.URL()+"/v1/health")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("not-ready health: %d", resp.StatusCode)
	}
	resp, _ = getJSON(t, leader.URL()+"/v1/stats")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("not-ready stats: %d", resp.StatusCode)
	}
	postJSON(t, leader.URL()+"/v1/batch", map[string]any{
		"nodes": []map[string]any{{"key": "a", "label": "S"}},
	})
	tc.waitAllPublished(1, 0, 5*time.Second)
	resp, out := getJSON(t, leader.URL()+"/v1/health")
	if resp.StatusCode != http.StatusOK || out["status"] != "ok" {
		t.Fatalf("ready health: %d %v", resp.StatusCode, out)
	}
}

func TestAPINeighborsAndCycles(t *testing.T) {
	tc := newTestCluster(t, 1)
	leader := tc.leaderWithin(10 * time.Second)
	postJSON(t, leader.URL()+"/v1/batch", map[string]any{
		"nodes": []map[string]any{{"key": "a", "label": "S"}, {"key": "b", "label": "S"}, {"key": "c", "label": "S"}},
		"edges": []map[string]any{
			{"srcKey": "a", "dstKey": "b", "label": "E"},
			{"srcKey": "b", "dstKey": "c", "label": "E"},
			{"srcKey": "c", "dstKey": "a", "label": "E"},
		},
	})
	tc.waitAllPublished(3, 3, 5*time.Second)

	resp, out := getJSON(t, leader.URL()+"/v1/neighbors?key=a&dir=out")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("neighbors: %d", resp.StatusCode)
	}
	if ns, _ := out["nodes"].([]any); len(ns) != 1 {
		t.Fatalf("neighbors = %v", out)
	}
	resp, out = getJSON(t, leader.URL()+"/v1/cycles")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cycles: %d", resp.StatusCode)
	}
	if cs, _ := out["cycles"].([]any); len(cs) != 1 {
		t.Fatalf("cycles = %v", out)
	}
}

func TestAPIJoinEndpoint(t *testing.T) {
	tc := newTestCluster(t, 2) // join already exercised in the helper; here we verify protocol details
	leader := tc.leaderWithin(10 * time.Second)
	// Duplicate join of the same node (idempotent case handled by raft AddVoter, not treated as an error path)
	resp, out := postJSON(t, leader.URL()+"/v1/join", map[string]any{
		"nodeID":   "n2",
		"raftAddr": tc.nodes[1].node.cfg.RaftAddr,
		"httpAddr": tc.nodes[1].URL(),
	})
	if resp.StatusCode == http.StatusOK {
		return // Idempotent success, acceptable
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("join duplicate request: %d %v", resp.StatusCode, out)
	}
}

func TestNodePublishedIsSealedGraph(t *testing.T) {
	tc := newTestCluster(t, 1)
	leader := tc.leaderWithin(10 * time.Second)
	postJSON(t, leader.URL()+"/v1/batch", map[string]any{
		"nodes": []map[string]any{{"key": "a", "label": "S"}, {"key": "b", "label": "S"}},
		"edges": []map[string]any{{"srcKey": "a", "dstKey": "b", "label": "E"}},
	})
	tc.waitAllPublished(2, 1, 5*time.Second)
	p := leader.node.Published()
	if p == nil || !p.Sealed() {
		t.Fatal("Published should return a sealed graph")
	}
	r, err := p.Reachable("a")
	if err != nil || len(r) != 2 {
		t.Fatalf("Published graph query: %v %v", r, err)
	}
	var _ *vectra.Graph = p
}

func TestAPIBatchDeletes(t *testing.T) {
	tc := newTestCluster(t, 1)
	leader := tc.leaderWithin(10 * time.Second)
	postJSON(t, leader.URL()+"/v1/batch", map[string]any{
		"nodes": []map[string]any{
			{"key": "a", "label": "Svc"}, {"key": "b", "label": "Svc"},
		},
		"edges": []map[string]any{{"srcKey": "a", "dstKey": "b", "label": "E"}},
	})
	resp, out := postJSON(t, leader.URL()+"/v1/batch", map[string]any{
		"edgeDeletes": []map[string]any{{"srcKey": "a", "dstKey": "b", "label": "E"}},
		"nodeDeletes": []string{"b"},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if out["edgesDeleted"] != float64(1) || out["nodesDeleted"] != float64(1) {
		t.Errorf("out = %v", out)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if p := leader.node.Published(); p != nil && p.NodeCount() == 1 && p.EdgeCount() == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("published graph never reached 1/0")
}
