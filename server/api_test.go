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
// 进程内测试集群（InmemTransport + httptest）——T4 API 测试与
// T5 混沌测试共用。
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

// newTestCluster 建集群：n1 空目录自动引导并当选，其余节点带 Join
// 创建（New 内走真实 /v1/join 协议）。transport 在 New 之前互联，
// 保证 AddVoter 能等到多数派提交。
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
		t.Fatal("n1 未当选 leader")
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

	// 先与既有节点互联（join 的 AddVoter 需要可达的多数派）
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

// testRaftConfig 混沌测试专用：tick 20ms、ElectionTick 12、HeartbeatTick 2。
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
	tc.t.Fatal("没有节点在期限内成为 leader")
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
	tc.t.Fatalf("集群未在期限内全部发布 %d/%d", nodes, edges)
}

// ============================================================
// HTTP API 契约测试
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
		t.Fatalf("batch 写入: %d %v", resp.StatusCode, out)
	}
	if out["nodesApplied"] != float64(3) || out["edgesApplied"] != float64(2) || out["edgesSkipped"] != float64(1) {
		t.Fatalf("applied 结果错误: %v", out)
	}

	tc.waitAllPublished(3, 2, 10*time.Second)

	// 查询端点（leader 上）
	resp, out = getJSON(t, leader.URL()+"/v1/reachable?from=gateway")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("reachable: %d", resp.StatusCode)
	}
	nodes, _ := out["nodes"].([]any)
	if len(nodes) != 3 {
		t.Fatalf("reachable nodes = %v, want 3 个", nodes)
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

	// follower 上的读（最终一致）：也能查到
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
		t.Fatalf("非 leader 写入应 503: %d %v", resp.StatusCode, out)
	}
	if errBody, ok := out["error"].(map[string]any); !ok || errBody["code"] != "NOT_LEADER" {
		t.Fatalf("错误码: %v", out)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Error("缺 Retry-After 头")
	}
	// leader 提示头可选（注册表尽力而为），不强制；但给了就必须可解析
	if h := resp.Header.Get("X-Vectra-Leader"); h != "" && h == follower.URL() {
		t.Error("leader 提示指向了自己")
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

	// 缺 from
	resp, out := postJSON(t, leader.URL()+"/v1/query", map[string]any{"maxDepth": 2})
	if resp.StatusCode != http.StatusBadRequest || out["error"].(map[string]any)["code"] != "VALIDATION" {
		t.Fatalf("缺 from: %d %v", resp.StatusCode, out)
	}
	// 不存在的节点
	resp, out = getJSON(t, leader.URL()+"/v1/nodes/missing")
	if resp.StatusCode != http.StatusNotFound || out["error"].(map[string]any)["code"] != "NODE_NOT_FOUND" {
		t.Fatalf("缺节点: %d %v", resp.StatusCode, out)
	}
	// 坏 JSON
	resp2, err := http.Post(leader.URL()+"/v1/query", "application/json", bytes.NewReader([]byte("{")))
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusBadRequest {
		t.Fatalf("坏 JSON: %d", resp2.StatusCode)
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
	// 未写入 → 未就绪（空图未发布）→ 503
	resp, _ := getJSON(t, leader.URL()+"/v1/health")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("未就绪 health: %d", resp.StatusCode)
	}
	resp, _ = getJSON(t, leader.URL()+"/v1/stats")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("未就绪 stats: %d", resp.StatusCode)
	}
	postJSON(t, leader.URL()+"/v1/batch", map[string]any{
		"nodes": []map[string]any{{"key": "a", "label": "S"}},
	})
	tc.waitAllPublished(1, 0, 5*time.Second)
	resp, out := getJSON(t, leader.URL()+"/v1/health")
	if resp.StatusCode != http.StatusOK || out["status"] != "ok" {
		t.Fatalf("就绪后 health: %d %v", resp.StatusCode, out)
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
	tc := newTestCluster(t, 2) // join 已在 helper 里走通，这里验证协议细节
	leader := tc.leaderWithin(10 * time.Second)
	// 重复 join 同一节点（幂等场景由 raft AddVoter 处理，不视为错误路径）
	resp, out := postJSON(t, leader.URL()+"/v1/join", map[string]any{
		"nodeID":   "n2",
		"raftAddr": tc.nodes[1].node.cfg.RaftAddr,
		"httpAddr": tc.nodes[1].URL(),
	})
	if resp.StatusCode == http.StatusOK {
		return // 幂等成功，可接受
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("join 重复请求: %d %v", resp.StatusCode, out)
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
		t.Fatal("Published 应返回密封图")
	}
	r, err := p.Reachable("a")
	if err != nil || len(r) != 2 {
		t.Fatalf("Published 图查询: %v %v", r, err)
	}
	var _ *vectra.Graph = p
}
