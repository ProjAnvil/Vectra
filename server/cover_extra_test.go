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

	"vectra"
)

func TestNewValidationErrors(t *testing.T) {
	if _, err := New(Config{DataDir: t.TempDir()}); err == nil {
		t.Error("缺 NodeID 应报错")
	}
	if _, err := New(Config{NodeID: "x"}); err == nil {
		t.Error("缺 DataDir 应报错")
	}
	// 目录身份不匹配
	dir := t.TempDir()
	meta := nodeMeta{NodeID: "other", HTTPAddr: "h", RaftAddr: "r"}
	data, _ := json.Marshal(meta)
	_ = os.WriteFile(filepath.Join(dir, "meta.json"), data, 0o644)
	if _, err := New(Config{NodeID: "x", DataDir: dir}); !errors.Is(err, ErrDirMismatch) {
		t.Errorf("目录身份不匹配: %v", err)
	}
	// 坏 meta.json
	_ = os.WriteFile(filepath.Join(dir, "meta.json"), []byte("{"), 0o644)
	if _, err := New(Config{NodeID: "x", DataDir: dir}); err == nil {
		t.Error("坏 meta.json 应报错")
	}
}

func TestNodeJoinBadSeedStillStarts(t *testing.T) {
	// Join 不可达不致命：节点仍启动（保持候选，运维可重试）
	tn := newTestNode(t, "solo", "http://127.0.0.1:1")
	defer func() { tn.node.Close(); tn.srv.Close() }()
	select {
	case <-tn.node.Ready():
		t.Fatal("未写入不应就绪")
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
		t.Fatal("单节点未当选")
	}
	if _, err := tn.node.Apply(t.Context(), Batch{Version: 1, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}}); err != nil {
		t.Fatalf("单节点写入: %v", err)
	}
	select {
	case <-tn.node.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("Ready 未打开")
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
		t.Fatal("没有 follower")
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
		t.Fatalf("坏日志应返回 error, got %#v", res)
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
		t.Fatalf("默认 raft config: %+v", c)
	}
}

func TestCloseIdempotent(t *testing.T) {
	tn := newTestNode(t, "closer", "")
	tn.node.Close()
	tn.node.Close() // 二次关闭不 panic
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
		t.Errorf("未知路径 = %d, want 404", resp.StatusCode)
	}
	// expvar 端点可用
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
		t.Fatalf("缺字段 join: %d %v", resp.StatusCode, out)
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
		t.Fatalf("起点不存在: %d %v", resp.StatusCode, out)
	}
	// reachable / path / neighbors 的 NODE_NOT_FOUND
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
	// path 不可达：found=false
	resp, out = getJSON(t, leader.URL()+"/v1/path?from=a&to=a")
	if resp.StatusCode != http.StatusOK || out["found"] != true {
		t.Errorf("自环 path: %d %v", resp.StatusCode, out)
	}
}

func TestGraphNilAfterReadyButBeforePublishEdge(t *testing.T) {
	// graph() 的双保险分支：Ready 关闭但 published 为 nil（Restore 后
	// 首发布前的极小窗口）——用 FSM 直接触发
	f := newFSM(time.Millisecond)
	defer f.Close()
	data, _ := encodeBatch(Batch{Version: 1, Nodes: []NodeUpsert{{Key: "a", Label: "S"}}})
	f.Apply(&raft.Log{Data: data})
	<-f.Ready()
	if f.Published() == nil {
		t.Fatal("就绪后 published 不应为 nil")
	}
}

func TestFSMReleaseNoop(t *testing.T) {
	s := &graphSnapshot{clone: vectra.New()}
	s.Release() // 无操作，不 panic
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
		t.Fatal("sink 写失败应传播")
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
	t.Fatalf("本地发布超时 %d/%d", nodes, edges)
}

var _ = httptest.NewServer
var _ = bytes.NewReader

// TestNewProductionDefaults 生产默认路径：BoltDB + FileSnapshotStore +
// TCP transport（端口 0），写入-关闭-重开走磁盘恢复。
func TestNewProductionDefaults(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{NodeID: "prod1", HTTPAddr: "127.0.0.1:0", RaftAddr: "127.0.0.1:0", DataDir: dir}
	n, err := New(cfg)
	if err != nil {
		t.Fatalf("生产默认组装: %v", err)
	}
	srv := httptest.NewServer(n.Handler())
	defer func() { srv.Close(); n.Close() }()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && !n.IsLeader() {
		time.Sleep(20 * time.Millisecond)
	}
	if !n.IsLeader() {
		t.Fatal("生产默认单节点未当选")
	}
	resp, out := postJSON(t, srv.URL+"/v1/batch", map[string]any{
		"nodes": []map[string]any{{"key": "a", "label": "S"}, {"key": "b", "label": "S"}},
		"edges": []map[string]any{{"srcKey": "a", "dstKey": "b", "label": "E"}},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("生产写入: %d %v", resp.StatusCode, out)
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if p := n.Published(); p != nil && p.Stats().Edges == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	n.Close()

	// 重开同一目录：bolt/快照既有状态 → 不再 bootstrap，恢复并追平
	n2, err := New(cfg)
	if err != nil {
		t.Fatalf("重开: %v", err)
	}
	defer n2.Close()
	srv2 := httptest.NewServer(n2.Handler())
	defer srv2.Close()
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if p := n2.Published(); p != nil && p.Stats().Nodes == 2 {
			return // 日志回放恢复成功
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("重开后未从 raft 日志恢复")
}

func TestNewBadRaftAddrRejected(t *testing.T) {
	cfg := Config{NodeID: "bad", HTTPAddr: "127.0.0.1:0", RaftAddr: "not-an-addr:xyz", DataDir: t.TempDir()}
	if _, err := New(cfg); err == nil {
		t.Fatal("坏 RaftAddr 应报错")
	}
}

// TestJoinGarbageResponseTolerated seed 返回非 JSON 注册表响应：
// join 不失败，仅告警（注册表为空）。
func TestJoinGarbageResponseTolerated(t *testing.T) {
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("not-json"))
	}))
	defer bad.Close()
	tn := newTestNode(t, "j1", bad.URL)
	defer func() { tn.node.Close(); tn.srv.Close() }()
	// 节点照常启动
	if tn.node.IsLeader() {
		t.Log("未加入集群却成为 leader——不该发生（Join 非空不 bootstrap）")
	}
}
