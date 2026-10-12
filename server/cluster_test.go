package server

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// ============================================================
// 三节点进程内混沌集群（Graphalytics robustacy 轴的集群版）
// ============================================================

// disconnect 模拟节点失联（双向断开 transport，非优雅）。
func (tc *testCluster) disconnect(tn *testNode) {
	tn.trans.DisconnectAll()
	for _, other := range tc.nodes {
		if other != tn {
			other.trans.Disconnect(raft.ServerAddress(tn.node.cfg.RaftAddr))
		}
	}
}

// reconnect 恢复双向连通。
func (tc *testCluster) reconnect(tn *testNode) {
	for _, other := range tc.nodes {
		if other != tn {
			tn.trans.Connect(raft.ServerAddress(other.node.cfg.RaftAddr), other.trans)
			other.trans.Connect(raft.ServerAddress(tn.node.cfg.RaftAddr), tn.trans)
		}
	}
}

func writeBatch(t *testing.T, tn *testNode, n int) {
	t.Helper()
	body := map[string]any{
		"nodes": []map[string]any{
			{"key": fmt.Sprintf("svc-%d", n), "label": "Service"},
		},
	}
	resp, out := postJSON(t, tn.URL()+"/v1/batch", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("写入 svc-%d 失败: %d %v", n, resp.StatusCode, out)
	}
}

// TestClusterBootstrapAndReplicate T1：引导 → 选主 → 写入 → 三副本一致。
func TestClusterBootstrapAndReplicate(t *testing.T) {
	tc := newTestCluster(t, 3)
	leader := tc.leaderWithin(15 * time.Second)
	writeBatch(t, leader, 1)
	writeBatch(t, leader, 2)
	tc.waitAllPublished(2, 0, 15*time.Second)

	// 三节点 stats 完全一致
	for _, tn := range tc.nodes {
		resp, out := getJSON(t, tn.URL()+"/v1/stats")
		if resp.StatusCode != http.StatusOK || out["nodes"] != float64(2) {
			t.Fatalf("节点 %s stats: %d %v", tn.node.cfg.NodeID, resp.StatusCode, out)
		}
	}
}

// TestClusterLeaderFailover T2：leader 失联（非优雅）→ 剩余节点重选 →
// 写入继续 → 旧 leader 回归后追平。
func TestClusterLeaderFailover(t *testing.T) {
	tc := newTestCluster(t, 3)
	leader := tc.leaderWithin(15 * time.Second)
	writeBatch(t, leader, 1)
	tc.waitAllPublished(1, 0, 15*time.Second)

	// leader 宕机
	tc.disconnect(leader)

	// 剩余两节点选出新 leader
	var newLeader *testNode
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		for _, tn := range tc.nodes {
			if tn != leader && tn.node.IsLeader() {
				newLeader = tn
				break
			}
		}
		if newLeader != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if newLeader == nil {
		t.Fatal("leader 宕机后没有新 leader 当选")
	}
	if newLeader == leader {
		t.Fatal("失联的旧 leader 不应仍是 leader")
	}

	// 新 leader 继续写入
	writeBatch(t, newLeader, 2)
	writeBatch(t, newLeader, 3)

	// 两个存活节点追平（失联节点除外）
	deadline = time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, tn := range tc.nodes {
			if tn == leader {
				continue
			}
			p := tn.node.Published()
			if p == nil || p.Stats().Nodes != 3 {
				ok = false
				break
			}
		}
		if ok {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// 旧 leader 回归 → 日志回放追平（T3 场景）
	tc.reconnect(leader)
	tc.waitAllPublished(3, 0, 20*time.Second)

	// 集群整体仍可写
	l2 := tc.leaderWithin(15 * time.Second)
	writeBatch(t, l2, 4)
	tc.waitAllPublished(4, 0, 20*time.Second)
}

// TestClusterFollowerCatchupByLog 边写边离线：follower 掉线期间的
// 提交在重连后经日志回放补齐（不只是快照路径）。
func TestClusterFollowerCatchupByLog(t *testing.T) {
	tc := newTestCluster(t, 3)
	leader := tc.leaderWithin(15 * time.Second)
	writeBatch(t, leader, 1)
	tc.waitAllPublished(1, 0, 15*time.Second)

	var follower *testNode
	for _, tn := range tc.nodes {
		if tn != leader {
			follower = tn
			break
		}
	}
	tc.disconnect(follower)
	for i := 2; i <= 6; i++ {
		writeBatch(t, leader, i)
	}
	// 存活的另一个节点追平
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, tn := range tc.nodes {
			if tn == follower {
				continue
			}
			p := tn.node.Published()
			if p == nil || p.Stats().Nodes != 6 {
				ok = false
				break
			}
		}
		if ok {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	tc.reconnect(follower)
	tc.waitAllPublished(6, 0, 20*time.Second)
}

// TestClusterEdgesReplicatedAcrossFailover 跨故障切换的边数据完整性：
// 宕机前写的边 + 新 leader 写的边在最终收敛后全部可见且可达性正确。
func TestClusterEdgesReplicatedAcrossFailover(t *testing.T) {
	tc := newTestCluster(t, 3)
	leader := tc.leaderWithin(15 * time.Second)
	resp, out := postJSON(t, leader.URL()+"/v1/batch", map[string]any{
		"nodes": []map[string]any{
			{"key": "gateway", "label": "Service"},
			{"key": "auth", "label": "Service"},
			{"key": "db", "label": "Storage"},
		},
		"edges": []map[string]any{
			{"srcKey": "gateway", "dstKey": "auth", "label": "CALLS"},
			{"srcKey": "auth", "dstKey": "db", "label": "QUERY"},
		},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("初始写入: %d %v", resp.StatusCode, out)
	}
	tc.waitAllPublished(3, 2, 15*time.Second)

	tc.disconnect(leader)
	var newLeader *testNode
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && newLeader == nil {
		for _, tn := range tc.nodes {
			if tn != leader && tn.node.IsLeader() {
				newLeader = tn
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if newLeader == nil {
		t.Fatal("无新 leader")
	}
	// 新 leader 补一条边
	resp, out = postJSON(t, newLeader.URL()+"/v1/batch", map[string]any{
		"edges": []map[string]any{
			{"srcKey": "gateway", "dstKey": "db", "label": "DIRECT"},
		},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("新 leader 写入: %d %v", resp.StatusCode, out)
	}

	tc.reconnect(leader)
	tc.waitAllPublished(3, 3, 20*time.Second)

	// 收敛后：跨两任 leader 的数据在所有副本上查询一致
	for _, tn := range tc.nodes {
		resp, out := getJSON(t, tn.URL()+"/v1/path?from=gateway&to=db")
		if resp.StatusCode != http.StatusOK || out["found"] != true || out["hops"] != float64(1) {
			t.Fatalf("节点 %s path: %d %v", tn.node.cfg.NodeID, resp.StatusCode, out)
		}
	}
}

// TestClusterReadsAvailableDuringSeal 查询不中断声明：写入与发布
// 并发进行时，follower 上的连续查询不失败（免锁读 + 原子指针交换）。
func TestClusterReadsAvailableDuringSeal(t *testing.T) {
	tc := newTestCluster(t, 3)
	leader := tc.leaderWithin(15 * time.Second)
	writeBatch(t, leader, 1)
	tc.waitAllPublished(1, 0, 15*time.Second)

	var follower *testNode
	for _, tn := range tc.nodes {
		if tn != leader {
			follower = tn
			break
		}
	}
	stop := make(chan struct{})
	fail := make(chan error, 1)
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			resp, _ := getJSON(t, follower.URL()+"/v1/stats")
			if resp.StatusCode != http.StatusOK {
				select {
				case fail <- fmt.Errorf("查询在发布期失败: %d", resp.StatusCode):
				default:
				}
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	for i := 2; i <= 20; i++ {
		writeBatch(t, leader, i)
	}
	close(stop)
	select {
	case err := <-fail:
		t.Fatal(err)
	default:
	}
	tc.waitAllPublished(20, 0, 20*time.Second)
}
