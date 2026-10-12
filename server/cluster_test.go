package server

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// ============================================================
// Three-node in-process chaos cluster (cluster edition of the
// Graphalytics robustness axis)
// ============================================================

// disconnect simulates a node losing contact (bidirectional transport break, ungraceful).
func (tc *testCluster) disconnect(tn *testNode) {
	tn.trans.DisconnectAll()
	for _, other := range tc.nodes {
		if other != tn {
			other.trans.Disconnect(raft.ServerAddress(tn.node.cfg.RaftAddr))
		}
	}
}

// reconnect restores bidirectional connectivity.
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
		t.Fatalf("write svc-%d failed: %d %v", n, resp.StatusCode, out)
	}
}

// TestClusterBootstrapAndReplicate T1: bootstrap → leader election → write → three-replica consistency.
func TestClusterBootstrapAndReplicate(t *testing.T) {
	tc := newTestCluster(t, 3)
	leader := tc.leaderWithin(15 * time.Second)
	writeBatch(t, leader, 1)
	writeBatch(t, leader, 2)
	tc.waitAllPublished(2, 0, 15*time.Second)

	// All three nodes' stats identical
	for _, tn := range tc.nodes {
		resp, out := getJSON(t, tn.URL()+"/v1/stats")
		if resp.StatusCode != http.StatusOK || out["nodes"] != float64(2) {
			t.Fatalf("node %s stats: %d %v", tn.node.cfg.NodeID, resp.StatusCode, out)
		}
	}
}

// TestClusterLeaderFailover T2: leader loses contact (ungraceful) → remaining
// nodes re-elect → writes continue → old leader catches up after returning.
func TestClusterLeaderFailover(t *testing.T) {
	tc := newTestCluster(t, 3)
	leader := tc.leaderWithin(15 * time.Second)
	writeBatch(t, leader, 1)
	tc.waitAllPublished(1, 0, 15*time.Second)

	// Leader goes down
	tc.disconnect(leader)

	// Remaining two nodes elect a new leader
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
		t.Fatal("no new leader elected after leader went down")
	}
	if newLeader == leader {
		t.Fatal("the disconnected old leader should no longer be leader")
	}

	// New leader continues writing
	writeBatch(t, newLeader, 2)
	writeBatch(t, newLeader, 3)

	// Two live nodes catch up (disconnected node excluded)
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

	// Old leader returns → catches up via log replay (T3 scenario)
	tc.reconnect(leader)
	tc.waitAllPublished(3, 0, 20*time.Second)

	// Cluster as a whole is still writable
	l2 := tc.leaderWithin(15 * time.Second)
	writeBatch(t, l2, 4)
	tc.waitAllPublished(4, 0, 20*time.Second)
}

// TestClusterFollowerCatchupByLog Writes while a follower is offline: commits
// made during the outage are caught up via log replay after reconnection
// (not just the snapshot path).
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
	// The other surviving node catches up
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

// TestClusterEdgesReplicatedAcrossFailover Edge data integrity across failover:
// edges written before the crash plus edges written by the new leader are all
// visible after final convergence, with correct reachability.
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
		t.Fatalf("initial write: %d %v", resp.StatusCode, out)
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
		t.Fatal("no new leader")
	}
	// New leader adds one more edge
	resp, out = postJSON(t, newLeader.URL()+"/v1/batch", map[string]any{
		"edges": []map[string]any{
			{"srcKey": "gateway", "dstKey": "db", "label": "DIRECT"},
		},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("new leader write: %d %v", resp.StatusCode, out)
	}

	tc.reconnect(leader)
	tc.waitAllPublished(3, 3, 20*time.Second)

	// After convergence: data from both leader terms queries identically on all replicas
	for _, tn := range tc.nodes {
		resp, out := getJSON(t, tn.URL()+"/v1/path?from=gateway&to=db")
		if resp.StatusCode != http.StatusOK || out["found"] != true || out["hops"] != float64(1) {
			t.Fatalf("node %s path: %d %v", tn.node.cfg.NodeID, resp.StatusCode, out)
		}
	}
}

// TestClusterReadsAvailableDuringSeal Uninterrupted-query claim: while writes
// and publishes run concurrently, continuous queries on a follower never fail
// (lock-free reads + atomic pointer swap).
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
				case fail <- fmt.Errorf("query failed during publish: %d", resp.StatusCode):
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
