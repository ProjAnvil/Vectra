package server

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// ============================================================
// Authenticated cluster formation (task 3): the join protocol
// carries the bearer token so an auth-gated cluster can grow.
// ============================================================

// writeTokenFile materializes one valid token file and returns its path.
func writeTokenFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tokens")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// newAuthTestNode mirrors newTestNode (api_test.go) but carries an
// AuthTokenFile, so every node in these clusters runs the bearer middleware
// and the join POST must present the token to get past it.
func newAuthTestNode(t *testing.T, id, join, tokenFile string, existing ...*testNode) *testNode {
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
		AuthTokenFile: tokenFile,
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

// postJSONAuth is postJSON (api_test.go) plus a Bearer token.
func postJSONAuth(t *testing.T, url, bearer string, body any) (*http.Response, map[string]any) {
	t.Helper()
	data, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+bearer)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return resp, out
}

// getJSONAuth is getJSON (api_test.go) plus a Bearer token.
func getJSONAuth(t *testing.T, url, bearer string) (*http.Response, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return resp, out
}

// hasVoter reports whether id is currently a voter in n's raft
// configuration. Membership has no HTTP endpoint; observed in-package.
func hasVoter(n *Node, id string) bool {
	fut := n.r.GetConfiguration()
	if err := fut.Error(); err != nil {
		return false
	}
	for _, s := range fut.Configuration().Servers {
		if s.ID == raft.ServerID(id) && s.Suffrage == raft.Voter {
			return true
		}
	}
	return false
}

// waitAuthLeader blocks until tn's node is leader (single-node bootstrap).
func waitAuthLeader(t *testing.T, tn *testNode, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) && !tn.node.IsLeader() {
		time.Sleep(20 * time.Millisecond)
	}
	if !tn.node.IsLeader() {
		t.Fatalf("%s not elected leader", tn.node.cfg.NodeID)
	}
}

// assertNeverVoter fails if id becomes a voter on seed's configuration
// within the deadline (a rejected joiner must never slip in later).
func assertNeverVoter(t *testing.T, seed *testNode, id string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if hasVoter(seed.node, id) {
			t.Fatalf("%s became a voter despite the rejected join", id)
		}
		time.Sleep(25 * time.Millisecond)
	}
	if hasVoter(seed.node, id) {
		t.Fatalf("%s became a voter despite the rejected join", id)
	}
}

// TestClusterFormsWithAuth A three-node cluster where every node (and thus
// every /v1/join POST) runs behind the bearer middleware: joins must carry
// the token, then the cluster elects, replicates a write, and serves
// authenticated reads on followers.
func TestClusterFormsWithAuth(t *testing.T) {
	tokFile := writeTokenFile(t, "cluster-tok\n")

	n1 := newAuthTestNode(t, "n1", "", tokFile)
	waitAuthLeader(t, n1, 10*time.Second)
	n2 := newAuthTestNode(t, "n2", n1.URL(), tokFile, n1)
	n3 := newAuthTestNode(t, "n3", n1.URL(), tokFile, n1, n2)

	// The joins carried the token: every node is a voter.
	for _, tn := range []*testNode{n1, n2, n3} {
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) && !hasVoter(tn.node, tn.node.cfg.NodeID) {
			time.Sleep(20 * time.Millisecond)
		}
		if !hasVoter(tn.node, tn.node.cfg.NodeID) {
			t.Fatalf("%s never became a voter (join rejected?)", tn.node.cfg.NodeID)
		}
	}

	// The cluster is really gated: an unauthenticated write is a 401.
	resp, out := postJSON(t, n1.URL()+"/v1/batch", map[string]any{
		"nodes": []map[string]any{{"key": "a", "label": "S"}},
	})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated batch write: %d %v, want 401", resp.StatusCode, out)
	}

	// An authenticated write on the leader replicates everywhere.
	resp, out = postJSONAuth(t, n1.URL()+"/v1/batch", "cluster-tok", map[string]any{
		"nodes": []map[string]any{{"key": "a", "label": "S"}, {"key": "b", "label": "S"}},
		"edges": []map[string]any{{"srcKey": "a", "dstKey": "b", "label": "E"}},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("authenticated batch write: %d %v", resp.StatusCode, out)
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, tn := range []*testNode{n1, n2, n3} {
			p := tn.node.Published()
			if p == nil || p.Stats().Nodes != 2 || p.Stats().Edges != 1 {
				ok = false
				break
			}
		}
		if ok {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, tn := range []*testNode{n1, n2, n3} {
		p := tn.node.Published()
		if p == nil || p.Stats().Nodes != 2 || p.Stats().Edges != 1 {
			t.Fatalf("node %s published = %v, want 2 nodes / 1 edge", tn.node.cfg.NodeID, p)
		}
	}

	// Readable on a follower, with the token (cluster_test.go's assertion shape).
	var follower *testNode
	for _, tn := range []*testNode{n2, n3} {
		follower = tn
		break
	}
	resp, out = getJSONAuth(t, follower.URL()+"/v1/stats", "cluster-tok")
	if resp.StatusCode != http.StatusOK || out["nodes"] != float64(2) || out["edges"] != float64(1) {
		t.Fatalf("follower stats: %d %v", resp.StatusCode, out)
	}
}

// TestJoinRejectedWithoutToken A joiner with no token file cannot join a
// token-gated seed: its POST is a 401 and it never becomes a voter.
func TestJoinRejectedWithoutToken(t *testing.T) {
	tokFile := writeTokenFile(t, "seed-tok\n")
	seed := newAuthTestNode(t, "n1", "", tokFile)
	waitAuthLeader(t, seed, 10*time.Second)
	joiner := newAuthTestNode(t, "n2", seed.URL(), "", seed) // no AuthTokenFile

	// The join POST itself is a 401 (New already tried once; repeat the
	// exact wire call for a deterministic observation).
	err := joiner.node.Join(seed.URL())
	if err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("Join without token: err = %v, want a 401 failure", err)
	}

	// Membership observed in-package: the seed never gains n2 as voter.
	assertNeverVoter(t, seed, "n2", time.Second)

	// The rejected joiner never entered any configuration (initial config
	// stays servers=[]) and elected no leader — join failure is non-fatal.
	if hasVoter(joiner.node, "n2") {
		t.Fatal("rejected joiner considers itself a voter")
	}
	if joiner.node.IsLeader() {
		t.Fatal("rejected joiner should not become leader")
	}
}

// TestJoinWithWrongTokenFails A joiner presenting a different token gets
// the same 401 and never joins: one trust domain per cluster.
func TestJoinWithWrongTokenFails(t *testing.T) {
	right := writeTokenFile(t, "right-tok\n")
	wrong := writeTokenFile(t, "wrong-tok\n")
	seed := newAuthTestNode(t, "n1", "", right)
	waitAuthLeader(t, seed, 10*time.Second)
	joiner := newAuthTestNode(t, "n2", seed.URL(), wrong, seed)

	err := joiner.node.Join(seed.URL())
	if err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("Join with wrong token: err = %v, want a 401 failure", err)
	}

	assertNeverVoter(t, seed, "n2", time.Second)
	if hasVoter(joiner.node, "n2") {
		t.Fatal("rejected joiner considers itself a voter")
	}
	if joiner.node.IsLeader() {
		t.Fatal("rejected joiner should not become leader")
	}
}
