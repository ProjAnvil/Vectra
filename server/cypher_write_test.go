package server

// Cypher write-statement routing over the HTTP API (2026-10-29 cypher
// write-clauses plan, Task 8): writes replicate through raft exactly like
// /v1/batch (the leader's FSM re-executes the statement and the response
// summary counts come from that APPLY result), reads keep the pre-branch
// response shape byte-for-byte.

import (
	"net/http"
	"testing"
	"time"
)

// TestCypherWriteRaftSingle pins the single-node write path: a CREATE
// statement POSTed to /v1/cypher commits through raft (not a mutation of
// the local published clone), responds 200 with the summary counters from
// the APPLY result, and the created entities are queryable afterwards.
func TestCypherWriteRaftSingle(t *testing.T) {
	tc := newTestCluster(t, 1)
	leader := tc.leaderWithin(10 * time.Second)

	resp, out := postJSON(t, leader.URL()+"/v1/cypher", map[string]any{
		"query": `CREATE (a:Service {key:'gateway', env:'prod'})-[:CALLS {w:1}]->(b:Service {key:'orders'})`,
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cypher write: %d %v", resp.StatusCode, out)
	}
	sum, ok := out["summary"].(map[string]any)
	if !ok {
		t.Fatalf("write response missing summary: %v", out)
	}
	if sum["createdNodes"] != float64(2) || sum["createdEdges"] != float64(1) {
		t.Fatalf("summary = %v want createdNodes=2 createdEdges=1", sum)
	}
	if _, has := out["count"]; has {
		t.Fatalf("write response must not carry the read-path count key: %v", out)
	}

	// The write went through the FSM: the published graph carries it.
	tc.waitAllPublished(2, 1, 5*time.Second)
	resp, out = getJSON(t, leader.URL()+"/v1/nodes/gateway")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("gateway not queryable after cypher write: %d %v", resp.StatusCode, out)
	}

	// A read statement sees it — and carries no summary of its own.
	resp, out = postJSON(t, leader.URL()+"/v1/cypher", map[string]any{
		"query": `MATCH (a {key:'gateway'})-[:CALLS]->(b) RETURN a.key, b.key`,
	})
	if resp.StatusCode != http.StatusOK || out["count"] != float64(1) {
		t.Fatalf("read after write: %d %v", resp.StatusCode, out)
	}
	if _, has := out["summary"]; has {
		t.Fatalf("read response must not carry summary: %v", out)
	}
}

// TestCypherWriteFollowerForward pins leader-check parity with /v1/batch
// (the existing forward spec, see the non-leader batch test in api_test.go):
// a write statement hitting a follower is rejected 503 NOT_LEADER with
// Retry-After and a non-self leader hint, and the same statement applied
// through the leader commits via raft and replicates to every published
// graph in the cluster.
func TestCypherWriteFollowerForward(t *testing.T) {
	tc := newTestCluster(t, 3)
	leader := tc.leaderWithin(15 * time.Second)
	var follower *testNode
	for _, tn := range tc.nodes {
		if tn != leader {
			follower = tn
			break
		}
	}
	resp, out := postJSON(t, follower.URL()+"/v1/cypher", map[string]any{
		"query": `CREATE (n:Service {key:'fwd'})`,
	})
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("non-leader cypher write should be 503: %d %v", resp.StatusCode, out)
	}
	if errBody, ok := out["error"].(map[string]any); !ok || errBody["code"] != "NOT_LEADER" {
		t.Fatalf("error code: %v", out)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Error("missing Retry-After header")
	}
	// Leader hint header is optional (registry is best effort); if present it must not point at the follower itself.
	if h := resp.Header.Get("X-Vectra-Leader"); h != "" && h == follower.URL() {
		t.Error("leader hint points to itself")
	}

	// The same statement through the leader applies on every replica.
	resp, out = postJSON(t, leader.URL()+"/v1/cypher", map[string]any{
		"query": `CREATE (n:Service {key:'fwd'})`,
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("leader cypher write: %d %v", resp.StatusCode, out)
	}
	sum, ok := out["summary"].(map[string]any)
	if !ok || sum["createdNodes"] != float64(1) {
		t.Fatalf("summary = %v want createdNodes=1", sum)
	}
	tc.waitAllPublished(1, 0, 10*time.Second)
}

// TestCypherReadUnchangedOnServer pins the read contract: read statements
// keep exactly the pre-branch response shape — columns/rows/count, no
// summary key — served from the local published graph with the snapshot
// header, with no raft hop.
func TestCypherReadUnchangedOnServer(t *testing.T) {
	tc := newTestCluster(t, 1)
	leader := tc.leaderWithin(10 * time.Second)
	postJSON(t, leader.URL()+"/v1/batch", map[string]any{
		"nodes": []map[string]any{
			{"key": "gw", "label": "Service", "props": map[string]any{"type": "controller"}},
			{"key": "order", "label": "Service", "props": map[string]any{"type": "service"}},
		},
		"edges": []map[string]any{{"srcKey": "gw", "dstKey": "order", "label": "CALLS"}},
	})
	tc.waitAllPublished(2, 1, 5*time.Second)

	resp, out := postJSON(t, leader.URL()+"/v1/cypher", map[string]any{
		"query": `MATCH (b {type:'controller'})-[:CALLS]->(s) RETURN b.key, s.key`,
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cypher read: %d %v", resp.StatusCode, out)
	}
	// Exact key set of the pre-branch response: columns, rows, count.
	for _, k := range []string{"columns", "rows", "count"} {
		if _, ok := out[k]; !ok {
			t.Fatalf("read response missing %q: %v", k, out)
		}
	}
	if len(out) != 3 {
		t.Fatalf("read response must carry exactly columns/rows/count: %v", out)
	}
	if out["count"] != float64(1) {
		t.Fatalf("count = %v want 1", out["count"])
	}
	row := out["rows"].([]any)[0].(map[string]any)
	if row["b.key"] != "gw" || row["s.key"] != "order" {
		t.Fatalf("row: %v", row)
	}
	if resp.Header.Get("X-Vectra-Snapshot") == "" {
		t.Fatal("snapshot header missing")
	}
}
