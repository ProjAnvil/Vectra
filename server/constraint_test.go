package server

// Unique-constraint enforcement across the server surfaces (spec
// 2026-10-30 §2.4/§2.6/§4.3): the FSM's node lane aborts on the first
// rejected upsert (no silent skip), every raft-backed write face answers a
// violation with 400 UNIQUE_VIOLATION (never 400 PARSE, never 500), the
// constraint DDL's own failure modes over the Cypher lane are client errors
// too (409 CONSTRAINT_EXISTS / 404 CONSTRAINT_NOT_FOUND / the declaration
// scan's 400 UNIQUE_VIOLATION — never 500), both replicas of a two-node
// cluster reject identically, and the constraint survives the GET /v1/backup
// restore round trip.

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/projanvil/vectra"
)

// constraintCreateStmt is the DDL entering through the Cypher lane — the
// server has no dedicated constraint endpoint; statement re-execution is
// the persistence (§4.3).
const constraintCreateStmt = `CREATE CONSTRAINT FOR (n:Person) REQUIRE n.email IS UNIQUE`

// violationFace pins the §2.6 wire contract on an error response: 400 with
// code UNIQUE_VIOLATION (not PARSE, not INTERNAL) and the violation payload
// in the message (constraint identity + contended value, "message from the
// error").
func violationFace(t *testing.T, resp *http.Response, out map[string]any) {
	t.Helper()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d (%v), want 400", resp.StatusCode, out)
	}
	e, ok := out["error"].(map[string]any)
	if !ok {
		t.Fatalf("response lacks the error object: %v", out)
	}
	if e["code"] != "UNIQUE_VIOLATION" {
		t.Fatalf("code = %v, want UNIQUE_VIOLATION (not PARSE, not INTERNAL)", e["code"])
	}
	msg, _ := e["message"].(string)
	if !strings.Contains(msg, "Person.email") || !strings.Contains(msg, "dup@x") {
		t.Fatalf("message %q lacks the violation payload (want Person.email + dup@x)", msg)
	}
}

// declareConstraintOverHTTP creates the Person.email constraint through the
// leader's /v1/cypher and fails the test unless it commits.
func declareConstraintOverHTTP(t *testing.T, base string) {
	t.Helper()
	resp, out := postJSON(t, base+"/v1/cypher", map[string]any{"query": constraintCreateStmt})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create constraint: %d %v", resp.StatusCode, out)
	}
}

// TestConstraintCypherFace: CREATE CONSTRAINT over /v1/cypher answers 200
// with summary constraintsCreated=1, and a violating CREATE through the
// same lane answers 400 UNIQUE_VIOLATION — not 400 PARSE (the statement
// parsed fine; the raw violation error must not be re-faced as a Cypher
// error) and not 500 (writeApplyErr's generic bucket).
func TestConstraintCypherFace(t *testing.T) {
	tc := newTestCluster(t, 1)
	leader := tc.leaderWithin(10 * time.Second)

	// The declare response carries the additive summary counter.
	resp, out := postJSON(t, leader.URL()+"/v1/cypher", map[string]any{"query": constraintCreateStmt})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create constraint: %d %v", resp.StatusCode, out)
	}
	sum, ok := out["summary"].(map[string]any)
	if !ok || sum["constraintsCreated"] != float64(1) {
		t.Fatalf("declare summary = %v, want constraintsCreated=1", out["summary"])
	}

	// A duplicate declare is an engine error (ErrConstraintExists), never a
	// silent success — and a CLIENT error, never 500: the VECTOR_INDEX_EXISTS
	// precedent face, 409 CONSTRAINT_EXISTS.
	resp, out = postJSON(t, leader.URL()+"/v1/cypher", map[string]any{"query": constraintCreateStmt})
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("duplicate create constraint: %d %v, want 409", resp.StatusCode, out)
	}
	e, ok := out["error"].(map[string]any)
	if !ok {
		t.Fatalf("duplicate create constraint: response lacks the error object: %v", out)
	}
	if e["code"] != "CONSTRAINT_EXISTS" {
		t.Fatalf("duplicate create constraint: code = %v, want CONSTRAINT_EXISTS (not INTERNAL)", e["code"])
	}

	// Seed the owned value through the batch lane, then violate via Cypher.
	resp, out = postJSON(t, leader.URL()+"/v1/batch", map[string]any{
		"nodes": []map[string]any{{"key": "a", "label": "Person", "props": map[string]any{"email": "dup@x"}}},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("seed write: %d %v", resp.StatusCode, out)
	}
	tc.waitAllPublished(1, 0, 5*time.Second)

	resp, out = postJSON(t, leader.URL()+"/v1/cypher", map[string]any{
		"query": `CREATE (n:Person {key:'b', email:'dup@x'})`,
	})
	violationFace(t, resp, out)

	// The violating CREATE never landed.
	if resp, out = getJSON(t, leader.URL()+"/v1/nodes/b"); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("violating node b: %d %v, want 404", resp.StatusCode, out)
	}
}

// TestConstraintDropUnknownFace: DROP CONSTRAINT for a pair never declared
// answers 404 CONSTRAINT_NOT_FOUND over /v1/cypher — the engine sentinel
// (ErrConstraintNotFound) replays deterministically on every replica and is
// a client error, so it must not fall to writeApplyErr's 500 INTERNAL.
func TestConstraintDropUnknownFace(t *testing.T) {
	tc := newTestCluster(t, 1)
	leader := tc.leaderWithin(10 * time.Second)

	resp, out := postJSON(t, leader.URL()+"/v1/cypher", map[string]any{
		"query": `DROP CONSTRAINT FOR (n:Person) REQUIRE n.email IS UNIQUE`,
	})
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("drop unknown constraint: %d %v, want 404", resp.StatusCode, out)
	}
	e, ok := out["error"].(map[string]any)
	if !ok {
		t.Fatalf("drop unknown constraint: response lacks the error object: %v", out)
	}
	if e["code"] != "CONSTRAINT_NOT_FOUND" {
		t.Fatalf("drop unknown constraint: code = %v, want CONSTRAINT_NOT_FOUND (not INTERNAL)", e["code"])
	}
}

// TestConstraintDeclScanFace: CREATE CONSTRAINT whose declaration validation
// scan finds legacy duplicate data (two live nodes already sharing the
// (label, prop) value, written before any constraint) answers 400
// UNIQUE_VIOLATION naming the constraint — the declaration-time face of the
// same client class as write-time violations. The engine raises its
// uniqueDeclError (unwraps to ErrUniqueViolation, but NOT a
// *UniqueViolationError), which previously slipped past the violation
// check and landed on 500 INTERNAL.
func TestConstraintDeclScanFace(t *testing.T) {
	tc := newTestCluster(t, 1)
	leader := tc.leaderWithin(10 * time.Second)

	// Pre-seed the duplicate pair — constraint-free writes, both land.
	resp, out := postJSON(t, leader.URL()+"/v1/batch", map[string]any{
		"nodes": []map[string]any{
			{"key": "a", "label": "Person", "props": map[string]any{"email": "dup@x"}},
			{"key": "b", "label": "Person", "props": map[string]any{"email": "dup@x"}},
		},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("seed write: %d %v", resp.StatusCode, out)
	}
	tc.waitAllPublished(2, 0, 5*time.Second)

	resp, out = postJSON(t, leader.URL()+"/v1/cypher", map[string]any{"query": constraintCreateStmt})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("declare over duplicate data: %d %v, want 400", resp.StatusCode, out)
	}
	e, ok := out["error"].(map[string]any)
	if !ok {
		t.Fatalf("declare over duplicate data: response lacks the error object: %v", out)
	}
	if e["code"] != "UNIQUE_VIOLATION" {
		t.Fatalf("declare over duplicate data: code = %v, want UNIQUE_VIOLATION (not INTERNAL)", e["code"])
	}
	msg, _ := e["message"].(string)
	if !strings.Contains(msg, "Person.email") {
		t.Fatalf("message %q lacks the constraint identity (want Person.email)", msg)
	}

	// Declaration is atomic: the constraint was NOT registered.
	if cs := tc.nodes[0].node.Published().UniqueConstraints(); len(cs) != 0 {
		t.Fatalf("constraint registered over duplicate data: %+v", cs)
	}
}

// TestConstraintBatchViolationFace: the §2.4 FSM abort surfaces on /v1/batch
// as 400 UNIQUE_VIOLATION (previously a silent skip returned 200 with the
// node missing), with the documented partial application: ops before the
// violation are durable, the violating op has zero side effects, ops after
// it never run.
func TestConstraintBatchViolationFace(t *testing.T) {
	tc := newTestCluster(t, 1)
	leader := tc.leaderWithin(10 * time.Second)
	declareConstraintOverHTTP(t, leader.URL())

	resp, out := postJSON(t, leader.URL()+"/v1/batch", map[string]any{
		"nodes": []map[string]any{
			{"key": "first", "label": "Person", "props": map[string]any{"email": "dup@x"}},
			{"key": "second", "label": "Person", "props": map[string]any{"email": "dup@x"}}, // violates
			{"key": "third", "label": "Person", "props": map[string]any{"email": "other@x"}},
		},
	})
	violationFace(t, resp, out)

	tc.waitAllPublished(1, 0, 5*time.Second) // "first" is durable
	if resp, out = getJSON(t, leader.URL()+"/v1/nodes/first"); resp.StatusCode != http.StatusOK {
		t.Fatalf("node first: %d %v, want 200 (partial application is durable)", resp.StatusCode, out)
	}
	for _, key := range []string{"second", "third"} {
		if resp, out = getJSON(t, leader.URL()+"/v1/nodes/"+key); resp.StatusCode != http.StatusNotFound {
			t.Fatalf("node %s: %d %v, want 404 (zero side effects / later ops never run)", key, resp.StatusCode, out)
		}
	}
}

// TestConstraintTxnViolationFace: /v1/txn rides the same FSM batch lane, so
// a violation aborts the guarded entry the same way — 400 UNIQUE_VIOLATION,
// not 409 (it is not a guard conflict) and not 500.
func TestConstraintTxnViolationFace(t *testing.T) {
	tc := newTestCluster(t, 1)
	leader := tc.leaderWithin(10 * time.Second)
	declareConstraintOverHTTP(t, leader.URL())

	resp, out := postJSON(t, leader.URL()+"/v1/batch", map[string]any{
		"nodes": []map[string]any{{"key": "a", "label": "Person", "props": map[string]any{"email": "dup@x"}}},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("seed write: %d %v", resp.StatusCode, out)
	}
	tc.waitAllPublished(1, 0, 5*time.Second)

	// A fresh guard snapshot from the read face.
	gotResp, _ := getJSON(t, leader.URL()+"/v1/nodes/a")
	snap, err := strconv.ParseUint(gotResp.Header.Get("X-Vectra-Snapshot"), 10, 64)
	if err != nil {
		t.Fatalf("snapshot header: %v", err)
	}
	resp, out = postJSON(t, leader.URL()+"/v1/txn", map[string]any{
		"nodes": []map[string]any{{"key": "b", "label": "Person", "props": map[string]any{"email": "dup@x"}}},
		"txn":   map[string]any{"snapshot": snap},
	})
	violationFace(t, resp, out)
}

// TestConstraintReplicaDeterminism: a two-node cluster rejects a violating
// write identically on both replicas. The violation is a function of log
// content and log-derived state, so the abort lands at the same op
// everywhere: the client sees 400 UNIQUE_VIOLATION, the entry's partial
// effects replicate, and a direct probe of each replica's FSM returns the
// same error class with the same payload (compared via errors.Is/As and
// field equality — never raw message strings; a multi-constraint write's
// payload follows map order, Task 2 carry).
func TestConstraintReplicaDeterminism(t *testing.T) {
	tc := newTestCluster(t, 2)
	leader := tc.leaderWithin(15 * time.Second)
	declareConstraintOverHTTP(t, leader.URL())

	// The violating entry carries a preceding ok op: its replication proves
	// the follower applied (and aborted at) the same entry.
	resp, out := postJSON(t, leader.URL()+"/v1/batch", map[string]any{
		"nodes": []map[string]any{
			{"key": "ok1", "label": "Person", "props": map[string]any{"email": "ok1@x"}},
			{"key": "dup1", "label": "Person", "props": map[string]any{"email": "ok1@x"}}, // violates
		},
	})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("violating write: %d %v, want 400", resp.StatusCode, out)
	}
	e, _ := out["error"].(map[string]any)
	if e["code"] != "UNIQUE_VIOLATION" {
		t.Fatalf("client code = %v, want UNIQUE_VIOLATION (not PARSE, not INTERNAL)", e["code"])
	}

	// Both replicas carry the same log-derived state: ok1 present, dup1
	// absent, constraint active.
	tc.waitAllPublished(1, 0, 10*time.Second)
	for _, tn := range tc.nodes {
		p := tn.node.Published()
		if p == nil {
			t.Fatalf("node %s has no published graph", tn.node.cfg.NodeID)
		}
		if _, ok := p.NodeByKey("ok1"); !ok {
			t.Fatalf("node %s: ok1 missing (partial effects must replicate)", tn.node.cfg.NodeID)
		}
		if _, ok := p.NodeByKey("dup1"); ok {
			t.Fatalf("node %s: dup1 landed (violating op must have zero side effects)", tn.node.cfg.NodeID)
		}
		if cs := p.UniqueConstraints(); len(cs) != 1 || cs[0].Label != "Person" || cs[0].Prop != "email" {
			t.Fatalf("node %s: published constraints = %+v, want [{Person email}]", tn.node.cfg.NodeID, cs)
		}
	}

	// Determinism probe: apply one fresh violating batch directly on each
	// replica's FSM (same synthetic index) and compare error class + payload.
	// The synthetic Index: 1 << 30 corrupts appliedIndex with a value the
	// log never had, so this probe must remain the FINAL assertion on this
	// FSM — nothing may be appended after it.
	probe := Batch{Version: 1, Nodes: []NodeUpsert{
		{Key: "ok2", Label: "Person", Props: map[string]any{"email": "ok2@x"}},
		{Key: "dup2", Label: "Person", Props: map[string]any{"email": "ok2@x"}},
	}}
	data := mustGob(t, probe)
	var payloads []*vectra.UniqueViolationError
	for _, tn := range tc.nodes {
		switch v := tn.node.fsm.Apply(&raft.Log{Data: data, Index: 1 << 30}).(type) {
		case error:
			if !errors.Is(v, vectra.ErrUniqueViolation) {
				t.Fatalf("node %s: error class = %v, want ErrUniqueViolation", tn.node.cfg.NodeID, v)
			}
			uv, ok := errors.AsType[*vectra.UniqueViolationError](v)
			if !ok {
				t.Fatalf("node %s: violation payload unreachable in %v", tn.node.cfg.NodeID, v)
			}
			payloads = append(payloads, uv)
		default:
			t.Fatalf("node %s: FSM did not reject the violating batch (silent-skip regression): %+v", tn.node.cfg.NodeID, v)
		}
	}
	if *payloads[0] != *payloads[1] {
		t.Fatalf("replicas diverged: %+v vs %+v", *payloads[0], *payloads[1])
	}
	if payloads[0].Value != "ok2@x" || payloads[0].ExistingKey != "ok2" || payloads[0].AttemptedKey != "dup2" {
		t.Fatalf("violation payload = %+v, want value ok2@x owner ok2 attempted dup2", *payloads[0])
	}
}

// TestConstraintBackupRestoreParity (spec §6 last row): declare + write on
// node A, GET /v1/backup, restore into a fresh dir — the constraint is
// active on the restored node (a duplicate write is rejected with the same
// 400 face, a fresh unique value still writes).
func TestConstraintBackupRestoreParity(t *testing.T) {
	src := bootBackupNode(t, "bk-uc1", t.TempDir(), "", "")
	waitBackupLeader(t, src, 10*time.Second)

	declareConstraintOverHTTP(t, src.url)
	resp, out := postJSON(t, src.url+"/v1/batch", map[string]any{
		"nodes": []map[string]any{{"key": "a", "label": "Person", "props": map[string]any{"email": "dup@x"}}},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("seed write: %d %v", resp.StatusCode, out)
	}
	waitBackupPublished(t, src, 1, 0, 5*time.Second)

	_, body := fetchBackup(t, src.url, "")
	restoreDir := t.TempDir()
	untarBackupInto(t, body, restoreDir)

	rn := bootBackupNode(t, "bk-uc1", restoreDir, "", "")
	waitBackupLeader(t, rn, 10*time.Second)
	waitBackupPublished(t, rn, 1, 0, 5*time.Second)

	// The constraint rode the snapshot: enforcement is live on the restored
	// node (behavior, not files).
	resp, out = postJSON(t, rn.url+"/v1/batch", map[string]any{
		"nodes": []map[string]any{{"key": "b", "label": "Person", "props": map[string]any{"email": "dup@x"}}},
	})
	violationFace(t, resp, out)
	resp, out = postJSON(t, rn.url+"/v1/batch", map[string]any{
		"nodes": []map[string]any{{"key": "c", "label": "Person", "props": map[string]any{"email": "c@x"}}},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("fresh unique value after restore: %d %v", resp.StatusCode, out)
	}
}
