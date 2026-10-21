package server

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"reflect"
	"testing"
	"time"
)

// ============================================================
// Vector REST endpoints (Task 13, spec 2026-10-21 §3.3):
// POST /v1/vector/search, POST/GET /v1/vector/indexes,
// DELETE /v1/vector/indexes/{name}.
// ============================================================

// vecBody builds one dim-4 embedding node ("emb" prop); a nil vector omits
// the prop entirely (the no-vector error fixture).
func vecBody(key, label string, v []float32) map[string]any {
	n := map[string]any{"key": key, "label": label}
	if v != nil {
		n["props"] = map[string]any{"emb": v}
	}
	return n
}

func deleteJSON(t *testing.T, url string) (*http.Response, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		t.Fatalf("DELETE %s: %v", url, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE %s: %v", url, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return resp, out
}

func errCode(t *testing.T, out map[string]any) string {
	t.Helper()
	e, ok := out["error"].(map[string]any)
	if !ok {
		t.Fatalf("response lacks the error envelope: %v", out)
	}
	c, _ := e["code"].(string)
	return c
}

// hitKeys extracts the ordered hit keys of a search response.
func hitKeys(t *testing.T, out map[string]any) []string {
	t.Helper()
	raw, ok := out["hits"].([]any)
	if !ok {
		t.Fatalf("response lacks hits: %v", out)
	}
	keys := make([]string, 0, len(raw))
	for _, h := range raw {
		m, ok := h.(map[string]any)
		if !ok {
			t.Fatalf("hit is not an object: %v", h)
		}
		keys = append(keys, m["key"].(string))
	}
	return keys
}

func TestAPIVectorSearchRoundTrip(t *testing.T) {
	tc := newTestCluster(t, 1)
	leader := tc.leaderWithin(10 * time.Second)

	resp, out := postJSON(t, leader.URL()+"/v1/vector/indexes", map[string]any{
		"name": "mem", "prop": "emb", "dim": 4, "metric": "cosine",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create index: %d %v", resp.StatusCode, out)
	}

	// Vector nodes through /v1/batch: JSON arrays ride the raft log and must
	// come back out of the published clone (the gob wire gap regression).
	resp, out = postJSON(t, leader.URL()+"/v1/batch", map[string]any{
		"nodes": []map[string]any{
			vecBody("doc1", "Doc", []float32{1, 0, 0, 0}),
			vecBody("doc2", "Doc", []float32{0, 1, 0, 0}),
			vecBody("doc3", "Doc", []float32{0, 0, 1, 0}),
			vecBody("novec", "Doc", nil),
			vecBody("near1", "Other", []float32{0.9999, 0.01, 0, 0}),
		},
	})
	if resp.StatusCode != http.StatusOK || out["nodesApplied"] != float64(5) {
		t.Fatalf("vector batch: %d %v", resp.StatusCode, out)
	}
	tc.waitAllPublished(5, 0, 5*time.Second)

	// Search by query_vector: the exact-match node leads with a numeric score.
	resp, out = postJSON(t, leader.URL()+"/v1/vector/search", map[string]any{
		"index": "mem", "query_vector": []float32{1, 0, 0, 0}, "k": 2,
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("search query_vector: %d %v", resp.StatusCode, out)
	}
	if out["count"] != float64(2) {
		t.Fatalf("count = %v, want 2", out["count"])
	}
	raw, _ := out["hits"].([]any)
	h0, _ := raw[0].(map[string]any)
	if h0["key"] != "doc1" {
		t.Fatalf("top hit = %v, want doc1", h0)
	}
	if _, ok := h0["score"].(float64); !ok {
		t.Fatalf("hit lacks a numeric score: %v", h0)
	}
	keys := hitKeys(t, out)
	if keys[0] != "doc1" {
		t.Fatalf("top hit = %v, want doc1", keys)
	}

	// Search by similar_to: the referenced node's own vector is the query.
	resp, out = postJSON(t, leader.URL()+"/v1/vector/search", map[string]any{
		"index": "mem", "similar_to": "doc2", "k": 2,
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("search similar_to: %d %v", resp.StatusCode, out)
	}
	if keys := hitKeys(t, out); keys[0] != "doc2" {
		t.Fatalf("similar_to top hit = %v, want doc2", keys)
	}

	// Label post-filter: near1 is the query's second-nearest neighbor but
	// carries label Other, so a Doc-scoped search must exclude it.
	resp, out = postJSON(t, leader.URL()+"/v1/vector/search", map[string]any{
		"index": "mem", "query_vector": []float32{1, 0, 0, 0}, "k": 10, "label": "Doc",
	})
	if resp.StatusCode != http.StatusOK || out["count"] != float64(3) {
		t.Fatalf("label Doc search: %d %v", resp.StatusCode, out)
	}
	for _, k := range hitKeys(t, out) {
		if k == "near1" {
			t.Fatalf("label Doc search returned near1: %v", out)
		}
	}
	resp, out = postJSON(t, leader.URL()+"/v1/vector/search", map[string]any{
		"index": "mem", "query_vector": []float32{1, 0, 0, 0}, "k": 10, "label": "Other",
	})
	if resp.StatusCode != http.StatusOK || out["count"] != float64(1) {
		t.Fatalf("label Other search: %d %v", resp.StatusCode, out)
	}
	if keys := hitKeys(t, out); keys[0] != "near1" {
		t.Fatalf("label Other top hit = %v, want near1", keys)
	}
}

func TestAPIVectorSearchKSemantics(t *testing.T) {
	tc := newTestCluster(t, 1)
	leader := tc.leaderWithin(10 * time.Second)

	resp, out := postJSON(t, leader.URL()+"/v1/vector/indexes", map[string]any{
		"name": "mem", "prop": "emb", "dim": 4, "metric": "cosine",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create index: %d %v", resp.StatusCode, out)
	}

	// 120 distinct directions: enough corpus that k bounds, not the data,
	// bound the result count.
	const corpus = 120
	nodes := make([]map[string]any, corpus)
	for i := 0; i < corpus; i++ {
		theta := 2 * math.Pi * float64(i) / corpus
		nodes[i] = vecBody(fmt.Sprintf("k%03d", i), "Doc",
			[]float32{float32(math.Cos(theta)), float32(math.Sin(theta)), 0, 0})
	}
	resp, out = postJSON(t, leader.URL()+"/v1/batch", map[string]any{"nodes": nodes})
	if resp.StatusCode != http.StatusOK || out["nodesApplied"] != float64(corpus) {
		t.Fatalf("vector batch: %d %v", resp.StatusCode, out)
	}
	tc.waitAllPublished(corpus, 0, 10*time.Second)

	query := []float32{1, 0, 0, 0}

	// k above the cap clamps to 100 (the MCP vector_search precedent; spec
	// Review Focus #5 "k>100 限幅"), never a client error.
	resp, out = postJSON(t, leader.URL()+"/v1/vector/search", map[string]any{
		"index": "mem", "query_vector": query, "k": 500,
	})
	if resp.StatusCode != http.StatusOK || out["count"] != float64(100) {
		t.Fatalf("k=500 search: %d count=%v, want 200/100 (clamped)", resp.StatusCode, out["count"])
	}

	// k omitted defaults to 10; explicit k=0 selects the same default.
	for _, k := range []any{nil, 0} {
		body := map[string]any{"index": "mem", "query_vector": query}
		if k != nil {
			body["k"] = k
		}
		resp, out = postJSON(t, leader.URL()+"/v1/vector/search", body)
		if resp.StatusCode != http.StatusOK || out["count"] != float64(10) {
			t.Fatalf("k=%v search: %d count=%v, want 200/10 (default)", k, resp.StatusCode, out["count"])
		}
	}
}

func TestAPIVectorSearchErrorFaces(t *testing.T) {
	tc := newTestCluster(t, 1)
	leader := tc.leaderWithin(10 * time.Second)

	resp, out := postJSON(t, leader.URL()+"/v1/vector/indexes", map[string]any{
		"name": "mem", "prop": "emb", "dim": 4, "metric": "cosine",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create index: %d %v", resp.StatusCode, out)
	}
	resp, out = postJSON(t, leader.URL()+"/v1/batch", map[string]any{
		"nodes": []map[string]any{
			vecBody("a", "Doc", []float32{1, 0, 0, 0}),
			vecBody("novec", "Doc", nil),
		},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("vector batch: %d %v", resp.StatusCode, out)
	}
	tc.waitAllPublished(2, 0, 5*time.Second)

	tests := []struct {
		name   string
		body   map[string]any
		status int
		code   string
	}{
		{"unknown index", map[string]any{"index": "nope", "query_vector": []float32{1, 0, 0, 0}},
			http.StatusNotFound, "VECTOR_INDEX_NOT_FOUND"},
		// The unknown-index face wins over similar_to resolution errors.
		{"unknown index via similar_to", map[string]any{"index": "nope", "similar_to": "a"},
			http.StatusNotFound, "VECTOR_INDEX_NOT_FOUND"},
		{"dim mismatch", map[string]any{"index": "mem", "query_vector": []float32{1, 0, 0}},
			http.StatusBadRequest, "VALIDATION"},
		{"similar_to node missing", map[string]any{"index": "mem", "similar_to": "ghost"},
			http.StatusBadRequest, "NODE_NOT_FOUND"},
		{"similar_to node without vector", map[string]any{"index": "mem", "similar_to": "novec"},
			http.StatusBadRequest, "VALIDATION"},
		{"neither query nor similar_to", map[string]any{"index": "mem"},
			http.StatusBadRequest, "VALIDATION"},
		{"both query and similar_to", map[string]any{"index": "mem", "query_vector": []float32{1, 0, 0, 0}, "similar_to": "a"},
			http.StatusBadRequest, "VALIDATION"},
		{"missing index", map[string]any{"query_vector": []float32{1, 0, 0, 0}},
			http.StatusBadRequest, "VALIDATION"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, out := postJSON(t, leader.URL()+"/v1/vector/search", tt.body)
			if resp.StatusCode != tt.status {
				t.Fatalf("status = %d (%v), want %d", resp.StatusCode, out, tt.status)
			}
			if c := errCode(t, out); c != tt.code {
				t.Fatalf("code = %q, want %q", c, tt.code)
			}
		})
	}

	// Malformed body JSON.
	resp2, err := http.Post(leader.URL()+"/v1/vector/search", "application/json",
		http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusBadRequest {
		t.Fatalf("empty body: %d, want 400", resp2.StatusCode)
	}
}

func TestAPIVectorIndexLifecycle(t *testing.T) {
	tc := newTestCluster(t, 1)
	leader := tc.leaderWithin(10 * time.Second)

	// Declaration validation faces.
	for _, bad := range []map[string]any{
		{"prop": "emb", "dim": 4, "metric": "cosine"}, // missing name
		{"name": "x", "dim": 4, "metric": "cosine"},   // missing prop
		{"name": "x", "prop": "emb", "dim": 0, "metric": "cosine"},
		{"name": "x", "prop": "emb", "dim": 4097, "metric": "cosine"},
		{"name": "x", "prop": "emb", "dim": 4, "metric": "euclid"},
	} {
		resp, out := postJSON(t, leader.URL()+"/v1/vector/indexes", bad)
		if resp.StatusCode != http.StatusBadRequest || errCode(t, out) != "VALIDATION" {
			t.Fatalf("bad decl %v: %d %v, want 400 VALIDATION", bad, resp.StatusCode, out)
		}
	}

	resp, out := postJSON(t, leader.URL()+"/v1/vector/indexes", map[string]any{
		"name": "mem", "prop": "emb", "dim": 4, "metric": "cosine",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create: %d %v", resp.StatusCode, out)
	}

	// The list read serves the declaration from the published clone.
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, out = getJSON(t, leader.URL()+"/v1/vector/indexes")
		if resp.StatusCode == http.StatusOK {
			if idx, ok := out["indexes"].([]any); ok && len(idx) == 1 {
				d, _ := idx[0].(map[string]any)
				if d["name"] == "mem" && d["prop"] == "emb" && d["dim"] == float64(4) && d["metric"] == "cosine" {
					break
				}
			}
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("index list did not converge: %d %v", resp.StatusCode, out)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Duplicate declaration is a conflict.
	resp, out = postJSON(t, leader.URL()+"/v1/vector/indexes", map[string]any{
		"name": "mem", "prop": "emb", "dim": 4, "metric": "cosine",
	})
	if resp.StatusCode != http.StatusConflict || errCode(t, out) != "VECTOR_INDEX_EXISTS" {
		t.Fatalf("duplicate create: %d %v, want 409 VECTOR_INDEX_EXISTS", resp.StatusCode, out)
	}

	// Drop: acknowledged, gone from the list, search falls to 404.
	resp, out = deleteJSON(t, leader.URL()+"/v1/vector/indexes/mem")
	if resp.StatusCode != http.StatusOK || out["dropped"] != "mem" {
		t.Fatalf("delete: %d %v", resp.StatusCode, out)
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		resp, out = getJSON(t, leader.URL()+"/v1/vector/indexes")
		if resp.StatusCode == http.StatusOK {
			if idx, _ := out["indexes"].([]any); len(idx) == 0 {
				break
			}
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("index list did not empty after drop: %d %v", resp.StatusCode, out)
		}
		time.Sleep(20 * time.Millisecond)
	}
	resp, out = postJSON(t, leader.URL()+"/v1/vector/search", map[string]any{
		"index": "mem", "query_vector": []float32{1, 0, 0, 0},
	})
	if resp.StatusCode != http.StatusNotFound || errCode(t, out) != "VECTOR_INDEX_NOT_FOUND" {
		t.Fatalf("search after drop: %d %v, want 404 VECTOR_INDEX_NOT_FOUND", resp.StatusCode, out)
	}

	// Dropping an unknown name is 404.
	resp, out = deleteJSON(t, leader.URL()+"/v1/vector/indexes/ghost")
	if resp.StatusCode != http.StatusNotFound || errCode(t, out) != "VECTOR_INDEX_NOT_FOUND" {
		t.Fatalf("delete unknown: %d %v, want 404 VECTOR_INDEX_NOT_FOUND", resp.StatusCode, out)
	}

	// Recreate after a drop succeeds (drop+recreate cycle).
	resp, out = postJSON(t, leader.URL()+"/v1/vector/indexes", map[string]any{
		"name": "mem", "prop": "emb", "dim": 4, "metric": "cosine",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("recreate after drop: %d %v", resp.StatusCode, out)
	}
}

func TestAPIVectorFollowerConsistency(t *testing.T) {
	tc := newTestCluster(t, 2)
	leader := tc.leaderWithin(10 * time.Second)
	var follower *testNode
	for _, tn := range tc.nodes {
		if tn != leader {
			follower = tn
			break
		}
	}

	// Declaration writes hit the shared non-leader write face.
	resp, out := postJSON(t, follower.URL()+"/v1/vector/indexes", map[string]any{
		"name": "mem", "prop": "emb", "dim": 4, "metric": "cosine",
	})
	if resp.StatusCode != http.StatusServiceUnavailable || errCode(t, out) != "NOT_LEADER" {
		t.Fatalf("follower create: %d %v, want 503 NOT_LEADER", resp.StatusCode, out)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Error("missing Retry-After header")
	}

	// Leader declares and writes; the follower converges at publish cadence
	// (eventual consistency, same as every other read).
	resp, out = postJSON(t, leader.URL()+"/v1/vector/indexes", map[string]any{
		"name": "mem", "prop": "emb", "dim": 4, "metric": "cosine",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("leader create: %d %v", resp.StatusCode, out)
	}
	resp, out = postJSON(t, leader.URL()+"/v1/batch", map[string]any{
		"nodes": []map[string]any{
			vecBody("doc1", "Doc", []float32{1, 0, 0, 0}),
			vecBody("doc2", "Doc", []float32{0, 1, 0, 0}),
			vecBody("doc3", "Doc", []float32{0, 0, 1, 0}),
		},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("leader vector batch: %d %v", resp.StatusCode, out)
	}
	tc.waitAllPublished(3, 0, 10*time.Second)

	// The follower's list read converges to the declaration.
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, out = getJSON(t, follower.URL()+"/v1/vector/indexes")
		if resp.StatusCode == http.StatusOK {
			if idx, _ := out["indexes"].([]any); len(idx) == 1 {
				if d, _ := idx[0].(map[string]any); d["name"] == "mem" {
					break
				}
			}
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("follower list did not converge: %d %v", resp.StatusCode, out)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// The follower's search serves from its published clone.
	deadline = time.Now().Add(10 * time.Second)
	for {
		resp, out = postJSON(t, follower.URL()+"/v1/vector/search", map[string]any{
			"index": "mem", "query_vector": []float32{1, 0, 0, 0}, "k": 3,
		})
		if resp.StatusCode == http.StatusOK && out["count"] == float64(3) {
			if keys := hitKeys(t, out); keys[0] == "doc1" {
				break
			}
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("follower search did not converge: %d %v", resp.StatusCode, out)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Drop on the leader; the follower eventually serves the 404 face.
	resp, out = deleteJSON(t, leader.URL()+"/v1/vector/indexes/mem")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("leader drop: %d %v", resp.StatusCode, out)
	}
	deadline = time.Now().Add(10 * time.Second)
	for {
		resp, out = postJSON(t, follower.URL()+"/v1/vector/search", map[string]any{
			"index": "mem", "query_vector": []float32{1, 0, 0, 0},
		})
		if resp.StatusCode == http.StatusNotFound && errCode(t, out) == "VECTOR_INDEX_NOT_FOUND" {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("follower search never fell to 404: %d %v", resp.StatusCode, out)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestWireableProps pins the JSON-boundary props normalization: JSON arrays
// decode as []any, which encoding/gob (the raft log codec) cannot carry —
// uniform numeric arrays become []float64 (gob-native, engine-identical).
func TestWireableProps(t *testing.T) {
	tests := []struct {
		name string
		in   map[string]any
		want map[string]any
	}{
		{"numeric array", map[string]any{"emb": []any{float64(1), float64(0), float64(0)}},
			map[string]any{"emb": []float64{1, 0, 0}}},
		{"empty array", map[string]any{"emb": []any{}},
			map[string]any{"emb": []float64{}}},
		{"mixed array untouched", map[string]any{"v": []any{float64(1), "x"}},
			map[string]any{"v": []any{float64(1), "x"}}},
		{"scalar untouched", map[string]any{"env": "prod", "n": float64(3)},
			map[string]any{"env": "prod", "n": float64(3)}},
		{"float32 untouched", map[string]any{"emb": []float32{1}},
			map[string]any{"emb": []float32{1}}},
		{"nil map", nil, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.in
			wireableProps(got)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("wireableProps() = %#v, want %#v", got, tt.want)
			}
		})
	}
}
