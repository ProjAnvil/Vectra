package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewCypherClientRequiresScheme(t *testing.T) {
	// "localhost:8080" parses with a bogus "localhost" scheme and no host —
	// the classic missing-http:// mistake — so both must be rejected.
	for _, raw := range []string{"localhost:8080", "//localhost:8080", "localhost"} {
		if _, err := newCypherClient(raw, "", time.Second); err == nil {
			t.Errorf("newCypherClient(%q): expected error for URL without scheme", raw)
		}
	}
	if _, err := newCypherClient("http://127.0.0.1:8080", "", time.Second); err != nil {
		t.Fatalf("newCypherClient rejected a valid URL: %v", err)
	}
}

func TestCypherClientReadResponse(t *testing.T) {
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/v1/cypher" {
			t.Errorf("path = %s, want /v1/cypher", r.URL.Path)
		}
		var req map[string]string
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request body: %v", err)
		}
		gotQuery = req["query"]
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"columns":["n.key","n.age"],"rows":[{"n.key":"a","n.age":1},{"n.key":"b","n.age":2}],"count":2}`)
	}))
	defer srv.Close()

	c, err := newCypherClient(srv.URL, "", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	res, err := c.cypher(context.Background(), "MATCH (n) RETURN n.key, n.age")
	if err != nil {
		t.Fatal(err)
	}
	if gotQuery != "MATCH (n) RETURN n.key, n.age" {
		t.Errorf("server received query %q", gotQuery)
	}
	if len(res.Columns) != 2 || res.Columns[0] != "n.key" || res.Columns[1] != "n.age" {
		t.Errorf("columns = %v", res.Columns)
	}
	if len(res.Rows) != 2 || res.Rows[0]["n.key"] != "a" || res.Rows[1]["n.age"] != float64(2) {
		t.Errorf("rows = %v", res.Rows)
	}
	if res.Count != 2 {
		t.Errorf("count = %d, want 2", res.Count)
	}
	if res.Summary != nil {
		t.Errorf("read response carried a summary: %+v", res.Summary)
	}
}

func TestCypherClientWriteResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"columns":[],"rows":[],"summary":{"createdNodes":2,"createdEdges":1,"deletedNodes":0,"deletedEdges":0,"propertiesSet":3,"indexesCreated":0}}`)
	}))
	defer srv.Close()

	c, err := newCypherClient(srv.URL, "", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	res, err := c.cypher(context.Background(), "CREATE (a) CREATE (b)")
	if err != nil {
		t.Fatal(err)
	}
	if res.Count != 0 {
		t.Errorf("write response decoded a count: %d", res.Count)
	}
	s := res.Summary
	if s == nil {
		t.Fatal("summary missing from write response")
	}
	if s.CreatedNodes != 2 || s.CreatedEdges != 1 || s.PropertiesSet != 3 {
		t.Errorf("summary = %+v", s)
	}
	if s.DeletedNodes != 0 || s.DeletedEdges != 0 || s.IndexesCreated != 0 {
		t.Errorf("summary zero-counters not zero: %+v", s)
	}
}

func TestCypherClientErrorEnvelope(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"error":{"code":"PARSE","message":"bad"}}`)
	}))
	defer srv.Close()

	c, err := newCypherClient(srv.URL, "", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	res, err := c.cypher(context.Background(), "MATCH (a) RETURN b")
	if res != nil {
		t.Errorf("response = %+v, want nil", res)
	}
	apiErr, ok := errors.AsType[*apiError](err)
	if !ok {
		t.Fatalf("err = %v (%T), want *apiError", err, err)
	}
	if apiErr.Error() != "PARSE: bad" {
		t.Errorf("apiError.Error() = %q, want %q", apiErr.Error(), "PARSE: bad")
	}
}

func TestCypherClientBearerHeader(t *testing.T) {
	var auths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auths = append(auths, r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"columns":[],"rows":[],"count":0}`)
	}))
	defer srv.Close()

	c, err := newCypherClient(srv.URL, "t0k3n", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.cypher(context.Background(), "MATCH (n) RETURN n"); err != nil {
		t.Fatal(err)
	}
	if len(auths) != 1 || auths[0] != "Bearer t0k3n" {
		t.Fatalf("Authorization headers = %v, want [Bearer t0k3n]", auths)
	}

	cNoToken, err := newCypherClient(srv.URL, "", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cNoToken.cypher(context.Background(), "MATCH (n) RETURN n"); err != nil {
		t.Fatal(err)
	}
	if len(auths) != 2 || auths[1] != "" {
		t.Fatalf("Authorization header with empty token = %q, want absent", auths[1])
	}
}
