package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/projanvil/vectra/server"
)

// ============================================================
// Scripted line source
// ============================================================

// sliceSource is a lineSource over a fixed script. When the script is
// exhausted Next reports EOF, or the scripted err if one is set (to exercise
// the REPL's error exit path).
type sliceSource struct {
	lines []string
	idx   int
	err   error
}

func (s *sliceSource) Next() (string, bool, error) {
	if s.idx >= len(s.lines) {
		if s.err != nil {
			return "", false, s.err
		}
		return "", false, nil
	}
	l := s.lines[s.idx]
	s.idx++
	return l, true, nil
}

// ============================================================
// Fake /v1/cypher backend
// ============================================================

// fakeCypher answers each POST /v1/cypher with the next canned response and
// records every query it received.
type fakeCypher struct {
	mu       sync.Mutex
	queries  []string
	statuses []int    // per-request status; 0 means 200
	bodies   []string // per-request JSON bodies
}

func (f *fakeCypher) handler(w http.ResponseWriter, r *http.Request) {
	var req map[string]string
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	n := len(f.queries)
	f.queries = append(f.queries, req["query"])
	f.mu.Unlock()
	status := http.StatusOK
	if n < len(f.statuses) && f.statuses[n] != 0 {
		status = f.statuses[n]
	}
	body := `{"columns":[],"rows":[],"count":0}`
	if n < len(f.bodies) && f.bodies[n] != "" {
		body = f.bodies[n]
	}
	w.Header().Set("Content-Type", "application/json")
	if status != http.StatusOK {
		w.WriteHeader(status)
	}
	fmt.Fprint(w, body)
}

func (f *fakeCypher) received() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.queries...)
}

// newFakeServer wires a fakeCypher behind httptest (a free 127.0.0.1 port)
// and returns its URL.
func newFakeServer(t *testing.T, f *fakeCypher) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(f.handler))
	t.Cleanup(srv.Close)
	return srv.URL
}

func testClient(t *testing.T, url string) *cypherClient {
	t.Helper()
	c, err := newCypherClient(url, "", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// ============================================================
// runREPL against the fake
// ============================================================

func TestRunREPLMultiLineSubmitsOnlyAtSemicolon(t *testing.T) {
	fake := &fakeCypher{
		bodies: []string{`{"columns":["n.key","n.age"],"rows":[{"n.key":"a","n.age":1},{"n.key":"b","n.age":2}],"count":2}`},
	}
	url := newFakeServer(t, fake)

	var out strings.Builder
	src := &sliceSource{lines: []string{"MATCH (n)", "  RETURN n.key, n.age ; ", ":q"}}
	if code := runREPL(context.Background(), testClient(t, url), src, &out); code != 0 {
		t.Fatalf("runREPL exit = %d, want 0", code)
	}

	got := fake.received()
	if len(got) != 1 {
		t.Fatalf("server saw %d queries, want 1: %q", len(got), got)
	}
	// Lines are trimmed, joined with spaces, and the submit strips ALL
	// trailing ';' plus whitespace.
	if want := "MATCH (n) RETURN n.key, n.age"; got[0] != want {
		t.Errorf("query = %q, want %q", got[0], want)
	}
	if !strings.Contains(out.String(), "n.key") || !strings.Contains(out.String(), "n.age") {
		t.Errorf("output missing table header:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "(2 rows)") {
		t.Errorf("output missing (2 rows) line:\n%s", out.String())
	}
}

func TestRunREPLWriteRendersTableAndSummary(t *testing.T) {
	// Write #1 carries RETURN columns → table + summary; write #2 has no
	// columns → summary only, no table.
	fake := &fakeCypher{
		bodies: []string{
			`{"columns":["n"],"rows":[{"n":{"id":1}}],"summary":{"createdNodes":1,"propertiesSet":1}}`,
			`{"columns":[],"rows":[],"summary":{"createdNodes":2,"createdEdges":1}}`,
		},
	}
	url := newFakeServer(t, fake)

	var out strings.Builder
	src := &sliceSource{lines: []string{
		"CREATE (n:P {key:'a'}) RETURN n;",
		"CREATE (a) CREATE (b) CREATE (a)-[:R]->(b);",
		":q",
	}}
	if code := runREPL(context.Background(), testClient(t, url), src, &out); code != 0 {
		t.Fatalf("runREPL exit = %d, want 0", code)
	}

	o := out.String()
	if !strings.Contains(o, "Summary: created 1 nodes, set 1 properties") {
		t.Errorf("write #1 summary missing:\n%s", o)
	}
	if !strings.Contains(o, "Summary: created 2 nodes, created 1 edges") {
		t.Errorf("write #2 summary missing:\n%s", o)
	}
	// Write #1's table: the node cell renders as compact JSON.
	if !strings.Contains(o, `{"id":1}`) {
		t.Errorf("write #1 RETURN table missing node cell:\n%s", o)
	}
	// Write #2 must render no table: exactly one summary-only write means
	// the header/row of write #1 is the only table content around it.
	if n := strings.Count(o, "Summary:"); n != 2 {
		t.Errorf("summary lines = %d, want 2:\n%s", n, o)
	}
}

func TestRunREPLErrorPrintsAndContinues(t *testing.T) {
	fake := &fakeCypher{
		statuses: []int{http.StatusBadRequest},
		bodies: []string{
			`{"error":{"code":"PARSE","message":"unparseable statement"}}`,
			`{"columns":["n.key"],"rows":[{"n.key":"ok"}],"count":1}`,
		},
	}
	url := newFakeServer(t, fake)

	var out strings.Builder
	src := &sliceSource{lines: []string{"SELECT 1;", "MATCH (n) RETURN n.key;", ":q"}}
	if code := runREPL(context.Background(), testClient(t, url), src, &out); code != 0 {
		t.Fatalf("runREPL exit = %d, want 0 (errors must not exit)", code)
	}

	o := out.String()
	if !strings.Contains(o, "error: PARSE: unparseable statement") {
		t.Errorf("error line missing:\n%s", o)
	}
	if got := fake.received(); len(got) != 2 {
		t.Fatalf("queries after error = %q, want both executed", got)
	}
	if !strings.Contains(o, "ok") || !strings.Contains(o, "(1 rows)") {
		t.Errorf("statement after error not rendered:\n%s", o)
	}
}

func TestRunREPLExitCommands(t *testing.T) {
	for _, cmd := range []string{":q", ":Q", ":quit", ":EXIT", "quit", "Exit", "exit", "  exit  "} {
		t.Run(cmd, func(t *testing.T) {
			fake := &fakeCypher{}
			url := newFakeServer(t, fake)
			var out strings.Builder
			src := &sliceSource{lines: []string{"MATCH (n) RETURN n; ", cmd}}
			if code := runREPL(context.Background(), testClient(t, url), src, &out); code != 0 {
				t.Fatalf("runREPL exit = %d, want 0", code)
			}
			// The exit command itself must never be sent as a query.
			if got := fake.received(); len(got) != 1 {
				t.Errorf("queries = %q, want only the statement before the command", got)
			}
		})
	}
}

func TestRunREPLCommandOnlyOnEmptyBuffer(t *testing.T) {
	// "exit" as a continuation line of an unterminated statement is data,
	// not a command: nothing submits, EOF ends the session with exit 0.
	fake := &fakeCypher{}
	url := newFakeServer(t, fake)
	var out strings.Builder
	src := &sliceSource{lines: []string{"MATCH (n)", "exit"}}
	if code := runREPL(context.Background(), testClient(t, url), src, &out); code != 0 {
		t.Fatalf("runREPL exit = %d, want 0", code)
	}
	if got := fake.received(); len(got) != 0 {
		t.Errorf("queries = %q, want none (statement never terminated)", got)
	}
}

func TestRunREPLHelpContinues(t *testing.T) {
	fake := &fakeCypher{
		bodies: []string{`{"columns":["n.key"],"rows":[{"n.key":"after-help"}],"count":1}`},
	}
	url := newFakeServer(t, fake)
	var out strings.Builder
	src := &sliceSource{lines: []string{":help", ":HELP", "MATCH (n) RETURN n.key;", ":q"}}
	if code := runREPL(context.Background(), testClient(t, url), src, &out); code != 0 {
		t.Fatalf("runREPL exit = %d, want 0", code)
	}
	o := out.String()
	if n := strings.Count(o, ":help"); n < 2 {
		t.Errorf("usage printed %d times, want >= 2:\n%s", n, o)
	}
	if !strings.Contains(o, "after-help") {
		t.Errorf("statement after :help not executed:\n%s", o)
	}
}

func TestRunREPLEmptyLinesAreNoops(t *testing.T) {
	fake := &fakeCypher{
		bodies: []string{`{"columns":["n.key"],"rows":[{"n.key":"v"}],"count":1}`},
	}
	url := newFakeServer(t, fake)
	var out strings.Builder
	src := &sliceSource{lines: []string{"", "   ", ";", "MATCH (n) RETURN n.key;", "", ":q"}}
	if code := runREPL(context.Background(), testClient(t, url), src, &out); code != 0 {
		t.Fatalf("runREPL exit = %d, want 0", code)
	}
	if got := fake.received(); len(got) != 1 {
		t.Fatalf("queries = %q, want exactly the one real statement", got)
	}
	if !strings.Contains(out.String(), "v") {
		t.Errorf("statement not rendered:\n%s", out.String())
	}
}

func TestRunREPLEOFReturnsZero(t *testing.T) {
	fake := &fakeCypher{}
	url := newFakeServer(t, fake)
	var out strings.Builder
	src := &sliceSource{} // EOF immediately
	if code := runREPL(context.Background(), testClient(t, url), src, &out); code != 0 {
		t.Fatalf("runREPL exit = %d, want 0 on EOF", code)
	}
}

func TestRunREPLSourceErrorReturnsOne(t *testing.T) {
	fake := &fakeCypher{}
	url := newFakeServer(t, fake)
	var out strings.Builder
	src := &sliceSource{err: fmt.Errorf("boom")}
	if code := runREPL(context.Background(), testClient(t, url), src, &out); code != 1 {
		t.Fatalf("runREPL exit = %d, want 1 on source error", code)
	}
}

// ============================================================
// history unit tests
// ============================================================

func TestHistoryAddSkipEmptyAndDupOfLast(t *testing.T) {
	h := loadHistory("", 10)
	h.add("")
	h.add("   ")
	h.add("MATCH (n);")
	h.add("MATCH (n);") // dup of last: skipped
	h.add("")
	if len(h.entries) != 1 || h.entries[0] != "MATCH (n);" {
		t.Fatalf("entries = %q, want [MATCH (n);]", h.entries)
	}
	// The same line again after a different one is NOT a dup.
	h.add("RETURN 1;")
	h.add("MATCH (n);")
	if len(h.entries) != 3 {
		t.Fatalf("entries = %q, want 3 (non-adjacent dup must be kept)", h.entries)
	}
}

func TestHistoryCapDropsOldest(t *testing.T) {
	h := loadHistory("", 3)
	for _, l := range []string{"a", "b", "c", "d"} {
		h.add(l)
	}
	if len(h.entries) != 3 {
		t.Fatalf("len(entries) = %d, want 3", len(h.entries))
	}
	want := []string{"b", "c", "d"}
	for i, e := range h.entries {
		if e != want[i] {
			t.Fatalf("entries = %q, want %q", h.entries, want)
		}
	}
}

func TestHistoryMissingFileIsEmpty(t *testing.T) {
	h := loadHistory(filepath.Join(t.TempDir(), "nope", "missing"), 5)
	if len(h.entries) != 0 {
		t.Fatalf("entries = %q, want empty for missing file", h.entries)
	}
}

func TestHistorySaveLoadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hist")
	h := loadHistory(path, 10)
	h.add("MATCH (n) RETURN n;")
	h.add("CREATE (n);")
	if err := h.save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	h2 := loadHistory(path, 10)
	if len(h2.entries) != 2 || h2.entries[0] != "MATCH (n) RETURN n;" || h2.entries[1] != "CREATE (n);" {
		t.Fatalf("loaded = %q", h2.entries)
	}

	// Empty path: save is a no-op.
	empty := loadHistory("", 10)
	empty.add("x")
	if err := empty.save(); err != nil {
		t.Fatalf("save with empty path: %v", err)
	}
}

// ============================================================
// In-process real-node end-to-end
// ============================================================

// newSingleNode boots a single-node server the way the server package's own
// tests do (inmem raft transport, httptest HTTP listener on a free 127.0.0.1
// port) and waits for it to elect itself leader.
func newSingleNode(t *testing.T) (*httptest.Server, *server.Node) {
	t.Helper()
	transAddr, trans := raft.NewInmemTransport(raft.NewInmemAddr())
	logs := raft.NewInmemStore()
	stable := raft.NewInmemStore()
	snaps := raft.NewInmemSnapshotStore()

	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID("n1")
	rc.HeartbeatTimeout = 240 * time.Millisecond
	rc.ElectionTimeout = 240 * time.Millisecond
	rc.LeaderLeaseTimeout = 180 * time.Millisecond
	rc.CommitTimeout = 20 * time.Millisecond
	rc.Logger = nil
	rc.LogOutput = io.Discard // keep raft election chatter out of test output

	n, err := server.New(server.Config{
		NodeID:        "n1",
		HTTPAddr:      "127.0.0.1:0",
		RaftAddr:      string(transAddr),
		DataDir:       t.TempDir(),
		SealInterval:  50 * time.Millisecond,
		LogStore:      logs,
		StableStore:   stable,
		SnapshotStore: snaps,
		Transport:     trans,
		RaftConfig:    rc,
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	t.Cleanup(n.Close)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && !n.IsLeader() {
		time.Sleep(20 * time.Millisecond)
	}
	if !n.IsLeader() {
		t.Fatal("single node not elected leader within 10s")
	}

	srv := httptest.NewServer(n.Handler())
	t.Cleanup(srv.Close)
	return srv, n
}

// waitPublished blocks until the node's published graph carries at least
// minNodes nodes (publishes run asynchronously of the raft apply).
func waitPublished(t *testing.T, n *server.Node, minNodes int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if g := n.Published(); g != nil && g.NodeCount() >= minNodes {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("node never published %d nodes within 10s", minNodes)
}

func TestRunREPLRealNodeEndToEnd(t *testing.T) {
	srv, node := newSingleNode(t)
	c := testClient(t, srv.URL)

	// Phase 1: the write. The server applies it through raft and answers
	// with the summary; the RETURN clause renders as a table.
	var writeOut strings.Builder
	writeSrc := &sliceSource{lines: []string{"CREATE (n:P {key:'a'}) RETURN n;"}}
	if code := runREPL(context.Background(), c, writeSrc, &writeOut); code != 0 {
		t.Fatalf("write phase exit = %d, want 0", code)
	}
	o := writeOut.String()
	// Inline CREATE properties count as createdNodes only; the fake-server
	// tests above cover the fuller counter rendering.
	if !strings.Contains(o, "Summary: created 1 nodes") {
		t.Errorf("write summary missing:\n%s", o)
	}
	// The RETURN n table rendered something besides the summary line.
	if got := strings.Split(strings.TrimSpace(o), "\n"); len(got) < 2 {
		t.Errorf("RETURN n table missing (output is only the summary):\n%s", o)
	}

	// Reads serve the sealed snapshot, which lands just after the write
	// response: wait for the publish before reading back.
	waitPublished(t, node, 1)

	// Phase 2: the read-back through the same REPL loop.
	var readOut strings.Builder
	readSrc := &sliceSource{lines: []string{"MATCH (n:P) RETURN n.key;"}}
	if code := runREPL(context.Background(), c, readSrc, &readOut); code != 0 {
		t.Fatalf("read phase exit = %d, want 0", code)
	}
	ro := readOut.String()
	if !strings.Contains(ro, "n.key") {
		t.Errorf("read table header missing:\n%s", ro)
	}
	if !strings.Contains(ro, "a") {
		t.Errorf("stored value 'a' not read back:\n%s", ro)
	}
	if !strings.Contains(ro, "(1 rows)") {
		t.Errorf("(1 rows) line missing:\n%s", ro)
	}
}
