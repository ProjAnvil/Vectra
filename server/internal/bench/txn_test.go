package bench

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// stubTxnServer serves GET /v1/neighbors (snapshot header + neighbor keys),
// POST /v1/txn (commits at index 7), and GET /v1/stats according to small
// scripts: snapshots, txn outcomes, and publishedSeq values are consumed per
// call in order; exhausted scripts stick to their last entry. An empty seqs
// script means "already published" (publishedSeq 100 >= index 7). Captured
// txn bodies are exposed via posts() for assertions.
type stubTxnServer struct {
	*httptest.Server
	mu         sync.Mutex
	snapshots  []string  // per neighbors-call X-Vectra-Snapshot values
	outcomes   []int     // per txn-call status codes
	seqs       []float64 // per stats-poll publishedSeq values
	snapIdx    int
	txnIdx     int
	seqIdx     int
	bodies     []txnRequestBody
	neighbors  atomic.Int64
	statsPolls atomic.Int64
}

func newStubTxnServer(t *testing.T, snapshots []string, outcomes []int, seqs []float64) *stubTxnServer {
	t.Helper()
	s := &stubTxnServer{snapshots: snapshots, outcomes: outcomes, seqs: seqs}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/neighbors":
			s.mu.Lock()
			snap := s.snapshots[min(s.snapIdx, len(s.snapshots)-1)]
			if s.snapIdx < len(s.snapshots) {
				s.snapIdx++
			}
			s.mu.Unlock()
			s.neighbors.Add(1)
			w.Header().Set("X-Vectra-Snapshot", snap)
			fmt.Fprint(w, `{"nodes":[{"key":"nbr-a","label":"Svc","props":{}},{"key":"nbr-b","label":"Svc","props":{}}]}`)
		case "/v1/stats":
			s.mu.Lock()
			seq := 100.0
			if len(s.seqs) > 0 {
				seq = s.seqs[min(s.seqIdx, len(s.seqs)-1)]
				if s.seqIdx < len(s.seqs) {
					s.seqIdx++
				}
			}
			s.mu.Unlock()
			s.statsPolls.Add(1)
			fmt.Fprintf(w, `{"publishedSeq":%v,"nodes":10,"edges":20}`, seq)
		case "/v1/txn":
			var body txnRequestBody
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode txn body: %v", err)
			}
			s.mu.Lock()
			s.bodies = append(s.bodies, body)
			code := s.outcomes[min(s.txnIdx, len(s.outcomes)-1)]
			if s.txnIdx < len(s.outcomes) {
				s.txnIdx++
			}
			s.mu.Unlock()
			if code == http.StatusConflict {
				fmt.Fprint(w, `{"committed":false,"conflicts":["nbr-a"],"conflictTotal":1}`)
				return
			}
			fmt.Fprint(w, `{"committed":true,"index":7}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *stubTxnServer) posts() []txnRequestBody {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.bodies)
}

func TestNewTxnOpHappyPath(t *testing.T) {
	srv := newStubTxnServer(t, []string{"42"}, []int{http.StatusOK}, nil)
	var stats TxnStats
	op := NewTxnOp(srv.Client(), srv.URL, func() string { return "start-1" }, false, 2, &stats)

	if err := op(); err != nil {
		t.Fatalf("op() error = %v, want nil", err)
	}
	if stats.Commits.Load() != 1 {
		t.Errorf("commits = %d, want 1", stats.Commits.Load())
	}
	if stats.Conflicts.Load() != 0 || stats.Retries.Load() != 0 {
		t.Errorf("conflicts = %d, retries = %d, want 0/0", stats.Conflicts.Load(), stats.Retries.Load())
	}
	// Default stats script is already-published: exactly one RYW poll.
	if got := srv.statsPolls.Load(); got != 1 {
		t.Errorf("stats polls = %d, want 1", got)
	}

	posts := srv.posts()
	if len(posts) != 1 {
		t.Fatalf("txn posts = %d, want 1", len(posts))
	}
	p := posts[0]
	if p.Txn.Snapshot != 42 {
		t.Errorf("snapshot = %d, want 42", p.Txn.Snapshot)
	}
	if p.Txn.SER {
		t.Error("ser = true, want false")
	}
	// Read set: start key always first, then up to readFanout neighbor keys.
	if want := []string{"start-1", "nbr-a", "nbr-b"}; !slices.Equal(p.Txn.ReadNodes, want) {
		t.Errorf("readNodes = %v, want %v", p.Txn.ReadNodes, want)
	}
	// Write set: guarded upsert of the start node in the Svc family.
	if len(p.Nodes) != 1 || p.Nodes[0].Key != "start-1" || p.Nodes[0].Label != "Svc" {
		t.Errorf("nodes = %+v, want one Svc upsert of start-1", p.Nodes)
	}
	if _, ok := p.Nodes[0].Props["w"]; !ok {
		t.Errorf("props = %v, want a w write marker", p.Nodes[0].Props)
	}
}

func TestNewTxnOpFanoutCap(t *testing.T) {
	srv := newStubTxnServer(t, []string{"42"}, []int{http.StatusOK, http.StatusOK, http.StatusOK}, nil)
	var stats TxnStats
	op := NewTxnOp(srv.Client(), srv.URL, func() string { return "start-1" }, false, 1, &stats)

	for range 3 {
		if err := op(); err != nil {
			t.Fatalf("op() error = %v, want nil", err)
		}
	}
	// Stub returns 2 neighbors; readFanout 1 must cap at start + 1 neighbor.
	for i, p := range srv.posts() {
		if want := []string{"start-1", "nbr-a"}; !slices.Equal(p.Txn.ReadNodes, want) {
			t.Errorf("post %d readNodes = %v, want %v", i, p.Txn.ReadNodes, want)
		}
	}
}

func TestNewTxnOpConflictThenSuccess(t *testing.T) {
	// First attempt reads snapshot 42 and is rejected; the retry must
	// re-read neighbors (snapshot refreshed to 99) before succeeding.
	srv := newStubTxnServer(t, []string{"42", "99"}, []int{http.StatusConflict, http.StatusOK}, nil)
	var stats TxnStats
	op := NewTxnOp(srv.Client(), srv.URL, func() string { return "start-1" }, true, 2, &stats)

	if err := op(); err != nil {
		t.Fatalf("op() error = %v, want nil", err)
	}
	if stats.Commits.Load() != 1 {
		t.Errorf("commits = %d, want 1", stats.Commits.Load())
	}
	if stats.Conflicts.Load() != 1 {
		t.Errorf("conflicts = %d, want 1", stats.Conflicts.Load())
	}
	if stats.Retries.Load() != 1 {
		t.Errorf("retries = %d, want 1", stats.Retries.Load())
	}
	if got := srv.neighbors.Load(); got != 2 {
		t.Errorf("neighbors calls = %d, want 2 (token refreshed on retry)", got)
	}
	posts := srv.posts()
	if len(posts) != 2 {
		t.Fatalf("txn posts = %d, want 2", len(posts))
	}
	if posts[0].Txn.Snapshot != 42 || posts[1].Txn.Snapshot != 99 {
		t.Errorf("snapshots = [%d %d], want [42 99]", posts[0].Txn.Snapshot, posts[1].Txn.Snapshot)
	}
	if !posts[1].Txn.SER {
		t.Error("retry ser = false, want true")
	}
}

func TestNewTxnOpPersistentConflict(t *testing.T) {
	srv := newStubTxnServer(t, []string{"42"}, []int{http.StatusConflict}, nil)
	var stats TxnStats
	op := NewTxnOp(srv.Client(), srv.URL, func() string { return "start-1" }, true, 2, &stats)

	// Exhausted retries are a normal OCC outcome, not an op error.
	if err := op(); err != nil {
		t.Fatalf("op() error = %v, want nil", err)
	}
	if stats.Commits.Load() != 0 {
		t.Errorf("commits = %d, want 0", stats.Commits.Load())
	}
	if stats.Conflicts.Load() != maxTxnAttempts {
		t.Errorf("conflicts = %d, want %d", stats.Conflicts.Load(), maxTxnAttempts)
	}
	if stats.Retries.Load() != maxTxnAttempts-1 {
		t.Errorf("retries = %d, want %d", stats.Retries.Load(), maxTxnAttempts-1)
	}
	// 409 paths never commit, so they must never poll stats (RYW waits
	// happen only after successful commits).
	if got := srv.statsPolls.Load(); got != 0 {
		t.Errorf("stats polls = %d, want 0 (conflict path must not poll)", got)
	}
}

func TestNewTxnOpWaitsForPublish(t *testing.T) {
	// The commit lands at index 7 but publishedSeq only crosses it on the
	// third poll (5 → 6 → 7): the op must keep polling instead of returning
	// straight after the 200, per the documented RYW etiquette.
	srv := newStubTxnServer(t, []string{"42"}, []int{http.StatusOK}, []float64{5, 6, 7})
	var stats TxnStats
	op := NewTxnOp(srv.Client(), srv.URL, func() string { return "start-1" }, false, 2, &stats)

	if err := op(); err != nil {
		t.Fatalf("op() error = %v, want nil", err)
	}
	if stats.Commits.Load() != 1 {
		t.Errorf("commits = %d, want 1", stats.Commits.Load())
	}
	if got := srv.statsPolls.Load(); got != 3 {
		t.Errorf("stats polls = %d, want 3 (poll until publishedSeq >= commit index)", got)
	}
}

func TestNewTxnOpAlreadyPublished(t *testing.T) {
	// publishedSeq (99) already covers the commit index (7): the wait
	// resolves on the very first poll.
	srv := newStubTxnServer(t, []string{"42"}, []int{http.StatusOK}, []float64{99})
	var stats TxnStats
	op := NewTxnOp(srv.Client(), srv.URL, func() string { return "start-1" }, false, 2, &stats)

	if err := op(); err != nil {
		t.Fatalf("op() error = %v, want nil", err)
	}
	if stats.Commits.Load() != 1 {
		t.Errorf("commits = %d, want 1", stats.Commits.Load())
	}
	if got := srv.statsPolls.Load(); got != 1 {
		t.Errorf("stats polls = %d, want exactly 1 when already published", got)
	}
}

func TestNewTxnOpPublishWaitCapExpiry(t *testing.T) {
	// publishedSeq never catches up: the wait must give up at the cap
	// (overridden small for the test; the production default stays 5s) and
	// the op still returns nil — etiquette, not a correctness gate.
	srv := newStubTxnServer(t, []string{"42"}, []int{http.StatusOK}, []float64{0})
	var stats TxnStats
	const waitCap = 50 * time.Millisecond
	op := newTxnOp(srv.Client(), srv.URL, func() string { return "start-1" }, false, 2, &stats, waitCap)

	start := time.Now()
	if err := op(); err != nil {
		t.Fatalf("op() error = %v, want nil on cap expiry", err)
	}
	if elapsed := time.Since(start); elapsed < waitCap {
		t.Errorf("elapsed = %v, want >= %v (cap respected before giving up)", elapsed, waitCap)
	}
	if got := srv.statsPolls.Load(); got < 2 {
		t.Errorf("stats polls = %d, want >= 2 (kept polling until the cap)", got)
	}
	if stats.Commits.Load() != 1 {
		t.Errorf("commits = %d, want 1", stats.Commits.Load())
	}
}

func TestNewTxnOpServerError(t *testing.T) {
	var stats TxnStats
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/neighbors":
			w.Header().Set("X-Vectra-Snapshot", "42")
			fmt.Fprint(w, `{"nodes":[]}`)
		case "/v1/txn":
			_, _ = io.Copy(io.Discard, r.Body)
			http.Error(w, `{"error":{"code":"INTERNAL","message":"boom"}}`, http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	op := NewTxnOp(srv.Client(), srv.URL, func() string { return "start-1" }, false, 2, &stats)
	if err := op(); err == nil {
		t.Fatal("op() error = nil, want error on 500")
	}
	if stats.Commits.Load() != 0 || stats.Conflicts.Load() != 0 {
		t.Errorf("stats moved on server failure: commits = %d, conflicts = %d, want 0/0",
			stats.Commits.Load(), stats.Conflicts.Load())
	}
}

func TestTxnStatsConcurrent(t *testing.T) {
	// Deterministic two-phase stub: everyone commits, then everyone conflicts.
	var conflictMode atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/neighbors":
			w.Header().Set("X-Vectra-Snapshot", "7")
			fmt.Fprint(w, `{"nodes":[{"key":"n1"},{"key":"n2"},{"key":"n3"},{"key":"n4"}]}`)
		case "/v1/txn":
			_, _ = io.Copy(io.Discard, r.Body)
			if conflictMode.Load() {
				w.WriteHeader(http.StatusConflict)
				fmt.Fprint(w, `{"committed":false,"conflictTotal":1}`)
				return
			}
			fmt.Fprint(w, `{"committed":true,"index":1}`)
		case "/v1/stats":
			// Always published (5 >= 1): one cheap poll per commit.
			fmt.Fprint(w, `{"publishedSeq":5}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	var stats TxnStats
	op := NewTxnOp(srv.Client(), srv.URL, func() string { return "hot" }, true, 2, &stats)

	const goroutines = 8
	run := func(iters int) {
		var wg sync.WaitGroup
		for range goroutines {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range iters {
					if err := op(); err != nil {
						t.Errorf("op() error = %v", err)
					}
				}
			}()
		}
		wg.Wait()
	}

	// Phase 1: commit path under concurrency.
	const happyIters = 25
	run(happyIters)
	if got := stats.Commits.Load(); got != goroutines*happyIters {
		t.Fatalf("commits = %d, want %d", got, goroutines*happyIters)
	}
	if stats.Conflicts.Load() != 0 || stats.Retries.Load() != 0 {
		t.Fatalf("conflicts = %d, retries = %d, want 0/0", stats.Conflicts.Load(), stats.Retries.Load())
	}

	// Phase 2: every attempt rejected; each op exhausts maxTxnAttempts.
	conflictMode.Store(true)
	const conflictIters = 10
	run(conflictIters)
	if got := stats.Conflicts.Load(); got != goroutines*conflictIters*maxTxnAttempts {
		t.Fatalf("conflicts = %d, want %d", got, goroutines*conflictIters*maxTxnAttempts)
	}
	if got := stats.Retries.Load(); got != goroutines*conflictIters*(maxTxnAttempts-1) {
		t.Fatalf("retries = %d, want %d", got, goroutines*conflictIters*(maxTxnAttempts-1))
	}
	if got := stats.Commits.Load(); got != goroutines*happyIters {
		t.Fatalf("commits = %d, want unchanged %d", got, goroutines*happyIters)
	}
}

func TestTxnStatsDerived(t *testing.T) {
	var s TxnStats
	if s.ConflictPct() != 0 {
		t.Errorf("empty ConflictPct = %f, want 0", s.ConflictPct())
	}
	if s.CommitQPS(time.Second) != 0 {
		t.Errorf("empty CommitQPS = %f, want 0", s.CommitQPS(time.Second))
	}
	s.Commits.Store(9)
	s.Conflicts.Store(1)
	if got := s.ConflictPct(); got != 10 {
		t.Errorf("ConflictPct = %f, want 10", got)
	}
	if got := s.CommitQPS(2 * time.Second); got != 4.5 {
		t.Errorf("CommitQPS = %f, want 4.5", got)
	}
	s.Reset()
	if s.Commits.Load() != 0 || s.Conflicts.Load() != 0 || s.Retries.Load() != 0 {
		t.Error("Reset did not zero counters")
	}
}
