package bench

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

// maxTxnAttempts bounds the total attempts (initial try plus retries) one
// transaction op makes. Exhausting them on persistent 409s is a normal OCC
// outcome, not an op error.
const maxTxnAttempts = 3

// txnPublishWaitCap bounds the read-your-writes wait after a commit. On
// expiry the op proceeds without error: the wait is best-effort client
// etiquette, not a correctness gate.
const txnPublishWaitCap = 5 * time.Second

// TxnStats tracks OCC outcomes for transaction workloads: a 409 is a normal
// optimistic-concurrency result, not an error. It also accumulates per-phase
// client latency (read / commit / read-your-writes wait) so bottleneck
// attribution can separate server pipeline cost from client etiquette cost.
type TxnStats struct {
	Commits   atomic.Int64
	Conflicts atomic.Int64 // 409 rejections encountered (each retry attempt counts)
	Retries   atomic.Int64

	readNs   atomic.Int64 // Σ readNeighbors duration (one per attempt)
	commitNs atomic.Int64 // Σ commitTxn duration (one per attempt)
	waitNs   atomic.Int64 // Σ read-your-writes wait duration (one per commit)
	attempts atomic.Int64 // read/commit samples
}

// PhaseMeans returns the mean client-side latency of each txn-op phase:
// read (GET /v1/neighbors), commit (POST /v1/txn), and the read-your-writes
// wait (per committed txn). Zero when no samples.
func (s *TxnStats) PhaseMeans() (read, commit, wait time.Duration) {
	if n := s.attempts.Load(); n > 0 {
		read = time.Duration(s.readNs.Load() / n)
		commit = time.Duration(s.commitNs.Load() / n)
	}
	if n := s.Commits.Load(); n > 0 {
		wait = time.Duration(s.waitNs.Load() / n)
	}
	return read, commit, wait
}

// ConflictPct returns the share of finished attempts (commits + conflicts)
// that were rejected with 409, as a percentage.
func (s *TxnStats) ConflictPct() float64 {
	total := s.Commits.Load() + s.Conflicts.Load()
	if total == 0 {
		return 0
	}
	return float64(s.Conflicts.Load()) / float64(total) * 100
}

// CommitQPS returns committed transactions per second over d.
func (s *TxnStats) CommitQPS(d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return float64(s.Commits.Load()) / d.Seconds()
}

// Reset zeroes all counters (discards warmup results).
func (s *TxnStats) Reset() {
	s.Commits.Store(0)
	s.Conflicts.Store(0)
	s.Retries.Store(0)
	s.readNs.Store(0)
	s.commitNs.Store(0)
	s.waitNs.Store(0)
	s.attempts.Store(0)
}

// NewTxnOp returns a workload op implementing the interactive-transaction
// client pattern: read neighbors of a rotating start node (capturing the
// X-Vectra-Snapshot token and neighbor keys), then commit a guarded upsert
// of the start node (SER additionally validates the read set). On 409 the
// op refreshes the token and retries, at most maxTxnAttempts total. After a
// successful commit the op follows the documented read-your-writes
// etiquette — poll GET /v1/stats until publishedSeq reaches the commit's
// index (capped at txnPublishWaitCap) — so the next read snapshot includes
// the op's own writes instead of conflicting with them. The wait is part of
// the measured op latency (documented client cost). Only transport/5xx
// failures surface as op errors.
func NewTxnOp(c *http.Client, base string, nextStart func() string, ser bool, readFanout int, stats *TxnStats) Op {
	return newTxnOp(c, base, nextStart, ser, readFanout, stats, txnPublishWaitCap)
}

// newTxnOp is NewTxnOp with an overridable read-your-writes wait cap so
// tests can exercise cap expiry quickly.
func newTxnOp(c *http.Client, base string, nextStart func() string, ser bool, readFanout int, stats *TxnStats, waitCap time.Duration) Op {
	return func() error {
		start := nextStart()
		for attempt := 1; attempt <= maxTxnAttempts; attempt++ {
			// 1. Read phase: neighbors of the start node at the current
			//    published snapshot; the token and read set guard the commit.
			t0 := time.Now()
			snapshot, readNodes, err := readNeighbors(c, base, start, readFanout)
			stats.readNs.Add(int64(time.Since(t0)))
			if err != nil {
				return err
			}
			// 2. Write phase: upsert the start node (same key family as the
			//    read → realistic hot-node contention; start nodes are
			//    degree-weighted by the parameter curator).
			t0 = time.Now()
			committed, index, err := commitTxn(c, base, start, attempt, snapshot, ser, readNodes)
			d := time.Since(t0)
			stats.commitNs.Add(int64(d))
			stats.attempts.Add(1)
			if err != nil {
				return err
			}
			if committed {
				stats.Commits.Add(1)
				// 3. Read-your-writes: the commit is durable in the raft log
				//    but the read snapshot is published on a SealInterval
				//    throttle; without waiting, our next guard conflicts
				//    with our own not-yet-published write.
				t0 = time.Now()
				waitForPublish(c, base, index, waitCap)
				stats.waitNs.Add(int64(time.Since(t0)))
				return nil
			}
			// 409: normal optimistic-concurrency rejection. Refresh the
			// token (re-read) and retry, with a small pause to avoid a
			// retry storm against the leader.
			stats.Conflicts.Add(1)
			if attempt < maxTxnAttempts {
				stats.Retries.Add(1)
				time.Sleep(time.Millisecond)
			}
		}
		return nil
	}
}

// txnRequestBody mirrors the POST /v1/txn wire format.
type txnRequestBody struct {
	Nodes []txnNode    `json:"nodes"`
	Txn   txnGuardBody `json:"txn"`
}

type txnNode struct {
	Key   string         `json:"key"`
	Label string         `json:"label"`
	Props map[string]any `json:"props"`
}

type txnGuardBody struct {
	Snapshot  uint64   `json:"snapshot"`
	SER       bool     `json:"ser"`
	ReadNodes []string `json:"readNodes"`
}

// neighborsResponse mirrors the GET /v1/neighbors wire format (key field only).
type neighborsResponse struct {
	Nodes []struct {
		Key string `json:"key"`
	} `json:"nodes"`
}

// readNeighbors fetches the out-neighbors of key and returns the snapshot
// token from the X-Vectra-Snapshot header plus the read set: the start key
// first, followed by up to readFanout neighbor keys.
func readNeighbors(c *http.Client, base, key string, readFanout int) (uint64, []string, error) {
	resp, err := c.Get(base + "/v1/neighbors?key=" + key + "&dir=out")
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, nil, fmt.Errorf("neighbors %q: %s", key, resp.Status)
	}
	snap, err := strconv.ParseUint(resp.Header.Get("X-Vectra-Snapshot"), 10, 64)
	if err != nil {
		return 0, nil, fmt.Errorf("neighbors %q: bad X-Vectra-Snapshot header: %w", key, err)
	}
	var body neighborsResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0, nil, fmt.Errorf("neighbors %q decode: %w", key, err)
	}
	read := make([]string, 0, 1+min(readFanout, len(body.Nodes)))
	read = append(read, key)
	for _, n := range body.Nodes {
		if len(read)-1 >= readFanout {
			break
		}
		read = append(read, n.Key)
	}
	return snap, read, nil
}

// commitTxn posts the guarded upsert of key (write marker w = attempt) and
// reports whether it committed plus the commit's raft index (for the
// read-your-writes wait). A 409 response is returned as committed=false
// with a nil error.
func commitTxn(c *http.Client, base, key string, attempt int, snapshot uint64, ser bool, readNodes []string) (bool, uint64, error) {
	body, err := json.Marshal(txnRequestBody{
		Nodes: []txnNode{{Key: key, Label: "Svc", Props: map[string]any{"w": attempt}}},
		Txn:   txnGuardBody{Snapshot: snapshot, SER: ser, ReadNodes: readNodes},
	})
	if err != nil {
		return false, 0, err
	}
	resp, err := c.Post(base+"/v1/txn", "application/json", bytes.NewReader(body))
	if err != nil {
		return false, 0, err
	}
	defer resp.Body.Close()
	var out struct {
		Committed bool    `json:"committed"`
		Index     float64 `json:"index"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return false, 0, fmt.Errorf("txn %q decode: %w", key, err)
	}
	switch resp.StatusCode {
	case http.StatusOK:
		return out.Committed, uint64(out.Index), nil
	case http.StatusConflict:
		return false, 0, nil
	default:
		return false, 0, fmt.Errorf("txn %q: %s", key, resp.Status)
	}
}

// awaitResponse mirrors GET /v1/await.
type awaitResponse struct {
	PublishedSeq float64 `json:"publishedSeq"`
	Reached      bool    `json:"reached"`
}

// waitForPublish blocks until the server reports a publishedSeq at or beyond
// the commit's raft index, using the /v1/await long poll (one round trip per
// server-side wait window instead of client-side poll quantization), at most
// waitCap total. Transport failures simply read as "not yet published" and
// re-await; cap expiry proceeds silently — the wait is best-effort client
// etiquette that keeps the next read from conflicting with our own
// not-yet-published write, not a correctness gate.
func waitForPublish(c *http.Client, base string, index uint64, waitCap time.Duration) {
	deadline := time.Now().Add(waitCap)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return
		}
		serverWait := min(remaining, 5*time.Second)
		resp, err := c.Get(base + "/v1/await?seq=" + strconv.FormatUint(index, 10) +
			"&timeoutMs=" + strconv.Itoa(int(serverWait.Milliseconds())))
		if err == nil {
			var out awaitResponse
			derr := json.NewDecoder(resp.Body).Decode(&out)
			resp.Body.Close()
			if derr == nil && out.Reached {
				return
			}
		}
		if !time.Now().Before(deadline) {
			return
		}
	}
}
