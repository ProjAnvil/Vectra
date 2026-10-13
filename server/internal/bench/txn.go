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

// txnPollInterval is the read-your-writes poll cadence against GET /v1/stats
// after a successful commit.
const txnPollInterval = 10 * time.Millisecond

// txnPublishWaitCap bounds the read-your-writes wait after a commit. On
// expiry the op proceeds without error: the wait is best-effort client
// etiquette, not a correctness gate.
const txnPublishWaitCap = 5 * time.Second

// TxnStats tracks OCC outcomes for transaction workloads: a 409 is a normal
// optimistic-concurrency result, not an error.
type TxnStats struct {
	Commits   atomic.Int64
	Conflicts atomic.Int64 // 409 rejections encountered (each retry attempt counts)
	Retries   atomic.Int64
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
			snapshot, readNodes, err := readNeighbors(c, base, start, readFanout)
			if err != nil {
				return err
			}
			// 2. Write phase: upsert the start node (same key family as the
			//    read → realistic hot-node contention; start nodes are
			//    degree-weighted by the parameter curator).
			committed, index, err := commitTxn(c, base, start, attempt, snapshot, ser, readNodes)
			if err != nil {
				return err
			}
			if committed {
				stats.Commits.Add(1)
				// 3. Read-your-writes: the commit is durable in the raft log
				//    but the read snapshot is published on a SealInterval
				//    throttle; without waiting, our next guard conflicts
				//    with our own not-yet-published write.
				waitForPublish(c, base, index, waitCap)
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

// waitForPublish blocks until the stats endpoint reports a publishedSeq at
// or beyond the commit's raft index, polling every txnPollInterval and at
// most waitCap total. Poll failures (transport, non-200, malformed body)
// simply read as "not yet published". Cap expiry proceeds silently: the
// wait is best-effort client etiquette that keeps the next read from
// conflicting with our own not-yet-published write, not a correctness gate.
func waitForPublish(c *http.Client, base string, index uint64, waitCap time.Duration) {
	deadline := time.Now().Add(waitCap)
	for {
		if seq, err := fetchPublishedSeq(c, base); err == nil && seq >= float64(index) {
			return
		}
		if !time.Now().Before(deadline) {
			return
		}
		time.Sleep(txnPollInterval)
	}
}

// fetchPublishedSeq reads publishedSeq from GET /v1/stats. The endpoint
// returns a heterogeneous JSON object, so it is decoded as a map and the
// field read as a float64 (JSON numbers decode to float64 in any-valued
// maps).
func fetchPublishedSeq(c *http.Client, base string) (float64, error) {
	resp, err := c.Get(base + "/v1/stats")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("stats: %s", resp.Status)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0, fmt.Errorf("stats decode: %w", err)
	}
	seq, ok := body["publishedSeq"].(float64)
	if !ok {
		return 0, fmt.Errorf("stats: publishedSeq missing or non-numeric")
	}
	return seq, nil
}
