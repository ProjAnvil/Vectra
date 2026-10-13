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
// op refreshes the token and retries, at most maxTxnAttempts total; only
// transport/5xx failures surface as op errors.
func NewTxnOp(c *http.Client, base string, nextStart func() string, ser bool, readFanout int, stats *TxnStats) Op {
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
			committed, err := commitTxn(c, base, start, attempt, snapshot, ser, readNodes)
			if err != nil {
				return err
			}
			if committed {
				stats.Commits.Add(1)
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
// reports whether it committed. A 409 response is returned as
// committed=false with a nil error.
func commitTxn(c *http.Client, base, key string, attempt int, snapshot uint64, ser bool, readNodes []string) (bool, error) {
	body, err := json.Marshal(txnRequestBody{
		Nodes: []txnNode{{Key: key, Label: "Svc", Props: map[string]any{"w": attempt}}},
		Txn:   txnGuardBody{Snapshot: snapshot, SER: ser, ReadNodes: readNodes},
	})
	if err != nil {
		return false, err
	}
	resp, err := c.Post(base+"/v1/txn", "application/json", bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	var out struct {
		Committed bool `json:"committed"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return false, fmt.Errorf("txn %q decode: %w", key, err)
	}
	switch resp.StatusCode {
	case http.StatusOK:
		return out.Committed, nil
	case http.StatusConflict:
		return false, nil
	default:
		return false, fmt.Errorf("txn %q: %s", key, resp.Status)
	}
}
