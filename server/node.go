// Package server is Vectra's optional server module: hashicorp/raft replication +
// HTTP JSON API + hot-swap publishing. The core engine module stays free of
// third-party dependencies; this module's dependency tree only affects users
// choosing the server form (full disclosure in the README).
//
// Consistency semantics: writes are linearizable (returns once raft commits);
// reads are bounded-stale (local published snapshot, freshness ≤ SealInterval +
// publish time). Anti-split-brain constraint: a node with non-empty Join never
// bootstraps; two fresh directories with empty Join each bootstrapping is an
// ops error (same class as etcd initial-cluster).
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"github.com/projanvil/vectra"
)

// DefaultSealInterval is the default publish throttle interval for large graphs;
// graphs smaller than smallGraphLimit publish immediately per batch.
const DefaultSealInterval = 2 * time.Second

var (
	ErrNotLeader   = errors.New("server: not leader")
	ErrTimeout     = errors.New("server: apply timeout")
	ErrShutdown    = errors.New("server: raft shutting down")
	ErrDirMismatch = errors.New("server: data dir belongs to a different node")
)

// Config assembles a server node. LogStore/StableStore/SnapshotStore/Transport
// use production defaults when nil (BoltDB + FileSnapshotStore + TCP);
// injecting the Inmem variants enables in-process cluster testing. RaftConfig
// nil means default parameters.
type Config struct {
	NodeID        string
	HTTPAddr      string // External HTTP listen address (:0 allowed)
	RaftAddr      string // raft peer address
	DataDir       string // Persistence directory (raft.db / snapshots / meta.json)
	Join          string // Seed node's HTTP address; empty = single-node bootstrap
	SealInterval  time.Duration
	LogStore      raft.LogStore
	StableStore   raft.StableStore
	SnapshotStore raft.SnapshotStore
	Transport     raft.Transport
	RaftConfig    *raft.Config
}

type nodeMeta struct {
	NodeID   string `json:"nodeID"`
	HTTPAddr string `json:"httpAddr"`
	RaftAddr string `json:"raftAddr"`
}

// Node is a Vectra server node: raft replication + FSM publishing + HTTP API.
type Node struct {
	cfg Config
	fsm *fsm
	r   *raft.Raft

	bolt  *raftboltdb.BoltStore // Held in production mode; nil in test mode
	trans raft.Transport

	regMu    sync.Mutex
	registry map[string]string // raftAddr -> httpAddr (best-effort hint table)

	closeOnce sync.Once
	closed    chan struct{}

	leaderObs *raft.Observer
	leaderChg chan raft.Observation

	queryCount atomic.Int64 // Used by api.go
}

// New opens (or creates) a node: restore raft state → assemble FSM →
// bootstrap/join as needed. Empty Join with no existing state = single-node bootstrap.
func New(cfg Config) (*Node, error) {
	if cfg.NodeID == "" {
		return nil, errors.New("server: NodeID required")
	}
	if cfg.DataDir == "" {
		return nil, errors.New("server: DataDir required")
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}
	if cfg.SealInterval == 0 {
		cfg.SealInterval = DefaultSealInterval
	}

	meta, err := loadOrCreateMeta(cfg)
	if err != nil {
		return nil, err
	}

	n := &Node{
		registry: map[string]string{cfg.RaftAddr: meta.HTTPAddr},
		closed:   make(chan struct{}),
	}

	// Persistence and transport
	if cfg.LogStore == nil || cfg.StableStore == nil {
		bolt, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
		if err != nil {
			return nil, err
		}
		n.bolt = bolt
		cfg.LogStore, cfg.StableStore = bolt, bolt
	}
	if cfg.SnapshotStore == nil {
		cfg.SnapshotStore, err = raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
		if err != nil {
			n.Close()
			return nil, err
		}
	}
	if cfg.Transport == nil {
		addr, err := net.ResolveTCPAddr("tcp", cfg.RaftAddr)
		if err != nil {
			n.Close()
			return nil, err
		}
		cfg.Transport, err = raft.NewTCPTransport(cfg.RaftAddr, addr, 3, 10*time.Second, os.Stderr)
		if err != nil {
			n.Close()
			return nil, err
		}
	}
	n.trans = cfg.Transport
	n.cfg = cfg // Attach only after injected defaults are fully populated

	// FSM (publisher starts with it)
	n.fsm = newFSM(cfg.SealInterval)

	// Bootstrap decision: existing state → leave alone; empty Join → single-node bootstrap; non-empty Join → wait for the seed to absorb us
	hasState, err := raft.HasExistingState(cfg.LogStore, cfg.StableStore, cfg.SnapshotStore)
	if err != nil {
		n.Close()
		return nil, err
	}
	if !hasState && cfg.Join == "" {
		conf := n.raftConfig()
		servers := []raft.Server{{
			ID:       raft.ServerID(cfg.NodeID),
			Address:  raft.ServerAddress(cfg.RaftAddr),
			Suffrage: raft.Voter,
		}}
		if err := raft.BootstrapCluster(conf, cfg.LogStore, cfg.StableStore,
			cfg.SnapshotStore, cfg.Transport, raft.Configuration{Servers: servers}); err != nil {
			n.Close()
			return nil, err
		}
	}

	raftCfg := n.raftConfig()
	r, err := raft.NewRaft(raftCfg, n.fsm, cfg.LogStore, cfg.StableStore, cfg.SnapshotStore, cfg.Transport)
	if err != nil {
		n.Close()
		return nil, err
	}
	n.r = r

	// Leader change observer (metrics)
	n.leaderChg = make(chan raft.Observation, 16)
	n.leaderObs = raft.NewObserver(n.leaderChg, false, func(o *raft.Observation) bool {
		_, ok := o.Data.(raft.LeaderObservation)
		return ok
	})
	r.RegisterObserver(n.leaderObs)
	go n.watchLeader()

	// Join an existing cluster (registry propagates via the join response)
	if cfg.Join != "" {
		if err := n.Join(cfg.Join); err != nil {
			// Not fatal: the node stays candidate; ops can retry join
			fmt.Fprintf(os.Stderr, "vectra/server: join %s failed: %v\n", cfg.Join, err)
		}
	}
	return n, nil
}

func (n *Node) raftConfig() *raft.Config {
	if n.cfg.RaftConfig != nil {
		return n.cfg.RaftConfig
	}
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(n.cfg.NodeID)
	return c
}

func loadOrCreateMeta(cfg Config) (nodeMeta, error) {
	path := filepath.Join(cfg.DataDir, "meta.json")
	var m nodeMeta
	if data, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(data, &m); err != nil {
			return m, fmt.Errorf("server: bad meta.json: %w", err)
		}
		if m.NodeID != cfg.NodeID {
			return m, fmt.Errorf("%w: dir=%s node=%s cfg=%s", ErrDirMismatch, cfg.DataDir, m.NodeID, cfg.NodeID)
		}
	} else if errors.Is(err, os.ErrNotExist) {
		m = nodeMeta{NodeID: cfg.NodeID}
	} else {
		return m, fmt.Errorf("server: read meta.json: %w", err) // Transient permission-class errors must not bypass the identity guard
	}
	// Addresses may drift (restart with new ports); IDs may not
	m.HTTPAddr, m.RaftAddr = cfg.HTTPAddr, cfg.RaftAddr
	data, _ := json.MarshalIndent(m, "", "  ")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return m, err
	}
	return m, nil
}

// Join registers this node with the seed (the seed side runs AddVoter and returns the registry).
func (n *Node) Join(seedHTTP string) error {
	body := map[string]string{
		"nodeID":   n.cfg.NodeID,
		"raftAddr": n.cfg.RaftAddr,
		"httpAddr": announceAddr(n.cfg.HTTPAddr),
	}
	data, _ := json.Marshal(body)
	resp, err := httpPost(seedHTTP+"/v1/join", data, 15*time.Second)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return fmt.Errorf("join: %s: %s", resp.Status, raw)
	}
	var out struct {
		Registry map[string]string `json:"registry"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		fmt.Fprintf(os.Stderr, "vectra/server: failed to parse join response (registry not synced): %v\n", err)
	}
	n.mergeRegistry(out.Registry)
	return nil
}

// announceAddr replaces placeholders like :0 with an announcable form (best effort).
func announceAddr(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	if host == "" || host == "0.0.0.0" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}

// Apply replicates a changeset through raft and commits it to the FSM. Returns ErrNotLeader when not the leader.
func (n *Node) Apply(ctx context.Context, b Batch) (ApplyResult, error) {
	data, err := encodeBatch(b)
	if err != nil {
		return ApplyResult{}, err
	}
	if n.r.State() != raft.Leader {
		return ApplyResult{}, ErrNotLeader
	}
	timeout := 10 * time.Second
	if dl, ok := ctx.Deadline(); ok {
		if t := time.Until(dl); t < timeout {
			timeout = t
		}
	}
	fut := n.r.Apply(data, timeout)
	if err := fut.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return ApplyResult{}, ErrNotLeader
		}
		if errors.Is(err, raft.ErrLeadershipLost) {
			return ApplyResult{}, ErrNotLeader
		}
		// raft's Apply surfaces backpressure on the apply channel as
		// ErrEnqueueTimeout (e.g. the FSM thread stalls on a large Clone);
		// an enqueued logFuture never carries context.DeadlineExceeded —
		// both mean "did not commit in time".
		if errors.Is(err, raft.ErrEnqueueTimeout) || errors.Is(err, context.DeadlineExceeded) {
			return ApplyResult{}, fmt.Errorf("%w: %w", ErrTimeout, err)
		}
		// A shutting-down node is unavailable, not timed out — etcd maps
		// this to Unavailable (retryable elsewhere/later); HTTP carries it
		// as 503 with Retry-After, never as a 504 timeout.
		if errors.Is(err, raft.ErrRaftShutdown) {
			return ApplyResult{}, fmt.Errorf("%w: %w", ErrShutdown, err)
		}
		return ApplyResult{}, err
	}
	switch v := fut.Response().(type) {
	case ApplyResult:
		return v, nil
	case error:
		return ApplyResult{}, v
	default:
		return ApplyResult{}, nil
	}
}

// IsLeader reports whether this node is currently the leader.
func (n *Node) IsLeader() bool { return n.r != nil && n.r.State() == raft.Leader }

// LeaderHint returns the known leader's HTTP address (best effort, may be empty).
func (n *Node) LeaderHint() string {
	if n.r == nil {
		return ""
	}
	addr := string(n.r.Leader())
	if addr == "" {
		return ""
	}
	n.regMu.Lock()
	defer n.regMu.Unlock()
	return n.registry[addr]
}

func (n *Node) mergeRegistry(m map[string]string) {
	if len(m) == 0 {
		return
	}
	n.regMu.Lock()
	defer n.regMu.Unlock()
	for k, v := range m {
		n.registry[k] = v
	}
}

// Published returns the most recently sealed-and-published local graph (nil = not yet published).
func (n *Node) Published() *vectra.Graph { return n.fsm.Published() }

// Ready becomes readable once the first publish completes (read readiness gate for restart recovery).
func (n *Node) Ready() <-chan struct{} { return n.fsm.Ready() }

// Close shuts down idempotently: stop publisher → raft.Shutdown → close persistence.
func (n *Node) Close() {
	n.closeOnce.Do(func() {
		close(n.closed)
		if n.fsm != nil {
			n.fsm.Close()
		}
		if n.r != nil {
			_ = n.r.Shutdown().Error()
		}
		if n.bolt != nil {
			_ = n.bolt.Close()
		}
		if closer, ok := n.trans.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
	})
}

func (n *Node) watchLeader() {
	for {
		select {
		case <-n.closed:
			return
		case <-n.leaderChg:
			if n.IsLeader() {
				n.fsm.metrics.LeaderChanges.Add(1)
			}
		}
	}
}
