package server

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"

	"github.com/projanvil/vectra"
)

// defaultSmallGraphLimit: when the live graph size is below this value, publishing
// is not throttled (each batch seals and publishes immediately); above it,
// publishing is throttled by SealInterval. Dependency-graph workloads are almost
// always far below this value.
const defaultSmallGraphLimit = 100_000

// ApplyResult is the return value of FSM.Apply (passed back via ApplyFuture.Response).
type ApplyResult struct {
	NodesApplied int
	EdgesApplied int
	EdgesSkipped int // Edges referencing nonexistent nodes are deterministically skipped (counted in metrics)
}

// Metrics is the server's atomic counters (expvar is mounted in api.go).
type Metrics struct {
	AppliedBatches atomic.Int64
	AppliedNodes   atomic.Int64
	AppliedEdges   atomic.Int64
	SkippedEdges   atomic.Int64
	Seals          atomic.Int64
	Restores       atomic.Int64
	LeaderChanges  atomic.Int64
}

// fsm implements raft.FSM: a live writable graph + a hot-swap publisher.
//
//   - Apply: deterministically replays a batch into live (short critical section under RLock);
//   - publisher: clone live (O(V+E) copy under RLock) → Seal outside the lock →
//     atomic.Pointer swap → queries never interrupted, lock-free concurrent reads;
//   - Snapshot/Persist: clone live and serialize directly (no Seal needed);
//   - Restore: rebuild live from the byte stream and immediately do the first publish.
type fsm struct {
	mu        sync.RWMutex
	live      *vectra.Graph
	dirty     bool
	version   int64 // Monotonically increasing; the publisher uses it to detect writes after cloning
	published atomic.Pointer[vectra.Graph]

	sealInterval    time.Duration
	smallGraphLimit int

	lastSealNanos atomic.Int64
	pubMu         sync.Mutex // Publish critical section: keeps published monotonically advancing by version
	publishedVer  int64      // Guarded by pubMu; prevents a late old clone from overwriting a newer snapshot

	ready     chan struct{}
	readyOnce sync.Once
	stop      chan struct{}
	stopOnce  sync.Once
	wake      chan struct{}

	metrics *Metrics
}

func newFSM(sealInterval time.Duration) *fsm {
	f := &fsm{
		live:            vectra.New(),
		sealInterval:    sealInterval,
		smallGraphLimit: defaultSmallGraphLimit,
		ready:           make(chan struct{}),
		stop:            make(chan struct{}),
		wake:            make(chan struct{}, 1),
		metrics:         &Metrics{},
	}
	// Throttle interval counts from process start to avoid the zero-value epoch (Since = 59 years) triggering an immediate publish
	f.lastSealNanos.Store(time.Now().UnixNano())
	go f.publishLoop()
	return f
}

// Apply is the deterministic replay entry point after a raft commit. Returns ApplyResult or error.
func (f *fsm) Apply(l *raft.Log) interface{} {
	b, err := decodeBatch(l.Data)
	if err != nil {
		return err
	}
	res := ApplyResult{}
	f.mu.Lock()
	for _, n := range b.Nodes {
		if _, err := f.live.AddNode(n.Key, n.Label, n.Props); err == nil {
			res.NodesApplied++
		}
	}
	for _, e := range b.Edges {
		if _, err := f.live.AddEdge(e.SrcKey, e.DstKey, e.Label, e.Props); err == nil {
			res.EdgesApplied++
		} else if errors.Is(err, vectra.ErrNodeNotFound) {
			res.EdgesSkipped++ // Authoritative validation: replay skips invalid items, no divergence
		}
	}
	f.dirty = true
	f.version++
	small := f.live.NodeCount()+f.live.EdgeCount() < f.smallGraphLimit
	f.mu.Unlock()

	f.metrics.AppliedBatches.Add(1)
	f.metrics.AppliedNodes.Add(int64(res.NodesApplied))
	f.metrics.AppliedEdges.Add(int64(res.EdgesApplied))
	f.metrics.SkippedEdges.Add(int64(res.EdgesSkipped))
	if small {
		f.wakePublisher()
	}
	return res
}

// Published returns the most recently sealed-and-published graph (immutable, lock-free concurrent reads).
// Returns nil before the first publish.
func (f *fsm) Published() *vectra.Graph {
	return f.published.Load()
}

// Ready closes once the first publish completes (read readiness gate for restart recovery).
func (f *fsm) Ready() <-chan struct{} { return f.ready }

func (f *fsm) wakePublisher() {
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

// publishInterval determines this publish's wait: small graphs publish immediately
// (no wait if wake already fired), large graphs throttle by SealInterval.
func (f *fsm) publishLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-f.stop:
			return
		case <-f.wake:
			f.publishOnce()
		case <-ticker.C:
			// Large graphs throttle by SealInterval; small graphs (< smallGraphLimit)
			// just need the per-tick check (Apply already wakes immediately).
			f.mu.RLock()
			dirty := f.dirty
			small := f.live.NodeCount()+f.live.EdgeCount() < f.smallGraphLimit
			f.mu.RUnlock()
			if dirty && (small || time.Since(time.Unix(0, f.lastSealNanos.Load())) >= f.sealInterval) {
				f.publishOnce()
			}
		}
	}
}

// sealNow publishes immediately (shared by tests and large-graph interval expiry).
func (f *fsm) sealNow() { f.publishOnce() }

func (f *fsm) publishOnce() {
	f.mu.RLock()
	if f.live == nil {
		f.mu.RUnlock()
		return
	}
	clone := f.live.Clone()
	version := f.version
	f.mu.RUnlock()

	if err := clone.Seal(); err != nil {
		fmt.Fprintf(os.Stderr, "vectra/server: publish failed (Seal): %v\n", err)
		return
	}
	f.pubMu.Lock()
	if version > f.publishedVer { // With Restore/publisher racing, only newer versions may land
		f.publishedVer = version
		f.published.Store(clone)
		f.metrics.Seals.Add(1)
		f.lastSealNanos.Store(time.Now().UnixNano())
	}
	f.pubMu.Unlock()

	f.mu.Lock()
	if f.version == version { // Clear dirty only if no new writes happened after cloning
		f.dirty = false
	}
	f.mu.Unlock()

	f.readyOnce.Do(func() { close(f.ready) })
}

// Snapshot implements raft.FSM: returns a cloned view of live (snapshot serialized outside the lock).
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return &graphSnapshot{clone: f.live.Clone()}, nil
}

type graphSnapshot struct {
	clone *vectra.Graph
}

// Persist serializes the cloned live graph into the sink (the unsealed state can
// be exported directly, no Seal required — saves sealing a large graph once).
func (s *graphSnapshot) Persist(sink raft.SnapshotSink) error {
	data, err := s.clone.ExportSnapshot()
	if err != nil {
		return err
	}
	if _, err := sink.Write(data); err != nil {
		return err
	}
	return sink.Close()
}

func (s *graphSnapshot) Release() {}

// Restore rebuilds live from snapshot bytes and immediately does the first publish (readiness gate opens here).
func (f *fsm) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	g, err := vectra.ImportSnapshot(data)
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.live = g
	f.dirty = true
	f.version++
	f.mu.Unlock()
	f.metrics.Restores.Add(1)
	f.sealNow()
	return nil
}

// Close stops the publisher (idempotent).
func (f *fsm) Close() {
	f.stopOnce.Do(func() { close(f.stop) })
}
