package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"maps"
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

// maxConflicts caps the per-key conflict list returned to a rejected client;
// ConflictTotal always reports the untruncated count.
const maxConflicts = 100

// ApplyResult is the return value of FSM.Apply (passed back via ApplyFuture.Response).
type ApplyResult struct {
	NodesApplied  int
	EdgesApplied  int
	EdgesSkipped  int // Edges referencing nonexistent nodes are deterministically skipped (counted in metrics)
	EdgesDeleted  int // Explicit deletes + cascade deletes
	NodesDeleted  int
	NodesSkipped  int      // Deletes of nonexistent keys
	Committed     bool     // false only for TxnGuard-rejected batches
	Index         uint64   // raft log index of this entry
	Conflicts     []string // conflicting keys, truncated to maxConflicts
	ConflictTotal int      // total conflict count before truncation
}

// Metrics is the server's atomic counters (expvar is mounted in api.go).
type Metrics struct {
	AppliedBatches atomic.Int64
	AppliedNodes   atomic.Int64
	AppliedEdges   atomic.Int64
	SkippedEdges   atomic.Int64
	DeletedEdges   atomic.Int64
	DeletedNodes   atomic.Int64
	Seals          atomic.Int64
	Restores       atomic.Int64
	LeaderChanges  atomic.Int64
	TxnCommits     atomic.Int64
	TxnConflicts   atomic.Int64
}

// edgeVerKey addresses an edge in the version table by its logical key
// (same identity discipline as EdgeDelete: Seal renumbers IDs, keys are stable).
type edgeVerKey struct{ Src, Dst, Label string }

// pubState bundles the published graph with the applied raft index it
// reflects, so readers can never pair a newer seq with an older graph
// (audit P1-2: token must never overshoot graph content).
type pubState struct {
	graph *vectra.Graph
	seq   uint64
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
	published atomic.Pointer[pubState]
	// pubNotify is the publish-notification channel (long-poll primitive):
	// closed on every successful publish, then swapped for a fresh one.
	// Waiters snapshot it and select; see WaitPublished.
	pubNotify atomic.Pointer[chan struct{}]

	nodeVer map[string]uint64     // per-node-key last-modified raft index; guarded by mu
	edgeVer map[edgeVerKey]uint64 // per-edge-key last-modified raft index; guarded by mu

	appliedIndex atomic.Uint64 // high-water mark of applied raft log indexes

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
		nodeVer:         map[string]uint64{},
		edgeVer:         map[edgeVerKey]uint64{},
		sealInterval:    sealInterval,
		smallGraphLimit: defaultSmallGraphLimit,
		ready:           make(chan struct{}),
		stop:            make(chan struct{}),
		wake:            make(chan struct{}, 1),
		metrics:         &Metrics{},
	}
	notify := make(chan struct{})
	f.pubNotify.Store(&notify)
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
	wNodes, wEdges := f.effectiveWriteSet(b) // always: blind writes version too
	if b.Txn != nil {
		if conflicts, total := f.validateTxn(b.Txn, l.Index, wNodes, wEdges); conflicts != nil {
			f.mu.Unlock()
			f.metrics.TxnConflicts.Add(1)
			return ApplyResult{Committed: false, Index: l.Index, Conflicts: conflicts, ConflictTotal: total}
		}
	}
	// One Batch boundary per committed entry: the replay materializes as a
	// single micro-layer (and, on a WAL-attached graph, one frame), not one
	// per op. Determinism is unchanged — the wrap preserves op order and
	// every per-op outcome, it only regroups layer/frame granularity, so all
	// replicas fold the same result. The boundary's flush error is ignored
	// deliberately: it depends on local disk state (not log content) and must
	// never influence ApplyResult or the in-memory replay, or replicas could
	// diverge; live has no WAL attached here, so it cannot fire anyway.
	_ = f.live.Batch(func(g *vectra.Graph) {
		for _, n := range b.Nodes {
			if _, err := g.AddNode(n.Key, n.Label, n.Props); err == nil {
				res.NodesApplied++
			}
		}
		for _, e := range b.Edges {
			var err error
			if e.AllowParallel {
				_, err = g.AddEdge(e.SrcKey, e.DstKey, e.Label, e.Props)
			} else {
				_, err = g.UpsertEdge(e.SrcKey, e.DstKey, e.Label, e.Props)
			}
			if err == nil {
				res.EdgesApplied++
			} else if errors.Is(err, vectra.ErrNodeNotFound) {
				res.EdgesSkipped++ // Authoritative validation: replay skips invalid items, no divergence
			}
		}
		for _, d := range b.EdgeDeletes {
			n, err := g.DeleteEdge(d.SrcKey, d.DstKey, d.Label)
			if err != nil {
				res.EdgesSkipped++ // References a nonexistent node: deterministic skip, no divergence
				continue
			}
			res.EdgesDeleted += n
		}
		for _, key := range b.NodeDeletes {
			cascaded, err := g.DeleteNode(key)
			if err != nil {
				res.NodesSkipped++
				continue
			}
			res.NodesDeleted++
			res.EdgesDeleted += cascaded
		}
	})
	f.dirty = true
	f.version++
	f.appliedIndex.Store(l.Index)
	for _, k := range wNodes {
		f.nodeVer[k] = l.Index
	}
	for _, k := range wEdges {
		f.edgeVer[k] = l.Index
	}
	small := f.live.NodeCount()+f.live.EdgeCount() < f.smallGraphLimit
	f.mu.Unlock()

	if b.Txn != nil {
		f.metrics.TxnCommits.Add(1)
	}
	f.metrics.AppliedBatches.Add(1)
	f.metrics.AppliedNodes.Add(int64(res.NodesApplied))
	f.metrics.AppliedEdges.Add(int64(res.EdgesApplied))
	f.metrics.SkippedEdges.Add(int64(res.EdgesSkipped))
	f.metrics.DeletedEdges.Add(int64(res.EdgesDeleted))
	f.metrics.DeletedNodes.Add(int64(res.NodesDeleted))
	if small {
		f.wakePublisher()
	}
	res.Committed = true
	res.Index = l.Index
	return res
}

// Published returns the most recently sealed-and-published graph (immutable, lock-free concurrent reads).
// Returns nil before the first publish.
func (f *fsm) Published() *vectra.Graph {
	if ps := f.published.Load(); ps != nil {
		return ps.graph
	}
	return nil
}

// PublishedSeq reports the applied raft index reflected by the current
// published graph (0 before the first publish).
func (f *fsm) PublishedSeq() uint64 {
	if ps := f.published.Load(); ps != nil {
		return ps.seq
	}
	return 0
}

// waitCh returns the current publish-notification channel; it is closed on
// the next successful publish.
func (f *fsm) waitCh() <-chan struct{} { return *f.pubNotify.Load() }

// WaitPublished blocks until publishedSeq >= minSeq or ctx ends, then
// returns the current publishedSeq. Lock-free: publishers close-and-swap
// pubNotify under pubMu; waiters double-check the seq around snapshotting
// the channel so a publish landing between check and arm is never missed.
func (f *fsm) WaitPublished(ctx context.Context, minSeq uint64) uint64 {
	for {
		if seq := f.PublishedSeq(); seq >= minSeq {
			return seq
		}
		ch := f.waitCh()
		if seq := f.PublishedSeq(); seq >= minSeq {
			return seq
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return f.PublishedSeq()
		}
	}
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
	seq := f.appliedIndex.Load()
	f.mu.RUnlock()

	if err := clone.Seal(); err != nil {
		fmt.Fprintf(os.Stderr, "vectra/server: publish failed (Seal): %v\n", err)
		return
	}
	f.pubMu.Lock()
	if version > f.publishedVer { // With Restore/publisher racing, only newer versions may land
		f.publishedVer = version
		f.published.Store(&pubState{graph: clone, seq: seq})
		f.metrics.Seals.Add(1)
		f.lastSealNanos.Store(time.Now().UnixNano())
		// Wake long-poll waiters: close the current notification channel and
		// arm a fresh one. Inside pubMu so publishes serialize the swap.
		fresh := make(chan struct{})
		old := f.pubNotify.Swap(&fresh)
		close(*old)
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
	// Copy the version tables under the same RLock as the graph clone: the
	// snapshot's raft metadata index aligns with this call on the FSM thread
	// (raft v1.8), and Persist runs later on the snapshot thread — it must
	// only ever see this frozen copy.
	return &graphSnapshot{
		clone:   f.live.Clone(),
		nodeVer: maps.Clone(f.nodeVer),
		edgeVer: maps.Clone(f.edgeVer),
		applied: f.appliedIndex.Load(),
	}, nil
}

type graphSnapshot struct {
	clone   *vectra.Graph
	nodeVer map[string]uint64
	edgeVer map[edgeVerKey]uint64
	applied uint64
}

// verSnapshot is the gob payload of the VCTX trailer: the version tables and
// the applied-index high-water mark at Snapshot time.
type verSnapshot struct {
	NodeVer      map[string]uint64
	EdgeVer      map[edgeVerKey]uint64
	AppliedIndex uint64
}

const vctxMagic = "VCTX"

// legacyStoreMagic is the header magic of pre-0.4.0 graph-only snapshots
// (internal/store: "VCTR" | u16 version | counts...). Read as the u32 LE
// framing length below, it parses as ~1.38e9, so genuine legacy snapshots
// always fail the framing guard — which is where they are detected.
const legacyStoreMagic = "VCTR"

// errLegacySnapshot is the runbook-pointing rejection for pre-0.4.0 graph-only
// snapshots: a replica restored from one would validate transactions against
// an empty version table and diverge permanently from log-derived replicas.
var errLegacySnapshot = errors.New("server: legacy snapshot without VCTX rejected; re-seed from a 0.4.0+ snapshot (see upgrade runbook)")

// Persist serializes the cloned live graph plus the VCTX version-table trailer
// into the sink (the unsealed state can be exported directly, no Seal required —
// saves sealing a large graph once).
func (s *graphSnapshot) Persist(sink raft.SnapshotSink) error {
	data, err := s.clone.ExportSnapshot()
	if err != nil {
		return err
	}
	var verBuf bytes.Buffer
	if err := gob.NewEncoder(&verBuf).Encode(verSnapshot{
		NodeVer: s.nodeVer, EdgeVer: s.edgeVer, AppliedIndex: s.applied,
	}); err != nil {
		return err
	}
	var head [4]byte
	binary.LittleEndian.PutUint32(head[:], uint32(len(data)))
	var fv [2]byte
	binary.LittleEndian.PutUint16(fv[:], 1) // u16 LE format version, matches splitSnapshot
	for _, chunk := range [][]byte{head[:], data, []byte(vctxMagic), fv[:], verBuf.Bytes()} {
		if _, err := sink.Write(chunk); err != nil {
			return err
		}
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
	graph, vers, err := splitSnapshot(data)
	if err != nil {
		return err
	}
	g, err := vectra.ImportSnapshot(graph)
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.live = g
	f.nodeVer = vers.NodeVer
	f.edgeVer = vers.EdgeVer
	f.appliedIndex.Store(vers.AppliedIndex)
	f.dirty = true
	f.version++
	f.mu.Unlock()
	f.metrics.Restores.Add(1)
	f.sealNow()
	return nil
}

// splitSnapshot separates the graph payload from the VCTX version-table
// trailer. Legacy snapshots (pre-0.4.0, graph only) are rejected: replicas
// restored from them would validate transactions against an empty table and
// diverge permanently from log-derived replicas.
func splitSnapshot(data []byte) ([]byte, verSnapshot, error) {
	var vs verSnapshot
	if len(data) < 6 { // u32 len + "VCTX" minimum
		return nil, vs, errors.New("server: snapshot too short")
	}
	n := binary.LittleEndian.Uint32(data[:4])
	// uint64 arithmetic: int(n)+10 wraps negative on 32-bit platforms for
	// crafted framing lengths and would slip past the guard into the slicing
	// below (out-of-range panic). 10 = u32 len + "VCTX" + u16 version.
	if uint64(n)+10 > uint64(len(data)) {
		// A genuine pre-0.4.0 snapshot starts with the store magic "VCTR",
		// which parses here as a ~1.38e9 framing length — surface the
		// runbook error, not the generic corrupt-framing one.
		if bytes.HasPrefix(data, []byte(legacyStoreMagic)) {
			return nil, vs, errLegacySnapshot
		}
		return nil, vs, errors.New("server: snapshot framing corrupt")
	}
	graph := data[4 : 4+n]
	rest := data[4+n:]
	if !bytes.HasPrefix(rest, []byte(vctxMagic)) {
		return nil, vs, errLegacySnapshot
	}
	rest = rest[len(vctxMagic):]
	if len(rest) < 2 {
		return nil, vs, errors.New("server: VCTX version missing")
	}
	if binary.LittleEndian.Uint16(rest[:2]) != 1 {
		return nil, vs, fmt.Errorf("server: unsupported VCTX version %d", binary.LittleEndian.Uint16(rest[:2]))
	}
	if err := gob.NewDecoder(bytes.NewReader(rest[2:])).Decode(&vs); err != nil {
		return nil, vs, err
	}
	if vs.NodeVer == nil {
		vs.NodeVer = map[string]uint64{}
	}
	if vs.EdgeVer == nil {
		vs.EdgeVer = map[edgeVerKey]uint64{}
	}
	return graph, vs, nil
}

// Close stops the publisher (idempotent).
func (f *fsm) Close() {
	f.stopOnce.Do(func() { close(f.stop) })
}

// effectiveWriteSet enumerates every logical key the batch will modify,
// including cascade peers of node deletes (their adjacency changes too).
// Must run under f.mu with replay not yet started. Unconditional: request
// keys are included even if replay will skip them (safe over-rejection only).
func (f *fsm) effectiveWriteSet(b Batch) (nodes []string, edges []edgeVerKey) {
	seenN := map[string]struct{}{}
	seenE := map[edgeVerKey]struct{}{}
	addN := func(k string) {
		if _, ok := seenN[k]; !ok {
			seenN[k] = struct{}{}
			nodes = append(nodes, k)
		}
	}
	addE := func(src, dst, label string) {
		k := edgeVerKey{Src: src, Dst: dst, Label: label}
		if _, ok := seenE[k]; !ok {
			seenE[k] = struct{}{}
			edges = append(edges, k)
		}
	}
	for _, n := range b.Nodes {
		addN(n.Key)
	}
	for _, e := range b.Edges {
		addN(e.SrcKey)
		addN(e.DstKey)
		addE(e.SrcKey, e.DstKey, e.Label)
	}
	for _, d := range b.EdgeDeletes {
		addN(d.SrcKey)
		addN(d.DstKey)
		addE(d.SrcKey, d.DstKey, d.Label)
	}
	for _, key := range b.NodeDeletes {
		addN(key)
		for _, dir := range []vectra.Direction{vectra.DirOut, vectra.DirIn} {
			es, err := f.live.EdgesOf(key, dir)
			if err != nil {
				continue // nonexistent key: no incident edges to cascade
			}
			for _, e := range es {
				// Edge.Src/Dst are NodeIDs; resolve back to string keys —
				// the version table is key-addressed (Seal renumbers IDs).
				src, ok1 := f.live.NodeByID(e.Src)
				dst, ok2 := f.live.NodeByID(e.Dst)
				if !ok1 || !ok2 {
					continue
				}
				addN(src.Key)
				addN(dst.Key)
				addE(src.Key, dst.Key, e.Label)
			}
		}
	}
	return nodes, edges
}

// validateTxn returns the first maxConflicts conflicting keys and the total
// conflict count for a guarded batch, or (nil, 0) when it may commit. Must
// run under f.mu before replay.
//
// A table entry equal to this entry's own log index is the entry's earlier
// application (same-index replay, e.g. re-applying an entry already included
// in a restored snapshot) — not a conflict — so a replayed committed entry
// reproduces its original decision. For fresh entries indexes strictly
// increase, so own-index entries never occur in normal operation.
func (f *fsm) validateTxn(tg *TxnGuard, idx uint64, wNodes []string, wEdges []edgeVerKey) ([]string, int) {
	var conflicts []string
	seen := map[string]struct{}{}
	total := 0
	add := func(k string) {
		if _, ok := seen[k]; ok {
			return
		}
		seen[k] = struct{}{}
		total++
		if len(conflicts) < maxConflicts {
			conflicts = append(conflicts, k)
		}
	}
	nodeDirty := func(k string) bool {
		return f.nodeVer[k] > tg.Snapshot && f.nodeVer[k] != idx
	}
	edgeDirty := func(k edgeVerKey) bool {
		return f.edgeVer[k] > tg.Snapshot && f.edgeVer[k] != idx
	}
	for _, k := range wNodes {
		if nodeDirty(k) {
			add(k)
		}
	}
	for _, k := range wEdges {
		if edgeDirty(k) {
			add(edgeKeyString(k))
		}
	}
	if tg.SER {
		for _, k := range tg.ReadNodes {
			if nodeDirty(k) {
				add(k)
			}
		}
		for _, e := range tg.ReadEdges {
			if edgeDirty(edgeVerKey{Src: e.Src, Dst: e.Dst, Label: e.Label}) {
				add(edgeKeyString(edgeVerKey{Src: e.Src, Dst: e.Dst, Label: e.Label}))
			}
		}
	}
	if total == 0 {
		return nil, 0
	}
	return conflicts, total
}

func edgeKeyString(k edgeVerKey) string { return k.Src + "\x00" + k.Dst + "\x00" + k.Label }
