// Package writer is the single-writer wrapper layer of v0.7 (spec §2.3
// "wrapper mode", the WriteHandle of spec §6): Handle.Submit feeds a
// bounded queue, one writer goroutine is the only mutator of the graph
// (Badger doWrites-style draining: everything queued becomes one g.Batch —
// one micro-layer, one WAL frame group, one fsync), and a background
// merger folds layer stacks so the writer never waits on a merge until
// the layer cap demands it (write stall). Queries stay lock-free reads of
// published generations the whole time.
//
// Attach claims the graph (writer discipline, R7): direct-mode writes from
// other goroutines then return ErrNotWriter in non-release builds. The
// Handle is the only supported write path while attached; Close drains,
// runs the final merge + checkpoint (g.Seal's pipeline), and yields the
// claim.
package writer

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/projanvil/vectra"
)

// Config tunes the wrapper. The zero value takes the spec defaults.
type Config struct {
	// ChanCap bounds the Submit queue; a full queue blocks submitters
	// (backpressure). Default 4096.
	ChanCap int

	// MaxLayers is the layer-count hard cap and the write-stall line: once
	// a batch leaves the stack at or above the cap, the writer forces a
	// fold and WAITS for it (the deep-stack backstop — read amplification
	// over the live stack is bounded here; the per-generation View cache
	// keeps construction cost off the query path while stacks run deep).
	// Background folds otherwise fire on the volume rule (MergeEdges), so
	// the cap should sit well above the layer count a base-proportional
	// interval would naturally produce: a small cap re-introduces the A6
	// miss (a fold every cap × changeset-size edges, each costing O(base)).
	// Default 2048.
	MaxLayers int

	// MergeEdges is the FLOOR of the volume-proportional background-merge
	// trigger: a fold fires when the unmerged layer stack carries at least
	// max(MergeEdges, base.EdgeCount) edge records (v0.7 hardening: the fold
	// interval stays proportional to the base — every fold costs
	// O(base + layers), so intervals carrying base-comparable volume
	// amortize to a bounded share, while any fixed interval eventually
	// re-introduces the A6 miss as the base grows). The floor keeps small
	// graphs folding. It never stalls the writer on its own; the layer cap
	// is the stall path. Default 64K.
	MergeEdges int

	// CheckpointBytes is spec §4's WAL volume trigger: once the log holds
	// this many valid frame bytes since the last checkpoint, the writer's
	// post-batch hook runs the full Seal pipeline (merge + watermark
	// snapshot + manifest + WAL reset) on the writer goroutine. Queries
	// stay lock-free throughout; Submits observe a checkpoint-shaped
	// pause. Default 64 MiB (measured cadence, see withDefaults); negative
	// disables the volume trigger (explicit Seal/Compact/Close only). A
	// failed volume checkpoint is sticky: the
	// error surfaces from Close and the trigger disables itself — an
	// ailing disk must not turn every batch into a retry storm.
	CheckpointBytes int64

	// MergeDelay is a test injection: the merger sleeps this long between
	// capturing the generation prefix and sealing it, widening the window
	// in which the writer publishes tail layers (exercises the rebase).
	MergeDelay time.Duration

	// FailMerge is a test injection: the first N background seal attempts
	// fail (backoff/retry and the synchronous fallback then engage). The
	// writer's synchronous fallback itself is never injected — it is the
	// last-resort path that must succeed for the stall to be releasable.
	FailMerge int

	// GCTune lowers GOGC to 50 for the duration of merges and restores it
	// after (spec ruling: off by default — merges allocate heavily and a
	// lower target trims peak RSS on merge-heavy workloads).
	GCTune bool
}

func (c Config) withDefaults() Config {
	if c.ChanCap <= 0 {
		c.ChanCap = 4096
	}
	if c.MaxLayers <= 0 {
		c.MaxLayers = 2048
	}
	if c.MergeEdges <= 0 {
		c.MergeEdges = 65536
	}
	if c.CheckpointBytes == 0 {
		// 64 MiB, measured (docs/benchmarks/2026-10-21-wrapper-checkpoint-
		// cadence.md): binary frames put a 768-dim vector at ~3.07KB, so the
		// old 4 MiB default sealed every ~1360 vectors — 76% of ingestion
		// wall (10.2k/s vs 44.8k/s at 64 MiB); plain-write deployments
		// checkpoint every few hundred thousand ops; layer folding is
		// MergeEdges-driven and unaffected.
		c.CheckpointBytes = 64 << 20
	}
	return c
}

// opKind tags a queued request. Vector index declarations and drops ride
// the same lane as changesets (ruling #8): Attach claims the graph, so the
// app goroutine has no legal direct declaration entry, and a shared queue
// makes the order against Submits total.
type opKind uint8

const (
	opChangeset opKind = iota // the zero value: Submit's construction stays unchanged
	opDeclare
	opDrop
)

// request is one queued operation. err carries the per-request outcome
// resolved inside the batch; replyCh (buffered, exactly one send) returns
// it to the submitter.
type request struct {
	op      opKind
	cs      vectra.Changeset       // opChangeset
	decl    vectra.VectorIndexDecl // opDeclare
	name    string                 // opDrop
	replyCh chan error
	err     error
}

// Handle is the single-writer wrapper created by Attach.
type Handle struct {
	g   *vectra.Graph
	cfg Config

	writeCh  chan request
	loopDone chan struct{} // closed after the final Seal and writer-claim yield

	// pubMu serializes generation publication between the writer's
	// g.Batch/Seal calls (held around them) and the merger's PublishMerge
	// (spec §2.3 two-writer protocol). Owned here; root stays lock-free.
	pubMu sync.Mutex

	// sendMu makes close(writeCh) safe: senders hold RLock, Close takes
	// the write lock before flipping the flag and closing the channel.
	sendMu     sync.RWMutex
	closed     bool
	closeOnce  sync.Once
	mergerOnce sync.Once

	quit       chan struct{} // closed by Close after loopDone: merger exits
	mergerDone chan struct{}
	mergeSig   chan struct{} // buffered 1: wake the merger (single-flight)
	// vecSig wakes the merger's VECTOR lane (spec D10): an independent
	// signal, deliberately sharing the merger goroutine but never the
	// graph-fold flight - the write-stall wait path (signalMerge) must
	// not observe vector work (vector convergence is lazy by design).
	vecSig chan struct{} // buffered 1

	// flightMu guards the flight channel: non-nil while a background merge
	// is wanted or in flight; closed exactly once when it resolves
	// (published, skipped, or handed off to the writer's sync fallback).
	flightMu sync.Mutex
	flight   chan struct{}

	needSync atomic.Bool  // merger gave up: the writer must fold synchronously
	fails    atomic.Int32 // consecutive background merge failures (reset on success)
	failN    atomic.Int32 // FailMerge injection counter (background attempts)

	cpOff atomic.Bool           // volume checkpoint disabled (failed once)
	cpErr atomic.Pointer[error] // that failure, surfaced by Close

	finalErr error // Close's final g.Seal outcome (published via loopDone)
}

// Attach wraps g with the single-writer Handle and starts the writer and
// merger goroutines. From here until Close, Submit is the only legal write
// path (R7; direct writes from other goroutines fail with ErrNotWriter in
// non-release builds). Attaching twice re-claims for the newer handle —
// don't.
//
// The caller's open direct-mode absorption window (unflushed unbatched
// writes) is materialized first when the caller is the window's writer —
// otherwise those writes would stay invisible until the writer's first
// batch boundary, and wrapper-mode reads never flush.
func Attach(g *vectra.Graph, cfg Config) *Handle {
	cfg = cfg.withDefaults()
	g.FlushPend()
	h := &Handle{
		g:          g,
		cfg:        cfg,
		writeCh:    make(chan request, cfg.ChanCap),
		loopDone:   make(chan struct{}),
		quit:       make(chan struct{}),
		mergerDone: make(chan struct{}),
		mergeSig:   make(chan struct{}, 1),
		vecSig:     make(chan struct{}, 1),
	}
	go h.run()
	go h.merger()
	return h
}

// Submit queues one changeset and blocks until the writer applied it (or
// failed it). A flush failure of the batch frame dominates: the error is
// returned although the entries are already in memory (direct-mode
// limitation, T2 discipline — retry or abandon).
func (h *Handle) Submit(cs vectra.Changeset) error {
	reply := make(chan error, 1)
	if err := h.send(request{cs: cs, replyCh: reply}); err != nil {
		return err
	}
	return <-reply
}

// SubmitAsync queues one changeset and returns the reply channel (buffered
// size 1: dropping it is fine, the send never blocks).
func (h *Handle) SubmitAsync(cs vectra.Changeset) <-chan error {
	reply := make(chan error, 1)
	if err := h.send(request{cs: cs, replyCh: reply}); err != nil {
		reply <- err
	}
	return reply
}

// DeclareVectorIndex declares a vector index through the writer lane
// (ruling #8): while attached, the Handle is the only legal declaration
// surface (direct CreateVectorIndex from another goroutine hits
// ErrNotWriter in debug builds). The request rides writeCh with changesets,
// so the order against Submits is total — a Submit that returns after this
// does so with the index installed and its vectors ingested.
func (h *Handle) DeclareVectorIndex(d vectra.VectorIndexDecl) error {
	reply := make(chan error, 1)
	if err := h.send(request{op: opDeclare, decl: d, replyCh: reply}); err != nil {
		return err
	}
	return <-reply
}

// DropVectorIndex removes a vector index through the writer lane, with the
// same total order as Submit: a Submit queued after the drop applies with
// the index already gone (later queries report ErrVectorIndexNotFound).
func (h *Handle) DropVectorIndex(name string) error {
	reply := make(chan error, 1)
	if err := h.send(request{op: opDrop, name: name, replyCh: reply}); err != nil {
		return err
	}
	return <-reply
}

// send enqueues under the sender lock; after Close it reports ErrClosed.
func (h *Handle) send(r request) error {
	h.sendMu.RLock()
	defer h.sendMu.RUnlock()
	if h.closed {
		return vectra.ErrClosed
	}
	h.writeCh <- r // may block when the queue is full: backpressure
	return nil
}

// Close ends the wrapper session: no further Submits are accepted
// (ErrClosed), queued ones are drained and applied, the final merge +
// checkpoint run through g.Seal on the writer goroutine, and the writer
// claim is yielded (direct mode is legal again). The underlying graph is
// NOT closed — queries stay legal and Graph.Close remains the caller's
// when the WAL handle should be torn down (Seal already checkpointed, so a
// following Graph.Close re-checkpoints cheaply). A volume-checkpoint
// failure recorded mid-session is joined into the returned error.
func (h *Handle) Close() error {
	h.closeOnce.Do(func() {
		h.sendMu.Lock()
		h.closed = true
		close(h.writeCh)
		h.sendMu.Unlock()
	})
	<-h.loopDone // drained + final Seal done + claim yielded
	h.mergerOnce.Do(func() { close(h.quit) })
	<-h.mergerDone
	if cp := h.cpErr.Load(); cp != nil {
		return errors.Join(*cp, h.finalErr)
	}
	return h.finalErr
}

// run is the writer goroutine: the graph's single mutator.
func (h *Handle) run() {
	h.g.ClaimWriter()
	for req := range h.writeCh {
		pending := []request{req}
	drain:
		for len(pending) < cap(h.writeCh) {
			select {
			case r, ok := <-h.writeCh:
				if !ok { // closed mid-drain (Close won the race)
					break drain
				}
				pending = append(pending, r)
			default:
				break drain
			}
		}
		h.applyBatch(pending)
	}
	// writeCh closed and drained: final merge + checkpoint via g.Seal
	// (spec ruling: the checkpoint pipeline is Seal's), still on the
	// writer goroutine so the merge may rebuild writer-private state.
	h.pubMu.Lock()
	h.finalErr = h.g.Seal()
	h.pubMu.Unlock()
	h.g.YieldWriter()
	close(h.loopDone)
}

// applyBatch commits one drained group as a single g.Batch — one
// micro-layer and one WAL frame group for the whole group (group commit) —
// then replies, then runs the merge triggers/stall.
//
// Before the batch applies, the C1 repair runs: if the merger published a
// rebase since the last batch, the graph's writer-private mirrors hold
// pre-rebase IDs that the rebase's dense renumbering may have assigned to
// other keys; stamping one into this batch's layer would put two live
// records on a single ID. RepairAfterRebase folds the generation once and
// rebuilds the mirrors (a no-op without a rebase).
//
// Declaration ops in the group apply on this goroutine AFTER the repair and
// BEFORE the changeset batch, through the graph's own decl APIs — outside
// the g.Batch closure, which they refuse (mid-batch backfill would scan a
// pre-publish view and miss the batch's own nodes). Landing every IndexDecl
// frame ahead of the group's changeset frames is replay-faithful either way
// the lane interleaved them: replay re-declares at the frame position, then
// the changesets ingest at apply time — the same path live intake takes —
// and the group's replies all go out after the whole group applied, so no
// submitter can observe the collapsed order.
func (h *Handle) applyBatch(pending []request) {
	h.pubMu.Lock()
	batchErr := h.g.RepairAfterRebase()
	if batchErr == nil {
		for i := range pending {
			switch pending[i].op {
			case opDeclare:
				pending[i].err = h.g.CreateVectorIndex(pending[i].decl)
			case opDrop:
				pending[i].err = h.g.DropVectorIndex(pending[i].name)
			}
		}
		batchErr = h.g.Batch(func(g *vectra.Graph) {
			for i := range pending {
				if pending[i].op == opChangeset {
					pending[i].err = applyChangeset(g, pending[i].cs)
				}
			}
		})
	}
	h.pubMu.Unlock()

	for i := range pending {
		reply := batchErr // a flush failure dominates every per-changeset outcome
		if reply == nil {
			reply = pending[i].err
		}
		if pending[i].replyCh != nil {
			pending[i].replyCh <- reply
		}
	}
	h.afterBatch()
}

// afterBatch runs the spec §3.1 triggers: the volume-proportional fold rule
// (unmerged layer edges ≥ max(MergeEdges, base edges) — the interval stays
// proportional to the base, A6's structural contract) signals a background
// merge; at the layer cap the writer additionally WAITS for the fold (write
// stall, §2.3 — the deep-stack backstop, and the only place the writer
// blocks on the merger). A merger handoff (3 consecutive failures) is
// consumed BEFORE the stall check: folding synchronously first usually
// drops the stack below the cap, and stalling on a merger that just gave up
// would only pay its backoff ladder for nothing. The §4 WAL volume
// checkpoint runs before the layer triggers — a Seal folds every layer, so
// it subsumes them.
func (h *Handle) afterBatch() {
	if h.needSync.Load() {
		h.syncMerge()
	}
	if h.volumeCheckpoint() {
		return // sealed + checkpointed: layer pressure is already reset
	}
	if h.g.VectorOverlayPressure() {
		select {
		case h.vecSig <- struct{}{}:
		default:
		}
	}
	layers, layerEdges := h.g.LayerPressure()
	switch {
	case layers >= h.cfg.MaxLayers:
		<-h.signalMerge() // write stall: the deep-stack backstop forces a fold
	case layerEdges >= h.mergeEdgeThreshold():
		h.signalMerge()
	}
}

// mergeEdgeThreshold is the volume-proportional fold trigger: the unmerged
// stack must carry edges comparable to the merged base before a background
// fold is worth its O(base) cost. MergeEdges is the floor, so small graphs
// still fold at the configured quantum.
func (h *Handle) mergeEdgeThreshold() int {
	t := h.cfg.MergeEdges
	if base := h.g.BaseEdgeCount(); base > t {
		t = base
	}
	return t
}

// volumeCheckpoint implements spec §4's "WAL ≥4MB since last checkpoint"
// trigger: at or above CheckpointBytes of valid frame bytes the writer runs
// the full Seal pipeline (flush + merge + watermark snapshot + manifest +
// WAL reset) on its own goroutine — a checkpoint-shaped write pause that is
// invisible to lock-free readers. It reports whether a checkpoint ran (so
// the caller can skip the layer triggers Seal already satisfied).
func (h *Handle) volumeCheckpoint() bool {
	if h.cfg.CheckpointBytes <= 0 || h.cpOff.Load() {
		return false
	}
	if h.g.WALSize() < h.cfg.CheckpointBytes {
		return false
	}
	h.pubMu.Lock()
	err := h.g.Seal()
	h.pubMu.Unlock()
	if err != nil {
		cpErr := fmt.Errorf("vectra/writer: volume checkpoint: %w", err)
		h.cpErr.Store(&cpErr)
		h.cpOff.Store(true) // sticky: surfaced by Close; explicit Compact still available
	}
	return true
}

// applyChangeset applies one submitted changeset with the write-method
// semantics, in the Changeset apply order (nodes, edges, edge deletes,
// node deletes — the same order WAL replay uses). The first error stops
// that changeset; its earlier entries stay applied (matching Open's replay
// discipline: a WAL error means retry-or-abandon, and the recorded ops
// replay exactly the entries that succeeded).
func applyChangeset(g *vectra.Graph, cs vectra.Changeset) error {
	for _, n := range cs.Nodes {
		if _, err := g.AddNode(n.Key, n.Label, n.Props); err != nil {
			return err
		}
	}
	for _, e := range cs.Edges {
		var err error
		if e.AllowParallel {
			_, err = g.AddEdge(e.SrcKey, e.DstKey, e.Label, e.Props)
		} else {
			_, err = g.UpsertEdge(e.SrcKey, e.DstKey, e.Label, e.Props)
		}
		if err != nil {
			return err
		}
	}
	for _, d := range cs.EdgeDeletes {
		if _, err := g.DeleteEdge(d.SrcKey, d.DstKey, d.Label); err != nil {
			return err
		}
	}
	for _, key := range cs.NodeDeletes {
		if _, err := g.DeleteNode(key); err != nil {
			return err
		}
	}
	return nil
}
