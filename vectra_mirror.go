// Writer-side mirror and absorption-window machinery: builder lifecycle,
// endpoint lift/materialization, layer publish/cut, and mirror reconciliation.

package vectra

import (
	"github.com/projanvil/vectra/internal/delta"
)

// ---- writer-side batch plumbing ---------------------------------------------

// pendWindow is the metadata of one open absorption window. owner is the
// goroutine token that opened it (written once before the atomic Store,
// read-only afterwards); ops/edges count write-method completions for the
// capacity caps. All three are writer-private state — readers only touch
// owner.
type pendWindow struct {
	owner uint64
	ops   int
	edges int
}

// Capacity caps of the absorption window: an unqueried write burst
// materializes at most this much per layer, bounding the window's memory
// (~5MB for a pure edge burst). Edge-appending ops count toward the edge
// cap; every other op (nodes, deletes, folds that append nothing) counts
// toward the op cap — a pure AddEdge burst is bounded by the edge cap
// alone, which amortizes the layer build far below the per-op cost while
// keeping mid-burst queries cheap.
const (
	pendMaxOps   = 4096
	pendMaxEdges = 1 << 16 // 64K edge records
	// pendLiftBudget caps endpoint materialization per window (see
	// materializeEndpoint): small/read-adjacent windows lift fully, bulk
	// import windows stop past the budget.
	pendLiftBudget = 512
)

// ensureBuilder opens the batch state if none is active. Outside Batch the
// builder doubles as the absorption window: an atomically-published
// pendWindow marks it open, and finishOp/flushPend decide when it
// materializes.
func (g *Graph) ensureBuilder() {
	if g.builder != nil {
		return
	}
	g.builder = delta.NewBuilder()
	// the endpoint registry survives window cuts (cutLayer restores it) —
	// only a full close leaves it nil here
	if g.bNodeIDs == nil {
		g.bNodeIDs = map[string]uint32{}
		g.bLiveNodes = map[string]bool{}
	}
	if g.bKilled == nil {
		g.bKilled = map[delta.EdgeKey]int{}
	}
	g.bEdgeIDs = nil
	g.bIndex = nil
	g.bFTS = nil
	if !g.batching {
		g.pend.Store(&pendWindow{owner: goroutineToken()})
	}
}

// finishOp closes a single write: inside Batch the boundary owns
// publication; outside it the op is absorbed into the open window, flushing
// only when a capacity cap fires (read entries, Batch/Seal/Close and the
// caps are the window's flush triggers). edges is the number of edge
// records the op appended into the window (0 for non-edge ops): an
// edge-appending op counts only toward the edge cap, everything else only
// toward the op cap.
func (g *Graph) finishOp(edges int) {
	if g.batching {
		return
	}
	if p := g.pend.Load(); p != nil {
		if edges > 0 {
			p.edges += edges
		} else {
			p.ops++
		}
		if p.ops >= pendMaxOps || p.edges >= pendMaxEdges {
			g.cutLayer(true) // write-path cap: folding (and sealing) allowed
		}
	}
}

// flushPend materializes the caller's absorption window, if one is open and
// the caller is its writer (direct mode: the reader IS the writer, so
// write-then-read stays immediate). Concurrent readers that are not the
// window's owner — and every reader of a wrapper-attached graph, whose
// writer only ever publishes at Batch boundaries — skip the flush and keep
// reading the last published generation. The owner-token read and the
// g.batching read are both reached only from the writer's goroutine, so no
// plain field is ever read across goroutines.
func (g *Graph) flushPend() {
	p := g.pend.Load()
	if p == nil {
		return
	}
	if g.writerAttached.Load() || p.owner != goroutineToken() {
		return
	}
	if g.batching { // mid-Batch reads see the pre-batch generation (atomicity)
		return
	}
	g.cutLayer(false) // read-triggered flush: publish only, never fold
}

// FlushPend materializes the caller's direct-mode absorption window (a
// no-op when none is open, when another goroutine owns it, or in wrapper
// mode). Useful before handing a graph to another goroutine or reading it
// from one: unflushed absorbed writes are invisible to readers until the
// writer flushes (its next query, boundary, or cap).
//
// Wrapper-mode coupling surface: internal/writer also calls this at Attach
// (to materialize the caller's window before claiming the graph); it
// doubles as a documented direct-mode utility and is not covered by API
// stability.
func (g *Graph) FlushPend() { g.flushPend() }

// nodeLive answers "is key a visible node right now" for endpoint
// validation, consulting both the batch and the published mirrors.
func (g *Graph) nodeLive(key string) bool {
	if g.bLiveNodes[key] {
		return true
	}
	if _, touched := g.bNodeIDs[key]; touched {
		return false // deleted within the current batch
	}
	ref, ok := g.nodes[key]
	return ok && ref.live
}

func (g *Graph) checkLiveNodes(srcKey, dstKey string) error {
	if !g.nodeLive(srcKey) || !g.nodeLive(dstKey) {
		return ErrNodeNotFound
	}
	return nil
}

// materializeEndpoint lifts a published (base- or older-layer-resident)
// node into the active builder as an override record carrying its current
// id and state. Read locality: the layer that just absorbed a burst carries
// its endpoints' records, so a post-write NodeByKey hits the newest layer
// instead of miss-scanning the whole stack down to the base. The lift is
// budgeted per window (pendLiftBudget): read-adjacent windows — every write
// followed by a query, the write-then-read contract's hot shape — lift
// fully, while bulk-import windows past the budget stop paying the
// per-endpoint materialization (their reads fall through to the base or an
// older layer; a merge or the next small window restores top-layer hits).
func (g *Graph) materializeEndpoint(key string) {
	if g.builder.HasNode(key) {
		return
	}
	if p := g.pend.Load(); p != nil && p.ops+p.edges >= pendLiftBudget {
		return
	}
	ref, ok := g.nodes[key]
	if !ok || !ref.live {
		return // defensive: validated by the caller
	}
	g.bNodeIDs[key] = uint32(ref.id)
	g.bLiveNodes[key] = true
	_ = g.builder.UpsertNode(key, ref.label, ref.props) // O(1) from the mirror
}

// newEdgeSlotID draws a fresh ratchet EdgeID and registers it for the next
// builder edge slot.
func (g *Graph) newEdgeSlotID() EdgeID {
	id := g.nextEdge
	g.nextEdge++
	g.bEdgeIDs = append(g.bEdgeIDs, uint32(id))
	return id
}

// olderIncidentEdges feeds Builder.DeleteNode's baseEdges hook: the keys of
// every VISIBLE edge (base via both CSR row scans, older layers via record
// scan) incident to key, one entry per dying record (multiplicity keeps the
// builder's cascade count equal to vectra.DeleteNode's).
func (g *Graph) olderIncidentEdges(key string) []delta.EdgeKey {
	v := g.view.Load()
	seen := map[delta.EdgeKey]bool{}
	var out []delta.EdgeKey
	add := func(k delta.EdgeKey) {
		if seen[k] {
			return
		}
		// batch-pending tombstones already own killed parallels; only the
		// not-yet-tombstoned published ones feed the cascade count
		n := g.edges[k].n - g.bKilled[k]
		if n <= 0 {
			return
		}
		seen[k] = true
		for range n {
			out = append(out, k)
		}
	}
	if bid, ok := v.baseKeys[key]; ok { // spec §3.1 R2: base entities via CSR row scans
		_, feids, _ := v.base.AdjacencyF().Row(uint32(bid))
		_, reids, _ := v.base.AdjacencyR().Row(uint32(bid))
		for _, eid := range feids {
			add(v.baseEdgeKeyOf(eid))
		}
		for _, eid := range reids {
			add(v.baseEdgeKeyOf(eid))
		}
	}
	for _, l := range v.layers {
		for _, rec := range l.Edges() {
			if rec.SrcKey != key && rec.DstKey != key {
				continue
			}
			add(delta.EdgeKey{SrcKey: rec.SrcKey, DstKey: rec.DstKey, Label: rec.Label})
		}
	}
	return out
}

// publishLayer materializes the active builder into a micro-layer, appends
// it to the generation stack, publishes the new genView, and reconciles the
// writer mirrors. An entirely empty layer (no records, tombstones or pending
// index declarations of either kind) is dropped — it must not flip Sealed()
// or grow the stack. The layers slice is extended by plain append: safe
// under the single writer because older generations only ever read their
// own shorter prefix (append either writes past the old len or reallocates).
// Full close: the builder, the absorption window and the endpoint registry
// all reset — the registry reset must happen even when the builder is
// already closed (a window cut by an earlier read), or IDs from before a
// renumbering merge would survive it (cutLayer restores the registry after
// this reset).
func (g *Graph) publishLayer() {
	g.pend.Store(nil)
	b := g.builder
	var l *delta.Layer
	if b != nil {
		l = b.BuildWithIDs(g.bNodeIDs, g.bEdgeIDs)
	}
	// full-close reset — unconditionally: a merge may have renumbered the
	// base while the registry still holds pre-merge IDs (the window was cut
	// by an earlier read, leaving no builder), and cutLayer restores the
	// registry afterwards for continued bursts
	g.builder = nil
	g.bNodeIDs, g.bEdgeIDs = nil, nil
	g.bLiveNodes, g.bKilled, g.bIndex, g.bFTS = nil, nil, nil, nil
	if l != nil && l.NodeCount() == 0 && l.EdgeCount() == 0 &&
		len(l.NodeTombstones()) == 0 && len(l.EdgeTombstones()) == 0 && len(l.IndexKeys()) == 0 && len(l.FTSKeys()) == 0 {
		l = nil // an entirely empty layer is dropped: it must not flip Sealed() or grow the stack
	}
	if l == nil && g.pendingFold.Load() == nil {
		return
	}
	if l != nil {
		g.reconcileMirrors(l)
	}

	v := g.view.Load()
	// Incorporate a finished background fold: the result stands on a prefix
	// of the current stack (the writer only appends), so the folded runs
	// replace that prefix and the layers stacked since ride on top. A
	// stale result (Seal/rebase replaced the stack) is dropped.
	stack, splitF, splitT := v.layers, v.splitF, v.splitT
	var from *viewBuild
	if r := g.pendingFold.Swap(nil); r != nil && !g.merging && len(r.src) <= len(stack) &&
		layerPrefixMatch(stack, r.src, len(r.src)) {
		spliced := make([]*delta.Layer, 0, len(r.folded)+len(stack)-len(r.src)+1)
		spliced = append(spliced, r.folded...)
		spliced = append(spliced, stack[len(r.src):]...)
		stack, splitF, splitT, from = spliced, r.splitF, r.splitT, r.ext
	}
	layers := stack[:len(stack):len(stack)]
	if l != nil {
		layers = append(layers, l)
	}
	lsn := v.lsn
	if g.wal != nil {
		lsn = g.wal.LastLSN()
	}
	nv := &genView{
		base: v.base, baseKeys: v.baseKeys, layers: layers,
		lsn: lsn, nodes: g.nodeTotal, edges: g.edgeTotal,
		splitF: splitF, splitT: splitT,
	}
	if !g.merging {
		g.prebuildView(nv, from) // fill the slot first; forceMerge's publish is superseded at once
	}
	g.view.Store(nv)
	g.vecPublish() // swap fresh vector snapshots with every generation
	// Vector lane (D10's direct-mode half): arm the folder wake on overlay
	// pressure. Every publish path lands here (window cuts, Batch ends,
	// read-triggered owner flushes) and all of them run on the writer
	// thread; the wrapper mode arms its own vecSig in afterBatch.
	if !g.writerAttached.Load() && g.vecOverlayPressure() {
		g.vecKick()
	}
}

// reconcileMirrors applies one published layer to the writer-private
// mirrors: tombstones first, live records on top (the layer's own live
// records win over its own tombstones).
func (g *Graph) reconcileMirrors(l *delta.Layer) {
	for k := range l.NodeTombstones() {
		if ref, ok := g.nodes[k]; ok && ref.live {
			ref.live = false
			g.nodes[k] = ref
			g.nodeTotal--
		}
	}
	for i := range l.Nodes() {
		rec := l.Nodes()[i]
		ref := g.nodes[rec.Key]
		if !ref.live {
			g.nodeTotal++
		}
		g.nodes[rec.Key] = nodeRef{id: NodeID(l.NodeIDAt(i)), live: true, label: rec.Label, props: rec.Props}
	}
	for k := range l.EdgeTombstones() {
		if ref, ok := g.edges[k]; ok {
			g.edgeTotal -= ref.n
			delete(g.edges, k)
		}
	}
	for i := range l.Edges() {
		rec := l.Edges()[i]
		k := delta.EdgeKey{SrcKey: rec.SrcKey, DstKey: rec.DstKey, Label: rec.Label}
		ref := g.edges[k]
		ref.winner = EdgeID(l.EdgeIDAt(i))
		ref.n++
		g.edges[k] = ref
		g.edgeTotal++
	}

}

// cutLayer is the mid-burst window flush: publishLayer, but the endpoint
// registry (bNodeIDs/bLiveNodes) survives the cut. Without the carry, every
// new window re-lifts each published endpoint its edges touch — one map
// insert per first touch plus registry regrowth, which dominated the
// per-op cost of write bursts. The registry stays coherent across cuts
// because every write mutates it exactly as it mutates the published
// mirrors. The next write's ensureBuilder reopens a fresh window counter.
//
// fold toggles the governor: write-path cuts (the capacity cap, Batch
// boundaries) fold the layer stack; read-triggered flushes (flushPend's
// write-then-read contract) publish only — folds cost O(T+tail) and landing
// them on queries blows write-then-read p99 (the A3 gate).
func (g *Graph) cutLayer(fold bool) {
	if g.builder == nil {
		if g.pendingFold.Load() != nil {
			g.publishLayer() // no pending writes: incorporate a finished fold (pure view swap)
		}
		return
	}
	keepIDs, keepLive := g.bNodeIDs, g.bLiveNodes
	g.publishLayer()
	g.bNodeIDs, g.bLiveNodes = keepIDs, keepLive
	if fold {
		g.afterPublish(true)
	}
}
