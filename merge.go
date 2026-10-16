package vectra

// Background merge driver surface (v0.7 spec §2.3, wrapper mode). These
// exported entry points exist for internal/writer — the single-writer
// wrapper that runs the merger goroutine. Direct-mode callers never touch
// them; root itself never calls them either.
//
// The two-writer publish protocol in three moves:
//
//  1. CaptureMerge pins the current generation prefix {base, layers[0..n]}
//     (atomic load; the prefix is immutable forever, so the pin is safe
//     while the writer keeps appending newer layers).
//  2. MergeCapture.Seal folds the PREFIX alone into a fresh immutable base
//     (engine.Seal over the pinned genView — pure, no locks, no
//     writer-private state: it can run on the merger goroutine while the
//     writer is mid-batch).
//  3. PublishMerge re-bases the live generation: under the CALLER's
//     publication mutex (owned by internal/writer; it also wraps the
//     writer's g.Batch calls) the current view's tail layers[n+1..] are
//     stacked on the new base and stored atomically. Layer addressing is
//     by logical key (tombstones and overrides never reference base
//     positions), so the tail keeps its exact semantics on the new base —
//     that rebase validity is the foundation of the whole protocol
//     (verified by the delta composition contract, Task 4).
//
// ID discipline under rebase (R1 corollary, C1 repair): the union of
// base+prefix has node/edge counts ≤ the ratchet values the writer used when
// allocating IDs in the tail layers, so tail-layer IDs drawn FRESH from the
// ratchet never collide with the new base's dense [0, count) range from
// FUTURE allocations. The writer-private MIRRORS, however, still hold
// pre-rebase IDs — and when the folded prefix contained tombstones, the
// dense renumbering assigns those IDs to other keys. Reusing a mirror ID
// into a fresh layer (AddNode override/revive, endpoint materialization,
// UpsertEdge folds) would put two live records of one generation on one ID
// with no later merge to clear it. PublishMerge therefore sets
// rebaseHappened, and the writer (internal/writer's loop, before every
// batch) rebuilds its mirrors via RepairAfterRebase — a full fold that
// redoes the rebase's work once (rare under default thresholds).
//
// Residual, documented (R1 transient): between a rebase's publication and
// the writer's next batch, tail-layer records that were stamped
// legitimately BEFORE the rebase (mirror-transferred IDs of overrides,
// revivals, folds) may briefly overlap the renumbered base's dense slots.
// NodeByID/EdgeByID resolve such overlaps deterministically (layer records
// win); the next batch's repair folds them away before any new write.
// Eliminating the overlap entirely would mean renumbering immutable tail
// layers — a v0.7.x candidate, out of scope here.

import (
	"github.com/projanvil/vectra/internal/delta"
	"github.com/projanvil/vectra/internal/engine"
)

// MergeCapture is an immutable pin of one generation prefix: the base plus
// the first n micro-layers as of capture time. Safe to hold across writer
// activity; Seal may be retried against it.
type MergeCapture struct {
	base   *engine.Snapshot
	layers []*delta.Layer // the captured prefix, oldest first
}

// MergeResult is the sealed output of one captured prefix. Single-use: the
// live-key sets are consumed (mutated) by PublishMerge's count fold.
type MergeResult struct {
	snap      *engine.Snapshot      // the sealed new base
	baseKeys  map[string]NodeID     // union node key -> new dense id
	liveNodes map[string]bool       // union live node keys
	liveEdges map[delta.EdgeKey]int // union live edge key -> parallel count
}

// CaptureMerge pins the current generation prefix for a background merge.
// Reading the layers prefix is race-free: layers are immutable once
// published, and the writer only ever appends past the captured length.
func (g *Graph) CaptureMerge() *MergeCapture {
	v := g.view.Load()
	return &MergeCapture{base: v.base, layers: v.layers}
}

// LayerPressure reports the unmerged layer count and the cumulative edge
// records across those layers — the two background-merge triggers of spec
// §3.1 (layer-count and delta-volume thresholds). Safe from any goroutine.
func (g *Graph) LayerPressure() (layers int, layerEdges int) {
	v := g.view.Load()
	for _, l := range v.layers {
		layerEdges += l.EdgeCount()
	}
	return len(v.layers), layerEdges
}

// BaseEdgeCount reports the merged base's edge count — the size input of
// the volume-proportional merge threshold (internal/writer folds when the
// unmerged stack carries max(MergeEdges, BaseEdgeCount) edges, keeping the
// fold interval proportional to the base). Safe from any goroutine.
func (g *Graph) BaseEdgeCount() int {
	return g.view.Load().base.EdgeCount()
}

// Seal folds the captured prefix into a fresh immutable base. Pure: no
// Graph state, no locks, no writer-private mirrors — the caller may run it
// on any goroutine while the writer continues. The result carries the new
// base's key index and the union's live-key sets (input to PublishMerge's
// count fold).
func (c *MergeCapture) Seal() (*MergeResult, error) {
	pin := &genView{base: c.base, layers: c.layers}
	nodes, edges, labels, indexKeys := pin.unionRecords()
	snap, err := engine.Seal(nodes, edges, labels, indexKeys)
	if err != nil {
		return nil, err
	}
	r := &MergeResult{
		snap:      snap,
		baseKeys:  make(map[string]NodeID, len(nodes)),
		liveNodes: make(map[string]bool, len(nodes)),
		liveEdges: make(map[delta.EdgeKey]int, len(edges)),
	}
	for i, n := range nodes {
		r.baseKeys[n.Key] = NodeID(i)
		r.liveNodes[n.Key] = true
	}
	for _, er := range edges {
		k := delta.EdgeKey{SrcKey: nodes[er.Src].Key, DstKey: nodes[er.Dst].Key, Label: labels[er.LabelID]}
		r.liveEdges[k]++
	}
	return r, nil
}

// PublishMerge re-bases the live generation onto r's sealed base, dropping
// the captured prefix: the current view's layers[n+1..] tail is stacked on
// the new base and the generation is stored atomically. Readers are never
// blocked (they Load either the old or the new generation).
//
// Serialization contract: the caller must hold the publication mutex that
// also serializes the writer's g.Batch/Seal/Compact calls (internal/
// writer's pubMu). Root never calls this on its own.
//
// Staleness: if a full merge superseded the capture (the current base is
// no longer the captured base, or the stack shrank), the publish is a
// no-op returning false — the rebase is simply dropped.
func (g *Graph) PublishMerge(c *MergeCapture, r *MergeResult) bool {
	cur := g.view.Load()
	if cur.base != c.base || len(cur.layers) < len(c.layers) {
		return false // superseded by a full merge (Seal/Compact/fallback)
	}
	tail := cur.layers[len(c.layers):]

	// Fold the tail's contribution into the visible counts, replaying the
	// layers oldest→newest with publishLayer's reconciliation semantics
	// (a layer's live records win over its own tombstones).
	nodes, edges := len(r.liveNodes), 0
	for _, n := range r.liveEdges {
		edges += n
	}
	for _, l := range tail {
		for k := range l.NodeTombstones() {
			if r.liveNodes[k] {
				delete(r.liveNodes, k)
				nodes--
			}
		}
		for _, rec := range l.Nodes() {
			if !r.liveNodes[rec.Key] {
				r.liveNodes[rec.Key] = true
				nodes++
			}
		}
		for k := range l.EdgeTombstones() {
			if n := r.liveEdges[k]; n > 0 {
				delete(r.liveEdges, k)
				edges -= n
			}
		}
		for _, rec := range l.Edges() {
			k := delta.EdgeKey{SrcKey: rec.SrcKey, DstKey: rec.DstKey, Label: rec.Label}
			r.liveEdges[k]++
			edges++
		}
	}
	g.view.Store(&genView{
		base: r.snap, baseKeys: r.baseKeys, layers: tail,
		lsn: cur.lsn, nodes: nodes, edges: edges,
	})
	// C1: the writer-private mirrors now hold pre-rebase IDs while the new
	// base has renumbered — flag it so the writer rebuilds before its next
	// batch stamps any of them into a fresh layer.
	g.rebaseHappened.Store(true)
	return true
}

// RepairAfterRebase rebuilds the writer-private mirrors when a background
// merge rebase has been published since the last full merge on this
// goroutine: after a rebase the mirrors carry pre-rebase node/edge IDs, and
// the rebase's dense renumbering may have assigned those IDs to other keys —
// reusing one into a fresh layer would collide two live records on a single
// ID. The rebuild is a forceMerge (full fold, ratchets reset, dense IDs);
// it redoes the rebase's sealing work once, which is the accepted cost under
// default thresholds. A no-op when no rebase intervened.
//
// Write-path method (it rewrites writer-private state): run it on the
// writer goroutine, under the same publication mutex that serializes
// PublishMerge — internal/writer's loop calls it before every batch.
func (g *Graph) RepairAfterRebase() error {
	if !g.rebaseHappened.Load() {
		return nil
	}
	return g.forceMerge()
}
