// Package vectra is an embedded in-memory graph engine: all-memory
// pointer-free CSR adjacency + write-side SCC compression + level-synchronous
// parallel BFS. It targets enterprise dependency-graph scenarios where
// "the graph always fits in single-machine memory": bulk import, lock-free
// concurrent reads, disk used only for snapshot recovery / WAL replay.
//
// v0.7 write model (spec §2.3, micro-layer generations): every write batch
// (a single public write, or one Batch(fn) boundary) is materialized into an
// immutable micro-layer and published as a new generation
// {base, layers[]} through an atomic.Pointer — readers never lock. Queries
// are always legal and every family is immediately consistent:
//
//   - Point-query family (NodeByKey/NodeByID/EdgeByID/NodeCount/EdgeCount/
//     EdgesOf/LookupBy*) reads the three sources base ∪ layers with
//     tombstone shadowing — write-then-read is immediate.
//   - Traversal family (BFS/Reachable/ShortestPath/Cypher/NeighborsOf)
//     iterates rows over the same union through the generation View
//     (internal/engine): with nothing unmerged it IS the base CSR (the
//     slice-zip hot path runs untouched); Reachable's SCC-supernode
//     acceleration exists only at merge boundaries, so unmerged delta falls
//     back to node-level BFS. Cycles is the exception and merges pending
//     layers first (SCC structures only exist on a merged base).
//   - Seal() is an explicit merge (+ WAL checkpoint when a WAL is attached):
//     layers fold into a fresh immutable base. Sealed() reports "no unmerged
//     layers"; the v0.6 sealed/unsealed gates are dissolved (spec §6) —
//     writes after Seal simply open a new generation.
//
// Write semantics: AddNode is idempotent, UpsertEdge keeps the (src,dst,
// label) triple unique (weight-like props update in place, the visible
// edge keeps its EdgeID), AddEdge is pure append; deletes are tombstones
// shadowing everything older than the deleting layer and compact at merge
// time — node and edge IDs may be renumbered by a merge (spec §3.1 R1).
// Within a generation IDs come from a ratchet and are never recycled.
//
// Layout: this package is the public API facade (writes, options, type
// translation); the merged base lives in internal/engine, micro-layers in
// internal/delta, query algorithms in internal/traverse, encode/decode in
// internal/store.
package vectra

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/projanvil/vectra/internal/delta"
	"github.com/projanvil/vectra/internal/engine"
	"github.com/projanvil/vectra/internal/store"
	"github.com/projanvil/vectra/internal/wal"
)

// NodeID / EdgeID are integer identifiers within the graph. After a merge all
// query structures are addressed by NodeID; string keys are converted once
// at the API boundary. A merge may renumber IDs (spec §3.1 R1); between
// merges new records draw IDs from a ratchet and deleted records' IDs are
// never reused.
type NodeID uint32
type EdgeID uint32

var (
	// ErrSealed / ErrNotSealed are v0.6 sentinels kept for one release:
	// v0.7 dissolves the sealed gates (spec §6) and never returns them.
	ErrSealed       = errors.New("vectra: graph sealed, mutation rejected")
	ErrNotSealed    = errors.New("vectra: graph not sealed, query rejected")
	ErrNodeNotFound = errors.New("vectra: node key not found")
	// ErrClosed is returned by write methods after Close. Queries remain
	// legal on a closed graph (it reads the last checkpointed state).
	ErrClosed = errors.New("vectra: graph closed, mutation rejected")

	// ErrNotWriter enforces wrapper-mode single-writer discipline (spec
	// §2.3 R7): after internal/writer.Attach claims the graph, write
	// methods (AddNode/AddEdge/UpsertEdge/DeleteEdge/DeleteNode/Batch)
	// called from any goroutine other than the attached writer are
	// rejected. Detection is goroutine-token based and only compiled into
	// non-release builds (build tag "release" compiles it away — release
	// builds rely on documentation instead; concurrent misuse there is a
	// data race regardless). IndexProperty is writer-private state too:
	// keep it on the writer goroutine as well (its signature predates this
	// check and cannot carry an error).
	ErrNotWriter = errors.New("vectra: mutation rejected: not the attached writer")
)

type Node struct {
	ID    NodeID         `json:"id"`
	Key   string         `json:"key"`
	Label string         `json:"label"`
	Props map[string]any `json:"props,omitempty"`
}

type Edge struct {
	ID    EdgeID         `json:"id"`
	Src   NodeID         `json:"src"`
	Dst   NodeID         `json:"dst"`
	Label string         `json:"label"`
	Props map[string]any `json:"props,omitempty"`
}

// Stats reports graph size and cycle-compression results. CyclicNodes is
// the node count of SCCs with size>1 (self-loops excluded, reported
// separately by Cycles()). SCC fields exist only on the merged base — with
// unmerged layers present Stats reports counts only.
type Stats struct {
	Nodes       int
	Edges       int
	SCCs        int
	CyclicNodes int
}

// genView is one immutable generation of the graph: a merged base plus the
// micro-layers stacked on top of it (newest last). Published through
// Graph.view (atomic.Pointer); after Store nothing reachable from a genView
// is ever mutated, so readers may walk it without synchronization.
//
// Composition contract (internal/delta package doc): a layer's tombstones
// shadow everything OLDER — the base and earlier layers; a layer's live
// records win over its own tombstones; a record of layer L is visible iff no
// layer newer than L tombstones its key; a base record is visible iff no
// layer tombstones its key.
type genView struct {
	base     *engine.Snapshot
	baseKeys map[string]NodeID // base node key -> base node id (rebuilt at merge/load)
	layers   []*delta.Layer    // immutable micro-layers, oldest first
	lsn      uint64            // WAL LSN covered by the newest layer (informational)
	nodes    int               // visible node count (three-source)
	edges    int               // visible edge count (three-source)
}

// nodeWinner resolves key to its visible node (three-source).
func (v *genView) nodeWinner(key string) (Node, bool) {
	for i := len(v.layers) - 1; i >= 0; i-- {
		l := v.layers[i]
		if rec, ok := l.LookupNode(key); ok {
			id, _ := l.LookupNodeID(key)
			return Node{ID: NodeID(id), Key: key, Label: rec.Label, Props: rec.Props}, true
		}
		if l.NodeTombstones()[key] {
			return Node{}, false
		}
	}
	if id, ok := v.baseKeys[key]; ok {
		if rec, ok2 := v.base.NodeInfo(uint32(id)); ok2 {
			return Node{ID: id, Key: key, Label: rec.Label, Props: rec.Props}, true
		}
	}
	return Node{}, false
}

// nodeIDOf resolves key to its winner id for the endpoint fields of layer
// edge records (EdgesOf, edgeByID).
//
// Invariant: a live layer edge never references a dead endpoint — AddEdge/
// UpsertEdge validate endpoints via checkLiveNodes, and DeleteNode cascade-
// tombstones every incident edge — so callers only reach the fallback
// through a broken invariant upstream, never through user input. The
// fallback returns 0 rather than panicking because the signatures cannot
// error and queries must stay total; a 0 here means "consistency bug to
// investigate", not a legitimate miss.
func (v *genView) nodeIDOf(key string) NodeID {
	if n, ok := v.nodeWinner(key); ok {
		return n.ID
	}
	return 0
}

// baseKeyOf maps a base node id to its key ("" when out of range).
func (v *genView) baseKeyOf(id uint32) string {
	rec, ok := v.base.NodeInfo(id)
	if !ok {
		return ""
	}
	return rec.Key
}

// baseEdgeKeyOf derives the logical key of a base edge id.
func (v *genView) baseEdgeKeyOf(eid uint32) delta.EdgeKey {
	src, dst, label, _, _ := v.base.EdgeInfo(eid)
	return delta.EdgeKey{SrcKey: v.baseKeyOf(src), DstKey: v.baseKeyOf(dst), Label: label}
}

// baseEdgeLive reports whether the base edge with this key is still visible
// (no layer tombstones it).
func (v *genView) baseEdgeLive(k delta.EdgeKey) bool {
	for _, l := range v.layers {
		if l.EdgeTombstones()[k] {
			return false
		}
	}
	return true
}

// layerEdgeLive reports whether a record of layer i with this key is still
// visible (no strictly newer layer tombstones it).
func (v *genView) layerEdgeLive(i int, k delta.EdgeKey) bool {
	for j := i + 1; j < len(v.layers); j++ {
		if v.layers[j].EdgeTombstones()[k] {
			return false
		}
	}
	return true
}

// nodeRef is the writer-side mirror of one node key: its stable id (kept
// across deletes for revive-reuse, v0.6 semantics), visibility, and the
// current winner label/props (O(1) endpoint materialization without walking
// the layer stack).
type nodeRef struct {
	id    NodeID
	live  bool
	label string
	props map[string]any
}

// edgeRef is the writer-side mirror of one edge key: the winner EdgeID
// (transferred by folds, v0.6 "keeps the most recent edge's ID") and the
// number of visible parallels.
type edgeRef struct {
	winner EdgeID
	n      int
}

// Graph is a Vectra graph. Single writer (spec D5: the caller's goroutine in
// direct mode); lock-free concurrent reads through the generation pointer.
// Writer-private mirrors (nodes/edges/labels/batch state) are only touched
// by the writer and are never read by query paths.
//
// WAL sessions (see Open): every successful write method is recorded as
// Changeset entries; writes outside Batch flush one frame (one fsync in
// ModeAlways) per write and materialize one one-op micro-layer, Batch
// flushes one frame per boundary and materializes one layer per boundary —
// frames and layers are 1:1 at Builder boundaries (a kind-regressing batch
// splits into frames, replay is order-faithful either way).
type Graph struct {
	view atomic.Pointer[genView]

	// writer-private published-state mirrors (reconciled at publish)
	nodes     map[string]nodeRef        // key -> id + visibility
	edges     map[delta.EdgeKey]edgeRef // key -> winner id + visible parallels
	nodeTotal int
	edgeTotal int
	nextNode  NodeID // ID ratchets: never reused within a generation
	nextEdge  EdgeID
	labels    map[string]bool // label strings known to have been used

	// active batch (a single write is a batch of one)
	builder    *delta.Builder
	bNodeIDs   map[string]uint32 // key -> assigned id (this batch)
	bEdgeIDs   []uint32          // one id per edge slot appended (this batch)
	bLiveNodes map[string]bool   // keys live in the builder
	bEdgeWin   map[delta.EdgeKey]EdgeID
	bEdgeN     map[delta.EdgeKey]int
	bKilled    map[delta.EdgeKey]bool // keys tombstoned within the batch
	bIndex     []string               // pending index declarations (this batch)

	// WAL session state (all zero for New()/Load() graphs — v0.6 behavior).
	wal      *wal.WAL // nil = no recording
	walDir   string   // checkpoint manifest location
	dbPath   string   // session path; checkpoints write <dbPath minus ext>-<lsn>.db next to it
	pendOps  []pendOp // recorded-but-unflushed ops (inside Batch, or single writes mid-record)
	batching bool     // inside Batch: records accumulate instead of flushing
	closed   bool     // after Close: writes fail with ErrClosed, queries legal

	// writerTok is the wrapper-mode writer discipline hook (spec §2.3 R7):
	// 0 = direct mode (all writes legal); non-zero = the goroutine token
	// recorded by ClaimWriter. Checked by checkWriter at write-method entry
	// (debug builds only; release builds compile the check away). Only the
	// hook pair in writer_hook*.go touches it.
	writerTok atomic.Uint64
}

// opKind orders pendOp entries exactly like the Changeset apply order:
// node upserts → edge upserts → edge deletes → node deletes.
type opKind int

const (
	opNode opKind = iota
	opEdge
	opEdgeDelete
	opNodeDelete
)

// pendOp is one recorded write, kept in original order until flush.
type pendOp struct {
	kind opKind
	node NodeUpsert
	edge EdgeUpsert
	edel EdgeDelete
	ndel string
}

// New returns an empty graph. The initial generation is an empty merged
// base (engine.Seal of nothing), so every query is legal from the start;
// Sealed() is true until the first write opens a layer.
func New() *Graph {
	g := &Graph{
		nodes:  map[string]nodeRef{},
		edges:  map[delta.EdgeKey]edgeRef{},
		labels: map[string]bool{},
	}
	snap, err := engine.Seal(nil, nil, nil, nil)
	if err != nil {
		panic(fmt.Sprintf("vectra: sealing an empty base: %v", err))
	}
	g.view.Store(&genView{base: snap, baseKeys: map[string]NodeID{}})
	return g
}

func copyProps(props map[string]any) map[string]any {
	if props == nil {
		return nil
	}
	m := make(map[string]any, len(props))
	for k, v := range props {
		m[k] = v
	}
	return m
}

// ---- writer-side batch plumbing ---------------------------------------------

// ensureBuilder opens the batch state if none is active.
func (g *Graph) ensureBuilder() {
	if g.builder != nil {
		return
	}
	g.builder = delta.NewBuilder()
	g.bNodeIDs = map[string]uint32{}
	g.bLiveNodes = map[string]bool{}
	g.bEdgeWin = map[delta.EdgeKey]EdgeID{}
	g.bEdgeN = map[delta.EdgeKey]int{}
	g.bKilled = map[delta.EdgeKey]bool{}
	g.bEdgeIDs = nil
	g.bIndex = nil
}

// finishOp closes a single write: outside Batch the op's frame is already on
// the WAL (log-first recording), so only the layer publication remains.
func (g *Graph) finishOp() error {
	if !g.batching {
		g.publishLayer()
	}
	return nil
}

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
// id and state, so builder-side edge ops can validate endpoints (internal/
// delta D4: the layer cannot see the base; the writer must materialize).
func (g *Graph) materializeEndpoint(key string) {
	if g.bLiveNodes[key] {
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
		n := 0
		if !g.bKilled[k] { // batch-pending tombstones already own these
			n = g.edges[k].n
		}
		if n == 0 {
			return
		}
		seen[k] = true
		for i := 0; i < n; i++ {
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
// index declarations) is dropped — it must not flip Sealed() or grow the
// stack. The layers slice is extended by plain append: safe under the single
// writer because older generations only ever read their own shorter prefix
// (append either writes past the old len or reallocates).
func (g *Graph) publishLayer() {
	b := g.builder
	if b == nil {
		return
	}
	l := b.BuildWithIDs(g.bNodeIDs, g.bEdgeIDs)
	g.builder = nil
	g.bNodeIDs, g.bEdgeIDs = nil, nil
	g.bLiveNodes, g.bEdgeWin, g.bEdgeN, g.bKilled, g.bIndex = nil, nil, nil, nil, nil

	if l.NodeCount() == 0 && l.EdgeCount() == 0 &&
		len(l.NodeTombstones()) == 0 && len(l.EdgeTombstones()) == 0 && len(l.IndexKeys()) == 0 {
		return
	}

	// reconcile the published mirrors: tombstones first, live records on top
	// (the layer's own live records win over its own tombstones).
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

	v := g.view.Load()
	layers := append(v.layers, l)
	lsn := v.lsn
	if g.wal != nil {
		lsn = g.wal.LastLSN()
	}
	g.view.Store(&genView{
		base: v.base, baseKeys: v.baseKeys, layers: layers,
		lsn: lsn, nodes: g.nodeTotal, edges: g.edgeTotal,
	})
}

// ---- public write path ------------------------------------------------------

// AddNode idempotently writes a node: if the key already exists it updates
// label/props and returns the original ID (a deleted key revives, reusing
// its ID). The write is immediately visible to every point query.
//
// Props sharing contract: this method copies the passed-in map; but the maps
// exposed via Node/Edge after a merge are internal references — updates must
// replace them wholesale (AddNode/AddEdge again); in-place mutation is
// forbidden.
func (g *Graph) AddNode(key, label string, props map[string]any) (NodeID, error) {
	if g.closed {
		return 0, ErrClosed
	}
	if err := g.checkWriter(); err != nil {
		return 0, err
	}
	if err := g.recordNode(key, label, props); err != nil {
		return 0, err
	}
	g.ensureBuilder()
	id, ok := g.bNodeIDs[key]
	if !ok {
		if ref, seen := g.nodes[key]; seen {
			id = uint32(ref.id) // override/revive keeps the original ID
		} else {
			id = uint32(g.nextNode)
			g.nextNode++
		}
	}
	g.bNodeIDs[key] = id
	g.bLiveNodes[key] = true
	if err := g.builder.UpsertNode(key, label, props); err != nil {
		return 0, err // unreachable for a fresh builder; surfaced anyway
	}
	return NodeID(id), g.finishOp()
}

// AddEdge appends a directed edge (multi-edges/self-loops both allowed).
// Immediately visible; new EdgeIDs come from the generation ratchet.
func (g *Graph) AddEdge(srcKey, dstKey, label string, props map[string]any) (EdgeID, error) {
	if g.closed {
		return 0, ErrClosed
	}
	if err := g.checkWriter(); err != nil {
		return 0, err
	}
	if err := g.checkLiveNodes(srcKey, dstKey); err != nil {
		return 0, err
	}
	if err := g.recordEdge(srcKey, dstKey, label, props, true); err != nil {
		return 0, err
	}
	g.ensureBuilder()
	g.materializeEndpoint(srcKey)
	g.materializeEndpoint(dstKey)
	g.labels[label] = true
	id := g.newEdgeSlotID()
	k := delta.EdgeKey{SrcKey: srcKey, DstKey: dstKey, Label: label}
	g.bEdgeWin[k] = id
	g.bEdgeN[k]++
	delete(g.bKilled, k)
	if err := g.builder.AddEdge(srcKey, dstKey, label, props); err != nil {
		return 0, err
	}
	return id, g.finishOp()
}

// UpsertEdge replaces-by-key: if live edges with the same (src, dst, label)
// exist anywhere in the visible state (base or layers), the triple collapses
// onto one edge — the visible edge keeps its EdgeID and its Props are
// replaced wholesale; otherwise a new edge is appended. Unlike AddEdge (pure
// append, multigraph semantics) this method guarantees the triple is unique
// — it is the entry point for updating weight-like props (relation
// strength, event counts).
func (g *Graph) UpsertEdge(srcKey, dstKey, label string, props map[string]any) (EdgeID, error) {
	if g.closed {
		return 0, ErrClosed
	}
	if err := g.checkWriter(); err != nil {
		return 0, err
	}
	if err := g.checkLiveNodes(srcKey, dstKey); err != nil {
		return 0, err
	}
	if err := g.recordEdge(srcKey, dstKey, label, props, false); err != nil {
		return 0, err
	}
	g.ensureBuilder()
	g.materializeEndpoint(srcKey)
	g.materializeEndpoint(dstKey)
	g.labels[label] = true
	k := delta.EdgeKey{SrcKey: srcKey, DstKey: dstKey, Label: label}
	var id EdgeID
	if g.bEdgeN[k] > 0 {
		id = g.bEdgeWin[k] // fold onto this batch's most recent parallel (in place)
	} else {
		// the fold appends a slot in this layer: it carries either the
		// transferred winner ID (visible edge keeps its ID) or a fresh one
		if ref, ok := g.edges[k]; ok && !g.bKilled[k] {
			id = ref.winner
		} else {
			id = g.nextEdge
			g.nextEdge++
		}
		g.bEdgeIDs = append(g.bEdgeIDs, uint32(id))
	}
	if err := g.builder.UpsertEdgeFold(srcKey, dstKey, label, props); err != nil {
		return 0, err
	}
	g.bEdgeWin[k] = id
	g.bEdgeN[k] = 1 // the fold guarantees triple uniqueness within the batch
	delete(g.bKilled, k)
	return id, g.finishOp()
}

// DeleteEdge removes every live parallel edge with the given (src, dst,
// label) triple across all three sources (idempotent: deleting a triple that
// never existed succeeds with 0). Returns the number of edges actually
// deleted. Immediately invisible afterwards.
func (g *Graph) DeleteEdge(srcKey, dstKey, label string) (int, error) {
	if g.closed {
		return 0, ErrClosed
	}
	if err := g.checkWriter(); err != nil {
		return 0, err
	}
	if err := g.checkLiveNodes(srcKey, dstKey); err != nil {
		return 0, err
	}
	if !g.labels[label] {
		return 0, nil // label never used by an edge: no match possible, idempotent success
	}
	k := delta.EdgeKey{SrcKey: srcKey, DstKey: dstKey, Label: label}
	n := g.bEdgeN[k]
	if !g.bKilled[k] {
		n += g.edges[k].n // published parallels (base + older layers)
	}
	if err := g.recordEdgeDelete(srcKey, dstKey, label); err != nil {
		return 0, err
	}
	g.ensureBuilder()
	if _, err := g.builder.DeleteEdge(k); err != nil {
		return 0, err
	}
	g.bKilled[k] = true
	g.bEdgeN[k] = 0
	delete(g.bEdgeWin, k)
	return n, g.finishOp()
}

// DeleteNode removes a node and cascade-deletes all its incident edges
// (DETACH DELETE semantics) across all three sources: base incidents are
// discovered by bidirectional CSR row scans (spec §3.1 R2), older-layer
// incidents by record scan; everything older dies by tombstone. Re-adding
// the same key via AddNode revives the node (ID reused) without resurrecting
// the cascaded edges. Returns the number of cascaded edge deletions.
func (g *Graph) DeleteNode(key string) (int, error) {
	if g.closed {
		return 0, ErrClosed
	}
	if err := g.checkWriter(); err != nil {
		return 0, err
	}
	if !g.nodeLive(key) {
		return 0, ErrNodeNotFound
	}
	if err := g.recordNodeDelete(key); err != nil {
		return 0, err
	}
	g.ensureBuilder()
	var hookKeys []delta.EdgeKey
	n, err := g.builder.DeleteNode(key, func(k string) []delta.EdgeKey {
		hookKeys = g.olderIncidentEdges(k)
		return hookKeys
	})
	if err != nil {
		return 0, err
	}
	g.bLiveNodes[key] = false
	for k := range g.bEdgeN {
		if k.SrcKey == key || k.DstKey == key {
			g.bEdgeN[k] = 0
			g.bKilled[k] = true
			delete(g.bEdgeWin, k)
		}
	}
	for _, k := range hookKeys {
		g.bKilled[k] = true
		g.bEdgeN[k] = 0
		delete(g.bEdgeWin, k)
	}
	return n, g.finishOp()
}

// --- WAL op recorder ---------------------------------------------------------
//
// When a WAL is attached, each successful write method records one pendOp
// BEFORE mutating memory (log-first for single writes). Outside Batch the op
// flushes immediately as its own frame — one fsync per write in ModeAlways,
// the documented cost of unbatched direct-mode writes — and the layer
// materialization follows the frame, so a flush failure leaves memory
// untouched. Inside Batch the ops accumulate and the boundary flushes them
// as frame(s), then materializes one micro-layer for the whole batch.
//
// Frame partitioning: a Changeset applies ops in kind order (nodes → edges →
// edge deletes → node deletes), so a batch whose op sequence is NOT
// kind-ordered (e.g. delete a key, then re-add it) cannot be expressed as one
// frame without reordering semantics. flushPending therefore cuts the op
// sequence at every kind regression and emits one frame per kind-ordered run
// — replaying the frames reproduces the original op order exactly. Normal
// batches stay a single frame (one fsync).
//
// Append/fsync failure discipline: a write method (or Batch/Close flush)
// that returns a WAL error must be retried or the graph abandoned — never
// silently continued. A failed Append may still have put the frame on disk
// (a write/fsync can fail after partial success), and any ops it did not
// persist stay buffered in pendOps, so the next flush (Batch boundary, or
// Close) re-attempts exactly those; continuing to write without a
// successful flush risks losing the earlier ops.

func (g *Graph) recordNode(key, label string, props map[string]any) error {
	if g.wal == nil {
		return nil
	}
	g.pendOps = append(g.pendOps, pendOp{kind: opNode, node: NodeUpsert{Key: key, Label: label, Props: copyProps(props)}})
	return g.maybeFlush()
}

func (g *Graph) recordEdge(srcKey, dstKey, label string, props map[string]any, allowParallel bool) error {
	if g.wal == nil {
		return nil
	}
	g.pendOps = append(g.pendOps, pendOp{kind: opEdge, edge: EdgeUpsert{
		SrcKey: srcKey, DstKey: dstKey, Label: label,
		Props: copyProps(props), AllowParallel: allowParallel,
	}})
	return g.maybeFlush()
}

func (g *Graph) recordEdgeDelete(srcKey, dstKey, label string) error {
	if g.wal == nil {
		return nil
	}
	g.pendOps = append(g.pendOps, pendOp{kind: opEdgeDelete, edel: EdgeDelete{SrcKey: srcKey, DstKey: dstKey, Label: label}})
	return g.maybeFlush()
}

func (g *Graph) recordNodeDelete(key string) error {
	if g.wal == nil {
		return nil
	}
	g.pendOps = append(g.pendOps, pendOp{kind: opNodeDelete, ndel: key})
	return g.maybeFlush()
}

// maybeFlush flushes unless a Batch boundary is holding the buffer.
func (g *Graph) maybeFlush() error {
	if g.batching {
		return nil
	}
	return g.flushPending()
}

// flushPending emits the buffered ops as one frame per kind-ordered run. On
// error the unflushed ops stay buffered so a retry (e.g. a retried Close)
// does not silently drop them.
func (g *Graph) flushPending() error {
	if g.wal == nil {
		g.pendOps = nil
		return nil
	}
	i := 0
	for i < len(g.pendOps) {
		j := i + 1
		for j < len(g.pendOps) && g.pendOps[j].kind >= g.pendOps[j-1].kind {
			j++
		}
		cs := Changeset{Version: changesetVersion}
		for _, op := range g.pendOps[i:j] {
			switch op.kind {
			case opNode:
				cs.Nodes = append(cs.Nodes, op.node)
			case opEdge:
				cs.Edges = append(cs.Edges, op.edge)
			case opEdgeDelete:
				cs.EdgeDeletes = append(cs.EdgeDeletes, op.edel)
			case opNodeDelete:
				cs.NodeDeletes = append(cs.NodeDeletes, op.ndel)
			}
		}
		if _, err := g.wal.Append(cs); err != nil {
			g.pendOps = append([]pendOp(nil), g.pendOps[i:]...)
			return err
		}
		i = j
	}
	g.pendOps = nil
	return nil
}

// applyChangeset replays one frame with the write-method semantics. Used by
// Open's recovery path (recording is detached there — replaying must not
// re-record). One Batch boundary per frame: recovery rebuilds one micro-layer
// per replayed frame. Any error means the frame references state the replay
// did not rebuild identically: surfaced, not skipped (the graph is abandoned
// by the caller, the partially-applied layer never escapes Open).
func (g *Graph) applyChangeset(cs Changeset) error {
	var firstErr error
	err := g.Batch(func(g *Graph) {
		fail := func(err error) bool {
			if firstErr == nil {
				firstErr = err
			}
			return true
		}
		for _, n := range cs.Nodes {
			if _, err := g.AddNode(n.Key, n.Label, n.Props); err != nil && fail(err) {
				return
			}
		}
		for _, e := range cs.Edges {
			var err error
			if e.AllowParallel {
				_, err = g.AddEdge(e.SrcKey, e.DstKey, e.Label, e.Props)
			} else {
				_, err = g.UpsertEdge(e.SrcKey, e.DstKey, e.Label, e.Props)
			}
			if err != nil && fail(err) {
				return
			}
		}
		for _, d := range cs.EdgeDeletes {
			if _, err := g.DeleteEdge(d.SrcKey, d.DstKey, d.Label); err != nil && fail(err) {
				return
			}
		}
		for _, key := range cs.NodeDeletes {
			if _, err := g.DeleteNode(key); err != nil && fail(err) {
				return
			}
		}
	})
	if err != nil {
		return err
	}
	return firstErr
}

// Batch groups writes into one WAL boundary AND one micro-layer: fn's write
// methods record Changeset entries without flushing, and the boundary flushes
// them as one frame (one fsync in ModeAlways — group commit for direct-mode
// writers) and materializes the whole batch as a single layer (published
// atomically at the boundary — writes inside fn are not yet visible to
// queries). A normal batch is exactly one frame; batches whose op order
// regresses across Changeset kinds are split at each regression so replay
// preserves the original op order (see the op-recorder comment).
//
// Nested Batch calls are absorbed into the outer boundary. WALOff callers
// pay nothing for frames but still get the per-batch layer grouping.
//
// Known direct-mode limitation (closed by the v0.7 wrapper-mode writer): a
// flush error at the boundary returns after fn's mutations already hit
// memory — memory may be ahead of the log until the next successful flush.
func (g *Graph) Batch(fn func(*Graph)) error {
	if g.closed {
		return ErrClosed
	}
	if err := g.checkWriter(); err != nil {
		return err
	}
	if g.batching {
		fn(g) // nested: absorbed into the outer boundary
		return nil
	}
	g.batching = true
	g.ensureBuilder()
	defer func() { // even if fn panics, never wedge the recorder
		g.batching = false
		if g.builder != nil {
			// Panic path: drop the un-published layer (the buffered ops stay
			// in pendOps for the next flush, T2 discipline).
			g.builder = nil
			g.bNodeIDs, g.bEdgeIDs = nil, nil
			g.bLiveNodes, g.bEdgeWin, g.bEdgeN, g.bKilled, g.bIndex = nil, nil, nil, nil, nil
		}
	}()
	fn(g)
	var err error
	if g.wal != nil {
		err = g.flushPending()
	}
	g.publishLayer()
	return err
}

// Open opens (or creates) a durable graph session at path. Unlike
// LoadFile — which merges the snapshot into a base — an Open session
// recovers into a writable graph: snapshot first (when a checkpoint exists),
// then WAL frames replayed in LSN order above the checkpoint boundary (one
// micro-layer per frame), with no recording during replay. Merging is
// deferred to the checkpoint at Close or an explicit Seal.
//
// Checkpoints never write `path` in place: each one writes a watermark
// snapshot <path minus ext>-<lsn>.db next to path and atomically points the
// manifest at it (old watermarks are deleted only after the new manifest
// lands). A crash between SaveFile and the manifest write therefore recovers
// from the previous, untouched watermark plus a full WAL replay — already
// checkpointed frames can never be applied twice (AllowParallel appends and
// deletes are not idempotent).
//
// Recovery rules:
//   - manifest present: load the snapshot file it names (resolved next to
//     path); frames with lsn > manifest.lsn replay on top. A missing
//     snapshot file is an error.
//   - no manifest, no WAL segments, snapshot present (first WAL-mode Open
//     next to a v0.6 file): the snapshot is adopted.
//   - no manifest, WAL segments present (checkpoint manifest lost, or a
//     session that never Closed): recovery starts from an empty graph and
//     replays every frame — the WAL is the source of truth.
//
// Close checkpoints (merge → SaveFile → manifest → WAL reset); a checkpoint
// failure returns an error and keeps the WAL for the next recovery attempt.
func Open(path string, opts Options) (*Graph, error) {
	if opts.WALMode == ModeOff {
		// v0.6 behavior: snapshot-only. Existing file loads merged; a missing
		// file starts a fresh writable graph (durability via Seal+SaveFile).
		if _, err := os.Stat(path); err == nil {
			return LoadFile(path)
		} else if !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
		return New(), nil
	}

	walDir := opts.WALDir
	if walDir == "" {
		walDir = path + ".wal"
	}
	man, hasMan, err := wal.ReadManifest(walDir)
	if err != nil {
		return nil, err
	}
	hasSegs, err := wal.HasSegments(walDir)
	if err != nil {
		return nil, err
	}

	var g *Graph
	loadSnapshot := func(p string) error {
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		defer f.Close()
		lg, err := loadGraph(f)
		if err != nil {
			return err
		}
		g = lg
		return nil
	}
	switch {
	case hasMan:
		// The manifest names the exact watermark snapshot this session
		// continues from; `path` itself is never the checkpoint target.
		snap := filepath.Join(filepath.Dir(path), man.Snapshot)
		if err := loadSnapshot(snap); err != nil {
			return nil, fmt.Errorf("vectra: manifest at %s references snapshot %s: %w", walDir, snap, err)
		}
	case hasSegs:
		// WAL-only recovery: start empty, replay everything below.
	default:
		if _, err := os.Stat(path); err == nil {
			if err := loadSnapshot(path); err != nil {
				return nil, err
			}
		} else if !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
	}
	if g == nil {
		g = New()
	}

	w, err := wal.Open(walDir, opts.WALMode)
	if err != nil {
		return nil, err
	}
	// Replay with recording detached (g.wal still nil): replaying must not
	// amplify frames. Torn tails stop at the last complete frame.
	if _, err := w.Replay(g.applyChangeset); err != nil {
		w.Close()
		return nil, err
	}
	g.wal, g.walDir, g.dbPath = w, walDir, path
	return g, nil
}

// snapshotFile is the checkpoint target for a given watermark: <dbPath minus
// ext>-<lsn>.db in the same directory. The LSN in the name is what closes
// the checkpoint crash window — a snapshot is only ever referenced by a
// manifest that names it exactly, and files are never overwritten in place
// across checkpoints.
func (g *Graph) snapshotFile(lsn uint64) string {
	return fmt.Sprintf("%s-%d.db", strings.TrimSuffix(g.dbPath, filepath.Ext(g.dbPath)), lsn)
}

// cleanupSnapshots deletes watermark snapshots from older checkpoints (best
// effort: leftovers are ignored by recovery — the manifest only ever names
// one — and the next checkpoint retries the cleanup).
func (g *Graph) cleanupSnapshots(keep string) {
	namePrefix := filepath.Base(strings.TrimSuffix(g.dbPath, filepath.Ext(g.dbPath)) + "-")
	dir := filepath.Dir(g.dbPath)
	es, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range es {
		if e.IsDir() || e.Name() == keep {
			continue
		}
		if !strings.HasPrefix(e.Name(), namePrefix) || !strings.HasSuffix(e.Name(), ".db") {
			continue
		}
		// Only <base>-<lsn>.db files are checkpoint artifacts: the middle
		// segment must be all digits. User files that merely share the
		// prefix (graph-backup.db, graph-export.db, date-stamped exports)
		// are never touched.
		mid := strings.TrimSuffix(strings.TrimPrefix(e.Name(), namePrefix), ".db")
		if _, err := strconv.ParseUint(mid, 10, 64); err != nil {
			continue
		}
		os.Remove(filepath.Join(dir, e.Name()))
	}
}

// Close ends a session. With a WAL attached, Close is a mandatory checkpoint:
// flush pending frames + fsync → merge → SaveFile(<dbPath minus ext>-<lsn>.db)
// → manifest (atomically pointing at that watermark) → WAL reset → delete
// older watermark snapshots.
//
// Failure semantics:
//   - before the manifest lands, any failure returns the error WITHOUT
//     deleting the WAL — the next Open recovers from it, and Close can be
//     retried (merge and SaveFile are safe on an already-merged graph);
//   - once the manifest has landed the checkpoint is committed: later
//     failures (segment rollover, fd close) are surfaced but the session is
//     already closed, so a retry is a clean no-op — it can never wedge;
//   - a WAL handle that died mid-checkpoint (wal.ErrClosed from Sync/Reset/
//     Close) is treated as benign: the manifest gates the un-reset segments.
//
// Append/fsync failure discipline (see the op-recorder comment): if a frame
// Append ever returned an error, the caller must retry the flush (Batch/
// Close) or abandon the graph — buffered ops stay in pendOps and are
// re-attempted by the next flush, because a frame whose fsync reported
// failure may still be on disk.
//
// WALOff sessions: Close only flips to closed (writes then return ErrClosed;
// durability was the caller's job, v0.6 style). In both modes queries stay
// legal after Close.
func (g *Graph) Close() error {
	if g.closed {
		return nil
	}
	if g.wal == nil {
		g.closed = true
		return nil
	}
	if err := g.flushPending(); err != nil {
		return err
	}
	if err := g.wal.Sync(); err != nil && !errors.Is(err, wal.ErrClosed) {
		return err
	}
	lsn := g.wal.LastLSN()
	if err := g.forceMerge(); err != nil {
		return err
	}
	snap := g.snapshotFile(lsn)
	if err := g.SaveFile(snap); err != nil {
		return err
	}
	if err := wal.WriteManifest(g.walDir, wal.Manifest{
		Snapshot: filepath.Base(snap),
		Segment:  g.wal.Segment(),
		LSN:      lsn,
	}); err != nil {
		return err
	}
	// The manifest has landed: this checkpoint is committed. From here on a
	// failure is surfaced but must never leave the session retryable-but-
	// unwritable — mark closed so a retry is a clean no-op.
	g.closed = true
	if err := g.wal.Reset(lsn); err != nil && !errors.Is(err, wal.ErrClosed) {
		g.wal.Close()
		g.wal = nil
		return err
	}
	if err := g.wal.Close(); err != nil && !errors.Is(err, wal.ErrClosed) {
		g.wal = nil
		return err
	}
	g.wal = nil
	g.cleanupSnapshots(filepath.Base(snap))
	return nil
}

// ---- point queries (three-source: layers new→old + base, tombstones) --------

// NodeByKey finds the winning (newest live) record for key across base and
// layers; tombstoned keys are invisible.
func (g *Graph) NodeByKey(key string) (Node, bool) {
	return g.view.Load().nodeWinner(key)
}

// NodeByID resolves an ID to its visible record. Overridden/deleted records
// (whose IDs may still be listed by older layers) do not resolve.
func (g *Graph) NodeByID(id NodeID) (Node, bool) {
	return g.view.Load().nodeByID(id)
}

// nodeByID is NodeByID pinned to this generation (the three-source surfaces
// in view.go run against exactly one published genView).
func (v *genView) nodeByID(id NodeID) (Node, bool) {
	for i := len(v.layers) - 1; i >= 0; i-- {
		l := v.layers[i]
		idx, ok := l.FindNodeID(uint32(id))
		if !ok {
			continue
		}
		rec := l.Nodes()[idx]
		// A newer layer's live record for the same key would carry the same
		// (transferred) id and have matched above; only tombstones can kill.
		for j := i + 1; j < len(v.layers); j++ {
			if v.layers[j].NodeTombstones()[rec.Key] {
				return Node{}, false
			}
		}
		return Node{ID: id, Key: rec.Key, Label: rec.Label, Props: rec.Props}, true
	}
	if uint32(id) < uint32(v.base.NodeCount()) {
		rec, ok := v.base.NodeInfo(uint32(id))
		if !ok {
			return Node{}, false
		}
		for _, l := range v.layers {
			if l.NodeTombstones()[rec.Key] {
				return Node{}, false
			}
		}
		return Node{ID: id, Key: rec.Key, Label: rec.Label, Props: rec.Props}, true
	}
	return Node{}, false
}

// EdgeByID resolves an EdgeID to its visible record. Layer records are
// resolved first (a fold transfers the winner ID onto the newer record);
// base records resolve unless a layer tombstones their key.
func (g *Graph) EdgeByID(id EdgeID) (Edge, bool) {
	return g.view.Load().edgeByID(id)
}

// edgeByID is EdgeByID pinned to this generation.
func (v *genView) edgeByID(id EdgeID) (Edge, bool) {
	for i := len(v.layers) - 1; i >= 0; i-- {
		l := v.layers[i]
		idx, ok := l.FindEdgeID(uint32(id))
		if !ok {
			continue
		}
		rec := l.Edges()[idx]
		k := delta.EdgeKey{SrcKey: rec.SrcKey, DstKey: rec.DstKey, Label: rec.Label}
		if !v.layerEdgeLive(i, k) {
			return Edge{}, false
		}
		return Edge{
			ID: id, Src: v.nodeIDOf(rec.SrcKey), Dst: v.nodeIDOf(rec.DstKey),
			Label: rec.Label, Props: rec.Props,
		}, true
	}
	if uint32(id) < uint32(v.base.EdgeCount()) {
		src, dst, label, props, ok := v.base.EdgeInfo(uint32(id))
		if !ok {
			return Edge{}, false
		}
		k := delta.EdgeKey{SrcKey: v.baseKeyOf(src), DstKey: v.baseKeyOf(dst), Label: label}
		if !v.baseEdgeLive(k) {
			return Edge{}, false
		}
		return Edge{ID: id, Src: NodeID(src), Dst: NodeID(dst), Label: label, Props: props}, true
	}
	return Edge{}, false
}

// NodeCount/EdgeCount report the visible size of the whole generation
// (base minus tombstoned plus visible layer records).
func (g *Graph) NodeCount() int { return g.view.Load().nodes }

func (g *Graph) EdgeCount() int { return g.view.Load().edges }

// Sealed reports whether the graph has no unmerged micro-layers. It flips to
// false with the first write after a merge and back on Seal/forceMerge.
func (g *Graph) Sealed() bool { return len(g.view.Load().layers) == 0 }

// Stats reports graph size and, when the generation is fully merged, the
// SCC compression results of the base. With unmerged layers present only the
// counts are reported (SCC structures exist on the merged base only).
func (g *Graph) Stats() Stats {
	v := g.view.Load()
	if len(v.layers) > 0 {
		return Stats{Nodes: v.nodes, Edges: v.edges}
	}
	st := Stats{
		Nodes: v.base.NodeCount(),
		Edges: v.base.EdgeCount(),
		SCCs:  int(v.base.SCCCount()),
	}
	for c := uint32(0); c < v.base.SCCCount(); c++ {
		if size := len(v.base.SCCMembers(c)); size > 1 {
			st.CyclicNodes += size
		}
	}
	return st
}

// ---- merge pipeline ----------------------------------------------------------

// unionRecords computes the logical view of the CURRENT generation (see the
// genView method for the folding rules). Used by forceMerge, Save,
// ExportSnapshot and Clone (merge-on-demand without publishing).
func (g *Graph) unionRecords() ([]engine.NodeRec, []engine.EdgeRec, []string, []string) {
	return g.view.Load().unionRecords()
}

// unionRecords computes the logical view of this generation as raw engine
// records: the base with all layer tombstones applied, overlaid by layer
// records oldest→newest (node override by key, edge append), labels merged
// (base dictionary first — base LabelIDs stay valid), and pending index
// declarations harvested. Deterministic order: base records first, then
// layer records in (layer, index) order.
//
// Purity: the function reads only the pinned generation (base + layers),
// never writer-private state — that is what lets the background merger
// (internal/writer) run it against a captured prefix while the writer
// keeps publishing newer layers (spec §2.3 two-writer publish protocol).
func (v *genView) unionRecords() ([]engine.NodeRec, []engine.EdgeRec, []string, []string) {
	labels := make([]string, 0, v.base.LabelCount())
	labID := make(map[string]uint32, v.base.LabelCount())
	intern := func(name string) uint32 {
		if id, ok := labID[name]; ok {
			return id
		}
		id := uint32(len(labels))
		labID[name] = id
		labels = append(labels, name)
		return id
	}
	for i := 0; i < v.base.LabelCount(); i++ {
		intern(v.base.LabelName(uint32(i)))
	}
	for _, l := range v.layers {
		for _, lb := range l.Labels() {
			intern(lb)
		}
	}

	// nodes: base order, overrides in place, layer-first-seen appended
	type nodeSlot struct {
		rec  engine.NodeRec
		dead bool
	}
	ns := make([]nodeSlot, 0, v.base.NodeCount())
	idx := make(map[string]int, v.base.NodeCount())
	for _, rec := range v.base.ExportNodes() {
		idx[rec.Key] = len(ns)
		ns = append(ns, nodeSlot{rec: rec})
	}
	for _, l := range v.layers {
		for k := range l.NodeTombstones() {
			if i, ok := idx[k]; ok {
				ns[i].dead = true
				delete(idx, k)
			}
		}
		for _, rec := range l.Nodes() {
			if i, ok := idx[rec.Key]; ok {
				ns[i].rec = engine.NodeRec{Key: rec.Key, Label: rec.Label, Props: rec.Props}
			} else {
				idx[rec.Key] = len(ns)
				ns = append(ns, nodeSlot{rec: engine.NodeRec{Key: rec.Key, Label: rec.Label, Props: rec.Props}})
			}
		}
	}
	nodes := make([]engine.NodeRec, 0, len(ns))
	for _, s := range ns {
		if s.dead {
			continue
		}
		idx[s.rec.Key] = len(nodes)
		nodes = append(nodes, s.rec)
	}

	// edges: a record survives iff no strictly newer layer tombstones its key
	suff := make([]map[delta.EdgeKey]bool, len(v.layers)+1)
	suff[len(v.layers)] = map[delta.EdgeKey]bool{}
	for i := len(v.layers) - 1; i >= 0; i-- {
		m := make(map[delta.EdgeKey]bool, len(suff[i+1])+len(v.layers[i].EdgeTombstones()))
		maps.Copy(m, suff[i+1])
		for k := range v.layers[i].EdgeTombstones() {
			m[k] = true
		}
		suff[i] = m
	}
	edges := make([]engine.EdgeRec, 0, v.base.EdgeCount())
	appendEdge := func(srcKey, dstKey, label string, lid uint32, props map[string]any) {
		s, okS := idx[srcKey]
		d, okD := idx[dstKey]
		if !okS || !okD {
			return // defensive: edges whose endpoints died (they are tombstoned anyway)
		}
		edges = append(edges, engine.EdgeRec{Src: uint32(s), Dst: uint32(d), LabelID: lid, Props: props})
	}
	for _, er := range v.base.ExportEdges() {
		k := v.baseEdgeKeyFrom(er)
		if suff[0][k] {
			continue
		}
		appendEdge(k.SrcKey, k.DstKey, k.Label, er.LabelID, er.Props)
	}
	for i, l := range v.layers {
		next := suff[i+1]
		for _, rec := range l.Edges() {
			k := delta.EdgeKey{SrcKey: rec.SrcKey, DstKey: rec.DstKey, Label: rec.Label}
			if next[k] {
				continue
			}
			appendEdge(rec.SrcKey, rec.DstKey, rec.Label, intern(rec.Label), rec.Props)
		}
	}

	// pending index declarations harvested into the merge
	keySet := map[string]bool{}
	for _, k := range v.base.IndexedKeys() {
		keySet[k] = true
	}
	for _, l := range v.layers {
		for _, k := range l.IndexKeys() {
			keySet[k] = true
		}
	}
	indexKeys := make([]string, 0, len(keySet))
	for k := range keySet {
		indexKeys = append(indexKeys, k)
	}
	slices.Sort(indexKeys)
	return nodes, edges, labels, indexKeys
}

// baseEdgeKeyFrom derives the logical key of a raw base edge record.
func (v *genView) baseEdgeKeyFrom(er engine.EdgeRec) delta.EdgeKey {
	return delta.EdgeKey{
		SrcKey: v.baseKeyOf(er.Src),
		DstKey: v.baseKeyOf(er.Dst),
		Label:  v.base.LabelName(er.LabelID),
	}
}

// forceMerge folds the whole generation (base ∪ layers) into a fresh
// immutable base and publishes {newBase, layers=nil}: tombstones applied to
// the base first, layer records overlaid oldest→newest, labels/index
// declarations merged, node and edge IDs renumbered densely (R1) with the
// ratchets reset to the new base size. No-op when nothing is unmerged. The
// old generation stays readable for concurrent holders; memory is reclaimed
// by GC. Cycles() uses this before answering (spec §3.2).
func (g *Graph) forceMerge() error {
	if g.builder != nil {
		g.publishLayer() // a mid-batch Seal flushes the pending batch first
	}
	v := g.view.Load()
	if len(v.layers) == 0 {
		return nil
	}
	nodes, edges, labels, indexKeys := g.unionRecords()
	snap, err := engine.Seal(nodes, edges, labels, indexKeys)
	if err != nil {
		return err
	}
	baseKeys := make(map[string]NodeID, len(nodes))
	g.nodes = make(map[string]nodeRef, len(nodes))
	for i, n := range nodes {
		baseKeys[n.Key] = NodeID(i)
		g.nodes[n.Key] = nodeRef{id: NodeID(i), live: true, label: n.Label, props: n.Props}
	}
	g.nodeTotal = len(nodes)
	g.edges = make(map[delta.EdgeKey]edgeRef, len(edges))
	g.edgeTotal = 0
	for i, er := range edges {
		k := delta.EdgeKey{SrcKey: nodes[er.Src].Key, DstKey: nodes[er.Dst].Key, Label: labels[er.LabelID]}
		ref := g.edges[k]
		ref.winner = EdgeID(i)
		ref.n++
		g.edges[k] = ref
		g.edgeTotal++
	}
	g.nextNode, g.nextEdge = NodeID(len(nodes)), EdgeID(len(edges))
	// The label mirror carries EDGE labels only (v0.6 labIDs parity:
	// internLabel ran on resolveEdge alone). The merged label dictionary is
	// a superset — delta Layers also intern node labels (D6) — so filtering
	// by edge usage keeps DeleteEdge's "label never used" no-op exact.
	g.labels = make(map[string]bool, len(labels))
	for _, er := range edges {
		g.labels[labels[er.LabelID]] = true
	}
	g.view.Store(&genView{
		base: snap, baseKeys: baseKeys, layers: nil,
		lsn: v.lsn, nodes: g.nodeTotal, edges: g.edgeTotal,
	})
	return nil
}

// Seal is an explicit merge: unmerged layers fold into a fresh immutable
// base (IDs may be renumbered, R1) and — when a WAL is attached — a
// checkpoint follows (watermark snapshot + manifest + WAL reset). Idempotent:
// on an already-merged graph it is at most a checkpoint. Writes after Seal
// are legal (they open a new generation); this method no longer makes the
// graph read-only. Seal is a write-path method guarded by the writer
// discipline: in wrapper mode it must run on the writer goroutine (debug
// builds return ErrNotWriter otherwise), and concurrent-safe only via the
// writer handle or single-goroutine discipline — it rebuilds writer-private
// state.
func (g *Graph) Seal() error {
	if err := g.checkWriter(); err != nil {
		return err
	}
	if g.wal != nil {
		if err := g.flushPending(); err != nil {
			return err
		}
	}
	if err := g.forceMerge(); err != nil {
		return err
	}
	if g.wal == nil {
		return nil
	}
	return g.checkpoint(g.wal.LastLSN())
}

// Compact is the explicit compaction entry point (spec §6): fold every
// unmerged layer into a fresh immutable base and — when a WAL is attached —
// checkpoint (watermark snapshot + manifest + WAL reset). Spec §3.1's
// "deep compaction renumbering" is absorbed by the merge itself: folding
// already rebuilds node and edge IDs densely (R1), so Compact runs the
// exact same pipeline as Seal and the two are semantically equivalent.
// Like Seal it is a write-path method guarded by the writer discipline
// (ErrNotWriter off-writer in debug builds): concurrent-safe only via the
// writer handle or single-goroutine discipline — it rebuilds
// writer-private state.
func (g *Graph) Compact() error {
	return g.Seal()
}

// checkpoint persists the merged state: watermark snapshot + atomic manifest
// + WAL reset + old-watermark cleanup. The WAL stays open for the session.
func (g *Graph) checkpoint(lsn uint64) error {
	snap := g.snapshotFile(lsn)
	if err := g.SaveFile(snap); err != nil {
		return err
	}
	if err := wal.WriteManifest(g.walDir, wal.Manifest{
		Snapshot: filepath.Base(snap),
		Segment:  g.wal.Segment(),
		LSN:      lsn,
	}); err != nil {
		return err
	}
	if err := g.wal.Reset(lsn); err != nil && !errors.Is(err, wal.ErrClosed) {
		return err
	}
	g.cleanupSnapshots(filepath.Base(snap))
	return nil
}

// ---- snapshot I/O -------------------------------------------------------------

// Save writes the whole graph to w: unconditionally exports the current
// logical view (base ∪ layers, tombstones applied — merge-on-demand into a
// temporary record set, nothing published). Property-index declarations —
// effective and pending — are persisted too (store v3), so Load restores
// both the graph and its lookup indexes.
func (g *Graph) Save(w io.Writer) error {
	nodes, edges, labels, indexKeys := g.unionRecords()
	sn := make([]store.Node, len(nodes))
	for i, n := range nodes {
		sn[i] = store.Node{Key: n.Key, Label: n.Label, Props: n.Props}
	}
	se := make([]store.Edge, 0, len(edges))
	for _, e := range edges {
		se = append(se, store.Edge{
			SrcKey: nodes[e.Src].Key,
			DstKey: nodes[e.Dst].Key,
			Label:  labels[e.LabelID],
			Props:  e.Props,
		})
	}
	return store.Encode(w, store.Data{Nodes: sn, Edges: se, IndexKeys: indexKeys})
}

// SaveFile saves to a path: temp file + fsync + atomic rename, avoiding
// half-written files.
func (g *Graph) SaveFile(path string) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if err := g.Save(f); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// Load restores a graph from a snapshot and merges it into the initial base
// (equivalent to a merge: the graph is immediately queryable AND writable).
// Index declarations persisted by the snapshot (store v3) are rebuilt, so
// LookupByProperty works immediately (v1/v2 files carry none).
func Load(r io.Reader) (*Graph, error) {
	g, err := loadGraph(r)
	if err != nil {
		return nil, err
	}
	return g, nil
}

func loadGraph(r io.Reader) (*Graph, error) {
	data, err := store.Decode(r)
	if err != nil {
		return nil, err
	}
	nodes := make([]engine.NodeRec, 0, len(data.Nodes))
	idx := make(map[string]uint32, len(data.Nodes))
	for _, n := range data.Nodes {
		rec := engine.NodeRec{Key: n.Key, Label: n.Label, Props: n.Props}
		if i, ok := idx[n.Key]; ok { // duplicate keys: last write wins (AddNode semantics)
			nodes[i] = rec
			continue
		}
		idx[n.Key] = uint32(len(nodes))
		nodes = append(nodes, rec)
	}
	labels := make([]string, 0, 8)
	labID := make(map[string]uint32, 8)
	intern := func(name string) uint32 {
		if id, ok := labID[name]; ok {
			return id
		}
		id := uint32(len(labels))
		labID[name] = id
		labels = append(labels, name)
		return id
	}
	edges := make([]engine.EdgeRec, 0, len(data.Edges))
	for _, e := range data.Edges {
		s, okS := idx[e.SrcKey]
		d, okD := idx[e.DstKey]
		if !okS || !okD {
			return nil, fmt.Errorf("vectra: snapshot edge %s->%s references unknown node", e.SrcKey, e.DstKey)
		}
		edges = append(edges, engine.EdgeRec{
			Src: s, Dst: d, LabelID: intern(e.Label), Props: e.Props,
		})
	}
	return graphFromEngine(nodes, edges, labels, data.IndexKeys)
}

// graphFromEngine builds a Graph whose initial generation is the merged base
// of the given records (writer mirrors and ratchets seeded from the base).
func graphFromEngine(nodes []engine.NodeRec, edges []engine.EdgeRec, labels []string, indexKeys []string) (*Graph, error) {
	snap, err := engine.Seal(nodes, edges, labels, indexKeys)
	if err != nil {
		return nil, err
	}
	g := New()
	baseKeys := make(map[string]NodeID, len(nodes))
	g.nodes = make(map[string]nodeRef, len(nodes))
	for i, n := range nodes {
		baseKeys[n.Key] = NodeID(i)
		g.nodes[n.Key] = nodeRef{id: NodeID(i), live: true, label: n.Label, props: n.Props}
	}
	g.edges = make(map[delta.EdgeKey]edgeRef, len(edges))
	for i, er := range edges {
		k := delta.EdgeKey{SrcKey: nodes[er.Src].Key, DstKey: nodes[er.Dst].Key, Label: labels[er.LabelID]}
		ref := g.edges[k]
		ref.winner = EdgeID(i)
		ref.n++
		g.edges[k] = ref
	}
	g.nodeTotal, g.edgeTotal = len(nodes), len(edges)
	g.nextNode, g.nextEdge = NodeID(len(nodes)), EdgeID(len(edges))
	// Edge labels only — the label mirror feeds DeleteEdge's no-op early
	// exit and callers may pass a node-label superset (Clone's union).
	g.labels = make(map[string]bool, len(labels))
	for _, er := range edges {
		g.labels[labels[er.LabelID]] = true
	}
	g.view.Store(&genView{
		base: snap, baseKeys: baseKeys,
		nodes: g.nodeTotal, edges: g.edgeTotal,
	})
	return g, nil
}

// LoadFile restores a graph from a path.
func LoadFile(path string) (*Graph, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Load(f)
}

// IndexProperty registers a string property key to index. Declarations made
// while layers are unmerged (or on a merged graph) are pending until the
// next merge (spec §3.3 R14): until then LookupBy* answers along the old
// base index plus a line scan of the delta (ok=true), and Seal folds the
// declaration into the base index. Idempotent; unregistered keys yield
// ok=false in LookupByProperty.
func (g *Graph) IndexProperty(key string) *Graph {
	if g.indexDeclared(key) {
		return g
	}
	g.ensureBuilder()
	g.builder.IndexProperty(key)
	g.bIndex = append(g.bIndex, key)
	if !g.batching {
		g.publishLayer()
	}
	return g
}

// indexDeclared reports whether key is already an effective (base) or
// pending (layer/batch) index declaration.
func (g *Graph) indexDeclared(key string) bool {
	v := g.view.Load()
	if _, ok := v.base.LookupIndex(key, ""); ok {
		return true
	}
	for _, l := range v.layers {
		for _, k := range l.IndexKeys() {
			if k == key {
				return true
			}
		}
	}
	return slices.Contains(g.bIndex, key)
}

// IndexedPropertyKeys returns the property-index declarations: the merged
// base's effective keys plus pending declarations from unmerged layers
// (sorted; nil when none). It answers from the published generation only —
// a declaration made inside a running Batch becomes visible at its boundary
// like every other batch write — which keeps it lock-free. Consumers that
// persist or hand over a graph (snapshot services, clone-and-extend flows)
// use it to preserve index configuration.
func (g *Graph) IndexedPropertyKeys() []string {
	v := g.view.Load()
	seen := make(map[string]bool)
	var out []string
	add := func(k string) {
		if !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	for _, k := range v.base.IndexedKeys() {
		add(k)
	}
	for _, l := range v.layers {
		for _, k := range l.IndexKeys() {
			add(k)
		}
	}
	slices.Sort(out)
	return out
}

// Clone deep-copies into a graph that can keep taking writes: the union
// records (base ∪ layers) build the clone's fresh base, so the clone starts
// fully merged and writable. Props maps share references per the sharing
// contract (relies on "replace, don't mutate" discipline). The server
// module's hot-swap publisher uses it to clone the live graph.
func (g *Graph) Clone() *Graph {
	nodes, edges, labels, indexKeys := g.unionRecords()
	c, err := graphFromEngine(nodes, edges, labels, indexKeys)
	if err != nil { // engine.Seal of valid records cannot fail; fail loud over corrupt state
		panic(fmt.Sprintf("vectra: clone merge: %v", err))
	}
	return c
}

// ---- property lookups (three-source with pending semantics) -------------------

// LookupByProperty finds nodes whose string property equals value. Keys with
// an effective base index answer from it (delta overrides filtered through
// the winner resolution); pending keys answer via line scan (R14); unknown
// keys return ok=false. Hits are sorted by NodeID ascending.
func (g *Graph) LookupByProperty(key, value string) ([]Node, bool) {
	return g.lookupNodes(key,
		func(v *genView) ([]uint32, bool) { return v.base.LookupIndex(key, value) },
		func(props map[string]any) bool {
			s, ok := props[key].(string)
			return ok && s == value
		})
}

// LookupByNumber finds nodes whose numeric property equals v. Numbers of any
// Go/JSON width normalize to float64 in the index, so int-written and
// round-tripped values match alike.
func (g *Graph) LookupByNumber(key string, v float64) ([]Node, bool) {
	return g.lookupNodes(key,
		func(gv *genView) ([]uint32, bool) { return gv.base.LookupNumber(key, v) },
		func(props map[string]any) bool {
			f, ok := toFloat64(props[key])
			return ok && f == v
		})
}

// LookupByNumberRange finds nodes whose numeric property lies in [min, max]
// (inclusive; nil leaves that side unbounded). int64 values beyond 2^53 lose
// precision (inherent to the JSON number model).
func (g *Graph) LookupByNumberRange(key string, min, max *float64) ([]Node, bool) {
	return g.lookupNodes(key,
		func(gv *genView) ([]uint32, bool) { return gv.base.LookupNumberRange(key, min, max) },
		func(props map[string]any) bool {
			f, ok := toFloat64(props[key])
			if !ok {
				return false
			}
			if min != nil && f < *min {
				return false
			}
			if max != nil && f > *max {
				return false
			}
			return true
		})
}

// LookupByBool finds nodes whose boolean property equals v.
func (g *Graph) LookupByBool(key string, v bool) ([]Node, bool) {
	return g.lookupNodes(key,
		func(gv *genView) ([]uint32, bool) { return gv.base.LookupBool(key, v) },
		func(props map[string]any) bool {
			b, ok := props[key].(bool)
			return ok && b == v
		})
}

// lookupNodes is the shared three-source lookup: base hits (from the index
// when effective, or a full base scan for pending keys) overridden by layer
// records and carved by layer tombstones, then filtered by match over the
// winner's props.
func (g *Graph) lookupNodes(key string, baseIndex func(*genView) ([]uint32, bool), match func(map[string]any) bool) ([]Node, bool) {
	return lookupNodesOf(g.view.Load(), key, baseIndex, match)
}

// lookupNodesOf is lookupNodes pinned to one generation (the Cypher
// execution source in view.go answers anchors against exactly the
// generation its rows came from).
func lookupNodesOf(v *genView, key string, baseIndex func(*genView) ([]uint32, bool), match func(map[string]any) bool) ([]Node, bool) {
	ids, effective := baseIndex(v)
	pending := false
	for _, l := range v.layers {
		for _, k := range l.IndexKeys() {
			if k == key {
				pending = true
				break
			}
		}
	}
	if !effective && !pending {
		return nil, false
	}
	type hit struct {
		id NodeID
		n  Node
	}
	m := make(map[string]hit)
	if effective {
		for _, id := range ids {
			rec, ok := v.base.NodeInfo(id)
			if !ok {
				continue
			}
			m[rec.Key] = hit{NodeID(id), Node{ID: NodeID(id), Key: rec.Key, Label: rec.Label, Props: rec.Props}}
		}
	} else {
		for id := 0; id < v.base.NodeCount(); id++ {
			rec, _ := v.base.NodeInfo(uint32(id))
			m[rec.Key] = hit{NodeID(id), Node{ID: NodeID(id), Key: rec.Key, Label: rec.Label, Props: rec.Props}}
		}
	}
	for _, l := range v.layers { // oldest → newest: overrides replace, tombstones carve
		for k := range l.NodeTombstones() {
			delete(m, k)
		}
		for i := range l.Nodes() {
			rec := l.Nodes()[i]
			id := NodeID(l.NodeIDAt(i))
			m[rec.Key] = hit{id, Node{ID: id, Key: rec.Key, Label: rec.Label, Props: rec.Props}}
		}
	}
	out := make([]Node, 0, len(m))
	for _, h := range m {
		if match(h.n.Props) {
			out = append(out, h.n)
		}
	}
	slices.SortFunc(out, func(a, b Node) int { return cmp.Compare(a.ID, b.ID) })
	return out, true
}

// toFloat64 normalizes any numeric Go value (engine.toFloat's contract).
func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	}
	return 0, false
}

// ExportSnapshot serializes the current logical view (base ∪ layers union —
// export works with unmerged layers too; the server module's FSM snapshot
// uses it on the live graph). The format matches Save (CRC included).
func (g *Graph) ExportSnapshot() ([]byte, error) {
	var buf bytes.Buffer
	if err := g.Save(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ImportSnapshot builds a graph from snapshot bytes: the records merge into
// the initial base (immediately queryable and writable again). The server
// FSM's Restore path uses it to rebuild the live graph.
func ImportSnapshot(data []byte) (*Graph, error) {
	return loadGraph(bytes.NewReader(data))
}
