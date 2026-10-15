// Package delta implements the immutable micro-layer of the v0.7 delta
// overlay (spec §2.3/§3.1). A Builder privately accumulates one write batch
// with the exact write semantics of an unsealed vectra.Graph — upsert fold,
// delete-revive, cascade tombstones — addressed by self-contained string
// keys/labels instead of base LabelIDs (engine-side type translation is the
// merger's job); Build materializes the batch into a Layer that is frozen
// forever after and safe for unsynchronized concurrent reads by any number
// of goroutines.
//
// Composition contract for the generation view and merger (T3b/T5):
//
//   - A layer never sees the base. Everything that must affect base state
//     is exported as tombstone sets (NodeTombstones/EdgeTombstones) that
//     shadow the BASE only. Live layer records always win over the layer's
//     own tombstone of the same key: every in-layer edge kill (fold, delete,
//     cascade) records the key's base tombstone, because the layer cannot
//     know whether base holds a copy — and the surviving or re-added layer
//     record is what the visible state must show.
//   - A node revived by UpsertNode has its node tombstone cleared at write
//     time; revive does NOT resurrect cascaded edges (vectra semantics).
//   - Merge applies tombstones to base first, then overlays layer records
//     (node override by key, edge append). Stale tombstones — keys absent
//     from base — are dropped at merge.
//   - Writes that need base knowledge are the caller's: AddEdge and
//     UpsertEdgeFold validate endpoints against layer-live nodes only
//     (base-resident endpoints must be pre-validated by the writer), and
//     DeleteNode takes a baseEdges hook for base incident edges.
//
// The package is core: no dependencies outside the standard library, no
// goroutines, channels, or locks (v0.7 decisions D4/D5, acceptance A8).
package delta

import (
	"errors"
	"fmt"
)

// EdgeKey is the logical identity of an edge: the (src, dst, label) triple
// addressed by string keys. Layer records are self-contained — no base
// LabelID dependency; translating to engine label IDs happens at merge.
type EdgeKey struct {
	SrcKey string
	DstKey string
	Label  string
}

// NodeRec is one node record in a layer. Props follow the vectra sharing
// contract: the Builder copies maps on ingress, the Layer exposes internal
// references — consumers must replace wholesale, never mutate in place.
type NodeRec struct {
	Key   string
	Label string
	Props map[string]any
}

// EdgeRec is one edge record in a layer (same Props contract as NodeRec).
type EdgeRec struct {
	SrcKey string
	DstKey string
	Label  string
	Props  map[string]any
}

var (
	// ErrSealed is returned by every Builder write method after Build.
	ErrSealed = errors.New("delta: builder sealed by Build, mutation rejected")
	// ErrNodeNotFound is returned by AddEdge/UpsertEdgeFold when an endpoint
	// is not a live node of this layer (vectra.resolveEdge semantics;
	// base-resident endpoints are the writer's responsibility — package doc).
	ErrNodeNotFound = errors.New("delta: node key not found")
)

// Layer is the immutable materialization of one Builder batch (the write
// side's counterpart of a sealed snapshot). Once Build has returned, a Layer
// and everything reachable from it are never mutated again: all read methods
// are safe for unsynchronized concurrent use from any number of goroutines.
//
// Returned slices and maps — Nodes, Edges, Labels, IndexKeys,
// NodeTombstones, EdgeTombstones and the Props maps inside records — are
// internal references and must be treated as read-only (same sharing
// contract as a sealed vectra graph). Tombstones shadow the BASE only; live
// layer records win over the layer's own tombstones (package doc).
//
// Writer-assigned IDs: a Layer built via BuildWithIDs additionally carries
// one ID per live record (spec §3.1 EdgeID ratchet — the writer's numbering
// space, transferred onto override records and never reused for a new
// record). ID structures are nil for plain-Build layers; all ID accessors
// miss there.
type Layer struct {
	nodes     []NodeRec         // live nodes, first-write slot order
	nodeIdx   map[string]int    // key -> index into nodes
	edges     []EdgeRec         // live edges, append order (dead slots dropped)
	edgeIdx   map[EdgeKey][]int // live edge indices per key, append order
	nodeTomb  map[string]bool   // base node keys to shadow
	edgeTomb  map[EdgeKey]bool  // base edge keys to shadow
	labels    []string          // labels introduced this layer, dedup, stable
	indexKeys []string          // pending index declarations

	nodeIDs   map[string]uint32 // key -> writer-assigned ID (BuildWithIDs only)
	nodeIDIdx map[uint32]int    // writer-assigned ID -> index into nodes
	edgeIDs   []uint32          // writer-assigned ID per live edge, append order
	edgeIDIdx map[uint32]int    // writer-assigned ID -> index into edges
}

// LookupNode returns the layer's live record for key. Base shadowing and
// base lookup are the generation view's job (T3b): this answers only for
// nodes written in this layer and not since deleted.
func (l *Layer) LookupNode(key string) (NodeRec, bool) {
	i, ok := l.nodeIdx[key]
	if !ok {
		return NodeRec{}, false
	}
	return l.nodes[i], true
}

// NodeTombstones returns the base node keys this layer shadows (read-only).
func (l *Layer) NodeTombstones() map[string]bool { return l.nodeTomb }

// EdgeTombstones returns the base edge keys this layer shadows (read-only).
func (l *Layer) EdgeTombstones() map[EdgeKey]bool { return l.edgeTomb }

// Nodes returns the live node records in first-write order (read-only).
func (l *Layer) Nodes() []NodeRec { return l.nodes }

// Edges returns the live edge records in append order — folded and deleted
// parallels are not included (read-only).
func (l *Layer) Edges() []EdgeRec { return l.edges }

// Labels returns every label string this layer introduced, deduplicated in
// first-seen order. Unlike vectra.go (whose dictionary serves edge LabelIDs
// only) node labels are interned too: Labels() feeds the merge-side LabelID
// remap (spec §3.1 pending labels), and a node-only label is a harmless
// dictionary entry.
func (l *Layer) Labels() []string { return l.labels }

// IndexKeys returns the pending property-index declarations, harvested by
// the merger at the next merge (spec §3.3, R14).
func (l *Layer) IndexKeys() []string { return l.indexKeys }

// NodeCount returns the number of live node records.
func (l *Layer) NodeCount() int { return len(l.nodes) }

// EdgeCount returns the number of live edge records.
func (l *Layer) EdgeCount() int { return len(l.edges) }

// LookupNodeID returns the writer-assigned ID of key's live record (misses
// on plain-Build layers and on keys not live in this layer).
func (l *Layer) LookupNodeID(key string) (uint32, bool) {
	id, ok := l.nodeIDs[key]
	return id, ok
}

// NodeIDAt returns the writer-assigned ID of the live node at index i
// (aligned with Nodes(); panics on plain-Build layers — the root facade
// always builds with IDs).
func (l *Layer) NodeIDAt(i int) uint32 { return l.nodeIDs[l.nodes[i].Key] }

// FindNodeID resolves a writer-assigned node ID to its index into Nodes().
func (l *Layer) FindNodeID(id uint32) (int, bool) {
	i, ok := l.nodeIDIdx[id]
	return i, ok
}

// EdgeIDAt returns the writer-assigned ID of the live edge at index i
// (aligned with Edges(); panics on plain-Build layers — the root facade
// always builds with IDs).
func (l *Layer) EdgeIDAt(i int) uint32 { return l.edgeIDs[i] }

// FindEdgeID resolves a writer-assigned edge ID to its index into Edges().
func (l *Layer) FindEdgeID(id uint32) (int, bool) {
	i, ok := l.edgeIDIdx[id]
	return i, ok
}

// nodeState is one node slot in the Builder's append-only node area. A
// deleted slot keeps its position: UpsertNode on the same key revives it in
// place, the way vectra reuses the node ID.
type nodeState struct {
	rec     NodeRec
	deleted bool // in-layer tombstone, revivable
}

// edgeState is one edge slot in the Builder's append-only edge area.
// deleted=true is an in-layer tombstone (folded parallels, DeleteEdge,
// DeleteNode cascade); Build drops the slot — in-layer tombstones never
// surface, only base tombstones do.
type edgeState struct {
	rec     EdgeRec
	deleted bool
}

// Builder accumulates one write batch (a future micro-layer). It is private
// to the single writer (spec D5) and not safe for concurrent use. Every
// method copies incoming props maps, so callers may reuse their maps
// afterwards. After Build the Builder is frozen: write methods return
// ErrSealed and a second Build panics. Builders come from NewBuilder.
type Builder struct {
	nodes   []nodeState
	byKey   map[string]int // node key -> slot in nodes
	deadN   int            // deleted node slots (live = len(nodes)-deadN)
	edges   []edgeState
	edgeIdx map[EdgeKey][]int // live edge slots per key, append order
	deadE   int               // deleted edge slots

	labels    []string // first-seen order, deduped (nodes and edges)
	labelSeen map[string]bool
	indexKeys []string // pending index declarations

	nodeTomb map[string]bool // base node keys to shadow
	edgeTomb map[EdgeKey]bool

	built bool
}

// NewBuilder returns an empty Builder.
func NewBuilder() *Builder {
	return &Builder{
		byKey:     map[string]int{},
		edgeIdx:   map[EdgeKey][]int{},
		labelSeen: map[string]bool{},
		nodeTomb:  map[string]bool{},
		edgeTomb:  map[EdgeKey]bool{},
	}
}

// copyProps is vectra.go's copyProps verbatim: nil stays nil, non-nil maps
// are copied shallow — nil and empty are distinct-but-equivalent.
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

// internLabel records a label string in first-seen order (idempotent).
func (b *Builder) internLabel(name string) {
	if b.labelSeen[name] {
		return
	}
	b.labelSeen[name] = true
	b.labels = append(b.labels, name)
}

// nodeLive reports whether key is a live (non-deleted) node of this layer.
func (b *Builder) nodeLive(key string) bool {
	i, ok := b.byKey[key]
	return ok && !b.nodes[i].deleted
}

// appendEdge is the shared tail of AddEdge/UpsertEdgeFold
// (vectra.appendEdge): append the record and register it under its key.
func (b *Builder) appendEdge(srcKey, dstKey, label string, props map[string]any) {
	b.edges = append(b.edges, edgeState{rec: EdgeRec{SrcKey: srcKey, DstKey: dstKey, Label: label, Props: copyProps(props)}})
	k := EdgeKey{SrcKey: srcKey, DstKey: dstKey, Label: label}
	b.edgeIdx[k] = append(b.edgeIdx[k], len(b.edges)-1)
}

// UpsertNode is vectra.AddNode: a new key appends; an existing key
// (including one written or deleted earlier in this layer) has label/props
// replaced wholesale (copyProps discipline — old keys are gone, nil clears
// the map). A deleted key revives in its original slot and its base node
// tombstone is cleared; revive does not resurrect cascaded edges.
func (b *Builder) UpsertNode(key, label string, props map[string]any) error {
	if b.built {
		return ErrSealed
	}
	b.internLabel(label)
	if i, ok := b.byKey[key]; ok {
		if b.nodes[i].deleted { // deleted then re-added: revive, reusing the slot
			b.nodes[i].deleted = false
			b.deadN--
			delete(b.nodeTomb, key)
		}
		b.nodes[i].rec = NodeRec{Key: key, Label: label, Props: copyProps(props)}
		return nil
	}
	b.byKey[key] = len(b.nodes)
	b.nodes = append(b.nodes, nodeState{rec: NodeRec{Key: key, Label: label, Props: copyProps(props)}})
	return nil
}

// AddEdge is vectra.AddEdge: pure append — parallel edges and self-loops
// both allowed, no fold, no base tombstone (a layer parallel stacks on top
// of base parallels). Endpoints are validated against this layer's live
// nodes only (ErrNodeNotFound, vectra.resolveEdge semantics); base-resident
// endpoints must be validated by the writer before calling (package doc).
func (b *Builder) AddEdge(srcKey, dstKey, label string, props map[string]any) error {
	if b.built {
		return ErrSealed
	}
	if !b.nodeLive(srcKey) || !b.nodeLive(dstKey) {
		return ErrNodeNotFound
	}
	b.internLabel(label)
	b.appendEdge(srcKey, dstKey, label, props)
	return nil
}

// UpsertEdgeFold is vectra.UpsertEdge: guarantees the (src, dst, label)
// triple is unique in the visible state. Live layer parallels of the key
// collapse onto the most recent one — props replaced wholesale, extra
// parallels become in-layer tombstones (vectra.go UpsertEdge). The key is
// also recorded as a base edge tombstone: the uniqueness contract must hold
// over base ∪ layer, the layer cannot see base, and shadowing every base
// parallel (a no-op when base holds none) beside the single surviving layer
// record is the same end state vectra.UpsertEdge produces. Endpoints are
// validated against layer-live nodes only (ErrNodeNotFound).
func (b *Builder) UpsertEdgeFold(srcKey, dstKey, label string, props map[string]any) error {
	if b.built {
		return ErrSealed
	}
	if !b.nodeLive(srcKey) || !b.nodeLive(dstKey) {
		return ErrNodeNotFound
	}
	b.internLabel(label)
	k := EdgeKey{SrcKey: srcKey, DstKey: dstKey, Label: label}
	ids := b.edgeIdx[k]
	if len(ids) == 0 {
		b.appendEdge(srcKey, dstKey, label, props)
	} else {
		for _, id := range ids[:len(ids)-1] { // drop extra parallels, keep the most recent
			b.edges[id].deleted = true
			b.deadE++
		}
		keep := ids[len(ids)-1]
		b.edges[keep].rec.Props = copyProps(props) // wholesale replacement
		b.edgeIdx[k] = []int{keep}
	}
	b.edgeTomb[k] = true
	return nil
}

// DeleteEdge is vectra.DeleteEdge addressed by EdgeKey: removes every live
// layer edge with that triple (idempotent — a repeat call deletes 0) and
// records the triple as a base edge tombstone so the deletion also holds
// over base. Unlike vectra.DeleteEdge there is no endpoint validation: the
// layer cannot verify base-resident endpoints, and a tombstone for a key
// that exists nowhere is a harmless no-op at merge. Returns the number of
// layer edges removed.
func (b *Builder) DeleteEdge(k EdgeKey) (int, error) {
	if b.built {
		return 0, ErrSealed
	}
	n := 0
	if ids, ok := b.edgeIdx[k]; ok {
		for _, id := range ids {
			b.edges[id].deleted = true
		}
		n = len(ids)
		b.deadE += n
		delete(b.edgeIdx, k)
	}
	b.edgeTomb[k] = true
	return n, nil
}

// DeleteNode is vectra.DeleteNode (DETACH DELETE): removes the layer node
// and cascade-deletes every live layer edge incident to it (src or dst;
// self-loops once), then records base tombstones — the node key itself, the
// cascaded in-layer edge keys (base may hold copies), and the base edges
// named by the baseEdges hook.
//
// baseEdges, when non-nil, is called once with the node key and must return
// the EdgeKeys of BASE edges incident to that key (both directions); the
// layer cannot derive them itself (spec §3.1: base entities are addressed
// by the writer via CSR row scans). A nil hook means "no base / base edges
// unknown" — specific base edges can still be shadowed later via
// DeleteEdge. The returned count is in-layer cascaded edges plus
// hook-named base edges; with a nil hook it is exactly vectra.DeleteNode's
// cascaded count.
//
// Unlike vectra.DeleteNode this never fails on a missing node: an overlay
// delete of a base-resident key is precisely the tombstone-recording path
// (the key was never written in this layer), and re-deleting an
// already-deleted key is an idempotent re-record.
func (b *Builder) DeleteNode(key string, baseEdges func(key string) []EdgeKey) (int, error) {
	if b.built {
		return 0, ErrSealed
	}
	removed := 0
	if i, ok := b.byKey[key]; ok && !b.nodes[i].deleted {
		for k, ids := range b.edgeIdx { // deleting during range is safe in Go
			if k.SrcKey != key && k.DstKey != key {
				continue
			}
			for _, id := range ids {
				b.edges[id].deleted = true
			}
			b.deadE += len(ids)
			removed += len(ids)
			b.edgeTomb[k] = true // base may hold a copy of this key
			delete(b.edgeIdx, k)
		}
		b.nodes[i].deleted = true
		b.deadN++
	}
	b.nodeTomb[key] = true
	if baseEdges != nil {
		base := baseEdges(key)
		for _, ek := range base {
			b.edgeTomb[ek] = true
		}
		removed += len(base)
	}
	return removed, nil
}

// IndexProperty declares a property key to index at the next merge
// (idempotent; pending declarations are harvested by the merger — spec
// §3.3, R14). A no-op after Build, mirroring vectra.IndexProperty.
func (b *Builder) IndexProperty(key string) {
	if b.built {
		return
	}
	for _, k := range b.indexKeys {
		if k == key {
			return
		}
	}
	b.indexKeys = append(b.indexKeys, key)
}

// Build freezes the batch into an immutable Layer (the write side's
// counterpart of vectra.compact): dead slots drop out, nodes keep
// first-write order, edges keep append order, indexes are rebuilt densely
// over the live records, and the base tombstone sets move into the Layer.
// The Builder is unusable afterwards — write methods return ErrSealed and a
// second Build panics (programmer error: Build has no error return).
func (b *Builder) Build() *Layer { return b.BuildWithIDs(nil, nil) }

// BuildWithIDs is Build carrying the writer's per-record IDs onto the live
// records: nodeIDs maps every node key ever written in this batch to its
// assigned ID (a live record whose key is missing is a programmer error and
// panics); edgeIDs holds one ID per edge SLOT in append order — every edge
// write that appends a slot consumes exactly one entry, folds reuse the
// surviving slot's entry — so len(edgeIDs) must equal the Builder's edge
// slot count (misalignment panics). Dead slots drop out together with their
// IDs; the surviving records' IDs are exposed via the Layer ID accessors.
// nil nodeIDs/edgeIDs build an ID-less Layer (plain Build).
func (b *Builder) BuildWithIDs(nodeIDs map[string]uint32, edgeIDs []uint32) *Layer {
	if b.built {
		panic("delta: Builder.Build called twice")
	}
	if edgeIDs != nil && len(edgeIDs) != len(b.edges) {
		panic(fmt.Sprintf("delta: edgeIDs misaligned: %d ids for %d edge slots", len(edgeIDs), len(b.edges)))
	}
	b.built = true
	l := &Layer{
		nodeIdx:   make(map[string]int, len(b.nodes)-b.deadN),
		edgeIdx:   make(map[EdgeKey][]int, len(b.edgeIdx)),
		nodeTomb:  b.nodeTomb,
		edgeTomb:  b.edgeTomb,
		labels:    b.labels,
		indexKeys: b.indexKeys,
	}
	l.nodes = make([]NodeRec, 0, len(b.nodes)-b.deadN)
	for _, ns := range b.nodes {
		if ns.deleted {
			continue
		}
		l.nodeIdx[ns.rec.Key] = len(l.nodes)
		l.nodes = append(l.nodes, ns.rec)
	}
	var slot2live []int
	if edgeIDs != nil {
		slot2live = make([]int, len(b.edges))
	}
	l.edges = make([]EdgeRec, 0, len(b.edges)-b.deadE)
	for i, es := range b.edges {
		if es.deleted {
			if slot2live != nil {
				slot2live[i] = -1
			}
			continue
		}
		k := EdgeKey{SrcKey: es.rec.SrcKey, DstKey: es.rec.DstKey, Label: es.rec.Label}
		l.edgeIdx[k] = append(l.edgeIdx[k], len(l.edges))
		if slot2live != nil {
			slot2live[i] = len(l.edges)
		}
		l.edges = append(l.edges, es.rec)
	}
	if nodeIDs != nil {
		l.nodeIDs = make(map[string]uint32, len(l.nodes))
		l.nodeIDIdx = make(map[uint32]int, len(l.nodes))
		for i, rec := range l.nodes {
			id, ok := nodeIDs[rec.Key]
			if !ok {
				panic(fmt.Sprintf("delta: nodeIDs missing live key %q", rec.Key))
			}
			l.nodeIDs[rec.Key] = id
			l.nodeIDIdx[id] = i
		}
	}
	if edgeIDs != nil {
		l.edgeIDs = make([]uint32, len(l.edges))
		l.edgeIDIdx = make(map[uint32]int, len(l.edges))
		for slot, live := range slot2live {
			if live < 0 {
				continue
			}
			id := edgeIDs[slot]
			l.edgeIDs[live] = id
			l.edgeIDIdx[id] = live
		}
	}
	return l
}
