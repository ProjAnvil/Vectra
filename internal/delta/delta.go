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
	"sync/atomic" // atomic.Pointer only — the lazy Layer edge-ID index (A8 allows atomics)
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
// Returned slices and maps — Nodes, Edges, Labels, IndexKeys, FTSKeys,
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
	nodes     []NodeRec        // live nodes, first-write slot order
	nodeIdx   map[string]int   // key -> index into nodes
	edges     []EdgeRec        // live edges, append order (dead slots dropped)
	nodeTomb  map[string]bool  // base node keys to shadow
	edgeTomb  map[EdgeKey]bool // base edge keys to shadow
	labels    []string         // labels introduced this layer, dedup, stable
	indexKeys []string         // pending index declarations
	ftsKeys   []string         // pending full-text index declarations (2026-10-26 FTS plan)

	nodeIDs   map[string]uint32 // key -> writer-assigned ID (BuildWithIDs only)
	nodeIDIdx map[uint32]int    // writer-assigned ID -> index into nodes
	edgeIDs   []uint32          // writer-assigned ID per live edge, append order
	// edgeIDIdx maps writer-assigned edge ID -> index into edges, built
	// lazily on the first FindEdgeID: write bursts cut layer after layer
	// without ever resolving an edge by ID, and concurrent first-readers
	// racing the build produce identical maps (atomic publish, no locks).
	edgeIDIdx atomic.Pointer[map[uint32]int]
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

// FTSKeys returns the pending full-text index declarations (2026-10-26 FTS
// plan), harvested by the merger at the next merge like IndexKeys. A key may
// appear here and in IndexKeys — the kinds coexist, never reject.
func (l *Layer) FTSKeys() []string { return l.ftsKeys }

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
// The ID index is built on first use (see the edgeIDIdx field note); misses
// on plain-Build layers.
func (l *Layer) FindEdgeID(id uint32) (int, bool) {
	m := l.edgeIDIdx.Load()
	if m == nil {
		if l.edgeIDs == nil {
			return 0, false
		}
		built := make(map[uint32]int, len(l.edgeIDs))
		for i, eid := range l.edgeIDs {
			built[eid] = i
		}
		l.edgeIDIdx.Store(&built)
		m = &built
	}
	i, ok := (*m)[id]
	return i, ok
}

// nodeState is one node slot in the Builder's append-only node area. A
// deleted slot keeps its position: UpsertNode on the same key revives it in
// place, the way vectra reuses the node ID.
type nodeState struct {
	rec     NodeRec
	deleted bool // in-layer tombstone, revivable
}

// Builder accumulates one write batch (a future micro-layer). It is private
// to the single writer (spec D5) and not safe for concurrent use. Every
// method copies incoming props maps, so callers may reuse their maps
// afterwards. After Build the Builder is frozen: write methods return
// ErrSealed and a second Build panics. Builders come from NewBuilder.
//
// The edge area is a pair of parallel arrays — records plus a dead bitmap —
// instead of a flagged struct: Build's no-dead-slots fast path can then
// adopt the record slice wholesale (types already match), which keeps
// append-only batches copy-free.
type Builder struct {
	nodes        []nodeState
	byKey        map[string]int    // node key -> slot in nodes
	deadN        int               // deleted node slots (live = len(nodes)-deadN)
	edges        []EdgeRec         // append order; dead slots keep their positions
	edgeDead     []bool            // parallel to edges: in-layer tombstone (fold, delete, cascade)
	edgeIdx      map[EdgeKey][]int // live edge slots per key, append order
	edgeIdxBuilt bool              // false while only appends happened (lazy index)
	deadE        int               // deleted edge slots

	labels    []string // first-seen order, deduped (nodes and edges)
	labelSeen map[string]bool
	indexKeys []string // pending index declarations
	ftsKeys   []string // pending full-text index declarations (2026-10-26 FTS plan)

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

// ensureEdgeIdx materializes the per-key index over the current live edge
// slots. appendEdge skips index maintenance while nothing needs it — the
// index (a map insert plus a small allocation per key) dominated the per-op
// cost of append-only batches — so it is built lazily on the first
// fold/delete/slots use and maintained eagerly afterwards.
func (b *Builder) ensureEdgeIdx() {
	if b.edgeIdxBuilt {
		return
	}
	b.edgeIdxBuilt = true
	for i := range b.edges {
		if b.edgeDead[i] {
			continue
		}
		r := b.edges[i]
		k := EdgeKey{SrcKey: r.SrcKey, DstKey: r.DstKey, Label: r.Label}
		b.edgeIdx[k] = append(b.edgeIdx[k], i)
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
// (vectra.appendEdge): append the record and register it under its key
// (index maintenance is skipped while the lazy index is unbuilt).
func (b *Builder) appendEdge(srcKey, dstKey, label string, props map[string]any) {
	b.edges = append(b.edges, EdgeRec{SrcKey: srcKey, DstKey: dstKey, Label: label, Props: copyProps(props)})
	b.edgeDead = append(b.edgeDead, false)
	if b.edgeIdxBuilt {
		k := EdgeKey{SrcKey: srcKey, DstKey: dstKey, Label: label}
		b.edgeIdx[k] = append(b.edgeIdx[k], len(b.edges)-1)
	}
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

// AddEdgeValidated is AddEdge for endpoints the CALLER has already validated
// against the whole visible state (the root facade does, per the package
// doc): the record is appended without consulting this builder's node
// registry. The root uses it so published-resident endpoints need not be
// materialized into the layer — edge records are self-contained (string
// endpoints), resolution happens at read time, so the lift would only
// duplicate published state and cost a map insert per first touch.
func (b *Builder) AddEdgeValidated(srcKey, dstKey, label string, props map[string]any) error {
	if b.built {
		return ErrSealed
	}
	b.internLabel(label)
	b.appendEdge(srcKey, dstKey, label, props)
	return nil
}

// HasNode reports whether key is a live node of this builder (written or
// materialized this window). The root uses it to lift each published-
// resident endpoint at most once per builder.
func (b *Builder) HasNode(key string) bool {
	i, ok := b.byKey[key]
	return ok && !b.nodes[i].deleted
}

// EdgeSlots reports the live in-builder parallels of k: how many and the
// slot index of the most recent one (the fold target). Dead slots are not
// counted — deletes and folds drop them from the index.
func (b *Builder) EdgeSlots(k EdgeKey) (n, last int, ok bool) {
	b.ensureEdgeIdx()
	ids := b.edgeIdx[k]
	if len(ids) == 0 {
		return 0, 0, false
	}
	return len(ids), ids[len(ids)-1], true
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
	return b.upsertEdgeFold(srcKey, dstKey, label, props)
}

// UpsertEdgeFoldValidated is UpsertEdgeFold for pre-validated endpoints
// (see AddEdgeValidated).
func (b *Builder) UpsertEdgeFoldValidated(srcKey, dstKey, label string, props map[string]any) error {
	if b.built {
		return ErrSealed
	}
	return b.upsertEdgeFold(srcKey, dstKey, label, props)
}

func (b *Builder) upsertEdgeFold(srcKey, dstKey, label string, props map[string]any) error {
	b.internLabel(label)
	k := EdgeKey{SrcKey: srcKey, DstKey: dstKey, Label: label}
	b.ensureEdgeIdx()
	ids := b.edgeIdx[k]
	if len(ids) == 0 {
		b.appendEdge(srcKey, dstKey, label, props)
	} else {
		for _, id := range ids[:len(ids)-1] { // drop extra parallels, keep the most recent
			b.edgeDead[id] = true
			b.deadE++
		}
		keep := ids[len(ids)-1]
		b.edges[keep].Props = copyProps(props) // wholesale replacement
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
	b.ensureEdgeIdx()
	n := 0
	if ids, ok := b.edgeIdx[k]; ok {
		for _, id := range ids {
			b.edgeDead[id] = true
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
	b.ensureEdgeIdx()
	removed := 0
	// The edge cascade runs whether or not key was written in this builder:
	// endpoints may be published-resident and unmaterialized (AddEdgeValidated
	// deliberately skips materializing them), yet their in-builder edges must
	// still die with the node.
	for k, ids := range b.edgeIdx { // deleting during range is safe in Go
		if k.SrcKey != key && k.DstKey != key {
			continue
		}
		for _, id := range ids {
			b.edgeDead[id] = true
		}
		b.deadE += len(ids)
		removed += len(ids)
		b.edgeTomb[k] = true // base may hold a copy of this key
		delete(b.edgeIdx, k)
	}
	if i, ok := b.byKey[key]; ok && !b.nodes[i].deleted {
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

// IndexFullText declares a property key to full-text index at the next merge
// (2026-10-26 FTS plan): same carriage contract as IndexProperty —
// idempotent, harvested by the merger, a no-op after Build. A key may be
// declared both ways; the kinds coexist independently.
func (b *Builder) IndexFullText(key string) {
	if b.built {
		return
	}
	for _, k := range b.ftsKeys {
		if k == key {
			return
		}
	}
	b.ftsKeys = append(b.ftsKeys, key)
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
//
// Fast path: a batch whose edge area has no dead slots adopts the builder's
// record slice and the caller's edgeIDs slice wholesale (slots are already
// live-aligned, and both builders and ID slices are single-use) — no
// per-record copy, no slot remap. The ID→index lookup maps build lazily on
// first use instead of per layer: write bursts cut layer after layer
// without ever resolving an edge by ID.
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
		nodeTomb:  b.nodeTomb,
		edgeTomb:  b.edgeTomb,
		labels:    b.labels,
		indexKeys: b.indexKeys,
		ftsKeys:   b.ftsKeys,
	}
	l.nodes = make([]NodeRec, 0, len(b.nodes)-b.deadN)
	for _, ns := range b.nodes {
		if ns.deleted {
			continue
		}
		l.nodeIdx[ns.rec.Key] = len(l.nodes)
		l.nodes = append(l.nodes, ns.rec)
	}
	if b.deadE == 0 {
		// adopt: every slot is live, so the builder's records and the
		// caller's per-slot IDs are already the layer's arrays
		l.edges = b.edges
		l.edgeIDs = edgeIDs
	} else {
		var slot2live []int
		if edgeIDs != nil {
			slot2live = make([]int, len(b.edges))
		}
		l.edges = make([]EdgeRec, 0, len(b.edges)-b.deadE)
		for i := range b.edges {
			if b.edgeDead[i] {
				if slot2live != nil {
					slot2live[i] = -1
				}
				continue
			}
			if slot2live != nil {
				slot2live[i] = len(l.edges)
			}
			l.edges = append(l.edges, b.edges[i])
		}
		if edgeIDs != nil {
			l.edgeIDs = make([]uint32, len(l.edges))
			for slot, live := range slot2live {
				if live < 0 {
					continue
				}
				l.edgeIDs[live] = edgeIDs[slot]
			}
		}
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
	return l
}
