// Three-source traversal view (v0.7 spec §3.2): the merged base snapshot
// unioned with the unmerged delta layers, addressed entirely by node ID in
// the generation's ID space (the writer's ratchet: base IDs stay, layer
// records carry their assigned IDs, overrides keep the original). The base
// keeps its CSR; layer rows ride along as per-source adjacency buckets. This
// file defines the engine-side shapes only — the delta layer adapters are
// assembled by the root facade (engine must not import internal/delta), and
// the query algorithms consume the row surface through their own local
// interfaces (traverse must not import engine), both satisfied structurally
// by *View.

package engine

import "github.com/projanvil/vectra/internal/csr"

// Direction codes for Rows/FastAdjacency (traverse's DirOut/DirIn values;
// independent of cypher's three-valued direction enum).
const (
	ViewDirOut = 0
	ViewDirIn  = 1
)

// DeltaSource is the layer side of the three-source view: one immutable
// adapter per micro-layer (or any layer set), prebuilt by the root facade.
// Rows carry SOURCE-LOCAL label IDs (indices into Labels()); View remaps
// them into the joint label table it computes at construction.
//
// All methods must be safe for unsynchronized concurrent reads.
type DeltaSource interface {
	// Rows returns u's layer-resident adjacency rows in direction dir
	// (ViewDirOut/ViewDirIn): records live at this source's publication.
	// Newer sources' tombstones are NOT applied here — View does that via
	// KilledEdgeIDs.
	Rows(u uint32, dir int) []csr.AdjRow
	// Labels returns the label names this source introduced, deduplicated
	// in stable order; a row's LabelID indexes into this slice.
	Labels() []string
	// DeadBaseEdges returns the base edge IDs shadowed by this source's
	// edge tombstones (folds, deletes, cascades).
	DeadBaseEdges() []uint32
	// KilledEdgeIDs returns the older sources' row EdgeIDs shadowed by
	// this source's edge tombstones.
	KilledEdgeIDs() []uint32
	// Empty reports whether this source contributes nothing to adjacency
	// (no rows, no base kills, no row kills) — the View fast-path signal.
	Empty() bool
}

// View is the base-∪-layers traversal surface: per-node row iteration over
// both sources with tombstone shadowing, a joint label table (base labels
// first — base LabelIDs stay valid — then each source's new labels in
// order), and the A1 fast-path probe. Built once per query by the root
// facade from one immutable generation; safe for concurrent use.
type View struct {
	base      *Snapshot
	sources   []DeltaSource
	nodeCount int      // union ID-space size (arrays are sized by this)
	labels    []string // joint label table
	labelID   map[string]uint32
	remap     [][]uint32      // per source: local label ID -> joint label ID
	deadBase  map[uint32]bool // base edge ID -> shadowed (nil when none)
	killed    map[uint32]bool // layer row EdgeID -> shadowed (nil when none)
}

// NewView assembles the three-source view. nodeCount is the union ID-space
// size the caller computed (base node count widened to cover every layer
// node ID). The joint label table is built here: base labels in base order,
// then each source's labels in source order (first-seen wins).
func NewView(base *Snapshot, sources []DeltaSource, nodeCount int) *View {
	v := &View{base: base, sources: sources, nodeCount: nodeCount}
	v.labels = make([]string, 0, base.LabelCount())
	v.labelID = make(map[string]uint32, base.LabelCount())
	intern := func(name string) uint32 {
		if id, ok := v.labelID[name]; ok {
			return id
		}
		id := uint32(len(v.labels))
		v.labelID[name] = id
		v.labels = append(v.labels, name)
		return id
	}
	for i := range base.LabelCount() {
		intern(base.LabelName(uint32(i)))
	}
	if len(sources) > 0 {
		v.remap = make([][]uint32, len(sources))
		for si, s := range sources {
			local := s.Labels()
			m := make([]uint32, len(local))
			for li, name := range local {
				m[li] = intern(name)
			}
			v.remap[si] = m
		}
	}
	for _, s := range sources {
		if db := s.DeadBaseEdges(); len(db) > 0 {
			if v.deadBase == nil {
				v.deadBase = make(map[uint32]bool, len(db))
			}
			for _, eid := range db {
				v.deadBase[eid] = true
			}
		}
		if k := s.KilledEdgeIDs(); len(k) > 0 {
			if v.killed == nil {
				v.killed = make(map[uint32]bool, len(k))
			}
			for _, eid := range k {
				v.killed[eid] = true
			}
		}
	}
	return v
}

// NodeCount returns the union ID-space size (visited/depth array bound).
func (v *View) NodeCount() int { return v.nodeCount }

// LabelCount returns the joint label table size — the label-mask bound for
// label-filtered traversal and Cypher.
func (v *View) LabelCount() int { return len(v.labels) }

// LabelID resolves a label name in the joint table (false when absent).
func (v *View) LabelID(name string) (uint32, bool) {
	id, ok := v.labelID[name]
	return id, ok
}

// LabelName returns the joint-table name for a label ID.
func (v *View) LabelName(id uint32) string { return v.labels[id] }

// FastAdjacency reports the A1 fast path: when every source is empty AND
// the union ID space did not widen (a layer-resident node — even a
// node-only layer — keeps an ID outside the base CSR's range), the caller
// traverses the plain base CSR (slice-zip hot loop, zero View overhead) and
// the returned adjacency carries the whole graph.
func (v *View) FastAdjacency(dir int) (csr.Adjacency, bool) {
	if v.nodeCount != v.base.NodeCount() {
		return csr.Adjacency{}, false // layer-resident nodes: rows beyond the CSR
	}
	for _, s := range v.sources {
		if !s.Empty() {
			return csr.Adjacency{}, false
		}
	}
	if dir == ViewDirIn {
		return v.base.AdjacencyR(), true
	}
	return v.base.AdjacencyF(), true
}

// Rows returns u's visible adjacency rows in direction dir: the base CSR row
// minus source-shadowed edges, plus each source's rows minus rows a newer
// source shadows — layer LabelIDs remapped into the joint table. Base rows
// come first, then source rows in source order. The returned slice is
// freshly allocated (callers may hold it unsynchronized).
func (v *View) Rows(u uint32, dir int) []csr.AdjRow {
	var out []csr.AdjRow
	if int(u) < v.base.NodeCount() {
		adj := v.base.AdjacencyF()
		if dir == ViewDirIn {
			adj = v.base.AdjacencyR()
		}
		tg, eids, lbs := adj.Row(u)
		for i := range tg {
			if v.deadBase != nil && v.deadBase[eids[i]] {
				continue
			}
			out = append(out, csr.AdjRow{Target: tg[i], EdgeID: eids[i], LabelID: lbs[i]})
		}
	}
	for si, s := range v.sources {
		rows := s.Rows(u, dir)
		if len(rows) == 0 {
			continue
		}
		remap := v.remap[si]
		for _, r := range rows {
			if v.killed != nil && v.killed[r.EdgeID] {
				continue
			}
			r.LabelID = remap[r.LabelID]
			out = append(out, r)
		}
	}
	return out
}
