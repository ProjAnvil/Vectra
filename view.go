package vectra

// Three-source traversal wiring (v0.7 spec §3.2, Task 4): the adapters that
// turn one immutable genView (base ∪ unmerged layers) into the row surfaces
// the query algorithms consume. Layer adapters live here — the root facade
// owns the delta layers, so internal/engine stays delta-agnostic; traverse
// and cypher see only their local interfaces, which engine.View satisfies
// structurally. Everything built here is per-query over one published
// generation: construction reads immutable state only, so the results are
// safe for unsynchronized concurrent use (and BFS's parallel expansion).

import (
	"slices"

	"github.com/projanvil/vectra/internal/csr"
	"github.com/projanvil/vectra/internal/delta"
	"github.com/projanvil/vectra/internal/engine"
	tr "github.com/projanvil/vectra/internal/traverse"
)

// snapRows is the empty-generation RowSource: with no unmerged layers the
// base CSR is the whole graph, so traversal/cypher keep the slice-zip hot
// path (FastAdjacency always true) and NodeCount stays the base count.
type snapRows struct{ base *engine.Snapshot }

func (s snapRows) NodeCount() int { return s.base.NodeCount() }

func (s snapRows) FastAdjacency(dir int) (csr.Adjacency, bool) {
	if dir == engine.ViewDirIn {
		return s.base.AdjacencyR(), true
	}
	return s.base.AdjacencyF(), true
}

// Rows zips the base row (used by NeighborsOf's unified code path; the BFS
// hot loop never calls it — FastAdjacency routes it to the CSR rower).
func (s snapRows) Rows(u uint32, dir int) []csr.AdjRow {
	adj := s.base.AdjacencyF()
	if dir == engine.ViewDirIn {
		adj = s.base.AdjacencyR()
	}
	tg, eids, lbs := adj.Row(u)
	rows := make([]csr.AdjRow, len(tg))
	for i := range tg {
		rows[i] = csr.AdjRow{Target: tg[i], EdgeID: eids[i], LabelID: lbs[i]}
	}
	return rows
}

// rows returns the traversal row source of this generation: the base itself
// when nothing is unmerged (A1: zero View construction on the hot path), or
// the three-source engine.View otherwise — routed through the graph's
// per-generation View cache so repeated queries over one generation build
// it once (see engineViewOf and genView.cached).
func (g *Graph) rowsOf(v *genView) tr.RowSource {
	if len(v.layers) == 0 {
		return snapRows{base: v.base}
	}
	return g.engineViewOf(v)
}

// layerSource adapts one immutable micro-layer into an engine.DeltaSource:
// adjacency buckets keyed by the generation's node IDs (writer-assigned, per
// the ID ratchet — override/revive keeps the original ID, so a key's ID is
// stable for the generation's lifetime), plus the base-edge and older-row
// kills derived from the layer's tombstones.
type layerSource struct {
	layer    *delta.Layer
	out      map[uint32][]csr.AdjRow // src node ID -> rows (local label IDs)
	in       map[uint32][]csr.AdjRow // dst node ID -> rows
	deadBase []uint32                // base edge IDs shadowed by this layer
	killed   []uint32                // older layers' record IDs shadowed here
}

func (s *layerSource) Rows(u uint32, dir int) []csr.AdjRow {
	if dir == engine.ViewDirIn {
		return s.in[u]
	}
	return s.out[u]
}

func (s *layerSource) Labels() []string        { return s.layer.Labels() }
func (s *layerSource) DeadBaseEdges() []uint32 { return s.deadBase }
func (s *layerSource) KilledEdgeIDs() []uint32 { return s.killed }

func (s *layerSource) Empty() bool {
	return len(s.out) == 0 && len(s.in) == 0 && len(s.deadBase) == 0 && len(s.killed) == 0
}

// viewBuild carries the incremental construction state of one generation's
// three-source View: everything engineView's loop accumulates across layers
// (the resolve memo, the cumulative live-record index, the per-layer
// sources, the widened ID space). The writer's prebuildView extends a
// previously built state across publishes — appending only the new layers'
// sources — so the per-publish cost stays O(new records) regardless of
// stack depth or volume; the background folder builds a fresh state for a
// folded prefix off the writer path; readers missing a slot build from
// scratch.
type viewBuild struct {
	builtTo int
	sources []engine.DeltaSource
	live    map[delta.EdgeKey][]uint32
	nodeID  map[string]NodeID
	baseLab map[string]uint32
	nodeN   int
}

func newViewBuild(v *genView) *viewBuild {
	baseLab := make(map[string]uint32, v.base.LabelCount())
	for i := range v.base.LabelCount() {
		baseLab[v.base.LabelName(uint32(i))] = uint32(i)
	}
	return &viewBuild{
		live:    make(map[delta.EdgeKey][]uint32),
		nodeID:  make(map[string]NodeID, 8),
		baseLab: baseLab,
		nodeN:   v.base.NodeCount(),
	}
}

// appendLayer folds v.layers[i]'s source into the build (verbatim from
// engineView's per-layer loop body; the cumulative live index and resolve
// memo carry across calls).
func (vb *viewBuild) appendLayer(v *genView, i int) {
	l := v.layers[i]
	s := &layerSource{
		layer: l,
		out:   make(map[uint32][]csr.AdjRow),
		in:    make(map[uint32][]csr.AdjRow),
	}
	labLocal := make(map[string]uint32, len(l.Labels()))
	for j, name := range l.Labels() {
		labLocal[name] = uint32(j)
	}
	resolve := func(key string) (uint32, bool) {
		if id, ok := vb.nodeID[key]; ok {
			return uint32(id), true
		}
		if n, ok := v.nodeWinner(key); ok {
			vb.nodeID[key] = n.ID
			return uint32(n.ID), true
		}
		return 0, false // dead endpoint: a live record cannot reference one (cascades), defensive skip
	}
	// widen the ID space to every layer node (ratchet IDs may exceed
	// base+Σcounts when a batch burned IDs on since-deleted nodes)
	for j := range l.Nodes() {
		if id := int(l.NodeIDAt(j)); id >= vb.nodeN {
			vb.nodeN = id + 1
		}
	}
	// bucket the layer's live records by winner-resolved endpoint IDs
	for j, rec := range l.Edges() {
		src, okS := resolve(rec.SrcKey)
		dst, okD := resolve(rec.DstKey)
		if !okS || !okD {
			continue
		}
		eid, lid := l.EdgeIDAt(j), labLocal[rec.Label]
		s.out[src] = append(s.out[src], csr.AdjRow{Target: dst, EdgeID: eid, LabelID: lid})
		s.in[dst] = append(s.in[dst], csr.AdjRow{Target: src, EdgeID: eid, LabelID: lid})
	}
	// resolve this layer's tombstones against everything older. Fold
	// transfer: the layer's own surviving record may carry an older
	// record's (winner) EdgeID, so own record IDs are collected first
	// and exempted from the kill list.
	own := make(map[delta.EdgeKey][]uint32, l.EdgeCount())
	for j, rec := range l.Edges() {
		k := delta.EdgeKey{SrcKey: rec.SrcKey, DstKey: rec.DstKey, Label: rec.Label}
		own[k] = append(own[k], l.EdgeIDAt(j))
	}
	for k := range l.EdgeTombstones() {
		for _, eid := range vb.live[k] { // older-layer parallels die
			if !slices.Contains(own[k], eid) {
				s.killed = append(s.killed, eid)
			}
		}
		delete(vb.live, k)
		sb, okS := v.baseKeys[k.SrcKey] // base parallels die (targeted row scan)
		db, okD := v.baseKeys[k.DstKey]
		lid, okL := vb.baseLab[k.Label]
		if !okS || !okD || !okL {
			continue
		}
		tg, eids, lbs := v.base.AdjacencyF().Row(uint32(sb))
		for x, t := range tg {
			if t == uint32(db) && lbs[x] == lid {
				s.deadBase = append(s.deadBase, eids[x])
			}
		}
	}
	for k, ids := range own {
		vb.live[k] = append(vb.live[k], ids...)
	}
	vb.sources = append(vb.sources, s)
	vb.builtTo++
}

// engineView builds the three-source View of this generation: one
// DeltaSource per layer (rows bucketed by node ID, tombstone effects
// resolved against the layers below), the union ID-space node count, and
// engine.NewView's joint label table on top. Cost is proportional to the
// unmerged delta (layer records + tombstone row scans), never to the base.
func (v *genView) engineView() *engine.View {
	vb := newViewBuild(v)
	for i := range v.layers {
		vb.appendLayer(v, i)
	}
	return engine.NewView(v.base, vb.sources, vb.nodeN)
}

// ---- Cypher's three-source execution surface ---------------------------------

// cypherSource implements cypher.Source over one generation: the engine.View
// for rows/labels, the genView's winner resolution for records, and the
// three-source lookup pipeline (base index + layer carve) for anchoring.
type cypherSource struct {
	gv *genView
	ev *engine.View
}

func (s cypherSource) NodeCount() int { return s.ev.NodeCount() }

func (s cypherSource) NodeInfo(id uint32) (engine.NodeRec, bool) {
	n, ok := s.gv.nodeByID(NodeID(id))
	if !ok {
		return engine.NodeRec{}, false
	}
	return engine.NodeRec{Key: n.Key, Label: n.Label, Props: n.Props}, true
}

func (s cypherSource) EdgeInfo(id uint32) (uint32, uint32, string, map[string]any, bool) {
	e, ok := s.gv.edgeByID(EdgeID(id))
	if !ok {
		return 0, 0, "", nil, false
	}
	return uint32(e.Src), uint32(e.Dst), e.Label, e.Props, true
}

func (s cypherSource) LabelCount() int                             { return s.ev.LabelCount() }
func (s cypherSource) LabelID(name string) (uint32, bool)          { return s.ev.LabelID(name) }
func (s cypherSource) FastAdjacency(dir int) (csr.Adjacency, bool) { return s.ev.FastAdjacency(dir) }
func (s cypherSource) Rows(u uint32, dir int) []csr.AdjRow         { return s.ev.Rows(u, dir) }

func (s cypherSource) LookupIndex(key, value string) ([]uint32, bool) {
	ns, ok := lookupNodesOf(s.gv, key,
		func(v *genView) ([]uint32, bool) { return v.base.LookupIndex(key, value) },
		func(props map[string]any) bool {
			sv, ok := props[key].(string)
			return ok && sv == value
		})
	return nodesToIDs(ns), ok
}

func (s cypherSource) LookupNumber(key string, value float64) ([]uint32, bool) {
	ns, ok := lookupNodesOf(s.gv, key,
		func(v *genView) ([]uint32, bool) { return v.base.LookupNumber(key, value) },
		func(props map[string]any) bool {
			f, ok := toFloat64(props[key])
			return ok && f == value
		})
	return nodesToIDs(ns), ok
}

func (s cypherSource) LookupBool(key string, value bool) ([]uint32, bool) {
	ns, ok := lookupNodesOf(s.gv, key,
		func(v *genView) ([]uint32, bool) { return v.base.LookupBool(key, value) },
		func(props map[string]any) bool {
			b, ok := props[key].(bool)
			return ok && b == value
		})
	return nodesToIDs(ns), ok
}

func nodesToIDs(ns []Node) []uint32 {
	ids := make([]uint32, len(ns))
	for i, n := range ns {
		ids[i] = uint32(n.ID)
	}
	return ids
}

// cypherRunViewOf returns the execution source for this generation: the base
// snapshot itself keeps Run's zero-indirection hot path; a generation with
// unmerged layers executes through RunView over the cached three-source
// View (see engineViewOf and genView.cached).
func (g *Graph) cypherRunViewOf(v *genView) (runView bool, src cypherSource) {
	if len(v.layers) == 0 {
		return false, cypherSource{}
	}
	return true, cypherSource{gv: v, ev: g.engineViewOf(v)}
}
