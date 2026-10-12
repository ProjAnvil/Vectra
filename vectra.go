// Package vectra is an embedded in-memory graph engine: all-memory
// pointer-free CSR adjacency + write-side SCC compression + level-synchronous
// parallel BFS. It targets enterprise dependency-graph scenarios where
// "the graph always fits in single-machine memory": bulk import, lock-free
// concurrent reads after Seal, disk used only for snapshot recovery.
//
// Write semantics: AddNode is idempotent, UpsertEdge keeps the (src,dst,
// label) triple unique (weight-like props update in place), AddEdge is pure
// append; deletes are recorded as tombstones and compacted at Seal/export
// time — node and edge IDs may be renumbered by compaction.
//
// Layout: this package is the public API facade (writes, options, type
// translation); sealed state lives in internal/engine, query algorithms in
// internal/traverse, encode/decode in internal/store.
package vectra

import (
	"bytes"
	"errors"
	"io"
	"maps"
	"os"
	"slices"

	"github.com/projanvil/vectra/internal/engine"
	"github.com/projanvil/vectra/internal/store"
)

// NodeID / EdgeID are integer identifiers within the graph. After Seal all
// query structures are addressed by NodeID; string keys are converted once
// at the API boundary.
type NodeID uint32
type EdgeID uint32

var (
	ErrSealed       = errors.New("vectra: graph sealed, mutation rejected")
	ErrNotSealed    = errors.New("vectra: graph not sealed, query rejected")
	ErrNodeNotFound = errors.New("vectra: node key not found")
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
// separately by Cycles()).
type Stats struct {
	Nodes       int
	Edges       int
	SCCs        int
	CyclicNodes int
}

type edgeRec struct {
	src, dst NodeID
	label    uint32
	props    map[string]any
}

// edgeKey is the logical identity of an edge for UpsertEdge/DeleteEdge:
// the (src, dst, label) triple. EdgeID cannot serve as identity because
// Seal-time compaction renumbers IDs.
type edgeKey struct {
	src, dst NodeID
	label    uint32
}

// Graph is a Vectra graph. Single writer during the write phase; immutable
// after Seal with lock-free concurrent reads. On Seal the write-state memory
// migrates into the snapshot and is released.
type Graph struct {
	nodes     []Node
	byKey     map[string]NodeID
	edges     []edgeRec
	labels    []string // labelID -> name
	labIDs    map[string]uint32
	indexKeys []string // property keys to index at Seal time
	sealed    bool
	snap      *engine.Snapshot

	edgeIdx map[edgeKey][]EdgeID // (src,dst,label) -> live edge IDs (append order)
	tombE   []bool               // parallel to edges: true = deleted (compacted at Seal)
	tombN   []bool               // parallel to nodes: true = deleted (revivable via AddNode)
	deadE   int                  // true-count in tombE (visible edges = len(edges)-deadE)
	deadN   int                  // true-count in tombN
}

func New() *Graph {
	return &Graph{byKey: map[string]NodeID{}, labIDs: map[string]uint32{}, edgeIdx: map[edgeKey][]EdgeID{}}
}

func (g *Graph) internLabel(name string) uint32 {
	if id, ok := g.labIDs[name]; ok {
		return id
	}
	id := uint32(len(g.labels))
	g.labels = append(g.labels, name)
	g.labIDs[name] = id
	return id
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

// AddNode idempotently writes a node: if the key already exists it updates
// label/props and returns the original ID. Re-adding a deleted (tombstoned)
// key revives the node, reusing its ID.
//
// Props sharing contract: this method copies the passed-in map; but the maps
// exposed via Node/Edge after Seal are internal references — updates must
// replace them wholesale (AddNode/AddEdge again); in-place mutation is
// forbidden.
func (g *Graph) AddNode(key, label string, props map[string]any) (NodeID, error) {
	if g.sealed {
		return 0, ErrSealed
	}
	if id, ok := g.byKey[key]; ok {
		if g.tombN[id] { // deleted then re-added: revive, reusing the ID
			g.tombN[id] = false
			g.deadN--
		}
		g.nodes[id].Label = label
		g.nodes[id].Props = copyProps(props)
		return id, nil
	}
	id := NodeID(len(g.nodes))
	g.nodes = append(g.nodes, Node{ID: id, Key: key, Label: label, Props: copyProps(props)})
	g.tombN = append(g.tombN, false)
	g.byKey[key] = id
	return id, nil
}

// AddEdge appends a directed edge (multi-edges/self-loops both allowed).
func (g *Graph) AddEdge(srcKey, dstKey, label string, props map[string]any) (EdgeID, error) {
	if g.sealed {
		return 0, ErrSealed
	}
	src, dst, lid, err := g.resolveEdge(srcKey, dstKey, label)
	if err != nil {
		return 0, err
	}
	return g.appendEdge(src, dst, lid, props), nil
}

// UpsertEdge replaces-by-key: if live edges with the same (src, dst, label)
// exist, their Props are replaced on the most recent one (edge ID kept;
// extra parallel edges deleted); otherwise a new edge is appended. Unlike
// AddEdge (pure append, multigraph semantics) this method guarantees the
// triple is unique — it is the entry point for updating weight-like props
// (relation strength, event counts).
func (g *Graph) UpsertEdge(srcKey, dstKey, label string, props map[string]any) (EdgeID, error) {
	if g.sealed {
		return 0, ErrSealed
	}
	src, dst, lid, err := g.resolveEdge(srcKey, dstKey, label)
	if err != nil {
		return 0, err
	}
	k := edgeKey{src: src, dst: dst, label: lid}
	ids := g.edgeIdx[k]
	if len(ids) == 0 {
		return g.appendEdge(src, dst, lid, props), nil
	}
	for _, id := range ids[:len(ids)-1] { // drop extra parallels, keep the most recent
		g.tombE[id] = true
		g.deadE++
	}
	keep := ids[len(ids)-1]
	g.edges[keep].props = copyProps(props) // wholesale replacement (same contract as AddNode)
	g.edgeIdx[k] = []EdgeID{keep}
	return keep, nil
}

// resolveEdge looks up edge endpoints and label; deleted (tombstoned) nodes
// count as nonexistent.
func (g *Graph) resolveEdge(srcKey, dstKey, label string) (NodeID, NodeID, uint32, error) {
	src, ok := g.byKey[srcKey]
	if !ok || g.tombN[src] {
		return 0, 0, 0, ErrNodeNotFound
	}
	dst, ok := g.byKey[dstKey]
	if !ok || g.tombN[dst] {
		return 0, 0, 0, ErrNodeNotFound
	}
	return src, dst, g.internLabel(label), nil
}

// appendEdge is the shared tail of AddEdge/UpsertEdge: appends the edge
// record and registers it in the key index.
func (g *Graph) appendEdge(src, dst NodeID, lid uint32, props map[string]any) EdgeID {
	g.edges = append(g.edges, edgeRec{src: src, dst: dst, label: lid, props: copyProps(props)})
	g.tombE = append(g.tombE, false)
	id := EdgeID(len(g.edges) - 1)
	k := edgeKey{src: src, dst: dst, label: lid}
	g.edgeIdx[k] = append(g.edgeIdx[k], id)
	return id
}

// DeleteEdge removes every live parallel edge with the given (src, dst,
// label) triple (idempotent: deleting a triple that never existed succeeds
// with 0). Returns the number of edges actually deleted.
func (g *Graph) DeleteEdge(srcKey, dstKey, label string) (int, error) {
	if g.sealed {
		return 0, ErrSealed
	}
	src, ok := g.byKey[srcKey]
	if !ok || g.tombN[src] {
		return 0, ErrNodeNotFound
	}
	dst, ok := g.byKey[dstKey]
	if !ok || g.tombN[dst] {
		return 0, ErrNodeNotFound
	}
	lid, ok := g.labIDs[label]
	if !ok {
		return 0, nil // label never interned: no match possible, idempotent success
	}
	k := edgeKey{src: src, dst: dst, label: lid}
	for _, id := range g.edgeIdx[k] {
		g.tombE[id] = true
	}
	n := len(g.edgeIdx[k])
	g.deadE += n
	delete(g.edgeIdx, k)
	return n, nil
}

// DeleteNode removes a node and cascade-deletes all its incident edges
// (DETACH DELETE semantics). Cost is O(live edge keys) — a scan of the key
// index. Re-adding the same key via AddNode revives the node (ID reused).
// Returns the number of cascaded edge deletions.
func (g *Graph) DeleteNode(key string) (int, error) {
	if g.sealed {
		return 0, ErrSealed
	}
	id, ok := g.byKey[key]
	if !ok || g.tombN[id] {
		return 0, ErrNodeNotFound
	}
	removed := 0
	for k, ids := range g.edgeIdx { // deleting during range is safe in Go
		if k.src != id && k.dst != id {
			continue
		}
		for _, eid := range ids {
			g.tombE[eid] = true
		}
		g.deadE += len(ids)
		removed += len(ids)
		delete(g.edgeIdx, k)
	}
	g.tombN[id] = true
	g.deadN++
	return removed, nil
}

func (g *Graph) NodeByKey(key string) (Node, bool) {
	id, ok := g.byKey[key]
	if !ok {
		return Node{}, false
	}
	return g.NodeByID(id)
}

func (g *Graph) NodeByID(id NodeID) (Node, bool) {
	if g.sealed {
		rec, ok := g.snap.NodeInfo(uint32(id))
		if !ok {
			return Node{}, false
		}
		return Node{ID: id, Key: rec.Key, Label: rec.Label, Props: rec.Props}, true
	}
	if int(id) >= len(g.nodes) || g.tombN[id] {
		return Node{}, false
	}
	return g.nodes[id], true
}

func (g *Graph) EdgeByID(id EdgeID) (Edge, bool) {
	if !g.sealed {
		if int(id) >= len(g.edges) || g.tombE[id] {
			return Edge{}, false
		}
		e := g.edges[id]
		return Edge{ID: id, Src: e.src, Dst: e.dst, Label: g.labels[e.label], Props: e.props}, true
	}
	src, dst, label, props, ok := g.snap.EdgeInfo(uint32(id))
	if !ok {
		return Edge{}, false
	}
	return Edge{ID: id, Src: NodeID(src), Dst: NodeID(dst), Label: label, Props: props}, true
}

// NodeCount/EdgeCount report the visible size in both states: before Seal
// tombstoned records are subtracted; after Seal the write state has already
// migrated into the snapshot and been released, reads come from the snapshot.
func (g *Graph) NodeCount() int {
	if g.sealed {
		return g.snap.NodeCount()
	}
	return len(g.nodes) - g.deadN
}

func (g *Graph) EdgeCount() int {
	if g.sealed {
		return g.snap.EdgeCount()
	}
	return len(g.edges) - g.deadE
}

func (g *Graph) Sealed() bool { return g.sealed }

func (g *Graph) Stats() Stats {
	if g.sealed {
		st := Stats{
			Nodes: g.snap.NodeCount(),
			Edges: g.snap.EdgeCount(),
			SCCs:  int(g.snap.SCCCount()),
		}
		for c := uint32(0); c < g.snap.SCCCount(); c++ {
			if size := len(g.snap.SCCMembers(c)); size > 1 {
				st.CyclicNodes += size
			}
		}
		return st
	}
	st := Stats{Nodes: g.NodeCount(), Edges: g.EdgeCount()}
	return st
}

// compact returns the tombstone-free raw records: tombstoned nodes/edges are
// skipped, edges whose endpoints were dropped are discarded too (defensive
// for import paths), and node/edge IDs are renumbered densely. remap maps
// old node ID -> new node ID (^NodeID(0) for dropped nodes). Shared by Seal
// and ExportSnapshot.
func (g *Graph) compact() (nodes []engine.NodeRec, edges []engine.EdgeRec, remap []NodeID) {
	remap = make([]NodeID, len(g.nodes))
	nodes = make([]engine.NodeRec, 0, len(g.nodes)-g.deadN)
	for i, n := range g.nodes {
		if g.tombN[i] {
			remap[i] = ^NodeID(0)
			continue
		}
		remap[i] = NodeID(len(nodes))
		nodes = append(nodes, engine.NodeRec{Key: n.Key, Label: n.Label, Props: n.Props})
	}
	edges = make([]engine.EdgeRec, 0, len(g.edges)-g.deadE)
	for i, e := range g.edges {
		if g.tombE[i] || remap[e.src] == ^NodeID(0) || remap[e.dst] == ^NodeID(0) {
			continue
		}
		edges = append(edges, engine.EdgeRec{
			Src: uint32(remap[e.src]), Dst: uint32(remap[e.dst]),
			LabelID: e.label, Props: e.props,
		})
	}
	return nodes, edges, remap
}

// Seal compacts tombstones first (node and edge IDs may be renumbered), then
// builds every immutable structure queries need (forward/reverse CSR, Tarjan
// SCC, forward/reverse condensation DAG, member tables, self-loop marks,
// property indexes); afterwards the graph is read-only with lock-free
// concurrent reads.
func (g *Graph) Seal() error {
	if g.sealed {
		return nil
	}
	nodes, edges, remap := g.compact()
	snap, err := engine.Seal(nodes, edges, g.labels, g.indexKeys)
	if err != nil {
		return err
	}
	for i, n := range g.nodes { // byKey re-points at compacted IDs
		if remap[i] != ^NodeID(0) {
			g.byKey[n.Key] = remap[i]
		} else {
			delete(g.byKey, n.Key)
		}
	}
	g.snap = snap
	g.sealed = true
	g.nodes, g.edges = nil, nil // write state migrated into snapshot, release
	g.edgeIdx, g.tombE, g.tombN = nil, nil, nil
	return nil
}

// Save writes the whole graph to w. Requires Seal first (guarantees a
// consistent exported state).
func (g *Graph) Save(w io.Writer) error {
	if !g.sealed {
		return ErrNotSealed
	}
	snapNodes := g.snap.ExportNodes()
	snapEdges := g.snap.ExportEdges()
	nodes := make([]store.Node, len(snapNodes))
	for i, n := range snapNodes {
		nodes[i] = store.Node{Key: n.Key, Label: n.Label, Props: n.Props}
	}
	edges := make([]store.Edge, 0, len(snapEdges))
	for _, e := range snapEdges {
		src, _ := g.snap.NodeInfo(e.Src)
		dst, _ := g.snap.NodeInfo(e.Dst)
		edges = append(edges, store.Edge{
			SrcKey: src.Key,
			DstKey: dst.Key,
			Label:  g.snap.LabelName(e.LabelID),
			Props:  e.Props,
		})
	}
	return store.Encode(w, nodes, edges)
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

// Load restores a graph from a snapshot and Seals it automatically.
func Load(r io.Reader) (*Graph, error) {
	g, err := loadGraph(r)
	if err != nil {
		return nil, err
	}
	if err := g.Seal(); err != nil {
		return nil, err
	}
	return g, nil
}

func loadGraph(r io.Reader) (*Graph, error) {
	nodes, edges, err := store.Decode(r)
	if err != nil {
		return nil, err
	}
	g := New()
	for _, n := range nodes {
		if _, err := g.AddNode(n.Key, n.Label, n.Props); err != nil {
			return nil, err
		}
	}
	for _, e := range edges {
		if _, err := g.AddEdge(e.SrcKey, e.DstKey, e.Label, e.Props); err != nil {
			return nil, err
		}
	}
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

// IndexProperty registers a string property key to index at Seal time
// (idempotent; a no-op after Seal). Unregistered keys yield ok=false in
// LookupByProperty.
func (g *Graph) IndexProperty(key string) *Graph {
	if g.sealed {
		return g
	}
	for _, k := range g.indexKeys {
		if k == key {
			return g
		}
	}
	g.indexKeys = append(g.indexKeys, key)
	return g
}

// Clone deep-copies into a graph that can keep taking writes (the result is
// always unsealed). Props maps share references per the sharing contract
// (relies on "replace, don't mutate" discipline). The server module's
// hot-swap publisher uses it to clone the live graph.
func (g *Graph) Clone() *Graph {
	c := New()
	if g.sealed {
		for _, n := range g.snap.ExportNodes() {
			_, _ = c.AddNode(n.Key, n.Label, n.Props)
		}
		for _, e := range g.snap.ExportEdges() {
			src, _ := g.snap.NodeInfo(e.Src)
			dst, _ := g.snap.NodeInfo(e.Dst)
			_, _ = c.AddEdge(src.Key, dst.Key, g.snap.LabelName(e.LabelID), e.Props)
		}
		c.indexKeys = slices.Clone(g.indexKeys) // index declarations carry over to the clone
		return c
	}
	for _, n := range g.nodes {
		c.nodes = append(c.nodes, Node{ID: NodeID(len(c.nodes)), Key: n.Key, Label: n.Label, Props: n.Props})
		c.byKey[n.Key] = n.ID
	}
	c.edges = slices.Clone(g.edges)
	c.labels = slices.Clone(g.labels)
	c.labIDs = maps.Clone(g.labIDs)
	c.indexKeys = slices.Clone(g.indexKeys)
	c.tombE = slices.Clone(g.tombE)
	c.tombN = slices.Clone(g.tombN)
	c.deadE, c.deadN = g.deadE, g.deadN
	c.edgeIdx = make(map[edgeKey][]EdgeID, len(g.edgeIdx))
	for k, ids := range g.edgeIdx {
		c.edgeIdx[k] = slices.Clone(ids)
	}
	return c
}

// LookupByProperty finds nodes by string property value on the Seal-time
// index (ascending IDs). An unregistered key returns ok=false; a registered
// one returns the hit set (possibly empty).
func (g *Graph) LookupByProperty(key, value string) ([]Node, bool) {
	if !g.sealed {
		return nil, false
	}
	ids, ok := g.snap.LookupIndex(key, value)
	if !ok {
		return nil, false
	}
	out := make([]Node, 0, len(ids))
	for _, id := range ids {
		n, _ := g.NodeByID(NodeID(id))
		out = append(out, n)
	}
	return out, true
}

// ExportSnapshot serializes the current write state or sealed state (export
// works before Seal too — the server module's FSM snapshot uses it on the
// live graph). The format matches Save (CRC included).
func (g *Graph) ExportSnapshot() ([]byte, error) {
	if g.sealed {
		var buf bytes.Buffer
		if err := g.Save(&buf); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}
	nodesC, edgesC, _ := g.compact()
	nodes := make([]store.Node, len(nodesC))
	for i, n := range nodesC {
		nodes[i] = store.Node{Key: n.Key, Label: n.Label, Props: n.Props}
	}
	edges := make([]store.Edge, 0, len(edgesC))
	for _, e := range edgesC {
		edges = append(edges, store.Edge{
			SrcKey: nodesC[e.Src].Key,
			DstKey: nodesC[e.Dst].Key,
			Label:  g.labels[e.LabelID],
			Props:  e.Props,
		})
	}
	var buf bytes.Buffer
	if err := store.Encode(&buf, nodes, edges); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ImportSnapshot builds an unsealed graph from snapshot bytes (writable
// again). The caller decides when to Seal — the server FSM's Restore path
// uses it to rebuild the live graph.
func ImportSnapshot(data []byte) (*Graph, error) {
	g, err := loadGraph(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	return g, nil
}
