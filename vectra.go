// Package vectra is an embedded in-memory graph engine: all-memory
// pointer-free CSR adjacency + write-side SCC compression + level-synchronous
// parallel BFS. It targets enterprise dependency-graph scenarios where
// "the graph always fits in single-machine memory": bulk import, lock-free
// concurrent reads after Seal, disk used only for snapshot recovery.
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
}

func New() *Graph {
	return &Graph{byKey: map[string]NodeID{}, labIDs: map[string]uint32{}}
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
// label/props and returns the original ID.
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
		g.nodes[id].Label = label
		g.nodes[id].Props = copyProps(props)
		return id, nil
	}
	id := NodeID(len(g.nodes))
	g.nodes = append(g.nodes, Node{ID: id, Key: key, Label: label, Props: copyProps(props)})
	g.byKey[key] = id
	return id, nil
}

// AddEdge appends a directed edge (multi-edges/self-loops both allowed).
func (g *Graph) AddEdge(srcKey, dstKey, label string, props map[string]any) (EdgeID, error) {
	if g.sealed {
		return 0, ErrSealed
	}
	src, ok := g.byKey[srcKey]
	if !ok {
		return 0, ErrNodeNotFound
	}
	dst, ok := g.byKey[dstKey]
	if !ok {
		return 0, ErrNodeNotFound
	}
	lid := g.internLabel(label)
	g.edges = append(g.edges, edgeRec{src: src, dst: dst, label: lid, props: copyProps(props)})
	return EdgeID(len(g.edges) - 1), nil
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
	if int(id) >= len(g.nodes) {
		return Node{}, false
	}
	return g.nodes[id], true
}

func (g *Graph) EdgeByID(id EdgeID) (Edge, bool) {
	if !g.sealed {
		if int(id) >= len(g.edges) {
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

// NodeCount/EdgeCount report the true size in both states (after Seal the
// write state has already migrated into the snapshot and been released;
// reads come from the snapshot).
func (g *Graph) NodeCount() int {
	if g.sealed {
		return g.snap.NodeCount()
	}
	return len(g.nodes)
}

func (g *Graph) EdgeCount() int {
	if g.sealed {
		return g.snap.EdgeCount()
	}
	return len(g.edges)
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
	st := Stats{Nodes: len(g.nodes), Edges: len(g.edges)}
	return st
}

// Seal builds every immutable structure queries need (forward/reverse CSR,
// Tarjan SCC, forward/reverse condensation DAG, member tables, self-loop
// marks, property indexes); afterwards the graph is read-only with
// lock-free concurrent reads.
func (g *Graph) Seal() error {
	if g.sealed {
		return nil
	}
	nodes := make([]engine.NodeRec, len(g.nodes))
	for i, n := range g.nodes {
		nodes[i] = engine.NodeRec{Key: n.Key, Label: n.Label, Props: n.Props}
	}
	edges := make([]engine.EdgeRec, len(g.edges))
	for i, e := range g.edges {
		edges[i] = engine.EdgeRec{Src: uint32(e.src), Dst: uint32(e.dst), LabelID: e.label, Props: e.props}
	}
	snap, err := engine.Seal(nodes, edges, g.labels, g.indexKeys)
	if err != nil {
		return err
	}
	g.snap = snap
	g.sealed = true
	g.nodes, g.edges = nil, nil // write state migrated into snapshot, release
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
	nodes := make([]store.Node, len(g.nodes))
	for i, n := range g.nodes {
		nodes[i] = store.Node{Key: n.Key, Label: n.Label, Props: n.Props}
	}
	edges := make([]store.Edge, 0, len(g.edges))
	for _, e := range g.edges {
		edges = append(edges, store.Edge{
			SrcKey: g.nodes[e.src].Key,
			DstKey: g.nodes[e.dst].Key,
			Label:  g.labels[e.label],
			Props:  e.props,
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
