// Package engine holds Vectra's sealed graph state: Seal outputs (CSR, SCC,
// condensation DAG, member tables, property indexes) and raw records
// (node/edge records). The package does state and construction only, no query
// algorithms — those live in internal/traverse; both are used only by the
// root package facade.
//
// Once built, a Snapshot is immutable and can be read concurrently by
// multiple goroutines without locks.
package engine

import (
	"cmp"
	"slices"

	"github.com/projanvil/vectra/internal/csr"
	"github.com/projanvil/vectra/internal/dag"
)

// NodeRec / EdgeRec are the graph's raw records. ID is the slice index;
// LabelID is assigned by the root package's label registry. The Props map is
// shared by reference with callers — the contract of this package and all
// consumers is "updates must replace the whole map; in-place mutation is
// forbidden".
type NodeRec struct {
	Key   string
	Label string
	Props map[string]any
}

type EdgeRec struct {
	Src     uint32
	Dst     uint32
	LabelID uint32
	Props   map[string]any
}

// indexEntry is a property index entry: sorted jointly by value and node ID,
// searched by binary search.
type indexEntry struct {
	val string
	id  uint32
}

// Snapshot is all query state after Seal.
type Snapshot struct {
	nodes  []NodeRec
	edges  []EdgeRec
	labels []string // labelID -> name

	csrF csr.Adjacency // forward adjacency
	csrR csr.Adjacency // reverse adjacency

	sccOf      []uint32      // node -> component
	sccCount   uint32        // component count
	cond       dag.Condensed // forward condensation DAG
	condR      dag.Condensed // reverse condensation DAG
	sccNodeOff []uint32      // component -> member range [off, off+1)
	sccNodes   []uint32      // members grouped by component (ascending within group)
	selfLoop   []bool

	indexes map[string][]indexEntry // property key -> sorted index (built at Seal time)
}

// Seal builds an immutable snapshot from raw records: forward/reverse CSR,
// Tarjan SCC, forward/reverse condensation DAG, component member tables,
// self-loop flags, and string property indexes.
func Seal(nodes []NodeRec, edges []EdgeRec, labels []string, indexKeys []string) (*Snapshot, error) {
	s := &Snapshot{nodes: nodes, edges: edges, labels: labels}
	n := uint32(len(nodes))
	e := len(edges)

	src := make([]uint32, e)
	dst := make([]uint32, e)
	eids := make([]uint32, e)
	lids := make([]uint32, e)
	for i, er := range edges {
		src[i] = er.Src
		dst[i] = er.Dst
		eids[i] = uint32(i)
		lids[i] = er.LabelID
	}
	s.csrF = csr.BuildPayloads(n, src, dst, eids, lids)
	s.csrR = csr.BuildPayloads(n, dst, src, eids, lids)

	adjF := func(u uint32) []uint32 { return s.csrF.Neighbors(u) }
	labF := func(u uint32) []uint32 {
		return s.csrF.Labels[s.csrF.Offsets[u]:s.csrF.Offsets[u+1]]
	}
	adjR := func(u uint32) []uint32 { return s.csrR.Neighbors(u) }
	labR := func(u uint32) []uint32 {
		return s.csrR.Labels[s.csrR.Offsets[u]:s.csrR.Offsets[u+1]]
	}

	s.sccOf, s.sccCount = dag.Tarjan(n, adjF)
	s.cond = dag.Condense(n, s.sccOf, s.sccCount, adjF, labF)
	s.condR = dag.Condense(n, s.sccOf, s.sccCount, adjR, labR)

	// Component member table: counting sort groups by sccOf; ascending u
	// iteration keeps members ascending within each group.
	s.sccNodeOff = make([]uint32, s.sccCount+1)
	for _, c := range s.sccOf {
		s.sccNodeOff[c+1]++
	}
	for i := uint32(1); i <= s.sccCount; i++ {
		s.sccNodeOff[i] += s.sccNodeOff[i-1]
	}
	s.sccNodes = make([]uint32, n)
	cursor := make([]uint32, s.sccCount)
	copy(cursor, s.sccNodeOff[:s.sccCount])
	for u := uint32(0); u < n; u++ {
		c := s.sccOf[u]
		s.sccNodes[cursor[c]] = u
		cursor[c]++
	}

	s.selfLoop = make([]bool, n)
	for _, er := range edges {
		if er.Src == er.Dst {
			s.selfLoop[er.Src] = true
		}
	}

	if len(indexKeys) > 0 {
		s.indexes = make(map[string][]indexEntry, len(indexKeys))
		for _, key := range indexKeys {
			var entries []indexEntry
			for id, nr := range nodes {
				if str, ok := nr.Props[key].(string); ok {
					entries = append(entries, indexEntry{val: str, id: uint32(id)})
				}
			}
			slices.SortFunc(entries, func(a, b indexEntry) int {
				if a.val != b.val {
					return cmp.Compare(a.val, b.val)
				}
				return cmp.Compare(a.id, b.id)
			})
			s.indexes[key] = entries
		}
	}
	return s, nil
}

// ---- read-only accessors (the full dependency surface of traverse and the root facade) ----

// AdjacencyF returns the forward CSR.
func (s *Snapshot) AdjacencyF() csr.Adjacency { return s.csrF }

// AdjacencyR returns the reverse CSR.
func (s *Snapshot) AdjacencyR() csr.Adjacency { return s.csrR }

// CondensedF returns the forward condensation DAG.
func (s *Snapshot) CondensedF() dag.Condensed { return s.cond }

// CondensedR returns the reverse condensation DAG.
func (s *Snapshot) CondensedR() dag.Condensed { return s.condR }

// NodeCount returns the total number of nodes.
func (s *Snapshot) NodeCount() int { return len(s.nodes) }

// EdgeCount returns the total number of edges.
func (s *Snapshot) EdgeCount() int { return len(s.edges) }

// NodeInfo returns the node record; out of range returns ok=false.
func (s *Snapshot) NodeInfo(id uint32) (NodeRec, bool) {
	if int(id) >= len(s.nodes) {
		return NodeRec{}, false
	}
	return s.nodes[id], true
}

// EdgeInfo returns the edge record (label name included); out of range
// returns ok=false.
func (s *Snapshot) EdgeInfo(id uint32) (src, dst uint32, label string, props map[string]any, ok bool) {
	if int(id) >= len(s.edges) {
		return 0, 0, "", nil, false
	}
	e := s.edges[id]
	return e.Src, e.Dst, s.labels[e.LabelID], e.Props, true
}

// LabelName returns the name for a label ID.
func (s *Snapshot) LabelName(id uint32) string { return s.labels[id] }

// SCCOf returns the component ID a node belongs to.
func (s *Snapshot) SCCOf(v uint32) uint32 { return s.sccOf[v] }

// SCCCount returns the total number of components.
func (s *Snapshot) SCCCount() uint32 { return s.sccCount }

// SCCMembers returns the members of component scc (ascending).
func (s *Snapshot) SCCMembers(scc uint32) []uint32 {
	return s.sccNodes[s.sccNodeOff[scc]:s.sccNodeOff[scc+1]]
}

// SelfLoop reports whether the node has a self-loop.
func (s *Snapshot) SelfLoop(v uint32) bool { return s.selfLoop[v] }

// LookupIndex binary-searches the string property index built at Seal time;
// an unregistered key returns ok=false.
func (s *Snapshot) LookupIndex(key, value string) ([]uint32, bool) {
	entries, ok := s.indexes[key]
	if !ok {
		return nil, false
	}
	lo, hi := 0, len(entries)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if entries[mid].val < value {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	var out []uint32
	for i := lo; i < len(entries) && entries[i].val == value; i++ {
		out = append(out, entries[i].id)
	}
	return out, true
}

// ExportNodes / ExportEdges return the raw record slices (shared references,
// read-only contract).
func (s *Snapshot) ExportNodes() []NodeRec { return s.nodes }
func (s *Snapshot) ExportEdges() []EdgeRec { return s.edges }
