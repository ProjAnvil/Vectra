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
	"encoding/json"
	"slices"
	"sort"

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

// Typed property-index entries: each bucket is sorted jointly by value and
// node ID, searched by binary search.
type strEntry struct {
	val string
	id  uint32
}

type numEntry struct {
	val float64
	id  uint32
}

type boolEntry struct {
	val bool
	id  uint32
}

// propIndex is the index of one property key: three type buckets. Numbers
// normalize to float64 (all int/uint/float widths and json.Number), so a
// value written as int 42 matches the float64 42 a JSON round-trip produces.
// Values of other kinds (nil, slices, maps) are not indexed.
type propIndex struct {
	strs  []strEntry
	nums  []numEntry
	bools []boolEntry
}

// toFloat normalizes any numeric Go value to float64; non-numerics return
// ok=false.
func toFloat(v any) (float64, bool) {
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
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	}
	return 0, false
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

	indexes map[string]*propIndex // property key -> typed index (built at Seal time)
}

// Seal builds an immutable snapshot from raw records: forward/reverse CSR,
// Tarjan SCC, forward/reverse condensation DAG, component member tables,
// self-loop flags, and typed property indexes.
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
		s.indexes = make(map[string]*propIndex, len(indexKeys))
		for _, key := range indexKeys {
			pi := &propIndex{}
			for id, nr := range nodes {
				switch v := nr.Props[key].(type) {
				case string:
					pi.strs = append(pi.strs, strEntry{val: v, id: uint32(id)})
				case bool:
					pi.bools = append(pi.bools, boolEntry{val: v, id: uint32(id)})
				default:
					if f, ok := toFloat(nr.Props[key]); ok {
						pi.nums = append(pi.nums, numEntry{val: f, id: uint32(id)})
					}
				}
			}
			slices.SortFunc(pi.strs, func(a, b strEntry) int {
				if a.val != b.val {
					return cmp.Compare(a.val, b.val)
				}
				return cmp.Compare(a.id, b.id)
			})
			slices.SortFunc(pi.nums, func(a, b numEntry) int {
				if a.val != b.val {
					return cmp.Compare(a.val, b.val)
				}
				return cmp.Compare(a.id, b.id)
			})
			slices.SortFunc(pi.bools, func(a, b boolEntry) int {
				if a.val != b.val {
					if !a.val {
						return -1
					}
					return 1
				}
				return cmp.Compare(a.id, b.id)
			})
			s.indexes[key] = pi
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

// LookupIndex binary-searches the string bucket of the property index built
// at Seal time; an unregistered key returns ok=false.
func (s *Snapshot) LookupIndex(key, value string) ([]uint32, bool) {
	pi, ok := s.indexes[key]
	if !ok {
		return nil, false
	}
	entries := pi.strs
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

// LookupNumber finds nodes whose numeric property equals value (numbers are
// indexed as normalized float64); an unregistered key returns ok=false.
func (s *Snapshot) LookupNumber(key string, value float64) ([]uint32, bool) {
	return s.LookupNumberRange(key, &value, &value)
}

// LookupNumberRange finds nodes whose numeric property lies in [lo, hi]
// (inclusive; a nil bound leaves that side unbounded), ascending by value
// then ID; an unregistered key returns ok=false.
func (s *Snapshot) LookupNumberRange(key string, lo, hi *float64) ([]uint32, bool) {
	pi, ok := s.indexes[key]
	if !ok {
		return nil, false
	}
	nums := pi.nums
	loIdx := 0
	if lo != nil {
		loIdx = sort.Search(len(nums), func(i int) bool { return nums[i].val >= *lo })
	}
	hiIdx := len(nums)
	if hi != nil {
		hiIdx = sort.Search(len(nums), func(i int) bool { return nums[i].val > *hi })
	}
	out := make([]uint32, 0, max(0, hiIdx-loIdx))
	for i := loIdx; i < hiIdx; i++ {
		out = append(out, nums[i].id)
	}
	return out, true
}

// LookupBool finds nodes whose boolean property equals value; an unregistered
// key returns ok=false.
func (s *Snapshot) LookupBool(key string, value bool) ([]uint32, bool) {
	pi, ok := s.indexes[key]
	if !ok {
		return nil, false
	}
	var out []uint32
	for _, e := range pi.bools {
		if e.val == value {
			out = append(out, e.id)
		}
	}
	return out, true
}

// LabelID resolves a label name to its ID (false when never interned).
func (s *Snapshot) LabelID(name string) (uint32, bool) {
	for id, l := range s.labels {
		if l == name {
			return uint32(id), true
		}
	}
	return 0, false
}

// IndexedKeys returns the property keys indexes were built for at Seal time,
// ascending (nil when none) — the declarations a snapshot file must persist so
// a reloaded graph can rebuild its indexes.
func (s *Snapshot) IndexedKeys() []string {
	if len(s.indexes) == 0 {
		return nil
	}
	keys := make([]string, 0, len(s.indexes))
	for k := range s.indexes {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// ExportNodes / ExportEdges return the raw record slices (shared references,
// read-only contract).
func (s *Snapshot) ExportNodes() []NodeRec { return s.nodes }
func (s *Snapshot) ExportEdges() []EdgeRec { return s.edges }
