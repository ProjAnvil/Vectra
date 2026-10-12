// Package engine 持有 Vectra 的密封图状态：Seal 产物（CSR、SCC、凝聚
// DAG、成员表、属性索引）与原料（节点/边记录）。包只做状态与构建，
// 不做查询算法——算法在 internal/traverse，两者都只被根包门面使用。
//
// Snapshot 构建后不可变，可被多 goroutine 免锁并发读。
package engine

import (
	"cmp"
	"slices"

	"github.com/projanvil/vectra/internal/csr"
	"github.com/projanvil/vectra/internal/dag"
)

// NodeRec / EdgeRec 是图的原料记录。ID 是切片下标；LabelID 由根包的
// 标签注册表分配。Props map 与调用方共享引用——本包与所有消费者的
// 契约是"更新必须替换整个 map，禁止就地修改"。
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

// indexEntry 是属性索引项：值与节点 ID 联合排序，二分查找。
type indexEntry struct {
	val string
	id  uint32
}

// Snapshot 是 Seal 之后的全部查询状态。
type Snapshot struct {
	nodes  []NodeRec
	edges  []EdgeRec
	labels []string // labelID -> name

	csrF csr.Adjacency // 正向邻接
	csrR csr.Adjacency // 反向邻接

	sccOf      []uint32      // 节点 -> 分量
	sccCount   uint32        // 分量总数
	cond       dag.Condensed // 正向凝聚 DAG
	condR      dag.Condensed // 反向凝聚 DAG
	sccNodeOff []uint32      // 分量 -> 成员区间 [off, off+1)
	sccNodes   []uint32      // 按分量分组的成员（组内升序）
	selfLoop   []bool

	indexes map[string][]indexEntry // 属性键 -> 有序索引（Seal 期构建）
}

// Seal 从原料构建不可变快照：正/反向 CSR、Tarjan SCC、正/反向凝聚
// DAG、分量成员表、自环标记、字符串属性索引。
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

	// 分量成员表：计数排序按 sccOf 分组；u 升序遍历保证组内升序。
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

// ---- 只读访问器（traverse 与根包门面的全部依赖面） ----

// AdjacencyF 返回正向 CSR。
func (s *Snapshot) AdjacencyF() csr.Adjacency { return s.csrF }

// AdjacencyR 返回反向 CSR。
func (s *Snapshot) AdjacencyR() csr.Adjacency { return s.csrR }

// CondensedF 返回正向凝聚 DAG。
func (s *Snapshot) CondensedF() dag.Condensed { return s.cond }

// CondensedR 返回反向凝聚 DAG。
func (s *Snapshot) CondensedR() dag.Condensed { return s.condR }

// NodeCount 返回节点总数。
func (s *Snapshot) NodeCount() int { return len(s.nodes) }

// EdgeCount 返回边总数。
func (s *Snapshot) EdgeCount() int { return len(s.edges) }

// NodeInfo 返回节点原料；越界返回 ok=false。
func (s *Snapshot) NodeInfo(id uint32) (NodeRec, bool) {
	if int(id) >= len(s.nodes) {
		return NodeRec{}, false
	}
	return s.nodes[id], true
}

// EdgeInfo 返回边原料（含标签名）；越界返回 ok=false。
func (s *Snapshot) EdgeInfo(id uint32) (src, dst uint32, label string, props map[string]any, ok bool) {
	if int(id) >= len(s.edges) {
		return 0, 0, "", nil, false
	}
	e := s.edges[id]
	return e.Src, e.Dst, s.labels[e.LabelID], e.Props, true
}

// LabelName 返回标签 ID 对应名称。
func (s *Snapshot) LabelName(id uint32) string { return s.labels[id] }

// SCCOf 返回节点所属分量 ID。
func (s *Snapshot) SCCOf(v uint32) uint32 { return s.sccOf[v] }

// SCCCount 返回分量总数。
func (s *Snapshot) SCCCount() uint32 { return s.sccCount }

// SCCMembers 返回分量 scc 的成员（升序）。
func (s *Snapshot) SCCMembers(scc uint32) []uint32 {
	return s.sccNodes[s.sccNodeOff[scc]:s.sccNodeOff[scc+1]]
}

// SelfLoop 报告节点是否有自环。
func (s *Snapshot) SelfLoop(v uint32) bool { return s.selfLoop[v] }

// LookupIndex 在 Seal 期构建的字符串属性索引上二分查找；
// key 未注册返回 ok=false。
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

// ExportNodes / ExportEdges 返回原料切片（共享引用，只读契约）。
func (s *Snapshot) ExportNodes() []NodeRec { return s.nodes }
func (s *Snapshot) ExportEdges() []EdgeRec { return s.edges }
