// Package vectra 是嵌入式内存图引擎：全内存无指针 CSR 邻接 + 写入侧
// SCC 压缩 + 层同步并行 BFS。面向"图永远小于单机内存"的企业依赖图谱
// 场景：批量导入、Seal 后并发只读、磁盘仅作快照恢复。
package vectra

import (
	"errors"

	"vectra/internal/csr"
	"vectra/internal/dag"
)

// NodeID / EdgeID 是图内整数标识。Seal 后所有查询结构以 NodeID 寻址，
// 字符串 key 只在 API 边界转换一次。
type NodeID uint32
type EdgeID uint32

var (
	ErrSealed       = errors.New("vectra: graph sealed, mutation rejected")
	ErrNotSealed    = errors.New("vectra: graph not sealed, query rejected")
	ErrNodeNotFound = errors.New("vectra: node key not found")
)

type Node struct {
	ID    NodeID
	Key   string
	Label string
	Props map[string]any
}

type Edge struct {
	ID    EdgeID
	Src   NodeID
	Dst   NodeID
	Label string
	Props map[string]any
}

// Stats 汇报图的规模与压环结果。CyclicNodes 是属于大小>1 的 SCC 的
// 节点数（自环不计入，由 Cycles() 单独报告）。
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

// Graph 是一张 Vectra 图。写入期单写者；Seal 后不可变，并发读免锁。
type Graph struct {
	nodes  []Node
	byKey  map[string]NodeID
	edges  []edgeRec
	labels []string // labelID -> name
	labIDs map[string]uint32
	sealed bool

	// Seal 产物（不可变，并发读免锁）
	csrF       csr.Adjacency // 正向邻接
	csrR       csr.Adjacency // 反向邻接
	sccOf      []uint32      // 节点 -> 分量 ID
	sccCount   uint32
	cond       dag.Condensed // 正向凝聚 DAG
	condR      dag.Condensed // 反向凝聚 DAG
	sccLevel   []uint32      // 分量 -> 最长路径拓扑层级
	sccNodeOff []uint32      // 分量 -> 成员区间 [off, off+1)
	sccNodes   []uint32      // 按分量分组的成员（组内 NodeID 升序）
	selfLoop   []bool
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

// AddNode 幂等写入节点：key 已存在时更新 label/props 并返回原 ID。
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

// AddEdge 追加一条有向边（多边/自环均允许）。
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
	return g.nodes[id], true
}

func (g *Graph) NodeByID(id NodeID) (Node, bool) {
	if int(id) >= len(g.nodes) {
		return Node{}, false
	}
	return g.nodes[id], true
}

func (g *Graph) EdgeByID(id EdgeID) (Edge, bool) {
	if int(id) >= len(g.edges) {
		return Edge{}, false
	}
	e := g.edges[id]
	return Edge{ID: id, Src: e.src, Dst: e.dst, Label: g.labels[e.label], Props: e.props}, true
}

func (g *Graph) NodeCount() int { return len(g.nodes) }
func (g *Graph) EdgeCount() int { return len(g.edges) }
func (g *Graph) Sealed() bool   { return g.sealed }

func (g *Graph) Stats() Stats {
	st := Stats{Nodes: len(g.nodes), Edges: len(g.edges)}
	if !g.sealed {
		return st
	}
	st.SCCs = int(g.sccCount)
	for s := uint32(0); s < g.sccCount; s++ {
		if size := g.sccNodeOff[s+1] - g.sccNodeOff[s]; size > 1 {
			st.CyclicNodes += int(size)
		}
	}
	return st
}

// Seal 构建查询所需的全部不可变结构：正/反向 CSR、SCC 分量、
// 正/反向凝聚 DAG、拓扑层级、分量成员表。此后图只读、并发读免锁。
func (g *Graph) Seal() error {
	if g.sealed {
		return nil
	}
	n := uint32(len(g.nodes))
	e := len(g.edges)
	src := make([]uint32, e)
	dst := make([]uint32, e)
	eids := make([]uint32, e)
	lids := make([]uint32, e)
	for i, er := range g.edges {
		src[i] = uint32(er.src)
		dst[i] = uint32(er.dst)
		eids[i] = uint32(i)
		lids[i] = er.label
	}
	g.csrF = csr.BuildPayloads(n, src, dst, eids, lids)
	g.csrR = csr.BuildPayloads(n, dst, src, eids, lids)

	adjF := func(u uint32) []uint32 { return g.csrF.Neighbors(u) }
	labF := func(u uint32) []uint32 {
		return g.csrF.Labels[g.csrF.Offsets[u]:g.csrF.Offsets[u+1]]
	}
	adjR := func(u uint32) []uint32 { return g.csrR.Neighbors(u) }
	labR := func(u uint32) []uint32 {
		return g.csrR.Labels[g.csrR.Offsets[u]:g.csrR.Offsets[u+1]]
	}

	g.sccOf, g.sccCount = dag.Tarjan(n, adjF)
	g.cond = dag.Condense(n, g.sccOf, g.sccCount, adjF, labF)
	g.condR = dag.Condense(n, g.sccOf, g.sccCount, adjR, labR)
	g.sccLevel = dag.Levels(g.cond)

	// 分量成员表：按 sccOf 计数排序分组；u 升序遍历保证组内 NodeID 升序。
	g.sccNodeOff = make([]uint32, g.sccCount+1)
	for _, s := range g.sccOf {
		g.sccNodeOff[s+1]++
	}
	for i := uint32(1); i <= g.sccCount; i++ {
		g.sccNodeOff[i] += g.sccNodeOff[i-1]
	}
	g.sccNodes = make([]uint32, n)
	cursor := make([]uint32, g.sccCount)
	copy(cursor, g.sccNodeOff[:g.sccCount])
	for u := uint32(0); u < n; u++ {
		s := g.sccOf[u]
		g.sccNodes[cursor[s]] = u
		cursor[s]++
	}

	g.selfLoop = make([]bool, n)
	for _, er := range g.edges {
		if er.src == er.dst {
			g.selfLoop[er.src] = true
		}
	}

	g.sealed = true
	return nil
}
