// Package vectra 是嵌入式内存图引擎：全内存无指针 CSR 邻接 + 写入侧
// SCC 压缩 + 层同步并行 BFS。面向"图永远小于单机内存"的企业依赖图谱
// 场景：批量导入、Seal 后并发只读、磁盘仅作快照恢复。
//
// 布局：本包是公共 API 门面（写入、选项、类型翻译）；密封状态在
// internal/engine，查询算法在 internal/traverse，编解码在 internal/store。
package vectra

import (
	"bytes"
	"errors"
	"io"
	"os"

	"vectra/internal/engine"
	"vectra/internal/store"
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
// Seal 后写态内存迁移进快照并释放。
type Graph struct {
	nodes     []Node
	byKey     map[string]NodeID
	edges     []edgeRec
	labels    []string // labelID -> name
	labIDs    map[string]uint32
	indexKeys []string // Seal 期要建索引的属性键
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

// AddNode 幂等写入节点：key 已存在时更新 label/props 并返回原 ID。
//
// Props 共享契约：本方法对传入 map 做了拷贝；但 Seal 后通过 Node/Edge
// 暴露的 map 是内部引用——更新必须整体替换（再次 AddNode/AddEdge），
// 禁止就地修改。
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

// NodeCount/EdgeCount 在两种状态下给出真实规模（Seal 后写态已迁移
// 进快照并释放，从快照读取）。
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

// Seal 构建查询所需的全部不可变结构（正/反向 CSR、Tarjan SCC、正/反向
// 凝聚 DAG、成员表、自环标记、属性索引），此后图只读、并发读免锁。
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
	snap, err := engine.Seal(nodes, edges, g.labels, nil)
	if err != nil {
		return err
	}
	g.snap = snap
	g.sealed = true
	g.nodes, g.edges = nil, nil // 写态已迁移进快照，释放
	return nil
}

// Save 把整图写入 w。要求已 Seal（保证导出一致状态）。
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

// SaveFile 保存到路径：临时文件 + fsync + 原子 rename，避免半写文件。
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

func (g *Graph) saveBytes() ([]byte, error) {
	var buf bytes.Buffer
	if err := g.Save(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Load 从快照恢复图并自动 Seal。
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

// LoadFile 从路径恢复图。
func LoadFile(path string) (*Graph, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Load(f)
}
