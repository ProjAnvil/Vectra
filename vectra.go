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
	"maps"
	"os"
	"slices"

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
	snap, err := engine.Seal(nodes, edges, g.labels, g.indexKeys)
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

// IndexProperty 注册一个 Seal 期构建索引的字符串属性键（幂等，
// Seal 后调用为空操作）。未注册的键在 LookupByProperty 中 ok=false。
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

// Clone 深拷贝一张可继续写入的图（结果恒为未密封态）。
// Props map 按共享契约共享引用（依赖"替换不修改"纪律）。
// server 模块的热替换发布器用它克隆 live 图。
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
		c.indexKeys = slices.Clone(g.indexKeys) // 索引声明随克隆迁移
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

// LookupByProperty 在 Seal 期索引上按字符串属性值查节点（升序 ID）。
// key 未注册索引返回 ok=false；注册过则返回命中集（可为空）。
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

// ExportSnapshot 序列化当前写态或密封态（未 Seal 也可导出——server
// 模块的 FSM 快照对 live 图使用）。格式与 Save 相同（含 CRC）。
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

// ImportSnapshot 从快照字节构建未密封图（可继续写入）。Seal 时机由
// 调用方决定——server FSM 的 Restore 路径用它重建 live 图。
func ImportSnapshot(data []byte) (*Graph, error) {
	g, err := loadGraph(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	return g, nil
}
