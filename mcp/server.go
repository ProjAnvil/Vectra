// Package mcpserver 用官方 Go SDK（modelcontextprotocol/go-sdk）把
// Vectra 引擎暴露为 MCP 服务：直接嵌入引擎（单二进制，stdio 即可用，
// 无需网络），供 Claude Desktop / Claude Code 等 MCP 宿主调用。
//
// 工具面遵循数据库 MCP 的最佳实践：小而全的 10 个工具、读写注解
// 分离、结果默认封顶（limit，防上下文窗口爆炸）。
package mcpserver

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/projanvil/vectra"
)

// Version 是 MCP 服务宣告的实现版本。
const Version = "0.1.0"

// defaultLimit 是 bfs/reachable 默认返回的节点数上限（结果封顶，
// total 字段仍反映真实规模——数据库 MCP 的安全默认实践）。
const defaultLimit = 1000

// Options 配置 MCP 服务。
type Options struct {
	// SnapshotPath 非空时：启动加载、每次 seal 后保存（单文件快照）。
	SnapshotPath string
}

// service 持有嵌入的引擎状态。写入累积在 live 图；seal 后查询走
// 密封视图。MCP 会话可并发调用工具——写入工具互斥，查询免锁。
type service struct {
	mu   sync.Mutex
	live *vectra.Graph
	view *vectra.Graph // 密封视图（nil = 未 seal）

	propKeys map[string]bool // 注入期见过的属性键，seal 时自动建索引
	opts     Options
}

// New 创建 MCP server（嵌入一个新图；有 SnapshotPath 则加载）。
func New(g *vectra.Graph, opts Options) *mcp.Server {
	svc := &service{live: g, propKeys: map[string]bool{}, opts: opts}
	if opts.SnapshotPath != "" {
		if loaded, err := vectra.LoadFile(opts.SnapshotPath); err == nil {
			svc.view = loaded // Load 自动 Seal
		}
	}
	s := mcp.NewServer(
		&mcp.Implementation{Name: "github.com/projanvil/vectra", Version: Version},
		&mcp.ServerOptions{
			Instructions: "Vectra 内存图引擎（依赖图谱）。写入用 inject_batch，" +
				"写入后必须调用 seal 才能查询；查询工具：neighbors（一跳）、" +
				"bfs（多跳）、shortest_path、reachable（影响面）、cycles（循环依赖）、" +
				"stats、get_node、lookup_property。",
		},
	)
	registerTools(s, svc)
	return s
}

// Handler 返回 Streamable HTTP 传输的 http.Handler（挂载到 /mcp）。
func Handler(g *vectra.Graph, opts Options) *mcp.StreamableHTTPHandler {
	return mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		return New(g, opts) // 每连接一个 server 实例（无状态传输模式）
	}, nil)
}

// ---- 输入/输出类型（JSON Schema 自动生成） ----

type nodeIn struct {
	Key   string         `json:"key" jsonschema:"节点唯一键"`
	Label string         `json:"label,omitempty"`
	Props map[string]any `json:"props,omitempty"`
}

type edgeIn struct {
	SrcKey string         `json:"srcKey" jsonschema:"起点 key"`
	DstKey string         `json:"dstKey" jsonschema:"终点 key"`
	Label  string         `json:"label,omitempty"`
	Props  map[string]any `json:"props,omitempty"`
}

type injectIn struct {
	Nodes []nodeIn `json:"nodes,omitempty"`
	Edges []edgeIn `json:"edges,omitempty"`
}

type countsOut struct {
	NodesApplied int `json:"nodesApplied"`
	EdgesApplied int `json:"edgesApplied"`
	EdgesSkipped int `json:"edgesSkipped"`
}

type statsOut struct {
	Nodes       int  `json:"nodes"`
	Edges       int  `json:"edges"`
	SCCs        int  `json:"sccs"`
	CyclicNodes int  `json:"cyclicNodes"`
	Sealed      bool `json:"sealed"`
}

type nodeOut struct {
	Key   string         `json:"key"`
	Label string         `json:"label"`
	Props map[string]any `json:"props,omitempty"`
}

type neighborsIn struct {
	Key       string `json:"key"`
	Direction string `json:"direction,omitempty" jsonschema:"out（默认）或 in"`
	Label     string `json:"label,omitempty"`
}

type bfsIn struct {
	From      []string `json:"from" jsonschema:"一个或多个起点 key"`
	MaxDepth  uint32   `json:"maxDepth,omitempty" jsonschema:"缺省表示不限跳数"`
	Labels    []string `json:"labels,omitempty"`
	Direction string   `json:"direction,omitempty"`
	Limit     int      `json:"limit,omitempty" jsonschema:"返回节点数上限，默认 1000"`
}

type listOut struct {
	Count int      `json:"count" jsonschema:"本次返回数"`
	Total int      `json:"total" jsonschema:"真实总数（可能被 limit 截断）"`
	Nodes []string `json:"nodes"`
}

type depthOut struct {
	listOut
	Depths map[string]uint32 `json:"depths,omitempty" jsonschema:"key->跳数"`
}

type pathIn struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type pathOut struct {
	Found bool     `json:"found"`
	Hops  int      `json:"hops"`
	Nodes []string `json:"nodes,omitempty"`
}

type reachIn struct {
	From  string `json:"from"`
	Limit int    `json:"limit,omitempty"`
}

type cyclesOut struct {
	Cycles [][]string `json:"cycles" jsonschema:"每个循环依赖的成员 key 列表，按规模降序"`
}

type lookupIn struct {
	PropKey string `json:"propKey"`
	Value   string `json:"value"`
	Limit   int    `json:"limit,omitempty"`
}

// ---- 工具注册 ----

func registerTools(s *mcp.Server, svc *service) {
	mcp.AddTool(s, &mcp.Tool{
		Name: "inject_batch",
		Description: "批量写入节点与边（upsert：同 key 更新属性）。" +
			"写入后需调用 seal 才能查询。",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: false},
	}, svc.injectBatch)

	mcp.AddTool(s, &mcp.Tool{
		Name:        "seal",
		Description: "密封图：构建查询索引后图变为只读可查询（写前需重新注入）。返回图统计。",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: false},
	}, svc.seal)

	mcp.AddTool(s, &mcp.Tool{
		Name: "stats", Description: "图统计：节点/边/强连通分量/环上节点数。",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, svc.stats)

	mcp.AddTool(s, &mcp.Tool{
		Name: "get_node", Description: "按 key 查节点（标签与属性）。",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, svc.getNode)

	mcp.AddTool(s, &mcp.Tool{
		Name: "neighbors", Description: "一跳邻居（direction: out/in，可按边标签过滤）。",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, svc.neighbors)

	mcp.AddTool(s, &mcp.Tool{
		Name: "bfs",
		Description: "多跳遍历（1..maxDepth 跳邻域）：maxDepth=2 即朋友的朋友；" +
			"0=不限。返回每节点跳数。结果默认封顶 1000，total 为真实总数。",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, svc.bfs)

	mcp.AddTool(s, &mcp.Tool{
		Name: "shortest_path", Description: "两节点间最短路径（双向 BFS）。",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, svc.shortestPath)

	mcp.AddTool(s, &mcp.Tool{
		Name: "reachable", Description: "从起点可达的全部节点（依赖影响面/爆炸半径）。",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, svc.reachable)

	mcp.AddTool(s, &mcp.Tool{
		Name: "cycles", Description: "全部循环依赖（强连通分量成员列表，按规模降序）。",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, svc.cycles)

	mcp.AddTool(s, &mcp.Tool{
		Name: "lookup_property", Description: "按字符串属性值查节点（env/team/tier 等标签类属性）。",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, svc.lookupProperty)
}

// ---- 工具实现 ----

func (svc *service) injectBatch(_ context.Context, _ *mcp.CallToolRequest, in injectIn) (*mcp.CallToolResult, countsOut, error) {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	out := countsOut{}
	for _, n := range in.Nodes {
		if _, err := svc.live.AddNode(n.Key, n.Label, n.Props); err == nil {
			out.NodesApplied++
		}
		for k := range n.Props {
			svc.propKeys[k] = true
		}
	}
	for _, e := range in.Edges {
		if _, err := svc.live.AddEdge(e.SrcKey, e.DstKey, e.Label, e.Props); err == nil {
			out.EdgesApplied++
		} else {
			out.EdgesSkipped++
		}
		for k := range e.Props {
			svc.propKeys[k] = true
		}
	}
	return nil, out, nil
}

func (svc *service) seal(_ context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, statsOut, error) {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	for k := range svc.propKeys {
		svc.live.IndexProperty(k) // 注入期见过的属性键自动建索引
	}
	if err := svc.live.Seal(); err != nil {
		return nil, statsOut{}, err
	}
	svc.view = svc.live
	if svc.opts.SnapshotPath != "" {
		if err := svc.view.SaveFile(svc.opts.SnapshotPath); err != nil {
			return nil, statsOut{}, fmt.Errorf("快照保存失败: %w", err)
		}
	}
	st := svc.view.Stats()
	return nil, statsOut{
		Nodes: st.Nodes, Edges: st.Edges, SCCs: st.SCCs,
		CyclicNodes: st.CyclicNodes, Sealed: true,
	}, nil
}

// view 返回密封视图；未密封时给出可操作的错误提示。
func (svc *service) viewOrSealHint() (*vectra.Graph, error) {
	if v := svc.view; v != nil {
		return v, nil
	}
	return nil, fmt.Errorf("图尚未密封：先调用 inject_batch 写入，再调用 seal 后查询")
}

func (svc *service) stats(_ context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, statsOut, error) {
	v, err := svc.viewOrSealHint()
	if err != nil {
		return nil, statsOut{}, err
	}
	st := v.Stats()
	return nil, statsOut{
		Nodes: st.Nodes, Edges: st.Edges, SCCs: st.SCCs,
		CyclicNodes: st.CyclicNodes, Sealed: true,
	}, nil
}

func (svc *service) getNode(_ context.Context, _ *mcp.CallToolRequest, in struct {
	Key string `json:"key"`
}) (*mcp.CallToolResult, nodeOut, error) {
	v, err := svc.viewOrSealHint()
	if err != nil {
		return nil, nodeOut{}, err
	}
	n, ok := v.NodeByKey(in.Key)
	if !ok {
		return nil, nodeOut{}, fmt.Errorf("节点 %q 不存在", in.Key)
	}
	return nil, nodeOut{Key: n.Key, Label: n.Label, Props: n.Props}, nil
}

func parseDir(s string) vectra.Direction {
	if s == "in" {
		return vectra.DirIn
	}
	return vectra.DirOut
}

func (svc *service) neighbors(_ context.Context, _ *mcp.CallToolRequest, in neighborsIn) (*mcp.CallToolResult, listOut, error) {
	v, err := svc.viewOrSealHint()
	if err != nil {
		return nil, listOut{}, err
	}
	var labels []string
	if in.Label != "" {
		labels = []string{in.Label}
	}
	ns, err := v.NeighborsOf(in.Key, parseDir(in.Direction), labels...)
	if err != nil {
		return nil, listOut{}, err
	}
	out := listOut{Total: len(ns), Nodes: make([]string, 0, len(ns))}
	for _, n := range ns {
		out.Nodes = append(out.Nodes, n.Key)
	}
	out.Count = len(out.Nodes)
	return nil, out, nil
}

func (svc *service) bfs(_ context.Context, _ *mcp.CallToolRequest, in bfsIn) (*mcp.CallToolResult, depthOut, error) {
	v, err := svc.viewOrSealHint()
	if err != nil {
		return nil, depthOut{}, err
	}
	limit := in.Limit
	if limit <= 0 {
		limit = defaultLimit
	}
	starts := make([]vectra.NodeID, 0, len(in.From))
	for _, k := range in.From {
		n, ok := v.NodeByKey(k)
		if !ok {
			return nil, depthOut{}, fmt.Errorf("起点 %q 不存在", k)
		}
		starts = append(starts, n.ID)
	}
	opts := []vectra.BFSOption{vectra.WithDirection(parseDir(in.Direction))}
	if in.MaxDepth > 0 {
		opts = append(opts, vectra.WithMaxDepth(in.MaxDepth))
	}
	if len(in.Labels) > 0 {
		opts = append(opts, vectra.WithLabels(in.Labels...))
	}
	res, err := v.BFS(starts, opts...)
	if err != nil {
		return nil, depthOut{}, err
	}
	all := res.Nodes()
	out := depthOut{}
	out.Total = len(all)
	out.Depths = map[string]uint32{}
	for _, id := range all {
		n, _ := v.NodeByID(id)
		if len(out.Nodes) < limit {
			out.Nodes = append(out.Nodes, n.Key)
			out.Depths[n.Key] = res.Depth(id)
		}
	}
	out.Count = len(out.Nodes)
	return nil, out, nil
}

func (svc *service) shortestPath(_ context.Context, _ *mcp.CallToolRequest, in pathIn) (*mcp.CallToolResult, pathOut, error) {
	v, err := svc.viewOrSealHint()
	if err != nil {
		return nil, pathOut{}, err
	}
	p, found, err := v.ShortestPath(in.From, in.To)
	if err != nil {
		return nil, pathOut{}, err
	}
	if !found {
		return nil, pathOut{Found: false, Hops: 0}, nil
	}
	out := pathOut{Found: true, Hops: p.Len(), Nodes: make([]string, len(p.Nodes))}
	for i, id := range p.Nodes {
		n, _ := v.NodeByID(id)
		out.Nodes[i] = n.Key
	}
	return nil, out, nil
}

func (svc *service) reachable(_ context.Context, _ *mcp.CallToolRequest, in reachIn) (*mcp.CallToolResult, listOut, error) {
	v, err := svc.viewOrSealHint()
	if err != nil {
		return nil, listOut{}, err
	}
	limit := in.Limit
	if limit <= 0 {
		limit = defaultLimit
	}
	keys, err := v.Reachable(in.From)
	if err != nil {
		return nil, listOut{}, err
	}
	out := listOut{Total: len(keys)}
	for i, k := range keys {
		if i >= limit {
			break
		}
		out.Nodes = append(out.Nodes, k)
	}
	out.Count = len(out.Nodes)
	return nil, out, nil
}

func (svc *service) cycles(_ context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, cyclesOut, error) {
	v, err := svc.viewOrSealHint()
	if err != nil {
		return nil, cyclesOut{}, err
	}
	cs := v.Cycles()
	out := cyclesOut{Cycles: make([][]string, 0, len(cs))}
	for _, c := range cs {
		mem := make([]string, len(c.Nodes))
		for i, id := range c.Nodes {
			n, _ := v.NodeByID(id)
			mem[i] = n.Key
		}
		out.Cycles = append(out.Cycles, mem)
	}
	return nil, out, nil
}

func (svc *service) lookupProperty(_ context.Context, _ *mcp.CallToolRequest, in lookupIn) (*mcp.CallToolResult, listOut, error) {
	v, err := svc.viewOrSealHint()
	if err != nil {
		return nil, listOut{}, err
	}
	limit := in.Limit
	if limit <= 0 {
		limit = defaultLimit
	}
	nodes, ok := v.LookupByProperty(in.PropKey, in.Value)
	if !ok {
		return nil, listOut{}, fmt.Errorf("属性 %q 未建索引（需在注入数据中出现后重新 seal）", in.PropKey)
	}
	out := listOut{Total: len(nodes)}
	for i, n := range nodes {
		if i >= limit {
			break
		}
		out.Nodes = append(out.Nodes, n.Key)
	}
	out.Count = len(out.Nodes)
	return nil, out, nil
}
