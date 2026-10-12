// vectra-bench 是 Vectra 的压测工具：生成幂律依赖图 → 注入（引擎直写
// 或经 HTTP/raft）→ 参数固定化 → 并发压测单跳/多跳/路径/可达负载 →
// 输出 markdown 报告。数据落本地临时目录，运行后删除（--keep 保留）。
//
// 用法：
//
//	vectra-bench -size small                      # 引擎模式，千级图
//	vectra-bench -size medium -mode http          # 服务模式（HTTP+raft）
//	vectra-bench -size large -duration 10s -workers 16
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/projanvil/vectra"
	"github.com/projanvil/vectra/server"
	"github.com/projanvil/vectra/server/internal/bench"
)

type sizeSpec struct {
	name         string
	nodes, edges int
}

var sizes = map[string]sizeSpec{
	"small":  {"S", 1_000, 10_000},
	"medium": {"M", 100_000, 1_000_000},
	"large":  {"L", 1_000_000, 10_000_000},
}

const (
	nodeChunk = 20_000  // HTTP 注入的节点分批
	edgeChunk = 100_000 // HTTP 注入的边分批
)

func main() {
	size := flag.String("size", "small", "small|medium|large")
	mode := flag.String("mode", "engine", "engine|http")
	workers := flag.Int("workers", 8, "并发 worker 数")
	duration := flag.Duration("duration", 5*time.Second, "每负载压测时长")
	warmup := flag.Duration("warmup", 500*time.Millisecond, "每负载预热时长（丢弃）")
	seed := flag.Int64("seed", 42, "图与参数种子")
	datadir := flag.String("datadir", "", "数据目录（默认系统临时目录，结束删除）")
	keep := flag.Bool("keep", false, "保留数据目录（调试）")
	out := flag.String("out", "", "结果写入文件（默认 stdout）")
	sealInterval := flag.Duration("seal-interval", 2*time.Second, "http 模式发布节流间隔")
	workloads := flag.String("workloads", "", "逗号分隔的负载子集（默认全部）: neighbors,bfs2,bfs3,bfsall,path,reach")
	flag.Parse()

	spec, ok := sizes[*size]
	if !ok {
		fmt.Fprintf(os.Stderr, "未知 size: %s（small|medium|large）\n", *size)
		os.Exit(2)
	}

	dir := *datadir
	cleanup := true
	if dir == "" {
		tmp, err := os.MkdirTemp("", "vectra-bench-")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		dir = tmp
	} else {
		cleanup = !*keep
	}
	if *keep {
		cleanup = false
	}
	defer func() {
		if cleanup {
			os.RemoveAll(dir)
		} else {
			fmt.Fprintf(os.Stderr, "数据目录保留: %s\n", dir)
		}
	}()

	rep := newReport(spec, *mode, *workers, *duration, *seed)

	// 1. 生成
	t0 := time.Now()
	bg := bench.Generate(bench.Spec{Nodes: spec.nodes, Edges: spec.edges, Seed: *seed})
	rep.GenTime = time.Since(t0)

	// 2. 注入
	var queryGraph *vectra.Graph
	var baseURL string
	var httpc *http.Client
	var node *server.Node
	switch *mode {
	case "engine":
		queryGraph = injectEngine(rep, bg)
	case "http":
		baseURL, httpc, node = injectHTTP(rep, bg, dir, *sealInterval, *keep)
		defer node.Close()
		queryGraph = waitPublished(node, spec)
	default:
		fmt.Fprintf(os.Stderr, "未知 mode: %s\n", *mode)
		os.Exit(2)
	}

	// 3. 参数固定化
	t0 = time.Now()
	params := bench.Curate(queryGraph, bg, 64, *seed+1)
	rep.CurateTime = time.Since(t0)

	// 4. 负载
	ops := buildWorkloads(*mode, queryGraph, baseURL, httpc, params)
	selected := ops
	if *workloads != "" {
		want := map[string]bool{}
		for _, w := range strings.Split(*workloads, ",") {
			want[strings.TrimSpace(w)] = true
		}
		selected = selected[:0]
		for _, o := range ops {
			if want[o.name] {
				selected = append(selected, o)
			}
		}
	}
	for _, w := range selected {
		bench.RunWorkload(w.name, w.op, 1, *warmup) // 预热丢弃
		res := bench.RunWorkload(w.name, w.op, *workers, *duration)
		fmt.Fprintf(os.Stderr, "  %-10s ops=%-7d p50=%-9s p99=%s\n",
			res.Name, res.Ops, bench.FormatDur(res.P50), bench.FormatDur(res.P99))
		rep.Results = append(rep.Results, res)
	}

	// 5. 输出
	md := rep.markdown()
	if *out != "" {
		if err := os.WriteFile(*out, []byte(md), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "结果已写入 %s\n", *out)
	} else {
		fmt.Print(md)
	}
}

// ---- 注入 ----

func injectEngine(rep *report, bg *bench.Graph) *vectra.Graph {
	g := vectra.New()
	t0 := time.Now()
	for _, n := range bg.Nodes {
		_, _ = g.AddNode(n.Key, n.Label, n.Props)
	}
	rep.InjectNodesPerS = rate(len(bg.Nodes), time.Since(t0))
	t0 = time.Now()
	for _, e := range bg.Edges {
		_, _ = g.AddEdge(bg.Nodes[e.Src].Key, bg.Nodes[e.Dst].Key, e.Label, nil)
	}
	rep.InjectEdgesPerS = rate(len(bg.Edges), time.Since(t0))
	t0 = time.Now()
	if err := g.Seal(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	rep.SealTime = time.Since(t0)
	return g
}

func injectHTTP(rep *report, bg *bench.Graph, dir string, sealInterval time.Duration, keep bool) (string, *http.Client, *server.Node) {
	// 探测空闲端口后释放，交给 server.New 的 TCP transport 绑定
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	raftAddr := probe.Addr().String()
	probe.Close()
	httpLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	n, err := server.New(server.Config{
		NodeID:       "bench-1",
		HTTPAddr:     httpLn.Addr().String(),
		RaftAddr:     raftAddr,
		DataDir:      filepath.Join(dir, "node1"),
		SealInterval: sealInterval,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	go func() { _ = http.Serve(httpLn, n.Handler()) }()
	waitLeader(n)

	client := &http.Client{
		Timeout: 120 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        256,
			MaxIdleConnsPerHost: 256,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	base := "http://" + httpLn.Addr().String()

	// 节点分批注入
	t0 := time.Now()
	for i := 0; i < len(bg.Nodes); i += nodeChunk {
		end := min(i+nodeChunk, len(bg.Nodes))
		postJSONAndWait(client, base+"/v1/batch", map[string]any{
			"nodes": upsertNodes(bg.Nodes[i:end]),
		})
	}
	rep.InjectNodesPerS = rate(len(bg.Nodes), time.Since(t0))

	// 边分批注入
	t0 = time.Now()
	chunk := make([]bench.Edge, 0, edgeChunk)
	flush := func() {
		if len(chunk) > 0 {
			postJSONAndWait(client, base+"/v1/batch", map[string]any{
				"edges": upsertEdges(bg, chunk),
			})
			chunk = chunk[:0]
		}
	}
	for _, e := range bg.Edges {
		chunk = append(chunk, e)
		if len(chunk) == edgeChunk {
			flush()
		}
	}
	flush()
	rep.InjectEdgesPerS = rate(len(bg.Edges), time.Since(t0))
	return base, client, n
}

func upsertNodes(nodes []bench.Node) []server.NodeUpsert {
	out := make([]server.NodeUpsert, len(nodes))
	for i, n := range nodes {
		out[i] = server.NodeUpsert{Key: n.Key, Label: n.Label, Props: n.Props}
	}
	return out
}

func upsertEdges(bg *bench.Graph, edges []bench.Edge) []server.EdgeUpsert {
	out := make([]server.EdgeUpsert, len(edges))
	for i, e := range edges {
		out[i] = server.EdgeUpsert{
			SrcKey: bg.Nodes[e.Src].Key,
			DstKey: bg.Nodes[e.Dst].Key,
			Label:  e.Label,
		}
	}
	return out
}

func postJSONAndWait(c *http.Client, url string, body any) {
	data, _ := json.Marshal(body)
	resp, err := c.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "注入失败: %s\n", resp.Status)
		os.Exit(1)
	}
}

func waitLeader(n *server.Node) {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if n.IsLeader() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	fmt.Fprintln(os.Stderr, "单节点未在期限内当选 leader")
	os.Exit(1)
}

func waitPublished(n *server.Node, spec sizeSpec) *vectra.Graph {
	fmt.Fprintf(os.Stderr, "等待发布（含大图 Seal）...\n")
	deadline := time.Now().Add(5 * time.Minute)
	last := time.Now()
	for time.Now().Before(deadline) {
		if p := n.Published(); p != nil {
			st := p.Stats()
			if st.Nodes >= spec.nodes && st.Edges >= spec.edges {
				return p
			}
			if time.Since(last) > 10*time.Second {
				fmt.Fprintf(os.Stderr, "  已发布 %d/%d（等待注入数据全部发布）...\n", st.Nodes, st.Edges)
				last = time.Now()
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Fprintln(os.Stderr, "发布超时")
	os.Exit(1)
	return nil
}

func rate(n int, d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return float64(n) / d.Seconds()
}

// ---- 负载定义 ----

type workload struct {
	name string
	op   bench.Op
}

func buildWorkloads(mode string, g *vectra.Graph, base string, c *http.Client, p bench.QueryParams) []workload {
	if mode == "engine" {
		return engineWorkloads(g, p)
	}
	return httpWorkloads(base, c, p)
}

func engineWorkloads(g *vectra.Graph, p bench.QueryParams) []workload {
	nextStart, nextPair := rotators(p)
	_ = nextStart
	_ = nextPair
	return []workload{
		{"neighbors", func() error {
			_, err := g.NeighborsOf(nextStart(), vectra.DirOut)
			return err
		}},
		{"bfs2", func() error {
			n, _ := g.NodeByKey(nextStart())
			_, err := g.BFS([]vectra.NodeID{n.ID}, vectra.WithMaxDepth(2))
			return err
		}},
		{"bfs3", func() error {
			n, _ := g.NodeByKey(nextStart())
			_, err := g.BFS([]vectra.NodeID{n.ID}, vectra.WithMaxDepth(3))
			return err
		}},
		{"bfsall", func() error {
			n, _ := g.NodeByKey(nextStart())
			_, err := g.BFS([]vectra.NodeID{n.ID})
			return err
		}},
		{"path", func() error {
			pair := nextPair()
			_, _, err := g.ShortestPath(pair[0], pair[1])
			return err
		}},
		{"reach", func() error {
			_, err := g.Reachable(nextStart())
			return err
		}},
	}
}

func httpWorkloads(base string, c *http.Client, p bench.QueryParams) []workload {
	nextStart, nextPair := rotators(p)
	get := func(url string) error {
		resp, err := c.Get(url)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("GET %s: %s", url, resp.Status)
		}
		return nil
	}
	postQ := func(depth int, from string) error {
		body, _ := json.Marshal(map[string]any{
			"from":     []string{from},
			"maxDepth": depth,
		})
		resp, err := c.Post(base+"/v1/query", "application/json", bytes.NewReader(body))
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("query: %s", resp.Status)
		}
		return nil
	}
	return []workload{
		{"neighbors", func() error {
			return get(base + "/v1/neighbors?key=" + nextStart() + "&dir=out")
		}},
		{"bfs2", func() error { return postQ(2, nextStart()) }},
		{"bfs3", func() error { return postQ(3, nextStart()) }},
		{"bfsall", func() error { return postQ(0, nextStart()) }},
		{"path", func() error {
			pair := nextPair()
			return get(base + "/v1/path?from=" + pair[0] + "&to=" + pair[1])
		}},
		{"reach", func() error {
			return get(base + "/v1/reachable?from=" + nextStart())
		}},
	}
}

// ---- 报告 ----

type report struct {
	Size    sizeSpec
	Mode    string
	Workers int
	Dur     time.Duration
	Seed    int64

	GenTime         time.Duration
	InjectNodesPerS float64
	InjectEdgesPerS float64
	SealTime        time.Duration
	CurateTime      time.Duration
	Results         []bench.LoadResult
}

func newReport(s sizeSpec, mode string, workers int, dur time.Duration, seed int64) *report {
	return &report{Size: s, Mode: mode, Workers: workers, Dur: dur, Seed: seed}
}

func (r *report) markdown() string {
	var b strings.Builder
	fmt.Fprintf(&b, "## vectra-bench %s（%s 模式，%d workers × %s，seed=%d）\n\n",
		r.Size.name, r.Mode, r.Workers, r.Dur, r.Seed)
	fmt.Fprintf(&b, "- 图：%s 节点 / %s 边（幂律 BA + 5%% 回边）\n",
		intComma(r.Size.nodes), intComma(r.Size.edges))
	fmt.Fprintf(&b, "- 生成：%s；注入：%.0f 节点/s、%.0f 边/s；Seal：%s；参数固定化：%s\n\n",
		bench.FormatDur(r.GenTime), r.InjectNodesPerS, r.InjectEdgesPerS,
		bench.FormatDur(r.SealTime), bench.FormatDur(r.CurateTime))
	rows := make([][]string, 0, len(r.Results))
	for _, res := range r.Results {
		rows = append(rows, []string{
			res.Name, intComma(res.Ops), fmt.Sprintf("%.0f", res.QPS),
			bench.FormatDur(res.P50), bench.FormatDur(res.P90), bench.FormatDur(res.P99),
			bench.FormatDur(res.Max), intComma(res.Errors),
		})
	}
	b.WriteString(bench.MarkdownTable(
		[]string{"负载", "ops", "QPS", "p50", "p90", "p99", "max", "err"},
		rows,
	))
	return b.String()
}

func intComma(n int) string {
	s := fmt.Sprint(n)
	var parts []string
	for len(s) > 3 {
		parts = append([]string{s[len(s)-3:]}, parts...)
		s = s[:len(s)-3]
	}
	return strings.Join(append([]string{s}, parts...), ",")
}

// rotators 返回参数轮换闭包（原子计数取模）。
func rotators(p bench.QueryParams) (func() string, func() [2]string) {
	var i, j atomic.Int64
	nextStart := func() string { return p.Starts[i.Add(1)%int64(len(p.Starts))] }
	var empty [2]string
	if len(p.Pairs) == 0 {
		return nextStart, func() [2]string { return empty }
	}
	nextPair := func() [2]string { return p.Pairs[j.Add(1)%int64(len(p.Pairs))] }
	return nextStart, nextPair
}
