package vectra_test

import (
	"compress/gzip"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/projanvil/vectra"
)

// ============================================================
// 独立参考实现（交叉验证用）
//
// 刻意用与 Vectra 完全不同的朴素写法（map 邻接 + 逐点遍历），
// 不共享任何生产代码路径。LDBC Graphalytics 的验证方法：
// 内核输出与参考实现逐一比对。
// ============================================================

// refBFS 朴素 map 邻接 BFS，返回 深度表（含起点 0）。
func refBFS(adj map[int][]int, start int) map[int]int {
	dist := map[int]int{start: 0}
	frontier := []int{start}
	for len(frontier) > 0 {
		var next []int
		for _, u := range frontier {
			for _, v := range adj[u] {
				if _, ok := dist[v]; !ok {
					dist[v] = dist[u] + 1
					next = append(next, v)
				}
			}
		}
		frontier = next
	}
	return dist
}

// refReach 计算全部节点的可达性闭包（n 越大越慢，限小图）。
func refReachAll(n int, adj map[int][]int) []map[int]bool {
	reach := make([]map[int]bool, n)
	for s := 0; s < n; s++ {
		reach[s] = map[int]bool{s: true}
		frontier := []int{s}
		for len(frontier) > 0 {
			var next []int
			for _, u := range frontier {
				for _, v := range adj[u] {
					if !reach[s][v] {
						reach[s][v] = true
						next = append(next, v)
					}
				}
			}
			frontier = next
		}
	}
	return reach
}

// refSCCBrute 用可达闭包定义直接求 SCC：u~v 当且仅当互达。
// O(V·E)，只用于小图（≤300 节点），作为 Tarjan 的独立对照组。
func refSCCBrute(n int, adj map[int][]int) [][]int {
	reach := refReachAll(n, adj)
	assigned := make([]bool, n)
	var sccs [][]int
	for u := 0; u < n; u++ {
		if assigned[u] {
			continue
		}
		comp := []int{u}
		assigned[u] = true
		for v := u + 1; v < n; v++ {
			if !assigned[v] && reach[u][v] && reach[v][u] {
				comp = append(comp, v)
				assigned[v] = true
			}
		}
		sccs = append(sccs, comp)
	}
	return sccs
}

// ============================================================
// 测试图生成器：多种拓扑形态（不同形态压不同的代码路径）
// ============================================================

type edgeList struct {
	src, dst []int
	n        int
}

func (e edgeList) adj() map[int][]int {
	adj := map[int][]int{}
	for i := range e.src {
		adj[e.src[i]] = append(adj[e.src[i]], e.dst[i])
	}
	return adj
}

func (e edgeList) graph(t *testing.T) *vectra.Graph {
	t.Helper()
	g := vectra.New()
	for v := 0; v < e.n; v++ {
		if _, err := g.AddNode(strconv.Itoa(v), "V", nil); err != nil {
			t.Fatal(err)
		}
	}
	for i := range e.src {
		if _, err := g.AddEdge(strconv.Itoa(e.src[i]), strconv.Itoa(e.dst[i]), "E", nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	return g
}

// erGraph 稀疏随机图（Erdős–Rényi，多种子）。
func erGraph(n int, p float64, seed int64) edgeList {
	rng := rand.New(rand.NewSource(seed))
	var e edgeList
	e.n = n
	for u := 0; u < n; u++ {
		for v := 0; v < n; v++ {
			if rng.Float64() < p {
				e.src = append(e.src, u)
				e.dst = append(e.dst, v)
			}
		}
	}
	return e
}

// layeredDAG 分层 DAG（层内无边、层间前向边）：无环、多源多汇。
func layeredDAG(layers []int, crossP float64, seed int64) edgeList {
	rng := rand.New(rand.NewSource(seed))
	var e edgeList
	e.n = 0
	offsets := make([]int, len(layers)+1)
	for i, size := range layers {
		offsets[i] = e.n
		e.n += size
	}
	offsets[len(layers)] = e.n
	for l := 0; l+1 < len(layers); l++ {
		for u := offsets[l]; u < offsets[l+1]; u++ {
			for v := offsets[l+1]; v < offsets[l+2]; v++ {
				if rng.Float64() < crossP {
					e.src = append(e.src, u)
					e.dst = append(e.dst, v)
				}
			}
		}
	}
	return e
}

// ringOfClusters k 个簇，簇内成环（大 SCC），簇间单向连接。
func ringOfClusters(clusters, size int) edgeList {
	var e edgeList
	e.n = clusters * size
	for c := 0; c < clusters; c++ {
		base := c * size
		for i := 0; i < size; i++ {
			e.src = append(e.src, base+i)
			e.dst = append(e.dst, base+(i+1)%size)
		}
		if c+1 < clusters {
			e.src = append(e.src, base)
			e.dst = append(e.dst, (c+1)*size)
		}
	}
	return e
}

var propertyShapes = []struct {
	name string
	make func() edgeList
}{
	{"ER-p0.02-seed1", func() edgeList { return erGraph(300, 0.02, 1) }},
	{"ER-p0.08-seed2", func() edgeList { return erGraph(300, 0.08, 2) }},
	{"layeredDAG", func() edgeList { return layeredDAG([]int{10, 80, 150, 60}, 0.15, 3) }},
	{"ringOfClusters-5x20", func() edgeList { return ringOfClusters(5, 20) }},
	{"denseER-p0.3-seed5", func() edgeList { return erGraph(200, 0.3, 5) }},
}

// TestPropertyBFSMatchesReference 每个形态、全部起点：
// Vectra BFS 深度（串行与并行）必须与参考 BFS 逐点一致。
// 这正是 Graphalytics BFS 内核的验证方式（输出 = 逐点 level 向量）。
func TestPropertyBFSMatchesReference(t *testing.T) {
	for _, shape := range propertyShapes {
		t.Run(shape.name, func(t *testing.T) {
			el := shape.make()
			g := el.graph(t)
			adj := el.adj()
			for s := 0; s < el.n; s++ {
				want := refBFS(adj, s)
				ser, err := g.BFS([]vectra.NodeID{vectra.NodeID(s)}, vectra.WithParallelThreshold(1<<30))
				if err != nil {
					t.Fatal(err)
				}
				par, err := g.BFS([]vectra.NodeID{vectra.NodeID(s)}, vectra.WithParallelThreshold(0))
				if err != nil {
					t.Fatal(err)
				}
				if ser.Count() != len(want) || par.Count() != len(want) {
					t.Fatalf("source %d: serial=%d parallel=%d reference=%d",
						s, ser.Count(), par.Count(), len(want))
				}
				for v, d := range want {
					if ser.Depth(vectra.NodeID(v)) != uint32(d) {
						t.Fatalf("source %d -> %d: serial depth=%d want %d",
							s, v, ser.Depth(vectra.NodeID(v)), d)
					}
					if par.Depth(vectra.NodeID(v)) != uint32(d) {
						t.Fatalf("source %d -> %d: parallel depth=%d want %d",
							s, v, par.Depth(vectra.NodeID(v)), d)
					}
				}
			}
		})
	}
}

// TestPropertyReachableMatchesReference 可达集与参考 BFS 逐点一致。
func TestPropertyReachableMatchesReference(t *testing.T) {
	for _, shape := range propertyShapes {
		t.Run(shape.name, func(t *testing.T) {
			el := shape.make()
			g := el.graph(t)
			adj := el.adj()
			for _, s := range []int{0, 1, el.n / 2, el.n - 1} {
				want := refBFS(adj, s)
				got, err := g.ReachableIDs(vectra.NodeID(s))
				if err != nil {
					t.Fatal(err)
				}
				if len(got) != len(want) {
					t.Fatalf("source %d: Reachable=%d reference=%d", s, len(got), len(want))
				}
				for _, id := range got {
					if _, ok := want[int(id)]; !ok {
						t.Fatalf("source %d: node %d in Reachable but not in reference set", s, id)
					}
				}
			}
		})
	}
}

// TestPropertyShortestPathMatchesReference 每形态抽样 150 对：
// 双向 BFS 路径长度 == 参考 BFS 距离，且路径连续、边真实存在。
func TestPropertyShortestPathMatchesReference(t *testing.T) {
	for _, shape := range propertyShapes {
		t.Run(shape.name, func(t *testing.T) {
			el := shape.make()
			g := el.graph(t)
			adj := el.adj()
			rng := rand.New(rand.NewSource(99))
			for i := 0; i < 150; i++ {
				u, v := rng.Intn(el.n), rng.Intn(el.n)
				want := refBFS(adj, u)
				p, found, err := g.ShortestPathIDs(vectra.NodeID(u), vectra.NodeID(v))
				if err != nil {
					t.Fatal(err)
				}
				if u == v {
					continue
				}
				if d, ok := want[v]; ok {
					if !found {
						t.Fatalf("%d->%d: reference dist=%d but not found", u, v, d)
					}
					if p.Len() != d {
						t.Fatalf("%d->%d: path len=%d reference dist=%d", u, v, p.Len(), d)
					}
					// 路径合法性与最优性：每步边存在；逐前缀重算距离应等于剩余长度
					for j := 0; j+1 < len(p.Nodes); j++ {
						e, ok := g.EdgeByID(p.Edges[j])
						if !ok || e.Src != p.Nodes[j] || e.Dst != p.Nodes[j+1] {
							t.Fatalf("%d->%d: edge %d mismatch on path", u, v, j)
						}
					}
				} else if found {
					t.Fatalf("%d->%d: found path but reference says unreachable", u, v)
				}
			}
		})
	}
}

// TestPropertyCyclesMatchBruteForceSCC Cycles() 的 SCC 划分与
// 可达闭包暴力定义完全一致（多环形态 + 稀疏随机形态）。
func TestPropertyCyclesMatchBruteForceSCC(t *testing.T) {
	for _, shape := range propertyShapes {
		t.Run(shape.name, func(t *testing.T) {
			el := shape.make()
			g := el.graph(t)
			adj := el.adj()
			want := refSCCBrute(el.n, adj)

			var wantMulti [][]int // 参考：大小>1 的分量
			selfLoops := map[int]bool{}
			for i := range el.src {
				if el.src[i] == el.dst[i] {
					selfLoops[el.src[i]] = true
				}
			}
			for _, comp := range want {
				if len(comp) > 1 {
					wantMulti = append(wantMulti, comp)
				}
			}

			got := g.Cycles()
			// SCC 环 + 自环节点
			wantCycleCount := len(wantMulti) + len(selfLoops)
			if len(got) != wantCycleCount {
				t.Fatalf("Cycles()=%d want %d (%d multi-SCC + %d self-loops)",
					len(got), wantCycleCount, len(wantMulti), len(selfLoops))
			}
			gotSets := map[string]bool{}
			for _, c := range got {
				keys := make([]string, len(c.Nodes))
				for i, id := range c.Nodes {
					keys[i] = strconv.Itoa(int(id))
				}
				gotSets[strings.Join(keys, ",")] = true
			}
			for _, comp := range wantMulti {
				keys := make([]string, len(comp))
				for i, id := range comp {
					keys[i] = strconv.Itoa(id)
				}
				if !gotSets[strings.Join(keys, ",")] {
					t.Fatalf("missing SCC %v in Cycles(); got %v", comp, gotSets)
				}
			}
		})
	}
}

// TestPropertyStatsSCCCount 一致性：Stats.SCCs == 参考分量总数。
func TestPropertyStatsSCCCount(t *testing.T) {
	for _, shape := range propertyShapes {
		t.Run(shape.name, func(t *testing.T) {
			el := shape.make()
			g := el.graph(t)
			want := len(refSCCBrute(el.n, el.adj()))
			if got := g.Stats().SCCs; got != want {
				t.Fatalf("Stats.SCCs=%d want %d", got, want)
			}
		})
	}
}

// ============================================================
// SNAP 真实数据集加载
// ============================================================

// loadSNAPEmail 读入 gzip 边表构建图，返回图与边表（供参考实现使用）。
func loadSNAPEmail(t testing.TB) (*vectra.Graph, edgeList) {
	t.Helper()
	f, err := os.Open("testdata/email-Eu-core.txt.gz")
	if err != nil {
		t.Fatalf("打开数据集失败（testdata/email-Eu-core.txt.gz）: %v", err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer zr.Close()

	el := edgeList{n: 1005}
	dec := make(map[string]vectra.NodeID, 1005)
	g := vectra.New()
	ensure := func(id int) vectra.NodeID {
		key := strconv.Itoa(id)
		if nid, ok := dec[key]; ok {
			return nid
		}
		nid, err := g.AddNode(key, "Person", nil)
		if err != nil {
			t.Fatal(err)
		}
		dec[key] = nid
		return nid
	}
	line := 0
	for {
		var u, v int
		if _, err := fmt.Fscan(zr, &u, &v); err != nil {
			break
		}
		line++
		ensure(u)
		ensure(v)
		if _, err := g.AddEdge(strconv.Itoa(u), strconv.Itoa(v), "EMAIL", nil); err != nil {
			t.Fatal(err)
		}
		el.src = append(el.src, u)
		el.dst = append(el.dst, v)
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	if line != 25571 {
		t.Fatalf("读到 %d 行，期望 25571", line)
	}
	return g, el
}
