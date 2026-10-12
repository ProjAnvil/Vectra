package vectra_test

import (
	"io"
	"math/rand"
	"strconv"
	"testing"

	"vectra"
)

// ============================================================
// LDBC SNB Interactive 风格工作负载基准
//
// 模拟 SNB 的复杂读查询形态（从一到两个起点做 2-3 跳邻域、点对路径），
// 并采用其 parameter curation 方法：查询参数（起点/终点）在基准外
// 预先固定成代表性集合（高扇出节点、已验证可达对），而不是每次迭代
// 随机重抽——保证迭代间工作负载一致、数字可比。
// 数据集：SNAP email-Eu-core 真实邮件图（1005 节点 / 25571 边）。
// ============================================================

// curateSNBParameters 预先固定基准参数：
// - kHopSources: 出度前 16 的节点（对应 SNB 的"活跃用户"起点）
// - pathPairs: 64 对已验证可达的节点（对应 CR14 路径查询参数）
func curateSNBParameters(t testing.TB, g *vectra.Graph, el edgeList) (kHopSources []vectra.NodeID, pathPairs [][2]vectra.NodeID) {
	t.Helper()
	outDeg := make([]int, el.n)
	for _, s := range el.src {
		outDeg[s]++
	}
	idx := make([]int, el.n)
	for i := range idx {
		idx[i] = i
	}
	// 出度降序取前 16
	for i := 0; i < len(idx); i++ {
		for j := i + 1; j < len(idx); j++ {
			if outDeg[idx[j]] > outDeg[idx[i]] {
				idx[i], idx[j] = idx[j], idx[i]
			}
		}
	}
	for i := 0; i < 16 && i < el.n; i++ {
		kHopSources = append(kHopSources, vectra.NodeID(idx[i]))
	}
	// 固定种子抽 64 对，只保留可达对（参数筛选 = curation）
	rng := rand.New(rand.NewSource(31))
	for len(pathPairs) < 64 {
		u, v := vectra.NodeID(rng.Intn(el.n)), vectra.NodeID(rng.Intn(el.n))
		if u == v {
			continue
		}
		if _, found, _ := g.ShortestPathIDs(u, v); found {
			pathPairs = append(pathPairs, [2]vectra.NodeID{u, v})
		}
	}
	return kHopSources, pathPairs
}

func benchSNB(b *testing.B) (*vectra.Graph, []vectra.NodeID, [][2]vectra.NodeID) {
	b.Helper()
	g, el := loadSNAPEmail(b)
	srcs, pairs := curateSNBParameters(b, g, el)
	return g, srcs, pairs
}

// bytesBuffer 是可重复 Read 的内存缓冲（bytes.Buffer 读后不可复用）。
type bytesBuffer struct {
	data []byte
	off  int
}

func (b *bytesBuffer) Write(p []byte) (int, error) {
	b.data = append(b.data, p...)
	return len(p), nil
}

func (b *bytesBuffer) Read(p []byte) (int, error) {
	if b.off >= len(b.data) {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.off:])
	b.off += n
	return n, nil
}

// BenchmarkSNB_kHop2/3 对应 SNB 复杂读的邻域发现类查询（CR3/CR9 形态）：
// 高扇出起点的 2/3 跳邻域计数。
func BenchmarkSNB_kHop2(b *testing.B) {
	g, srcs, _ := benchSNB(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := g.BFS([]vectra.NodeID{srcs[i%len(srcs)]}, vectra.WithMaxDepth(2))
		if err != nil {
			b.Fatal(err)
		}
		if res.Count() < 10 {
			b.Fatalf("高扇出起点 2 跳邻域异常小: %d", res.Count())
		}
	}
}

func BenchmarkSNB_kHop3(b *testing.B) {
	g, srcs, _ := benchSNB(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := g.BFS([]vectra.NodeID{srcs[i%len(srcs)]}, vectra.WithMaxDepth(3))
		if err != nil {
			b.Fatal(err)
		}
		if res.Count() < 10 {
			b.Fatalf("高扇出起点 3 跳邻域异常小: %d", res.Count())
		}
	}
}

// BenchmarkSNB_ShortestPath 对应 CR14：两节点间最短路径（双向 BFS）。
func BenchmarkSNB_ShortestPath(b *testing.B) {
	g, _, pairs := benchSNB(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := pairs[i%len(pairs)]
		if _, found, err := g.ShortestPathIDs(p[0], p[1]); err != nil || !found {
			b.Fatalf("curated pair 不可达: %v", p)
		}
	}
}

// BenchmarkSNB_Reachability 可达性判定（超节点快路径）。
func BenchmarkSNB_Reachability(b *testing.B) {
	g, srcs, _ := benchSNB(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.ReachableIDs(srcs[i%len(srcs)]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSNB_MultiSourceBFS 多源批量遍历（一个需求对应多个入口服务
// 的调用图场景）：16 个高扇出起点一次 BFS。
func BenchmarkSNB_MultiSourceBFS(b *testing.B) {
	g, srcs, _ := benchSNB(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := g.BFS(srcs, vectra.WithParallelThreshold(0))
		if err != nil {
			b.Fatal(err)
		}
		if res.Count() < 800 {
			b.Fatalf("多源邻域异常小: %d", res.Count())
		}
	}
}

// BenchmarkSNB_LoadSnapshot 冷启动路径：快照加载 + 自动 Seal 的吞吐
// （对应服务模式重启恢复成本）。
func BenchmarkSNB_LoadSnapshot(b *testing.B) {
	g := vectra.New()
	for i := 0; i < 1005; i++ {
		_, _ = g.AddNode(strconv.Itoa(i), "V", nil)
	}
	for i := 0; i < 25571; i++ {
		_, _ = g.AddEdge(strconv.Itoa(i%1005), strconv.Itoa((i*7+3)%1005), "E", nil)
	}
	if err := g.Seal(); err != nil {
		b.Fatal(err)
	}
	var buf bytesBuffer
	if err := g.Save(&buf); err != nil {
		b.Fatal(err)
	}
	snap := buf.data
	b.SetBytes(int64(len(snap)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := vectra.Load(&bytesBuffer{data: snap}); err != nil {
			b.Fatal(err)
		}
	}
}
