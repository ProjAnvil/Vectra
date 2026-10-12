package vectra_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"

	"vectra"
)

// ============================================================
// 鲁棒性维度（对应 LDBC Graphalytics 的 robustness 轴）
// ============================================================

// TestConcurrentReadersMixedQueries 验证核心声明："Seal 后不可变，
// 并发读免锁"。8 个 goroutine 混跑全部只读 API（含并行 BFS、快照导出），
// 在 -race 下无数据竞争即通过。任何一个查询结果与其他 goroutine
// 的查询交错后仍必须自洽（与单线程基准值比对）。
func TestConcurrentReadersMixedQueries(t *testing.T) {
	g := randGraph(t, 5000, 50000)
	// 单线程基准值
	baseReach, err := g.ReachableIDs(0)
	if err != nil {
		t.Fatal(err)
	}
	basePath, _, err := g.ShortestPathIDs(0, 4999)
	if err != nil {
		t.Fatal(err)
	}
	baseBFS, err := g.BFS([]vectra.NodeID{0}, vectra.WithParallelThreshold(0))
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w)))
			for i := 0; i < 30; i++ {
				var err error
				switch rng.Intn(5) {
				case 0: // 并行 BFS
					res, e := g.BFS([]vectra.NodeID{0}, vectra.WithParallelThreshold(0))
					if e != nil {
						err = e
					} else if res.Count() != baseBFS.Count() {
						err = fmt.Errorf("并发 BFS count=%d 基准=%d", res.Count(), baseBFS.Count())
					}
				case 1: // 可达性
					r, e := g.ReachableIDs(0)
					if e != nil {
						err = e
					} else if len(r) != len(baseReach) {
						err = fmt.Errorf("并发 Reachable=%d 基准=%d", len(r), len(baseReach))
					}
				case 2: // 最短路径
					p, found, e := g.ShortestPathIDs(0, 4999)
					if e != nil {
						err = e
					} else if !found || p.Len() != basePath.Len() {
						err = fmt.Errorf("并发最短路 len=%d 基准=%d", p.Len(), basePath.Len())
					}
				case 3: // 环清单
					if cs := g.Cycles(); cs == nil && g.Stats().CyclicNodes > 0 {
						err = fmt.Errorf("Cycles() 为空但 CyclicNodes>0")
					}
				case 4: // 快照导出到各自缓冲
					var buf bytes.Buffer
					if e := g.Save(&buf); e != nil {
						err = e
					} else if buf.Len() == 0 {
						err = fmt.Errorf("空快照")
					}
				}
				if err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestDegenerateGraphs 退化形态表驱动：每种形态 Seal 后全部查询
// 必须给出结构正确的答案（空图、孤立点、稠密团、链、星、多边、自环海）。
func TestDegenerateGraphs(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		g := vectra.New()
		if err := g.Seal(); err != nil {
			t.Fatal(err)
		}
		if st := g.Stats(); st.Nodes != 0 || st.Edges != 0 || st.SCCs != 0 {
			t.Errorf("空图 stats = %+v", st)
		}
		if cs := g.Cycles(); len(cs) != 0 {
			t.Errorf("空图 Cycles = %d", len(cs))
		}
		res, err := g.BFS(nil)
		if err != nil || res.Count() != 0 {
			t.Errorf("空图 BFS = %d err=%v", res.Count(), err)
		}
		var buf bytes.Buffer
		if err := g.Save(&buf); err != nil {
			t.Fatal(err)
		}
		if g2, err := vectra.Load(&buf); err != nil || g2.Stats() != g.Stats() {
			t.Errorf("空图快照往返失败: %v", err)
		}
	})
	t.Run("SingleIsolatedNode", func(t *testing.T) {
		g := vectra.New()
		if _, err := g.AddNode("only", "V", nil); err != nil {
			t.Fatal(err)
		}
		if err := g.Seal(); err != nil {
			t.Fatal(err)
		}
		if st := g.Stats(); st.Nodes != 1 || st.SCCs != 1 || st.CyclicNodes != 0 {
			t.Errorf("stats = %+v", st)
		}
		r, err := g.ReachableIDs(0)
		if err != nil || len(r) != 1 {
			t.Errorf("Reachable = %v err=%v", r, err)
		}
	})
	t.Run("SingleSelfLoop", func(t *testing.T) {
		g := vectra.New()
		if _, err := g.AddNode("a", "V", nil); err != nil {
			t.Fatal(err)
		}
		if _, err := g.AddEdge("a", "a", "E", nil); err != nil {
			t.Fatal(err)
		}
		if err := g.Seal(); err != nil {
			t.Fatal(err)
		}
		cs := g.Cycles()
		if len(cs) != 1 || cs[0].Len() != 1 {
			t.Errorf("自环 Cycles = %v", cs)
		}
	})
	t.Run("ParallelMultiEdges", func(t *testing.T) {
		g := vectra.New()
		for _, k := range []string{"a", "b"} {
			if _, err := g.AddNode(k, "V", nil); err != nil {
				t.Fatal(err)
			}
		}
		for i := 0; i < 5; i++ {
			if _, err := g.AddEdge("a", "b", "E", nil); err != nil {
				t.Fatal(err)
			}
		}
		if err := g.Seal(); err != nil {
			t.Fatal(err)
		}
		if g.EdgeCount() != 5 {
			t.Errorf("多边保留失败: %d", g.EdgeCount())
		}
		res, err := g.BFS([]vectra.NodeID{0})
		if err != nil || res.Count() != 2 {
			t.Errorf("多边 BFS count = %d err=%v", res.Count(), err)
		}
		p, found, err := g.ShortestPathIDs(0, 1)
		if err != nil || !found || p.Len() != 1 {
			t.Errorf("多边最短路 = %v found=%v", p, found)
		}
		ns, _ := g.NeighborsOf("a", vectra.DirOut)
		if len(ns) != 1 {
			t.Errorf("一跳邻居应去重: %d", len(ns))
		}
	})
	t.Run("Clique50", func(t *testing.T) {
		n := 50
		g := vectra.New()
		for i := 0; i < n; i++ {
			_, _ = g.AddNode(strconv.Itoa(i), "V", nil)
		}
		for i := 0; i < n; i++ {
			for j := 0; j < n; j++ {
				if i != j {
					_, _ = g.AddEdge(strconv.Itoa(i), strconv.Itoa(j), "E", nil)
				}
			}
		}
		if err := g.Seal(); err != nil {
			t.Fatal(err)
		}
		st := g.Stats()
		if st.SCCs != 1 || st.CyclicNodes != n {
			t.Errorf("完全图应为单一 SCC: %+v", st)
		}
		p, found, _ := g.ShortestPathIDs(0, vectra.NodeID(n-1))
		if !found || p.Len() != 1 {
			t.Errorf("完全图任意两点应 1 跳: %v", p)
		}
	})
	t.Run("Star1000", func(t *testing.T) {
		g := vectra.New()
		_, _ = g.AddNode("c", "V", nil)
		for i := 0; i < 1000; i++ {
			_, _ = g.AddNode(strconv.Itoa(i), "V", nil)
			_, _ = g.AddEdge("c", strconv.Itoa(i), "E", nil)
		}
		if err := g.Seal(); err != nil {
			t.Fatal(err)
		}
		res, err := g.BFS([]vectra.NodeID{0}, vectra.WithParallelThreshold(0)) // 中心 ID=0，强制并行
		if err != nil || res.Count() != 1001 {
			t.Errorf("星形 BFS = %d err=%v", res.Count(), err)
		}
		if st := g.Stats(); st.SCCs != 1001 || st.CyclicNodes != 0 {
			t.Errorf("星形应无环: %+v", st)
		}
	})
	t.Run("AllSelfLoops", func(t *testing.T) {
		g := vectra.New()
		for i := 0; i < 100; i++ {
			k := strconv.Itoa(i)
			_, _ = g.AddNode(k, "V", nil)
			_, _ = g.AddEdge(k, k, "E", nil)
		}
		if err := g.Seal(); err != nil {
			t.Fatal(err)
		}
		if cs := g.Cycles(); len(cs) != 100 {
			t.Errorf("全自环 Cycles = %d, want 100", len(cs))
		}
		if st := g.Stats(); st.SCCs != 100 || st.CyclicNodes != 0 {
			t.Errorf("自环不计入 CyclicNodes: %+v", st)
		}
	})
}

// TestDeepChain1M 百万节点链：迭代 Tarjan 的调用栈压力
// （递归实现会在此类深图上膨胀栈）+ 全链 BFS 深度正确性 + 双向搜索。
// 耗时约数秒，-short 模式跳过。
func TestDeepChain1M(t *testing.T) {
	if testing.Short() {
		t.Skip("short 模式跳过百万节点压力测试")
	}
	const n = 1_000_000
	g := vectra.New()
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = strconv.Itoa(i)
		_, _ = g.AddNode(keys[i], "V", nil)
	}
	for i := 0; i+1 < n; i++ {
		_, _ = g.AddEdge(keys[i], keys[i+1], "E", nil)
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	if st := g.Stats(); st.Nodes != n || st.SCCs != n {
		t.Errorf("链 stats = %+v", st)
	}
	ser, err := g.BFS([]vectra.NodeID{0}, vectra.WithParallelThreshold(1<<30))
	if err != nil {
		t.Fatal(err)
	}
	if ser.Count() != n {
		t.Errorf("链 BFS count = %d, want %d", ser.Count(), n)
	}
	if d := ser.Depth(vectra.NodeID(n - 1)); d != n-1 {
		t.Errorf("链尾深度 = %d, want %d", d, n-1)
	}
	par, err := g.BFS([]vectra.NodeID{0}, vectra.WithParallelThreshold(0))
	if err != nil {
		t.Fatal(err)
	}
	if par.Count() != ser.Count() || par.Depth(vectra.NodeID(n-1)) != ser.Depth(vectra.NodeID(n-1)) {
		t.Error("深链上并行与串行结果不一致")
	}
	// 双向搜索在最长链上：1M 跳路径，长度精确等于中点距离
	p, found, err := g.ShortestPathIDs(0, vectra.NodeID(n-1))
	if err != nil || !found {
		t.Fatalf("链最短路: found=%v err=%v", found, err)
	}
	if p.Len() != n-1 {
		t.Errorf("链最短路 = %d, want %d", p.Len(), n-1)
	}
	r, err := g.ReachableIDs(0)
	if err != nil || len(r) != n {
		t.Errorf("链 Reachable = %d err=%v", len(r), err)
	}
}

// TestDeterminismSameSeedSameResults 同种子构建两次，全部查询结果
// 逐位一致（LDBC 基准的 validate 模式要求可重复性）。
func TestDeterminismSameSeedSameResults(t *testing.T) {
	build := func() *vectra.Graph {
		g := vectra.New()
		rng := rand.New(rand.NewSource(2024))
		for i := 0; i < 2000; i++ {
			_, _ = g.AddNode(strconv.Itoa(i), "V", nil)
		}
		for i := 0; i < 20000; i++ {
			_, _ = g.AddEdge(strconv.Itoa(rng.Intn(2000)), strconv.Itoa(rng.Intn(2000)), "E", nil)
		}
		if err := g.Seal(); err != nil {
			t.Fatal(err)
		}
		return g
	}
	g1, g2 := build(), build()
	if g1.Stats() != g2.Stats() {
		t.Fatalf("stats 不一致: %+v vs %+v", g1.Stats(), g2.Stats())
	}
	c1, c2 := g1.Cycles(), g2.Cycles()
	if len(c1) != len(c2) {
		t.Fatalf("Cycles 数不一致")
	}
	for i := range c1 {
		for j := range c1[i].Nodes {
			if c1[i].Nodes[j] != c2[i].Nodes[j] {
				t.Fatalf("Cycles[%d] 成员不一致", i)
			}
		}
	}
	r1, _ := g1.ReachableIDs(7)
	r2, _ := g2.ReachableIDs(7)
	for i := range r1 {
		if r1[i] != r2[i] {
			t.Fatalf("Reachable 结果在第 %d 位不一致", i)
		}
	}
	p1, ok1, _ := g1.ShortestPathIDs(3, 1997)
	p2, ok2, _ := g2.ShortestPathIDs(3, 1997)
	if ok1 != ok2 {
		t.Fatal("可达性判定不一致")
	}
	for i := range p1.Nodes {
		if p1.Nodes[i] != p2.Nodes[i] {
			t.Fatalf("最短路径在第 %d 跳不一致", i)
		}
	}
}
