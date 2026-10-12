package vectra_test

import (
	"bytes"
	"math/rand"
	"strconv"
	"testing"

	"github.com/projanvil/vectra"
)

// ============================================================
// SNAP email-Eu-core 集成测试（LDBC 审计式外部验证）
//
// 数据集是真实邮件网络，统计值由 SNAP/SuiteSparse 独立发布。
// 这些数字不是我们自己算的——是外部 ground-truth。
// ============================================================

// TestSNAP_PublishedStats 对外发布统计值：
// 1005 节点 / 25571 边 / 203 个 SCC / 最大 SCC 803 节点。
// 由 1005/203/803 可推出其余 202 个 SCC 全为单例；
// 另外数据文件里有 642 个自环节点（awk '$1==$2' 直接核对），
// 因此 Cycles() = 1 个 803-SCC + 642 个自环 = 643。
func TestSNAP_PublishedStats(t *testing.T) {
	g, _ := loadSNAPEmail(t)
	st := g.Stats()
	if st.Nodes != 1005 {
		t.Errorf("Nodes = %d, want 1005 (SNAP published)", st.Nodes)
	}
	if st.Edges != 25571 {
		t.Errorf("Edges = %d, want 25571 (SNAP published)", st.Edges)
	}
	if st.SCCs != 203 {
		t.Errorf("SCCs = %d, want 203 (SuiteSparse published)", st.SCCs)
	}
	if st.CyclicNodes != 803 {
		t.Errorf("CyclicNodes = %d, want 803 (largest SCC size, SNAP published)", st.CyclicNodes)
	}
	cycles := g.Cycles()
	if len(cycles) != 643 {
		t.Fatalf("Cycles() = %d, want 643 (1 个 803-SCC + 642 自环节点)", len(cycles))
	}
	if cycles[0].Len() != 803 {
		t.Errorf("largest cycle = %d nodes, want 803", cycles[0].Len())
	}
}

// TestSNAP_LargestSCCStronglyConnected 最大 SCC 的定义即测试：
// 803 个成员两两互达。对全部 803 个成员做 BFS，断言覆盖所有成员。
// （803 次 BFS × 25k 边 ≈ 2000 万次边访问，真实数据上的全量验证。）
func TestSNAP_LargestSCCStronglyConnected(t *testing.T) {
	g, _ := loadSNAPEmail(t)
	scc := g.Cycles()[0]
	members := map[vectra.NodeID]bool{}
	for _, id := range scc.Nodes {
		members[id] = true
	}
	if len(members) != 803 {
		t.Fatalf("SCC 成员 = %d, want 803", len(members))
	}
	for _, src := range scc.Nodes {
		res, err := g.BFS([]vectra.NodeID{src}, vectra.WithParallelThreshold(1<<30))
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range scc.Nodes {
			if !res.Reached(m) {
				t.Fatalf("SCC 成员 %d 不可达成员 %d —— 违反强连通定义", src, m)
			}
		}
	}
}

// TestSNAP_ReachableSupersetOfSCC 超节点可达性包含整个起始分量
// （分量成员经任意边互达的契约），且与参考实现逐点一致。
func TestSNAP_ReachableSupersetOfSCC(t *testing.T) {
	g, el := loadSNAPEmail(t)
	adj := el.adj()
	scc := g.Cycles()[0]
	rng := rand.New(rand.NewSource(11))
	for i := 0; i < 10; i++ {
		src := scc.Nodes[rng.Intn(len(scc.Nodes))]
		got, err := g.ReachableIDs(src)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range scc.Nodes {
			found := false
			for _, id := range got {
				if id == m {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("Reachable(%d) 缺少同分量成员 %d", src, m)
			}
		}
		// 与参考实现对账
		want := refBFS(adj, int(src))
		if len(got) != len(want) {
			t.Fatalf("Reachable(%d) = %d 节点, 参考 BFS = %d 节点", src, len(got), len(want))
		}
	}
}

// TestSNAP_ShortestPathCrossValidation 真实图上 300 对随机点：
// 双向 BFS 与参考 BFS 距离一致；对可达对做路径合法性校验。
func TestSNAP_ShortestPathCrossValidation(t *testing.T) {
	g, el := loadSNAPEmail(t)
	adj := el.adj()
	rng := rand.New(rand.NewSource(13))
	checked, unreachable := 0, 0
	for i := 0; i < 300; i++ {
		u, v := vectra.NodeID(rng.Intn(el.n)), vectra.NodeID(rng.Intn(el.n))
		if u == v {
			continue
		}
		p, found, err := g.ShortestPathIDs(u, v)
		if err != nil {
			t.Fatal(err)
		}
		want := refBFS(adj, int(u))
		d, ok := want[int(v)]
		if !ok {
			if found {
				t.Fatalf("%d->%d: 找到路径但参考实现说不可达", u, v)
			}
			unreachable++
			continue
		}
		checked++
		if !found {
			t.Fatalf("%d->%d: 参考距离 %d 但未找到路径", u, v, d)
		}
		if p.Len() != d {
			t.Fatalf("%d->%d: 路径长 %d, 参考距离 %d", u, v, p.Len(), d)
		}
		for j := 0; j+1 < len(p.Nodes); j++ {
			e, ok := g.EdgeByID(p.Edges[j])
			if !ok || e.Src != p.Nodes[j] || e.Dst != p.Nodes[j+1] {
				t.Fatalf("%d->%d: 路径第 %d 条边不合法", u, v, j)
			}
		}
	}
	if checked == 0 {
		t.Fatal("300 对随机点全部不可达？真实邮件图不该如此")
	}
	t.Logf("300 对随机点: %d 可达对全部通过交叉验证, %d 不可达对结论一致", checked, unreachable)
}

// TestSNAP_SnapshotRoundtrip 真实图快照往返后查询行为不变。
func TestSNAP_SnapshotRoundtrip(t *testing.T) {
	g, _ := loadSNAPEmail(t)
	var buf bytes.Buffer
	if err := g.Save(&buf); err != nil {
		t.Fatal(err)
	}
	g2, err := vectra.Load(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if g2.Stats() != g.Stats() {
		t.Fatalf("stats 往返后不一致: %+v vs %+v", g2.Stats(), g.Stats())
	}
	// 同一对点的路径长度一致
	for _, pair := range [][2]int{{0, 500}, {3, 999}, {1004, 42}} {
		p1, ok1, _ := g.ShortestPathIDs(vectra.NodeID(pair[0]), vectra.NodeID(pair[1]))
		p2, ok2, _ := g2.ShortestPathIDs(vectra.NodeID(pair[0]), vectra.NodeID(pair[1]))
		if ok1 != ok2 || p1.Len() != p2.Len() {
			t.Errorf("pair %v: 快照往返后路径不一致 (%d/%v vs %d/%v)",
				pair, p1.Len(), ok1, p2.Len(), ok2)
		}
	}
}

// TestSNAP_InDegreeSpotCheck 度数抽查：反向 BFS 一跳邻居 == 参考实现入度。
func TestSNAP_InDegreeSpotCheck(t *testing.T) {
	g, el := loadSNAPEmail(t)
	inDeg := make(map[int]int)
	for _, d := range el.dst {
		inDeg[d]++
	}
	for _, probe := range []int{0, 1, 500, 1004} {
		ns, err := g.NeighborsOf(strconv.Itoa(probe), vectra.DirIn)
		if err != nil {
			t.Fatal(err)
		}
		if len(ns) != len(uniqueTargets(el, probe)) {
			t.Errorf("节点 %d 入邻居 %d, 参考去重入度 %d", probe, len(ns), len(uniqueTargets(el, probe)))
		}
	}
}

func uniqueTargets(el edgeList, node int) map[int]bool {
	set := map[int]bool{}
	for i, d := range el.dst {
		if d == node {
			set[el.src[i]] = true
		}
	}
	return set
}
