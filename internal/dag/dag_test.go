package dag_test

import (
	"testing"

	"vectra/internal/dag"
)

func graph(n uint32, edges [][2]uint32) func(uint32) []uint32 {
	adj := make([][]uint32, n)
	for _, e := range edges {
		adj[e[0]] = append(adj[e[0]], e[1])
	}
	return func(u uint32) []uint32 { return adj[u] }
}

func normalizeSCC(sccOf []uint32) map[uint32][]uint32 {
	m := map[uint32][]uint32{}
	for v, s := range sccOf {
		m[s] = append(m[s], uint32(v))
	}
	return m
}

func membersEqual(got []uint32, want ...uint32) bool {
	if len(got) != len(want) {
		return false
	}
	seen := map[uint32]bool{}
	for _, v := range got {
		seen[v] = true
	}
	for _, w := range want {
		if !seen[w] {
			return false
		}
	}
	return true
}

func TestTarjanTwoNodeCycle(t *testing.T) {
	adj := graph(4, [][2]uint32{{0, 1}, {1, 0}, {1, 2}, {2, 3}})
	sccOf, count := dag.Tarjan(4, adj)
	if count != 3 {
		t.Fatalf("count = %d, want 3 (SCC{0,1}, {2}, {3})", count)
	}
	m := normalizeSCC(sccOf)
	found := false
	for _, mem := range m {
		if membersEqual(mem, 0, 1) {
			found = true
		}
	}
	if !found {
		t.Errorf("no SCC with members {0,1}: %v", m)
	}
}

func TestTarjanDAGAllSingletons(t *testing.T) {
	adj := graph(3, [][2]uint32{{0, 1}, {1, 2}, {0, 2}})
	sccOf, count := dag.Tarjan(3, adj)
	if count != 3 {
		t.Fatalf("count = %d, want 3", count)
	}
	for v, s := range sccOf {
		if s >= count {
			t.Errorf("sccOf[%d]=%d out of range", v, s)
		}
	}
}

func TestTarjanSelfLoopIsSingleton(t *testing.T) {
	// 自环不构成多于 1 个成员的 SCC；Vectra 在上层单独标记自环为环。
	adj := graph(1, [][2]uint32{{0, 0}})
	sccOf, count := dag.Tarjan(1, adj)
	if count != 1 || sccOf[0] != 0 {
		t.Fatalf("count=%d sccOf=%v, want 1 [0]", count, sccOf)
	}
}

func TestTarjanNestedAndDisjoint(t *testing.T) {
	// 两个独立环 {0,1,2} 和 {3,4}，5->0 指入第一个环
	adj := graph(6, [][2]uint32{
		{0, 1}, {1, 2}, {2, 0},
		{3, 4}, {4, 3},
		{5, 0},
	})
	sccOf, count := dag.Tarjan(6, adj)
	if count != 3 {
		t.Fatalf("count = %d, want 3 ({0,1,2}, {3,4}, {5})", count)
	}
	m := normalizeSCC(sccOf)
	if !membersOf(m, 0, 1, 2) || !membersOf(m, 3, 4) || !membersOf(m, 5) {
		t.Errorf("SCC partition wrong: %v", m)
	}
}

func membersOf(m map[uint32][]uint32, want ...uint32) bool {
	for _, mem := range m {
		if membersEqual(mem, want...) {
			return true
		}
	}
	return false
}

func TestCondenseDedupAndLabel(t *testing.T) {
	// 0<->1 环, 1->2, 0->2, 2->3。凝聚后跨 SCC 边按 (s,d,label) 去重。
	adj := graph(4, [][2]uint32{{0, 1}, {1, 0}, {1, 2}, {0, 2}, {2, 3}})
	// 边序: e0:0->1(L0) e1:1->0(L0) e2:1->2(L1) e3:0->2(L2) e4:2->3(L3)
	lab := func(u uint32) []uint32 {
		switch u {
		case 0:
			return []uint32{0, 2} // e0, e3
		case 1:
			return []uint32{0, 1} // e1, e2
		case 2:
			return []uint32{3} // e4
		}
		return nil
	}
	sccOf, count := dag.Tarjan(4, adj)
	if count != 3 {
		t.Fatalf("count = %d, want 3", count)
	}
	cond := dag.Condense(4, sccOf, count, adj, lab)
	// scc: A={0,1} B={2} C={3}; 跨 SCC 边: e2:A->B(L1), e3:A->B(L2), e4:B->C(L3)
	total := 0
	for s := uint32(0); s < count; s++ {
		tg, lb := cond.Row(s)
		for i := range tg {
			total++
			t.Logf("cond edge %d -> %d label %d", s, tg[i], lb[i])
		}
	}
	if total != 3 {
		t.Errorf("condensed edges = %d, want 3 (三元组去重后)", total)
	}
}

func TestLevelsLongestPath(t *testing.T) {
	// DAG: 0->1, 0->2, 1->3, 2->3, 3->4; 每点单 SCC
	adj := graph(5, [][2]uint32{{0, 1}, {0, 2}, {1, 3}, {2, 3}, {3, 4}})
	sccOf, count := dag.Tarjan(5, adj)
	cond := dag.Condense(5, sccOf, count, adj, nil)
	lv := dag.Levels(cond)
	for _, tc := range []struct{ node, want uint32 }{{0, 0}, {1, 1}, {2, 1}, {3, 2}, {4, 3}} {
		if got := lv[sccOf[tc.node]]; got != tc.want {
			t.Errorf("level[%d] = %d, want %d", tc.node, got, tc.want)
		}
	}
	// 所有凝聚边严格升层
	for s := uint32(0); s < count; s++ {
		tg, _ := cond.Row(s)
		for _, d := range tg {
			if lv[d] <= lv[s] {
				t.Errorf("edge %d->%d: level %d -> %d not increasing", s, d, lv[s], lv[d])
			}
		}
	}
}
