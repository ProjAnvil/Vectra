package bench

import (
	"fmt"
	"testing"
)

func TestGenerateDeterministic(t *testing.T) {
	a := Generate(Spec{Nodes: 500, Edges: 5000, Seed: 7})
	b := Generate(Spec{Nodes: 500, Edges: 5000, Seed: 7})
	if len(a.Edges) != len(b.Edges) || len(a.Nodes) != len(b.Nodes) {
		t.Fatalf("规模不一致: %d/%d vs %d/%d", len(a.Nodes), len(a.Edges), len(b.Nodes), len(b.Edges))
	}
	for i := range a.Edges {
		if a.Edges[i] != b.Edges[i] {
			t.Fatalf("第 %d 条边不一致: %v vs %v", i, a.Edges[i], b.Edges[i])
		}
	}
}

func TestGenerateShape(t *testing.T) {
	g := Generate(Spec{Nodes: 2000, Edges: 20000, Seed: 42})
	if len(g.Nodes) != 2000 {
		t.Fatalf("节点数 = %d", len(g.Nodes))
	}
	if len(g.Edges) != 20000 {
		t.Fatalf("边数 = %d", len(g.Edges))
	}
	// 幂律度分布：前 1% 节点（按总度）应占 >= 15% 的边端点
	outDeg := make([]int, len(g.Nodes))
	inDeg := make([]int, len(g.Nodes))
	for _, e := range g.Edges {
		outDeg[e.Src]++
		inDeg[e.Dst]++
	}
	type kv struct{ id, deg int }
	var all []kv
	total := 0
	for i := range g.Nodes {
		d := outDeg[i] + inDeg[i]
		all = append(all, kv{i, d})
		total += d
	}
	// 简单排序取 top 1%
	for i := 1; i < len(all); i++ {
		for j := i; j > 0 && all[j].deg > all[j-1].deg; j-- {
			all[j], all[j-1] = all[j-1], all[j]
		}
	}
	top1 := len(all) / 100
	if top1 == 0 {
		top1 = 1
	}
	topSum := 0
	for _, k := range all[:top1] {
		topSum += k.deg
	}
	if float64(topSum)/float64(total) < 0.15 {
		t.Errorf("幂律性不足: top1%% 节点只占 %.1f%% 度", 100*float64(topSum)/float64(total))
	}
	// 边标签来自固定集合
	for _, e := range g.Edges {
		switch e.Label {
		case "CALLS", "DATA", "EVENT":
		default:
			t.Fatalf("非法标签 %q", e.Label)
		}
	}
	// 节点属性存在且带 env
	for i, n := range g.Nodes {
		if n.Key == "" || n.Props["tier"] == nil {
			t.Fatalf("节点 %d 属性不完整: %+v", i, n)
		}
	}
}

func TestGenerateCyclesExist(t *testing.T) {
	// cycleRate 造环：默认参数下应有 SCC>1 的环存在（幂律图 + 回边）
	g := Generate(Spec{Nodes: 3000, Edges: 30000, Seed: 11})
	back := 0 // 老节点 -> 新节点方向的边（近似回边）
	for _, e := range g.Edges {
		if e.Dst > e.Src {
			back++
		}
	}
	if back == 0 {
		t.Error("没有任何回边——cycleRate 未生效")
	}
	_ = fmt.Sprint(back)
}
