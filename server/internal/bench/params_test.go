package bench

import (
	"testing"

	"vectra"
)

func buildSealed(t *testing.T, spec Spec) (*vectra.Graph, *Graph) {
	t.Helper()
	bg := Generate(spec)
	g := vectra.New()
	for i, n := range bg.Nodes {
		if _, err := g.AddNode(n.Key, n.Label, n.Props); err != nil {
			t.Fatal(err)
		}
		_ = i
	}
	for _, e := range bg.Edges {
		sk := bg.Nodes[e.Src].Key
		dk := bg.Nodes[e.Dst].Key
		if _, err := g.AddEdge(sk, dk, e.Label, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	return g, bg
}

func TestCurateDeterministic(t *testing.T) {
	g, bg := buildSealed(t, Spec{Nodes: 800, Edges: 8000, Seed: 3})
	a := Curate(g, bg, 32, 99)
	b := Curate(g, bg, 32, 99)
	if len(a.Starts) != 32 || len(a.Pairs) != 32 {
		t.Fatalf("采样数: %d/%d", len(a.Starts), len(a.Pairs))
	}
	for i := range a.Starts {
		if a.Starts[i] != b.Starts[i] {
			t.Fatalf("起点[%d] 不确定", i)
		}
	}
	for i := range a.Pairs {
		if a.Pairs[i] != b.Pairs[i] {
			t.Fatalf("点对[%d] 不确定", i)
		}
	}
}

func TestCurateStartsDegreeWeighted(t *testing.T) {
	g, bg := buildSealed(t, Spec{Nodes: 2000, Edges: 20000, Seed: 5})
	p := Curate(g, bg, 200, 7)
	outDeg := make(map[string]int)
	for _, e := range bg.Edges {
		outDeg[bg.Nodes[e.Src].Key]++
	}
	// 高扇出节点（top 1% 出度）应显著过采样
	hits := map[string]bool{}
	for _, s := range p.Starts {
		hits[s] = true
	}
	top := 0
	for _, d := range outDeg {
		if d >= 20 { // 该规格下的高扇出阈值
			top++
		}
	}
	if top == 0 {
		t.Fatal("没有高扇出节点")
	}
	// 至少命中一个高扇出节点（度加权下概率极高）
	hitHigh := false
	for s := range hits {
		if outDeg[s] >= 20 {
			hitHigh = true
		}
	}
	if !hitHigh {
		t.Error("度加权采样未命中任何高扇出节点")
	}
}

func TestCuratePairsReachable(t *testing.T) {
	g, bg := buildSealed(t, Spec{Nodes: 1500, Edges: 15000, Seed: 8})
	p := Curate(g, bg, 64, 13)
	for _, pair := range p.Pairs {
		if pair[0] == pair[1] {
			t.Fatalf("自环点对: %v", pair)
		}
		path, found, err := g.ShortestPath(pair[0], pair[1])
		if err != nil {
			// 起点或终点不存在
			t.Fatalf("点对引用不存在的节点: %v %v", pair, err)
		}
		if !found {
			t.Fatalf("点对不可达（curation 失职）: %v", pair)
		}
		if path.Len() < 2 {
			// 允许少量 1 跳，但多数应 ≥2
			t.Logf("短路径点对: %v hops=%d", pair, path.Len())
		}
	}
}
