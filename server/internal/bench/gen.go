// Package bench 提供 Vectra 压测的图生成、参数固定化、延迟统计与
// 负载执行。设计文档见 docs/benchmarks/DESIGN.md。
package bench

import (
	"fmt"
	"math/rand"
)

// Spec 描述一次生成的图：幂律度分布（BA 优先连接，新节点连老节点），
// cycleRate 比例的边反向制造环（真实依赖图存在少量循环依赖）。
type Spec struct {
	Nodes     int
	Edges     int
	Seed      int64
	CycleRate float64 // 0~1，默认 0.05
}

// Node / Edge 是生成产物（与 server.Batch 的 upsert 结构对齐）。
type Node struct {
	Key   string
	Label string
	Props map[string]any
}

type Edge struct {
	Src   int
	Dst   int
	Label string
}

// Graph 是确定性生成结果。
type Graph struct {
	Nodes []Node
	Edges []Edge
}

var edgeLabels = []string{"CALLS", "DATA", "EVENT"}

// Generate 按 BA 模型生成幂律图：节点带 tier 属性，边按序取标签。
// 新→老方向为主（时间箭头），CycleRate 比例反向造环。
func Generate(spec Spec) *Graph {
	if spec.CycleRate == 0 {
		spec.CycleRate = 0.05
	}
	rng := rand.New(rand.NewSource(spec.Seed))
	g := &Graph{Nodes: make([]Node, 0, spec.Nodes)}

	m := spec.Edges / spec.Nodes // 每新节点附带的边数（平均入度）
	if m < 1 {
		m = 1
	}
	// 重复目标表：节点每获得一个度就在表中出现一次——按度比例抽样的经典实现
	targets := make([]int, 0, spec.Edges*2)

	addNode := func() int {
		id := len(g.Nodes)
		tier := "t" + string(rune('0'+id%4))
		env := "prod"
		if id%5 == 0 {
			env = "staging"
		}
		g.Nodes = append(g.Nodes, Node{
			Key:   fmt.Sprintf("svc-%07d", id),
			Label: "Service",
			Props: map[string]any{"tier": tier, "env": env},
		})
		return id
	}

	// 种子团（BA 的初始完全图）
	const seedCluster = 4
	for i := 0; i < seedCluster && len(g.Nodes) < spec.Nodes; i++ {
		u := addNode()
		for v := 0; v < u; v++ {
			g.Edges = append(g.Edges, Edge{Src: u, Dst: v, Label: edgeLabels[len(g.Edges)%len(edgeLabels)]})
			targets = append(targets, u, v)
		}
	}

	for len(g.Nodes) < spec.Nodes && len(g.Edges) < spec.Edges {
		u := addNode()
		targets = append(targets, u)
		for i := 0; i < m && len(g.Edges) < spec.Edges; i++ {
			var v int
			if len(targets) > 0 {
				v = targets[rng.Intn(len(targets))]
			} else {
				v = rng.Intn(len(g.Nodes))
			}
			if v == u {
				continue
			}
			src, dst := u, v // 新 -> 老
			if rng.Float64() < spec.CycleRate {
				// 回边造环：两端都是"老节点获胜"，都积累选择权重
				src, dst = v, u
				targets = append(targets, src, dst)
			} else {
				// 前向边只给被选中的老节点加权（经典 BA：rich-get-richer
				// 只作用于既有节点，新节点初始权重为 1，避免分布被压平）
				targets = append(targets, dst)
			}
			g.Edges = append(g.Edges, Edge{Src: src, Dst: dst, Label: edgeLabels[len(g.Edges)%len(edgeLabels)]})
		}
	}
	// 补足边数（节点先用尽的场景）：随机节点对
	for len(g.Edges) < spec.Edges {
		u := rng.Intn(len(g.Nodes))
		v := rng.Intn(len(g.Nodes))
		if u == v {
			continue
		}
		g.Edges = append(g.Edges, Edge{Src: u, Dst: v, Label: edgeLabels[len(g.Edges)%len(edgeLabels)]})
	}
	return g
}
