package bench

import (
	"math/rand"

	"vectra"
)

// QueryParams 是预固定的查询参数集（LDBC parameter curation）：
// 起点按出度加权（模拟"网关/核心服务"被高频查询），点对只保留
// 可达对且路径长度 ≥2，避免大量无意义的 1 跳/不可达样本。
type QueryParams struct {
	Starts []string
	Pairs  [][2]string
}

// Curate 从密封图与生成产物采样 n 个起点与 n 个可达点对。确定性种子。
func Curate(g *vectra.Graph, bg *Graph, n int, seed int64) QueryParams {
	rng := rand.New(rand.NewSource(seed))
	keyOf := func(id int) string { return bg.Nodes[id].Key }

	// 出度加权起点池：每条出边给源节点一票
	pool := make([]int, 0, len(bg.Edges))
	outDeg := make([]int, len(bg.Nodes))
	for _, e := range bg.Edges {
		pool = append(pool, e.Src)
		outDeg[e.Src]++
	}
	p := QueryParams{}
	if len(pool) == 0 {
		// 无边图：均匀采样
		for i := 0; i < n && i < len(bg.Nodes); i++ {
			p.Starts = append(p.Starts, keyOf(i))
		}
		return p
	}
	seen := map[int]bool{}
	for len(p.Starts) < n && len(seen) < len(bg.Nodes) {
		id := pool[rng.Intn(len(pool))]
		if seen[id] {
			continue
		}
		seen[id] = true
		p.Starts = append(p.Starts, keyOf(id))
	}

	// 可达点对：从采样起点 BFS，取深度 ≥2 的到达节点
	for _, startKey := range p.Starts {
		node, ok := g.NodeByKey(startKey)
		if !ok {
			continue
		}
		res, err := g.BFS([]vectra.NodeID{node.ID}, vectra.WithMaxDepth(4))
		if err != nil {
			continue
		}
		var candidates []string
		for _, id := range res.Nodes() {
			if res.Depth(id) < 2 {
				continue
			}
			nd, _ := g.NodeByID(id)
			candidates = append(candidates, nd.Key)
		}
		if len(candidates) == 0 {
			continue
		}
		p.Pairs = append(p.Pairs, [2]string{startKey, candidates[rng.Intn(len(candidates))]})
	}
	return p
}
