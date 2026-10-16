package bench

import (
	"math/rand"

	"github.com/projanvil/vectra"
)

// QueryParams is the pre-curated query parameter set (LDBC parameter curation):
// start nodes are weighted by out-degree (simulating "gateway/core services"
// being queried at high frequency), node pairs keep only reachable pairs with
// path length ≥2, avoiding many meaningless 1-hop/unreachable samples.
type QueryParams struct {
	Starts []string
	Pairs  [][2]string
}

// Curate samples n start nodes and n reachable node pairs from the sealed graph
// and generation artifacts. Deterministic seed.
func Curate(g *vectra.Graph, bg *Graph, n int, seed int64) QueryParams {
	rng := rand.New(rand.NewSource(seed))
	keyOf := func(id int) string { return bg.Nodes[id].Key }

	// Out-degree weighted start-node pool: each out-edge gives the source node one vote
	pool := make([]int, 0, len(bg.Edges))
	outDeg := make([]int, len(bg.Nodes))
	for _, e := range bg.Edges {
		pool = append(pool, e.Src)
		outDeg[e.Src]++
	}
	p := QueryParams{}
	if len(pool) == 0 {
		// Edgeless graph: uniform sampling
		for i := range min(n, len(bg.Nodes)) {
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

	// Reachable node pairs: BFS from sampled start nodes, take nodes reached at depth ≥2
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
