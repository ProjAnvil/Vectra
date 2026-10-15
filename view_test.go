package vectra_test

// Task 4 behavior tests: the traversal family (BFS/ShortestPath/Cypher/
// NeighborsOf) sees unmerged micro-layers immediately through the
// three-source View (spec §3.2), tombstones shadow base rows, Reachable
// falls back to node-level BFS while delta is unmerged, and the empty-layer
// fast path is bit-identical to the merged-base entry.

import (
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/projanvil/vectra"
)

// keyed renders a BFS result as depth-grouped key lists (sorted within a
// layer; terminal empty expansion layers dropped), the ID-agnostic logical
// form a merge must preserve.
func keyedLayers(t *testing.T, g *vectra.Graph, res *vectra.BFSResult) []string {
	t.Helper()
	var out []string
	for _, l := range res.Layers {
		if len(l) == 0 {
			continue
		}
		keys := make([]string, 0, len(l))
		for _, id := range l {
			n, ok := g.NodeByID(id)
			if !ok {
				t.Fatalf("BFS reached invisible node %d", id)
			}
			keys = append(keys, n.Key)
		}
		slices.Sort(keys)
		out = append(out, strings.Join(keys, ","))
	}
	return out
}

// baseGraph builds a merged base: a -CALLS-> b -CALLS-> c, a -DATA-> c.
func baseGraph(t *testing.T) *vectra.Graph {
	t.Helper()
	g := vectra.New()
	for _, k := range []string{"a", "b", "c"} {
		if _, err := g.AddNode(k, "S", nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, e := range []struct{ s, d, l string }{
		{"a", "b", "CALLS"}, {"b", "c", "CALLS"}, {"a", "c", "DATA"},
	} {
		if _, err := g.AddEdge(e.s, e.d, e.l, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	return g
}

// TestTraversalSeesLayersImmediately: post-Seal writes (micro-layers) are
// visible to BFS/ShortestPath/NeighborsOf/Cypher at once — edges crossing
// base→layer nodes and layer→base, layer-resident starts, and label filters
// spanning both label spaces (spec §3.2: traversal family = immediate).
func TestTraversalSeesLayersImmediately(t *testing.T) {
	g := baseGraph(t)
	if _, err := g.AddNode("d", "S", nil); err != nil { // layer-resident node
		t.Fatal(err)
	}
	if _, err := g.AddEdge("c", "d", "CALLS", nil); err != nil { // base -> layer node
		t.Fatal(err)
	}
	if _, err := g.AddEdge("d", "a", "DATA", nil); err != nil { // layer node -> base
		t.Fatal(err)
	}
	if g.Sealed() {
		t.Fatal("precondition: unmerged layers present")
	}

	// BFS from a base node reaches the layer-resident node d at depth 3.
	res, err := g.BFS([]vectra.NodeID{mustID(t, g, "a")})
	if err != nil {
		t.Fatalf("BFS with layers: %v", err)
	}
	want := []string{"a", "b,c", "d"}
	if got := keyedLayers(t, g, res); !slices.Equal(got, want) {
		t.Fatalf("BFS layers = %v, want %v", got, want)
	}

	// BFS from a layer-resident start (out and in).
	d := mustID(t, g, "d")
	res, err = g.BFS([]vectra.NodeID{d})
	if err != nil {
		t.Fatalf("BFS from layer-resident start: %v", err)
	}
	if got := keyedLayers(t, g, res); !slices.Equal(got, []string{"d", "a", "b,c"}) {
		t.Fatalf("BFS from d = %v", got)
	}
	res, err = g.BFS([]vectra.NodeID{d}, vectra.WithDirection(vectra.DirIn))
	if err != nil {
		t.Fatal(err)
	}
	// in(d) = c; in(c) = b (CALLS) and a (DATA, base) — both depth 2
	if got := keyedLayers(t, g, res); !slices.Equal(got, []string{"d", "c", "a,b"}) {
		t.Fatalf("BFS DirIn from d = %v", got)
	}

	// Label filter over a layer-introduced edge usage (DATA crosses d->a).
	res, err = g.BFS([]vectra.NodeID{mustID(t, g, "a")}, vectra.WithLabels("DATA"))
	if err != nil {
		t.Fatal(err)
	}
	if got := keyedLayers(t, g, res); !slices.Equal(got, []string{"a", "c"}) {
		t.Fatalf("BFS DATA-only from a = %v", got)
	}
	res, err = g.BFS([]vectra.NodeID{d}, vectra.WithLabels("DATA"))
	if err != nil {
		t.Fatal(err)
	}
	// d -DATA-> a (layer), a -DATA-> c (base)
	if got := keyedLayers(t, g, res); !slices.Equal(got, []string{"d", "a", "c"}) {
		t.Fatalf("BFS DATA-only from d = %v", got)
	}

	// ShortestPath spans base and layer edges: a -DATA-> c (base) -> d
	// (layer edge) is the 2-hop optimum; d->a is the direct layer DATA edge.
	p, found, err := g.ShortestPath("a", "d")
	if err != nil || !found {
		t.Fatalf("ShortestPath(a,d) = %+v %v %v", p, found, err)
	}
	if got := pathKeys(t, g, p); !slices.Equal(got, []string{"a", "c", "d"}) || p.Len() != 2 {
		t.Fatalf("ShortestPath(a,d) = %v", got)
	}
	p, found, err = g.ShortestPath("d", "a")
	if err != nil || !found || p.Len() != 1 {
		t.Fatalf("ShortestPath(d,a) = %+v %v %v", p, found, err)
	}
	if e, ok := g.EdgeByID(p.Edges[0]); !ok || e.Label != "DATA" {
		t.Fatalf("path edge = %+v ok=%v, want the layer DATA edge", e, ok)
	}

	// NeighborsOf sees the layer edge from both endpoints and directions.
	ns, err := g.NeighborsOf("c", vectra.DirOut)
	if err != nil || len(ns) != 1 || ns[0].Key != "d" {
		t.Fatalf("NeighborsOf(c,out) = %+v %v", ns, err)
	}
	ns, err = g.NeighborsOf("d", vectra.DirIn)
	if err != nil || len(ns) != 1 || ns[0].Key != "c" {
		t.Fatalf("NeighborsOf(d,in) = %+v %v", ns, err)
	}

	// Cypher matches across sources and anchors layer-resident nodes.
	cy, err := g.Cypher(`MATCH (x {key:'c'})-[:CALLS]->(y) RETURN y.key AS k ORDER BY k`)
	if err != nil {
		t.Fatalf("Cypher: %v", err)
	}
	if len(cy.Rows) != 1 || cy.Rows[0]["k"] != "d" {
		t.Fatalf("Cypher c-[:CALLS]->? = %+v, want layer node d", cy.Rows)
	}
	cy, err = g.Cypher(`MATCH (x {key:'d'})-[:DATA]->(y) RETURN y.key AS k`)
	if err != nil {
		t.Fatal(err)
	}
	if len(cy.Rows) != 1 || cy.Rows[0]["k"] != "a" {
		t.Fatalf("Cypher d-[:DATA]->? = %+v, want a", cy.Rows)
	}
	// variable-length across sources: c reaches a in 2 hops (c->d->a)
	cy, err = g.Cypher(`MATCH (x {key:'c'})-[*2]->(y {key:'a'}) RETURN count(*) AS n`)
	if err != nil {
		t.Fatal(err)
	}
	if cy.Rows[0]["n"] != float64(1) {
		t.Fatalf("Cypher var-length across sources = %+v", cy.Rows)
	}

	// NodeFilter reads layer node records during traversal; c (on the only
	// route to d) filtered means d is never expanded into.
	res, err = g.BFS([]vectra.NodeID{mustID(t, g, "a")},
		vectra.WithNodeFilter(func(n vectra.Node) bool { return n.Key != "c" }))
	if err != nil {
		t.Fatal(err)
	}
	if got := keyedLayers(t, g, res); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("BFS with NodeFilter = %v", got)
	}
}

func mustID(t *testing.T, g *vectra.Graph, key string) vectra.NodeID {
	t.Helper()
	n, ok := g.NodeByKey(key)
	if !ok {
		t.Fatalf("key %q not found", key)
	}
	return n.ID
}

func pathKeys(t *testing.T, g *vectra.Graph, p vectra.Path) []string {
	t.Helper()
	out := make([]string, len(p.Nodes))
	for i, id := range p.Nodes {
		n, ok := g.NodeByID(id)
		if !ok {
			t.Fatalf("path through invisible node %d", id)
		}
		out[i] = n.Key
	}
	return out
}

// TestTraversalTombstones: layer tombstones shadow base rows during
// traversal — deleted edges are not walked, fold keeps exactly one survivor,
// paths re-route or break.
func TestTraversalTombstones(t *testing.T) {
	g := vectra.New()
	for _, k := range []string{"a", "b", "c", "d"} {
		if _, err := g.AddNode(k, "S", nil); err != nil {
			t.Fatal(err)
		}
	}
	// a->b->d and a->c->d: two routes plus a parallel USES pair a->b.
	for _, e := range []struct{ s, d, l string }{
		{"a", "b", "CALLS"}, {"b", "d", "CALLS"},
		{"a", "c", "CALLS"}, {"c", "d", "CALLS"},
		{"a", "b", "USES"}, {"a", "b", "USES"},
	} {
		if _, err := g.AddEdge(e.s, e.d, e.l, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}

	// Delete the a->c base edge: BFS from a must route through b only.
	if n, err := g.DeleteEdge("a", "c", "CALLS"); err != nil || n != 1 {
		t.Fatalf("DeleteEdge = %d %v", n, err)
	}
	res, err := g.BFS([]vectra.NodeID{mustID(t, g, "a")}, vectra.WithLabels("CALLS"))
	if err != nil {
		t.Fatal(err)
	}
	if got := keyedLayers(t, g, res); !slices.Equal(got, []string{"a", "b", "d"}) {
		t.Fatalf("after delete BFS = %v (old base edge must not be walked)", got)
	}
	// shortest path re-routes through b (still 3 nodes, but via b not c).
	p, found, err := g.ShortestPath("a", "d")
	if err != nil || !found {
		t.Fatalf("ShortestPath after delete = %+v %v %v", p, found, err)
	}
	if got := pathKeys(t, g, p); !slices.Equal(got, []string{"a", "b", "d"}) {
		t.Fatalf("ShortestPath after delete = %v", got)
	}

	// Fold the USES pair: exactly one survivor, BFS/NeighborsOf unchanged
	// in shape, and the fold's tombstone kills both base parallels.
	fid, err := g.UpsertEdge("a", "b", "USES", map[string]any{"w": 9})
	if err != nil {
		t.Fatal(err)
	}
	ns, err := g.NeighborsOf("a", vectra.DirOut, "USES")
	if err != nil || len(ns) != 1 || ns[0].Key != "b" {
		t.Fatalf("NeighborsOf after fold = %+v %v", ns, err)
	}
	cy, err := g.Cypher(`MATCH (x {key:'a'})-[e:USES]->(y) RETURN count(*) AS n`)
	if err != nil {
		t.Fatal(err)
	}
	if cy.Rows[0]["n"] != float64(1) {
		t.Fatalf("folded USES pair still parallel in Cypher: %+v", cy.Rows)
	}
	if e, ok := g.EdgeByID(fid); !ok || e.Props["w"] != 9 {
		t.Fatalf("fold survivor = %+v ok=%v", e, ok)
	}

	// Break the last route: a is isolated from d under CALLS.
	if _, err := g.DeleteEdge("b", "d", "CALLS"); err != nil {
		t.Fatal(err)
	}
	if _, found, err := g.ShortestPath("a", "d"); err != nil || found {
		t.Fatalf("ShortestPath after disconnect = found=%v err=%v", found, err)
	}
	res, err = g.BFS([]vectra.NodeID{mustID(t, g, "a")}, vectra.WithLabels("CALLS"))
	if err != nil {
		t.Fatal(err)
	}
	if got := keyedLayers(t, g, res); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("BFS after disconnect = %v", got)
	}

	// Deleting a node removes it from traversal entirely (cascade).
	if _, err := g.DeleteNode("b"); err != nil {
		t.Fatal(err)
	}
	ns, err = g.NeighborsOf("a", vectra.DirOut)
	if err != nil || len(ns) != 0 {
		t.Fatalf("NeighborsOf after DeleteNode(b) = %+v %v", ns, err)
	}
	if _, err := g.BFS([]vectra.NodeID{mustID(t, g, "a")}); err != nil {
		t.Fatal(err)
	}
	res, _ = g.BFS([]vectra.NodeID{mustID(t, g, "a")})
	if got := keyedLayers(t, g, res); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("BFS after DeleteNode(b) = %v", got)
	}
}

// TestUnsealedGraphFullyTraversable: a graph that never Sealed still
// traverses everything written so far (write-then-query with no merge ever).
func TestUnsealedGraphFullyTraversable(t *testing.T) {
	g := vectra.New()
	for _, k := range []string{"a", "b", "c"} {
		if _, err := g.AddNode(k, "S", nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := g.AddEdge("a", "b", "E", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddEdge("b", "c", "E", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddEdge("c", "a", "E", nil); err != nil { // cycle across layers
		t.Fatal(err)
	}
	res, err := g.BFS([]vectra.NodeID{mustID(t, g, "b")})
	if err != nil {
		t.Fatalf("BFS on never-sealed graph: %v", err)
	}
	if got := keyedLayers(t, g, res); !slices.Equal(got, []string{"b", "c", "a"}) {
		t.Fatalf("BFS = %v", got)
	}
	if ids, err := g.Reachable("b"); err != nil || !slices.Equal(ids, []string{"a", "b", "c"}) {
		t.Fatalf("Reachable(b) = %v %v", ids, err)
	}
	if p, found, err := g.ShortestPath("b", "a"); err != nil || !found || p.Len() != 2 {
		t.Fatalf("ShortestPath(b,a) = %+v %v %v", p, found, err)
	}
	cy, err := g.Cypher(`MATCH (x)-[:E]->(y) RETURN count(*) AS n`)
	if err != nil {
		t.Fatal(err)
	}
	if cy.Rows[0]["n"] != float64(3) {
		t.Fatalf("Cypher over never-sealed graph = %+v", cy.Rows)
	}
}

// TestReachableNodeLevelFallbackMatchesMerged: while delta is unmerged,
// Reachable runs node-level BFS; after the merge it takes the supernode
// path — the two must agree on the logical answer (spec §3.2 Reachable row).
func TestReachableNodeLevelFallbackMatchesMerged(t *testing.T) {
	g := baseGraph(t)
	if _, err := g.AddNode("d", "S", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddEdge("c", "d", "CALLS", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddEdge("d", "a", "DATA", nil); err != nil {
		t.Fatal(err)
	} // a<->d cycle spans base and layer
	got, err := g.Reachable("a")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"a", "b", "c", "d"}; !slices.Equal(got, want) {
		t.Fatalf("node-level Reachable(a) = %v, want %v", got, want)
	}
	// label-filtered + direction variants (DATA edges: d->a layer, a->c base)
	got, err = g.Reachable("d", vectra.ReachWithLabels("DATA"))
	if err != nil || !slices.Equal(got, []string{"a", "c", "d"}) {
		t.Fatalf("Reachable(d, DATA) = %v %v", got, err)
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	// post-merge the supernode path answers the same
	got, err = g.Reachable("a")
	if err != nil || !slices.Equal(got, []string{"a", "b", "c", "d"}) {
		t.Fatalf("post-merge Reachable(a) = %v %v", got, err)
	}
	got, err = g.Reachable("d", vectra.ReachWithLabels("DATA"))
	if err != nil || !slices.Equal(got, []string{"a", "c", "d"}) {
		t.Fatalf("post-merge Reachable(d, DATA) = %v %v", got, err)
	}
}

// traversalFace renders the ID-agnostic traversal-family answer set: BFS
// layer groups per start, Reachable sets, NeighborsOf per key/direction,
// ShortestPath over all ordered key pairs, and a Cypher full-edge listing.
func traversalFace(t *testing.T, g *vectra.Graph, keys []string) string {
	t.Helper()
	var b strings.Builder
	for _, k := range keys {
		n, ok := g.NodeByKey(k)
		if !ok {
			fmt.Fprintf(&b, "%s DEAD\n", k)
			continue
		}
		res, err := g.BFS([]vectra.NodeID{n.ID})
		if err != nil {
			t.Fatalf("BFS(%s): %v", k, err)
		}
		fmt.Fprintf(&b, "%s bfs: %v\n", k, keyedLayers(t, g, res))
		res, err = g.BFS([]vectra.NodeID{n.ID}, vectra.WithLabels("CALLS"))
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&b, "%s bfs-calls: %v\n", k, keyedLayers(t, g, res))
		res, err = g.BFS([]vectra.NodeID{n.ID}, vectra.WithDirection(vectra.DirIn))
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&b, "%s bfs-in: %v\n", k, keyedLayers(t, g, res))
		for _, d := range []vectra.Direction{vectra.DirOut, vectra.DirIn} {
			ns, err := g.NeighborsOf(k, d)
			if err != nil {
				t.Fatalf("NeighborsOf(%s): %v", k, err)
			}
			ks := make([]string, 0, len(ns))
			for _, n := range ns {
				ks = append(ks, n.Key)
			}
			slices.Sort(ks)
			fmt.Fprintf(&b, "%s nb-%d: %v\n", k, d, ks)
		}
	}
	for _, k := range keys {
		n, ok := g.NodeByKey(k)
		if !ok {
			continue
		}
		ids, err := g.ReachableIDs(n.ID)
		if err != nil {
			t.Fatalf("ReachableIDs(%s): %v", k, err)
		}
		ks := make([]string, 0, len(ids))
		for _, id := range ids {
			kk, _ := g.NodeByID(id)
			ks = append(ks, kk.Key)
		}
		slices.Sort(ks)
		fmt.Fprintf(&b, "%s reach: %v\n", k, ks)
		ids, err = g.ReachableIDs(n.ID, vectra.ReachWithLabels("DATA"))
		if err != nil {
			t.Fatal(err)
		}
		ks = ks[:0]
		for _, id := range ids {
			kk, _ := g.NodeByID(id)
			ks = append(ks, kk.Key)
		}
		slices.Sort(ks)
		fmt.Fprintf(&b, "%s reach-data: %v\n", k, ks)
	}
	for _, s := range keys {
		for _, d := range keys {
			if s == d {
				continue
			}
			p, found, err := g.ShortestPath(s, d)
			if err != nil {
				continue // dead endpoint: both faces take the same branch
			}
			if !found {
				fmt.Fprintf(&b, "%s->%s path: NONE\n", s, d)
				continue
			}
			fmt.Fprintf(&b, "%s->%s path: %v\n", s, d, pathKeys(t, g, p))
		}
	}
	cy, err := g.Cypher(`MATCH (x)-[e]->(y) RETURN x.key AS s, y.key AS d, e.label AS l ORDER BY s, d, l`)
	if err != nil {
		t.Fatalf("Cypher: %v", err)
	}
	for _, r := range cy.Rows {
		fmt.Fprintf(&b, "edge %s-%s->%s\n", r["s"], r["l"], r["d"])
	}
	return b.String()
}

// TestTraversalFamilyViewMergesEquivalence: under random op sequences the
// traversal family's logical answer is identical before the merge (View
// path) and after it (fast path over the merged base) — and stays identical
// across repeated mid-sequence merges (spec §3.2 + R1 ID-agnosticism).
func TestTraversalFamilyViewMergesEquivalence(t *testing.T) {
	for seed := int64(1); seed <= 6; seed++ {
		g := vectra.New()
		rng := rand.New(rand.NewSource(seed))
		keys := []string{"a", "b", "c", "d"}
		labels := []string{"CALLS", "DATA"}
		live := map[string]bool{}
		for i := 0; i < 160; i++ {
			k := keys[rng.Intn(len(keys))]
			l := labels[rng.Intn(len(labels))]
			switch rng.Intn(8) {
			case 0, 1:
				_, _ = g.AddNode(k, fmt.Sprintf("L%d", rng.Intn(2)), map[string]any{"n": rng.Intn(3)})
				live[k] = true
			case 2, 3:
				s, d := keys[rng.Intn(len(keys))], keys[rng.Intn(len(keys))]
				if live[s] && live[d] {
					_, _ = g.AddEdge(s, d, l, map[string]any{"w": rng.Intn(3)})
				}
			case 4, 5:
				s, d := keys[rng.Intn(len(keys))], keys[rng.Intn(len(keys))]
				if live[s] && live[d] {
					_, _ = g.UpsertEdge(s, d, l, map[string]any{"w": rng.Intn(3)})
				}
			case 6:
				s, d := keys[rng.Intn(len(keys))], keys[rng.Intn(len(keys))]
				_, _ = g.DeleteEdge(s, d, l)
			case 7:
				_, _ = g.DeleteNode(k)
				delete(live, k)
			}
			if i%53 == 52 { // mid-sequence merge: View face vs fast-path face
				before := traversalFace(t, g, keys)
				if err := g.Seal(); err != nil {
					t.Fatal(err)
				}
				after := traversalFace(t, g, keys)
				if before != after {
					t.Fatalf("seed %d op %d: traversal face diverged across merge\nbefore:\n%s\nafter:\n%s", seed, i, before, after)
				}
			}
		}
		before := traversalFace(t, g, keys)
		if err := g.Seal(); err != nil {
			t.Fatal(err)
		}
		if after := traversalFace(t, g, keys); before != after {
			t.Fatalf("seed %d: final merge diverged\nbefore:\n%s\nafter:\n%s", seed, before, after)
		}
	}
}

// TestTraversalConcurrentWithWriter: -race guard — the traversal family
// reads the published generation while the writer keeps publishing layers.
func TestTraversalConcurrentWithWriter(t *testing.T) {
	g := vectra.New()
	for i := 0; i < 30; i++ {
		_, _ = g.AddNode(fmt.Sprintf("n%d", i), "S", nil)
	}
	for i := 0; i < 29; i++ {
		_, _ = g.AddEdge(fmt.Sprintf("n%d", i), fmt.Sprintf("n%d", i+1), "CALLS", nil)
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				if n, ok := g.NodeByKey(fmt.Sprintf("n%d", i%30)); ok {
					if _, err := g.BFS([]vectra.NodeID{n.ID}, vectra.WithMaxDepth(3)); err != nil && !errors.Is(err, vectra.ErrNodeNotFound) {
						t.Errorf("BFS: %v", err)
						return
					}
					_, _ = g.ReachableIDs(n.ID)
					_, _, _ = g.ShortestPathIDs(n.ID, vectra.NodeID(0))
				}
				_, _ = g.NeighborsOf("n0", vectra.DirOut)
				if i%8 == 0 {
					_, _ = g.Cypher(`MATCH (x)-[:CALLS]->(y) RETURN count(*) AS n`)
				}
			}
		}(r)
	}
	for i := 0; i < 300; i++ {
		k := fmt.Sprintf("w%d", i)
		_, _ = g.AddNode(k, "S", nil)
		if i > 0 {
			// the DATA chain w0->…->w299 stays intact; NOISE edges absorb
			// the fold/delete churn so the final reachability is exact
			_, _ = g.AddEdge(fmt.Sprintf("w%d", i-1), k, "DATA", nil)
			_, _ = g.AddEdge(k, "n0", "NOISE", nil)
			if i%5 == 0 {
				_ = g.Batch(func(g *vectra.Graph) {
					_, _ = g.UpsertEdge(k, "n0", "NOISE", map[string]any{"w": i})
					_, _ = g.AddEdge("n0", k, "CALLS", nil)
				})
			}
			if i%7 == 0 {
				_, _ = g.DeleteEdge(k, "n0", "NOISE")
			}
		}
	}
	close(stop)
	wg.Wait()
	// the writer's layers are all visible to traversal afterwards
	res, err := g.BFS([]vectra.NodeID{mustID(t, g, "w299")}, vectra.WithDirection(vectra.DirIn), vectra.WithLabels("DATA"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Count() != 300 {
		t.Fatalf("chain reach = %d, want 300", res.Count())
	}
}

// useFace is the USES-triple face of the fold-transfer probe below: the
// surviving parallel count from Cypher and the winner's props (by ID before
// the merge, by triple scan after — Seal may renumber, R1).
func useFace(t *testing.T, g *vectra.Graph, winner vectra.EdgeID, byID bool) (count float64, propsOK bool) {
	t.Helper()
	cy, err := g.Cypher(`MATCH (x {key:'a'})-[e:USES]->(y) RETURN count(*) AS n`)
	if err != nil {
		t.Fatalf("Cypher: %v", err)
	}
	if byID {
		e, ok := g.EdgeByID(winner)
		return cy.Rows[0]["n"].(float64), ok && e.Props["w"] == 7
	}
	es, err := g.EdgesOf("a", vectra.DirOut, "USES")
	if err != nil {
		t.Fatal(err)
	}
	if len(es) != 1 {
		return cy.Rows[0]["n"].(float64), false
	}
	return cy.Rows[0]["n"].(float64), es[0].Props["w"] == 7
}

// TestLayerFoldTransferInView: a fold whose winner lives in an OLDER unmerged
// layer transfers that layer record's EdgeID onto the fold record, so for a
// moment two layer sources carry the SAME EdgeID (the older one is logically
// shadowed, but EdgeID-keyed kills cannot distinguish them — View exempts the
// transferred ID and consumers dedup identical bindings). Every consumer must
// still see exactly one logical edge with the winner's props, before and
// after the merge (post-merge IDs may be renumbered — R1 — so the face is
// ID-agnostic there).
func TestLayerFoldTransferInView(t *testing.T) {
	g := baseGraph(t) // a -CALLS-> b -CALLS-> c, a -DATA-> c, merged
	// two parallel USES edges across two separate micro-layers (multigraph)
	e1, err := g.AddEdge("a", "b", "USES", nil)
	if err != nil {
		t.Fatal(err)
	}
	e2, err := g.AddEdge("a", "b", "USES", nil)
	if err != nil {
		t.Fatal(err)
	}
	cy, err := g.Cypher(`MATCH (x {key:'a'})-[e:USES]->(y) RETURN count(*) AS n`)
	if err != nil || cy.Rows[0]["n"] != float64(2) {
		t.Fatalf("parallel layer edges: rows=%v err=%v, want 2", cy.Rows, err)
	}
	// fold in a third layer: the winner (most recent, e2) keeps its ID and
	// its props are replaced wholesale
	fid, err := g.UpsertEdge("a", "b", "USES", map[string]any{"w": 7})
	if err != nil || fid != e2 {
		t.Fatalf("fold winner = %v %v, want %v", fid, err, e2)
	}
	count, propsOK := useFace(t, g, e2, true)
	if count != 1 || !propsOK {
		t.Fatalf("post-fold view face: count=%v props=%v, want 1/true", count, propsOK)
	}
	if _, ok := g.EdgeByID(e1); ok {
		t.Fatal("folded-away parallel e1 still resolves")
	}
	ns, err := g.NeighborsOf("a", vectra.DirOut, "USES")
	if err != nil || len(ns) != 1 || ns[0].Key != "b" {
		t.Fatalf("NeighborsOf after fold = %+v %v", ns, err)
	}
	// the merge must land on the same logical face (IDs may be renumbered)
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	if count, propsOK := useFace(t, g, e2, false); count != 1 || !propsOK {
		t.Fatalf("post-merge face: count=%v props=%v, want 1/true", count, propsOK)
	}
	cy, err = g.Cypher(`MATCH (x {key:'a'})-[e]->(y) RETURN count(*) AS n`)
	if err != nil || cy.Rows[0]["n"] != float64(3) {
		t.Fatalf("a out-edges after merge = %v %v, want 3 (CALLS, DATA, USES)", cy.Rows, err)
	}
}
