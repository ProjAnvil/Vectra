package vectra_test

import (
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/projanvil/vectra"
)

// buildDAG builds the fixed test graph (acyclic):
// gateway -CALLS-> auth -QUERY-> db; gateway -CALLS-> ledger -QUERY-> db; db -EVENT-> kafka
func buildDAG(t *testing.T) *vectra.Graph {
	t.Helper()
	g := vectra.New()
	for _, n := range []string{"gateway", "auth", "ledger", "db", "kafka"} {
		if _, err := g.AddNode(n, "Svc", nil); err != nil {
			t.Fatal(err)
		}
	}
	edges := []struct{ s, d, l string }{
		{"gateway", "auth", "CALLS"}, {"gateway", "ledger", "CALLS"},
		{"auth", "db", "QUERY"}, {"ledger", "db", "QUERY"},
		{"db", "kafka", "EVENT"},
	}
	for _, e := range edges {
		if _, err := g.AddEdge(e.s, e.d, e.l, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	return g
}

func ids(t *testing.T, g *vectra.Graph, keys ...string) []vectra.NodeID {
	t.Helper()
	out := make([]vectra.NodeID, 0, len(keys))
	for _, k := range keys {
		n, ok := g.NodeByKey(k)
		if !ok {
			t.Fatalf("node %s missing", k)
		}
		out = append(out, n.ID)
	}
	return out
}

func keysOf(t *testing.T, g *vectra.Graph, ids []vectra.NodeID) []string {
	t.Helper()
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		n, _ := g.NodeByID(id)
		out = append(out, n.Key)
	}
	return out
}

// randGraph generates a fixed-seed random graph (with cycles).
func randGraph(t *testing.T, nodes, edges int) *vectra.Graph {
	t.Helper()
	rng := rand.New(rand.NewSource(42))
	g := vectra.New()
	for i := 0; i < nodes; i++ {
		if _, err := g.AddNode(fmt.Sprintf("n%05d", i), "Svc", nil); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < edges; i++ {
		s, d := rng.Intn(nodes), rng.Intn(nodes)
		l := "CALLS"
		if i%3 == 0 {
			l = "DATA"
		}
		if _, err := g.AddEdge(fmt.Sprintf("n%05d", s), fmt.Sprintf("n%05d", d), l, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	return g
}

func TestBFSDepthAndNodes(t *testing.T) {
	g := buildDAG(t)
	gw := ids(t, g, "gateway")[0]
	res, err := g.BFS([]vectra.NodeID{gw})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]uint32{"gateway": 0, "auth": 1, "ledger": 1, "db": 2, "kafka": 3}
	if res.Count() != len(want) {
		t.Fatalf("Count = %d, want %d entries", res.Count(), len(want))
	}
	for key, d := range want {
		id := ids(t, g, key)[0]
		if res.Depth(id) != d {
			t.Errorf("depth[%s] = %d, want %d", key, res.Depth(id), d)
		}
	}
	if res.Count() != 5 {
		t.Errorf("Count = %d, want 5", res.Count())
	}
}

func TestBFSMaxDepth(t *testing.T) {
	g := buildDAG(t)
	gw := ids(t, g, "gateway")[0]
	res, err := g.BFS([]vectra.NodeID{gw}, vectra.WithMaxDepth(2))
	if err != nil {
		t.Fatal(err)
	}
	if res.Count() != 4 { // gateway, auth, ledger, db
		t.Errorf("Count = %d, want 4", res.Count())
	}
	if res.Reached(ids(t, g, "kafka")[0]) {
		t.Error("kafka reached with maxDepth=2")
	}
}

func TestBFSLabelFilter(t *testing.T) {
	g := buildDAG(t)
	gw := ids(t, g, "gateway")[0]
	res, err := g.BFS([]vectra.NodeID{gw}, vectra.WithLabels("QUERY"))
	if err != nil {
		t.Fatal(err)
	}
	// from gateway there are no QUERY edges: gateway's out-edges are all CALLS, nothing reachable
	if res.Count() != 1 {
		t.Errorf("Count = %d, want 1 (start node only)", res.Count())
	}
	// from auth: auth->db(QUERY) reachable; db->kafka is EVENT, filtered out
	auth := ids(t, g, "auth")[0]
	res, _ = g.BFS([]vectra.NodeID{auth}, vectra.WithLabels("QUERY"))
	if res.Count() != 2 {
		t.Errorf("Count = %d, want 2 (auth, db)", res.Count())
	}
}

func TestBFSLabelFilterUnknownLabel(t *testing.T) {
	g := buildDAG(t)
	gw := ids(t, g, "gateway")[0]
	res, err := g.BFS([]vectra.NodeID{gw}, vectra.WithLabels("NOT_A_LABEL"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Count() != 1 {
		t.Errorf("Count = %d, want 1 (unknown label filters everything)", res.Count())
	}
}

func TestBFSMultiSource(t *testing.T) {
	g := buildDAG(t)
	starts := ids(t, g, "auth", "ledger")
	res, err := g.BFS(starts)
	if err != nil {
		t.Fatal(err)
	}
	if res.Depth(ids(t, g, "auth")[0]) != 0 || res.Depth(ids(t, g, "ledger")[0]) != 0 {
		t.Error("start nodes should have depth 0")
	}
	if res.Depth(ids(t, g, "db")[0]) != 1 {
		t.Errorf("db depth = %d, want 1", res.Depth(ids(t, g, "db")[0]))
	}
	if res.Count() != 4 {
		t.Errorf("Count = %d, want 4", res.Count())
	}
}

func TestBFSMultiSourceDedup(t *testing.T) {
	g := buildDAG(t)
	// a duplicated start node passed twice counts once
	res, err := g.BFS(ids(t, g, "gateway", "gateway"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Count() != 5 {
		t.Errorf("Count = %d, want 5", res.Count())
	}
}

func TestBFSDirectionIn(t *testing.T) {
	g := buildDAG(t)
	db := ids(t, g, "db")[0]
	res, err := g.BFS([]vectra.NodeID{db}, vectra.WithDirection(vectra.DirIn))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]uint32{"db": 0, "auth": 1, "ledger": 1, "gateway": 2}
	if res.Count() != len(want) {
		t.Fatalf("Count = %d, want %d", res.Count(), len(want))
	}
	for k, d := range want {
		if res.Depth(ids(t, g, k)[0]) != d {
			t.Errorf("depth[%s] = %d, want %d", k, res.Depth(ids(t, g, k)[0]), d)
		}
	}
}

func TestBFSHandlesCycles(t *testing.T) {
	g := vectra.New()
	for _, k := range []string{"a", "b", "c", "d"} {
		if _, err := g.AddNode(k, "Svc", nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, e := range []struct{ s, d string }{{"a", "b"}, {"b", "c"}, {"c", "a"}, {"c", "d"}} {
		if _, err := g.AddEdge(e.s, e.d, "CALLS", nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	a := ids(t, g, "a")[0]
	res, err := g.BFS([]vectra.NodeID{a})
	if err != nil {
		t.Fatal(err)
	}
	if res.Count() != 4 {
		t.Errorf("Count = %d, want 4 (cycle must terminate)", res.Count())
	}
	if res.Depth(ids(t, g, "a")[0]) != 0 || res.Depth(ids(t, g, "b")[0]) != 1 {
		t.Error("depths wrong for cycle graph")
	}
}

// TestBFSUnsealedSeesLayerStart: the sealed gate is dissolved and the
// traversal family is three-source (Task 4) — BFS anchors on a
// layer-resident start in a graph that never merged anything.
func TestBFSUnsealedSeesLayerStart(t *testing.T) {
	g := vectra.New()
	if _, err := g.AddNode("a", "S", nil); err != nil {
		t.Fatal(err)
	}
	res, err := g.BFS([]vectra.NodeID{0})
	if err != nil {
		t.Fatalf("BFS on layer-resident start: %v", err)
	}
	if res.Count() != 1 {
		t.Errorf("Count = %d, want 1", res.Count())
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	if _, err := g.BFS([]vectra.NodeID{0}); err != nil {
		t.Errorf("after Seal: %v", err)
	}
}

func TestBFSUnknownStartRejected(t *testing.T) {
	g := buildDAG(t)
	if _, err := g.BFS([]vectra.NodeID{99}); err == nil {
		t.Error("expected error for out-of-range start node")
	}
}

func TestBFSEmptyStarts(t *testing.T) {
	g := buildDAG(t)
	res, err := g.BFS(nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Count() != 0 {
		t.Errorf("Count = %d, want 0", res.Count())
	}
}

// buildCyclic is reused by later Reachable/ShortestPath/Cycles tests:
// cycle a<->b, b->c, c->d, d->b (b,c,d form a cycle), e -DATA-> a
func buildCyclic(t *testing.T) *vectra.Graph {
	t.Helper()
	g := vectra.New()
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		if _, err := g.AddNode(k, "Svc", nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, e := range []struct{ s, d, l string }{
		{"a", "b", "CALLS"}, {"b", "a", "CALLS"},
		{"b", "c", "CALLS"}, {"c", "d", "CALLS"}, {"d", "b", "CALLS"},
		{"e", "a", "DATA"},
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

func TestParallelBFSEquivalentToSerial(t *testing.T) {
	g := randGraph(t, 20000, 200000)
	serial, err := g.BFS([]vectra.NodeID{0}, vectra.WithParallelThreshold(1<<30)) // always serial
	if err != nil {
		t.Fatal(err)
	}
	parallel, err := g.BFS([]vectra.NodeID{0}, vectra.WithParallelThreshold(0)) // always parallel
	if err != nil {
		t.Fatal(err)
	}
	if serial.Count() != parallel.Count() {
		t.Fatalf("count serial=%d parallel=%d", serial.Count(), parallel.Count())
	}
	if serial.Count() != parallel.Count() {
		t.Fatalf("count serial=%d parallel=%d", serial.Count(), parallel.Count())
	}
	for _, id := range serial.Nodes() {
		if serial.Depth(id) != parallel.Depth(id) {
			t.Fatalf("node %d depth serial=%d parallel=%d", id, serial.Depth(id), parallel.Depth(id))
		}
	}
}

func TestParallelBFSRespectsMaxDepthAndLabels(t *testing.T) {
	g := randGraph(t, 20000, 200000)
	par, err := g.BFS([]vectra.NodeID{7}, vectra.WithParallelThreshold(0), vectra.WithMaxDepth(4), vectra.WithLabels("CALLS"))
	if err != nil {
		t.Fatal(err)
	}
	ser, err := g.BFS([]vectra.NodeID{7}, vectra.WithParallelThreshold(1<<30), vectra.WithMaxDepth(4), vectra.WithLabels("CALLS"))
	if err != nil {
		t.Fatal(err)
	}
	if par.Count() != ser.Count() {
		t.Errorf("parallel=%d serial=%d", par.Count(), ser.Count())
	}
	for _, id := range ser.Nodes() {
		if !par.Reached(id) {
			t.Errorf("node %d missing in parallel result", id)
		}
	}
}

func TestParallelBFSDirectionInEquivalence(t *testing.T) {
	g := randGraph(t, 20000, 200000)
	par, err := g.BFS([]vectra.NodeID{123}, vectra.WithParallelThreshold(0), vectra.WithDirection(vectra.DirIn))
	if err != nil {
		t.Fatal(err)
	}
	ser, err := g.BFS([]vectra.NodeID{123}, vectra.WithParallelThreshold(1<<30), vectra.WithDirection(vectra.DirIn))
	if err != nil {
		t.Fatal(err)
	}
	if par.Count() != ser.Count() {
		t.Errorf("parallel=%d serial=%d", par.Count(), ser.Count())
	}
	for _, id := range ser.Nodes() {
		if ser.Depth(id) != par.Depth(id) {
			t.Fatalf("node %d depth diverged", id)
		}
	}
}

func TestReachableThroughCycles(t *testing.T) {
	g := buildCyclic(t)
	res, err := g.ReachableIDs(ids(t, g, "a")[0])
	if err != nil {
		t.Fatal(err)
	}
	got := keysOf(t, g, res)
	want := []string{"a", "b", "c", "d"}
	if !slices.Equal(got, want) {
		t.Errorf("Reachable(a) = %v, want %v (sorted, excludes e)", got, want)
	}
}

func TestReachableLabels(t *testing.T) {
	g := buildCyclic(t)
	// e's out-edge is DATA; walking CALLS only leaves just the start itself
	res, err := g.ReachableIDs(ids(t, g, "e")[0], vectra.ReachWithLabels("CALLS"))
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOf(t, g, res); !slices.Equal(got, []string{"e"}) {
		t.Errorf("Reachable(e, CALLS) = %v, want [e]", got)
	}
	// e -DATA-> a, then enters the CALLS domain {a,b,c,d}
	res, err = g.ReachableIDs(ids(t, g, "e")[0], vectra.ReachWithLabels("DATA"))
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOf(t, g, res); !slices.Equal(got, []string{"a", "e"}) {
		t.Errorf("Reachable(e, DATA) = %v, want [a e] (e reaches a via DATA; a has no DATA out-edge)", got)
	}
}

func TestReachableDirectionIn(t *testing.T) {
	g := buildCyclic(t)
	res, err := g.ReachableIDs(ids(t, g, "c")[0], vectra.ReachWithDirection(vectra.DirIn))
	if err != nil {
		t.Fatal(err)
	}
	got := keysOf(t, g, res)
	if !slices.Equal(got, []string{"a", "b", "c", "d", "e"}) {
		t.Errorf("Reachable(c, In) = %v, want [a b c d e]", got)
	}
}

func TestReachableMatchesBFSOnRandomGraph(t *testing.T) {
	g := randGraph(t, 20000, 200000)
	for _, start := range []vectra.NodeID{0, 1, 42, 19999} {
		bfs, err := g.BFS([]vectra.NodeID{start}, vectra.WithParallelThreshold(1<<30))
		if err != nil {
			t.Fatal(err)
		}
		reach, err := g.ReachableIDs(start)
		if err != nil {
			t.Fatal(err)
		}
		if bfs.Count() != len(reach) {
			t.Fatalf("start %d: BFS=%d Reachable=%d", start, bfs.Count(), len(reach))
		}
		bm := map[vectra.NodeID]bool{}
		for _, n := range bfs.Nodes() {
			bm[n] = true
		}
		for _, n := range reach {
			if !bm[n] {
				t.Fatalf("start %d: node %d in Reachable but not BFS", start, n)
			}
		}
	}
}

func TestReachableByStringKey(t *testing.T) {
	g := buildCyclic(t)
	res, err := g.Reachable("a")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(res, []string{"a", "b", "c", "d"}) {
		t.Errorf("Reachable(a) = %v", res)
	}
	if _, err := g.Reachable("nope"); !errors.Is(err, vectra.ErrNodeNotFound) {
		t.Errorf("err = %v, want ErrNodeNotFound", err)
	}
}

func TestReachableLabelFilteredMatchesBFS(t *testing.T) {
	g := randGraph(t, 20000, 200000)
	for _, start := range []vectra.NodeID{0, 5, 777} {
		bfs, err := g.BFS([]vectra.NodeID{start}, vectra.WithLabels("CALLS"))
		if err != nil {
			t.Fatal(err)
		}
		reach, err := g.ReachableIDs(start, vectra.ReachWithLabels("CALLS"))
		if err != nil {
			t.Fatal(err)
		}
		if bfs.Count() != len(reach) {
			t.Fatalf("start %d: BFS=%d Reachable=%d", start, bfs.Count(), len(reach))
		}
		bm := map[vectra.NodeID]bool{}
		for _, n := range bfs.Nodes() {
			bm[n] = true
		}
		for _, n := range reach {
			if !bm[n] {
				t.Fatalf("start %d: node %d in Reachable but not label-BFS", start, n)
			}
		}
	}
}

func TestShortestPathBasic(t *testing.T) {
	g := buildDAG(t)
	p, found, err := g.ShortestPath("gateway", "kafka")
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	wantNodes := []string{"gateway", "auth", "db", "kafka"}
	if p.Len() != 3 {
		t.Errorf("Len = %d, want 3", p.Len())
	}
	got := keysOf(t, g, p.Nodes)
	if !slices.Equal(got, wantNodes) {
		t.Errorf("path = %v, want %v", got, wantNodes)
	}
	wantLabels := []string{"CALLS", "QUERY", "EVENT"}
	for i, e := range p.Edges {
		edge, _ := g.EdgeByID(e)
		if edge.Label != wantLabels[i] {
			t.Errorf("edge[%d] label = %s, want %s", i, edge.Label, wantLabels[i])
		}
	}
}

func TestShortestPathSameNode(t *testing.T) {
	g := buildDAG(t)
	p, found, err := g.ShortestPath("db", "db")
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if len(p.Nodes) != 1 || p.Len() != 0 {
		t.Errorf("self path = %v, want single node", p.Nodes)
	}
}

func TestShortestPathNotFound(t *testing.T) {
	g := buildDAG(t)
	_, found, err := g.ShortestPath("kafka", "gateway")
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Error("kafka->gateway should not be reachable (DAG)")
	}
}

func TestShortestPathThroughCycle(t *testing.T) {
	g := buildCyclic(t)
	// c's component is {a,b,c,d}; c->d->b: shortest c-to-b is 2 hops
	p, found, err := g.ShortestPath("c", "b")
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if p.Len() != 2 {
		t.Errorf("Len = %d, want 2", p.Len())
	}
	got := keysOf(t, g, p.Nodes)
	if !slices.Equal(got, []string{"c", "d", "b"}) {
		t.Errorf("path = %v, want [c d b]", got)
	}
}

func TestShortestPathUnknownKeys(t *testing.T) {
	g := buildDAG(t)
	if _, _, err := g.ShortestPath("nope", "db"); !errors.Is(err, vectra.ErrNodeNotFound) {
		t.Errorf("err = %v, want ErrNodeNotFound", err)
	}
	if _, _, err := g.ShortestPath("db", "nope"); !errors.Is(err, vectra.ErrNodeNotFound) {
		t.Errorf("err = %v, want ErrNodeNotFound", err)
	}
}

// TestShortestPathBidirectionalMatchesBFS: property test — 200 random
// node pairs; bidirectional path length must equal unidirectional BFS
// distance, with a contiguous path of real edges.
func TestShortestPathBidirectionalMatchesBFS(t *testing.T) {
	g := randGraph(t, 5000, 50000)
	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 200; i++ {
		from, to := vectra.NodeID(rng.Intn(5000)), vectra.NodeID(rng.Intn(5000))
		p, found, err := g.ShortestPathIDs(from, to)
		if err != nil {
			t.Fatal(err)
		}
		bfs, err := g.BFS([]vectra.NodeID{from})
		if err != nil {
			t.Fatal(err)
		}
		if !bfs.Reached(to) {
			if found {
				t.Fatalf("pair %d->%d: bidir found but BFS did not reach", from, to)
			}
			continue
		}
		d := bfs.Depth(to)
		if !found {
			t.Fatalf("pair %d->%d: BFS depth=%d but bidir not found", from, to, d)
		}
		if p.Len() != int(d) {
			t.Fatalf("pair %d->%d: bidir len=%d BFS depth=%d", from, to, p.Len(), d)
		}
		if p.Nodes[0] != from || p.Nodes[len(p.Nodes)-1] != to {
			t.Fatalf("path endpoints wrong: %v", p.Nodes)
		}
		for j, e := range p.Edges {
			edge, ok := g.EdgeByID(e)
			if !ok || edge.Src != p.Nodes[j] || edge.Dst != p.Nodes[j+1] {
				t.Fatalf("edge %d mismatch on path %v", j, p.Nodes)
			}
		}
	}
}

func TestCyclesFindsSCCAndSelfLoop(t *testing.T) {
	g := vectra.New()
	for _, k := range []string{"a", "b", "c", "d", "solo"} {
		if _, err := g.AddNode(k, "Svc", nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, e := range []struct{ s, d string }{
		{"a", "b"}, {"b", "c"}, {"c", "a"}, // SCC {a,b,c}
		{"c", "d"},       // d not on a cycle
		{"solo", "solo"}, // self-loop
	} {
		if _, err := g.AddEdge(e.s, e.d, "CALLS", nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	cycles := g.Cycles()
	if len(cycles) != 2 {
		t.Fatalf("cycles = %d, want 2 (SCC{a,b,c} + self-loop solo)", len(cycles))
	}
	first := keysOf(t, g, cycles[0].Nodes)
	slices.Sort(first)
	if !slices.Equal(first, []string{"a", "b", "c"}) {
		t.Errorf("first cycle = %v, want [a b c]", first)
	}
	if second := keysOf(t, g, cycles[1].Nodes); !slices.Equal(second, []string{"solo"}) {
		t.Errorf("second cycle = %v, want [solo]", second)
	}
}

func TestCyclesEmptyOnDAG(t *testing.T) {
	g := buildDAG(t)
	if cs := g.Cycles(); len(cs) != 0 {
		t.Errorf("Cycles() = %d, want 0", len(cs))
	}
}

// TestCyclesWorksBeforeSeal: the sealed gate is dissolved — Cycles on a
// fresh graph is a legal query (empty answer; any pending layers are
// force-merged first, spec §3.2).
func TestCyclesWorksBeforeSeal(t *testing.T) {
	g := vectra.New()
	if cs := g.Cycles(); len(cs) != 0 {
		t.Errorf("Cycles() on empty graph = %v, want empty", cs)
	}
}
