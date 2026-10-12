package vectra_test

import (
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"vectra"
)

// buildDAG 构建固定测试图（无环）:
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

// randGraph 生成固定种子随机图（含环）。
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
	if len(res.Depths) != len(want) {
		t.Fatalf("Depths = %v, want %d entries", res.Depths, len(want))
	}
	for key, d := range want {
		id := ids(t, g, key)[0]
		if res.Depths[id] != d {
			t.Errorf("depth[%s] = %d, want %d", key, res.Depths[id], d)
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
	if _, ok := res.Depths[ids(t, g, "kafka")[0]]; ok {
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
	// 从 gateway 出发只有 QUERY 边：gateway 的出边都是 CALLS，无可达
	if res.Count() != 1 {
		t.Errorf("Count = %d, want 1 (仅起点)", res.Count())
	}
	// 从 auth 出发: auth->db(QUERY) 可达，db->kafka 是 EVENT 被过滤
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
		t.Errorf("Count = %d, want 1 (未知标签全过滤)", res.Count())
	}
}

func TestBFSMultiSource(t *testing.T) {
	g := buildDAG(t)
	starts := ids(t, g, "auth", "ledger")
	res, err := g.BFS(starts)
	if err != nil {
		t.Fatal(err)
	}
	if res.Depths[ids(t, g, "auth")[0]] != 0 || res.Depths[ids(t, g, "ledger")[0]] != 0 {
		t.Error("start nodes should have depth 0")
	}
	if res.Depths[ids(t, g, "db")[0]] != 1 {
		t.Errorf("db depth = %d, want 1", res.Depths[ids(t, g, "db")[0]])
	}
	if res.Count() != 4 {
		t.Errorf("Count = %d, want 4", res.Count())
	}
}

func TestBFSMultiSourceDedup(t *testing.T) {
	g := buildDAG(t)
	// 同一起点重复传入只计一次
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
		if res.Depths[ids(t, g, k)[0]] != d {
			t.Errorf("depth[%s] = %d, want %d", k, res.Depths[ids(t, g, k)[0]], d)
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
	if res.Depths[ids(t, g, "a")[0]] != 0 || res.Depths[ids(t, g, "b")[0]] != 1 {
		t.Errorf("depths wrong: %v", res.Depths)
	}
}

func TestBFSUnsealedRejected(t *testing.T) {
	g := vectra.New()
	if _, err := g.AddNode("a", "S", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := g.BFS([]vectra.NodeID{0}); !errors.Is(err, vectra.ErrNotSealed) {
		t.Errorf("err = %v, want ErrNotSealed", err)
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

// buildCyclic 供后续 Reachable/ShortestPath/Cycles 测试复用：
// 环 a<->b, b->c, c->d, d->b (b,c,d 成环), e -DATA-> a
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
	serial, err := g.BFS([]vectra.NodeID{0}, vectra.WithParallelThreshold(1<<30)) // 永远串行
	if err != nil {
		t.Fatal(err)
	}
	parallel, err := g.BFS([]vectra.NodeID{0}, vectra.WithParallelThreshold(0)) // 永远并行
	if err != nil {
		t.Fatal(err)
	}
	if serial.Count() != parallel.Count() {
		t.Fatalf("count serial=%d parallel=%d", serial.Count(), parallel.Count())
	}
	for id, d := range serial.Depths {
		if parallel.Depths[id] != d {
			t.Fatalf("node %d depth serial=%d parallel=%d", id, d, parallel.Depths[id])
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
	for id := range ser.Depths {
		if _, ok := par.Depths[id]; !ok {
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
	for id, d := range ser.Depths {
		if par.Depths[id] != d {
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
	// e 的出边是 DATA；只走 CALLS 时仅剩起点自身
	res, err := g.ReachableIDs(ids(t, g, "e")[0], vectra.ReachWithLabels("CALLS"))
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOf(t, g, res); !slices.Equal(got, []string{"e"}) {
		t.Errorf("Reachable(e, CALLS) = %v, want [e]", got)
	}
	// e -DATA-> a 之后进入 CALLS 域 {a,b,c,d}
	res, err = g.ReachableIDs(ids(t, g, "e")[0], vectra.ReachWithLabels("DATA"))
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOf(t, g, res); !slices.Equal(got, []string{"a", "e"}) {
		t.Errorf("Reachable(e, DATA) = %v, want [a e] (e 经 DATA 到 a，a 无 DATA 出边)", got)
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
	// c 所在分量 {a,b,c,d}；c->d->b：c 到 b 最短 2 跳
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

// TestShortestPathBidirectionalMatchesBFS 属性测试：200 对随机点，
// 双向路径长度必须等于单向 BFS 距离，且路径连续、边真实存在。
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
		d, ok := bfs.Depths[to]
		if !ok {
			if found {
				t.Fatalf("pair %d->%d: bidir found but BFS did not reach", from, to)
			}
			continue
		}
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
