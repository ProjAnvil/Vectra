package vectra_test

import (
	"errors"
	"fmt"
	"math/rand"
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

