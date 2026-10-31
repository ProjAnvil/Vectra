package vectra

import (
	"maps"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// Writer adapter tests (2026-10-29 cypher write-clauses plan, Task 2):
// the semantics of the root facade's cypher.Writer implementation against
// a real Graph — key/triple addressing, deterministic keygen, fold and
// parallel-edge behavior, index declarations, WAL durability.

// liveTriple lists the live parallel edges of (src, dst, label) through
// the public read path (EdgesOf output is EdgeID-ascending).
func liveTriple(t *testing.T, g *Graph, src, dst, label string) []Edge {
	t.Helper()
	es, err := g.EdgesOf(src, DirOut, label)
	if err != nil {
		t.Fatal(err)
	}
	var out []Edge
	for _, e := range es {
		if dn, ok := g.NodeByID(e.Dst); ok && dn.Key == dst {
			out = append(out, e)
		}
	}
	return out
}

// TestWriterCreateNodeUpsert pins CreateNode's AddNode inheritance: a new
// key is created, an existing key is upserted (stable NodeID, label and
// props replaced wholesale, created=false).
func TestWriterCreateNodeUpsert(t *testing.T) {
	g := New()
	w := newWriteGraph(g)

	id1, created, err := w.CreateNode("a", "P", map[string]any{"x": 1.0})
	if err != nil {
		t.Fatalf("create new key: %v", err)
	}
	if !created {
		t.Fatal("first CreateNode must report created=true")
	}
	got, ok := w.NodeIDByKey("a")
	if !ok || got != id1 {
		t.Fatalf("NodeIDByKey = %d,%v want %d", got, ok, id1)
	}

	id2, created, err := w.CreateNode("a", "Q", map[string]any{"y": 2.0})
	if err != nil {
		t.Fatalf("upsert existing key: %v", err)
	}
	if created {
		t.Fatal("second CreateNode on the same key must report created=false")
	}
	if id2 != id1 {
		t.Fatalf("upsert must keep the NodeID: %d want %d", id2, id1)
	}
	n, ok := g.NodeByKey("a")
	if !ok {
		t.Fatal("node a missing after upsert")
	}
	if n.Label != "Q" {
		t.Errorf("label = %q want Q (replaced)", n.Label)
	}
	if !maps.Equal(n.Props, map[string]any{"y": 2.0}) {
		t.Errorf("props = %v want wholesale-replaced {y:2}", n.Props)
	}
}

// TestWriterNewNodeKeyDeterministic pins the keygen contract: identical
// state + identical call sequence draws identical keys (raft FSM
// re-execution), draws never repeat, generated keys never collide with
// user keys across 10k interleaved draws, and a user key squatting in the
// "_c<id>" namespace is skipped.
func TestWriterNewNodeKeyDeterministic(t *testing.T) {
	seq := func() ([]string, error) {
		g := New()
		w := newWriteGraph(g)
		if _, err := g.AddNode("seed", "P", map[string]any{"i": 1.0}); err != nil {
			return nil, err
		}
		k1 := w.NewNodeKey()
		k2 := w.NewNodeKey()
		if _, err := g.AddNode("u1", "P", nil); err != nil {
			return nil, err
		}
		k3 := w.NewNodeKey()
		return []string{k1, k2, k3}, nil
	}
	a, err := seq()
	if err != nil {
		t.Fatal(err)
	}
	b, err := seq()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(a, b) {
		t.Fatalf("identical state + sequence drew %v then %v", a, b)
	}
	if a[0] == a[1] || a[1] == a[2] || a[0] == a[2] {
		t.Fatalf("repeated draw: %v", a)
	}
	for _, k := range a {
		if !strings.HasPrefix(k, "_c") {
			t.Fatalf("drawn key %q lacks the _c prefix", k)
		}
	}

	// 10k draws interleaved with user-keyed nodes: pairwise distinct and
	// never live at draw time.
	g := New()
	w := newWriteGraph(g)
	seen := make(map[string]bool, 10000)
	for i := 0; i < 10000; i++ {
		if _, err := g.AddNode("u"+strconv.Itoa(i), "U", nil); err != nil {
			t.Fatal(err)
		}
		k := w.NewNodeKey()
		if seen[k] {
			t.Fatalf("draw %d repeated key %s", i, k)
		}
		if g.nodeLive(k) {
			t.Fatalf("draw %d returned a live key: %s", i, k)
		}
		seen[k] = true
	}

	// a user key ahead of the ratchet is skipped, never handed out
	g2 := New()
	w2 := newWriteGraph(g2)
	if _, err := g2.AddNode("_c5", "P", nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if k := w2.NewNodeKey(); k == "_c5" {
			t.Fatal("draw returned a key squatted by a user node")
		}
	}
}

// TestWriterSetNodePropsMergeAndDelete pins SetNodeProps: merge overlay,
// nil deletes a key, the counter counts sets + deletes, untouched keys
// survive, and a missing key errors.
func TestWriterSetNodePropsMergeAndDelete(t *testing.T) {
	g := New()
	if _, err := g.AddNode("a", "P", map[string]any{"x": 1.0, "keep": "yes"}); err != nil {
		t.Fatal(err)
	}
	w := newWriteGraph(g)

	n, err := w.SetNodeProps("a", map[string]any{"x": 2.0, "new": "n"})
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if n != 2 {
		t.Fatalf("counter = %d want 2 (sets + deletes)", n)
	}
	want := map[string]any{"x": 2.0, "keep": "yes", "new": "n"}
	node, ok := g.NodeByKey("a")
	if !ok || !maps.Equal(node.Props, want) {
		t.Fatalf("after merge props = %v want %v", node.Props, want)
	}

	n, err = w.SetNodeProps("a", map[string]any{"x": nil, "keep": nil})
	if err != nil {
		t.Fatalf("nil-delete: %v", err)
	}
	if n != 2 {
		t.Fatalf("counter = %d want 2", n)
	}
	want = map[string]any{"new": "n"}
	node, ok = g.NodeByKey("a")
	if !ok || !maps.Equal(node.Props, want) {
		t.Fatalf("after nil-delete props = %v want %v", node.Props, want)
	}

	if _, err := w.SetNodeProps("missing", map[string]any{"x": 1.0}); err != ErrNodeNotFound {
		t.Fatalf("missing key error = %v want ErrNodeNotFound", err)
	}
}

// TestWriterEdgeTripleOps pins the triple-addressed edge surface: the
// upsert created flag and its fold, CreateEdge parallels, SetEdgeProps
// touching every parallel, EdgeIDByTriple rebinding, and triple deletion.
func TestWriterEdgeTripleOps(t *testing.T) {
	g := New()
	for _, k := range []string{"a", "b"} {
		if _, err := g.AddNode(k, "P", nil); err != nil {
			t.Fatal(err)
		}
	}
	w := newWriteGraph(g)

	// upsert: creates, then folds in place
	created, err := w.UpsertEdgeTriple("a", "b", "rel", map[string]any{"w": 1.0})
	if err != nil {
		t.Fatalf("upsert create: %v", err)
	}
	if !created {
		t.Fatal("first UpsertEdgeTriple must report created=true")
	}
	eid, ok := w.EdgeIDByTriple("a", "b", "rel")
	if !ok {
		t.Fatal("EdgeIDByTriple after create")
	}
	created, err = w.UpsertEdgeTriple("a", "b", "rel", map[string]any{"w": 2.0})
	if err != nil {
		t.Fatalf("upsert fold: %v", err)
	}
	if created {
		t.Fatal("fold must report created=false")
	}
	if eid2, ok := w.EdgeIDByTriple("a", "b", "rel"); !ok || eid2 != eid {
		t.Fatalf("fold must keep the winner ID: %d,%v want %d", eid2, ok, eid)
	}
	if e, ok := g.EdgeByID(EdgeID(eid)); !ok || e.Props["w"] != 2.0 {
		t.Fatalf("fold must replace props wholesale: %+v", e)
	}

	// CreateEdge appends a NEW parallel (CREATE semantics: duplicates allowed)
	id2, err := w.CreateEdge("a", "b", "rel", map[string]any{"w": 5.0})
	if err != nil {
		t.Fatalf("CreateEdge: %v", err)
	}
	if id2 == eid {
		t.Fatal("CreateEdge must append a new parallel, not fold")
	}
	if es := liveTriple(t, g, "a", "b", "rel"); len(es) != 2 {
		t.Fatalf("parallels = %d want 2", len(es))
	}
	if first, ok := w.EdgeIDByTriple("a", "b", "rel"); !ok || first != eid {
		t.Fatalf("EdgeIDByTriple = %d,%v want the first parallel %d", first, ok, eid)
	}

	// SetEdgeProps touches every parallel; the parallels survive it
	n, err := w.SetEdgeProps("a", "b", "rel", map[string]any{"tag": "x"})
	if err != nil {
		t.Fatalf("SetEdgeProps: %v", err)
	}
	if n != 2 {
		t.Fatalf("SetEdgeProps touched %d want 2", n)
	}
	es := liveTriple(t, g, "a", "b", "rel")
	if len(es) != 2 {
		t.Fatalf("parallels after SetEdgeProps = %d want 2", len(es))
	}
	ws := map[float64]bool{}
	for _, e := range es {
		if e.Props["tag"] != "x" {
			t.Errorf("edge %d missed the written prop: %v", e.ID, e.Props)
		}
		if wv, ok := e.Props["w"].(float64); ok {
			ws[wv] = true
		}
	}
	if !ws[2.0] || !ws[5.0] {
		t.Errorf("per-parallel props lost: %v", es)
	}

	// triple delete removes every parallel
	n, err = w.DeleteEdgeTriple("a", "b", "rel")
	if err != nil {
		t.Fatalf("DeleteEdgeTriple: %v", err)
	}
	if n != 2 {
		t.Fatalf("DeleteEdgeTriple = %d want 2", n)
	}
	if es := liveTriple(t, g, "a", "b", "rel"); len(es) != 0 {
		t.Fatalf("edges survive triple delete: %v", es)
	}
	if _, ok := w.EdgeIDByTriple("a", "b", "rel"); ok {
		t.Fatal("EdgeIDByTriple must miss after triple delete")
	}

	// CreateEdge on a missing endpoint is a statement error (first-error-stops)
	if _, err := w.CreateEdge("a", "missing", "rel", nil); err != ErrNodeNotFound {
		t.Fatalf("missing endpoint error = %v want ErrNodeNotFound", err)
	}
}

// TestWriterIndexDecls pins the Writer index surface: declarations stay
// pending (an unmerged layer) until Seal, re-declaring is a no-op, and
// after Seal both read paths serve.
func TestWriterIndexDecls(t *testing.T) {
	g := New()
	if _, err := g.AddNode("a", "Svc", map[string]any{"env": "prod", "desc": "hello world"}); err != nil {
		t.Fatal(err)
	}
	w := newWriteGraph(g)

	if err := w.IndexProperty("env"); err != nil {
		t.Fatalf("IndexProperty: %v", err)
	}
	if err := w.IndexFullText("desc"); err != nil {
		t.Fatalf("IndexFullText: %v", err)
	}
	if g.Sealed() {
		t.Fatal("declarations must stay pending (an unmerged layer) until Seal")
	}
	// idempotent pre-Seal: re-declaring the same keys changes nothing
	if err := w.IndexProperty("env"); err != nil {
		t.Fatalf("re-IndexProperty: %v", err)
	}
	if err := w.IndexFullText("desc"); err != nil {
		t.Fatalf("re-IndexFullText: %v", err)
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	if !g.Sealed() {
		t.Fatal("Seal must materialize the declarations into the base")
	}
	if nodes, ok := g.LookupByProperty("env", "prod"); !ok || len(nodes) != 1 || nodes[0].Key != "a" {
		t.Fatalf("LookupByProperty = %d nodes, ok=%v want [a]", len(nodes), ok)
	}
	if nodes, ok := g.LookupByContains("desc", "hello"); !ok || len(nodes) != 1 || nodes[0].Key != "a" {
		t.Fatalf("LookupByContains = %d nodes, ok=%v want [a]", len(nodes), ok)
	}
	// idempotent over the sealed base: a re-declare opens no window
	if err := w.IndexProperty("env"); err != nil {
		t.Fatalf("post-Seal re-declare: %v", err)
	}
	if !g.Sealed() {
		t.Fatal("re-declare over the base must be a no-op")
	}
}

// TestWriterWALReopen pins WAL durability through the Writer surface:
// everything written under WALMode Always reopens with identical state and
// keys, the generated keys included.
func TestWriterWALReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "graph.db")
	g, err := Open(path, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	w := newWriteGraph(g)
	if _, err := g.AddNode("a", "P", map[string]any{"n": 1.0}); err != nil {
		t.Fatal(err)
	}
	ck := w.NewNodeKey()
	if _, _, err := w.CreateNode(ck, "C", map[string]any{"v": 2.0}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.SetNodeProps("a", map[string]any{"m": 3.0}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.UpsertEdgeTriple("a", ck, "rel", map[string]any{"w": 1.0}); err != nil {
		t.Fatal(err)
	}
	want := map[string]Node{}
	for _, k := range []string{"a", ck} {
		n, ok := g.NodeByKey(k)
		if !ok {
			t.Fatalf("node %q missing before Close", k)
		}
		want[k] = n
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}

	g2, err := Open(path, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer g2.Close()
	for k, wn := range want {
		n, ok := g2.NodeByKey(k)
		if !ok {
			t.Fatalf("node %q lost across reopen", k)
		}
		if n.Label != wn.Label || !maps.Equal(n.Props, wn.Props) {
			t.Fatalf("node %q drifted across reopen: %+v want %+v", k, n, wn)
		}
	}
	if es := liveTriple(t, g2, "a", ck, "rel"); len(es) != 1 || es[0].Props["w"] != 1.0 {
		t.Fatalf("edge triple after reopen = %v", es)
	}
}

// TestCypherIsWrite pins the classifier: write clauses and index DDL are
// writes, read pipelines are not, and parse errors pass through unchanged.
func TestCypherIsWrite(t *testing.T) {
	cases := []struct {
		q    string
		want bool
	}{
		{"MATCH (a:P) RETURN a", false},
		{"MATCH (a:P)-[r:CALLS]->(b:P) RETURN b", false},
		{"MATCH (n:P) WITH n.key AS k RETURN k", false},
		{"CREATE (n:P {key:'k'})", true},
		{"MATCH (a {key:'x'}) CREATE (a)-[:CALLS]->(b {key:'y'}) RETURN b", true},
		{"MATCH (a) SET a.env = 'staging' RETURN a", true},
		{"MATCH (a) DELETE a", true},
		{"MATCH (a)-[r:CALLS]->(b) DETACH DELETE a, r", true},
		{"MERGE (n:P {key:'x'})", true},
		{"MERGE (n:P {key:'x'}) ON CREATE SET n.created = true", true},
		{"CREATE INDEX ON (n.env)", true},
		{"CREATE FULLTEXT INDEX ON (n.body)", true},
		{"CREATE CONSTRAINT FOR (n:Person) REQUIRE n.email IS UNIQUE", true},
		{"DROP CONSTRAINT FOR (n:Person) REQUIRE n.email IS UNIQUE", true},
	}
	for _, c := range cases {
		got, err := CypherIsWrite(c.q)
		if err != nil {
			t.Errorf("%q: %v", c.q, err)
			continue
		}
		if got != c.want {
			t.Errorf("%q: IsWrite = %v want %v", c.q, got, c.want)
		}
	}
	if got, err := CypherIsWrite("MATCH ("); err == nil || got {
		t.Fatalf("parse error must pass through unchanged: got=%v err=%v", got, err)
	}
}
