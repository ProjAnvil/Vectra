package vectra

// End-to-end Cypher CREATE tests (2026-10-29 cypher write-clauses plan,
// Task 3): statements enter through the public (*Graph).Cypher entry, route
// to cypher.RunWrite over the Task 2 writeGraph, and their mutations are
// verified through the ordinary read APIs (WAL durability included).

import (
	"errors"
	"maps"
	"path/filepath"
	"strings"
	"testing"
)

// TestCypherCreateKeyedNode pins the CREATE statement's primary shape: a
// keyed node with extra props, the key extracted out of the stored property
// map, RETURN projecting the created node's scalars, and the summary
// counters on the public result.
func TestCypherCreateKeyedNode(t *testing.T) {
	g := New()
	res, err := g.Cypher(`CREATE (n:Service {key:'gateway', env:'prod'}) RETURN n.key, n.env`)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("rows = %d want 1", len(res.Rows))
	}
	if got := res.Rows[0]["n.key"]; got != "gateway" {
		t.Errorf("n.key = %v want gateway", got)
	}
	if got := res.Rows[0]["n.env"]; got != "prod" {
		t.Errorf("n.env = %v want prod", got)
	}
	if res.Summary == nil {
		t.Fatal("write statement must carry a Summary")
	}
	if res.Summary.CreatedNodes != 1 || res.Summary.CreatedEdges != 0 {
		t.Errorf("summary = %+v want CreatedNodes=1", res.Summary)
	}
	n, ok := g.NodeByKey("gateway")
	if !ok {
		t.Fatal("created node missing through the read path")
	}
	if n.Label != "Service" {
		t.Errorf("label = %q want Service", n.Label)
	}
	// The logical key is identity, not a stored property.
	if _, has := n.Props["key"]; has {
		t.Errorf("stored props must not contain the key: %v", n.Props)
	}
	if !maps.Equal(n.Props, map[string]any{"env": "prod"}) {
		t.Errorf("props = %v want {env:prod}", n.Props)
	}
}

// TestCypherCreateGeneratedKey pins keyless CREATE: the key comes from the
// deterministic "_c<id>" ratchet (identical across identical graph runs),
// and the created node is projectable in RETURN.
func TestCypherCreateGeneratedKey(t *testing.T) {
	run := func() (key, label string) {
		g := New()
		res, err := g.Cypher(`CREATE (n:P) RETURN n`)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Rows) != 1 {
			t.Fatalf("rows = %d want 1", len(res.Rows))
		}
		n, ok := res.Rows[0]["n"].(Node)
		if !ok {
			t.Fatalf("RETURN n cell = %#v want a Node", res.Rows[0]["n"])
		}
		return n.Key, n.Label
	}
	k1, l1 := run()
	k2, l2 := run()
	if !strings.HasPrefix(k1, "_c") {
		t.Fatalf("generated key %q lacks the _c prefix", k1)
	}
	if l1 != "P" || l2 != "P" {
		t.Fatalf("label = %q/%q want P/P", l1, l2)
	}
	if k1 != k2 {
		t.Fatalf("keygen not deterministic across identical graphs: %q vs %q", k1, k2)
	}

	// two keyless creates on one graph draw distinct keys, same ratchet
	g := New()
	res, err := g.Cypher(`CREATE (a:P), (b:P) RETURN a.key AS ka, b.key AS kb`)
	if err != nil {
		t.Fatal(err)
	}
	ka, kb := res.Rows[0]["ka"].(string), res.Rows[0]["kb"].(string)
	if ka == kb || !strings.HasPrefix(ka, "_c") || !strings.HasPrefix(kb, "_c") {
		t.Fatalf("draws = %q, %q want distinct _c keys", ka, kb)
	}
	if res.Summary.CreatedNodes != 2 {
		t.Errorf("summary = %+v want CreatedNodes=2", res.Summary)
	}
}

// TestCypherCreateEdgeAndEndpoints pins a one-statement path creation: both
// endpoints created BEFORE the edge call (the Writer never auto-creates
// endpoints), edge props evaluated from the write pattern, whole-entity
// RETURN cells retyped through the post-write view.
func TestCypherCreateEdgeAndEndpoints(t *testing.T) {
	g := New()
	res, err := g.Cypher(`CREATE (a:P {key:'a'})-[:CALLS {w:1}]->(b:P {key:'b'}) RETURN a, b`)
	if err != nil {
		t.Fatal(err)
	}
	if res.Summary.CreatedNodes != 2 || res.Summary.CreatedEdges != 1 {
		t.Fatalf("summary = %+v want 2 nodes / 1 edge", res.Summary)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("rows = %d want 1", len(res.Rows))
	}
	na, okA := res.Rows[0]["a"].(Node)
	nb, okB := res.Rows[0]["b"].(Node)
	if !okA || !okB || na.Key != "a" || nb.Key != "b" || na.Label != "P" {
		t.Fatalf("RETURN cells = %#v / %#v", res.Rows[0]["a"], res.Rows[0]["b"])
	}
	es := liveTriple(t, g, "a", "b", "CALLS")
	if len(es) != 1 || es[0].Props["w"] != 1.0 {
		t.Fatalf("created edge = %+v want one CALLS with w=1", es)
	}
}

// TestCypherCreateRebindEndpoint pins the MATCH + CREATE shape: a bound
// endpoint rebinds (no recreate, no label/props re-specification), the
// fresh endpoint is created, and the edge lands between them.
func TestCypherCreateRebindEndpoint(t *testing.T) {
	g := New()
	if _, err := g.AddNode("x", "T", nil); err != nil {
		t.Fatal(err)
	}
	res, err := g.Cypher(`MATCH (a {key:'x'}) CREATE (a)-[:CALLS {w:1}]->(b:Service {key:'y'})`)
	if err != nil {
		t.Fatal(err)
	}
	if res.Summary.CreatedNodes != 1 || res.Summary.CreatedEdges != 1 {
		t.Fatalf("summary = %+v want 1 node / 1 edge (rebind creates nothing)", res.Summary)
	}
	// the rebound node keeps its identity: not recreated, not relabeled
	x, ok := g.NodeByKey("x")
	if !ok || x.Label != "T" || len(x.Props) != 0 {
		t.Fatalf("rebound endpoint drifted: %+v", x)
	}
	es := liveTriple(t, g, "x", "y", "CALLS")
	if len(es) != 1 || es[0].Props["w"] != 1.0 {
		t.Fatalf("rebind edge = %+v", es)
	}
	// the write is visible to the next read statement (read-your-write)
	rr, err := g.Cypher(`MATCH (a {key:'x'})-[r:CALLS]->(b) RETURN b.key`)
	if err != nil {
		t.Fatal(err)
	}
	if len(rr.Rows) != 1 || rr.Rows[0]["b.key"] != "y" {
		t.Fatalf("read-your-write rows = %+v", rr.Rows)
	}
}

// TestCypherCreateParallelEdge pins CREATE's multigraph semantics: two CREATE
// clauses over the same triple append two parallel edges (AddEdge, not the
// UpsertEdge fold).
func TestCypherCreateParallelEdge(t *testing.T) {
	g := New()
	if _, err := g.Cypher(`CREATE (a:P {key:'a'}), (b:P {key:'b'})`); err != nil {
		t.Fatal(err)
	}
	res, err := g.Cypher(`MATCH (a {key:'a'}), (b {key:'b'}) CREATE (a)-[:E]->(b) CREATE (a)-[:E]->(b)`)
	if err != nil {
		t.Fatal(err)
	}
	if res.Summary.CreatedEdges != 2 {
		t.Fatalf("summary = %+v want CreatedEdges=2", res.Summary)
	}
	if es := liveTriple(t, g, "a", "b", "E"); len(es) != 2 {
		t.Fatalf("parallel edges = %d want 2 (CREATE appends, never folds)", len(es))
	}
}

// TestCypherWriteNoReturn pins the RETURN-less write statement: empty
// columns and rows, summary still carried.
func TestCypherWriteNoReturn(t *testing.T) {
	g := New()
	res, err := g.Cypher(`CREATE (n:P {key:'a'})`)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Columns) != 0 || len(res.Rows) != 0 {
		t.Fatalf("columns/rows = %d/%d want 0/0", len(res.Columns), len(res.Rows))
	}
	if res.Summary == nil || res.Summary.CreatedNodes != 1 {
		t.Fatalf("summary = %+v want CreatedNodes=1", res.Summary)
	}
	if _, ok := g.NodeByKey("a"); !ok {
		t.Fatal("node missing after RETURN-less CREATE")
	}
}

// TestCypherUnwindCreateBulk pins the bulk-import idiom: UNWIND fans rows
// out, the CREATE pattern's property values are EXPRESSIONS evaluated per
// row, and count(n) sees one created node per row.
func TestCypherUnwindCreateBulk(t *testing.T) {
	g := New()
	res, err := g.Cypher(`UNWIND ['a','b','c'] AS k CREATE (n:P {key:k}) RETURN count(n)`)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("rows = %d want 1", len(res.Rows))
	}
	if got, ok := res.Rows[0]["count(n)"].(float64); !ok || got != 3 {
		t.Fatalf("count(n) = %#v want 3", res.Rows[0]["count(n)"])
	}
	if res.Summary.CreatedNodes != 3 {
		t.Fatalf("summary = %+v want CreatedNodes=3", res.Summary)
	}
	for _, k := range []string{"a", "b", "c"} {
		n, ok := g.NodeByKey(k)
		if !ok || n.Label != "P" {
			t.Fatalf("bulk node %q = %+v", k, n)
		}
	}
}

// TestCypherWriteFirstErrorStops pins the first-error-stops contract on the
// per-row runtime type guard: row 1's node survives the statement error
// raised by row 2's non-string key (Review Focus 2).
func TestCypherWriteFirstErrorStops(t *testing.T) {
	g := New()
	_, err := g.Cypher(`UNWIND ['a', 2] AS k CREATE (n:P {key:k})`)
	if err == nil {
		t.Fatal("non-string key must error the statement")
	}
	if !strings.Contains(err.Error(), "key property must be a string") {
		t.Fatalf("error = %v want the key-type message", err)
	}
	var ce *CypherError
	if !errors.As(err, &ce) {
		t.Fatalf("error = %T want *CypherError", err)
	}
	if _, ok := g.NodeByKey("a"); !ok {
		t.Fatal("row 1's node must REMAIN after the statement error (first-error-stops)")
	}
	if got := g.NodeCount(); got != 1 {
		t.Fatalf("node count = %d want 1 (row 2 created nothing)", got)
	}
}

// TestCypherWriteWALRoundTrip pins WAL durability of Cypher CREATE: keyed
// and generated-key nodes reopen with identical keys, labels and props.
func TestCypherWriteWALRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "graph.db")
	g, err := Open(path, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Cypher(`CREATE (n:Svc {key:'gateway', env:'prod'})`); err != nil {
		t.Fatal(err)
	}
	res, err := g.Cypher(`CREATE (m:Gen {v:1}) RETURN m.key`)
	if err != nil {
		t.Fatal(err)
	}
	genKey, ok := res.Rows[0]["m.key"].(string)
	if !ok || !strings.HasPrefix(genKey, "_c") {
		t.Fatalf("generated key = %#v", res.Rows[0]["m.key"])
	}
	want, ok := g.NodeByKey("gateway")
	if !ok {
		t.Fatal("gateway missing before Close")
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}

	g2, err := Open(path, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer g2.Close()
	got, ok := g2.NodeByKey("gateway")
	if !ok {
		t.Fatal("gateway lost across reopen")
	}
	if got.Label != want.Label || !maps.Equal(got.Props, want.Props) {
		t.Fatalf("gateway drifted: %+v want %+v", got, want)
	}
	// the generated key is durable and identical
	gm, ok := g2.NodeByKey(genKey)
	if !ok || gm.Label != "Gen" || gm.Props["v"] != 1.0 {
		t.Fatalf("generated-key node after reopen = %+v (key %q)", gm, genKey)
	}
}

// TestCypherWriteReadPathUnchanged pins three-source visibility around a
// Cypher write: an existing read query over base + unmerged layer keeps its
// exact results (minus the new row) after a Cypher CREATE appends another
// unmerged layer — base, layer and write-layer all visible, nothing lost.
func TestCypherWriteReadPathUnchanged(t *testing.T) {
	g := New()
	for _, k := range []string{"a", "b", "c"} {
		if _, err := g.AddNode(k, "SVC", nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	// an unmerged layer: reads run on the three-source view
	if _, err := g.AddNode("d", "SVC", nil); err != nil {
		t.Fatal(err)
	}
	q := `MATCH (x:SVC) RETURN x.key ORDER BY x.key`
	before, err := g.Cypher(q)
	if err != nil {
		t.Fatal(err)
	}
	if len(before.Rows) != 4 {
		t.Fatalf("before rows = %d want 4", len(before.Rows))
	}

	res, err := g.Cypher(`CREATE (n:SVC {key:'e'})`)
	if err != nil {
		t.Fatal(err)
	}
	if res.Summary == nil || res.Summary.CreatedNodes != 1 {
		t.Fatalf("summary = %+v", res.Summary)
	}
	after, err := g.Cypher(q)
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Rows) != 5 {
		t.Fatalf("after rows = %d want 5 (write visible)", len(after.Rows))
	}
	for i, r := range before.Rows {
		if after.Rows[i]["x.key"] != r["x.key"] {
			t.Fatalf("row %d drifted: %v -> %v", i, r["x.key"], after.Rows[i]["x.key"])
		}
	}
	if after.Rows[4]["x.key"] != "e" {
		t.Fatalf("new row = %v want e", after.Rows[4]["x.key"])
	}
	// aggregation over the same three-source state agrees
	cnt, err := g.Cypher(`MATCH (x:SVC) RETURN count(x)`)
	if err != nil {
		t.Fatal(err)
	}
	if got := cnt.Rows[0]["count(x)"].(float64); got != 5 {
		t.Fatalf("count(x) = %v want 5", got)
	}
}
