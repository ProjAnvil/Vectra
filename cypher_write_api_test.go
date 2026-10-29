package vectra

// End-to-end Cypher CREATE tests (2026-10-29 cypher write-clauses plan,
// Task 3): statements enter through the public (*Graph).Cypher entry, route
// to cypher.RunWrite over the Task 2 writeGraph, and their mutations are
// verified through the ordinary read APIs (WAL durability included).

import (
	"errors"
	"fmt"
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

// TestCypherSetNodeProp pins the SET statement's primary shape: per-item
// property writes on a matched node (merge, never wholesale replace),
// RETURN projecting the post-SET values, and PropertiesSet counting every
// write.
func TestCypherSetNodeProp(t *testing.T) {
	g := New()
	if _, err := g.AddNode("x", "Svc", map[string]any{"env": "dev"}); err != nil {
		t.Fatal(err)
	}
	res, err := g.Cypher(`MATCH (a {key:'x'}) SET a.env = 'staging', a.weight = 3 RETURN a.env, a.weight`)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("rows = %d want 1", len(res.Rows))
	}
	if got := res.Rows[0]["a.env"]; got != "staging" {
		t.Errorf("a.env = %v want staging", got)
	}
	if got := res.Rows[0]["a.weight"]; got != 3.0 {
		t.Errorf("a.weight = %v want 3", got)
	}
	if res.Summary == nil || res.Summary.PropertiesSet != 2 {
		t.Fatalf("summary = %+v want PropertiesSet=2", res.Summary)
	}
	n, ok := g.NodeByKey("x")
	if !ok {
		t.Fatal("node x missing after SET")
	}
	if !maps.Equal(n.Props, map[string]any{"env": "staging", "weight": 3.0}) {
		t.Errorf("stored props = %v want merged {env:staging, weight:3}", n.Props)
	}
}

// TestCypherSetNullDeletes pins the null-RHS form: SET a.gone = null removes
// the property, RETURN renders it null, and the delete still counts toward
// PropertiesSet.
func TestCypherSetNullDeletes(t *testing.T) {
	g := New()
	if _, err := g.AddNode("x", "Svc", map[string]any{"env": "dev", "gone": 1.0}); err != nil {
		t.Fatal(err)
	}
	res, err := g.Cypher(`MATCH (a {key:'x'}) SET a.gone = null RETURN a.gone, a.env`)
	if err != nil {
		t.Fatal(err)
	}
	if got := res.Rows[0]["a.gone"]; got != nil {
		t.Errorf("a.gone = %v want nil (deleted)", got)
	}
	if got := res.Rows[0]["a.env"]; got != "dev" {
		t.Errorf("a.env = %v want dev (untouched)", got)
	}
	if res.Summary == nil || res.Summary.PropertiesSet != 1 {
		t.Fatalf("summary = %+v want PropertiesSet=1 (deletes count)", res.Summary)
	}
	n, _ := g.NodeByKey("x")
	if _, has := n.Props["gone"]; has {
		t.Errorf("stored props must not contain gone: %v", n.Props)
	}
}

// TestCypherSetFromOtherVar pins RHS reads of OTHER entities: the value
// resolves through the pinned pre-write source via the ordinary evaluator,
// and only the target changes.
func TestCypherSetFromOtherVar(t *testing.T) {
	g := New()
	if _, err := g.AddNode("x", "P", map[string]any{"env": "prod"}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("y", "P", map[string]any{"env": "canary"}); err != nil {
		t.Fatal(err)
	}
	res, err := g.Cypher(`MATCH (a {key:'x'}), (b {key:'y'}) SET a.env = b.env RETURN a.env`)
	if err != nil {
		t.Fatal(err)
	}
	if got := res.Rows[0]["a.env"]; got != "canary" {
		t.Fatalf("a.env = %v want canary (copied from b)", got)
	}
	if res.Summary == nil || res.Summary.PropertiesSet != 1 {
		t.Fatalf("summary = %+v want PropertiesSet=1", res.Summary)
	}
	a, _ := g.NodeByKey("x")
	if a.Props["env"] != "canary" {
		t.Errorf("a.env stored = %v want canary", a.Props["env"])
	}
	b, _ := g.NodeByKey("y")
	if b.Props["env"] != "canary" {
		t.Errorf("b.env = %v want canary (source unchanged)", b.Props["env"])
	}
}

// TestCypherSetSeesEarlierItem pins left-to-right item visibility: a later
// item's RHS reads the value an earlier item staged in the same row, not
// the pre-write value.
func TestCypherSetSeesEarlierItem(t *testing.T) {
	g := New()
	if _, err := g.AddNode("x", "P", map[string]any{"x": 7.0}); err != nil {
		t.Fatal(err)
	}
	res, err := g.Cypher(`MATCH (a {key:'x'}) SET a.x = 1, a.y = a.x RETURN a.x, a.y`)
	if err != nil {
		t.Fatal(err)
	}
	if got := res.Rows[0]["a.x"]; got != 1.0 {
		t.Errorf("a.x = %v want 1 (overwritten)", got)
	}
	if got := res.Rows[0]["a.y"]; got != 1.0 {
		t.Errorf("a.y = %v want 1 (read the staged a.x, not the pre-write 7)", got)
	}
}

// TestCypherSetEdgeProp pins the edge form on both fronts: same-row item
// visibility through the pending overlay (the edge case the node tests
// cannot reach), and the triple-addressed divergence — a MATCH-bound edge
// variable writes EVERY live parallel, with the parallel count unchanged.
func TestCypherSetEdgeProp(t *testing.T) {
	g := New()
	if _, err := g.Cypher(`CREATE (a:P {key:'a'})-[:CALLS {w:1}]->(b:P {key:'b'})`); err != nil {
		t.Fatal(err)
	}
	// one parallel: one match row; item 2 reads item 1's staged value
	res, err := g.Cypher(`MATCH (a {key:'a'})-[r:CALLS]->(b {key:'b'}) SET r.w = 9, r.note = r.w`)
	if err != nil {
		t.Fatal(err)
	}
	if es := liveTriple(t, g, "a", "b", "CALLS"); len(es) != 1 || es[0].Props["w"] != 9.0 || es[0].Props["note"] != 9.0 {
		t.Fatalf("edge after same-row items = %+v want one CALLS {w:9, note:9}", es)
	}
	if res.Summary == nil || res.Summary.PropertiesSet != 2 {
		t.Fatalf("summary = %+v want PropertiesSet=2", res.Summary)
	}

	// a second parallel: SET reaches BOTH parallels of the triple
	if _, err := g.Cypher(`MATCH (a {key:'a'}), (b {key:'b'}) CREATE (a)-[:CALLS {w:2}]->(b)`); err != nil {
		t.Fatal(err)
	}
	res, err = g.Cypher(`MATCH (a {key:'a'})-[r:CALLS]->(b {key:'b'}) SET r.tag = 'go'`)
	if err != nil {
		t.Fatal(err)
	}
	es := liveTriple(t, g, "a", "b", "CALLS")
	if len(es) != 2 {
		t.Fatalf("parallels = %d want 2 (count unchanged through the rewrite)", len(es))
	}
	ws := map[float64]bool{}
	for _, e := range es {
		if e.Props["tag"] != "go" {
			t.Errorf("parallel %d tag = %v want go (triple-addressed)", e.ID, e.Props["tag"])
		}
		w, isNum := e.Props["w"].(float64)
		if !isNum {
			t.Fatalf("parallel %d w = %#v want a number", e.ID, e.Props["w"])
		}
		ws[w] = true
	}
	if !ws[9.0] || !ws[2.0] {
		t.Errorf("per-parallel w values = %v want {9, 2} preserved", ws)
	}
	// 2 match rows x 1 item x 2 parallels touched per call
	if res.Summary.PropertiesSet != 4 {
		t.Errorf("PropertiesSet = %d want 4 (2 rows x 2 edges touched)", res.Summary.PropertiesSet)
	}
}

// TestCypherSetEdgeReturnScalars pins the edge SET read overlay: RETURN of
// edge SCALARS after SET serves the staged props AND the pre-existing ones
// — the fold seeds from the pre-write record of the bound parallel (never
// from the freshly rewritten canonical ID, which resolves nowhere) and
// accumulates per triple across items (Review F1).
func TestCypherSetEdgeReturnScalars(t *testing.T) {
	g := New()
	if _, err := g.Cypher(`CREATE (a:P {key:'a'})-[:CALLS {keep:'me', w:1}]->(b:P {key:'b'})`); err != nil {
		t.Fatal(err)
	}
	// shape 1: one item; the untouched prop must survive the RETURN
	res, err := g.Cypher(`MATCH (a {key:'a'})-[r:CALLS]->(b {key:'b'}) SET r.tag = 'go' RETURN r.keep, r.tag`)
	if err != nil {
		t.Fatal(err)
	}
	if got := res.Rows[0]["r.keep"]; got != "me" {
		t.Errorf("r.keep = %v want me (seed from the pre-write record)", got)
	}
	if got := res.Rows[0]["r.tag"]; got != "go" {
		t.Errorf("r.tag = %v want go", got)
	}
	// shape 2: two items; every staged prop renders, not just the last
	res, err = g.Cypher(`MATCH (a {key:'a'})-[r:CALLS]->(b {key:'b'}) SET r.w = 9, r.note = 'x' RETURN r.w, r.note, r.keep`)
	if err != nil {
		t.Fatal(err)
	}
	row := res.Rows[0]
	if row["r.w"] != 9.0 || row["r.note"] != "x" || row["r.keep"] != "me" {
		t.Fatalf("row = %v want w=9, note=x, keep=me (fold accumulates across items)", row)
	}
}

// TestCypherSetUnboundErrors pins the OPTIONAL MATCH miss: the target var
// is unbound in the kept row and SET errors (Neo4j parity),
// first-error-stops, inventing nothing.
func TestCypherSetUnboundErrors(t *testing.T) {
	g := New()
	if _, err := g.AddNode("keep", "P", nil); err != nil {
		t.Fatal(err)
	}
	_, err := g.Cypher(`OPTIONAL MATCH (a {key:'missing'}) SET a.x = 1`)
	if err == nil {
		t.Fatal("SET on an OPTIONAL MATCH null must error")
	}
	if !strings.Contains(err.Error(), "SET requires a bound node or edge variable") {
		t.Fatalf("error = %v want the bound-variable message", err)
	}
	var ce *CypherError
	if !errors.As(err, &ce) {
		t.Fatalf("error = %T want *CypherError", err)
	}
	if n := g.NodeCount(); n != 1 {
		t.Fatalf("node count = %d want 1 (nothing invented)", n)
	}
}

// TestCypherCreateEdgeLabelRequired pins the one-label rule as a STATIC
// check: a label-less write edge is a validation error BEFORE any row
// executes, so no partial node writes survive the statement error (the old
// runtime-per-row check let the endpoint creates land first).
func TestCypherCreateEdgeLabelRequired(t *testing.T) {
	g := New()
	_, err := g.Cypher(`CREATE (a:P {key:'x'})-[r]->(b:P {key:'y'})`)
	if err == nil || !strings.Contains(err.Error(), "write edge patterns require exactly one relationship label") {
		t.Fatalf("err = %v want the one-label message", err)
	}
	if n := g.NodeCount(); n != 0 {
		t.Fatalf("nodes = %d want 0 (static check precedes any write)", n)
	}
}

// TestCypherSetEdgeWALRoundTrip pins WAL durability of the edge SET path:
// SetEdgeProps rewrites each parallel as a delete+re-add composite (fresh
// EdgeIDs), so a Cypher SET over a multi-parallel triple under WALMode
// Always must reopen with every parallel carrying the new props and the
// parallel count unchanged.
func TestCypherSetEdgeWALRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "graph.db")
	g, err := Open(path, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Cypher(`CREATE (a:P {key:'a'}), (b:P {key:'b'})`); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Cypher(`MATCH (a {key:'a'}), (b {key:'b'}) CREATE (a)-[:CALLS {w:1}]->(b) CREATE (a)-[:CALLS {w:2}]->(b)`); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Cypher(`MATCH (a {key:'a'})-[r:CALLS]->(b {key:'b'}) SET r.w = 9`); err != nil {
		t.Fatal(err)
	}
	if es := liveTriple(t, g, "a", "b", "CALLS"); len(es) != 2 {
		t.Fatalf("parallels before Close = %d want 2", len(es))
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}

	g2, err := Open(path, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer g2.Close()
	es := liveTriple(t, g2, "a", "b", "CALLS")
	if len(es) != 2 {
		t.Fatalf("parallels after reopen = %d want 2 (count unchanged)", len(es))
	}
	for _, e := range es {
		if e.Props["w"] != 9.0 {
			t.Errorf("parallel %d w = %v want 9", e.ID, e.Props["w"])
		}
	}
}

// TestCypherDeleteDetached pins DETACH DELETE: the node and every incident
// edge die (cascade), the neighbors survive, and the summary counts BOTH
// sides — DeletedNodes the actual node deletions (a node bound in several
// rows deletes once; later rows are silent not-found no-ops) and
// DeletedEdges the cascade's edge kills (the Writer's DeleteNodeCascade
// count, not just edge-var deletions).
func TestCypherDeleteDetached(t *testing.T) {
	g := New()
	if _, err := g.Cypher(`CREATE (h:P {key:'hub'}), (x:P {key:'x'}), (y:P {key:'y'})`); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"x", "y"} {
		if _, err := g.Cypher(`MATCH (h {key:'hub'}), (o {key:'` + k + `'}) CREATE (h)-[:E]->(o)`); err != nil {
			t.Fatal(err)
		}
	}
	res, err := g.Cypher(`MATCH (h {key:'hub'}) DETACH DELETE h`)
	if err != nil {
		t.Fatal(err)
	}
	if res.Summary == nil || res.Summary.DeletedNodes != 1 {
		t.Fatalf("summary = %+v want DeletedNodes=1", res.Summary)
	}
	if res.Summary.DeletedEdges != 2 {
		t.Fatalf("summary = %+v want DeletedEdges=2 (the cascade's kills are counted)", res.Summary)
	}
	if _, ok := g.NodeByKey("hub"); ok {
		t.Fatal("hub survived DETACH DELETE")
	}
	// both incident edges died with the cascade (EdgeCount, not EdgesOf:
	// the hub key no longer resolves)
	if n := g.EdgeCount(); n != 0 {
		t.Fatalf("edge count = %d want 0 (cascade killed both)", n)
	}
	for _, k := range []string{"x", "y"} {
		if _, ok := g.NodeByKey(k); !ok {
			t.Fatalf("neighbor %q must survive the cascade", k)
		}
	}

	// repeat-row no-op: t is bound once per :P row (5 rows here); row 1
	// deletes it (no edges: DETACH not required), the rest resolve nowhere.
	if _, err := g.Cypher(`CREATE (t:P {key:'t'}), (o1:P {key:'o1'}), (o2:P {key:'o2'})`); err != nil {
		t.Fatal(err)
	}
	res, err = g.Cypher(`MATCH (t {key:'t'}), (o:P) DELETE t`)
	if err != nil {
		t.Fatal(err)
	}
	if res.Summary == nil || res.Summary.DeletedNodes != 1 {
		t.Fatalf("summary = %+v want DeletedNodes=1 (repeat rows are no-ops)", res.Summary)
	}
	if _, ok := g.NodeByKey("t"); ok {
		t.Fatal("t survived DELETE")
	}
	if n := g.NodeCount(); n != 4 { // x, y, o1, o2
		t.Fatalf("node count = %d want 4 (only t died)", n)
	}
}

// TestCypherDeleteNodeWithEdgesErrors pins the non-DETACH guard: a node
// with live incident edges errors with the DETACH guidance, and
// first-error-stops fires BEFORE any write — the node and its edge survive.
func TestCypherDeleteNodeWithEdgesErrors(t *testing.T) {
	g := New()
	if _, err := g.Cypher(`CREATE (a:P {key:'a'})-[:E]->(b:P {key:'b'})`); err != nil {
		t.Fatal(err)
	}
	_, err := g.Cypher(`MATCH (a {key:'a'}) DELETE a`)
	if err == nil {
		t.Fatal("DELETE of an edge-holding node without DETACH must error")
	}
	if !strings.Contains(err.Error(), "use DETACH DELETE") {
		t.Fatalf("error = %v want the DETACH guidance message", err)
	}
	var ce *CypherError
	if !errors.As(err, &ce) {
		t.Fatalf("error = %T want *CypherError", err)
	}
	if _, ok := g.NodeByKey("a"); !ok {
		t.Fatal("node a must survive the guard error")
	}
	if es := liveTriple(t, g, "a", "b", "E"); len(es) != 1 {
		t.Fatalf("edge must survive the guard error: %+v", es)
	}
}

// TestCypherDeleteEdgeTripleParallel pins the E6 divergence (Review Focus
// 5): one DELETE r removes EVERY parallel of the triple and counts them all
// — two parallels bound as two match rows yield DeletedEdges=2 (row 1 kills
// both, row 2 finds none), and the endpoints survive.
func TestCypherDeleteEdgeTripleParallel(t *testing.T) {
	g := New()
	if _, err := g.Cypher(`CREATE (a:P {key:'a'}), (b:P {key:'b'})`); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Cypher(`MATCH (a {key:'a'}), (b {key:'b'}) CREATE (a)-[:E]->(b) CREATE (a)-[:E]->(b)`); err != nil {
		t.Fatal(err)
	}
	res, err := g.Cypher(`MATCH (a {key:'a'})-[r:E]->(b {key:'b'}) DELETE r`)
	if err != nil {
		t.Fatal(err)
	}
	if res.Summary == nil || res.Summary.DeletedEdges != 2 {
		t.Fatalf("summary = %+v want DeletedEdges=2 (one DELETE r removes both parallels)", res.Summary)
	}
	if res.Summary.DeletedNodes != 0 {
		t.Errorf("DeletedNodes = %d want 0 (endpoints untouched)", res.Summary.DeletedNodes)
	}
	if es := liveTriple(t, g, "a", "b", "E"); len(es) != 0 {
		t.Fatalf("parallels survived: %+v", es)
	}
	for _, k := range []string{"a", "b"} {
		if _, ok := g.NodeByKey(k); !ok {
			t.Fatalf("endpoint %q must survive an edge DELETE", k)
		}
	}
}

// TestCypherDeleteThenReturn pins the post-DELETE rebind (Review Focus 4):
// the trailing RETURN keeps the row but DROPS the deleted binding, so the
// projected scalar renders a nil cell — never a stale value, never an error.
func TestCypherDeleteThenReturn(t *testing.T) {
	g := New()
	if _, err := g.AddNode("x", "P", map[string]any{"v": 1.0}); err != nil {
		t.Fatal(err)
	}
	res, err := g.Cypher(`MATCH (a {key:'x'}) DELETE a RETURN a.key`)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("rows = %d want 1 (the row survives; the binding drops)", len(res.Rows))
	}
	if got := res.Rows[0]["a.key"]; got != nil {
		t.Fatalf("a.key = %v want nil (deleted binding renders null)", got)
	}
	if res.Summary == nil || res.Summary.DeletedNodes != 1 {
		t.Fatalf("summary = %+v want DeletedNodes=1", res.Summary)
	}
	if _, ok := g.NodeByKey("x"); ok {
		t.Fatal("node x survived DELETE")
	}
}

// TestCypherDeleteUnboundErrors pins the OPTIONAL MATCH miss on DELETE: the
// var is unbound in the kept row and the statement errors (the SET message
// shape), first-error-stops, deleting nothing.
func TestCypherDeleteUnboundErrors(t *testing.T) {
	g := New()
	if _, err := g.AddNode("keep", "P", nil); err != nil {
		t.Fatal(err)
	}
	_, err := g.Cypher(`OPTIONAL MATCH (a {key:'missing'}) DELETE a`)
	if err == nil {
		t.Fatal("DELETE on an OPTIONAL MATCH null must error")
	}
	if !strings.Contains(err.Error(), "DELETE requires a bound node or edge variable") {
		t.Fatalf("error = %v want the bound-variable message", err)
	}
	var ce *CypherError
	if !errors.As(err, &ce) {
		t.Fatalf("error = %T want *CypherError", err)
	}
	if n := g.NodeCount(); n != 1 {
		t.Fatalf("node count = %d want 1 (nothing deleted, nothing invented)", n)
	}
}

// TestCypherDeleteCascadeThenEdgeVar pins the edge arm's endpoint-liveness
// gate: in a mixed DETACH DELETE, the node arm's cascade may kill an edge
// var's triple before the edge arm runs — the dead triple is a silent no-op
// skip (mirroring the node arm's earlier-row not-found rule), never a
// leaked root ErrNodeNotFound. The statement succeeds with both entities
// gone, the cascade's kill counted in DeletedEdges and the skipped arm
// contributing nothing.
func TestCypherDeleteCascadeThenEdgeVar(t *testing.T) {
	g := New()
	if _, err := g.Cypher(`CREATE (a:P {key:'x'})-[:CALLS]->(b:P {key:'y'})`); err != nil {
		t.Fatal(err)
	}
	res, err := g.Cypher(`MATCH (a {key:'x'})-[r:CALLS]->(b {key:'y'}) DETACH DELETE a, r`)
	if err != nil {
		t.Fatal(err)
	}
	if res.Summary == nil || res.Summary.DeletedNodes != 1 || res.Summary.DeletedEdges != 1 {
		t.Fatalf("summary = %+v want DeletedNodes=1, DeletedEdges=1 (cascade kill; r's arm no-ops)", res.Summary)
	}
	if _, ok := g.NodeByKey("x"); ok {
		t.Fatal("node x survived DETACH DELETE")
	}
	if _, ok := g.NodeByKey("y"); !ok {
		t.Fatal("node y must survive the cascade")
	}
	if n := g.EdgeCount(); n != 0 {
		t.Fatalf("edge count = %d want 0 (the cascade killed it; EdgeCount, not EdgesOf: the x key no longer resolves)", n)
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

// TestCypherMergeNodeCreateThenMatch pins the node MERGE identity rule: the
// same statement run twice merges by KEY — the first run creates (ON CREATE
// SET fires, CreatedNodes=1, the pattern's props stored MINUS key), the
// second matches (ON MATCH SET fires, CreatedNodes=0, the pattern's props
// NOT rewritten — match-by-key, not by full prop map).
func TestCypherMergeNodeCreateThenMatch(t *testing.T) {
	g := New()
	q := `MERGE (n:P {key:'m', v:1}) ON CREATE SET n.fresh = true ON MATCH SET n.seen = true RETURN n.key, n.v`
	res, err := g.Cypher(q)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 || res.Rows[0]["n.key"] != "m" || res.Rows[0]["n.v"] != 1.0 {
		t.Fatalf("rows = %+v want one row m/1", res.Rows)
	}
	if res.Summary == nil || res.Summary.CreatedNodes != 1 {
		t.Fatalf("summary = %+v want CreatedNodes=1 on the create arm", res.Summary)
	}
	n, ok := g.NodeByKey("m")
	if !ok {
		t.Fatal("merged node missing through the read path")
	}
	if n.Label != "P" {
		t.Errorf("label = %q want P", n.Label)
	}
	if _, has := n.Props["key"]; has {
		t.Errorf("stored props must not contain the key: %v", n.Props)
	}
	if n.Props["v"] != 1.0 || n.Props["fresh"] != true {
		t.Fatalf("props = %v want v=1 and fresh=true (ON CREATE fired)", n.Props)
	}
	if _, has := n.Props["seen"]; has {
		t.Fatalf("ON MATCH fired on a create: %v", n.Props)
	}

	res, err = g.Cypher(q)
	if err != nil {
		t.Fatal(err)
	}
	if res.Summary == nil || res.Summary.CreatedNodes != 0 {
		t.Fatalf("summary = %+v want CreatedNodes=0 on the match arm", res.Summary)
	}
	if res.Summary.PropertiesSet != 1 {
		t.Errorf("PropertiesSet = %d want 1 (the ON MATCH item)", res.Summary.PropertiesSet)
	}
	n, _ = g.NodeByKey("m")
	if n.Props["seen"] != true {
		t.Fatalf("ON MATCH did not fire on the match arm: %v", n.Props)
	}
	if n.Props["v"] != 1.0 || n.Props["fresh"] != true {
		t.Fatalf("match arm rewrote creation-time props: %v", n.Props)
	}
	if got := g.NodeCount(); got != 1 {
		t.Fatalf("node count = %d want 1 (the second merge created nothing)", got)
	}
}

// TestCypherMergeNodeMatchByKeyOnly pins the documented divergence: node
// MERGE matches by key alone — an existing node whose other props differ
// from the pattern's is matched as-is, its props untouched (creation-time
// only; ON MATCH SET is the update path), and no node is created.
func TestCypherMergeNodeMatchByKeyOnly(t *testing.T) {
	g := New()
	if _, err := g.Cypher(`CREATE (n:P {key:'x', v:'orig'})`); err != nil {
		t.Fatal(err)
	}
	res, err := g.Cypher(`MERGE (n:P {key:'x', v:'other', extra:9})`)
	if err != nil {
		t.Fatal(err)
	}
	if res.Summary == nil || res.Summary.CreatedNodes != 0 {
		t.Fatalf("summary = %+v want CreatedNodes=0 (matched by key)", res.Summary)
	}
	if got := g.NodeCount(); got != 1 {
		t.Fatalf("node count = %d want 1", got)
	}
	n, _ := g.NodeByKey("x")
	if n.Props["v"] != "orig" {
		t.Errorf("v = %v want orig (a match must not rewrite props)", n.Props["v"])
	}
	if _, has := n.Props["extra"]; has {
		t.Errorf("a pattern-only prop leaked into the matched node: %v", n.Props)
	}
	if n.Label != "P" {
		t.Errorf("label = %q want P", n.Label)
	}
}

// TestCypherMergeEdgeFoldOrCreate pins the edge MERGE arms. Miss: the
// (srcKey,dstKey,label) triple check fails, endpoints resolve per the
// createStage rules (an existing key binds — only 'b' is created — and the
// edge is written with UpsertEdgeTriple fold semantics, CreatedEdges=1, ON
// CREATE SET firing on the edge var). Hit: the same merge again matches the
// triple — no duplicate edge, no endpoint churn, the pattern's {w:2} is
// creation-time only (w stays 1), ON MATCH SET the update path.
func TestCypherMergeEdgeFoldOrCreate(t *testing.T) {
	g := New()
	// endpoint 'a' exists; 'b' does not: the miss arm must create only b.
	if _, err := g.Cypher(`CREATE (a:P {key:'a', tag:1})`); err != nil {
		t.Fatal(err)
	}
	res, err := g.Cypher(`MERGE (a:P {key:'a'})-[r:CALLS {w:1}]->(b:P {key:'b'}) ON CREATE SET r.fresh = true RETURN r.w`)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 || res.Rows[0]["r.w"] != 1.0 {
		t.Fatalf("rows = %+v want r.w=1", res.Rows)
	}
	if res.Summary == nil || res.Summary.CreatedNodes != 1 || res.Summary.CreatedEdges != 1 {
		t.Fatalf("summary = %+v want CreatedNodes=1 (b only), CreatedEdges=1", res.Summary)
	}
	es := liveTriple(t, g, "a", "b", "CALLS")
	if len(es) != 1 {
		t.Fatalf("parallels = %d want 1", len(es))
	}
	if es[0].Props["w"] != 1.0 || es[0].Props["fresh"] != true {
		t.Fatalf("edge props = %v want w=1 and fresh=true (ON CREATE fired)", es[0].Props)
	}
	if got := g.NodeCount(); got != 2 {
		t.Fatalf("node count = %d want 2 (existing 'a' bound, not recreated)", got)
	}
	if a, _ := g.NodeByKey("a"); a.Props["tag"] != 1.0 {
		t.Fatalf("existing endpoint 'a' was rewritten: %v", a.Props)
	}

	res, err = g.Cypher(`MERGE (a:P {key:'a'})-[r:CALLS {w:2}]->(b:P {key:'b'}) ON MATCH SET r.hits = 1 RETURN r.w`)
	if err != nil {
		t.Fatal(err)
	}
	if res.Summary == nil || res.Summary.CreatedNodes != 0 || res.Summary.CreatedEdges != 0 {
		t.Fatalf("summary = %+v want all create counters 0 on the match arm", res.Summary)
	}
	if len(res.Rows) != 1 || res.Rows[0]["r.w"] != 1.0 {
		t.Fatalf("rows = %+v want r.w=1 (pattern props are creation-time only)", res.Rows)
	}
	es = liveTriple(t, g, "a", "b", "CALLS")
	if len(es) != 1 {
		t.Fatalf("parallels = %d want 1 (no duplicate edge on the match arm)", len(es))
	}
	if es[0].Props["w"] != 1.0 || es[0].Props["hits"] != 1.0 {
		t.Fatalf("edge props = %v want w=1 (untouched) and hits=1 (ON MATCH fired)", es[0].Props)
	}
	if got := g.NodeCount(); got != 2 {
		t.Fatalf("node count = %d want 2 (the match arm created no endpoints)", got)
	}
}

// TestCypherMergeEdgeExistingSkipsEndpointCreation pins the Neo4j
// full-pattern-match rule: when the triple already exists, endpoint
// creation is suppressed even though the pattern spells its endpoints as
// fresh labeled propertied node patterns — their keys name the existing
// endpoints, so no node is created, the would-be endpoint props land
// nowhere, and the fresh vars bind the existing entities.
func TestCypherMergeEdgeExistingSkipsEndpointCreation(t *testing.T) {
	g := New()
	if _, err := g.Cypher(`CREATE (a:P {key:'a'})-[:CALLS {w:1}]->(b:P {key:'b'})`); err != nil {
		t.Fatal(err)
	}
	before := g.NodeCount()
	res, err := g.Cypher(`MERGE (a2:P {key:'a', fresh:1})-[r:CALLS]->(b2:P {key:'b', fresh:1}) RETURN a2.key, b2.key`)
	if err != nil {
		t.Fatal(err)
	}
	if res.Summary == nil || res.Summary.CreatedNodes != 0 || res.Summary.CreatedEdges != 0 {
		t.Fatalf("summary = %+v want all create counters 0 (the whole pattern matched)", res.Summary)
	}
	if got := g.NodeCount(); got != before {
		t.Fatalf("node count = %d want %d (an existing edge suppresses endpoint creation)", got, before)
	}
	if len(res.Rows) != 1 || res.Rows[0]["a2.key"] != "a" || res.Rows[0]["b2.key"] != "b" {
		t.Fatalf("rows = %+v want a2/b2 bound to the existing endpoints", res.Rows)
	}
	for _, k := range []string{"a", "b"} {
		n, _ := g.NodeByKey(k)
		if _, has := n.Props["fresh"]; has {
			t.Errorf("endpoint %q gained a pattern-only prop: %v", k, n.Props)
		}
	}
	if es := liveTriple(t, g, "a", "b", "CALLS"); len(es) != 1 || es[0].Props["w"] != 1.0 {
		t.Fatalf("edge = %+v want the untouched existing one", es)
	}
}

// TestCypherMergeDeterminismReplicas pins Review Focus 1's statement-level
// determinism on the MERGE path (full-statement version): two graphs with
// identical starting state replayed an identical MERGE-heavy statement
// sequence produce identical summaries and identical end state — same props
// on every key, same single edge with the same props (the key ratchet never
// enters: every merge carries an explicit key or rides existing keys, and
// no map-iteration order drives an observable write).
func TestCypherMergeDeterminismReplicas(t *testing.T) {
	run := func() (sums [][4]int, props map[string]map[string]any, edge string) {
		g := New()
		stmts := []string{
			`MERGE (u:User {key:'alice', name:'Alice'}) ON CREATE SET u.hits = 1`,
			`MERGE (u:User {key:'bob'}) ON CREATE SET u.hits = 1`,
			`MERGE (a:User {key:'alice'})-[r:KNOWS {since:2020}]->(b:User {key:'bob'}) ON CREATE SET r.w = 7`,
			`UNWIND ['x', 'y', 'x'] AS k MERGE (n:P {key:k, v:1}) ON CREATE SET n.f = true ON MATCH SET n.f = false`,
			`MERGE (a:User {key:'alice'}) ON MATCH SET a.hits = 2`,
		}
		for _, q := range stmts {
			res, err := g.Cypher(q)
			if err != nil {
				t.Fatalf("%q: %v", q, err)
			}
			s := res.Summary
			if s == nil {
				t.Fatalf("%q returned no summary", q)
			}
			sums = append(sums, [4]int{s.CreatedNodes, s.CreatedEdges, s.PropertiesSet, s.DeletedNodes})
		}
		props = make(map[string]map[string]any)
		for _, k := range []string{"alice", "bob", "x", "y"} {
			n, ok := g.NodeByKey(k)
			if !ok {
				t.Fatalf("node %q missing", k)
			}
			props[k] = n.Props
		}
		es := liveTriple(t, g, "alice", "bob", "KNOWS")
		if len(es) != 1 {
			t.Fatalf("KNOWS parallels = %d want exactly 1", len(es))
		}
		e := es[0]
		edge = fmt.Sprintf("since=%v w=%v keys=%d", e.Props["since"], e.Props["w"], len(e.Props))
		return sums, props, edge
	}
	s1, p1, e1 := run()
	s2, p2, e2 := run()
	for i := range s1 {
		if s1[i] != s2[i] {
			t.Fatalf("statement %d summary drift: %v vs %v", i, s1[i], s2[i])
		}
	}
	for k, want := range p1 {
		if !maps.Equal(want, p2[k]) {
			t.Fatalf("node %q prop drift: %v vs %v", k, want, p2[k])
		}
	}
	if e1 != e2 {
		t.Fatalf("edge state drift: %q vs %q", e1, e2)
	}
	// the expected end state, both replicas: ON CREATE/ON MATCH outcomes
	// settled per arm (x merged twice: create then match — f ends false).
	if p1["alice"]["hits"] != 2.0 || p1["alice"]["name"] != "Alice" {
		t.Fatalf("alice props = %v want hits=2 (final ON MATCH), name=Alice", p1["alice"])
	}
	if p1["bob"]["hits"] != 1.0 {
		t.Fatalf("bob props = %v want hits=1", p1["bob"])
	}
	if p1["x"]["f"] != false || p1["y"]["f"] != true || p1["x"]["v"] != 1.0 {
		t.Fatalf("unwind-merge props = x:%v y:%v want x.f=false (matched), y.f=true (created)", p1["x"], p1["y"])
	}
	if e1 != "since=2020 w=7 keys=2" {
		t.Fatalf("edge = %q want since=2020 w=7 keys=2", e1)
	}
}

// TestCypherMergeResurrectOnCreateNoPanic pins the resurrection path's
// writtenNodes record (review F1): a MERGE that recreates a bound var
// deleted earlier in the SAME statement must record a NON-NIL props map —
// overlayNodeProp assigns straight into an existing record's map, so a nil
// one panicked with "assignment to entry in nil map" the moment an ON
// CREATE SET item touched the recreated node.
func TestCypherMergeResurrectOnCreateNoPanic(t *testing.T) {
	g := New()
	if _, err := g.Cypher(`CREATE (a:P {key:'x', v:1})`); err != nil {
		t.Fatal(err)
	}
	res, err := g.Cypher(`MATCH (a {key:'x'}) DETACH DELETE a MERGE (a) ON CREATE SET a.z = 1 RETURN a.key, a.z`)
	if err != nil {
		t.Fatal(err)
	}
	if res.Summary == nil || res.Summary.DeletedNodes != 1 || res.Summary.CreatedNodes != 1 {
		t.Fatalf("summary = %+v want DeletedNodes=1, CreatedNodes=1 (delete then resurrect)", res.Summary)
	}
	if res.Summary.PropertiesSet != 1 {
		t.Errorf("PropertiesSet = %d want 1 (the ON CREATE item)", res.Summary.PropertiesSet)
	}
	if len(res.Rows) != 1 || res.Rows[0]["a.key"] != "x" || res.Rows[0]["a.z"] != 1.0 {
		t.Fatalf("rows = %+v want a.key=x, a.z=1", res.Rows)
	}
	n, ok := g.NodeByKey("x")
	if !ok {
		t.Fatal("resurrected node missing through the read path")
	}
	if n.Props["z"] != 1.0 {
		t.Fatalf("props = %v want z=1 (ON CREATE fired on the resurrection)", n.Props)
	}
	if _, has := n.Props["v"]; has {
		t.Fatalf("props = %v want v gone (the delete killed the original props; AddNode revive does not resurrect them)", n.Props)
	}
}
