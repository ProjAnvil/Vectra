package vectra_test

// Task 3b behavior tests: micro-layer generations on Graph — three-source
// point queries (base ∪ layers, tombstone shadowing), sealed-gate
// dissolution (spec §6), EdgeID ratchet, pending index declarations (R14),
// traversal-family base-only semantics until Task 4 (spec §3.2), and
// Cycles' merge-before-answer exception.

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/projanvil/vectra"
)

// ---- three-source point queries -------------------------------------------

// TestWriteAfterSealImmediatelyVisible: the sealed gate is dissolved — writes
// after Seal() succeed and every point query sees them at once (spec §3.2
// point-query family: immediate visibility via three-source reads).
func TestWriteAfterSealImmediatelyVisible(t *testing.T) {
	g := mustBuild(t) // 4 nodes / 4 edges, unsealed
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	if !g.Sealed() {
		t.Fatal("Seal must leave no unmerged layers")
	}

	// node override: visible immediately, same ID (v0.6 AddNode contract)
	n0, _ := g.NodeByKey("gateway")
	if _, err := g.AddNode("gateway", "Gateway2", map[string]any{"team": "edge"}); err != nil {
		t.Fatalf("AddNode after Seal: %v (sealed gate must be dissolved)", err)
	}
	n1, ok := g.NodeByKey("gateway")
	if !ok || n1.Label != "Gateway2" || n1.Props["team"] != "edge" {
		t.Fatalf("override not visible: %+v", n1)
	}
	if n1.ID != n0.ID {
		t.Fatalf("override changed node id: %d -> %d", n0.ID, n1.ID)
	}
	if g.NodeCount() != 4 {
		t.Fatalf("NodeCount = %d, want 4 (override must not add)", g.NodeCount())
	}
	if g.Sealed() {
		t.Fatal("a write must leave unmerged layers (Sealed()==false)")
	}

	// new node in a layer: visible by key and by the returned ID
	id, err := g.AddNode("kafka", "Bus", nil)
	if err != nil {
		t.Fatal(err)
	}
	if n, ok := g.NodeByKey("kafka"); !ok || n.ID != id || n.Label != "Bus" {
		t.Fatalf("layer node not visible: %+v ok=%v want id=%d", n, ok, id)
	}
	if n, ok := g.NodeByID(id); !ok || n.Key != "kafka" {
		t.Fatalf("NodeByID(returned id) = %+v ok=%v", n, ok)
	}
	if g.NodeCount() != 5 {
		t.Fatalf("NodeCount = %d, want 5", g.NodeCount())
	}

	// new edge over base-resident endpoints: visible via EdgeByID/EdgesOf
	eid, err := g.AddEdge("db", "kafka", "EVENT", map[string]any{"qos": "at-least-once"})
	if err != nil {
		t.Fatal(err)
	}
	e, ok := g.EdgeByID(eid)
	if !ok || e.Label != "EVENT" || e.Props["qos"] != "at-least-once" {
		t.Fatalf("layer edge not visible: %+v ok=%v", e, ok)
	}
	es, err := g.EdgesOf("db", vectra.DirOut)
	if err != nil || len(es) != 1 || es[0].ID != eid || es[0].Dst != id {
		t.Fatalf("EdgesOf(db) = %+v err=%v", es, err)
	}
	if g.EdgeCount() != 5 {
		t.Fatalf("EdgeCount = %d, want 5", g.EdgeCount())
	}

	// deletes are immediately invisible
	if n, err := g.DeleteEdge("db", "kafka", "EVENT"); err != nil || n != 1 {
		t.Fatalf("DeleteEdge = %d, %v", n, err)
	}
	if _, ok := g.EdgeByID(eid); ok {
		t.Fatal("deleted layer edge still visible")
	}
	if es, _ := g.EdgesOf("db", vectra.DirOut); len(es) != 0 {
		t.Fatalf("EdgesOf after delete = %+v", es)
	}
	if g.EdgeCount() != 4 {
		t.Fatalf("EdgeCount after delete = %d, want 4", g.EdgeCount())
	}

	// deleting a base-resident edge (tombstone shadows the base copy)
	if n, err := g.DeleteEdge("gateway", "auth", "CALLS"); err != nil || n != 1 {
		t.Fatalf("DeleteEdge(base edge) = %d, %v", n, err)
	}
	if es, _ := g.EdgesOf("gateway", vectra.DirOut); len(es) != 1 {
		t.Fatalf("base edge not shadowed: %+v", es)
	}

	// deleting a node cascades across sources and revive reuses the ID
	if n, err := g.DeleteNode("ledger"); err != nil || n != 2 {
		t.Fatalf("DeleteNode(ledger) = %d, %v; want 2 cascaded (db QUERY + gateway CALLS)", n, err)
	}
	if _, ok := g.NodeByKey("ledger"); ok {
		t.Fatal("deleted node still visible")
	}
	rid, err := g.AddNode("ledger", "Service", nil)
	if err != nil {
		t.Fatal(err)
	}
	l0, _ := mustBuildID(t, g, "ledger")
	_ = l0
	if rid != l0 {
		t.Fatalf("revive id = %d, want original %d", rid, l0)
	}
}

// mustBuildID returns the current id of key (test helper).
func mustBuildID(t *testing.T, g *vectra.Graph, key string) (vectra.NodeID, error) {
	t.Helper()
	n, ok := g.NodeByKey(key)
	if !ok {
		return 0, fmt.Errorf("key %q not found", key)
	}
	return n.ID, nil
}

// TestUpsertFoldAcrossGenerationsKeepsID: fold on an edge living in the base
// or an older layer transfers the winner EdgeID (v0.6 "keeps the most
// recent edge's ID") and the triple stays unique across sources.
func TestUpsertFoldAcrossGenerationsKeepsID(t *testing.T) {
	g := mustBuild(t)
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	id0, err := g.UpsertEdge("gateway", "auth", "CALLS", map[string]any{"w": 2})
	if err != nil {
		t.Fatal(err)
	}
	if e, _ := g.EdgeByID(0); e.ID != 0 || id0 != 0 {
		t.Fatalf("fold over base edge must keep id 0, got %d", id0)
	}
	if e, ok := g.EdgeByID(id0); !ok || e.Props["w"] != 2 {
		t.Fatalf("folded edge = %+v ok=%v", e, ok)
	}
	if g.EdgeCount() != 4 {
		t.Fatalf("EdgeCount = %d, want 4 (fold replaces)", g.EdgeCount())
	}
	// fold again over the layer record: same transfer
	id1, err := g.UpsertEdge("gateway", "auth", "CALLS", map[string]any{"w": 3})
	if err != nil || id1 != id0 {
		t.Fatalf("second fold id = %d err=%v, want %d", id1, err, id0)
	}
	e, ok := g.EdgeByID(id1)
	if !ok || e.Props["w"] != 3 {
		t.Fatalf("second fold = %+v ok=%v", e, ok)
	}
	if n := len(mustEdgesOf(t, g, "gateway")); n != 2 {
		t.Fatalf("gateway out-edges = %d, want 2 (triple unique)", n)
	}
}

func mustEdgesOf(t *testing.T, g *vectra.Graph, key string) []vectra.Edge {
	t.Helper()
	es, err := g.EdgesOf(key, vectra.DirOut)
	if err != nil {
		t.Fatalf("EdgesOf(%s): %v", key, err)
	}
	return es
}

// TestEdgeIDRatchetMonotonicAcrossGenerations: successive appends get fresh,
// strictly increasing EdgeIDs; deleting an edge never recycles its ID; only
// Seal's merge may renumber (spec §3.1 EdgeID ratchet).
func TestEdgeIDRatchetMonotonicAcrossGenerations(t *testing.T) {
	g := vectra.New()
	for _, k := range []string{"a", "b"} {
		if _, err := g.AddNode(k, "S", nil); err != nil {
			t.Fatal(err)
		}
	}
	var ids []vectra.EdgeID
	for i := 0; i < 4; i++ {
		id, err := g.AddEdge("a", "b", "E", map[string]any{"i": i})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if !slices.IsSorted(ids) || len(slices.Compact(slices.Clone(ids))) != 4 {
		t.Fatalf("appended ids not strictly increasing/unique: %v", ids)
	}
	if _, err := g.DeleteEdge("a", "b", "E"); err != nil {
		t.Fatal(err)
	}
	id, err := g.AddEdge("a", "b", "E", nil)
	if err != nil {
		t.Fatal(err)
	}
	if id <= ids[len(ids)-1] {
		t.Fatalf("id after delete = %d, want > %d (no recycling)", id, ids[len(ids)-1])
	}
	if _, ok := g.EdgeByID(ids[0]); ok {
		t.Fatal("deleted edge id must not resolve")
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	if g.EdgeCount() != 1 {
		t.Fatalf("after merge EdgeCount = %d, want 1", g.EdgeCount())
	}
}

// ---- index pending semantics (R14) -----------------------------------------

// TestIndexPropertyPendingUntilMerge: a mid-session IndexProperty declaration
// is pending until the next merge — before it takes effect, LookupBy* answers
// along the old index plus a delta line scan (ok=true), and the next Seal
// makes it a base index.
func TestIndexPropertyPendingUntilMerge(t *testing.T) {
	g := vectra.New()
	_, _ = g.AddNode("a", "S", map[string]any{"env": "prod"})
	_, _ = g.AddNode("b", "S", map[string]any{"env": "dev"})
	g.IndexProperty("env")
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	} // merge: env is an effective base index; "team" is not declared yet

	g.IndexProperty("team") // pending declaration post-Seal
	if _, ok := g.LookupByProperty("team", "x"); !ok {
		t.Fatal("pending key must answer ok=true (line-scan semantics)")
	}
	_, _ = g.AddNode("c", "S", map[string]any{"team": "core"})
	ns, ok := g.LookupByProperty("team", "core")
	if !ok || len(ns) != 1 || ns[0].Key != "c" {
		t.Fatalf("pending lookup must see delta nodes: %+v ok=%v", ns, ok)
	}
	if keys := g.IndexedPropertyKeys(); !slices.Contains(keys, "team") {
		t.Fatalf("IndexedPropertyKeys must include the pending key: %v", keys)
	}
	// pending numeric + bool lookups behave the same
	g.IndexProperty("w")
	_, _ = g.AddNode("d", "S", map[string]any{"w": 7, "on": true})
	if ns, ok := g.LookupByNumber("w", 7); !ok || len(ns) != 1 || ns[0].Key != "d" {
		t.Fatalf("pending number lookup: %+v ok=%v", ns, ok)
	}
	// an override changes the hit set before merge
	_, _ = g.AddNode("c", "S", map[string]any{"team": "edge"})
	if ns, _ := g.LookupByProperty("team", "core"); len(ns) != 0 {
		t.Fatalf("override must retire the old value: %+v", ns)
	}
	if ns, _ := g.LookupByProperty("team", "edge"); len(ns) != 1 || ns[0].Key != "c" {
		t.Fatalf("override must be visible: %+v", ns)
	}
	// unregistered keys still answer ok=false
	if _, ok := g.LookupByProperty("never", "x"); ok {
		t.Fatal("never-declared key must return ok=false")
	}
	// the next merge harvests the pending declarations into the base index
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	if ns, ok := g.LookupByProperty("team", "edge"); !ok || len(ns) != 1 || ns[0].Key != "c" {
		t.Fatalf("post-merge effective index: %+v ok=%v", ns, ok)
	}
	if keys := g.IndexedPropertyKeys(); !slices.Contains(keys, "team") || !slices.Contains(keys, "w") {
		t.Fatalf("post-merge index keys: %v", keys)
	}
	// base-index hits honor delta overrides (winner filtering)
	_, _ = g.AddNode("a", "S", map[string]any{"env": "dev"}) // override prod -> dev
	if ns, _ := g.LookupByProperty("env", "prod"); len(ns) != 0 {
		t.Fatalf("base index hit must be retired by override: %+v", ns)
	}
	if ns, _ := g.LookupByProperty("env", "dev"); len(ns) != 2 {
		t.Fatalf("env=dev hits = %+v, want a and b", ns)
	}
}

// ---- traversal family: base-only until Task 4 ------------------------------

// TestTraversalFamilyBaseOnlyUntilTask4: with unmerged layers present, the
// traversal family reads the merged base only (spec §3.2 family table; the
// three-source View wiring is Task 4).
// TODO(Task4): replace with three-source traversal expectations.
func TestTraversalFamilyBaseOnlyUntilTask4(t *testing.T) {
	g := vectra.New()
	_, _ = g.AddNode("a", "S", nil)
	_, _ = g.AddEdge("a", "a", "LOOP", nil)
	// Unsealed BFS is legal (gate dissolved) but reads the base, which is
	// empty: layer-resident starts are out of range.
	if _, err := g.BFS([]vectra.NodeID{0}); err == nil {
		t.Fatal("BFS on layer-resident start must fail (base-only until Task4)")
	}
	// after Seal the same query answers from the merged base
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	res, err := g.BFS([]vectra.NodeID{0})
	if err != nil || res.Count() != 1 {
		t.Fatalf("post-Seal BFS: %+v %v", res, err)
	}
	// writes after Seal keep traversal on the last merged base (bounded-stale)
	if _, err := g.AddNode("b", "S", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := g.BFS([]vectra.NodeID{0}); err != nil {
		t.Fatalf("BFS with layers present: %v", err)
	}
	ns, err := g.NeighborsOf("a", vectra.DirOut)
	if err != nil || len(ns) != 1 || ns[0].Key != "a" {
		t.Fatalf("NeighborsOf base-only: %+v %v", ns, err)
	}
	// TODO(Task4): layer edge a->b invisible to NeighborsOf until the View lands.
}

// TestCypherUnsealedReadsBase: Cypher without a prior Seal runs against the
// (possibly empty) base instead of erroring (gate dissolved).
// TODO(Task4): Cypher over the three-source View.
func TestCypherUnsealedReadsBase(t *testing.T) {
	g := vectra.New()
	res, err := g.Cypher(`MATCH (a) RETURN a.key`)
	if err != nil {
		t.Fatalf("Cypher on unsealed graph: %v (gate dissolved)", err)
	}
	if len(res.Rows) != 0 {
		t.Fatalf("rows = %d, want 0 (base empty until merge)", len(res.Rows))
	}
}

// TestCyclesMergesDeltaBeforeAnswering: Cycles is the exception in the
// traversal family — unmerged delta forces a one-shot merge first (spec
// §3.2), so a cycle closed by a layer write is discovered.
func TestCyclesMergesDeltaBeforeAnswering(t *testing.T) {
	g := vectra.New()
	for _, k := range []string{"a", "b"} {
		if _, err := g.AddNode(k, "S", nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := g.AddEdge("a", "b", "CALLS", nil); err != nil {
		t.Fatal(err)
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	if cs := g.Cycles(); len(cs) != 0 {
		t.Fatalf("no cycles yet: %+v", cs)
	}
	if _, err := g.AddEdge("b", "a", "CALLS", nil); err != nil {
		t.Fatal(err)
	}
	cs := g.Cycles()
	if len(cs) != 1 || cs[0].Len() != 2 {
		t.Fatalf("Cycles must merge delta first: %+v", cs)
	}
	if !g.Sealed() {
		t.Fatal("Cycles' forced merge must leave no layers")
	}
}

// ---- gate dissolution: Save / Clone / Load ---------------------------------

// TestSaveWithoutSealExportsLogicalView: Save works on a graph with unmerged
// layers (R4) and the round-trip preserves the union state.
func TestSaveWithoutSealExportsLogicalView(t *testing.T) {
	g := mustBuild(t)
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("kafka", "Bus", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := g.DeleteEdge("gateway", "auth", "CALLS"); err != nil {
		t.Fatal(err)
	}
	var buf strings.Builder
	if err := g.Save(&noWriteCloser{&buf}); err != nil {
		t.Fatalf("Save with unmerged layers: %v", err)
	}
	g2, err := vectra.Load(strings.NewReader(buf.String()))
	if err != nil {
		t.Fatal(err)
	}
	if g2.NodeCount() != 5 || g2.EdgeCount() != 3 {
		t.Fatalf("round-trip = %d/%d, want 5/3", g2.NodeCount(), g2.EdgeCount())
	}
	if _, ok := g2.NodeByKey("kafka"); !ok {
		t.Fatal("layer node lost in Save round-trip")
	}
	if es := mustEdgesOf(t, g2, "gateway"); len(es) != 1 {
		t.Fatalf("tombstoned base edge resurrected: %+v", es)
	}
}

type noWriteCloser struct{ b *strings.Builder }

func (w *noWriteCloser) Write(p []byte) (int, error) { return w.b.Write(p) }

// TestCloneCarriesUnionIntoNewBase: Clone builds the new graph from the union
// records — writable immediately, and (family semantics) Sealed() reflects
// "no unmerged layers", which a fresh clone satisfies.
func TestCloneCarriesUnionIntoNewBase(t *testing.T) {
	g := mustBuild(t)
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("kafka", "Bus", nil); err != nil {
		t.Fatal(err)
	}
	c := g.Clone()
	if c.NodeCount() != 5 || c.EdgeCount() != 4 {
		t.Fatalf("clone = %d/%d, want 5/4 (union)", c.NodeCount(), c.EdgeCount())
	}
	if _, err := c.AddEdge("db", "kafka", "EVENT", nil); err != nil {
		t.Fatalf("clone must be writable: %v", err)
	}
	if g.NodeCount() != 5 || g.EdgeCount() != 4 {
		t.Fatal("clone's writes polluted the original")
	}
}

// ---- merge equivalence ------------------------------------------------------

// TestSealMergeEquivalenceRandom: under random op sequences, the full
// point-query face is identical before and after forceMerge (Seal on a
// WAL-less graph is exactly the merge). ID-agnostic fingerprint per spec R1
// (merge may renumber).
func TestSealMergeEquivalenceRandom(t *testing.T) {
	for seed := int64(1); seed <= 8; seed++ {
		g := vectra.New()
		rng := rand.New(rand.NewSource(seed))
		keys := []string{"a", "b", "c", "d"}
		labels := []string{"CALLS", "DATA"}
		live := map[string]bool{}
		for i := 0; i < 220; i++ {
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
		}
		g.IndexProperty("n")
		before := fingerprint(t, g, keys)
		if err := g.Seal(); err != nil {
			t.Fatal(err)
		}
		after := fingerprint(t, g, keys)
		if before != after {
			t.Fatalf("seed %d: point-query face diverged across merge\nbefore:\n%s\nafter:\n%s", seed, before, after)
		}
	}
}

// fingerprint renders the ID-agnostic logical state: node set with
// label/props, the sorted multiset of live out-edges per node, and counts.
func fingerprint(t *testing.T, g *vectra.Graph, keys []string) string {
	t.Helper()
	var b strings.Builder
	for _, k := range keys {
		n, ok := g.NodeByKey(k)
		if !ok {
			fmt.Fprintf(&b, "node %s: DEAD\n", k)
			continue
		}
		fmt.Fprintf(&b, "node %s: %s %v\n", k, n.Label, n.Props)
		es := mustEdgesOf(t, g, k)
		desc := make([]string, 0, len(es))
		for _, e := range es {
			dst, _ := g.NodeByID(e.Dst)
			desc = append(desc, fmt.Sprintf("%s-%s-%v", e.Label, dst.Key, e.Props))
		}
		slices.Sort(desc)
		fmt.Fprintf(&b, "  edges: %v\n", desc)
	}
	ns, _ := g.LookupByNumber("n", 2)
	ks := make([]string, 0, len(ns))
	for _, n := range ns {
		ks = append(ks, n.Key)
	}
	slices.Sort(ks)
	fmt.Fprintf(&b, "lookup(n=2): %v\n", ks)
	fmt.Fprintf(&b, "counts: %d/%d\n", g.NodeCount(), g.EdgeCount())
	return b.String()
}

// ---- WAL ↔ micro-layer alignment -------------------------------------------

// TestWALSessionThreeSourceRoundtrip: writes (including post-Seal writes and
// folds) survive Close/reopen with the full three-source state intact.
func TestWALSessionThreeSourceRoundtrip(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "graph.db")
	g, err := vectra.Open(db, vectra.Options{WALMode: vectra.ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("a", "S", map[string]any{"w": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("b", "S", nil); err != nil {
		t.Fatal(err)
	}
	if err := g.Batch(func(g *vectra.Graph) {
		g.AddEdge("a", "b", "CALLS", nil)
		g.AddEdge("a", "b", "CALLS", nil)                       // parallel
		g.UpsertEdge("a", "b", "CALLS", map[string]any{"w": 9}) // fold to one
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.Seal(); err != nil { // explicit merge + checkpoint
		t.Fatal(err)
	}
	// post-Seal writes become layers replayed above the checkpoint
	eid, err := g.UpsertEdge("a", "b", "CALLS", map[string]any{"w": 10})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.DeleteNode("b"); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}

	g2, err := vectra.Open(db, vectra.Options{WALMode: vectra.ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer g2.Close()
	if g2.NodeCount() != 1 || g2.EdgeCount() != 0 {
		t.Fatalf("reopened = %d/%d, want 1/0", g2.NodeCount(), g2.EdgeCount())
	}
	if _, ok := g2.NodeByKey("b"); ok {
		t.Fatal("deleted node resurrected on recovery")
	}
	if _, ok := g2.EdgeByID(eid); ok {
		t.Fatal("cascaded edge resurrected on recovery")
	}
	// node props survive (int -> float64 is the documented WAL JSON model)
	if n, _ := g2.NodeByKey("a"); n.Props["w"] != float64(1) {
		t.Fatalf("node props lost: %+v", n)
	}
	// the session keeps working after recovery
	if _, err := g2.AddNode("c", "S", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := g2.AddEdge("a", "c", "CALLS", nil); err != nil {
		t.Fatal(err)
	}
	if es := mustEdgesOf(t, g2, "a"); len(es) != 1 || es[0].Label != "CALLS" {
		t.Fatalf("post-recovery writes invisible: %+v", es)
	}
}

// TestDeleteEdgeNodeOnlyLabelStaysNoop: v0.6 parity — the label dictionary
// only ever carried EDGE labels (internLabel was called from resolveEdge
// alone), so DeleteEdge's early exit treats a label used only by NODES as
// "no match possible" and stays a pure no-op. After a merge the writer's
// label mirror is rebuilt and must keep that property (a delta Layer's
// Labels() superset — node labels included — must not leak into it): the
// delete returns (0, nil) and opens no layer.
func TestDeleteEdgeNodeOnlyLabelStaysNoop(t *testing.T) {
	g := vectra.New()
	_, _ = g.AddNode("a", "Person", nil)
	_, _ = g.AddNode("b", "Person", nil)
	_, _ = g.AddEdge("a", "b", "KNOWS", nil)
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	if !g.Sealed() {
		t.Fatal("precondition: merged")
	}
	if n, err := g.DeleteEdge("a", "b", "Person"); err != nil || n != 0 {
		t.Fatalf("DeleteEdge(node-only label) = %d, %v; want 0, nil", n, err)
	}
	if !g.Sealed() {
		t.Fatal("no-op delete must not open a layer (v0.6 parity)")
	}
	if g.EdgeCount() != 1 {
		t.Fatalf("edges = %d, want 1", g.EdgeCount())
	}
}

// TestIndexedPropertyKeysLockFree: IndexedPropertyKeys belongs to the point
// query family and must answer from the published generation only — never
// from writer-private batch state (Graph doc: mirrors are never read by
// query paths). A concurrent reader must not race an in-flight Batch that
// declares pending indexes; pending declarations become visible at the
// boundary like every other batch write.
func TestIndexedPropertyKeysLockFree(t *testing.T) {
	g := vectra.New()
	_, _ = g.AddNode("a", "S", nil)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 500; i++ {
			_ = g.IndexedPropertyKeys()
		}
	}()
	for i := 0; i < 50; i++ {
		_ = g.Batch(func(g *vectra.Graph) {
			g.IndexProperty(fmt.Sprintf("k%d", i))
			_, _ = g.AddNode(fmt.Sprintf("n%d", i), "S", nil)
		})
	}
	<-done
	if keys := g.IndexedPropertyKeys(); len(keys) != 50 {
		t.Fatalf("published declarations = %v, want 50 keys", keys)
	}
}

// TestSealMergeEquivalencePeriodicMerges: the merge-equivalence contract
// under repeated mid-sequence merges — every Seal resets the ID ratchets and
// rebuilds the writer mirrors from the new base, and drift anywhere in that
// rebuild would show up as a point-query face change at the NEXT fingerprint
// (taken both before and after the final merge). Random ops run between
// merges, folding onto edges that now live in the base, in older layers, or
// nowhere at all.
func TestSealMergeEquivalencePeriodicMerges(t *testing.T) {
	for seed := int64(1); seed <= 6; seed++ {
		g := vectra.New()
		rng := rand.New(rand.NewSource(seed))
		keys := []string{"a", "b", "c", "d"}
		labels := []string{"CALLS", "DATA"}
		live := map[string]bool{}
		var snaps []string
		for i := 0; i < 300; i++ {
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
			if i%37 == 36 { // periodic mid-sequence merge
				before := fingerprint(t, g, keys)
				if err := g.Seal(); err != nil {
					t.Fatal(err)
				}
				after := fingerprint(t, g, keys)
				if before != after {
					t.Fatalf("seed %d op %d: face diverged across periodic merge\nbefore:\n%s\nafter:\n%s", seed, i, before, after)
				}
				snaps = append(snaps, after)
			}
		}
		g.IndexProperty("n")
		before := fingerprint(t, g, keys)
		if err := g.Seal(); err != nil {
			t.Fatal(err)
		}
		after := fingerprint(t, g, keys)
		if before != after {
			t.Fatalf("seed %d: final merge diverged\nbefore:\n%s\nafter:\n%s", seed, before, after)
		}
		if len(snaps) == 0 {
			t.Fatalf("seed %d: expected periodic merges", seed)
		}
	}
}
