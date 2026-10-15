// Package delta_test pins internal/delta's Builder/Layer semantics against
// the write path of an unsealed *vectra.Graph used as the oracle (spec
// P3-19 golden technique). Op mapping under test:
//
//	UpsertNode       <-> vectra.AddNode
//	AddEdge          <-> vectra.AddEdge
//	UpsertEdgeFold   <-> vectra.UpsertEdge
//	DeleteEdge       <-> vectra.DeleteEdge
//	DeleteNode(nil)  <-> vectra.DeleteNode
//
// The test lives in the external test package delta_test (not package
// delta) so it can import vectra without an import cycle once T3b makes
// vectra itself depend on delta.
//
// Overlay-only behavior — base tombstones, deletes of base-resident keys,
// the baseEdges hook — has no single-graph oracle and is covered by unit
// assertions instead.
package delta_test

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"testing"

	"github.com/projanvil/vectra"
	"github.com/projanvil/vectra/internal/delta"
)

// --- golden harness -----------------------------------------------------------

type nodeView struct {
	key, label string
	props      map[string]any
}

func (v nodeView) String() string { return fmt.Sprintf("{%s %s %v}", v.key, v.label, v.props) }

type edgeView struct {
	src, dst, label string
	props           map[string]any
}

func (v edgeView) String() string {
	return fmt.Sprintf("{%s->%s %s %v}", v.src, v.dst, v.label, v.props)
}

// propsEqual treats nil and empty as equivalent — the documented nil/empty
// equivalence of the props contract.
func propsEqual(a, b map[string]any) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || !reflect.DeepEqual(v, w) {
			return false
		}
	}
	return true
}

// oracleNodes extracts the oracle's live nodes in slot order via the public
// API (NodeByID works on unsealed graphs; probing past the top is a miss).
func oracleNodes(g *vectra.Graph, maxID int) []nodeView {
	var out []nodeView
	for id := 0; id < maxID; id++ {
		n, ok := g.NodeByID(vectra.NodeID(id))
		if !ok {
			continue
		}
		out = append(out, nodeView{key: n.Key, label: n.Label, props: n.Props})
	}
	return out
}

// oracleEdges extracts the oracle's live edges in append order, endpoints
// mapped back to keys.
func oracleEdges(g *vectra.Graph, maxID int) []edgeView {
	var out []edgeView
	for id := 0; id < maxID; id++ {
		e, ok := g.EdgeByID(vectra.EdgeID(id))
		if !ok {
			continue
		}
		src, _ := g.NodeByID(e.Src)
		dst, _ := g.NodeByID(e.Dst)
		out = append(out, edgeView{src: src.Key, dst: dst.Key, label: e.Label, props: e.Props})
	}
	return out
}

func layerNodes(l *delta.Layer) []nodeView {
	out := make([]nodeView, 0, l.NodeCount())
	for _, n := range l.Nodes() {
		out = append(out, nodeView{key: n.Key, label: n.Label, props: n.Props})
	}
	return out
}

func layerEdges(l *delta.Layer) []edgeView {
	out := make([]edgeView, 0, l.EdgeCount())
	for _, e := range l.Edges() {
		out = append(out, edgeView{src: e.SrcKey, dst: e.DstKey, label: e.Label, props: e.Props})
	}
	return out
}

// assertSameView checks oracle vs layer field by field: live counts, ordered
// live node list, ordered live edge list, and a point lookup for every known
// key (presence + content).
func assertSameView(t *testing.T, g *vectra.Graph, l *delta.Layer, keys []string, maxNodeID, maxEdgeID int) {
	t.Helper()
	if g.NodeCount() != l.NodeCount() {
		t.Fatalf("node count: oracle %d, layer %d", g.NodeCount(), l.NodeCount())
	}
	if g.EdgeCount() != l.EdgeCount() {
		t.Fatalf("edge count: oracle %d, layer %d", g.EdgeCount(), l.EdgeCount())
	}
	on, ln := oracleNodes(g, maxNodeID), layerNodes(l)
	if len(on) != len(ln) {
		t.Fatalf("live nodes: oracle %d %v, layer %d %v", len(on), on, len(ln), ln)
	}
	for i := range on {
		if on[i].key != ln[i].key || on[i].label != ln[i].label || !propsEqual(on[i].props, ln[i].props) {
			t.Fatalf("node[%d]: oracle %s, layer %s", i, on[i], ln[i])
		}
	}
	oe, le := oracleEdges(g, maxEdgeID), layerEdges(l)
	if len(oe) != len(le) {
		t.Fatalf("live edges: oracle %d %v, layer %d %v", len(oe), oe, len(le), le)
	}
	for i := range oe {
		if oe[i].src != le[i].src || oe[i].dst != le[i].dst || oe[i].label != le[i].label || !propsEqual(oe[i].props, le[i].props) {
			t.Fatalf("edge[%d]: oracle %s, layer %s", i, oe[i], le[i])
		}
	}
	for _, k := range keys {
		gn, gok := g.NodeByKey(k)
		lr, lok := l.LookupNode(k)
		if gok != lok {
			t.Fatalf("point lookup %q: oracle ok=%v, layer ok=%v", k, gok, lok)
		}
		if gok && (gn.Label != lr.Label || !propsEqual(gn.Props, lr.Props)) {
			t.Fatalf("point lookup %q: oracle (%s %v), layer (%s %v)", k, gn.Label, gn.Props, lr.Label, lr.Props)
		}
	}
}

func sameErrness(t *testing.T, step string, oracle, layer error) {
	t.Helper()
	if (oracle != nil) != (layer != nil) {
		t.Fatalf("%s: oracle err=%v, layer err=%v", step, oracle, layer)
	}
}

// both* helpers apply one op to both sides and assert matching returns.

func bothNode(t *testing.T, g *vectra.Graph, b *delta.Builder, key, label string, props map[string]any) {
	t.Helper()
	if _, err := g.AddNode(key, label, props); err != nil {
		t.Fatalf("oracle AddNode(%s): %v", key, err)
	}
	if err := b.UpsertNode(key, label, props); err != nil {
		t.Fatalf("layer UpsertNode(%s): %v", key, err)
	}
}

func bothAddEdge(t *testing.T, g *vectra.Graph, b *delta.Builder, src, dst, label string, props map[string]any) {
	t.Helper()
	_, gerr := g.AddEdge(src, dst, label, props)
	lerr := b.AddEdge(src, dst, label, props)
	sameErrness(t, fmt.Sprintf("AddEdge(%s,%s,%s)", src, dst, label), gerr, lerr)
}

func bothFold(t *testing.T, g *vectra.Graph, b *delta.Builder, src, dst, label string, props map[string]any) {
	t.Helper()
	_, gerr := g.UpsertEdge(src, dst, label, props)
	lerr := b.UpsertEdgeFold(src, dst, label, props)
	sameErrness(t, fmt.Sprintf("UpsertEdgeFold(%s,%s,%s)", src, dst, label), gerr, lerr)
}

func bothDeleteEdge(t *testing.T, g *vectra.Graph, b *delta.Builder, k delta.EdgeKey) {
	t.Helper()
	gn, gerr := g.DeleteEdge(k.SrcKey, k.DstKey, k.Label)
	ln, lerr := b.DeleteEdge(k)
	sameErrness(t, fmt.Sprintf("DeleteEdge(%s,%s,%s)", k.SrcKey, k.DstKey, k.Label), gerr, lerr)
	if gn != ln {
		t.Fatalf("DeleteEdge(%s,%s,%s): oracle removed %d, layer removed %d", k.SrcKey, k.DstKey, k.Label, gn, ln)
	}
}

func bothDeleteNode(t *testing.T, g *vectra.Graph, b *delta.Builder, key string) {
	t.Helper()
	gn, gerr := g.DeleteNode(key)
	ln, lerr := b.DeleteNode(key, nil)
	sameErrness(t, "DeleteNode("+key+")", gerr, lerr)
	if gn != ln {
		t.Fatalf("DeleteNode(%s): oracle removed %d, layer removed %d", key, gn, ln)
	}
}

// --- golden tests -------------------------------------------------------------

func TestGoldenNodeUpsertPropsEquivalence(t *testing.T) {
	g, b := vectra.New(), delta.NewBuilder()

	bothNode(t, g, b, "a", "Person", map[string]any{"name": "x"})
	bothNode(t, g, b, "b", "Person", nil)              // nil props
	bothNode(t, g, b, "c", "Device", map[string]any{}) // empty map == nil
	// Overwrite of a live key: wholesale replacement — old keys are gone.
	bothNode(t, g, b, "a", "Service", map[string]any{"rank": 1})
	// nil replaces a non-nil map wholesale.
	bothNode(t, g, b, "b", "Person", map[string]any{"extra": true})

	l := b.Build()
	assertSameView(t, g, l, []string{"a", "b", "c", "ghost"}, 4, 2)

	// Layer-only metadata: labels deduped in first-seen order (nodes intern
	// too — a documented superset of vectra.go feeding the merge remap).
	if got := l.Labels(); !reflect.DeepEqual(got, []string{"Person", "Device", "Service"}) {
		t.Fatalf("Labels: got %v", got)
	}
	if len(l.IndexKeys()) != 0 || len(l.Edges()) != 0 || l.EdgeCount() != 0 {
		t.Fatalf("unexpected edges/index keys on a node-only layer")
	}
	if len(l.NodeTombstones()) != 0 || len(l.EdgeTombstones()) != 0 {
		t.Fatalf("unexpected tombstones on a write-only layer")
	}
}

func TestGoldenDeleteReviveCascade(t *testing.T) {
	g, b := vectra.New(), delta.NewBuilder()

	bothNode(t, g, b, "a", "Person", nil)
	bothNode(t, g, b, "b", "Person", nil)
	bothNode(t, g, b, "c", "Person", nil)
	bothAddEdge(t, g, b, "a", "b", "FOLLOW", map[string]any{"w": 1})
	bothAddEdge(t, g, b, "b", "c", "FOLLOW", map[string]any{"w": 2})
	bothAddEdge(t, g, b, "c", "a", "FOLLOW", nil)

	// Cascade: both incident edges die, the unrelated one survives.
	bothDeleteNode(t, g, b, "b")
	if g.NodeCount() != 2 || g.EdgeCount() != 1 {
		t.Fatalf("mid-sequence oracle counts: nodes=%d edges=%d", g.NodeCount(), g.EdgeCount())
	}

	// Revive: same key, new label/props, slot position kept on both sides.
	bothNode(t, g, b, "b", "Service", map[string]any{"fresh": true})
	bothAddEdge(t, g, b, "a", "b", "NEW", map[string]any{"z": 1})

	l := b.Build()
	assertSameView(t, g, l, []string{"a", "b", "c"}, 3, 8)
}

func TestGoldenEdgeFoldParallelIdempotentDelete(t *testing.T) {
	g, b := vectra.New(), delta.NewBuilder()

	bothNode(t, g, b, "a", "P", nil)
	bothNode(t, g, b, "b", "P", nil)
	k := delta.EdgeKey{SrcKey: "a", DstKey: "b", Label: "KNOWS"}

	bothAddEdge(t, g, b, "a", "b", "KNOWS", map[string]any{"w": 1})
	bothAddEdge(t, g, b, "a", "b", "KNOWS", map[string]any{"w": 2}) // parallel
	// Fold two parallels: most recent survives with the new props.
	bothFold(t, g, b, "a", "b", "KNOWS", map[string]any{"w": 3})
	if g.EdgeCount() != 1 {
		t.Fatalf("after fold: oracle edges=%d", g.EdgeCount())
	}
	// Fold then Add parallel: back to two live edges.
	bothAddEdge(t, g, b, "a", "b", "KNOWS", map[string]any{"w": 9})
	// Fold again over the pair.
	bothFold(t, g, b, "a", "b", "KNOWS", map[string]any{"w": 4})

	// Delete the live edge, then idempotent zero-delete, then a triple whose
	// label was never interned.
	bothDeleteEdge(t, g, b, k)
	bothDeleteEdge(t, g, b, k) // idempotent: 0 removed, no error
	bothDeleteEdge(t, g, b, delta.EdgeKey{SrcKey: "a", DstKey: "b", Label: "NEVER"})
	// Delete then re-add: edge live again (its base tombstone persists — the
	// layer record wins over the layer's own tombstone).
	bothAddEdge(t, g, b, "a", "b", "KNOWS", map[string]any{"w": 5})

	l := b.Build()
	assertSameView(t, g, l, []string{"a", "b"}, 2, 16)
	if !l.EdgeTombstones()[k] {
		t.Fatalf("folded/deleted key %v must carry a base edge tombstone", k)
	}
}

func TestGoldenEndpointErrorsSelfLoopCascade(t *testing.T) {
	g, b := vectra.New(), delta.NewBuilder()

	bothNode(t, g, b, "a", "P", nil)
	// Endpoint validation matches resolveEdge: missing endpoint errors on
	// both sides.
	bothAddEdge(t, g, b, "a", "missing", "K", nil)
	bothFold(t, g, b, "missing", "a", "K", nil)
	// Self-loop allowed, and it cascades exactly once on DeleteNode.
	bothAddEdge(t, g, b, "a", "a", "SELF", map[string]any{"s": 1})
	bothNode(t, g, b, "x", "P", nil)
	bothAddEdge(t, g, b, "x", "a", "K", nil)

	bothDeleteNode(t, g, b, "a") // cascades the self-loop and x->a: 2

	l := b.Build()
	assertSameView(t, g, l, []string{"a", "x", "missing"}, 3, 6)
}

// TestGoldenRandomSequences cross-checks the op mapping over randomized op
// sequences with deterministic seeds. Divergence guards (paths where the
// overlay deliberately differs from the single-graph oracle — missing-node
// DeleteNode, dead-endpoint DeleteEdge — see delta package doc) are avoided
// by construction: DeleteNode targets only live keys, DeleteEdge only
// triples with live endpoints. AddEdge/UpsertEdgeFold use any key because
// their endpoint errors match on both sides.
func TestGoldenRandomSequences(t *testing.T) {
	const nkeys = 6
	labels := []string{"A", "B", "C"}
	for seed := uint64(1); seed <= 10; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			rnd := rand.New(rand.NewPCG(seed, 0xdecafbad))
			g, b := vectra.New(), delta.NewBuilder()
			key := func(i int) string { return fmt.Sprintf("n%d", i) }
			live := make([]bool, nkeys)
			liveList := func() []int {
				var out []int
				for i := range live {
					if live[i] {
						out = append(out, i)
					}
				}
				return out
			}
			rprops := func() map[string]any {
				switch rnd.IntN(10) {
				case 0:
					return nil
				case 1:
					return map[string]any{}
				}
				m := make(map[string]any, 3)
				for n := rnd.IntN(3) + 1; n > 0; n-- {
					m[fmt.Sprintf("p%d", rnd.IntN(4))] = rnd.IntN(100)
				}
				return m
			}
			for op := 0; op < 150; op++ {
				i, j := rnd.IntN(nkeys), rnd.IntN(nkeys)
				label := labels[rnd.IntN(len(labels))]
				switch rnd.IntN(5) {
				case 0:
					bothNode(t, g, b, key(i), label, rprops())
					live[i] = true // (re)vive
				case 1:
					bothAddEdge(t, g, b, key(i), key(j), label, rprops())
				case 2:
					bothFold(t, g, b, key(i), key(j), label, rprops())
				case 3:
					ll := liveList()
					if len(ll) == 0 {
						bothNode(t, g, b, key(0), label, rprops())
						live[0] = true
						continue
					}
					ei, ej := ll[rnd.IntN(len(ll))], ll[rnd.IntN(len(ll))]
					bothDeleteEdge(t, g, b, delta.EdgeKey{SrcKey: key(ei), DstKey: key(ej), Label: label})
				case 4:
					ll := liveList()
					if len(ll) == 0 {
						bothNode(t, g, b, key(0), label, rprops())
						live[0] = true
						continue
					}
					di := ll[rnd.IntN(len(ll))]
					bothDeleteNode(t, g, b, key(di))
					live[di] = false
				}
			}
			all := make([]string, nkeys)
			for i := range all {
				all[i] = key(i)
			}
			l := b.Build()
			assertSameView(t, g, l, all, nkeys, 320)
		})
	}
}

// --- overlay-only unit assertions (no single-graph oracle) --------------------

func TestDeleteNodePureBaseTombstones(t *testing.T) {
	b := delta.NewBuilder()
	base := []delta.EdgeKey{
		{SrcKey: "ghost", DstKey: "x", Label: "R"},
		{SrcKey: "y", DstKey: "ghost", Label: "S"},
	}
	n, err := b.DeleteNode("ghost", func(k string) []delta.EdgeKey {
		if k != "ghost" {
			t.Fatalf("hook called with %q, want %q", k, "ghost")
		}
		return base
	})
	if err != nil || n != 2 {
		t.Fatalf("pure-base DeleteNode: n=%d err=%v, want 2 nil", n, err)
	}
	l := b.Build()
	if !l.NodeTombstones()["ghost"] {
		t.Fatalf("ghost must be node-tombstoned")
	}
	for _, k := range base {
		if !l.EdgeTombstones()[k] {
			t.Fatalf("base edge %v must be tombstoned", k)
		}
	}
	if _, ok := l.LookupNode("ghost"); ok {
		t.Fatalf("ghost must not be a live layer node")
	}
	if l.NodeCount() != 0 || l.EdgeCount() != 0 {
		t.Fatalf("pure-base delete must not create records: nodes=%d edges=%d", l.NodeCount(), l.EdgeCount())
	}
}

func TestDeleteNodeCascadeRecordsBaseTombstones(t *testing.T) {
	b := delta.NewBuilder()
	_ = b.UpsertNode("a", "P", nil)
	_ = b.UpsertNode("b", "P", nil)
	_ = b.AddEdge("a", "b", "R", map[string]any{"w": 1})

	n, err := b.DeleteNode("a", func(string) []delta.EdgeKey {
		return []delta.EdgeKey{{SrcKey: "a", DstKey: "z", Label: "R"}}
	})
	if err != nil || n != 2 { // 1 in-layer cascade + 1 hook-named base edge
		t.Fatalf("DeleteNode: n=%d err=%v, want 2 nil", n, err)
	}
	l := b.Build()
	if !l.NodeTombstones()["a"] {
		t.Fatalf("a must be node-tombstoned")
	}
	// The cascaded in-layer key is tombstoned too — base may hold a copy the
	// layer cannot see.
	if !l.EdgeTombstones()[delta.EdgeKey{SrcKey: "a", DstKey: "b", Label: "R"}] {
		t.Fatalf("cascaded in-layer edge key must carry a base tombstone")
	}
	if !l.EdgeTombstones()[delta.EdgeKey{SrcKey: "a", DstKey: "z", Label: "R"}] {
		t.Fatalf("hook-named base edge must be tombstoned")
	}
	if l.EdgeCount() != 0 || l.NodeCount() != 1 {
		t.Fatalf("post-cascade counts: nodes=%d edges=%d", l.NodeCount(), l.EdgeCount())
	}
}

func TestUpsertEdgeFoldRecordsBaseTombstone(t *testing.T) {
	b := delta.NewBuilder()
	_ = b.UpsertNode("a", "P", nil)
	_ = b.UpsertNode("b", "P", nil)
	_ = b.AddEdge("a", "b", "K", map[string]any{"w": 1})
	_ = b.AddEdge("a", "b", "K", map[string]any{"w": 2})
	if err := b.UpsertEdgeFold("a", "b", "K", map[string]any{"w": 3}); err != nil {
		t.Fatalf("fold: %v", err)
	}
	l := b.Build()
	if l.EdgeCount() != 1 || l.Edges()[0].Props["w"] != 3 {
		t.Fatalf("fold must leave one edge with the latest props: %+v", l.Edges())
	}
	// UpsertEdge's uniqueness contract must hold over base ∪ layer: base
	// parallels of the key are shadowed so only the folded layer record
	// remains.
	if !l.EdgeTombstones()[delta.EdgeKey{SrcKey: "a", DstKey: "b", Label: "K"}] {
		t.Fatalf("folded key must carry a base edge tombstone")
	}
}

func TestDeleteEdgePureBaseTombstone(t *testing.T) {
	b := delta.NewBuilder()
	k := delta.EdgeKey{SrcKey: "a", DstKey: "b", Label: "K"}
	for i := 0; i < 2; i++ { // idempotent
		n, err := b.DeleteEdge(k)
		if n != 0 || err != nil {
			t.Fatalf("pure-base DeleteEdge #%d: n=%d err=%v", i, n, err)
		}
	}
	l := b.Build()
	if !l.EdgeTombstones()[k] {
		t.Fatalf("deleted base key must be tombstoned")
	}
}

func TestReviveClearsNodeTombstoneButNotEdgeTombstones(t *testing.T) {
	// Delete of an in-layer node leaves a (possibly stale) node tombstone.
	b1 := delta.NewBuilder()
	_ = b1.UpsertNode("a", "P", nil)
	_, _ = b1.DeleteNode("a", nil)
	if !b1.Build().NodeTombstones()["a"] {
		t.Fatalf("delete must record the node tombstone")
	}

	// Revive clears the node tombstone (mirror of tombN) but does not
	// resurrect cascaded edges — their tombstones persist.
	b2 := delta.NewBuilder()
	_ = b2.UpsertNode("a", "P", nil)
	_ = b2.UpsertNode("b", "P", nil)
	_ = b2.AddEdge("a", "b", "L", nil)
	_, _ = b2.DeleteNode("a", nil)
	_ = b2.UpsertNode("a", "Service", map[string]any{"fresh": true})
	l2 := b2.Build()
	if len(l2.NodeTombstones()) != 0 {
		t.Fatalf("revive must clear the node tombstone: %v", l2.NodeTombstones())
	}
	rec, ok := l2.LookupNode("a")
	if !ok || rec.Label != "Service" || rec.Props["fresh"] != true {
		t.Fatalf("revived node: %+v ok=%v", rec, ok)
	}
	if l2.EdgeCount() != 0 {
		t.Fatalf("revive must not resurrect cascaded edges: %d", l2.EdgeCount())
	}
	if !l2.EdgeTombstones()[delta.EdgeKey{SrcKey: "a", DstKey: "b", Label: "L"}] {
		t.Fatalf("cascaded edge tombstone must persist across revive")
	}
}

func TestEndpointValidationLayerOnly(t *testing.T) {
	b := delta.NewBuilder()
	if err := b.AddEdge("a", "b", "K", nil); !errors.Is(err, delta.ErrNodeNotFound) {
		t.Fatalf("AddEdge with unknown endpoints: %v", err)
	}
	_ = b.UpsertNode("a", "P", nil)
	if err := b.AddEdge("a", "b", "K", nil); !errors.Is(err, delta.ErrNodeNotFound) {
		t.Fatalf("AddEdge with one unknown endpoint: %v", err)
	}
	if err := b.UpsertEdgeFold("a", "b", "K", nil); !errors.Is(err, delta.ErrNodeNotFound) {
		t.Fatalf("UpsertEdgeFold with unknown endpoint: %v", err)
	}
	_ = b.UpsertNode("b", "P", nil)
	if err := b.AddEdge("a", "b", "K", nil); err != nil {
		t.Fatalf("AddEdge with live endpoints: %v", err)
	}
	// A deleted endpoint counts as nonexistent (resolveEdge semantics).
	_, _ = b.DeleteNode("b", nil)
	if err := b.AddEdge("a", "b", "K", nil); !errors.Is(err, delta.ErrNodeNotFound) {
		t.Fatalf("AddEdge with deleted endpoint: %v", err)
	}
}

func TestBuilderSealedAfterBuild(t *testing.T) {
	b := delta.NewBuilder()
	_ = b.UpsertNode("a", "P", nil)
	_ = b.UpsertNode("b", "P", nil)
	_ = b.AddEdge("a", "b", "K", nil)
	l := b.Build()

	if err := b.UpsertNode("c", "P", nil); !errors.Is(err, delta.ErrSealed) {
		t.Fatalf("UpsertNode after Build: %v", err)
	}
	if err := b.AddEdge("a", "b", "K", nil); !errors.Is(err, delta.ErrSealed) {
		t.Fatalf("AddEdge after Build: %v", err)
	}
	if err := b.UpsertEdgeFold("a", "b", "K", nil); !errors.Is(err, delta.ErrSealed) {
		t.Fatalf("UpsertEdgeFold after Build: %v", err)
	}
	if n, err := b.DeleteEdge(delta.EdgeKey{SrcKey: "a", DstKey: "b", Label: "K"}); n != 0 || !errors.Is(err, delta.ErrSealed) {
		t.Fatalf("DeleteEdge after Build: n=%d err=%v", n, err)
	}
	if n, err := b.DeleteNode("a", nil); n != 0 || !errors.Is(err, delta.ErrSealed) {
		t.Fatalf("DeleteNode after Build: n=%d err=%v", n, err)
	}
	b.IndexProperty("k") // silent no-op after Build (vectra.IndexProperty parity)
	if got := l.IndexKeys(); len(got) != 0 {
		t.Fatalf("IndexProperty after Build must not register: %v", got)
	}

	defer func() {
		if recover() == nil {
			t.Fatal("second Build must panic")
		}
	}()
	b.Build()
}

func TestPropsDefensiveCopy(t *testing.T) {
	b := delta.NewBuilder()
	np := map[string]any{"k": "v"}
	_ = b.UpsertNode("a", "L", np)
	ep := map[string]any{"w": 1}
	_ = b.AddEdge("a", "a", "K", ep)
	// Caller mutates its maps after the call: the layer must be unaffected.
	np["k"] = "mutated"
	np["extra"] = 1
	ep["w"] = 2

	l := b.Build()
	rec, ok := l.LookupNode("a")
	if !ok || !propsEqual(rec.Props, map[string]any{"k": "v"}) {
		t.Fatalf("node props must be copied on ingress: %+v", rec)
	}
	if es := l.Edges(); len(es) != 1 || !propsEqual(es[0].Props, map[string]any{"w": 1}) {
		t.Fatalf("edge props must be copied on ingress: %+v", es)
	}
}

func TestIndexPropertyAndLabels(t *testing.T) {
	b := delta.NewBuilder()
	b.IndexProperty("name")
	b.IndexProperty("age")
	b.IndexProperty("name") // dedup
	_ = b.UpsertNode("a", "L1", nil)
	_ = b.UpsertNode("b", "L2", nil)
	_ = b.AddEdge("a", "b", "L2", nil) // dup label
	_ = b.AddEdge("b", "a", "L3", nil)
	_ = b.UpsertNode("a", "L1", nil) // dup label

	l := b.Build()
	if got := l.IndexKeys(); !reflect.DeepEqual(got, []string{"name", "age"}) {
		t.Fatalf("IndexKeys: %v", got)
	}
	if got := l.Labels(); !reflect.DeepEqual(got, []string{"L1", "L2", "L3"}) {
		t.Fatalf("Labels: %v", got)
	}
}

func TestBuildEmpty(t *testing.T) {
	l := delta.NewBuilder().Build()
	if l.NodeCount() != 0 || l.EdgeCount() != 0 || len(l.Nodes()) != 0 || len(l.Edges()) != 0 {
		t.Fatalf("empty layer must be empty")
	}
	if len(l.Labels()) != 0 || len(l.IndexKeys()) != 0 || len(l.NodeTombstones()) != 0 || len(l.EdgeTombstones()) != 0 {
		t.Fatalf("empty layer metadata must be empty")
	}
	if _, ok := l.LookupNode("anything"); ok {
		t.Fatalf("LookupNode on empty layer must miss")
	}
}
