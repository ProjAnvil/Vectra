package delta

import (
	"math/rand"
	"reflect"
	"testing"
)

// buildLayersFrom runs each fn through a fresh Builder and returns the built
// layers (ID-less, plain Build).
func buildLayersFrom(t *testing.T, fns ...func(b *Builder)) []*Layer {
	t.Helper()
	out := make([]*Layer, 0, len(fns))
	for _, fn := range fns {
		b := NewBuilder()
		fn(b)
		out = append(out, b.Build())
	}
	return out
}

// TestFoldLayersNodeAndEdgeSemantics pins the fold rules on a deterministic
// stack: cross-layer node override, tombstone-then-revive, parallel edges
// across layers, a later edge tombstone killing older parallels, and the
// label/index-key union.
func TestFoldLayersNodeAndEdgeSemantics(t *testing.T) {
	layers := buildLayersFrom(t,
		func(b *Builder) {
			_ = b.UpsertNode("a", "A0", map[string]any{"v": 0})
			_ = b.UpsertNode("b", "B", nil)
			_ = b.AddEdge("a", "b", "CALLS", nil)
			_ = b.AddEdge("a", "b", "CALLS", map[string]any{"n": 1}) // parallel
			b.IndexProperty("tier")
		},
		func(b *Builder) {
			_ = b.UpsertNode("a", "A1", map[string]any{"v": 1}) // override
			_ = b.UpsertNode("b", "B", nil)                     // endpoint lift for the new edge
			_, _ = b.DeleteEdge(EdgeKey{SrcKey: "a", DstKey: "b", Label: "CALLS"}) // kills layer-1 parallels via tombstone
			_ = b.AddEdge("b", "a", "DATA", nil)                                     // survives
		},
		func(b *Builder) {
			_, _ = b.DeleteNode("b", nil) // tombstone (b dead until revived)
			_ = b.UpsertNode("c", "C", nil)
		},
		func(b *Builder) {
			_ = b.UpsertNode("b", "B2", map[string]any{"r": 1}) // revive
		},
	)
	f := FoldLayers(layers)
	if f == nil {
		t.Fatal("nil fold")
	}

	// nodes: a=A1 override, b=B2 revived (first-seen order a, b, c).
	// Builder copyProps(nil) yields an empty (non-nil) map.
	wantNodes := []NodeRec{
		{Key: "a", Label: "A1", Props: map[string]any{"v": 1}},
		{Key: "b", Label: "B2", Props: map[string]any{"r": 1}},
		{Key: "c", Label: "C", Props: nil},
	}
	if got := f.Nodes(); !reflect.DeepEqual(got, wantNodes) {
		t.Fatalf("nodes = %+v, want %+v", got, wantNodes)
	}

	// edges: layer-1 CALLS parallels killed by layer-2 tombstone; DATA survives
	wantEdges := []EdgeRec{{SrcKey: "b", DstKey: "a", Label: "DATA", Props: nil}}
	if got := f.Edges(); !reflect.DeepEqual(got, wantEdges) {
		t.Fatalf("edges = %+v, want %+v", got, wantEdges)
	}

	// tombstones: full union (both must keep shadowing any base records)
	for _, k := range []string{"b"} {
		if !f.NodeTombstones()[k] {
			t.Errorf("node tombstone %q lost in fold", k)
		}
	}
	ek := EdgeKey{SrcKey: "a", DstKey: "b", Label: "CALLS"}
	if !f.EdgeTombstones()[ek] {
		t.Error("edge tombstone lost in fold")
	}

	// labels + index keys: first-seen union
	labs := map[string]bool{}
	for _, l := range f.Labels() {
		labs[l] = true
	}
	for _, want := range []string{"A0", "B", "CALLS", "A1", "DATA", "C", "B2"} {
		if !labs[want] {
			t.Errorf("label %q missing from fold union: %v", want, f.Labels())
		}
	}
	if !reflect.DeepEqual(f.IndexKeys(), []string{"tier"}) {
		t.Errorf("indexKeys = %v", f.IndexKeys())
	}
}

// foldOracle replays the documented fold rules independently of FoldLayers:
// node winner per key (tombstone kills, later record wins), edge survivors
// per key (later tombstone wipes older parallels, survivors keep per-key
// order), tombstone-set union.
type foldOracle struct {
	nodes    map[string]NodeRec
	dead     map[string]bool
	order    []string
	edges    map[EdgeKey][]EdgeRec
	nodeTomb map[string]bool
	edgeTomb map[EdgeKey]bool
}

func newFoldOracle() *foldOracle {
	return &foldOracle{
		nodes: map[string]NodeRec{}, dead: map[string]bool{},
		edges: map[EdgeKey][]EdgeRec{}, nodeTomb: map[string]bool{}, edgeTomb: map[EdgeKey]bool{},
	}
}

func (o *foldOracle) apply(l *Layer) {
	for k := range l.NodeTombstones() {
		o.nodeTomb[k] = true
		o.dead[k] = true
	}
	for _, rec := range l.Nodes() {
		if _, seen := o.nodes[rec.Key]; !seen {
			o.order = append(o.order, rec.Key)
		}
		o.nodes[rec.Key] = rec
		o.dead[rec.Key] = false
	}
	for k := range l.EdgeTombstones() {
		o.edgeTomb[k] = true
		o.edges[k] = nil
	}
	for _, rec := range l.Edges() {
		k := EdgeKey{SrcKey: rec.SrcKey, DstKey: rec.DstKey, Label: rec.Label}
		o.edges[k] = append(o.edges[k], rec)
	}
}

func (o *foldOracle) check(t *testing.T, f *Layer) {
	t.Helper()
	var wantNodes []NodeRec
	for _, k := range o.order {
		if !o.dead[k] {
			wantNodes = append(wantNodes, o.nodes[k])
		}
	}
	if got := f.Nodes(); len(got) != len(wantNodes) {
		t.Fatalf("nodes\n got  %+v\n want %+v", got, wantNodes)
	}
	for i := range wantNodes {
		if !reflect.DeepEqual(f.Nodes()[i], wantNodes[i]) {
			t.Fatalf("nodes[%d]\n got  %+v\n want %+v", i, f.Nodes()[i], wantNodes[i])
		}
	}
	if !reflect.DeepEqual(f.NodeTombstones(), o.nodeTomb) {
		t.Fatalf("nodeTomb\n got  %v\n want %v", f.NodeTombstones(), o.nodeTomb)
	}
	if !reflect.DeepEqual(f.EdgeTombstones(), o.edgeTomb) {
		t.Fatalf("edgeTomb\n got  %v\n want %v", f.EdgeTombstones(), o.edgeTomb)
	}
}

func TestFoldLayersRandomDifferential(t *testing.T) {
	const rounds = 200
	for round := range rounds {
		rng := rand.New(rand.NewSource(int64(round)))
		keys := []string{"k0", "k1", "k2", "k3"}
		labs := []string{"A", "B", "C"}
		var layers []*Layer
		o := newFoldOracle()
		for range 1 + rng.Intn(5) {
			b := NewBuilder()
			for range 1 + rng.Intn(6) {
				k := keys[rng.Intn(len(keys))]
				dk := keys[rng.Intn(len(keys))]
				switch rng.Intn(5) {
				case 0:
					_ = b.UpsertNode(k, labs[rng.Intn(len(labs))], map[string]any{"v": rng.Intn(3)})
				case 1:
					_, _ = b.DeleteNode(k, nil)
				case 2:
					if k != dk {
						_ = b.AddEdge(k, dk, labs[rng.Intn(len(labs))], nil)
					}
				case 3:
					_, _ = b.DeleteEdge(EdgeKey{SrcKey: k, DstKey: dk, Label: labs[rng.Intn(len(labs))]})
				case 4:
					_ = b.UpsertNode(dk, labs[rng.Intn(len(labs))], nil)
				}
			}
			l := b.Build()
			layers = append(layers, l)
			o.apply(l)
		}
		f := FoldLayers(layers)
		o.check(t, f)

			// per-key surviving edge sequences match the oracle
			for k, want := range o.edges {
				var got []EdgeRec
				for _, rec := range f.Edges() {
					if (EdgeKey{SrcKey: rec.SrcKey, DstKey: rec.DstKey, Label: rec.Label}) == k {
						got = append(got, rec)
					}
				}
				if len(got) != len(want) {
					t.Fatalf("round %d key %v: %d surviving records, want %d", round, k, len(got), len(want))
				}
				for i := range got {
					if !reflect.DeepEqual(got[i], want[i]) {
						t.Fatalf("round %d key %v [%d]: got %+v want %+v", round, k, i, got[i], want[i])
					}
			}
		}
	}
}

func TestFoldLayersIdempotent(t *testing.T) {
	layers := buildLayersFrom(t,
		func(b *Builder) {
			_ = b.UpsertNode("a", "A", nil)
			_ = b.AddEdge("a", "a", "L", nil)
		},
		func(b *Builder) {
			_ = b.UpsertNode("b", "B", nil)
			_, _ = b.DeleteEdge(EdgeKey{SrcKey: "a", DstKey: "a", Label: "L"})
		},
	)
	once := FoldLayers(layers)
	twice := FoldLayers([]*Layer{once})
	if once == twice && len(layers) > 1 {
		// single-layer fast path may return the same pointer; with a real fold
		// the results must still be field-identical
	}
	if !reflect.DeepEqual(once.Nodes(), twice.Nodes()) || !reflect.DeepEqual(once.Edges(), twice.Edges()) ||
		!reflect.DeepEqual(once.NodeTombstones(), twice.NodeTombstones()) ||
		!reflect.DeepEqual(once.EdgeTombstones(), twice.EdgeTombstones()) ||
		!reflect.DeepEqual(once.Labels(), twice.Labels()) {
		t.Fatal("fold not idempotent")
	}
}

// TestFoldLayersIDPreservation: ID-carrying layers keep every surviving
// record's writer-assigned ID through the fold (override keeps the original
// node ID; killed edges drop their IDs).
func TestFoldLayersIDPreservation(t *testing.T) {
	mk := func(fn func(b *Builder), nodeIDs map[string]uint32, edgeIDs []uint32) *Layer {
		b := NewBuilder()
		fn(b)
		return b.BuildWithIDs(nodeIDs, edgeIDs)
	}
	l1 := mk(func(b *Builder) {
		_ = b.UpsertNode("a", "A", nil)
		_ = b.UpsertNode("b", "B", nil)
		_ = b.AddEdge("a", "b", "L", nil)
	}, map[string]uint32{"a": 10, "b": 11}, []uint32{100})
	l2 := mk(func(b *Builder) {
		_ = b.UpsertNode("a", "A2", nil) // override, same ID per writer contract
		_ = b.UpsertNode("c", "C", nil)
	}, map[string]uint32{"a": 10, "c": 12}, []uint32{})

	f := FoldLayers([]*Layer{l1, l2})
	if len(f.Nodes()) != 3 {
		t.Fatalf("nodes = %d, want 3", len(f.Nodes()))
	}
	if id, ok := f.LookupNodeID("a"); !ok || id != 10 {
		t.Errorf("override ID = %d ok=%v, want 10", id, ok)
	}
	if id, ok := f.LookupNodeID("c"); !ok || id != 12 {
		t.Errorf("new node ID = %d ok=%v, want 12", id, ok)
	}
	if len(f.Edges()) != 1 || f.EdgeIDAt(0) != 100 {
		t.Errorf("edge IDs not preserved: n=%d id0=%v", len(f.Edges()), f.EdgeIDAt(0))
	}
	// FindEdgeID resolves through the lazily-built index
	if i, ok := f.FindEdgeID(100); !ok || i != 0 {
		t.Errorf("FindEdgeID = %d %v", i, ok)
	}
}
