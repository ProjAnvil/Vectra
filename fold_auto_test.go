package vectra

import (
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// ---- helpers ---------------------------------------------------------------

// compareGraphs asserts two graphs that received the same op stream are
// indistinguishable on every observable surface. Auto-compaction must be
// exactly that: a view-level transformation. strictIDs compares record IDs
// too (pre-Seal: folds preserve IDs); after an explicit Seal the dense
// renumbering (R1) is order-dependent and legitimately differs between a
// folded and an unfolded stack, so only ID-free structure is compared there.
func compareGraphs(t *testing.T, gOn, gOff *Graph, keys []string, strictIDs bool) {
	t.Helper()
	if a, b := gOn.NodeCount(), gOff.NodeCount(); a != b {
		t.Fatalf("NodeCount %d vs %d", a, b)
	}
	if a, b := gOn.EdgeCount(), gOff.EdgeCount(); a != b {
		t.Fatalf("EdgeCount %d vs %d", a, b)
	}
	for _, k := range keys {
		a, aok := gOn.NodeByKey(k)
		b, bok := gOff.NodeByKey(k)
		if aok != bok || (aok && a.Label != b.Label) {
			t.Fatalf("NodeByKey(%q): %+v/%v vs %+v/%v", k, a, aok, b, bok)
		}
		if strictIDs && aok && a.ID != b.ID {
			t.Fatalf("NodeByKey(%q) ID: %d vs %d", k, a.ID, b.ID)
		}
		ea, erra := gOn.EdgesOf(k, DirOut)
		eb, errb := gOff.EdgesOf(k, DirOut)
		if erra != errb || len(ea) != len(eb) {
			t.Fatalf("EdgesOf(%q): %v(%v) vs %v(%v)", k, ea, erra, eb, errb)
		}
		for i := range ea {
			sameEnds := ea[i].Src == eb[i].Src && ea[i].Dst == eb[i].Dst
			if !sameEnds && !strictIDs {
				// post-Seal: dense renumbering (R1) differs between a folded and
				// an unfolded stack — compare endpoints by key, not number
				sa, _ := gOn.NodeByID(ea[i].Src)
				da, _ := gOn.NodeByID(ea[i].Dst)
				sb, _ := gOff.NodeByID(eb[i].Src)
				db, _ := gOff.NodeByID(eb[i].Dst)
				sameEnds = sa.Key == sb.Key && da.Key == db.Key
			}
			if !sameEnds || ea[i].Label != eb[i].Label {
				t.Fatalf("EdgesOf(%q)[%d]: %+v vs %+v", k, i, ea[i], eb[i])
			}
			if strictIDs && ea[i].ID != eb[i].ID {
				t.Fatalf("EdgesOf(%q)[%d] ID: %d vs %d", k, i, ea[i].ID, eb[i].ID)
			}
		}
	}
	// traversal surfaces: BFS layer sets, reachability, shortest paths.
	// Start keys must exist on BOTH sides (a deleted key resolves to a zero
	// ID, and post-Seal renumbering would point the two graphs' zero IDs at
	// different nodes — a false divergence).
	for _, k := range keys {
		na, aok := gOn.NodeByKey(k)
		nb, bok := gOff.NodeByKey(k)
		if aok != bok {
			t.Fatalf("existence mismatch for %q: %v vs %v", k, aok, bok)
		}
		if !aok {
			continue
		}
		la, err := gOn.BFS([]NodeID{na.ID})
		if err != nil {
			t.Fatal(err)
		}
		lb, err := gOff.BFS([]NodeID{nb.ID})
		if err != nil {
			t.Fatal(err)
		}
		if la.Count() != lb.Count() {
			t.Fatalf("BFS(%q) count %d vs %d", k, la.Count(), lb.Count())
		}
		ra, err := gOn.ReachableIDs(na.ID)
		if err != nil {
			t.Fatal(err)
		}
		rb, err := gOff.ReachableIDs(nb.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(ra) != len(rb) {
			t.Fatalf("Reachable(%q) %d vs %d", k, len(ra), len(rb))
		}
	}
	for i, a := range keys {
		for j, b := range keys {
			if i == j {
				continue
			}
			pa, fa, _ := gOn.ShortestPath(a, b)
			pb, fb, _ := gOff.ShortestPath(a, b)
			if fa != fb || (fa && pa.Len() != pb.Len()) {
				t.Fatalf("ShortestPath(%q,%q): %v/%v vs %v/%v", a, b, fa, pa.Len(), fb, pb.Len())
			}
		}
	}
}

// ---- differential equivalence ----------------------------------------------

type foldOp struct {
	kind int // 0 addEdge, 1 upsertEdge, 2 deleteEdge, 3 deleteNode, 4 reviveNode
	a, b string
	prop int
}

func applyFoldOps(g *Graph, batch []foldOp) {
	_ = g.Batch(func(gg *Graph) {
		for _, o := range batch {
			switch o.kind {
			case 0:
				_, _ = gg.AddEdge(o.a, o.b, "MIXED", map[string]any{"w": o.prop})
			case 1:
				_, _ = gg.UpsertEdge(o.a, o.b, "MIXED", map[string]any{"w": o.prop})
			case 2:
				_, _ = gg.DeleteEdge(o.a, o.b, "MIXED")
			case 3:
				_, _ = gg.DeleteNode(o.a)
			case 4:
				_, _ = gg.AddNode(o.a, "SVC", nil)
			}
		}
	})
}

func TestAutoCompactDifferentialEquivalence(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	keys := make([]string, 12)
	for i := range keys {
		keys[i] = "n" + string(rune('0'+i))
	}
	gOn := New().SetAutoCompact(4)
	gOff := New().SetAutoCompact(0)
	for _, g := range []*Graph{gOn, gOff} {
		for _, k := range keys {
			if _, err := g.AddNode(k, "SVC", nil); err != nil {
				t.Fatal(err)
			}
		}
		for i := 0; i < len(keys)-1; i++ {
			if _, err := g.AddEdge(keys[i], keys[i+1], "BASE", nil); err != nil {
				t.Fatal(err)
			}
		}
		if err := g.Seal(); err != nil {
			t.Fatal(err)
		}
	}
	folded := false
	for batch := 0; batch < 60; batch++ {
		n := 4 + rng.Intn(8)
		ops := make([]foldOp, 0, n)
		for range n {
			a, b := keys[rng.Intn(len(keys))], keys[rng.Intn(len(keys))]
			if a == b {
				b = keys[(slices.Index(keys, a)+1)%len(keys)]
			}
			ops = append(ops, foldOp{kind: rng.Intn(5), a: a, b: b, prop: rng.Intn(1000)})
		}
		applyFoldOps(gOn, ops)
		applyFoldOps(gOff, ops)
		if l, _ := gOn.LayerPressure(); l <= 2 {
			folded = true // the stack was folded down to runs
		}
		if batch%25 == 24 {
			compareGraphs(t, gOn, gOff, keys, true)
		}
	}
	if !folded {
		l, _ := gOn.LayerPressure()
		t.Fatalf("governor never folded during the stream: %d layers", l)
	}
	if err := gOn.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := gOff.Seal(); err != nil {
		t.Fatal(err)
	}
	compareGraphs(t, gOn, gOff, keys, false)
}

// ---- trigger + continuation --------------------------------------------------

func TestAutoCompactTriggerAndContinue(t *testing.T) {
	g := New().SetAutoCompact(4)
	for i := range 10 {
		if _, err := g.AddNode("n"+string(rune('0'+i)), "SVC", nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	folded := false
	for b := 0; b < 12; b++ {
		ops := make([]foldOp, 0, 20)
		for e := 0; e < 20; e++ {
			ops = append(ops, foldOp{kind: 0, a: "n0", b: "n" + string(rune('0'+1+(b*20+e)%9)), prop: e})
		}
		applyFoldOps(g, ops)
		if l, _ := g.LayerPressure(); l <= 2 {
			folded = true
		}
		if l, _ := g.LayerPressure(); l > 8 { // volume gate: ~64-edge tail = a few raw layers between folds
			t.Fatalf("after batch %d: %d layers", b, l)
		}
	}
	if !folded {
		l, _ := g.LayerPressure()
		t.Fatalf("governor never folded: %d layers", l)
	}
	// writes and queries stay correct after folds
	if g.EdgeCount() < 240 {
		t.Fatalf("edges = %d after folds", g.EdgeCount())
	}
	if _, err := g.NeighborsOf("n0", DirOut); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddEdge("n1", "n2", "AFTER", nil); err != nil {
		t.Fatal(err)
	}
	n, ok := g.NodeByKey("n5")
	if !ok {
		t.Fatal("node lost after folds")
	}
	if _, err := g.BFS([]NodeID{n.ID}); err != nil {
		t.Fatal(err)
	}
}

func TestAutoCompactDisabledByZero(t *testing.T) {
	g := New().SetAutoCompact(0)
	for i := range 10 {
		if _, err := g.AddNode("n"+string(rune('0'+i)), "SVC", nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	for b := 0; b < 8; b++ {
		applyFoldOps(g, []foldOp{{kind: 0, a: "n0", b: "n" + string(rune('0'+1+b%9)), prop: b}})
	}
	if l, _ := g.LayerPressure(); l != 8 {
		t.Fatalf("compaction ran despite cap 0: %d layers", l)
	}
}

// ---- volume auto-seal --------------------------------------------------------

func TestAutoSealVolumeTrigger(t *testing.T) {
	g := New().SetAutoCompact(4).SetAutoSealEdges(10)
	for i := range 8 {
		if _, err := g.AddNode("s"+string(rune('0'+i)), "SVC", nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	sealed := false
	for b := 0; b < 12; b++ {
		ops := make([]foldOp, 0, 8)
		for e := 0; e < 8; e++ {
			ops = append(ops, foldOp{kind: 0, a: "s0", b: "s" + string(rune('0'+1+(b*8+e)%7)), prop: e})
		}
		applyFoldOps(g, ops)
		if l, _ := g.LayerPressure(); l == 0 {
			sealed = true // the volume trigger ran forceMerge
		}
	}
	if !sealed {
		t.Fatal("volume auto-seal never fired (threshold 10 edges)")
	}
	if g.EdgeCount() < 96 {
		t.Fatalf("edges = %d after auto-seal", g.EdgeCount())
	}
	if _, err := g.NeighborsOf("s0", DirOut); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddEdge("s1", "s2", "POST", nil); err != nil {
		t.Fatal(err)
	}
}

func TestAutoSealDisabledByZero(t *testing.T) {
	g := New().SetAutoCompact(4).SetAutoSealEdges(0)
	for i := range 8 {
		if _, err := g.AddNode("s"+string(rune('0'+i)), "SVC", nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	for b := 0; b < 12; b++ {
		applyFoldOps(g, []foldOp{{kind: 0, a: "s0", b: "s" + string(rune('0'+1+b%7)), prop: b}})
	}
	if l, _ := g.LayerPressure(); l == 0 {
		t.Fatal("auto-seal fired despite SetAutoSealEdges(0)")
	}
}

// ---- WAL replay safety -------------------------------------------------------

func TestAutoCompactWALReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "graph.db")
	g, err := Open(path, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	g.SetAutoCompact(2)
	for i := range 6 {
		if _, err := g.AddNode("w"+string(rune('0'+i)), "SVC", nil); err != nil {
			t.Fatal(err)
		}
	}
	for b := 0; b < 8; b++ {
		ops := make([]foldOp, 0, 16)
		for e := 0; e < 16; e++ {
			ops = append(ops, foldOp{kind: 0, a: "w0", b: "w" + string(rune('0'+1+(b*16+e)%5)), prop: e})
		}
		applyFoldOps(g, ops)
		if l, _ := g.LayerPressure(); l > 4 {
			t.Fatalf("%d layers > cap 4", l)
		}
	}
	wantNodes, wantEdges := g.NodeCount(), g.EdgeCount()
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g2, err := Open(path, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer g2.Close()
	if g2.NodeCount() != wantNodes || g2.EdgeCount() != wantEdges {
		t.Fatalf("replay: %d/%d want %d/%d", g2.NodeCount(), g2.EdgeCount(), wantNodes, wantEdges)
	}
	if _, err := g2.NeighborsOf("w0", DirOut); err != nil {
		t.Fatal(err)
	}
	_ = os.Remove(path)
}
