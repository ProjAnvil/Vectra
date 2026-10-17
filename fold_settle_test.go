package vectra

import (
	"testing"
	"time"
)

// settleFolds waits for the background folder to finish any in-flight fold,
// then incorporates the hand-over through the writer's own flush path.
func settleFolds(t *testing.T, g *Graph) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for g.foldInFlight.Load() && time.Now().Before(deadline) {
		time.Sleep(100 * time.Microsecond)
	}
	if g.foldInFlight.Load() {
		t.Fatal("background fold did not finish in time")
	}
	if g.pendingFold.Load() != nil {
		g.publishLayer() // writer-side empty publish incorporates the hand-over
	}
}

// TestFolderLifecycle: the folder goroutine starts lazily on pressure, exits
// after foldIdleExit without signals, and a fold whose source prefix was
// replaced by a Seal is discarded (never spliced onto the new stack).
func TestFolderLifecycle(t *testing.T) {
	g := New().SetAutoCompact(4)
	for i := range 10 {
		if _, err := g.AddNode("f"+itoa(i), "SVC", nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	if g.folderRunning.Load() {
		t.Fatal("folder running before any pressure")
	}
	for b := 0; b < 6; b++ {
		ops := make([]foldOp, 0, 20)
		for e := 0; e < 20; e++ {
			ops = append(ops, foldOp{kind: 0, a: "f0", b: "f" + itoa(1+(b*20+e)%9), prop: e})
		}
		applyFoldOps(g, ops)
	}
	settleFolds(t, g)
	if l, _ := g.LayerPressure(); l > 4 {
		t.Fatalf("stack not folded: %d layers", l)
	}
	// idle exit
	deadline := time.Now().Add(3 * time.Second)
	for g.folderRunning.Load() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if g.folderRunning.Load() {
		t.Fatal("folder goroutine did not exit when idle")
	}
	// prefix break: provoke a fold, then Seal before the writer incorporates
	for b := 0; b < 6; b++ {
		ops := make([]foldOp, 0, 20)
		for e := 0; e < 20; e++ {
			ops = append(ops, foldOp{kind: 0, a: "f1", b: "f" + itoa(2+(b*20+e)%8), prop: e})
		}
		applyFoldOps(g, ops)
	}
	deadline = time.Now().Add(3 * time.Second)
	for g.foldInFlight.Load() && time.Now().Before(deadline) {
		time.Sleep(100 * time.Microsecond)
	}
	edgesBefore := g.EdgeCount()
	if err := g.Seal(); err != nil { // replaces the stack; the pending result's prefix no longer matches
		t.Fatal(err)
	}
	if l, _ := g.LayerPressure(); l != 0 {
		t.Fatalf("after Seal: %d layers", l)
	}
	if _, err := g.AddEdge("f0", "f1", "POST", nil); err != nil {
		t.Fatal(err)
	}
	g.FlushPend()
	if g.EdgeCount() != edgesBefore+1 {
		t.Fatalf("edges after seal+write = %d, want %d", g.EdgeCount(), edgesBefore+1)
	}
	if l, _ := g.LayerPressure(); l != 1 {
		t.Fatalf("stale fold spliced onto a sealed stack: %d layers", l)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
}
