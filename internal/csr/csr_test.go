package csr_test

import (
	"testing"

	"github.com/projanvil/vectra/internal/csr"
)

func TestBuildDegreesAndNeighbors(t *testing.T) {
	// Edges in insertion order: 0->1, 0->2, 2->0, 3->3 (self-loop)
	src := []uint32{0, 0, 2, 3}
	dst := []uint32{1, 2, 0, 3}
	a := csr.Build(4, src, dst)

	if got := a.Degree(0); got != 2 {
		t.Errorf("Degree(0) = %d, want 2", got)
	}
	if got := a.Degree(1); got != 0 {
		t.Errorf("Degree(1) = %d, want 0", got)
	}
	if got := a.Degree(3); got != 1 {
		t.Errorf("Degree(3) = %d, want 1 (self-loop)", got)
	}
	got := a.Neighbors(0)
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Errorf("Neighbors(0) = %v, want [1 2]", got)
	}
	if n := a.Neighbors(1); len(n) != 0 {
		t.Errorf("Neighbors(1) = %v, want empty", n)
	}
	if n := a.Neighbors(3); len(n) != 1 || n[0] != 3 {
		t.Errorf("Neighbors(3) = %v, want [3]", n)
	}
}

func TestBuildPayloadsKeepsSlotPairing(t *testing.T) {
	src := []uint32{0, 0, 1}
	dst := []uint32{1, 2, 2}
	edgeIDs := []uint32{10, 20, 30}
	labels := []uint32{5, 6, 7}
	a := csr.BuildPayloads(3, src, dst, edgeIDs, labels)

	tg, eids, lbs := a.Row(0)
	if len(tg) != 2 || tg[0] != 1 || tg[1] != 2 {
		t.Errorf("row targets = %v, want [1 2]", tg)
	}
	if eids[0] != 10 || eids[1] != 20 {
		t.Errorf("row edgeIDs = %v, want [10 20]", eids)
	}
	if lbs[0] != 5 || lbs[1] != 6 {
		t.Errorf("row labels = %v, want [5 6]", lbs)
	}
	// Reverse build: payloads still pair with targets
	r := csr.BuildPayloads(3, dst, src, edgeIDs, labels)
	if got := r.Neighbors(2); len(got) != 2 {
		t.Fatalf("reverse Neighbors(2) len = %d, want 2", len(got))
	}
	rt, re, rl := r.Row(2)
	// Insertion order: edge 20 (0->2) comes before edge 30 (1->2)
	if rt[0] != 0 || rt[1] != 1 || re[0] != 20 || re[1] != 30 || rl[0] != 6 || rl[1] != 7 {
		t.Errorf("reverse row = %v %v %v, want [0 1] [20 30] [6 7]", rt, re, rl)
	}
}

func TestBuildEmptyGraph(t *testing.T) {
	a := csr.Build(3, nil, nil)
	if len(a.Offsets) != 4 {
		t.Errorf("Offsets len = %d, want 4", len(a.Offsets))
	}
	for i, o := range a.Offsets {
		if o != 0 {
			t.Errorf("Offsets[%d] = %d, want 0", i, o)
		}
	}
}
