package engine

import (
	"slices"
	"testing"
)

func TestKillSetBasics(t *testing.T) {
	var ks KillSet
	if ks.Has(7) {
		t.Fatal("empty set has")
	}
	ks = ks.With([]uint32{7, 100_000, 100_001})
	for _, id := range []uint32{7, 100_000, 100_001} {
		if !ks.Has(id) {
			t.Fatalf("missing %d", id)
		}
	}
	for _, id := range []uint32{0, 6, 8, 99_999, 100_002, 1 << 30} {
		if ks.Has(id) {
			t.Fatalf("phantom %d", id)
		}
	}
	// idempotent append
	ks2 := ks.With([]uint32{7})
	if !ks2.Has(7) || !ks2.Has(100_000) {
		t.Fatal("append lost entries")
	}
}

// TestKillSetCOWSharing: appending copies only the touched chunk — untouched
// chunks share their backing arrays across generations (the whole point:
// per-publish aggregation is O(new kills), not O(all kills)).
func TestKillSetCOWSharing(t *testing.T) {
	ks := KillSet{}.With([]uint32{5, 5 + KillChunkIDs, 5 + 2*KillChunkIDs}) // 3 chunks
	if len(ks) != 3 {
		t.Fatalf("chunks = %d", len(ks))
	}
	ks2 := ks.With([]uint32{6}) // touches chunk 0 only
	if len(ks2) != 3 {
		t.Fatalf("chunks after append = %d", len(ks2))
	}
	if &ks2[0][0] == &ks[0][0] {
		t.Fatal("touched chunk not copied")
	}
	if &ks2[1][0] != &ks[1][0] || &ks2[2][0] != &ks[2][0] {
		t.Fatal("untouched chunks not shared")
	}
	if ks.Has(6) {
		t.Fatal("append leaked into the original")
	}
}

// TestKillSetAllOnes: every bit in a chunk round-trips.
func TestKillSetAllOnes(t *testing.T) {
	var ids []uint32
	for i := range uint32(KillChunkIDs) {
		ids = append(ids, KillChunkBase(1)+i)
	}
	ks := KillSet{}.With(ids)
	if len(ks) != 1 {
		t.Fatalf("chunks = %d", len(ks))
	}
	for _, id := range ids {
		if !ks.Has(id) {
			t.Fatalf("missing %d", id)
		}
	}
	_ = slices.Contains(ids, 0)
}
