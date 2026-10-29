package vector

import (
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"testing"
)

// bruteTopKeyed returns the exact top-k key set over (vecs, keys).
func bruteTopKeyed(vecs [][]float32, keys []string, q []float32, k int) map[string]bool {
	out := map[string]bool{}
	for _, i := range bruteTopIDs(vecs, q, k) {
		out[keys[i]] = true
	}
	return out
}

// indexKeys returns k0..k{len(vecs)-1}.
func indexKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", i)
	}
	return keys
}

func TestStateOverlayImmediateVisibility(t *testing.T) {
	s := NewState(Cosine, 4, 16, 200, 1)
	target := Normalize([]float32{1, 0, 0, 0})
	s.Add("needle", target)
	for i := 0; i < 50; i++ {
		v := normalizeRand(rand.New(rand.NewSource(int64(i))), 4)
		s.Add(fmt.Sprintf("filler%d", i), v)
	}
	got := s.Search(target, 1, 64)
	if len(got) != 1 || got[0].Key != "needle" {
		t.Fatalf("overlay must make committed vectors immediately searchable, got %+v", got)
	}
}

func TestStateUpdateShadowsOld(t *testing.T) {
	s := NewState(Cosine, 4, 16, 200, 1)
	old := Normalize([]float32{1, 0, 0, 0})
	s.Add("k", old)
	s.Pack(true) // old vector now lives in a segment
	fresh := Normalize([]float32{0, 1, 0, 0})
	s.Add("k", fresh) // update via overlay
	got := s.Search(fresh, 1, 64)
	if len(got) != 1 || got[0].Key != "k" {
		t.Fatalf("updated key must still be found, got %+v", got)
	}
	got = s.Search(old, 1, 64)
	if len(got) == 1 && got[0].Key == "k" && got[0].Score < 0.5 {
		t.Fatal("query near the OLD vector must not return the new vector at a tiny score")
	}
}

// TestStateSameCycleUpdate: the same key added twice within one overlay
// cycle must keep only the newest vector through the pack (spec §4.2).
func TestStateSameCycleUpdate(t *testing.T) {
	s := NewState(Cosine, 4, 16, 200, 1)
	old := Normalize([]float32{1, 0, 0, 0})
	s.Add("k", old)
	for i := 0; i < 100; i++ {
		s.Add(fmt.Sprintf("f%d", i), normalizeRand(rand.New(rand.NewSource(int64(i))), 4))
	}
	fresh := Normalize([]float32{0, 1, 0, 0})
	s.Add("k", fresh)
	s.Pack(true)
	got := s.Search(fresh, 1, 64)
	if len(got) != 1 || got[0].Key != "k" || got[0].Score > 1e-6 {
		t.Fatalf("newest vector must win the pack: %+v", got)
	}
	got = s.Search(old, 3, 64)
	for _, g := range got {
		if g.Key == "k" && g.Score < 0.5 {
			t.Fatalf("old vector leaked with tiny score: %+v", g)
		}
	}
}

func TestStateDeleteAndRevive(t *testing.T) {
	s := NewState(Cosine, 4, 16, 200, 1)
	v := Normalize([]float32{1, 0, 0, 0})
	s.Add("k", v)
	s.Pack(true)
	if !s.Delete("k") {
		t.Fatal("delete of existing key must succeed")
	}
	if got := s.Search(v, 1, 64); len(got) != 0 {
		t.Fatalf("deleted key leaked: %+v", got)
	}
	s.Add("k", v) // revive
	got := s.Search(v, 1, 64)
	if len(got) != 1 || got[0].Key != "k" {
		t.Fatalf("revived key must be found, got %+v", got)
	}
}

func TestStateWrongDimQuery(t *testing.T) {
	s := NewState(Cosine, 4, 16, 200, 1)
	s.Add("k", Normalize([]float32{1, 0, 0, 0}))
	if got := s.Search([]float32{1, 0, 0}, 1, 64); got != nil {
		t.Fatal("wrong-dim query must return nil")
	}
}

func TestStateTierCascadeRecall(t *testing.T) {
	const n, dim = 6000, 32 // 2 full packs + remainder: tier0 holds 3 L0 segments (4-way fan-in)
	rng := rand.New(rand.NewSource(21))
	s := NewState(Cosine, dim, 16, 200, 7)
	vecs := make([][]float32, n)
	for i := range vecs {
		vecs[i] = normalizeRand(rng, dim)
		s.Add(fmt.Sprintf("k%d", i), vecs[i])
		if s.OverlayLen() >= packSize {
			s.Pack(false)
		}
	}
	s.Pack(true)
	if s.OverlayLen() != 0 {
		t.Fatal("pack must clear the overlay")
	}
	keys := indexKeys(n)
	recall := 0.0
	for qi := 0; qi < 20; qi++ {
		q := vecs[rng.Intn(n)]
		got := s.Search(q, 10, 64)
		truth := bruteTopKeyed(vecs, keys, q, 10)
		hit := 0
		for _, g := range got {
			if truth[g.Key] {
				hit++
			}
		}
		recall += float64(hit) / 10
	}
	recall /= 20
	if recall < 0.9 {
		t.Fatalf("post-cascade recall@10 = %.3f, want >= 0.9", recall)
	}
}

// TestStateTierFourWayCascade locks the 4-way merge fan-in (spec §4.2
// amendment, 2026-10-19): a tier holds up to 3 segments; the 4th arrival
// merges all 4 into one and promotes it. Failed under the previous 2-way
// policy (merge on 2nd arrival), which is the point.
func TestStateTierFourWayCascade(t *testing.T) {
	const dim = 8
	pack := func(s *State, n int, base int, rng *rand.Rand) {
		for i := 0; i < n; i++ {
			s.Add(fmt.Sprintf("k%d", base+i), normalizeRand(rng, dim))
			if s.OverlayLen() >= packSize {
				s.Pack(false)
			}
		}
	}
	segLens := func(t *testing.T, s *State) [][]int {
		out := make([][]int, 0, len(s.tiers))
		for _, tier := range s.tiers {
			lens := make([]int, 0, len(tier))
			for _, seg := range tier {
				lens = append(lens, seg.h.Len())
			}
			out = append(out, lens)
		}
		return out
	}

	s := NewState(Cosine, dim, 16, 100, 51)
	rng := rand.New(rand.NewSource(52))
	pack(s, 3*2048, 0, rng) // 3 full packs
	s.Pack(true)
	got := segLens(t, s)
	want := [][]int{{2048, 2048, 2048}} // 3 arrivals: no merge yet
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("after 3 packs: tiers = %v, want %v", got, want)
	}

	pack(s, 2048, 3*2048, rng) // 4th arrival merges 4 -> 1, promotes
	s.Pack(true)
	got = segLens(t, s)
	want = [][]int{{}, {8192}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("after 4th pack: tiers = %v, want %v", got, want)
	}
}

// TestStateDeathCompaction verifies the 30% compaction path. Ground truth
// is computed over LIVE vectors only — half the space is dead, so a
// whole-space truth would cap recall near 0.6 by construction.
func TestStateDeathCompaction(t *testing.T) {
	const n, dim = 4096, 16
	rng := rand.New(rand.NewSource(31))
	s := NewState(Cosine, dim, 16, 200, 17)
	vecs := make([][]float32, n)
	for i := range vecs {
		vecs[i] = normalizeRand(rng, dim)
		s.Add(fmt.Sprintf("k%d", i), vecs[i])
	}
	s.Pack(true)
	for i := 0; i < n; i += 2 { // 50% death -> exceeds the 30% threshold
		s.Delete(fmt.Sprintf("k%d", i))
	}
	s.Pack(true) // triggers compaction pass
	live := make([][]float32, 0, n/2)
	liveKeys := make([]string, 0, n/2)
	for i := 1; i < n; i += 2 {
		live = append(live, vecs[i])
		liveKeys = append(liveKeys, fmt.Sprintf("k%d", i))
	}
	recall := 0.0
	for qi := 0; qi < 20; qi++ {
		q := vecs[2*rng.Intn(n/2)+1] // query at a live point
		got := s.Search(q, 10, 64)
		truth := bruteTopKeyed(live, liveKeys, q, 10)
		hit := 0
		for _, g := range got {
			if truth[g.Key] {
				hit++
			}
		}
		recall += float64(hit) / 10
	}
	recall /= 20
	if recall < 0.9 {
		t.Fatalf("post-compaction recall@10 = %.3f, want >= 0.9 (live-only truth)", recall)
	}
}

// TestStateNoPhantomRetention guards the nil-drop memory rule: after a
// build with cascading merges, live heap stays close to one vector copy.
func TestStateNoPhantomRetention(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	if raceEnabled {
		t.Skip("race: 8192x768 HNSW construction runs ~30x under instrumentation (32s -> 15m+) and blows the package's 600s budget; single-goroutine test, so -race adds no coverage — Pack/merge paths stay race-covered by the tier-cascade and death-compaction tests")
	}
	const n, dim = 8192, 768 // dim matters: at 128 the ~2-3MB fixed overhead (maps, links, slice headers) cannot amortize under 4MB theory and the line is meaningless
	rng := rand.New(rand.NewSource(41))
	s := NewState(Cosine, dim, 16, 200, 43)
	for i := 0; i < n; i++ {
		s.Add(fmt.Sprintf("k%d", i), normalizeRand(rng, dim))
		if (i+1)%packSize == 0 {
			s.Pack(false)
		}
	}
	s.Pack(true)
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	runtime.KeepAlive(s)
	amp := float64(ms.HeapAlloc) / float64(n*dim*4)
	if amp > 1.6 {
		t.Fatalf("live heap amplification = %.2fx, want <= 1.6 (stale-pointer retention?)", amp)
	}
}
