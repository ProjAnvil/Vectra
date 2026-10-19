package vector

import (
	"math/rand"
	"testing"
)

// bruteCheck returns recall of got against the brute-force top-k.
func bruteCheck(vecs [][]float32, q []float32, k int, got []Hit) float64 {
	truth := map[uint32]bool{}
	for _, i := range bruteTopIDs(vecs, q, k) {
		truth[uint32(i)] = true
	}
	hit := 0
	for _, g := range got {
		if truth[g.ID] {
			hit++
		}
	}
	return float64(hit) / float64(k)
}

func TestHNSWRecallAndBasics(t *testing.T) {
	const n, dim = 1000, 32
	rng := rand.New(rand.NewSource(7))
	vecs := make([][]float32, n)
	for i := range vecs {
		vecs[i] = normalizeRand(rng, dim)
	}
	h := NewHNSW(Cosine, 16, 200, 42)
	for i, v := range vecs {
		h.Insert(uint32(i), v)
	}
	if h.Len() != n {
		t.Fatalf("len = %d, want %d", h.Len(), n)
	}
	// Independent queries: training-point queries inflate recall.
	queries := make([][]float32, 20)
	for i := range queries {
		queries[i] = normalizeRand(rng, dim)
	}
	recall := 0.0
	for _, q := range queries {
		got := h.Search(q, 10, 64, nil)
		if len(got) != 10 {
			t.Fatalf("got %d hits, want 10", len(got))
		}
		for i := 1; i < len(got); i++ { // ascending order invariant
			if got[i].Score < got[i-1].Score {
				t.Fatal("hits must be ascending by score")
			}
		}
		recall += bruteCheck(vecs, q, 10, got)
	}
	recall /= 20
	if recall < 0.9 {
		t.Fatalf("recall@10 = %.3f, want >= 0.9", recall)
	}
}

func TestHNSWDeadFilter(t *testing.T) {
	const n, dim = 400, 16
	rng := rand.New(rand.NewSource(11))
	vecs := make([][]float32, n)
	for i := range vecs {
		vecs[i] = normalizeRand(rng, dim)
	}
	h := NewHNSW(Cosine, 16, 200, 13)
	for i, v := range vecs {
		h.Insert(uint32(i), v)
	}
	dead := map[uint32]bool{5: true, 17: true, 100: true}
	got := h.Search(vecs[5], 10, 64, func(id uint32) bool { return !dead[id] })
	for _, g := range got {
		if dead[g.ID] {
			t.Fatalf("dead id %d leaked into results", g.ID)
		}
	}
	if got := h.Search(vecs[0], 0, 64, nil); got != nil {
		t.Fatal("k=0 must return nil")
	}
}

func TestHNSWDeterminism(t *testing.T) {
	build := func() []Hit {
		rng := rand.New(rand.NewSource(3))
		h := NewHNSW(Cosine, 16, 200, 5)
		for i := 0; i < 200; i++ {
			h.Insert(uint32(i), normalizeRand(rng, 8))
		}
		return h.Search(normalizeRand(rand.New(rand.NewSource(9)), 8), 10, 64, nil)
	}
	a, b := build(), build()
	if len(a) != len(b) {
		t.Fatalf("same seed different lengths: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i].ID != b[i].ID || a[i].Score != b[i].Score {
			t.Fatalf("same seed different results at %d", i)
		}
	}
}

func TestHNSWEmpty(t *testing.T) {
	h := NewHNSW(L2, 0, 0, 1) // defaults kick in
	if got := h.Search([]float32{1}, 5, 10, nil); got != nil {
		t.Fatal("empty index must return nil")
	}
}

// normalizeRand draws a uniform vector and normalizes it.
func normalizeRand(rng *rand.Rand, dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = rng.Float32()*2 - 1
	}
	return Normalize(v)
}

// bruteTopIDs is replaced by bench_test.go's version in Task 5.
func bruteTopIDs(vecs [][]float32, q []float32, k int) []int {
	type idd struct {
		i int
		d float64
	}
	qn := Normalize(q)
	all := make([]idd, len(vecs))
	for i, v := range vecs {
		all[i] = idd{i, Distance(Cosine, qn, v)}
	}
	for i := 0; i < k; i++ { // selection sort: fine for unit sizes
		for j := i + 1; j < len(all); j++ {
			if all[j].d < all[i].d {
				all[i], all[j] = all[j], all[i]
			}
		}
	}
	out := make([]int, k)
	for i := range out {
		out[i] = all[i].i
	}
	return out
}
