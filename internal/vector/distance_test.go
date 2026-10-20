package vector

import (
	"math"
	"math/rand"
	"testing"
)

func TestDistanceKnownValues(t *testing.T) {
	a := []float32{1, 0}
	b := []float32{0, 1}
	if got := Distance(Cosine, a, b); math.Abs(got-1) > 1e-6 {
		t.Fatalf("cosine orthogonal = %v, want 1", got)
	}
	if got := Distance(Cosine, a, a); math.Abs(got) > 1e-6 {
		t.Fatalf("cosine identical = %v, want 0", got)
	}
	if got := Distance(L2, []float32{0, 0}, []float32{3, 4}); math.Abs(got-25) > 1e-4 {
		t.Fatalf("l2 squared = %v, want 25", got)
	}
	if got := Distance(Dot, []float32{2, 1}, []float32{3, 4}); math.Abs(got+10) > 1e-4 {
		t.Fatalf("dot negated = %v, want -10", got)
	}
}

// TestDistanceLongVectors exercises the unrolled kernel body (multiples of
// 8 plus tails) against a float64 reference across several lengths,
// including dims that hit every remainder 0-7. Added with the 8-wide
// accumulator unroll: the known-values test above only covers tails.
func TestDistanceLongVectors(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for _, n := range []int{8, 15, 16, 23, 24, 31, 100, 768} {
		a := make([]float32, n)
		b := make([]float32, n)
		var refDot, refL2 float64
		for i := range a {
			a[i] = rng.Float32() // [-0,1); magnitudes and signs irrelevant
			b[i] = rng.Float32()*2 - 1
			refDot += float64(a[i]) * float64(b[i])
			refL2 += float64(a[i]-b[i]) * float64(a[i]-b[i])
		}
		if got := Distance(Dot, a, b); math.Abs(got+refDot) > 1e-4*float64(n) {
			t.Fatalf("dot n=%d = %v, want ~%v", n, got, -refDot)
		}
		if got := Distance(Cosine, a, b); math.Abs(got-(1-refDot)) > 1e-4*float64(n) {
			t.Fatalf("cosine n=%d = %v, want ~%v", n, got, 1-refDot)
		}
		if got := Distance(L2, a, b); math.Abs(got-refL2) > 1e-3*float64(n) {
			t.Fatalf("l2 n=%d = %v, want ~%v", n, got, refL2)
		}
	}
}

func TestNormalize(t *testing.T) {
	out := Normalize([]float32{3, 4})
	var n float64
	for _, x := range out {
		n += float64(x) * float64(x)
	}
	if math.Abs(n-1) > 1e-6 {
		t.Fatalf("norm = %v, want 1", n)
	}
	src := []float32{3, 4}
	_ = Normalize(src)
	if src[0] != 3 { // defensive copy: source untouched
		t.Fatal("Normalize must not modify its input")
	}
}

func TestNormalizeVec(t *testing.T) {
	direct := []float32{0.5, 0.5, 0.5, 0.5}
	if v, ok := NormalizeVec(direct, 4); !ok || len(v) != 4 || v[0] != 0.5 {
		t.Fatal("[]float32 of matching dim must pass")
	}
	jsonish := []any{float64(0.1), float64(0.2)}
	if v, ok := NormalizeVec(jsonish, 2); !ok || v[1] != 0.2 {
		t.Fatal("[]any of float64 must convert")
	}
	f64 := []float64{0.1, 0.2}
	if _, ok := NormalizeVec(f64, 2); !ok {
		t.Fatal("[]float64 must convert")
	}
	if _, ok := NormalizeVec(direct, 3); ok {
		t.Fatal("wrong dim must fail")
	}
	if _, ok := NormalizeVec([]any{0.1, "x"}, 2); ok {
		t.Fatal("non-numeric element must fail")
	}
	if _, ok := NormalizeVec([]any{0.1, math.NaN()}, 2); ok {
		t.Fatal("NaN must fail")
	}
	if _, ok := NormalizeVec([]any{0.1, math.Inf(1)}, 2); ok {
		t.Fatal("+Inf must fail")
	}
	if _, ok := NormalizeVec("nope", 2); ok {
		t.Fatal("non-slice must fail")
	}
	v, _ := NormalizeVec(direct, 4)
	v[0] = 99
	if direct[0] != 0.5 {
		t.Fatal("NormalizeVec must return a copy")
	}
}
