// Package vector implements Vectra's embedded approximate nearest-neighbor
// index: an HNSW engine, a tiered segment state, and the JSON-safe vector
// normalization layer (spec 2026-10-19, Phase 0 spike).
package vector

import "math"

// Metric is the distance metric of one vector index. Smaller is always
// better: Dot is negated so all three metrics share one ordering.
type Metric uint8

const (
	Cosine Metric = iota
	L2
	Dot
)

// Distance computes the metric between two equal-length vectors. Cosine
// assumes pre-normalized inputs and returns 1 - dot. L2 returns the
// squared euclidean distance (monotone in true L2, cheaper).
//
// The kernels carry eight independent accumulators: the Go compiler does
// not auto-vectorize float loops (strict FP semantics), and a single
// accumulator serializes on the ~4-cycle FADD latency — eight chains keep
// the multiplier/adder pipes busy instead (profile 2026-10-20: Distance
// was 88% of build CPU in the chained form). Summation order differs from
// the naive loop, shifting results by last-ulp amounts, far below ANN
// margins.
func Distance(m Metric, a, b []float32) float64 {
	if m == L2 {
		return l2Squared(a, b)
	}
	// float32 accumulation: ~2-3x faster than float64 on this hot path;
	// the ~1e-5 relative error over <=4096 dims is far below ANN margins.
	dot := dotProduct(a, b)
	if m == Cosine {
		return 1 - float64(dot)
	}
	return -float64(dot)
}

func dotProduct(a, b []float32) float32 {
	var s0, s1, s2, s3, s4, s5, s6, s7 float32
	i, n := 0, len(a)&^7
	for ; i < n; i += 8 {
		av := a[i : i+8 : i+8]
		bv := b[i : i+8 : i+8]
		s0 += av[0] * bv[0]
		s1 += av[1] * bv[1]
		s2 += av[2] * bv[2]
		s3 += av[3] * bv[3]
		s4 += av[4] * bv[4]
		s5 += av[5] * bv[5]
		s6 += av[6] * bv[6]
		s7 += av[7] * bv[7]
	}
	dot := ((s0+s4)+(s1+s5))+((s2+s6)+(s3+s7))
	var tail float32
	for ; i < len(a); i++ {
		tail += a[i] * b[i]
	}
	return dot + tail
}

func l2Squared(a, b []float32) float64 {
	var s0, s1, s2, s3 float64
	i, n := 0, len(a)&^3
	for ; i < n; i += 4 {
		t0 := float64(a[i] - b[i])
		t1 := float64(a[i+1] - b[i+1])
		t2 := float64(a[i+2] - b[i+2])
		t3 := float64(a[i+3] - b[i+3])
		s0 += t0 * t0
		s1 += t1 * t1
		s2 += t2 * t2
		s3 += t3 * t3
	}
	sq := (s0+s1)+(s2+s3)
	for ; i < len(a); i++ {
		t := float64(a[i] - b[i])
		sq += t * t
	}
	return sq
}

// Normalize returns a unit-length defensive copy of v. The zero vector is
// returned unchanged (it stays orthogonal to everything; callers treat it
// as ordinary data).
func Normalize(v []float32) []float32 {
	out := make([]float32, len(v))
	var n float64
	for _, x := range v {
		n += float64(x) * float64(x)
	}
	n = math.Sqrt(n)
	if n == 0 {
		copy(out, v)
		return out
	}
	for i, x := range v {
		out[i] = float32(float64(x) / n)
	}
	return out
}

// NormalizeVec coerces a property value into a fresh []float32 of exactly
// dim elements. The WAL and store encode props as JSON, so a []float32
// round-trips as []any of float64; this is the single intake point that
// both paths (and live writes) share. Wrong length, non-numeric elements,
// NaN and Inf return false — the caller skips the node with a warning.
func NormalizeVec(v any, dim int) ([]float32, bool) {
	out := make([]float32, dim)
	switch t := v.(type) {
	case []float32:
		if len(t) != dim {
			return nil, false
		}
		copy(out, t)
	case []float64:
		if len(t) != dim {
			return nil, false
		}
		for i, x := range t {
			out[i] = float32(x)
		}
	case []any:
		if len(t) != dim {
			return nil, false
		}
		for i, x := range t {
			f, ok := x.(float64)
			if !ok {
				return nil, false
			}
			out[i] = float32(f)
		}
	default:
		return nil, false
	}
	for _, x := range out {
		if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
			return nil, false
		}
	}
	return out, true
}
