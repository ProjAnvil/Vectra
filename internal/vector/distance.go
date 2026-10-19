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
func Distance(m Metric, a, b []float32) float64 {
	// float32 accumulation: ~2-3x faster than float64 on this hot path;
	// the ~1e-5 relative error over <=4096 dims is far below ANN margins.
	var dot float32
	for i := range a {
		dot += a[i] * b[i]
	}
	switch m {
	case Cosine:
		return 1 - float64(dot)
	case Dot:
		return -float64(dot)
	default:
		var sq float64
		for i := range a {
			t := float64(a[i] - b[i])
			sq += t * t
		}
		return sq
	}
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
