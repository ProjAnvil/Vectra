package vector

import "math"

// AdaptiveEf returns the default search width for an index of n vectors
// (spec §4.3/§13-3): ef = max(64, c*log2(n)), floored at k.
func AdaptiveEf(k, n int, c float64) int {
	ef := 64
	if n > 1 {
		if v := int(c * math.Log2(float64(n))); v > ef {
			ef = v
		}
	}
	if ef < k {
		ef = k
	}
	return ef
}

// DefaultC is the adaptive-ef constant locked from the Phase 0 recall-ef
// sweep (Task 6). 0 means "not locked yet"; callers must treat 0 as
// "use plain 64".
var DefaultC float64
