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
// sweep: the 0.95 recall line is already crossed at ef=32 on 100k x 768
// (in-distribution protocol), c = 32/log2(100k) ~= 1.93 -> 2. The 64
// floor dominates for every n this side of 2^32.
var DefaultC float64 = 2
