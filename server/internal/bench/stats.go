package bench

import (
	"math"
	"slices"
	"time"
)

// Percentile computes a percentile via the nearest-rank method, without
// modifying the input. Empty samples return 0.
func Percentile(durs []time.Duration, p float64) time.Duration {
	if len(durs) == 0 {
		return 0
	}
	sorted := slices.Clone(durs)
	slices.Sort(sorted)
	return percentileOf(sorted, p)
}

func percentileOf(sorted []time.Duration, p float64) time.Duration {
	// Nearest rank: the ceil(p*N)-th sample (1-based)
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	idx = min(max(idx, 0), len(sorted)-1)
	return sorted[idx]
}

// Summary holds the statistics of a set of latency samples.
type Summary struct {
	N    int
	P50  time.Duration
	P90  time.Duration
	P99  time.Duration
	Max  time.Duration
	Mean time.Duration
}

func Summarize(durs []time.Duration) Summary {
	if len(durs) == 0 {
		return Summary{}
	}
	sorted := slices.Clone(durs)
	slices.Sort(sorted)
	var total time.Duration
	for _, d := range durs {
		total += d
	}
	return Summary{
		N:    len(durs),
		P50:  percentileOf(sorted, 0.50),
		P90:  percentileOf(sorted, 0.90),
		P99:  percentileOf(sorted, 0.99),
		Max:  sorted[len(sorted)-1],
		Mean: total / time.Duration(len(durs)),
	}
}
