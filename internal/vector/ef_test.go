package vector

import "testing"

func TestAdaptiveEf(t *testing.T) {
	if got := AdaptiveEf(10, 1000, 0.5); got != 64 { // below floor
		t.Fatalf("floor = %d, want 64", got)
	}
	if got := AdaptiveEf(100, 10, 8); got != 100 { // ef >= k
		t.Fatalf("k floor = %d, want 100", got)
	}
	if got := AdaptiveEf(10, 1, 8); got != 64 { // degenerate n
		t.Fatalf("n=1 = %d, want 64", got)
	}
	if got := AdaptiveEf(10, 1<<20, 8); got != 160 { // log2(2^20) = 20 exact
		t.Fatalf("c8@1M = %d, want 160", got)
	}
}
