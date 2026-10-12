package bench

import (
	"testing"
	"time"
)

func TestPercentileNearestRank(t *testing.T) {
	durs := make([]time.Duration, 100)
	for i := range durs {
		durs[i] = time.Duration(i+1) * time.Millisecond // 1ms..100ms
	}
	if got := Percentile(durs, 0.5); got != 50*time.Millisecond {
		t.Errorf("p50 = %v, want 50ms", got)
	}
	if got := Percentile(durs, 0.90); got != 90*time.Millisecond {
		t.Errorf("p90 = %v, want 90ms", got)
	}
	if got := Percentile(durs, 0.99); got != 99*time.Millisecond {
		t.Errorf("p99 = %v, want 99ms", got)
	}
	if got := Percentile(durs, 1.0); got != 100*time.Millisecond {
		t.Errorf("max = %v, want 100ms", got)
	}
}

func TestPercentileEmptyAndSingle(t *testing.T) {
	if got := Percentile(nil, 0.5); got != 0 {
		t.Errorf("空样本 = %v, want 0", got)
	}
	if got := Percentile([]time.Duration{7 * time.Millisecond}, 0.99); got != 7*time.Millisecond {
		t.Errorf("单样本 = %v, want 7ms", got)
	}
}

func TestPercentileDoesNotMutateInput(t *testing.T) {
	durs := []time.Duration{3 * time.Millisecond, 1 * time.Millisecond, 2 * time.Millisecond}
	_ = Percentile(durs, 0.5)
	if durs[0] != 3*time.Millisecond || durs[1] != 1*time.Millisecond || durs[2] != 2*time.Millisecond {
		t.Error("Percentile 不应修改输入切片")
	}
}

func TestSummarize(t *testing.T) {
	s := Summarize([]time.Duration{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	if s.N != 10 || s.P50 != 5 || s.P99 != 10 {
		t.Errorf("Summarize = %+v", s)
	}
}
