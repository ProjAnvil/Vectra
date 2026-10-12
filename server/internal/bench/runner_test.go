package bench

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRunWorkloadCountsAndLatency(t *testing.T) {
	i := 0
	res := RunWorkload("probe", func() error {
		i++
		if i%3 == 0 {
			return errors.New("boom")
		}
		time.Sleep(200 * time.Microsecond)
		return nil
	}, 4, 300*time.Millisecond)
	if res.Ops < 4 { // 4 workers × at least a few dozen ops
		t.Fatalf("ops = %d too few", res.Ops)
	}
	if res.Errors == 0 {
		t.Error("errors not counted")
	}
	if res.P50 < 100*time.Microsecond || res.P50 > 10*time.Millisecond {
		t.Errorf("p50 = %v abnormal", res.P50)
	}
	if res.QPS <= 0 {
		t.Errorf("QPS = %f", res.QPS)
	}
}

func TestMarkdownTable(t *testing.T) {
	out := MarkdownTable([]string{"a", "b"}, [][]string{{"1", "2"}, {"3", "4"}})
	want := "| a | b |\n|---|---|\n| 1 | 2 |\n| 3 | 4 |\n"
	if out != want {
		t.Errorf("table = %q, want %q", out, want)
	}
}

func TestFormatDur(t *testing.T) {
	if FormatDur(1500*time.Microsecond) != "1.50ms" {
		t.Error(FormatDur(1500 * time.Microsecond))
	}
	if !strings.HasSuffix(FormatDur(150*time.Microsecond), "µs") {
		t.Error(FormatDur(150 * time.Microsecond))
	}
	if FormatDur(500*time.Nanosecond) != "500ns" {
		t.Error(FormatDur(500 * time.Nanosecond))
	}
}
