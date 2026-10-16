package bench

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunWorkloadCountsAndLatency(t *testing.T) {
	// Op is invoked from concurrent workers; the counter must be atomic
	// (RunWorkload gives no serialization guarantees to the closure).
	var i atomic.Int64
	res := RunWorkload("probe", func() error {
		n := i.Add(1)
		if n%3 == 0 {
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

func TestRunMixedSeparatesReadsAndWrites(t *testing.T) {
	var reads, writes atomic.Int64
	rd, wr := RunMixed("probe",
		func() error { reads.Add(1); return nil },
		func() error { writes.Add(1); return errors.New("werr") },
		4, 400*time.Millisecond, 50)
	if rd.Ops != int(reads.Load()) {
		t.Errorf("read ops = %d, counter = %d", rd.Ops, reads.Load())
	}
	if wr.Ops != int(writes.Load()) {
		t.Errorf("write ops = %d, counter = %d", wr.Ops, writes.Load())
	}
	if wr.Errors != wr.Ops {
		t.Errorf("write errors = %d, ops = %d", wr.Errors, wr.Ops)
	}
	if rd.Errors != 0 {
		t.Errorf("read errors = %d", rd.Errors)
	}
	// 50 ops/s paced over 400ms → ~20 ops; tolerate timer slack both ways
	if wr.Ops < 5 || wr.Ops > 60 {
		t.Errorf("write ops = %d outside pacing window", wr.Ops)
	}
	if rd.QPS <= 0 || wr.QPS <= 0 {
		t.Errorf("QPS read=%f write=%f", rd.QPS, wr.QPS)
	}
}

func TestRunMixedZeroWriteRateDisablesWriter(t *testing.T) {
	var writes atomic.Int64
	rd, wr := RunMixed("probe",
		func() error { return nil },
		func() error { writes.Add(1); return nil },
		2, 200*time.Millisecond, 0)
	if writes.Load() != 0 || wr.Ops != 0 {
		t.Errorf("writer ran %d ops despite writeRate=0", wr.Ops)
	}
	if rd.Ops < 2 {
		t.Errorf("read ops = %d", rd.Ops)
	}
	if wr.P50 != 0 || wr.N != 0 {
		t.Errorf("write summary should be zero, got N=%d P50=%v", wr.N, wr.P50)
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
