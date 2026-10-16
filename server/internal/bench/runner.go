package bench

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Op is a single workload operation. The implementer is responsible for
// parameter rotation (atomic counter inside a closure).
type Op func() error

// LoadResult is the complete stress result of one workload.
type LoadResult struct {
	Name   string
	Ops    int
	Errors int
	Summary
	QPS float64
}

// RunWorkload executes op concurrently: workers goroutines run flat out for duration.
// Latency samples are recorded in per-worker local buffers and merged afterwards —
// avoiding shared-lock contamination of measurements.
func RunWorkload(name string, op Op, workers int, duration time.Duration) LoadResult {
	if workers < 1 {
		workers = 1
	}
	deadline := time.Now().Add(duration)
	var wg sync.WaitGroup
	var errs atomic.Int64
	locs := make([][]time.Duration, workers)
	var stop atomic.Bool
	for w := range workers {
		wg.Go(func() {
			local := make([]time.Duration, 0, 4096)
			for !stop.Load() && time.Now().Before(deadline) {
				t0 := time.Now()
				err := op()
				d := time.Since(t0)
				local = append(local, d)
				if err != nil {
					errs.Add(1)
				}
			}
			locs[w] = local
		})
	}
	// Stop at the deadline (duration already judged via deadline; this guarantees exit)
	time.AfterFunc(duration, func() { stop.Store(true) })
	wg.Wait()

	var all []time.Duration
	total := 0
	for _, l := range locs {
		total += len(l)
		all = append(all, l...)
	}
	res := LoadResult{Name: name, Ops: total, Errors: int(errs.Load()), Summary: Summarize(all)}
	res.QPS = float64(total) / duration.Seconds()
	return res
}

// MarkdownTable renders an aligned markdown table (for stress reports).
func MarkdownTable(header []string, rows [][]string) string {
	var b strings.Builder
	b.WriteString("| ")
	b.WriteString(strings.Join(header, " | "))
	b.WriteString(" |\n|")
	for range header {
		b.WriteString("---|")
	}
	b.WriteString("\n")
	for _, r := range rows {
		b.WriteString("| ")
		b.WriteString(strings.Join(r, " | "))
		b.WriteString(" |\n")
	}
	return b.String()
}

// FormatDur renders a latency as human-readable µs/ms.
func FormatDur(d time.Duration) string {
	switch {
	case d >= time.Millisecond:
		return fmt.Sprintf("%.2fms", float64(d)/float64(time.Millisecond))
	case d >= time.Microsecond:
		return fmt.Sprintf("%.1fµs", float64(d)/float64(time.Microsecond))
	default:
		return fmt.Sprintf("%dns", d)
	}
}
