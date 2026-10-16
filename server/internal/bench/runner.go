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
	return mergeLoad(name, locs, int(errs.Load()), duration)
}

// RunMixed executes readOp from `workers` reader goroutines at full speed while
// a single writer goroutine paces writeOp at writeRate ops/s — one writer
// because the engine serializes writes anyway (single-writer model; the mixed
// number then reports read-side latency under a sustained, paced write load).
// writeRate <= 0 disables the writer. Pacing relies on time.Sleep granularity,
// so rates above ~500/s lose accuracy. Returns the read and write sides
// separately; both QPS are computed over the full duration.
func RunMixed(name string, readOp Op, writeOp Op, workers int, duration time.Duration, writeRate int) (LoadResult, LoadResult) {
	if workers < 1 {
		workers = 1
	}
	deadline := time.Now().Add(duration)
	var stop atomic.Bool
	time.AfterFunc(duration, func() { stop.Store(true) })

	var wg sync.WaitGroup
	var rerrs atomic.Int64
	rlocs := make([][]time.Duration, workers)
	for w := range workers {
		wg.Go(func() {
			local := make([]time.Duration, 0, 4096)
			for !stop.Load() && time.Now().Before(deadline) {
				t0 := time.Now()
				err := readOp()
				d := time.Since(t0)
				local = append(local, d)
				if err != nil {
					rerrs.Add(1)
				}
			}
			rlocs[w] = local
		})
	}

	var wwg sync.WaitGroup
	var werrs atomic.Int64
	var wlocal []time.Duration
	if writeRate > 0 {
		interval := time.Second / time.Duration(writeRate)
		wwg.Go(func() {
			for !stop.Load() && time.Now().Before(deadline) {
				next := time.Now().Add(interval)
				t0 := time.Now()
				err := writeOp()
				d := time.Since(t0)
				wlocal = append(wlocal, d)
				if err != nil {
					werrs.Add(1)
				}
				if sleep := time.Until(next); sleep > 0 {
					time.Sleep(sleep)
				}
			}
		})
	}
	wg.Wait()
	wwg.Wait()
	rd := mergeLoad(name, rlocs, int(rerrs.Load()), duration)
	wr := mergeLoad(name, [][]time.Duration{wlocal}, int(werrs.Load()), duration)
	return rd, wr
}

func mergeLoad(name string, locs [][]time.Duration, errs int, duration time.Duration) LoadResult {
	var all []time.Duration
	total := 0
	for _, l := range locs {
		total += len(l)
		all = append(all, l...)
	}
	res := LoadResult{Name: name, Ops: total, Errors: errs, Summary: Summarize(all)}
	if duration > 0 {
		res.QPS = float64(total) / duration.Seconds()
	}
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
