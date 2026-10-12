package bench

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Op 是一次负载操作。实现方负责参数轮换（闭包内原子计数）。
type Op func() error

// LoadResult 是一个负载的完整压测结果。
type LoadResult struct {
	Name   string
	Ops    int
	Errors int
	Summary
	QPS float64
}

// RunWorkload 并发执行 op：workers 个 goroutine 跑满 duration。
// 延迟样本按 worker 本地缓冲记录、事后合并——避免共享锁污染测量。
func RunWorkload(name string, op Op, workers int, duration time.Duration) LoadResult {
	if workers < 1 {
		workers = 1
	}
	deadline := time.Now().Add(duration)
	var wg sync.WaitGroup
	var errs atomic.Int64
	locs := make([][]time.Duration, workers)
	var stop atomic.Bool
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
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
		}(w)
	}
	// 到点后停（duration 已由 deadline 判定，此处保证退出）
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

// MarkdownTable 渲染对齐的 markdown 表（压测报告用）。
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

// FormatDur 把延迟渲染成人类可读的 µs/ms。
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
