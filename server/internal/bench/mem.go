package bench

import (
	"fmt"
	"runtime"
	"time"
)

// MemSnapshot is the Go runtime memory state at one phase boundary. Graph-DB
// vendors report process RSS; for an embedded Go engine the honest
// equivalents are heap occupancy and GC cost, so those are what the stress
// report records (Sys is retained as the OS-allocation upper bound).
type MemSnapshot struct {
	HeapAllocBytes uint64
	HeapSysBytes   uint64
	NumGC          uint32
	GCPauseTotal   time.Duration
	GCCPUFraction  float64
}

// SampleMem stops the world briefly (runtime.ReadMemStats). Call it at phase
// boundaries only, never inside a measured window.
func SampleMem() MemSnapshot {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return MemSnapshot{
		HeapAllocBytes: m.HeapAlloc,
		HeapSysBytes:   m.HeapSys,
		NumGC:          m.NumGC,
		GCPauseTotal:   time.Duration(m.PauseTotalNs),
		GCCPUFraction:  m.GCCPUFraction,
	}
}

// FormatBytes renders a byte count with a binary prefix.
func FormatBytes(b uint64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.2fGiB", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1fMiB", float64(b)/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1fKiB", float64(b)/(1<<10))
	default:
		return fmt.Sprintf("%dB", b)
	}
}
