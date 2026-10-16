package bench

import (
	"testing"
)

func TestSampleMem(t *testing.T) {
	s := SampleMem()
	if s.HeapAllocBytes == 0 {
		t.Error("HeapAllocBytes = 0")
	}
	if s.HeapSysBytes < s.HeapAllocBytes {
		t.Errorf("HeapSys %d < HeapAlloc %d", s.HeapSysBytes, s.HeapAllocBytes)
	}
	// An active test binary has usually forced at least one GC.
	if s.GCPauseTotal < 0 {
		t.Errorf("GCPauseTotal = %v", s.GCPauseTotal)
	}
}

func TestFormatBytes(t *testing.T) {
	cases := []struct {
		in   uint64
		want string
	}{
		{512, "512B"},
		{2048, "2.0KiB"},
		{5 << 20, "5.0MiB"},
		{3 << 30, "3.00GiB"},
	}
	for _, c := range cases {
		if got := FormatBytes(c.in); got != c.want {
			t.Errorf("FormatBytes(%d) = %q, want %q", c.in, got, c.want)
		}
	}
}
