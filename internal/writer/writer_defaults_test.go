package writer

// Defaults for the volume-checkpoint trigger. The 64 MiB CheckpointBytes
// default is a measured cadence ruling (docs/benchmarks/2026-10-21-wrapper-
// checkpoint-cadence.md); these tests pin the default and the pass-through
// of explicit values (negative disables the trigger per Config docs).

import "testing"

func TestCheckpointBytesDefault(t *testing.T) {
	if got := (Config{}).withDefaults().CheckpointBytes; got != 64<<20 {
		t.Fatalf("default CheckpointBytes = %d, want %d (64 MiB)", got, 64<<20)
	}
}

func TestCheckpointBytesExplicit(t *testing.T) {
	tests := []struct {
		name string
		in   int64
	}{
		{"small positive", 8 << 10},
		{"old default", 4 << 20},
		{"large", 1 << 30},
		{"negative disables trigger", -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := (Config{CheckpointBytes: tt.in}).withDefaults().CheckpointBytes; got != tt.in {
				t.Fatalf("explicit CheckpointBytes %d rewritten to %d; explicit values must pass through", tt.in, got)
			}
		})
	}
}
