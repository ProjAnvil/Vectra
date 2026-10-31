package propval

import (
	"encoding/json"
	"testing"
)

func TestCopyNilVsEmpty(t *testing.T) {
	if got := Copy(nil); got != nil {
		t.Fatalf("Copy(nil) = %v, want nil", got)
	}
	// Copy(empty) is empty-but-non-nil: nil and empty stay distinct.
	if got := Copy(map[string]any{}); got == nil || len(got) != 0 {
		t.Fatalf("Copy(empty) = %v, want non-nil empty", got)
	}
	src := map[string]any{"k": "v", "n": 1.0}
	got := Copy(src)
	if len(got) != 2 || got["k"] != "v" || got["n"] != 1.0 {
		t.Fatalf("Copy = %v", got)
	}
	// Shallow: the copy's shape is independent, values are shared.
	src["k"] = "other"
	if got["k"] != "v" {
		t.Fatalf("Copy did not snapshot the map shape: %v", got)
	}
}

func TestToFloatWidthsAndNonNumerics(t *testing.T) {
	nums := []any{
		int(3), int8(3), int16(3), int32(3), int64(3),
		uint(3), uint8(3), uint16(3), uint32(3), uint64(3),
		float32(3), float64(3), json.Number("3"),
	}
	for _, v := range nums {
		if f, ok := ToFloat(v); !ok || f != 3 {
			t.Errorf("ToFloat(%T %v) = %v, %v; want 3, true", v, v, f, ok)
		}
	}
	if f, ok := ToFloat(json.Number("not-a-number")); ok || f != 0 {
		t.Errorf("ToFloat(bad json.Number) = %v, %v; want 0, false", f, ok)
	}
	for _, v := range []any{nil, "3", true, []any{3}} {
		if _, ok := ToFloat(v); ok {
			t.Errorf("ToFloat(%T %v) = ok; want false", v, v)
		}
	}
}
