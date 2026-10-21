package vecbin

import (
	"encoding/json"
	"maps"
	"reflect"
	"slices"
	"testing"
)

// wireRoundTrip marshals props and decodes them back, reproducing what a
// hoisted map looks like after crossing the WAL/snapshot JSON channel
// (numbers become float64, arrays become []any).
func wireRoundTrip(t *testing.T, props map[string]any) map[string]any {
	t.Helper()
	b, err := json.Marshal(props)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return out
}

// refOffDim asserts v is a single-key {"$vec":[off,dim]} ref and returns it.
func refOffDim(t *testing.T, v any) (off, dim float64) {
	t.Helper()
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("ref is %T, want map[string]any", v)
	}
	if len(m) != 1 {
		t.Fatalf("ref has %d keys, want exactly 1", len(m))
	}
	a, ok := m["$vec"].([]any)
	if !ok {
		t.Fatalf("$vec payload is %T, want []any", m["$vec"])
	}
	if len(a) != 2 {
		t.Fatalf("$vec payload has %d elements, want 2", len(a))
	}
	off, ok = a[0].(float64)
	if !ok {
		t.Fatalf("$vec off is %T, want float64", a[0])
	}
	dim, ok = a[1].(float64)
	if !ok {
		t.Fatalf("$vec dim is %T, want float64", a[1])
	}
	return off, dim
}

func TestHoistRestoreRoundTrip(t *testing.T) {
	decls := PropDecls{"emb": 3, "prof": 2}
	props := map[string]any{
		"name":  "svc-a",
		"count": float64(7),
		// Go-native vector source.
		"emb": []float32{1, 2, 3},
		// JSON-pipeline source: []any(float64), including 0.1 which is not
		// exactly representable, proving float64→float32 collapses on append.
		"prof": []any{float64(4.5), float64(0.1)},
		// Undeclared numeric array: data, not a vector — must stay JSON.
		"raw": []any{float64(9), float64(8)},
		// Undeclared mixed array: not a numeric array at all.
		"tags": []any{"a", float64(1)},
	}
	before := maps.Clone(props)

	var sec []float32
	hoisted, anyHoisted := Hoist(props, decls, &sec)
	if !anyHoisted {
		t.Fatal("Hoist reported nothing hoisted")
	}
	if !reflect.DeepEqual(props, before) {
		t.Fatalf("Hoist mutated input: %v", props)
	}

	// Section holds exactly the five vector floats, float32-truncated.
	if len(sec) != 5 {
		t.Fatalf("section len = %d, want 5", len(sec))
	}
	sorted := slices.Clone(sec)
	slices.Sort(sorted)
	if want := []float32{float32(0.1), 1, 2, 3, float32(4.5)}; !reflect.DeepEqual(sorted, want) {
		t.Fatalf("section = %v, want %v", sorted, want)
	}

	// Both refs are well-shaped and tile the section exactly once.
	type ref struct{ off, dim float64 }
	refs := []ref{}
	for _, k := range []string{"emb", "prof"} {
		off, dim := refOffDim(t, hoisted[k])
		refs = append(refs, ref{off, dim})
	}
	slices.SortFunc(refs, func(a, b ref) int {
		switch {
		case a.off < b.off:
			return -1
		default:
			return 1
		}
	})
	if refs[0].off != 0 || refs[1].off != refs[0].dim || refs[0].dim+refs[1].dim != 5 {
		t.Fatalf("refs %+v do not tile section of len 5", refs)
	}
	if !((refs[0].dim == 3 && refs[1].dim == 2) || (refs[0].dim == 2 && refs[1].dim == 3)) {
		t.Fatalf("ref dims = %+v, want {3,2} in some order", refs)
	}

	// Non-vector props pass through untouched.
	for k, want := range map[string]any{
		"name": "svc-a", "count": float64(7),
		"raw":  []any{float64(9), float64(8)},
		"tags": []any{"a", float64(1)},
	} {
		if got := hoisted[k]; !reflect.DeepEqual(got, want) {
			t.Fatalf("hoisted[%q] = %#v, want %#v", k, got, want)
		}
	}

	want := map[string]any{
		"name": "svc-a", "count": float64(7),
		"emb":  []float32{1, 2, 3},
		"prof": []float32{float32(4.5), float32(0.1)},
		"raw":  []any{float64(9), float64(8)},
		"tags": []any{"a", float64(1)},
	}

	// Hoist's own refs restore directly (gob/raft path: no JSON crossing).
	direct, err := Restore(hoisted, decls, sec)
	if err != nil {
		t.Fatalf("direct Restore: %v", err)
	}
	if !reflect.DeepEqual(direct, want) {
		t.Fatalf("direct restore = %#v, want %#v", direct, want)
	}

	// And after crossing the JSON wire (WAL/snapshot path).
	viaWire, err := Restore(wireRoundTrip(t, hoisted), decls, sec)
	if err != nil {
		t.Fatalf("wire Restore: %v", err)
	}
	if !reflect.DeepEqual(viaWire, want) {
		t.Fatalf("wire restore = %#v, want %#v", viaWire, want)
	}

	// Empty decls: nothing is a vector, copy is faithful, section untouched.
	var empty []float32
	copyProps, anyHoisted := Hoist(props, PropDecls{}, &empty)
	if anyHoisted {
		t.Fatal("Hoist with empty decls reported a hoist")
	}
	if !reflect.DeepEqual(copyProps, props) || len(empty) != 0 {
		t.Fatalf("decls-free hoist = %#v (sec %v)", copyProps, empty)
	}
}

func TestRestoreRefOutOfBand(t *testing.T) {
	sec := []float32{0, 1, 2, 3}
	decls := PropDecls{"emb": 2}
	for _, tt := range []struct {
		name string
		ref  map[string]any
	}{
		{"off at end", map[string]any{"$vec": []any{float64(4), float64(1)}}},
		{"off past end", map[string]any{"$vec": []any{float64(9), float64(1)}}},
		{"dim overruns", map[string]any{"$vec": []any{float64(2), float64(3)}}},
		{"negative off", map[string]any{"$vec": []any{float64(-1), float64(2)}}},
		{"negative dim", map[string]any{"$vec": []any{float64(0), float64(-2)}}},
		{"fractional off", map[string]any{"$vec": []any{float64(1.5), float64(2)}}},
		{"one-element payload", map[string]any{"$vec": []any{float64(0)}}},
		{"non-numeric payload", map[string]any{"$vec": []any{"0", float64(2)}}},
		{"payload not an array", map[string]any{"$vec": "0,2"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Restore(map[string]any{"emb": tt.ref}, decls, sec); err == nil {
				t.Fatalf("out-of-band ref %v restored without error", tt.ref)
			}
		})
	}

	// Boundary refs that do fit restore the exact slice.
	got, err := Restore(map[string]any{"emb": map[string]any{"$vec": []any{float64(1), float64(2)}}}, decls, sec)
	if err != nil {
		t.Fatalf("in-range ref: %v", err)
	}
	if want := []float32{1, 2}; !reflect.DeepEqual(got["emb"], want) {
		t.Fatalf("restored = %v, want %v", got["emb"], want)
	}
}

func TestRestoreLiteralDollarVecOnUndeclared(t *testing.T) {
	decls := PropDecls{"emb": 2}
	props := map[string]any{
		// User literal identical in shape to a ref: no reserved-word conflict.
		"meta": map[string]any{"$vec": []any{float64(999), float64(999)}},
		"emb":  []float32{1, 2},
	}

	// Restore passes the literal through verbatim, pre- and post-wire.
	for name, in := range map[string]any{"direct": props, "wire": wireRoundTrip(t, props)} {
		got, err := Restore(in.(map[string]any), decls, []float32{5, 6})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		want := in.(map[string]any)["meta"]
		if !reflect.DeepEqual(got["meta"], want) {
			t.Fatalf("%s: meta = %#v, want verbatim %#v", name, got["meta"], want)
		}
	}

	// Hoist hoists the declared prop but leaves the literal alone: a map is
	// not a numeric array, declared or not.
	var sec []float32
	hoisted, anyHoisted := Hoist(props, decls, &sec)
	if !anyHoisted || !reflect.DeepEqual(sec, []float32{1, 2}) {
		t.Fatalf("declared emb not hoisted (hoisted=%v sec=%v)", anyHoisted, sec)
	}
	if !reflect.DeepEqual(hoisted["meta"], props["meta"]) {
		t.Fatalf("hoisted meta = %#v, want verbatim %#v", hoisted["meta"], props["meta"])
	}
}

func TestNumericArrayRejectsMixed(t *testing.T) {
	for _, tt := range []struct {
		name string
		v    any
		dim  int
		ok   bool
	}{
		{"float32 slice", []float32{1, 2}, 2, true},
		{"float64 elements", []any{float64(1), float64(2)}, 2, true},
		{"int elements", []any{int(1), int(2)}, 2, true},
		{"mixed numeric kinds", []any{float64(1), float32(2), int(3)}, 3, true},
		{"empty any slice", []any{}, 0, true},
		{"string element", []any{float64(1), "x"}, 0, false},
		{"bool element", []any{true, float64(1)}, 0, false},
		{"nil element", []any{float64(1), nil}, 0, false},
		// Only the two wire shapes count ([]float32, []any); typed slices
		// from other decoders stay on the JSON channel.
		{"float64 slice", []float64{1, 2}, 0, false},
		{"int slice", []int{1}, 0, false},
		{"scalar", float64(3), 0, false},
		{"string", "abc", 0, false},
		{"map", map[string]any{}, 0, false},
		{"nil", nil, 0, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dim, ok := NumericArray(tt.v)
			if ok != tt.ok || dim != tt.dim {
				t.Fatalf("NumericArray(%#v) = (%d, %v), want (%d, %v)", tt.v, dim, ok, tt.dim, tt.ok)
			}
		})
	}
}
