// Package propval holds the property-value helpers shared by every state
// container that touches node/edge properties (the root facade, internal/delta
// layers, internal/engine indexes): the defensive shallow copy and the numeric
// normalization to float64. One home so the type switches cannot drift — the
// same numeric kind must convert identically on the index path and the lookup
// path, or a value written as one width stops matching itself after a seal
// (audit F-18).
package propval

import "encoding/json"

// Copy shallow-copies a property map. nil stays nil and non-nil maps are
// copied shallow — nil and empty are distinct-but-equivalent. Values are
// shared, not cloned: property maps follow the "replace, don't mutate"
// discipline, so the copy needs to protect only the map's own shape.
func Copy(props map[string]any) map[string]any {
	if props == nil {
		return nil
	}
	m := make(map[string]any, len(props))
	for k, v := range props {
		m[k] = v
	}
	return m
}

// ToFloat normalizes any numeric Go value to float64 — all int/uint/float
// widths, plus json.Number (values reach properties as json.Number when a
// caller decodes JSON with UseNumber and hands the map straight to an
// AddNode-class API). Non-numerics return ok=false. This is the engine
// index's admission rule and the root lookup paths' match rule; the two
// must agree arm for arm.
func ToFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	}
	return 0, false
}
