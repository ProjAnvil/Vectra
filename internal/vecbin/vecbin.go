// Package vecbin implements the value-level vector codec shared by the WAL
// frame format v2 and the store snapshot's contiguous float32 section (the
// v5 layout, still current under the v6 version-only stamp): declared
// vector props leave the JSON props channel and live in that section,
// represented inside props as {"$vec":[off,dim]} refs into it.
//
// Detection is declaration-based, never Go-type-based: a prop is a vector
// candidate only when its name appears in PropDecls AND its value is a
// numeric array in one of the two wire shapes ([]float32, or []any of
// float64/float32/int — snapshot reloads and JSON pipelines deliver
// float64). Undeclared numeric arrays are data, not vectors, and stay JSON.
// "$vec" refs resolve only on declared props; the same literal on an
// undeclared prop is user data and passes through verbatim (no reserved-word
// conflict). An out-of-range or malformed ref on a declared prop is
// corruption and errors.
//
// Dim checks are not this package's job: intake (NormalizeVec) has already
// rejected wrong-dim values before they reach the WAL or a snapshot, so Hoist
// records the dim the value actually has and Restore returns it.
package vecbin

import (
	"fmt"
	"math"
)

// vecRefKey is the props-channel marker for a section reference. It is only
// interpreted on declared props, so user data may use the same shape freely.
const vecRefKey = "$vec"

// PropDecls maps a declared vector prop name to its dimension.
type PropDecls map[string]int

// NumericArray reports v's element count and true when v is []float32 or
// []any whose elements are all float64/float32/int (uniform numeric).
// It allocates nothing: callers use it to pick encodings without paying
// for the conversion.
func NumericArray(v any) (dim int, ok bool) {
	switch t := v.(type) {
	case []float32:
		return len(t), true
	case []any:
		for _, e := range t {
			if _, ok := elemFloat(e); !ok {
				return 0, false
			}
		}
		return len(t), true
	default:
		return 0, false
	}
}

// Hoist copies props, replacing declared vector-prop values that are
// numeric arrays with {"$vec": [off, dim]} refs; the floats append to
// *sec (off is the pre-call len(*sec)). Returns the copy and whether
// anything was hoisted. Input map is never mutated.
func Hoist(props map[string]any, decls PropDecls, sec *[]float32) (map[string]any, bool) {
	out := make(map[string]any, len(props))
	hoisted := false
	for k, v := range props {
		if _, declared := decls[k]; !declared {
			out[k] = v
			continue
		}
		f, ok := numFloats(v)
		if !ok {
			// Declared prop holding non-vector data: not this codec's call.
			out[k] = v
			continue
		}
		// off is the section length before this prop's floats land; each
		// hoisted prop records where its own run begins.
		off := len(*sec)
		*sec = append(*sec, f...)
		// float64 elements keep the ref identical pre- and post-JSON, so
		// the gob (raft) and JSON (WAL/snapshot) channels see one shape.
		out[k] = map[string]any{vecRefKey: []any{float64(off), float64(len(f))}}
		hoisted = true
	}
	return out, hoisted
}

// Restore reverses Hoist: "$vec" refs on declared props resolve against
// sec (bounds-checked, error on violation); everything else passes
// through verbatim - including "$vec"-shaped literals on undeclared
// props.
func Restore(props map[string]any, decls PropDecls, sec []float32) (map[string]any, error) {
	out := make(map[string]any, len(props))
	for k, v := range props {
		if _, declared := decls[k]; declared {
			if vec, isRef, err := resolveRef(v, sec); err != nil {
				return nil, fmt.Errorf("prop %q: %w", k, err)
			} else if isRef {
				out[k] = vec
				continue
			}
		}
		out[k] = v
	}
	return out, nil
}

// resolveRef resolves v against sec when v is a "$vec" ref: a single-key
// map keyed by vecRefKey whose payload decodes to [off,dim]. isRef=false
// means v is not ref-shaped and passes through untouched; a ref that is
// malformed or out of bounds is corruption and errors.
func resolveRef(v any, sec []float32) (vec []float32, isRef bool, err error) {
	m, ok := v.(map[string]any)
	if !ok || len(m) != 1 {
		return nil, false, nil
	}
	payload, has := m[vecRefKey]
	if !has {
		return nil, false, nil
	}
	a, ok := payload.([]any)
	if !ok || len(a) != 2 {
		return nil, true, fmt.Errorf("$vec payload must be [off,dim], got %#v", payload)
	}
	off, err := refInt(a[0])
	if err != nil {
		return nil, true, err
	}
	dim, err := refInt(a[1])
	if err != nil {
		return nil, true, err
	}
	if off < 0 || dim < 0 || off+dim > len(sec) {
		return nil, true, fmt.Errorf("$vec ref [%d,%d] out of range (section len %d)", off, dim, len(sec))
	}
	vec = make([]float32, dim)
	copy(vec, sec[off:off+dim])
	return vec, true, nil
}

// refInt decodes one ref component. Refs cross the wire as JSON numbers,
// so they arrive as float64 and stay exact up to 2^53 section floats;
// fractional or non-numeric values are corruption.
func refInt(e any) (int, error) {
	var f float64
	switch n := e.(type) {
	case float64:
		f = n
	case float32:
		f = float64(n)
	case int:
		return n, nil
	default:
		return 0, fmt.Errorf("$vec offset/dim must be numeric, got %T", e)
	}
	if f != math.Trunc(f) {
		return 0, fmt.Errorf("$vec offset/dim must be integral, got %v", f)
	}
	return int(f), nil
}

// elemFloat is the accepted-element table for []any payloads.
func elemFloat(e any) (f float32, ok bool) {
	switch n := e.(type) {
	case float64:
		return float32(n), true
	case float32:
		return n, true
	case int:
		return float32(n), true
	}
	return 0, false
}

// numFloats widens a numeric array to its float32 elements — the single
// point where float64/int converge. The []float32 result aliases v for the
// []float32 case; callers must only read it.
func numFloats(v any) ([]float32, bool) {
	switch t := v.(type) {
	case []float32:
		return t, true
	case []any:
		out := make([]float32, len(t))
		for i, e := range t {
			f, ok := elemFloat(e)
			if !ok {
				return nil, false
			}
			out[i] = f
		}
		return out, true
	default:
		return nil, false
	}
}
