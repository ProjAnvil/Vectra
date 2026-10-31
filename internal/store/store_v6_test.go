package store_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/projanvil/vectra/internal/store"
)

// v6 was a VERSION-ONLY bump over v5 (retained-WAL checkpoint tripwire,
// plan 2026-10-22): the stamp moved to 6, the bytes did not. v7 (2026-10-26
// FTS plan) appended the ftsKey section and v8 (2026-10-30 unique-constraints
// plan) the unique-constraint section, moving the tripwire forward each
// time; this file now pins the older-format read lines that must survive
// under the v8 writer. The written-version and one-past-the-product
// tripwire tests moved forward with the stamp: store_v8_test.go.

// resealAsV5 turns this binary's v8 Encode output into a genuine v5 file:
// v5 is v8 minus the appended ftsKey and unique-constraint sections (v6 was
// only the stamp), so cutting exactly those section bytes, patching the
// version back to 5 and re-sealing the CRC yields what a pre-v6 encoder
// wrote (see restampDownlevel in store_test.go — version-byte patching
// alone cannot).
func resealAsV5(t *testing.T, b []byte, ftsKeys []string) []byte {
	t.Helper()
	return restampDownlevel(t, b, ftsKeys, nil, store.VersionV5)
}

// TestV5StillReadableUnderV6: files written by every v5-era binary keep
// decoding after the bumps — the tripwire only fences OLD readers, never
// this one against its predecessors.
func TestV5StillReadableUnderV6(t *testing.T) {
	in := store.Data{
		Nodes: []store.Node{
			{Key: "a", Label: "mem", Props: map[string]any{"embedding": []float32{0.1, 0.2, 0.3}, "team": "core"}},
			{Key: "b", Label: "mem", Props: map[string]any{"embedding": []any{float64(0.4), float64(0.5), float64(0.6)}}},
		},
		Edges:       []store.Edge{{SrcKey: "a", DstKey: "b", Label: "NEXT", Props: map[string]any{"w": float64(2)}}},
		IndexKeys:   []string{"team"},
		VectorDecls: []store.VectorIndexDecl{{Name: "mem", Prop: "embedding", Dim: 3, Metric: "cosine", M: 16, EfConstruction: 200}},
		FTSKeys:     []string{"ftsTitle"}, // sizes the v7-only section that resealAsV5 strips
	}
	var buf bytes.Buffer
	if err := store.Encode(&buf, in); err != nil {
		t.Fatal(err)
	}
	v5 := resealAsV5(t, buf.Bytes(), in.FTSKeys)
	if v, err := store.ProbeVersion(bytes.NewReader(v5)); err != nil || v != store.VersionV5 {
		t.Fatalf("probe downgraded file = %d, %v; want %d, nil", v, err, store.VersionV5)
	}
	out, err := store.Decode(bytes.NewReader(v5))
	if err != nil {
		t.Fatalf("v5 snapshot rejected under v8 writer: %v", err)
	}
	if len(out.Nodes) != 2 || len(out.Edges) != 1 || len(out.IndexKeys) != 1 || len(out.VectorDecls) != 1 {
		t.Fatalf("v5 counts: %+v", out)
	}
	if out.FTSKeys != nil {
		t.Fatalf("v5 file decodes FTSKeys = %q, want nil (v5 has no ftsKey section)", out.FTSKeys)
	}
	want := []float32{0.1, 0.2, 0.3}
	if got, ok := out.Nodes[0].Props["embedding"].([]float32); !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("v5 embedding = %#v, want %v", out.Nodes[0].Props["embedding"], want)
	}
}
