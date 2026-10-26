package store_test

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"strings"
	"testing"

	"github.com/projanvil/vectra/internal/store"
)

// v6 was a VERSION-ONLY bump over v5 (retained-WAL checkpoint tripwire,
// plan 2026-10-22): the stamp moved to 6, the bytes did not. v7 (2026-10-26
// FTS plan) appends the ftsKey section after the float32 section and moves
// the tripwire one version forward; this file now pins the written version
// plus the older-format read lines that must survive under the v7 writer.

// TestV7IsWrittenVersion: Encode stamps 7 on the wire and the product still
// round-trips through Decode (the dispatch must accept what Encode writes).
func TestV7IsWrittenVersion(t *testing.T) {
	in := store.Data{
		Nodes:       []store.Node{{Key: "a", Label: "mem", Props: map[string]any{"embedding": []float32{0.1, 0.2}, "team": "core"}}},
		Edges:       []store.Edge{{SrcKey: "a", DstKey: "a", Label: "SELF"}},
		IndexKeys:   []string{"team"},
		VectorDecls: []store.VectorIndexDecl{{Name: "mem", Prop: "embedding", Dim: 2, Metric: "cosine"}},
		FTSKeys:     []string{"team"},
	}
	var buf bytes.Buffer
	if err := store.Encode(&buf, in); err != nil {
		t.Fatal(err)
	}
	if got := binary.LittleEndian.Uint16(buf.Bytes()[4:6]); got != store.VersionV7 {
		t.Fatalf("written version = %d, want %d", got, store.VersionV7)
	}
	v, err := store.ProbeVersion(bytes.NewReader(buf.Bytes()))
	if err != nil || v != store.VersionV7 {
		t.Fatalf("probe = %d, %v; want %d, nil", v, err, store.VersionV7)
	}
	out, err := store.Decode(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("v7 product must decode: %v", err)
	}
	if !reflect.DeepEqual(out.Nodes[0].Props["team"], in.Nodes[0].Props["team"]) || len(out.IndexKeys) != 1 || len(out.VectorDecls) != 1 || len(out.FTSKeys) != 1 {
		t.Fatalf("v7 round-trip = %+v", out)
	}
	if got, ok := out.Nodes[0].Props["embedding"].([]float32); !ok || !reflect.DeepEqual(got, []float32{0.1, 0.2}) {
		t.Fatalf("v7 vector restore = %#v", out.Nodes[0].Props["embedding"])
	}
}

// resealAsV5 turns this binary's v7 Encode output into a genuine v5 file:
// v5 is v7 minus the appended ftsKey section (v6 was only the stamp), so
// cutting exactly the section bytes, patching the version back to 5 and
// re-sealing the CRC yields what a pre-v6 encoder wrote (see
// restampDownlevel in store_test.go — version-byte patching alone cannot).
func resealAsV5(t *testing.T, b []byte, ftsKeys []string) []byte {
	t.Helper()
	return restampDownlevel(t, b, ftsKeys, store.VersionV5)
}

// TestV5StillReadableUnderV6: files written by every v5-era binary keep
// decoding after the bump — the tripwire only fences OLD readers, never this
// one against its predecessors.
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
		t.Fatalf("v5 snapshot rejected under v7 writer: %v", err)
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

// TestV8Rejected: a hypothetical v8 header hard-fails with the dispatch error
// — this binary never misreads a newer layout, the same contract the v7
// tripwire imposes on v0.12.0 binaries.
func TestV8Rejected(t *testing.T) {
	var buf bytes.Buffer
	if err := store.Encode(&buf, store.Data{}); err != nil {
		t.Fatal(err)
	}
	forged := append([]byte(nil), buf.Bytes()...)
	binary.LittleEndian.PutUint16(forged[4:6], store.VersionV7+1)
	// ProbeVersion reports the stamp without validating; Decode is the gate.
	if v, err := store.ProbeVersion(bytes.NewReader(forged)); err != nil || v != store.VersionV7+1 {
		t.Fatalf("probe v8 = %d, %v; want %d, nil", v, err, store.VersionV7+1)
	}
	_, err := store.Decode(bytes.NewReader(forged))
	if err == nil || !strings.Contains(err.Error(), "unsupported version") {
		t.Fatalf("err = %v, want unsupported version", err)
	}
}
