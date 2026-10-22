package store_test

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"reflect"
	"strings"
	"testing"

	"github.com/projanvil/vectra/internal/store"
)

// v6 is a VERSION-ONLY bump over v5: byte layout identical, written version
// stamp 6. The retained-WAL checkpoint chain (plan 2026-10-22) leaves the base
// snapshot on disk for long stretches while WAL segments carry the increments;
// a v0.11.1 binary pointed at such a library would load the base and MISS the
// retained segments (its reader ignores the manifest v2 fields), silently
// losing data. Bumping the stamp makes those binaries hard-fail on the version
// byte instead — same downgrade line as v4 -> v5.

// TestV6IsWrittenVersion: Encode stamps 6 on the wire and the product still
// round-trips through Decode (the dispatch must accept what Encode writes).
func TestV6IsWrittenVersion(t *testing.T) {
	in := store.Data{
		Nodes:       []store.Node{{Key: "a", Label: "mem", Props: map[string]any{"embedding": []float32{0.1, 0.2}, "team": "core"}}},
		Edges:       []store.Edge{{SrcKey: "a", DstKey: "a", Label: "SELF"}},
		IndexKeys:   []string{"team"},
		VectorDecls: []store.VectorIndexDecl{{Name: "mem", Prop: "embedding", Dim: 2, Metric: "cosine"}},
	}
	var buf bytes.Buffer
	if err := store.Encode(&buf, in); err != nil {
		t.Fatal(err)
	}
	if got := binary.LittleEndian.Uint16(buf.Bytes()[4:6]); got != 6 {
		t.Fatalf("written version = %d, want 6", got)
	}
	v, err := store.ProbeVersion(bytes.NewReader(buf.Bytes()))
	if err != nil || v != 6 {
		t.Fatalf("probe = %d, %v; want 6, nil", v, err)
	}
	out, err := store.Decode(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("v6 product must decode: %v", err)
	}
	if !reflect.DeepEqual(out.Nodes[0].Props["team"], in.Nodes[0].Props["team"]) || len(out.IndexKeys) != 1 || len(out.VectorDecls) != 1 {
		t.Fatalf("v6 round-trip = %+v", out)
	}
	if got, ok := out.Nodes[0].Props["embedding"].([]float32); !ok || !reflect.DeepEqual(got, []float32{0.1, 0.2}) {
		t.Fatalf("v6 vector restore = %#v", out.Nodes[0].Props["embedding"])
	}
}

// resealAsV5 turns this binary's v6 Encode output into a genuine v5 file:
// v6 is a version-only bump (identical layout), so patching the version bytes
// back to 5 and re-sealing the CRC yields exactly what a pre-v6 encoder wrote.
func resealAsV5(b []byte) []byte {
	out := append([]byte(nil), b...)
	binary.LittleEndian.PutUint16(out[4:6], 5)
	want := crc32.ChecksumIEEE(out[:len(out)-4])
	binary.LittleEndian.PutUint32(out[len(out)-4:], want)
	return out
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
	}
	var buf bytes.Buffer
	if err := store.Encode(&buf, in); err != nil {
		t.Fatal(err)
	}
	v5 := resealAsV5(buf.Bytes())
	if v, err := store.ProbeVersion(bytes.NewReader(v5)); err != nil || v != 5 {
		t.Fatalf("probe downgraded file = %d, %v; want 5, nil", v, err)
	}
	out, err := store.Decode(bytes.NewReader(v5))
	if err != nil {
		t.Fatalf("v5 snapshot rejected under v6 writer: %v", err)
	}
	if len(out.Nodes) != 2 || len(out.Edges) != 1 || len(out.IndexKeys) != 1 || len(out.VectorDecls) != 1 {
		t.Fatalf("v5 counts: %+v", out)
	}
	want := []float32{0.1, 0.2, 0.3}
	if got, ok := out.Nodes[0].Props["embedding"].([]float32); !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("v5 embedding = %#v, want %v", out.Nodes[0].Props["embedding"], want)
	}
}

// TestV7Rejected: a hypothetical v7 header hard-fails with the dispatch error
// — this binary never misreads a newer layout, the same contract the v6
// tripwire imposes on v0.11.1 binaries.
func TestV7Rejected(t *testing.T) {
	var buf bytes.Buffer
	if err := store.Encode(&buf, store.Data{}); err != nil {
		t.Fatal(err)
	}
	forged := append([]byte(nil), buf.Bytes()...)
	binary.LittleEndian.PutUint16(forged[4:6], 7)
	// ProbeVersion reports the stamp without validating; Decode is the gate.
	if v, err := store.ProbeVersion(bytes.NewReader(forged)); err != nil || v != 7 {
		t.Fatalf("probe v7 = %d, %v; want 7, nil", v, err)
	}
	_, err := store.Decode(bytes.NewReader(forged))
	if err == nil || !strings.Contains(err.Error(), "unsupported version") {
		t.Fatalf("err = %v, want unsupported version", err)
	}
}
