package store_test

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/projanvil/vectra/internal/store"
)

// writeV4 lays out a literal v4 snapshot: declarations trailing the indexKey
// section, vector props riding the JSON channel, no float section. Encode
// writes v5 now, so read compatibility for pre-v5 files is pinned against
// hand-built bytes (the root-package testdata/legacy-v4 golden holds a
// real-world sample of the same layout).
func writeV4(t *testing.T, d store.Data) []byte {
	t.Helper()
	var buf bytes.Buffer
	str := func(s string) {
		_ = binary.Write(&buf, binary.LittleEndian, uint32(len(s)))
		buf.WriteString(s)
	}
	props := func(p map[string]any) {
		if p == nil {
			_ = binary.Write(&buf, binary.LittleEndian, uint32(0))
			return
		}
		b, err := json.Marshal(p)
		if err != nil {
			t.Fatalf("marshal v4 props: %v", err)
		}
		_ = binary.Write(&buf, binary.LittleEndian, uint32(len(b)))
		buf.Write(b)
	}
	buf.WriteString("VCTR")
	_ = binary.Write(&buf, binary.LittleEndian, uint16(4))
	_ = binary.Write(&buf, binary.LittleEndian, uint32(len(d.Nodes)))
	_ = binary.Write(&buf, binary.LittleEndian, uint32(len(d.Edges)))
	for _, n := range d.Nodes {
		str(n.Key)
		str(n.Label)
		props(n.Props)
	}
	for _, e := range d.Edges {
		str(e.SrcKey)
		str(e.DstKey)
		str(e.Label)
		props(e.Props)
	}
	_ = binary.Write(&buf, binary.LittleEndian, uint32(len(d.IndexKeys)))
	for _, k := range d.IndexKeys {
		str(k)
	}
	_ = binary.Write(&buf, binary.LittleEndian, uint32(len(d.VectorDecls)))
	for _, vd := range d.VectorDecls {
		blob, err := json.Marshal(vd)
		if err != nil {
			t.Fatalf("marshal v4 decl: %v", err)
		}
		str(string(blob))
	}
	_ = binary.Write(&buf, binary.LittleEndian, crc32.ChecksumIEEE(buf.Bytes()))
	return buf.Bytes()
}

// TestV5RoundTripWithVectors: declared vector props of both wire shapes
// ([]float32 and float64-source []any) survive Encode/Decode as []float32 —
// the declarations precede the nodes, so Decode can resolve every "$vec" ref
// against the shared float section. Undeclared numeric arrays are data and
// stay JSON; "$vec"-shaped literals on undeclared props pass through.
func TestV5RoundTripWithVectors(t *testing.T) {
	note := map[string]any{"$vec": []any{float64(9), float64(9)}} // undeclared: verbatim
	in := store.Data{
		Nodes: []store.Node{
			{Key: "a", Label: "mem", Props: map[string]any{"embedding": []float32{0.1, 0.2, 0.3}, "team": "core"}},
			{Key: "b", Label: "mem", Props: map[string]any{
				"embedding": []any{float64(0.4), float64(0.5), float64(0.6)},
				"scores":    []any{float64(1), float64(2)},
				"note":      note,
			}},
		},
		Edges:     []store.Edge{{SrcKey: "a", DstKey: "b", Label: "NEXT", Props: map[string]any{"w": float64(2)}}},
		IndexKeys: []string{"team"},
		VectorDecls: []store.VectorIndexDecl{
			{Name: "mem", Prop: "embedding", Dim: 3, Metric: "cosine", M: 16, EfConstruction: 200},
		},
	}
	var buf bytes.Buffer
	if err := store.Encode(&buf, in); err != nil {
		t.Fatal(err)
	}
	// v6 is a version-only bump over this v5 layout (retained-WAL tripwire,
	// 2026-10-22 plan Task 1): the stamp moved to 6, the bytes did not.
	if got := binary.LittleEndian.Uint16(buf.Bytes()[4:6]); got != 6 {
		t.Fatalf("version = %d, want 6", got)
	}
	// ruling #2 pinned on the wire: the decl blob physically precedes the
	// node section (external v5 readers rely on the byte order, not just on
	// this package's encoder/decoder agreeing with each other)
	declAt := bytes.Index(buf.Bytes(), []byte(`"prop":"embedding"`))
	nodeAt := bytes.Index(buf.Bytes(), []byte(`"team":"core"`))
	if declAt < 0 || nodeAt < 0 || declAt > nodeAt {
		t.Fatalf("decl blob at %d must precede node props at %d", declAt, nodeAt)
	}
	out, err := store.Decode(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if len(out.VectorDecls) != 1 || out.VectorDecls[0] != in.VectorDecls[0] {
		t.Fatalf("vector decls = %+v, want %+v", out.VectorDecls, in.VectorDecls)
	}
	for i, want := range [][]float32{{0.1, 0.2, 0.3}, {0.4, 0.5, 0.6}} {
		got, ok := out.Nodes[i].Props["embedding"].([]float32)
		if !ok || !reflect.DeepEqual(got, want) {
			t.Errorf("node %d embedding = %#v, want %v", i, out.Nodes[i].Props["embedding"], want)
		}
	}
	if out.Nodes[0].Props["team"] != "core" {
		t.Errorf("non-vector prop lost: %v", out.Nodes[0].Props)
	}
	if want := []any{float64(1), float64(2)}; !reflect.DeepEqual(out.Nodes[1].Props["scores"], want) {
		t.Errorf("undeclared numeric array must stay JSON: %#v", out.Nodes[1].Props["scores"])
	}
	if !reflect.DeepEqual(out.Nodes[1].Props["note"], note) {
		t.Errorf(`"$vec" literal on undeclared prop must pass through: %#v`, out.Nodes[1].Props["note"])
	}
	if len(out.IndexKeys) != 1 || out.IndexKeys[0] != "team" {
		t.Errorf("indexKeys = %v", out.IndexKeys)
	}
	if out.Edges[0].Props["w"] != float64(2) {
		t.Errorf("edge props lost: %v", out.Edges[0].Props)
	}
}

// TestV5HoistsFloat64Props (Review Focus #1): float64-source vectors hoist
// into the binary float32 section like []float32 sources do — the section is
// non-empty and holds the raw LE float32 bits (the decimal text is gone from
// the JSON channel), which also makes the v5 stream smaller than the v4
// encoding of the same Data.
func TestV5HoistsFloat64Props(t *testing.T) {
	vec := make([]any, 8)
	for i := range vec {
		vec[i] = float64(i) + 0.123456789012345
	}
	in := store.Data{
		Nodes: []store.Node{{Key: "a", Label: "mem", Props: map[string]any{"embedding": vec, "team": "core"}}},
		VectorDecls: []store.VectorIndexDecl{
			{Name: "mem", Prop: "embedding", Dim: 8, Metric: "cosine"},
		},
	}
	var v5 bytes.Buffer
	if err := store.Encode(&v5, in); err != nil {
		t.Fatal(err)
	}
	if got := binary.LittleEndian.Uint16(v5.Bytes()[4:6]); got != 6 { // version-only v6 bump; layout is still v5's
		t.Fatalf("version = %d, want 6", got)
	}
	if v4 := writeV4(t, in); len(v5.Bytes()) >= len(v4) {
		t.Errorf("v5 = %d bytes >= v4 = %d bytes: hoisting must shrink the stream", len(v5.Bytes()), len(v4))
	}
	var raw []byte
	for _, e := range vec {
		raw = binary.LittleEndian.AppendUint32(raw, math.Float32bits(float32(e.(float64))))
	}
	if !bytes.Contains(v5.Bytes(), raw) {
		t.Error("float64 source did not land in the binary float32 section")
	}
	if bytes.Contains(v5.Bytes(), []byte("123456789")) {
		t.Error("vector floats stayed in the JSON props channel")
	}
	out, err := store.Decode(bytes.NewReader(v5.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	want := make([]float32, len(vec))
	for i, e := range vec {
		want[i] = float32(e.(float64))
	}
	if got := out.Nodes[0].Props["embedding"]; !reflect.DeepEqual(got, want) {
		t.Errorf("embedding = %#v, want %v", got, want)
	}
}

// TestV4StillReadable: a hand-built v4 snapshot (decls trailing, vectors in
// the JSON channel) keeps decoding — the Task 5 migration reads v4 files.
func TestV4StillReadable(t *testing.T) {
	in := store.Data{
		Nodes: []store.Node{
			{Key: "a", Label: "mem", Props: map[string]any{"embedding": []float32{0.1, 0.2, 0.3}, "team": "core"}},
			{Key: "b", Label: "mem", Props: map[string]any{"embedding": []any{float64(0.4), float64(0.5), float64(0.6)}}},
		},
		Edges:     []store.Edge{{SrcKey: "a", DstKey: "b", Label: "NEXT"}},
		IndexKeys: []string{"team"},
		VectorDecls: []store.VectorIndexDecl{
			{Name: "mem", Prop: "embedding", Dim: 3, Metric: "cosine", M: 16, EfConstruction: 200},
		},
	}
	got, err := store.Decode(bytes.NewReader(writeV4(t, in)))
	if err != nil {
		t.Fatalf("v4 snapshot rejected: %v", err)
	}
	if len(got.Nodes) != 2 || len(got.Edges) != 1 || len(got.IndexKeys) != 1 {
		t.Fatalf("v4 counts: %+v", got)
	}
	if len(got.VectorDecls) != 1 || got.VectorDecls[0] != in.VectorDecls[0] {
		t.Fatalf("v4 decls = %+v", got.VectorDecls)
	}
	// v4 has no float section: vectors arrive via JSON as []any, and the
	// intake layer (NormalizeVec) widens them — the store stays faithful to
	// what the file holds.
	emb, ok := got.Nodes[0].Props["embedding"].([]any)
	if !ok || len(emb) != 3 || emb[0] != float64(0.1) {
		t.Fatalf("v4 embedding = %#v, want JSON channel", got.Nodes[0].Props["embedding"])
	}
}

// TestProbeVersion: the header-only probe recognizes the written v6 product
// and v4 files; a bad magic (or a header too short to hold one) errors.
func TestProbeVersion(t *testing.T) {
	var buf bytes.Buffer
	if err := store.Encode(&buf, store.Data{}); err != nil {
		t.Fatal(err)
	}
	if v, err := store.ProbeVersion(bytes.NewReader(buf.Bytes())); err != nil || v != 6 {
		t.Fatalf("probe v6 = %d, %v; want 6, nil", v, err)
	}
	if v, err := store.ProbeVersion(bytes.NewReader(writeV4(t, store.Data{}))); err != nil || v != 4 {
		t.Fatalf("probe v4 = %d, %v; want 4, nil", v, err)
	}
	if _, err := store.ProbeVersion(bytes.NewReader([]byte("NOPE\x05\x00"))); err == nil {
		t.Fatal("bad magic must error")
	}
	if _, err := store.ProbeVersion(bytes.NewReader([]byte("VCT"))); err == nil {
		t.Fatal("truncated header must error")
	}
}

// TestUnsupportedVersionFails: a v7 header hard-fails with the dispatch
// error — old readers never misread a newer layout. (v6 was this test's
// forged version until the retained-WAL tripwire made it real: store_v6_test.go.)
func TestUnsupportedVersionFails(t *testing.T) {
	data := []byte("VCTR\x07\x00" + "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")
	_, err := store.Decode(bytes.NewReader(data))
	if err == nil || !strings.Contains(err.Error(), "unsupported version") {
		t.Fatalf("err = %v, want unsupported version", err)
	}
}
