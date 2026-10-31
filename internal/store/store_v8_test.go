package store_test

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"reflect"
	"strings"
	"testing"

	"github.com/projanvil/vectra/internal/store"
)

// v8 (2026-10-30 unique-constraints plan, spec §4.2) appends the
// unique-constraint section after the ftsKey section: u32 count + one JSON
// blob per constraint (the vector-decl section shape). Open rules mirror the
// v6 -> v7 bump exactly: decode gates the section on ver >= v8 (older files
// decode a nil field — zero constraints, recompact-eligible to v8), and any
// newer version hard-fails on the version byte instead of misreading the
// layout.

// TestV8IsWrittenVersion: Encode stamps 8 on the wire and the product still
// round-trips through Decode, constraints included (the dispatch must accept
// what Encode writes).
func TestV8IsWrittenVersion(t *testing.T) {
	in := store.Data{
		Nodes:       []store.Node{{Key: "a", Label: "Person", Props: map[string]any{"email": "a@x", "team": "core"}}},
		Edges:       []store.Edge{{SrcKey: "a", DstKey: "a", Label: "SELF"}},
		IndexKeys:   []string{"team"},
		VectorDecls: []store.VectorIndexDecl{{Name: "mem", Prop: "embedding", Dim: 2, Metric: "cosine"}},
		FTSKeys:     []string{"team"},
		UniqueConstraints: []store.UniqueConstraint{
			{Label: "Person", Prop: "email"},
			{Label: "Company", Prop: "vat"},
		},
	}
	var buf bytes.Buffer
	if err := store.Encode(&buf, in); err != nil {
		t.Fatal(err)
	}
	if got := binary.LittleEndian.Uint16(buf.Bytes()[4:6]); got != store.VersionV8 {
		t.Fatalf("written version = %d, want %d", got, store.VersionV8)
	}
	v, err := store.ProbeVersion(bytes.NewReader(buf.Bytes()))
	if err != nil || v != store.VersionV8 {
		t.Fatalf("probe = %d, %v; want %d, nil", v, err, store.VersionV8)
	}
	// The blob's JSON names are the wire contract shared with
	// wal.UniqueConstraint; a same-struct round-trip cannot catch a tag typo.
	if !bytes.Contains(buf.Bytes(), []byte(`{"label":"Person","prop":"email"}`)) {
		t.Fatalf("constraint blob wire names drifted: %q", buf.Bytes())
	}
	out, err := store.Decode(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("v8 product must decode: %v", err)
	}
	if !reflect.DeepEqual(out.UniqueConstraints, in.UniqueConstraints) {
		t.Fatalf("v8 constraints round-trip = %+v, want %+v", out.UniqueConstraints, in.UniqueConstraints)
	}
	if len(out.IndexKeys) != 1 || len(out.VectorDecls) != 1 || len(out.FTSKeys) != 1 {
		t.Fatalf("v8 round-trip = %+v", out)
	}
	// Zero constraints encode as a bare count and decode back as nil —
	// callers treat nil as "no constraints persisted" (pre-v8 files alike).
	var bare bytes.Buffer
	if err := store.Encode(&bare, store.Data{Nodes: in.Nodes}); err != nil {
		t.Fatal(err)
	}
	bareOut, err := store.Decode(bytes.NewReader(bare.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if bareOut.UniqueConstraints != nil {
		t.Fatalf("UniqueConstraints = %v, want nil when none persisted", bareOut.UniqueConstraints)
	}
}

// resealAsV7 turns this binary's v8 Encode output into a genuine v7 file:
// v8's only layout delta over v7 is the trailing unique-constraint section,
// so cutting exactly the section bytes (u32 count + one length-prefixed
// JSON blob per constraint), patching the version back to 7 and re-sealing
// the CRC yields what a v7-era encoder wrote (see restampDownlevel —
// version-byte patching alone cannot).
func resealAsV7(t *testing.T, v8 []byte, ucs []store.UniqueConstraint) []byte {
	t.Helper()
	secLen := 4 // u32 constraint count
	for _, uc := range ucs {
		blob, err := json.Marshal(uc)
		if err != nil {
			t.Fatalf("marshal constraint: %v", err)
		}
		secLen += 4 + len(blob)
	}
	if len(v8)-4-secLen < 10 {
		t.Fatalf("v8 buffer too short to hold header + section of %d bytes", secLen)
	}
	out := append([]byte(nil), v8[:len(v8)-4-secLen]...)
	binary.LittleEndian.PutUint16(out[4:6], store.VersionV7)
	return binary.LittleEndian.AppendUint32(out, crc32.ChecksumIEEE(out))
}

// TestV7StillReadableUnderV8: files written by every v7-era binary keep
// decoding after the constraint bump — the tripwire only fences OLD readers,
// never this one against its predecessors. A genuine v7 buffer is v8 minus
// the unique-constraint section, rebuilt via resealAsV7; its decoded
// UniqueConstraints field is nil.
func TestV7StillReadableUnderV8(t *testing.T) {
	in := store.Data{
		Nodes:       []store.Node{{Key: "a", Label: "mem", Props: map[string]any{"embedding": []float32{0.1, 0.2, 0.3}, "team": "core"}}},
		Edges:       []store.Edge{{SrcKey: "a", DstKey: "b", Label: "NEXT"}},
		IndexKeys:   []string{"team"},
		VectorDecls: []store.VectorIndexDecl{{Name: "mem", Prop: "embedding", Dim: 3, Metric: "cosine", M: 16, EfConstruction: 200}},
		FTSKeys:     []string{"title"},
		UniqueConstraints: []store.UniqueConstraint{
			{Label: "Person", Prop: "email"},
			{Label: "Company", Prop: "vat"},
		}, // sizes the v8-only section that resealAsV7 strips
	}
	var buf bytes.Buffer
	if err := store.Encode(&buf, in); err != nil {
		t.Fatal(err)
	}
	v7 := resealAsV7(t, buf.Bytes(), in.UniqueConstraints)
	if v, err := store.ProbeVersion(bytes.NewReader(v7)); err != nil || v != store.VersionV7 {
		t.Fatalf("probe downgraded file = %d, %v; want %d, nil", v, err, store.VersionV7)
	}
	out, err := store.Decode(bytes.NewReader(v7))
	if err != nil {
		t.Fatalf("v7 snapshot rejected under v8 writer: %v", err)
	}
	if out.UniqueConstraints != nil {
		t.Fatalf("v7 file decodes UniqueConstraints = %v, want nil (the section does not exist in v7)", out.UniqueConstraints)
	}
	if len(out.Nodes) != 1 || len(out.Edges) != 1 || len(out.IndexKeys) != 1 || len(out.VectorDecls) != 1 || len(out.FTSKeys) != 1 {
		t.Fatalf("v7 counts: %+v", out)
	}
	want := []float32{0.1, 0.2, 0.3}
	if got, ok := out.Nodes[0].Props["embedding"].([]float32); !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("v7 embedding = %#v, want %v", out.Nodes[0].Props["embedding"], want)
	}
}

// TestV9Rejected: a hypothetical v9 header hard-fails with the dispatch
// error — this binary never misreads a newer layout, the same contract the
// v8 tripwire imposes on v0.12.x binaries (TestV8Rejected was this test
// until v8 became real).
func TestV9Rejected(t *testing.T) {
	var buf bytes.Buffer
	if err := store.Encode(&buf, store.Data{}); err != nil {
		t.Fatal(err)
	}
	forged := append([]byte(nil), buf.Bytes()...)
	binary.LittleEndian.PutUint16(forged[4:6], store.VersionV8+1)
	// ProbeVersion reports the stamp without validating; Decode is the gate.
	if v, err := store.ProbeVersion(bytes.NewReader(forged)); err != nil || v != store.VersionV8+1 {
		t.Fatalf("probe v9 = %d, %v; want %d, nil", v, err, store.VersionV8+1)
	}
	_, err := store.Decode(bytes.NewReader(forged))
	if err == nil || !strings.Contains(err.Error(), "unsupported version") {
		t.Fatalf("err = %v, want unsupported version", err)
	}
}
