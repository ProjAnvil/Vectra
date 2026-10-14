package store_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"strings"
	"testing"

	"github.com/projanvil/vectra/internal/store"
)

func TestEncodeDecodeRoundtrip(t *testing.T) {
	var buf bytes.Buffer
	nodes := []store.Node{
		{Key: "gw", Label: "Service", Props: map[string]any{"team": "core", "qps": float64(1200)}},
		{Key: "db", Label: "Storage"},
	}
	edges := []store.Edge{
		{SrcKey: "gw", DstKey: "db", Label: "QUERY", Props: map[string]any{"mode": "read"}},
		{SrcKey: "db", DstKey: "gw", Label: "EVENT"},
	}
	if err := store.Encode(&buf, store.Data{Nodes: nodes, Edges: edges}); err != nil {
		t.Fatal(err)
	}
	got, err := store.Decode(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Nodes) != 2 || len(got.Edges) != 2 {
		t.Fatalf("counts = %d/%d, want 2/2", len(got.Nodes), len(got.Edges))
	}
	if got.Nodes[0].Key != "gw" || got.Nodes[0].Label != "Service" || got.Nodes[0].Props["team"] != "core" || got.Nodes[0].Props["qps"] != float64(1200) {
		t.Errorf("node0 = %+v", got.Nodes[0])
	}
	if got.Nodes[1].Props != nil {
		t.Errorf("node1 props = %v, want nil", got.Nodes[1].Props)
	}
	if got.Edges[0].SrcKey != "gw" || got.Edges[0].DstKey != "db" || got.Edges[0].Label != "QUERY" || got.Edges[0].Props["mode"] != "read" {
		t.Errorf("edge0 = %+v", got.Edges[0])
	}
	if got.Edges[1].Props != nil {
		t.Errorf("edge1 props = %v, want nil", got.Edges[1].Props)
	}
	if got.IndexKeys != nil {
		t.Errorf("indexKeys = %v, want nil when none declared", got.IndexKeys)
	}
}

func TestIndexKeysRoundtrip(t *testing.T) {
	var buf bytes.Buffer
	d := store.Data{
		Nodes:     []store.Node{{Key: "a", Label: "Svc", Props: map[string]any{"env": "prod"}}},
		IndexKeys: []string{"env", "team"},
	}
	if err := store.Encode(&buf, d); err != nil {
		t.Fatal(err)
	}
	got, err := store.Decode(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if len(got.IndexKeys) != 2 || got.IndexKeys[0] != "env" || got.IndexKeys[1] != "team" {
		t.Fatalf("indexKeys = %v, want [env team]", got.IndexKeys)
	}
}

// TestV2SnapshotStillReadable handcrafts a v2 file (no indexKey section, CRC
// trailer) — pre-0.5.0 snapshots must keep loading, with no index keys.
func TestV2SnapshotStillReadable(t *testing.T) {
	body := []byte{'V', 'C', 'T', 'R', 2, 0} // magic + version 2
	body = binary.LittleEndian.AppendUint32(body, 1) // 1 node
	body = binary.LittleEndian.AppendUint32(body, 0) // 0 edges
	body = append(body, 1, 0, 0, 0, 'a')             // key "a"
	body = append(body, 1, 0, 0, 0, 'L')             // label "L"
	body = append(body, 0, 0, 0, 0)                  // props nil
	crc := crc32.ChecksumIEEE(body)
	body = binary.LittleEndian.AppendUint32(body, crc)
	got, err := store.Decode(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("v2 snapshot rejected: %v", err)
	}
	if len(got.Nodes) != 1 || got.Nodes[0].Key != "a" {
		t.Fatalf("v2 nodes = %+v", got.Nodes)
	}
	if got.IndexKeys != nil {
		t.Errorf("v2 file cannot carry index keys, got %v", got.IndexKeys)
	}
}

func TestV3RejectsForgedIndexKeyCount(t *testing.T) {
	body := []byte{'V', 'C', 'T', 'R', 3, 0}         // magic + version 3
	body = binary.LittleEndian.AppendUint32(body, 0) // 0 nodes
	body = binary.LittleEndian.AppendUint32(body, 0) // 0 edges
	body = binary.LittleEndian.AppendUint32(body, 0xFFFFFFFF)
	crc := crc32.ChecksumIEEE(body)
	body = binary.LittleEndian.AppendUint32(body, crc)
	if _, err := store.Decode(bytes.NewReader(body)); err == nil {
		t.Fatal("forged indexKey count must be rejected by the lower-bound check")
	}
}

func TestV3RejectsTruncatedIndexKeys(t *testing.T) {
	var buf bytes.Buffer
	if err := store.Encode(&buf, store.Data{IndexKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	full := buf.Bytes()
	// drop the CRC and the last byte of the key string, re-append a valid CRC
	tampered := full[:len(full)-5]
	tampered = binary.LittleEndian.AppendUint32(tampered, crc32.ChecksumIEEE(tampered))
	if _, err := store.Decode(bytes.NewReader(tampered)); err == nil {
		t.Fatal("truncated indexKey string should be rejected")
	}
}

func TestDecodeRejectsBadMagic(t *testing.T) {
	if _, err := store.Decode(bytes.NewReader([]byte("NOPE"))); err == nil {
		t.Fatal("expected error for bad magic")
	}
}

func TestDecodeRejectsTruncated(t *testing.T) {
	var buf bytes.Buffer
	_ = store.Encode(&buf, store.Data{Nodes: []store.Node{{Key: "a", Label: "L"}}})
	if _, err := store.Decode(bytes.NewReader(buf.Bytes()[:5])); err == nil {
		t.Fatal("expected error for truncated input")
	}
}

func TestUnicodeKeysRoundtrip(t *testing.T) {
	var buf bytes.Buffer
	nodes := []store.Node{{Key: "café-Ø", Label: "Svc"}}
	if err := store.Encode(&buf, store.Data{Nodes: nodes}); err != nil {
		t.Fatal(err)
	}
	got, err := store.Decode(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if got.Nodes[0].Key != "café-Ø" {
		t.Errorf("key = %q", got.Nodes[0].Key)
	}
}

func TestEmptyGraphRoundtrip(t *testing.T) {
	var buf bytes.Buffer
	if err := store.Encode(&buf, store.Data{}); err != nil {
		t.Fatal(err)
	}
	got, err := store.Decode(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Nodes) != 0 || len(got.Edges) != 0 {
		t.Errorf("counts = %d/%d, want 0/0", len(got.Nodes), len(got.Edges))
	}
}

func TestDecodeRejectsUnsupportedVersion(t *testing.T) {
	data := []byte("VCTR\x09\x00" + "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")
	if _, err := store.Decode(bytes.NewReader(data)); err == nil {
		t.Fatal("v9 should be rejected")
	}
}

func TestDecodeRejectsTrailingGarbage(t *testing.T) {
	var buf bytes.Buffer
	_ = store.Encode(&buf, store.Data{Nodes: []store.Node{{Key: "a", Label: "L"}}})
	data := append([]byte{}, buf.Bytes()...)
	data = append(data, 0xFF, 0xFF)
	if _, err := store.Decode(bytes.NewReader(data)); err == nil {
		t.Fatal("v1 trailing garbage should be rejected")
	}
}

func TestDecodeRejectsHugeLengthField(t *testing.T) {
	// v1 file with the key length field forged to 4GB — must be rejected
	// quickly by the bounds check
	data := []byte("VCTR\x01\x00")
	data = append(data, 1, 0, 0, 0)             // 1 node
	data = append(data, 0, 0, 0, 0)             // 0 edges
	data = append(data, 0xFF, 0xFF, 0xFF, 0xFF) // key len = 4294967295
	if _, err := store.Decode(bytes.NewReader(data)); err == nil {
		t.Fatal("oversized length field should be rejected")
	}
}

func TestDecodeRejectsV2WithoutCRC(t *testing.T) {
	var buf bytes.Buffer
	_ = store.Encode(&buf, store.Data{Nodes: []store.Node{{Key: "a", Label: "L"}}})
	data := buf.Bytes()[:len(buf.Bytes())-3] // cut off part of the CRC
	if _, err := store.Decode(bytes.NewReader(data)); err == nil {
		t.Fatal("incomplete CRC should be rejected")
	}
}

func TestDecodeRejectsBadPropsJSON(t *testing.T) {
	// handcrafted v1: props length 5 but contents are invalid JSON
	data := []byte("VCTR\x01\x00")
	data = append(data, 1, 0, 0, 0) // 1 node
	data = append(data, 0, 0, 0, 0) // 0 edges
	data = append(data, 1, 0, 0, 0)
	data = append(data, 'a') // key "a"
	data = append(data, 1, 0, 0, 0)
	data = append(data, 'L')                     // label "L"
	data = append(data, 5, 0, 0, 0)              // props len 5
	data = append(data, '{', 'b', 'a', 'd', '!') // invalid JSON
	if _, err := store.Decode(bytes.NewReader(data)); err == nil {
		t.Fatal("bad props JSON should be rejected")
	}
}

type failWriter struct{}

func (failWriter) Write(p []byte) (int, error) { return 0, errors.New("disk full") }

func TestEncodePropagatesWriteErrors(t *testing.T) {
	if err := store.Encode(failWriter{}, store.Data{Nodes: []store.Node{{Key: "a", Label: "L"}}}); err == nil {
		t.Fatal("write failure should propagate an error")
	}
}

// failAfterWriter starts failing after the n-th write — sweeping n covers all
// of Encode's error branches.
type failAfterWriter struct{ n int }

func (w *failAfterWriter) Write(p []byte) (int, error) {
	w.n--
	if w.n < 0 {
		return 0, errors.New("disk full")
	}
	return len(p), nil
}

func TestEncodeFlushFailurePropagates(t *testing.T) {
	// bufio aggregates intermediate writes; failures surface at the Flush
	// point — a single injection covers them all
	w := &failAfterWriter{n: 0}
	big := strings.Repeat("k", 64<<10)
	err := store.Encode(w, store.Data{Nodes: []store.Node{{Key: big, Label: "L"}}})
	if err == nil {
		t.Fatal("Flush failure was not propagated")
	}
}

func TestEncodeRejectsUnmarshalableProps(t *testing.T) {
	// func values are not JSON-marshalable -> the Marshal error branch of
	// writeProps
	var buf bytes.Buffer
	err := store.Encode(&buf, store.Data{Nodes: []store.Node{{Key: "a", Label: "L", Props: map[string]any{"f": func() {}}}}})
	if err == nil {
		t.Fatal("unmarshalable props should error")
	}
}
