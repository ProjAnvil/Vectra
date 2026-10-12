package store_test

import (
	"bytes"
	"errors"
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
	if err := store.Encode(&buf, nodes, edges); err != nil {
		t.Fatal(err)
	}
	gotNodes, gotEdges, err := store.Decode(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if len(gotNodes) != 2 || len(gotEdges) != 2 {
		t.Fatalf("counts = %d/%d, want 2/2", len(gotNodes), len(gotEdges))
	}
	if gotNodes[0].Key != "gw" || gotNodes[0].Label != "Service" || gotNodes[0].Props["team"] != "core" || gotNodes[0].Props["qps"] != float64(1200) {
		t.Errorf("node0 = %+v", gotNodes[0])
	}
	if gotNodes[1].Props != nil {
		t.Errorf("node1 props = %v, want nil", gotNodes[1].Props)
	}
	if gotEdges[0].SrcKey != "gw" || gotEdges[0].DstKey != "db" || gotEdges[0].Label != "QUERY" || gotEdges[0].Props["mode"] != "read" {
		t.Errorf("edge0 = %+v", gotEdges[0])
	}
	if gotEdges[1].Props != nil {
		t.Errorf("edge1 props = %v, want nil", gotEdges[1].Props)
	}
}

func TestDecodeRejectsBadMagic(t *testing.T) {
	if _, _, err := store.Decode(bytes.NewReader([]byte("NOPE"))); err == nil {
		t.Fatal("expected error for bad magic")
	}
}

func TestDecodeRejectsTruncated(t *testing.T) {
	var buf bytes.Buffer
	_ = store.Encode(&buf, []store.Node{{Key: "a", Label: "L"}}, nil)
	if _, _, err := store.Decode(bytes.NewReader(buf.Bytes()[:5])); err == nil {
		t.Fatal("expected error for truncated input")
	}
}

func TestUnicodeKeysRoundtrip(t *testing.T) {
	var buf bytes.Buffer
	nodes := []store.Node{{Key: "café-Ø", Label: "Svc"}}
	if err := store.Encode(&buf, nodes, nil); err != nil {
		t.Fatal(err)
	}
	got, _, err := store.Decode(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Key != "café-Ø" {
		t.Errorf("key = %q", got[0].Key)
	}
}

func TestEmptyGraphRoundtrip(t *testing.T) {
	var buf bytes.Buffer
	if err := store.Encode(&buf, nil, nil); err != nil {
		t.Fatal(err)
	}
	nodes, edges, err := store.Decode(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 0 || len(edges) != 0 {
		t.Errorf("counts = %d/%d, want 0/0", len(nodes), len(edges))
	}
}

func TestDecodeRejectsUnsupportedVersion(t *testing.T) {
	data := []byte("VCTR\x09\x00" + "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")
	if _, _, err := store.Decode(bytes.NewReader(data)); err == nil {
		t.Fatal("v9 should be rejected")
	}
}

func TestDecodeRejectsTrailingGarbage(t *testing.T) {
	var buf bytes.Buffer
	_ = store.Encode(&buf, []store.Node{{Key: "a", Label: "L"}}, nil)
	data := append([]byte{}, buf.Bytes()...)
	data = append(data, 0xFF, 0xFF)
	if _, _, err := store.Decode(bytes.NewReader(data)); err == nil {
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
	if _, _, err := store.Decode(bytes.NewReader(data)); err == nil {
		t.Fatal("oversized length field should be rejected")
	}
}

func TestDecodeRejectsV2WithoutCRC(t *testing.T) {
	var buf bytes.Buffer
	_ = store.Encode(&buf, []store.Node{{Key: "a", Label: "L"}}, nil)
	data := buf.Bytes()[:len(buf.Bytes())-3] // cut off part of the CRC
	if _, _, err := store.Decode(bytes.NewReader(data)); err == nil {
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
	if _, _, err := store.Decode(bytes.NewReader(data)); err == nil {
		t.Fatal("bad props JSON should be rejected")
	}
}

type failWriter struct{}

func (failWriter) Write(p []byte) (int, error) { return 0, errors.New("disk full") }

func TestEncodePropagatesWriteErrors(t *testing.T) {
	if err := store.Encode(failWriter{}, []store.Node{{Key: "a", Label: "L"}}, nil); err == nil {
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
	err := store.Encode(w, []store.Node{{Key: big, Label: "L"}}, nil)
	if err == nil {
		t.Fatal("Flush failure was not propagated")
	}
}

func TestEncodeRejectsUnmarshalableProps(t *testing.T) {
	// func values are not JSON-marshalable -> the Marshal error branch of
	// writeProps
	var buf bytes.Buffer
	err := store.Encode(&buf, []store.Node{{Key: "a", Label: "L", Props: map[string]any{"f": func() {}}}}, nil)
	if err == nil {
		t.Fatal("unmarshalable props should error")
	}
}
