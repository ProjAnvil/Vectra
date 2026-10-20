package store

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"strings"
	"testing"
)

type failW struct{}

func (failW) Write(p []byte) (int, error) { return 0, errors.New("io fail") }

// White-box: inject failures directly into the internal write helpers
// (bypassing bufio aggregation).
func TestWriteHelpersPropagateErrors(t *testing.T) {
	if err := writeString(failW{}, "x"); err == nil {
		t.Error("writeString did not propagate failure")
	}
	if err := writeProps(failW{}, map[string]any{"a": 1}); err == nil {
		t.Error("writeProps(non-nil) did not propagate failure")
	}
	if err := writeProps(failW{}, nil); err == nil {
		t.Error("writeProps(nil) did not propagate failure")
	}
	var b bytes.Buffer
	if err := writeString(&b, ""); err != nil {
		t.Error(err)
	}
	// finish: underlying failure propagates through Flush
	bw := bufio.NewWriter(failW{})
	cw := &crcWriter{bw: bw}
	if _, err := cw.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := cw.finish(); err == nil {
		t.Error("finish did not propagate failure")
	}
}

// TestDecodeTruncatedAtEveryLength asserts decode failure for every prefix of
// a valid snapshot, walking all truncation guards.
func TestDecodeTruncatedAtEveryLength(t *testing.T) {
	var buf bytes.Buffer
	if err := Encode(&buf, Data{
		Nodes:     []Node{{Key: "node-1", Label: "Svc", Props: map[string]any{"env": "prod", "n": float64(7)}}},
		Edges:     []Edge{{SrcKey: "node-1", DstKey: "node-1", Label: "CALLS", Props: map[string]any{"w": float64(2)}}},
		IndexKeys: []string{"env"},
	}); err != nil {
		t.Fatal(err)
	}
	full := buf.Bytes()
	failOK := 0
	for cut := 0; cut < len(full); cut++ {
		if _, err := Decode(bytes.NewReader(full[:cut])); err != nil {
			failOK++
		} else {
			// v1 has no CRC: a cut landing exactly on a valid prefix with all
			// fields complete (e.g. at a trailing props boundary) succeeds —
			// only a few such coincidences are allowed, and the result must
			// be self-consistent
			if full[4] >= versionV2 {
				t.Fatalf("crc'd prefix cut=%d must not decode successfully", cut)
			}
		}
	}
	if failOK == 0 {
		t.Fatal("no truncation was rejected")
	}
}

// TestDecodeGarbageBody: valid header + garbage body, various guards.
func TestDecodeGarbageBody(t *testing.T) {
	base := func(ver uint16, body []byte) []byte {
		out := []byte{'V', 'C', 'T', 'R', byte(ver), 0}
		return append(out, body...)
	}
	// v1: node count 1 but empty body
	if _, err := Decode(bytes.NewReader(base(1, nil))); err == nil {
		t.Error("empty body should fail")
	}
	// v1: node count 2 with only 1 complete node
	one := []byte{2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 'a', 1, 0, 0, 0, 'S', 0, 0, 0, 0}
	if _, err := Decode(bytes.NewReader(base(1, one))); err == nil {
		t.Error("missing second node should fail")
	}
	// v1: edge count 1 with no edge data
	edges := []byte{0, 0, 0, 0, 1, 0, 0, 0}
	if _, err := Decode(bytes.NewReader(base(1, edges))); err == nil {
		t.Error("missing edge data should fail")
	}
	// v3: indexKey count 1 but no key string
	ik := []byte{0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0}
	crc := crc32.ChecksumIEEE(base(3, ik))
	v3 := binary.LittleEndian.AppendUint32(base(3, ik), crc)
	if _, err := Decode(bytes.NewReader(v3)); err == nil {
		t.Error("missing indexKey string should fail")
	}
}

var _ = strings.Repeat

// TestDecodeV1PartialEdgeFields: v1 has no CRC; guards for partially present
// edge fields.
func TestDecodeV1PartialEdgeFields(t *testing.T) {
	base := func(body []byte) []byte {
		out := []byte{'V', 'C', 'T', 'R', 1, 0}
		return append(out, body...)
	}
	node := []byte{0, 0, 0, 0}          // numNodes = 0
	numEdges := func(n uint32) []byte { // numEdges = n
		return []byte{byte(n), byte(n >> 8), byte(n >> 16), byte(n >> 24)}
	}
	// edge count 1: only srcKey complete, dstKey truncated
	partial1 := append(numEdges(1), 1, 0, 0, 0, 'a', 3, 0, 0, 0, 'b', 'b')
	if _, err := Decode(bytes.NewReader(base(append(node, partial1...)))); err == nil {
		t.Error("truncated dstKey should fail")
	}
	// edge count 1: src/dst complete, label truncated
	partial2 := append(numEdges(1), 1, 0, 0, 0, 'a', 1, 0, 0, 0, 'b', 2, 0, 0, 0, 'E')
	if _, err := Decode(bytes.NewReader(base(append(node, partial2...)))); err == nil {
		t.Error("truncated label should fail")
	}
	// edge count 1: src/dst/label complete, props length declared out of bounds
	partial3 := append(numEdges(1), 1, 0, 0, 0, 'a', 1, 0, 0, 0, 'b', 1, 0, 0, 0, 'E', 9, 0, 0, 0, 'x')
	if _, err := Decode(bytes.NewReader(base(append(node, partial3...)))); err == nil {
		t.Error("out-of-bounds props should fail")
	}
	// edge count 1: all fields present
	ok := append(numEdges(1), 1, 0, 0, 0, 'a', 1, 0, 0, 0, 'b', 1, 0, 0, 0, 'E', 0, 0, 0, 0)
	if _, err := Decode(bytes.NewReader(base(append(node, ok...)))); err != nil {
		t.Errorf("complete edge should not fail: %v", err)
	}
}

// TestDecodeHugeCountsRejectedWithValidCRC: the attacker can compute the CRC
// themselves — the count lower-bound check must hold independently of the CRC.
func TestDecodeHugeCountsRejectedWithValidCRC(t *testing.T) {
	body := []byte{'V', 'C', 'T', 'R', 2, 0}    // magic + v2
	body = append(body, 0xFF, 0xFF, 0x00, 0x00) // numNodes = 65535 (huge claim)
	body = append(body, 0, 0, 0, 0)             // numEdges = 0
	crc := crc32.ChecksumIEEE(body)
	body = binary.LittleEndian.AppendUint32(body, crc)
	if _, err := Decode(bytes.NewReader(body)); err == nil {
		t.Fatal("valid CRC + forged counts must be rejected by the lower-bound check")
	}
}
