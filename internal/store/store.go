// Package store implements Vectra's single-file binary snapshot. Under the
// small-graph all-in-memory assumption the disk only serves crash recovery:
// whole-graph rewrite, rebuild CSR/SCC on load, no incremental WAL.
// Format v3 (little-endian): "VCTR" | u16 version(3) | u32 node count | u32 edge count |
// node{ key string, label string, props JSON }* | edge{ srcKey, dstKey, label, props }*
// | u32 indexKey count | key string* | u32 CRC32(IEEE, covering all preceding bytes).
// string = u32 length + bytes; props = u32 JSON length + JSON (0 = nil).
// The indexKey section persists the property-index declarations so a reloaded
// graph can rebuild them at Seal time (without it, Load-based services such as
// vectra-mcp -snapshot lose every lookup index). v2 (no indexKey section) and
// v1 (additionally no CRC trailer) remain readable; dispatched by header version.
package store

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
)

const (
	magic         = "VCTR"
	versionV3     = 3
	versionV2     = 2 // no indexKey section, read compatibility only
	versionLegacy = 1 // no CRC trailer, read compatibility only
	headerSize    = 6 // magic(4) + version(2); count fields start after this
)

// Node / Edge use string keys rather than internal NodeIDs: the snapshot is
// decoupled from ID assignment order and rebuilt on load. props serialize to
// JSON (numbers become float64 on roundtrip, a known limitation — see the
// README's snapshot notes).
type Node struct {
	Key   string
	Label string
	Props map[string]any
}

type Edge struct {
	SrcKey string
	DstKey string
	Label  string
	Props  map[string]any
}

// Data is the full decoded content of a snapshot: nodes, edges, and the
// property-index declarations to re-register before Seal. IndexKeys is nil
// for v1/v2 files (they never persisted any).
type Data struct {
	Nodes     []Node
	Edges     []Edge
	IndexKeys []string
}

// crcWriter aggregates the CRC of the written byte stream (appended as a
// trailer, covering all preceding bytes).
type crcWriter struct {
	bw  *bufio.Writer
	crc uint32
}

func (c *crcWriter) Write(p []byte) (int, error) {
	n, err := c.bw.Write(p)
	if n > 0 {
		c.crc = crc32.Update(c.crc, crc32.IEEETable, p[:n])
	}
	return n, err
}

func (c *crcWriter) finish() error {
	var tail [4]byte
	binary.LittleEndian.PutUint32(tail[:], c.crc)
	if _, err := c.bw.Write(tail[:]); err != nil {
		return err
	}
	return c.bw.Flush()
}

func putU32(w io.Writer, v uint32) error {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	_, err := w.Write(b[:])
	return err
}

func writeString(w io.Writer, s string) error {
	if err := putU32(w, uint32(len(s))); err != nil {
		return err
	}
	_, err := io.WriteString(w, s)
	return err
}

func writeProps(w io.Writer, props map[string]any) error {
	if props == nil {
		return putU32(w, 0)
	}
	b, err := json.Marshal(props)
	if err != nil {
		return err
	}
	if err := putU32(w, uint32(len(b))); err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

// Encode writes the whole graph to w in v3 format (indexKey section + CRC32
// trailer).
func Encode(w io.Writer, d Data) error {
	cw := &crcWriter{bw: bufio.NewWriter(w)}
	var head [4]byte
	copy(head[:], magic)
	if _, err := cw.Write(head[:]); err != nil {
		return err
	}
	if err := binary.Write(cw, binary.LittleEndian, uint16(versionV3)); err != nil {
		return err
	}
	if err := putU32(cw, uint32(len(d.Nodes))); err != nil {
		return err
	}
	if err := putU32(cw, uint32(len(d.Edges))); err != nil {
		return err
	}
	for _, n := range d.Nodes {
		if err := writeString(cw, n.Key); err != nil {
			return err
		}
		if err := writeString(cw, n.Label); err != nil {
			return err
		}
		if err := writeProps(cw, n.Props); err != nil {
			return err
		}
	}
	for _, e := range d.Edges {
		if err := writeString(cw, e.SrcKey); err != nil {
			return err
		}
		if err := writeString(cw, e.DstKey); err != nil {
			return err
		}
		if err := writeString(cw, e.Label); err != nil {
			return err
		}
		if err := writeProps(cw, e.Props); err != nil {
			return err
		}
	}
	if err := putU32(cw, uint32(len(d.IndexKeys))); err != nil {
		return err
	}
	for _, k := range d.IndexKeys {
		if err := writeString(cw, k); err != nil {
			return err
		}
	}
	return cw.finish()
}

// reader consumes a byte slice directly: every length field is bounds-checked
// against the remaining bytes, so bad input can never trigger oversized
// allocations.
type reader struct {
	b []byte
	i int
}

var errTruncated = errors.New("store: truncated input")

func (rd *reader) take(n int) ([]byte, error) {
	if n < 0 || rd.i+n > len(rd.b) {
		return nil, errTruncated
	}
	p := rd.b[rd.i : rd.i+n]
	rd.i += n
	return p, nil
}

func (rd *reader) u32() (uint32, error) {
	p, err := rd.take(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(p), nil
}

func (rd *reader) str() (string, error) {
	n, err := rd.u32()
	if err != nil {
		return "", err
	}
	p, err := rd.take(int(n))
	if err != nil {
		return "", err
	}
	return string(p), nil
}

func (rd *reader) props() (map[string]any, error) {
	n, err := rd.u32()
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	p, err := rd.take(int(n))
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(p, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// Decode restores the snapshot content. Reads fully first; v3/v2 verify the
// CRC; v2 and v1 are accepted for compatibility (they carry no index keys).
func Decode(r io.Reader) (Data, error) {
	raw, err := io.ReadAll(r)
	if err != nil {
		return Data{}, err
	}
	if len(raw) < headerSize+4 {
		return Data{}, errTruncated
	}
	if string(raw[:4]) != magic {
		return Data{}, errors.New("store: bad magic")
	}
	ver := binary.LittleEndian.Uint16(raw[4:6])
	body := raw
	switch ver {
	case versionV3, versionV2:
		body = raw[:len(raw)-4]
		want := binary.LittleEndian.Uint32(raw[len(raw)-4:])
		if got := crc32.ChecksumIEEE(body); got != want {
			return Data{}, errors.New("store: CRC mismatch")
		}
	case versionLegacy:
		// v1 has no CRC trailer
	default:
		return Data{}, errors.New("store: unsupported version")
	}

	rd := &reader{b: body[headerSize:]} // header already pre-parsed
	nn, err := rd.u32()
	if err != nil {
		return Data{}, err
	}
	ne, err := rd.u32()
	if err != nil {
		return Data{}, err
	}
	// Count lower bound: at least 12 fixed-size bytes per node and 16 per
	// edge, preventing forged counts from triggering oversized preallocation
	// (a CRC'd file can be forged by anyone who computes the CRC themselves).
	if int64(nn)*12+int64(ne)*16 > int64(len(rd.b)) {
		return Data{}, errTruncated
	}
	nodes := make([]Node, nn)
	for i := range nodes {
		if nodes[i].Key, err = rd.str(); err != nil {
			return Data{}, err
		}
		if nodes[i].Label, err = rd.str(); err != nil {
			return Data{}, err
		}
		if nodes[i].Props, err = rd.props(); err != nil {
			return Data{}, err
		}
	}
	edges := make([]Edge, ne)
	for i := range edges {
		if edges[i].SrcKey, err = rd.str(); err != nil {
			return Data{}, err
		}
		if edges[i].DstKey, err = rd.str(); err != nil {
			return Data{}, err
		}
		if edges[i].Label, err = rd.str(); err != nil {
			return Data{}, err
		}
		if edges[i].Props, err = rd.props(); err != nil {
			return Data{}, err
		}
	}
	var indexKeys []string
	if ver == versionV3 {
		nk, err := rd.u32()
		if err != nil {
			return Data{}, err
		}
		if int64(nk)*4 > int64(len(rd.b)-rd.i) { // same forged-count guard, 4 bytes per key minimum
			return Data{}, errTruncated
		}
		if nk > 0 {
			indexKeys = make([]string, nk)
			for i := range indexKeys {
				if indexKeys[i], err = rd.str(); err != nil {
					return Data{}, err
				}
			}
		}
	}
	if rd.i != len(rd.b) {
		return Data{}, errors.New("store: trailing garbage")
	}
	return Data{Nodes: nodes, Edges: edges, IndexKeys: indexKeys}, nil
}
