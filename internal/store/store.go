// Package store implements Vectra's single-file binary snapshot. Under the
// small-graph all-in-memory assumption the disk only serves crash recovery:
// whole-graph rewrite, rebuild CSR/SCC on load, no incremental WAL.
// Layout (v5, still current under the v6 stamp; little-endian):
// "VCTR" | u16 version(6) | u32 node count | u32 edge count |
// u32 vector-decl count | decl JSON string* | node{ key string, label string, props JSON }* |
// edge{ srcKey, dstKey, label, props }* | u32 indexKey count | key string* |
// u32 float count | float32 LE* | u32 CRC32(IEEE, covering all preceding bytes).
// string = u32 length + bytes; props = u32 JSON length + JSON (0 = nil).
// The vector declarations precede the nodes (spec 2026-10-21 §1.4, ruling
// #2) so the decoder knows the declared-prop set while resolving per-node
// "$vec" refs: Encode hoists declared vector props ([]float32 and
// float64-source []any alike, ruling #1) out of the JSON channel via
// vecbin.Hoist into ONE shared trailing float32 section; Decode restores
// them as []float32. Undeclared numeric arrays are data and stay JSON; edge
// props are never hoisted. The indexKey section persists the property-index
// declarations so a reloaded graph can rebuild them at Seal time (without
// it, Load-based services such as vectra-mcp -snapshot lose every lookup
// index). v4 (vector decls trailing, vectors in the JSON channel), v3, v2
// (no indexKey section) and v1 (additionally no CRC trailer) remain
// readable; dispatched by header version. Pre-v6 readers hard-fail on the
// version byte instead of misreading the layout. v6 is a VERSION-ONLY bump
// over v5 (byte layout identical): the retained-WAL checkpoint chain keeps
// the base snapshot on disk while WAL segments carry the increments, and a
// v0.11.1 binary would silently load only that base — the v6 stamp makes it
// hard-fail on the version byte instead of losing the retained segments.
package store

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"

	"github.com/projanvil/vectra/internal/vecbin"
)

const (
	magic         = "VCTR"
	versionV6     = 6 // version-only bump over v5: retained-WAL checkpoint tripwire (see package doc)
	versionV5     = 5 // declarations-first layout + shared binary float32 section
	versionV4     = 4 // adds the vector index declaration section (trailing)
	versionV3     = 3
	versionV2     = 2 // no indexKey section, read compatibility only
	versionLegacy = 1 // no CRC trailer, read compatibility only
	headerSize    = 6 // magic(4) + version(2); count fields start after this
)

// VersionV5 is the retired baseline of the 2026-10-21 Open-time migration
// gate: vectra.Open then probed the recovered head's version and sealed
// anything below v5 (spec 2026-10-21 §1.3). Since the 2026-10-22 checkpoint
// plan the live gate is VersionV6 — Encode stamps the version-only v6 bump
// and Open migrates v1-v5 heads to it — so this constant now only pins the
// bottom of that chain (tests forge "one version past the product" from it).
const VersionV5 = versionV5

// VersionV6 exports the v6 tripwire version (2026-10-22 checkpoint plan): a
// light checkpoint keeps the base snapshot alive under a manifest v2, so the
// base's version byte is the ONLY hard downgrade gate — vectra's checkpoint
// tiering probes it to decide whether a light checkpoint is expressible at
// all (pre-v6 bases must recompact).
const VersionV6 = versionV6

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

// VectorIndexDecl is the store mirror of the wire declaration (JSON tags
// match wal.VectorIndexDecl; spec §3.1/§3.3). Vector payloads themselves
// ride node props through the existing JSON channel - the intake layer
// (vector.NormalizeVec) recovers []float32 from the float64 round-trip.
type VectorIndexDecl struct {
	Name           string `json:"name"`
	Prop           string `json:"prop"`
	Dim            int    `json:"dim"`
	Metric         string `json:"metric"`
	M              int    `json:"m,omitempty"`
	EfConstruction int    `json:"efConstruction,omitempty"`
}

// Data is the full decoded content of a snapshot: nodes, edges, and the
// property-index declarations to re-register before Seal. IndexKeys is nil
// for v1/v2 files (they never persisted any); VectorDecls is nil for
// pre-v4 files. In v5 files declared vector props come back as []float32
// (restored from the binary section); in v4-and-older files they arrive via
// JSON as []any.
type Data struct {
	Nodes       []Node
	Edges       []Edge
	IndexKeys   []string
	VectorDecls []VectorIndexDecl
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

// putF32 appends one float32 section entry (raw LE bits, no JSON detour).
func putF32(w io.Writer, f float32) error {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], math.Float32bits(f))
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

// Encode writes the whole graph to w stamped v6 (byte-identical to the v5
// layout — the version-only retained-WAL tripwire, see package doc): the
// vector-declaration section precedes the nodes (ruling #2) and declared
// vector props hoist out of the JSON channel into one shared trailing
// float32 section (ruling #1); the CRC trailer covers everything. Pre-v6
// readers hard-fail on the version byte instead of misreading the layout
// (same downgrade line as v4 -> v5).
func Encode(w io.Writer, d Data) error {
	cw := &crcWriter{bw: bufio.NewWriter(w)}
	var head [4]byte
	copy(head[:], magic)
	if _, err := cw.Write(head[:]); err != nil {
		return err
	}
	if err := binary.Write(cw, binary.LittleEndian, uint16(versionV6)); err != nil {
		return err
	}
	if err := putU32(cw, uint32(len(d.Nodes))); err != nil {
		return err
	}
	if err := putU32(cw, uint32(len(d.Edges))); err != nil {
		return err
	}
	// v5 vector declaration section, relocated from v4's tail to ahead of
	// the nodes (ruling #2): the decoder needs the declared-prop set to
	// resolve "$vec" refs while reading nodes. Same encoding as v4: count +
	// one JSON blob per decl.
	if err := putU32(cw, uint32(len(d.VectorDecls))); err != nil {
		return err
	}
	for _, vd := range d.VectorDecls {
		blob, err := json.Marshal(vd)
		if err != nil {
			return err
		}
		if err := writeString(cw, string(blob)); err != nil {
			return err
		}
	}
	// Hoisting pass: declared vector props ([]float32 and float64 sources
	// alike) leave the JSON channel as "$vec" refs into ONE section shared
	// by all nodes. Undeclared numeric arrays are data and stay JSON; edge
	// props are never indexed (spec §1.2) and stay JSON.
	decls := make(vecbin.PropDecls, len(d.VectorDecls))
	for _, vd := range d.VectorDecls {
		decls[vd.Prop] = vd.Dim
	}
	var sec []float32
	for _, n := range d.Nodes {
		if err := writeString(cw, n.Key); err != nil {
			return err
		}
		if err := writeString(cw, n.Label); err != nil {
			return err
		}
		props := n.Props
		if len(decls) > 0 && n.Props != nil {
			if p, ok := vecbin.Hoist(n.Props, decls, &sec); ok {
				props = p
			}
		}
		if err := writeProps(cw, props); err != nil {
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
	// v1-v4 reject non-finite floats at json.Marshal time; the binary
	// section would carry them silently, so v5 refuses them too (WAL v2
	// frames enforce the same line — NormalizeVec makes this unreachable
	// from the engine, the guard covers direct callers).
	for _, f := range sec {
		if math.IsNaN(float64(f)) || math.IsInf(float64(f), 0) {
			return fmt.Errorf("store: hoisted vector section holds non-finite float %v (the JSON channel rejects it; so does v5)", f)
		}
	}
	// Trailing float32 section: u32 count + raw LE bits. The count keeps the
	// stream self-delimiting, so the trailing-garbage guard stays exact.
	if err := putU32(cw, uint32(len(sec))); err != nil {
		return err
	}
	for _, f := range sec {
		if err := putF32(cw, f); err != nil {
			return err
		}
	}
	return cw.finish()
}

// ProbeVersion reads only the header (magic + version) and reports the
// file's version. It does not validate the body or CRC; callers use it to
// pick a migration/rebuild path before committing to a full Decode.
func ProbeVersion(r io.Reader) (uint16, error) {
	var head [headerSize]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return 0, err
	}
	if string(head[:4]) != magic {
		return 0, errors.New("store: bad magic")
	}
	return binary.LittleEndian.Uint16(head[4:6]), nil
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

// vectorDecls reads the declaration section shared by v5 (leading, before
// the nodes) and v4 (trailing, after indexKeys): u32 count + one JSON blob
// per decl. The forged-count guard bounds the preallocation (one u32 length
// per decl minimum).
func (rd *reader) vectorDecls() ([]VectorIndexDecl, error) {
	nd, err := rd.u32()
	if err != nil {
		return nil, err
	}
	if int64(nd)*4 > int64(len(rd.b)-rd.i) {
		return nil, errTruncated
	}
	if nd == 0 {
		return nil, nil
	}
	decls := make([]VectorIndexDecl, nd)
	for i := range decls {
		blob, err := rd.str()
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(blob), &decls[i]); err != nil {
			return nil, err
		}
	}
	return decls, nil
}

// floatSection reads the v5 trailing float32 section: u32 count + raw
// little-endian bits, with the same forged-count guard (4 bytes per float).
func (rd *reader) floatSection() ([]float32, error) {
	nf, err := rd.u32()
	if err != nil {
		return nil, err
	}
	if int64(nf)*4 > int64(len(rd.b)-rd.i) {
		return nil, errTruncated
	}
	if nf == 0 {
		return nil, nil
	}
	sec := make([]float32, nf)
	for i := range sec {
		p, err := rd.take(4)
		if err != nil {
			return nil, err
		}
		sec[i] = math.Float32frombits(binary.LittleEndian.Uint32(p))
	}
	return sec, nil
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
	case versionV6, versionV5, versionV4, versionV3, versionV2:
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
	// v5 hoists the declaration section ahead of the nodes (ruling #2): the
	// declared-prop set must be known before node props resolve "$vec" refs.
	var vecDecls []VectorIndexDecl
	if ver >= versionV5 {
		if vecDecls, err = rd.vectorDecls(); err != nil {
			return Data{}, err
		}
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
	if ver >= versionV3 {
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
	// vecDecls was read before the nodes on v5; on v4 it trails here.
	if ver >= versionV5 {
		sec, err := rd.floatSection()
		if err != nil {
			return Data{}, err
		}
		// Restore only when a declaration exists: refs resolve solely on
		// declared props (vecbin), and a decl-free v5 file hoisted nothing.
		if len(vecDecls) > 0 {
			decls := make(vecbin.PropDecls, len(vecDecls))
			for _, vd := range vecDecls {
				decls[vd.Prop] = vd.Dim
			}
			for i := range nodes {
				if nodes[i].Props == nil {
					continue // nil props stay nil, exactly as pre-hoist layouts decode them
				}
				p, rerr := vecbin.Restore(nodes[i].Props, decls, sec)
				if rerr != nil {
					return Data{}, fmt.Errorf("store: node %q: %w", nodes[i].Key, rerr)
				}
				nodes[i].Props = p
			}
		}
	} else if ver == versionV4 {
		// v4 kept the declaration section at the tail; its vectors rode the
		// JSON channel (no float section, nothing to restore).
		if vecDecls, err = rd.vectorDecls(); err != nil {
			return Data{}, err
		}
	}
	if rd.i != len(rd.b) {
		return Data{}, errors.New("store: trailing garbage")
	}
	return Data{Nodes: nodes, Edges: edges, IndexKeys: indexKeys, VectorDecls: vecDecls}, nil
}
