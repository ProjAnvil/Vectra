package vector

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"
)

// Forest serialization (spec 2026-10-21 §2.2). Segments are immutable once
// built - every rebuild path (Pack, mergeSegments, compact, PackPlan.Build)
// constructs a fresh segment - so the graphs persist as-built and load never
// rebuilds them. rng state and build scratch stay behind: a decoded HNSW
// gets a fresh rng it can only reach through an insert that no existing
// segment ever takes; the scratch fields are lazily grown on first use.
//
// Layout ("VCS1", little-endian, version 1):
//
//	u8 metric | u32 dim | u32 m | u32 efc | u64 seed | u64 seq | u32 nextVID
//	u32 vidOf count | { u32 keyLen, key bytes, u32 vid }*
//	u32 dead chunk count | { u32 chunkID, 1024 x u64 }*
//	u32 overlay chunk count | { u32 entry count, { u32 keyLen, key bytes,
//	  u32 vid, dim x f32 raw bits }* }*
//	u32 tier count | { u32 segment count | { u64 seq, i32 entry, u32 node
//	  count, node header { u32 id, u32 level }*, links { (level+1) x
//	  [u32 count, count x u32] }*, vecs { dim x f32 }*, keys { str }* }* }*
//	u32 CRC32 (IEEE, covering all preceding bytes)
//
// Vectors ride the wire as raw float32 bits (trivially exact round-trip);
// a segment's vecs/keys are dim-implicit, keyed off the config section.
const (
	vcMagic    = "VCS1"
	vcVersion  = uint16(1)
	vcHeaderSz = 6 // magic(4) + version(2); fields start after this
)

var errVCTruncated = errors.New("vector: truncated input")

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

func vcPutU8(w io.Writer, v uint8) error {
	var b [1]byte
	b[0] = v
	_, err := w.Write(b[:])
	return err
}

func vcPutU32(w io.Writer, v uint32) error {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	_, err := w.Write(b[:])
	return err
}

func vcPutU64(w io.Writer, v uint64) error {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	_, err := w.Write(b[:])
	return err
}

func vcPutStr(w io.Writer, s string) error {
	if err := vcPutU32(w, uint32(len(s))); err != nil {
		return err
	}
	_, err := io.WriteString(w, s)
	return err
}

// vcPutF32s writes one stored-form vector as raw little-endian bits.
func vcPutF32s(w io.Writer, v []float32) error {
	var b [4]byte
	for _, f := range v {
		binary.LittleEndian.PutUint32(b[:], math.Float32bits(f))
		if _, err := w.Write(b[:]); err != nil {
			return err
		}
	}
	return nil
}

// Encode writes s's full state: config (metric/dim/m/efc/seed), vid
// space (nextVID, vidOf), deadBits, overlay entries, and the tiered
// segment forest (per segment: tier, seq, HNSW ids/vecs/level/links/
// entry, keys). Format: magic "VCS1" | fields... | CRC32 trailer. rng
// state and build scratch are not persisted (segments are immutable).
func (s *State) Encode(w io.Writer) error {
	cw := &crcWriter{bw: bufio.NewWriter(w)}
	var head [vcHeaderSz]byte
	copy(head[:4], vcMagic)
	binary.LittleEndian.PutUint16(head[4:6], vcVersion)
	if _, err := cw.Write(head[:]); err != nil {
		return err
	}
	if err := vcPutU8(cw, uint8(s.metric)); err != nil {
		return err
	}
	if err := vcPutU32(cw, uint32(s.dim)); err != nil {
		return err
	}
	if err := vcPutU32(cw, uint32(s.m)); err != nil {
		return err
	}
	if err := vcPutU32(cw, uint32(s.efc)); err != nil {
		return err
	}
	if err := vcPutU64(cw, uint64(s.seed)); err != nil {
		return err
	}
	if err := vcPutU64(cw, s.seq); err != nil {
		return err
	}
	if err := vcPutU32(cw, s.nextVID); err != nil {
		return err
	}
	if err := vcPutU32(cw, uint32(len(s.vidOf))); err != nil {
		return err
	}
	for k, vid := range s.vidOf {
		if err := vcPutStr(cw, k); err != nil {
			return err
		}
		if err := vcPutU32(cw, vid); err != nil {
			return err
		}
	}
	if err := vcPutU32(cw, uint32(len(s.dead.chunks))); err != nil {
		return err
	}
	for c, bits := range s.dead.chunks {
		if err := vcPutU32(cw, c); err != nil {
			return err
		}
		for _, w := range bits {
			if err := vcPutU64(cw, w); err != nil {
				return err
			}
		}
	}
	if err := vcPutU32(cw, uint32(len(s.overlay))); err != nil {
		return err
	}
	for _, ch := range s.overlay {
		if err := vcPutU32(cw, uint32(len(ch))); err != nil {
			return err
		}
		for _, e := range ch {
			if err := vcPutStr(cw, e.key); err != nil {
				return err
			}
			if err := vcPutU32(cw, e.vid); err != nil {
				return err
			}
			if err := vcPutF32s(cw, e.vec); err != nil {
				return err
			}
		}
	}
	if err := vcPutU32(cw, uint32(len(s.tiers))); err != nil {
		return err
	}
	for _, tier := range s.tiers {
		if err := vcPutU32(cw, uint32(len(tier))); err != nil {
			return err
		}
		for _, seg := range tier {
			if err := vcPutSegment(cw, seg); err != nil {
				return err
			}
		}
	}
	return cw.finish()
}

func vcPutSegment(cw *crcWriter, seg *segment) error {
	h := seg.h
	if err := vcPutU64(cw, seg.seq); err != nil {
		return err
	}
	if err := vcPutU32(cw, uint32(int32(h.entry))); err != nil {
		return err
	}
	if err := vcPutU32(cw, uint32(len(h.ids))); err != nil {
		return err
	}
	for i, id := range h.ids {
		if err := vcPutU32(cw, id); err != nil {
			return err
		}
		if err := vcPutU32(cw, uint32(h.level[i])); err != nil {
			return err
		}
	}
	for _, lsts := range h.links {
		for _, lst := range lsts {
			if err := vcPutU32(cw, uint32(len(lst))); err != nil {
				return err
			}
			for _, n := range lst {
				if err := vcPutU32(cw, n); err != nil {
					return err
				}
			}
		}
	}
	for _, v := range h.vecs {
		if err := vcPutF32s(cw, v); err != nil {
			return err
		}
	}
	for _, k := range seg.keys {
		if err := vcPutStr(cw, k); err != nil {
			return err
		}
	}
	return nil
}

// decReader consumes a byte slice directly: every length field is
// bounds-checked against the remaining bytes, so bad input can never
// trigger oversized allocations.
type decReader struct {
	b []byte
	i int
}

func (rd *decReader) remain() int { return len(rd.b) - rd.i }

func (rd *decReader) take(n int) ([]byte, error) {
	if n < 0 || rd.i+n > len(rd.b) {
		return nil, errVCTruncated
	}
	p := rd.b[rd.i : rd.i+n]
	rd.i += n
	return p, nil
}

func (rd *decReader) u8() (uint8, error) {
	p, err := rd.take(1)
	if err != nil {
		return 0, err
	}
	return p[0], nil
}

func (rd *decReader) u32() (uint32, error) {
	p, err := rd.take(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(p), nil
}

func (rd *decReader) u64() (uint64, error) {
	p, err := rd.take(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(p), nil
}

func (rd *decReader) str() (string, error) {
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

// DecodeState restores what Encode wrote; bad magic/version/CRC = error.
// Count fields carry lower-bound guards (minimum wire bytes per element)
// that hold independently of the CRC - a CRC'd file can be forged by
// anyone who recomputes the CRC themselves - so forged counts fail as
// truncation before any preallocation happens.
func DecodeState(r io.Reader) (*State, error) {
	raw, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if len(raw) < vcHeaderSz+4 {
		return nil, errVCTruncated
	}
	if string(raw[:4]) != vcMagic {
		return nil, errors.New("vector: bad magic")
	}
	if ver := binary.LittleEndian.Uint16(raw[4:6]); ver != vcVersion {
		return nil, errors.New("vector: unsupported version")
	}
	body := raw[:len(raw)-4]
	if want := binary.LittleEndian.Uint32(raw[len(raw)-4:]); crc32.ChecksumIEEE(body) != want {
		return nil, errors.New("vector: CRC mismatch")
	}
	rd := &decReader{b: body[vcHeaderSz:]}

	metric, err := rd.u8()
	if err != nil {
		return nil, err
	}
	if metric > uint8(Dot) {
		return nil, errors.New("vector: bad metric")
	}
	dim, err := rd.u32()
	if err != nil {
		return nil, err
	}
	m, err := rd.u32()
	if err != nil {
		return nil, err
	}
	efc, err := rd.u32()
	if err != nil {
		return nil, err
	}
	// Sane caps keep every later int conversion safe and stop absurd (but
	// well-formed) configs from wasting decode time.
	if dim == 0 || dim > 1<<24 || m == 0 || m > 1<<20 || efc == 0 || efc > 1<<24 {
		return nil, errors.New("vector: bad config")
	}
	seed, err := rd.u64()
	if err != nil {
		return nil, err
	}
	seq, err := rd.u64()
	if err != nil {
		return nil, err
	}
	nextVID, err := rd.u32()
	if err != nil {
		return nil, err
	}
	s := NewState(Metric(metric), int(dim), int(m), int(efc), int64(seed))
	s.seq = seq
	s.nextVID = nextVID

	nk, err := rd.u32()
	if err != nil {
		return nil, err
	}
	// Forged-count guard: 8 bytes per vidOf entry minimum (keyLen u32 + vid).
	if int64(nk)*8 > int64(rd.remain()) {
		return nil, errVCTruncated
	}
	s.vidOf = make(map[string]uint32, nk)
	for i := uint32(0); i < nk; i++ {
		k, err := rd.str()
		if err != nil {
			return nil, err
		}
		vid, err := rd.u32()
		if err != nil {
			return nil, err
		}
		s.vidOf[k] = vid
	}

	nc, err := rd.u32()
	if err != nil {
		return nil, err
	}
	// 4 (chunkID) + 8192 (1024 x u64) bytes per dead chunk minimum.
	if int64(nc)*(4+8192) > int64(rd.remain()) {
		return nil, errVCTruncated
	}
	for i := uint32(0); i < nc; i++ {
		id, err := rd.u32()
		if err != nil {
			return nil, err
		}
		p, err := rd.take(8192)
		if err != nil {
			return nil, err
		}
		var bits [1024]uint64
		for j := range bits {
			bits[j] = binary.LittleEndian.Uint64(p[j*8:])
		}
		if s.dead.chunks == nil {
			s.dead.chunks = map[uint32][1024]uint64{}
		}
		s.dead.chunks[id] = bits
	}

	nch, err := rd.u32()
	if err != nil {
		return nil, err
	}
	if int64(nch)*4 > int64(rd.remain()) { // entry-count u32 per chunk minimum
		return nil, errVCTruncated
	}
	for c := uint32(0); c < nch; c++ {
		ne, err := rd.u32()
		if err != nil {
			return nil, err
		}
		// keyLen u32 + vid u32 + dim f32s per entry minimum.
		if int64(ne)*(8+int64(dim)*4) > int64(rd.remain()) {
			return nil, errVCTruncated
		}
		ch := make([]entry, 0, ne)
		for i := uint32(0); i < ne; i++ {
			key, err := rd.str()
			if err != nil {
				return nil, err
			}
			vid, err := rd.u32()
			if err != nil {
				return nil, err
			}
			p, err := rd.take(int(dim) * 4)
			if err != nil {
				return nil, err
			}
			vec := make([]float32, dim)
			for j := range vec {
				vec[j] = math.Float32frombits(binary.LittleEndian.Uint32(p[j*4:]))
			}
			ch = append(ch, entry{key: key, vid: vid, vec: vec})
		}
		s.overlay = append(s.overlay, ch)
		s.overlayLen += len(ch)
	}

	nt, err := rd.u32()
	if err != nil {
		return nil, err
	}
	if int64(nt)*4 > int64(rd.remain()) { // segment-count u32 per tier minimum
		return nil, errVCTruncated
	}
	for ti := uint32(0); ti < nt; ti++ {
		ns, err := rd.u32()
		if err != nil {
			return nil, err
		}
		// seq u64 + entry i32 + node-count u32 per segment minimum.
		if int64(ns)*16 > int64(rd.remain()) {
			return nil, errVCTruncated
		}
		// A cascade-emptied tier is nil in a built State: keep it nil.
		var tier []*segment
		if ns > 0 {
			tier = make([]*segment, 0, ns)
		}
		for i := uint32(0); i < ns; i++ {
			seg, err := vcReadSegment(rd, s.metric, int(dim), int(m), int(efc), int64(seed))
			if err != nil {
				return nil, err
			}
			tier = append(tier, seg)
		}
		s.tiers = append(s.tiers, tier)
	}
	if rd.i != len(rd.b) {
		return nil, errors.New("vector: trailing garbage")
	}
	return s, nil
}

// vcReadSegment restores one built graph as-built: ids, level, links,
// vecs, keys, entry. Graph invariants that search would otherwise trust
// blindly (entry range, link indices) are validated here - a forged file
// must fail with an error, not panic the reader.
func vcReadSegment(rd *decReader, metric Metric, dim, m, efc int, seed int64) (*segment, error) {
	seq, err := rd.u64()
	if err != nil {
		return nil, err
	}
	entry32, err := rd.u32()
	if err != nil {
		return nil, err
	}
	nn, err := rd.u32()
	if err != nil {
		return nil, err
	}
	entry := int(int32(entry32))
	// id u32 + level u32 + one link-count u32 + dim f32s per node minimum.
	if int64(nn)*(12+int64(dim)*4) > int64(rd.remain()) {
		return nil, errVCTruncated
	}
	h := NewHNSW(metric, m, efc, seed+int64(seq)) // fresh rng: never used, segments are immutable
	if nn == 0 {
		if entry != -1 {
			return nil, errors.New("vector: bad entry point")
		}
		return &segment{seq: seq, h: h}, nil
	}
	if entry < 0 || entry >= int(nn) {
		return nil, errors.New("vector: bad entry point")
	}
	h.ids = make([]uint32, nn)
	h.level = make([]int, nn)
	h.links = make([][][]uint32, nn)
	h.vecs = make([][]float32, nn)
	keys := make([]string, nn)
	for i := uint32(0); i < nn; i++ {
		id, err := rd.u32()
		if err != nil {
			return nil, err
		}
		lvl, err := rd.u32()
		if err != nil {
			return nil, err
		}
		// (level+1) link lists, each at least a count u32: the allocation
		// bound before make([][]uint32, lvl+1).
		if (int64(lvl)+1)*4 > int64(rd.remain()) {
			return nil, errVCTruncated
		}
		h.ids[i] = id
		h.level[i] = int(lvl)
		h.links[i] = make([][]uint32, lvl+1)
	}
	for i := uint32(0); i < nn; i++ {
		for l := range h.links[i] {
			cnt, err := rd.u32()
			if err != nil {
				return nil, err
			}
			if int64(cnt)*4 > int64(rd.remain()) {
				return nil, errVCTruncated
			}
			// A never-appended list is nil in a built graph (levels with no
			// selected neighbors): keep it nil so the decoded form matches
			// the in-memory form exactly.
			var lst []uint32
			if cnt > 0 {
				lst = make([]uint32, cnt)
				for j := uint32(0); j < cnt; j++ {
					n, err := rd.u32()
					if err != nil {
						return nil, err
					}
					if n >= nn { // links are internal indices; out-of-range panics search
						return nil, errors.New("vector: link out of range")
					}
					lst[j] = n
				}
			}
			h.links[i][l] = lst
		}
	}
	for i := uint32(0); i < nn; i++ {
		p, err := rd.take(dim * 4)
		if err != nil {
			return nil, err
		}
		vec := make([]float32, dim)
		for j := range vec {
			vec[j] = math.Float32frombits(binary.LittleEndian.Uint32(p[j*4:]))
		}
		h.vecs[i] = vec
	}
	for i := range keys {
		if keys[i], err = rd.str(); err != nil {
			return nil, err
		}
	}
	h.entry = entry
	return &segment{seq: seq, h: h, keys: keys}, nil
}
