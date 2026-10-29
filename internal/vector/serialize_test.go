package vector

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"math/rand"
	"reflect"
	"testing"
)

// buildForestState builds a State exercising every serialized shape: four
// full packs whose fourth cascades a 4-way merge into tier 1, dead bits
// (deletes over packed vectors and over the overlay tail), a same-key
// update riding the overlay, and a non-empty overlay tail.
func buildForestState() *State {
	const dim = 16
	rng := rand.New(rand.NewSource(42))
	s := NewState(Cosine, dim, 8, 64, 7)
	for b := 0; b < 4; b++ {
		for i := 0; i < packSize; i++ {
			s.Add(fmt.Sprintf("k%d", b*packSize+i), normalizeRand(rng, dim))
		}
		s.Pack(false)
	}
	s.Delete("k5000")
	s.Delete("k7000")                      // dead bits over packed vectors
	s.Add("k100", normalizeRand(rng, dim)) // update: overlay beats segment
	for i := 0; i < 100; i++ {
		s.Add(fmt.Sprintf("t%d", i), normalizeRand(rng, dim))
	}
	s.Delete("t5") // dead overlay entry
	return s
}

// hitsIdentical reports bit-exact hit equality: same length, order, keys,
// and score bits (floats ride the wire as raw bits, so anything looser
// would hide a lost-precision bug).
func hitsIdentical(a, b []VectorHit) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Key != b[i].Key || math.Float64bits(a[i].Score) != math.Float64bits(b[i].Score) {
			return false
		}
	}
	return true
}

func TestStateEncodeDecodeIdentity(t *testing.T) {
	s := buildForestState()
	if len(s.tiers) < 2 || len(s.tiers[1]) == 0 {
		t.Fatalf("fixture must cascade into tier 1, got %d tiers", len(s.tiers))
	}
	if s.OverlayLen() == 0 || len(s.dead.chunks) == 0 {
		t.Fatal("fixture must hold an overlay tail and dead bits")
	}
	const dim = 16
	qrng := rand.New(rand.NewSource(99))
	queries := make([][]float32, 12)
	for i := range queries {
		queries[i] = normalizeRand(qrng, dim)
	}
	pre := make([][]VectorHit, len(queries))
	r := s.NewReader()
	for i, q := range queries {
		pre[i] = s.SearchReader(q, 10, 128, r)
	}

	var buf bytes.Buffer
	if err := s.Encode(&buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	dec, err := DecodeState(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("DecodeState: %v", err)
	}

	// Structural identity: every persisted field comes back bit-exact.
	if dec.metric != s.metric || dec.dim != s.dim || dec.m != s.m || dec.efc != s.efc || dec.seed != s.seed {
		t.Fatal("config must round-trip")
	}
	if dec.nextVID != s.nextVID || dec.seq != s.seq {
		t.Fatal("vid space and segment seq allocator must round-trip")
	}
	if !reflect.DeepEqual(dec.vidOf, s.vidOf) {
		t.Fatal("vidOf must round-trip")
	}
	if !reflect.DeepEqual(dec.dead.chunks, s.dead.chunks) {
		t.Fatal("dead bits must round-trip")
	}
	if !reflect.DeepEqual(dec.overlay, s.overlay) {
		t.Fatal("overlay chunks must round-trip")
	}
	if dec.OverlayLen() != s.OverlayLen() || dec.Len() != s.Len() {
		t.Fatal("derived counts must match")
	}
	if len(dec.tiers) != len(s.tiers) {
		t.Fatalf("tier count changed: %d vs %d", len(dec.tiers), len(s.tiers))
	}
	for ti, tier := range s.tiers {
		if len(dec.tiers[ti]) != len(tier) {
			t.Fatalf("tier %d segment count changed: %d vs %d", ti, len(dec.tiers[ti]), len(tier))
		}
		for si, seg := range tier {
			d := dec.tiers[ti][si]
			if d.seq != seg.seq || !reflect.DeepEqual(d.keys, seg.keys) {
				t.Fatalf("segment %d/%d metadata changed", ti, si)
			}
			if !reflect.DeepEqual(d.h.ids, seg.h.ids) || !reflect.DeepEqual(d.h.level, seg.h.level) ||
				!reflect.DeepEqual(d.h.links, seg.h.links) || d.h.entry != seg.h.entry ||
				!reflect.DeepEqual(d.h.vecs, seg.h.vecs) {
				t.Fatalf("segment %d/%d graph changed", ti, si)
			}
		}
	}

	// Behavioral identity: same queries, bit-identical hits, live form and
	// snapshot form both.
	dr := dec.NewReader()
	for i, q := range queries {
		if got := dec.SearchReader(q, 10, 128, dr); !hitsIdentical(pre[i], got) {
			t.Fatalf("query %d: decoded live hits diverge\n pre: %+v\npost: %+v", i, pre[i], got)
		}
	}
	snap := dec.Snapshot()
	sr := dec.NewReader()
	for i, q := range queries {
		if got := snap.Search(q, 10, 128, sr); !hitsIdentical(pre[i], got) {
			t.Fatalf("query %d: decoded snapshot hits diverge\n pre: %+v\npost: %+v", i, pre[i], got)
		}
	}
}

func TestStateEncodeDecodeEmpty(t *testing.T) {
	s := NewState(L2, 8, 16, 200, 3)
	var buf bytes.Buffer
	if err := s.Encode(&buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	dec, err := DecodeState(&buf)
	if err != nil {
		t.Fatalf("DecodeState: %v", err)
	}
	if got := dec.Search([]float32{1, 0, 0, 0, 0, 0, 0, 0}, 1, 8); len(got) != 0 {
		t.Fatalf("empty state must stay empty, got %+v", got)
	}
	// Immediately usable: a decoded empty state must accept writes (vidOf
	// decodes as a live map, not nil).
	dec.Add("k", []float32{1, 0, 0, 0, 0, 0, 0, 0})
	if got := dec.Search([]float32{1, 0, 0, 0, 0, 0, 0, 0}, 1, 8); len(got) != 1 || got[0].Key != "k" {
		t.Fatalf("post-decode Add must be searchable, got %+v", got)
	}
}

func TestDecodeRejectsCorruption(t *testing.T) {
	s := NewState(Cosine, 4, 4, 32, 1)
	s.Add("k", Normalize([]float32{1, 0, 0, 0}))
	s.Add("j", Normalize([]float32{0, 1, 0, 0}))
	s.Pack(true)
	s.Add("tail", Normalize([]float32{0, 0, 1, 0}))
	s.Delete("j")
	var buf bytes.Buffer
	if err := s.Encode(&buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	raw := buf.Bytes()

	// Any single flipped byte (magic, version, body, CRC trailer) fails:
	// CRC32 catches every <=32-bit burst, and the header checks catch the
	// magic/version before the body is ever parsed.
	for _, pos := range []int{0, 4, len(raw) / 2, len(raw) - 1} {
		corrupt := append([]byte(nil), raw...)
		corrupt[pos] ^= 0xff
		if _, err := DecodeState(bytes.NewReader(corrupt)); err == nil {
			t.Fatalf("byte flip at %d must be rejected", pos)
		}
	}
	if _, err := DecodeState(bytes.NewReader(raw[:len(raw)-1])); err == nil {
		t.Fatal("truncated input must be rejected")
	}

	// Forged vidOf count under a recomputed CRC: the lower-bound check must
	// hold independently of the CRC (offset: header 6 + metric 1 + dim 4 +
	// m 4 + efc 4 + seed 8 + seq 8 + nextVID 4 = 39).
	forged := append([]byte(nil), raw...)
	binary.LittleEndian.PutUint32(forged[39:], math.MaxUint32)
	binary.LittleEndian.PutUint32(forged[len(forged)-4:], crc32.ChecksumIEEE(forged[:len(forged)-4]))
	if _, err := DecodeState(bytes.NewReader(forged)); err == nil {
		t.Fatal("forged count must be rejected by the lower-bound check despite a valid CRC")
	}
}
