package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/projanvil/vectra/internal/vecbin"
)

// --- helpers ---------------------------------------------------------------

// frameAt returns frame idx's type byte and payload read straight off the
// segment bytes (0 = the leading marker). The v2 tests pin the on-disk frame
// type and envelope through it, never through the decoder under test.
func frameAt(buf []byte, idx int) (byte, []byte) {
	off := frameStart(buf, idx)
	n := binary.LittleEndian.Uint64(buf[off:])
	return buf[off+12], buf[off+13 : off+12+int(n)]
}

// vecCS is the vector-carrying changeset used by the v2 tests: a []float32
// prop (the Go shape), a []any(float64) value on the same declared prop (the
// JSON/snapshot pipeline shape), a plain prop, an undeclared numeric array
// (data, not a vector), and a declared-name prop on an edge (edges are never
// indexed, spec §1.2).
func vecCS() Changeset {
	return Changeset{
		Version: 1,
		Nodes: []NodeUpsert{
			{Key: "a", Label: "Doc", Props: map[string]any{
				"emb":  []float32{1, 2, 3, 4},
				"name": "alpha",
			}},
			{Key: "b", Label: "Doc", Props: map[string]any{
				"emb":  []any{float64(5), float64(6), float64(7), float64(8)},
				"vals": []any{float64(1), float64(2)}, // undeclared: stays JSON
			}},
		},
		Edges: []EdgeUpsert{{SrcKey: "a", DstKey: "b", Label: "NEAR", Props: map[string]any{
			"emb": []float32{9, 9, 9, 9},
		}}},
	}
}

// --- tests -----------------------------------------------------------------

// TestAppendHoistedV2RoundTrip: a vector changeset through AppendHoisted
// replays with the vector props back as []float32 and every other prop riding
// the JSON channel exactly as v1 would.
func TestAppendHoistedV2RoundTrip(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, ModeAlways)
	if err != nil {
		t.Fatal(err)
	}
	decls := vecbin.PropDecls{"emb": 4}
	lsn, err := w.AppendHoisted(vecCS(), decls)
	if err != nil || lsn != 1 {
		t.Fatalf("AppendHoisted: lsn=%d err=%v; want 1, nil", lsn, err)
	}
	// The caller's changeset must come back untouched — no $vec refs, no
	// widened values.
	in := vecCS()
	if _, err := w.AppendHoisted(in, decls); err != nil {
		t.Fatal(err)
	}
	if v, ok := in.Nodes[0].Props["emb"].([]float32); !ok || !slices.Equal(v, []float32{1, 2, 3, 4}) {
		t.Fatalf("AppendHoisted mutated its input: %#v", in.Nodes[0].Props)
	}
	if len(in.Nodes[0].Props) != 2 {
		t.Fatalf("input props grown: %#v", in.Nodes[0].Props)
	}
	// v1 rejects non-finite floats at json.Marshal time; the binary section
	// would carry them silently, so v2 must refuse them the same way.
	nan := Changeset{Version: 1, Nodes: []NodeUpsert{{Key: "nan", Label: "Doc",
		Props: map[string]any{"emb": []float32{float32(math.NaN())}}}}}
	if _, err := w.AppendHoisted(nan, decls); err == nil {
		t.Fatalf("NaN vector: AppendHoisted succeeded, want the v1-style rejection")
	}
	w.Close()

	frames, lsns, last := collect(t, dir)
	if len(frames) != 2 || last != 2 || lsns[0] != 1 || lsns[1] != 2 {
		t.Fatalf("replay: %d frames lsns=%v last=%d; want 2, [1 2], 2", len(frames), lsns, last)
	}
	got := frames[0]
	if len(got.Nodes) != 2 || len(got.Edges) != 1 {
		t.Fatalf("changeset shape: %+v", got)
	}
	v, ok := got.Nodes[0].Props["emb"].([]float32)
	if !ok || !slices.Equal(v, []float32{1, 2, 3, 4}) {
		t.Fatalf("node a emb = %#v, want []float32{1,2,3,4}", got.Nodes[0].Props["emb"])
	}
	if got.Nodes[0].Props["name"] != "alpha" {
		t.Fatalf("node a name = %#v, want alpha", got.Nodes[0].Props["name"])
	}
	// The JSON-pipeline shape ([]any of float64) hoists identically.
	v, ok = got.Nodes[1].Props["emb"].([]float32)
	if !ok || !slices.Equal(v, []float32{5, 6, 7, 8}) {
		t.Fatalf("node b emb = %#v, want []float32{5,6,7,8}", got.Nodes[1].Props["emb"])
	}
	// Undeclared numeric arrays are data: JSON round-trip like any v1 prop.
	vals, ok := got.Nodes[1].Props["vals"].([]any)
	if !ok || len(vals) != 2 || vals[0] != float64(1) || vals[1] != float64(2) {
		t.Fatalf("node b vals = %#v, want []any{1,2} (JSON channel)", got.Nodes[1].Props["vals"])
	}
	// Edge props are never indexed: a declared name on an edge stays JSON.
	ev, ok := got.Edges[0].Props["emb"].([]any)
	if !ok || len(ev) != 4 || ev[0] != float64(9) {
		t.Fatalf("edge emb = %#v, want []any of 4x9 (edges never hoist)", got.Edges[0].Props["emb"])
	}
}

// TestAppendHoistedFallsBackToV1: when nothing hoists — no declared prop in
// the data, a declared prop holding non-vector data, or no declarations at
// all — the on-disk frame is a plain type-1 frame byte-identical to Append's.
func TestAppendHoistedFallsBackToV1(t *testing.T) {
	decls := vecbin.PropDecls{"emb": 4}
	strCS := Changeset{Version: 1, Nodes: []NodeUpsert{{Key: "s", Label: "Doc",
		Props: map[string]any{"emb": "text-not-a-vector"}}}}
	cases := []struct {
		cs    Changeset
		decls vecbin.PropDecls
	}{
		{testCS(1), decls}, // no declared prop present at all
		{strCS, decls},     // declared prop holding non-vector data: not this codec's call
		{testCS(3), nil},   // no declarations whatsoever
	}
	run := func(dir string, hoist bool) {
		w, err := Open(dir, ModeAlways)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range cases {
			var err error
			if hoist {
				_, err = w.AppendHoisted(c.cs, c.decls)
			} else {
				_, err = w.Append(c.cs)
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	hoistedDir, plainDir := t.TempDir(), t.TempDir()
	run(hoistedDir, true)
	run(plainDir, false)

	got := segmentFile(t, hoistedDir, "wal-0.log")
	want := segmentFile(t, plainDir, "wal-0.log")
	for idx := 1; idx <= len(cases); idx++ {
		gt, gp := frameAt(got, idx)
		if gt != frameChangeset {
			t.Fatalf("frame %d type = %d, want %d (pure-graph changesets must stay v1)", idx, gt, frameChangeset)
		}
		wt, wp := frameAt(want, idx)
		if wt != frameChangeset || !bytes.Equal(gp, wp) {
			t.Fatalf("frame %d payload differs from plain Append:\n got %s\nwant %s", idx, gp, wp)
		}
	}
	if frames, _, last := collect(t, hoistedDir); len(frames) != 3 || last != 3 {
		t.Fatalf("fallback replay: %d frames last=%d; want 3, 3", len(frames), last)
	}
}

// TestMixedV1V2ChainReplay: v1, v2 and v1 frames interleave in one segment;
// replay applies all of them in order, and recovery parks the write head
// after the v2 frame (it consumes an LSN like every non-marker frame).
func TestMixedV1V2ChainReplay(t *testing.T) {
	decls := vecbin.PropDecls{"emb": 4}
	dir := t.TempDir()
	w, err := Open(dir, ModeAlways)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(testCS(1)); err != nil { // v1
		t.Fatal(err)
	}
	if _, err := w.AppendHoisted(vecCS(), decls); err != nil { // v2
		t.Fatal(err)
	}
	if _, err := w.Append(testCS(2)); err != nil { // v1
		t.Fatal(err)
	}
	w.Close()

	buf := segmentFile(t, dir, "wal-0.log")
	for i, want := range []byte{frameChangeset, frameChangesetV2, frameChangeset} {
		if typ, _ := frameAt(buf, i+1); typ != want {
			t.Fatalf("frame %d type = %d, want %d", i+1, typ, want)
		}
	}

	frames, lsns, last := collect(t, dir)
	if len(frames) != 3 || last != 3 {
		t.Fatalf("replay: %d frames last=%d; want 3, 3", len(frames), last)
	}
	for i, l := range lsns {
		if l != uint64(i+1) {
			t.Fatalf("lsns = %v, want 1..3", lsns)
		}
	}
	if frames[0].Nodes[0].Props["id"] != float64(1) {
		t.Fatalf("frame 0 props = %#v", frames[0].Nodes[0].Props)
	}
	v, ok := frames[1].Nodes[0].Props["emb"].([]float32)
	if !ok || !slices.Equal(v, []float32{1, 2, 3, 4}) {
		t.Fatalf("frame 1 (v2) emb = %#v, want []float32{1,2,3,4}", frames[1].Nodes[0].Props["emb"])
	}
	if frames[2].Nodes[0].Props["id"] != float64(2) {
		t.Fatalf("frame 2 props = %#v", frames[2].Nodes[0].Props)
	}

	// Recovery: if the v2 frame were not counted, the head would park before
	// it and the next Append would overwrite live frames.
	w2, err := Open(dir, ModeAlways)
	if err != nil {
		t.Fatal(err)
	}
	if got := w2.LastLSN(); got != 3 {
		t.Fatalf("recovered LastLSN = %d, want 3", got)
	}
	if _, err := w2.Append(testCS(9)); err != nil {
		t.Fatal(err)
	}
	w2.Close()
	if frames, _, last = collect(t, dir); len(frames) != 4 || last != 4 {
		t.Fatalf("post-recovery: %d frames last=%d; want 4, 4 (v2 frame overwritten?)", len(frames), last)
	}
}

// TestV2EnvelopeSelfDescribing pins the wire layout: the hint section records
// exactly the props hoisted in THIS frame (name + dim), the skeleton carries
// {"$vec":[off,dim]} refs, and the payload tail is the raw float32 LE
// section. Replay therefore needs no cross-frame declaration state — no
// IndexDecl frame ever precedes these frames, before or after a Reset. A
// CRC-intact frame with a forged out-of-range ref must surface an error
// rather than silently truncate the frames behind it.
func TestV2EnvelopeSelfDescribing(t *testing.T) {
	decls := vecbin.PropDecls{"emb": 4, "never": 8} // "never" is declared but absent from the data
	dir := t.TempDir()
	w, err := Open(dir, ModeAlways)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.AppendHoisted(vecCS(), decls); err != nil {
		t.Fatal(err)
	}
	w.Close()

	buf := segmentFile(t, dir, "wal-0.log")
	typ, payload := frameAt(buf, 1)
	if typ != frameChangesetV2 {
		t.Fatalf("frame type = %d, want %d", typ, frameChangesetV2)
	}
	// Hand-parse the envelope so the assertions pin bytes, not the codec:
	// [4B "VVB2"][u16 nProps][u16 nameLen+name+u16 dim]*[u32 jsonLen][skeleton][floats]
	if !bytes.Equal(payload[:4], []byte("VVB2")) {
		t.Fatalf("magic = %q", payload[:4])
	}
	if n := binary.LittleEndian.Uint16(payload[4:6]); n != 1 {
		t.Fatalf("hint nProps = %d, want 1 (only props actually hoisted)", n)
	}
	if l := binary.LittleEndian.Uint16(payload[6:8]); int(l) != len("emb") {
		t.Fatalf("hint nameLen = %d, want 3", l)
	}
	if string(payload[8:11]) != "emb" {
		t.Fatalf("hint name = %q", payload[8:11])
	}
	if d := binary.LittleEndian.Uint16(payload[11:13]); d != 4 {
		t.Fatalf("hint dim = %d, want 4", d)
	}
	jl := int(binary.LittleEndian.Uint32(payload[13:17]))
	skeleton := payload[17 : 17+jl]
	if !bytes.Contains(skeleton, []byte(`"emb":{"$vec":[0,4]}`)) {
		t.Fatalf("node a ref missing from skeleton: %s", skeleton)
	}
	if !bytes.Contains(skeleton, []byte(`"emb":{"$vec":[4,4]}`)) {
		t.Fatalf("node b ref missing from skeleton: %s", skeleton)
	}
	// The edge's 9s live in the JSON skeleton, not the section: 2 vectors x
	// 4 dims = 8 floats and nothing else.
	sec := payload[17+jl:]
	if len(sec) != 8*4 {
		t.Fatalf("float section len = %d, want 32", len(sec))
	}
	for i, want := range []float32{1, 2, 3, 4, 5, 6, 7, 8} {
		if got := math.Float32frombits(binary.LittleEndian.Uint32(sec[4*i:])); got != want {
			t.Fatalf("section[%d] = %v, want %v", i, got, want)
		}
	}

	// No declaration frame exists anywhere in this log, yet replay resolves
	// the refs from the frame alone (self-describing).
	frames, _, _ := collect(t, dir)
	v, ok := frames[0].Nodes[0].Props["emb"].([]float32)
	if !ok || !slices.Equal(v, []float32{1, 2, 3, 4}) {
		t.Fatalf("stateless restore = %#v, want []float32{1,2,3,4}", frames[0].Nodes[0].Props["emb"])
	}

	// Forged out-of-range ref: the CRC is recomputed so scanFrames accepts
	// the frame — only the decode path can catch it, and it must fail loudly.
	forged := t.TempDir()
	skel := []byte(`{"version":1,"nodes":[{"key":"f","label":"Doc","props":{"emb":{"$vec":[999,4]}}}]}`)
	p := appendV2Envelope(nil, []v2Hint{{name: "emb", dim: 4}}, skel, []float32{1, 2, 3, 4})
	mj, err := json.Marshal(marker{Salt: 0, Base: 1})
	if err != nil {
		t.Fatal(err)
	}
	var fb []byte
	fb = appendFrame(fb, frameMarker, mj)
	fb = appendFrame(fb, frameChangesetV2, p)
	if err := os.WriteFile(filepath.Join(forged, "wal-0.log"), fb, 0o644); err != nil {
		t.Fatal(err)
	}
	fw, err := Open(forged, ModeAlways) // recovery scan accepts type 4 without decoding payloads
	if err != nil {
		t.Fatalf("open forged: %v", err)
	}
	defer fw.Close()
	applied := 0
	if _, err := fw.Replay(func(Frame) error { applied++; return nil }); err == nil {
		t.Fatalf("forged out-of-range $vec ref: replay succeeded, want error")
	} else if applied != 0 {
		t.Fatalf("forged frame applied %d frames, want 0", applied)
	}
}

// TestDropSegmentsBelowCovered pins the async compact's busy-case WAL
// primitive (2026-10-24 fix arc, spec item 2): segment-granular deletion of
// fully-covered retained segments. A segment's frames run from its marker
// base to the next segment's base minus one, so coverage is decidable from
// the in-memory base map; the ACTIVE segment always counts as uncovered
// (the write head lives there). Discipline under test: only whole segments
// below the named survivor go, retained bytes shrink, and the surviving
// chain reopens and replays cleanly.
func TestDropSegmentsBelowCovered(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, ModeAlways)
	if err != nil {
		t.Fatal(err)
	}
	// Three retained segments (append + Rotate each) plus frames in the
	// active fourth segment.
	var lsn1, lsn3, lsn4 uint64
	for round := 0; round < 3; round++ {
		l, err := w.Append(testCS(round + 1))
		if err != nil {
			t.Fatal(err)
		}
		switch round {
		case 0:
			lsn1 = l
		case 2:
			lsn3 = l
		}
		if err := w.Rotate(); err != nil {
			t.Fatal(err)
		}
	}
	if lsn4, err = w.Append(testCS(4)); err != nil {
		t.Fatal(err)
	}
	if lsn1 == 0 || lsn3 <= lsn1 || lsn4 <= lsn3 {
		t.Fatalf("unexpected lsn layout: %d %d %d", lsn1, lsn3, lsn4)
	}
	active := w.Segment()
	retainedBefore := w.RetainedBytes()

	// Cover through lsn3: segments 0..2 are fully covered, the active
	// segment holds lsn4 > cover and must be the first survivor.
	if got := w.FirstUncoveredSegment(lsn3); got != active {
		t.Fatalf("FirstUncoveredSegment(cover=lsn3)=%d, want the active segment %d", got, active)
	}
	// Covering even the active's frames keeps it the first survivor: the
	// write head must never be dropped.
	if got := w.FirstUncoveredSegment(lsn4); got != active {
		t.Fatalf("FirstUncoveredSegment(cover=lsn4)=%d, want the active segment %d", got, active)
	}

	// The discipline under test: the re-anchoring manifest lands FIRST
	// (LSN = the cover, naming the survivor), then the covered segments
	// go. Without the manifest, recovery's reconciliation invariant (first
	// replayed lsn == baseLSN+1) correctly refuses the surviving chain.
	if err := WriteManifest(dir, Manifest{Snapshot: "v-test.db", LSN: lsn3, Segment: active, RetainFrom: active, Format: 2}); err != nil {
		t.Fatal(err)
	}
	if err := w.DropSegmentsBelow(active); err != nil {
		t.Fatal(err)
	}
	segs, err := listSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 1 || segs[0] != active {
		t.Fatalf("segments after drop: %v, want only the active %d", segs, active)
	}
	// RetainedBytes is the next recovery's replay volume: the dropped
	// chain is gone but the ACTIVE segment's own frames remain counted.
	if got := w.RetainedBytes(); got <= 0 || got >= retainedBefore {
		t.Fatalf("retained after drop: %d (before %d, want the active segment's frames only)", got, retainedBefore)
	}

	// The surviving chain stays appendable and replayable: more frames,
	// reopen, replay must deliver exactly the post-drop frames.
	for i := 5; i <= 6; i++ {
		if _, err := w.Append(testCS(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w2, err := Open(dir, ModeAlways)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	var replayed []uint64
	if _, err := w2.Replay(func(fr Frame) error {
		replayed = append(replayed, fr.LSN)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := []uint64{lsn4, lsn4 + 1, lsn4 + 2}
	if len(replayed) != len(want) {
		t.Fatalf("replayed %d frames, want %d", len(replayed), len(want))
	}
	for i := range want {
		if replayed[i] != want[i] {
			t.Fatalf("replayed[%d]=%d, want %d", i, replayed[i], want[i])
		}
	}
}
