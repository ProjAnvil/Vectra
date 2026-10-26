package wal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// --- helpers ---------------------------------------------------------------

func testCS(id int) Changeset {
	return Changeset{
		Version: 1,
		Nodes:   []NodeUpsert{{Key: "n", Label: "Svc", Props: map[string]any{"id": float64(id)}}},
		Edges: []EdgeUpsert{
			{SrcKey: "n", DstKey: "m", Label: "CALLS", Props: map[string]any{"w": 1.5}, AllowParallel: true},
		},
		EdgeDeletes: []EdgeDelete{{SrcKey: "n", DstKey: "m", Label: "CALLS"}},
		NodeDeletes: []string{"gone"},
	}
}

// collect reopens the WAL dir and replays with LSN visibility (white box).
func collect(t *testing.T, dir string) (frames []Changeset, lsns []uint64, last uint64) {
	t.Helper()
	w, err := Open(dir, ModeAlways)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()
	last, err = w.replayLSN(func(lsn uint64, fr Frame) error {
		if fr.Changeset != nil {
			frames = append(frames, *fr.Changeset)
		}
		lsns = append(lsns, lsn)
		return nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	return frames, lsns, last
}

func segmentFile(t *testing.T, dir, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// frameStart returns the byte offset of frame idx (0 = the leading marker).
func frameStart(buf []byte, idx int) int {
	off := 0
	for i := 0; i < idx; i++ {
		n := binary.LittleEndian.Uint64(buf[off:])
		off += 12 + int(n)
	}
	return off
}

// writeSegment crafts a segment file by hand (white box).
func writeSegment(t *testing.T, dir, name string, salt uint32, base uint64, frames ...Changeset) {
	t.Helper()
	mj, err := json.Marshal(marker{Salt: salt, Base: base})
	if err != nil {
		t.Fatal(err)
	}
	var buf []byte
	buf = appendFrame(buf, frameMarker, mj)
	for _, cs := range frames {
		cj, err := json.Marshal(cs)
		if err != nil {
			t.Fatal(err)
		}
		buf = appendFrame(buf, frameChangeset, cj)
	}
	if err := os.WriteFile(filepath.Join(dir, name), buf, 0o644); err != nil {
		t.Fatal(err)
	}
}

// --- tests -----------------------------------------------------------------

func TestFrameRoundtripAndZeroTail(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, ModeAlways)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		if _, err := w.Append(testCS(i)); err != nil {
			t.Fatal(err)
		}
	}
	// Preallocation: a fresh segment is zero-extended to the segment size.
	if fi, err := os.Stat(filepath.Join(dir, "wal-0.log")); err != nil {
		t.Fatal(err)
	} else if fi.Size() != int64(segmentSize) {
		t.Fatalf("segment size = %d, want %d (preallocated)", fi.Size(), segmentSize)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	frames, lsns, last := collect(t, dir)
	if len(frames) != 3 || last != 3 {
		t.Fatalf("replay: %d frames, lastLSN %d; want 3, 3", len(frames), last)
	}
	for i, l := range lsns {
		if l != uint64(i+1) {
			t.Fatalf("lsns = %v, want 1..3", lsns)
		}
	}
	want := testCS(1)
	if frames[0].Version != want.Version || len(frames[0].Nodes) != 1 || frames[0].Nodes[0].Key != "n" {
		t.Fatalf("frame 0 = %+v", frames[0])
	}
	if frames[0].Nodes[0].Props["id"] != float64(1) {
		t.Fatalf("props roundtrip = %#v", frames[0].Nodes[0].Props)
	}
	if len(frames[0].Edges) != 1 || frames[0].Edges[0].AllowParallel != true {
		t.Fatalf("edges roundtrip = %+v", frames[0].Edges)
	}
	if len(frames[0].EdgeDeletes) != 1 || frames[0].EdgeDeletes[0].Label != "CALLS" {
		t.Fatalf("edgeDeletes roundtrip = %+v", frames[0].EdgeDeletes)
	}
	if len(frames[0].NodeDeletes) != 1 || frames[0].NodeDeletes[0] != "gone" {
		t.Fatalf("nodeDeletes roundtrip = %+v", frames[0].NodeDeletes)
	}
}

func TestFreshSegmentReplayEmpty(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, ModeAlways)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	frames, _, last := collect(t, dir)
	if len(frames) != 0 || last != 0 {
		t.Fatalf("empty WAL replay: %d frames, lastLSN %d; want 0, 0", len(frames), last)
	}
	// Appending after a zero-tail scan overwrites the preallocated zeros.
	if _, err := w.Append(testCS(1)); err != nil {
		t.Fatal(err)
	}
	frames, lsns, last := collect(t, dir)
	if len(frames) != 1 || last != 1 || lsns[0] != 1 {
		t.Fatalf("post-zero-tail append: frames=%d last=%d lsns=%v", len(frames), last, lsns)
	}
}

func TestReplayStopsOnCRCMismatch(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, ModeAlways)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		w.Append(testCS(i))
	}
	w.Close()

	name := filepath.Join(dir, "wal-0.log")
	buf := segmentFile(t, dir, "wal-0.log")
	off := frameStart(buf, 2) + 13 // first payload byte of the 2nd Changeset frame
	buf[off] ^= 0xFF
	if err := os.WriteFile(name, buf, 0o644); err != nil {
		t.Fatal(err)
	}

	frames, _, last := collect(t, dir)
	if len(frames) != 1 || last != 1 {
		t.Fatalf("CRC corruption: %d frames, lastLSN %d; want replay stops before frame 2", len(frames), last)
	}
}

func TestReplayStopsOnTruncatedTail(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, ModeAlways)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		w.Append(testCS(i))
	}
	w.Close()

	// Cut into the 3rd frame's header: frames 1-2 survive, 3 is torn.
	end2 := frameStart(segmentFile(t, dir, "wal-0.log"), 3)
	if err := os.Truncate(filepath.Join(dir, "wal-0.log"), int64(end2+5)); err != nil {
		t.Fatal(err)
	}

	frames, _, last := collect(t, dir)
	if len(frames) != 2 || last != 2 {
		t.Fatalf("truncated tail: %d frames, lastLSN %d; want 2, 2", len(frames), last)
	}
}

func TestRolloverCrossesSegments(t *testing.T) {
	old := segmentSize
	segmentSize = 300 // tiny segments force rollover (test-only override)
	defer func() { segmentSize = old }()

	dir := t.TempDir()
	w, err := Open(dir, ModeInterval)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 6; i++ {
		if _, err := w.Append(testCS(i)); err != nil {
			t.Fatal(err)
		}
	}
	if w.Segment() < 2 {
		t.Fatalf("segment = %d, want >= 2 after rollovers", w.Segment())
	}
	w.Close()

	frames, lsns, last := collect(t, dir)
	if len(frames) != 6 || last != 6 {
		t.Fatalf("cross-segment replay: %d frames, lastLSN %d; want 6, 6", len(frames), last)
	}
	for i, l := range lsns {
		if l != uint64(i+1) {
			t.Fatalf("lsns = %v, want 1..6 (order preserved across segments)", lsns)
		}
	}
}

func TestSaltRegressionAndGapStopReplay(t *testing.T) {
	for _, tc := range []struct {
		name  string
		salt2 uint32
		want  int // frames applied
	}{
		{"continuity ok", 1, 2},
		{"salt regression rejected", 0, 1},
		{"salt gap rejected", 2, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeSegment(t, dir, "wal-0.log", 0, 1, testCS(1))
			writeSegment(t, dir, "wal-1.log", tc.salt2, 2, testCS(2))

			frames, _, last := collect(t, dir)
			if len(frames) != tc.want || last != uint64(tc.want) {
				t.Fatalf("replay: %d frames, lastLSN %d; want %d, %d", len(frames), last, tc.want, tc.want)
			}
		})
	}
}

func TestResetRejectsStaleFrames(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, ModeAlways)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(testCS(1))
	w.Append(testCS(2))
	stale := segmentFile(t, dir, "wal-0.log") // save the pre-checkpoint content

	if err := WriteManifest(dir, Manifest{Snapshot: "graph.db", Segment: 0, LSN: 2}); err != nil {
		t.Fatal(err)
	}
	if err := w.Reset(2); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(testCS(3)); err != nil {
		t.Fatal(err)
	}
	w.Close()

	// Simulate a crash between checkpoint and old-segment cleanup: the stale
	// wal-0.log reappears next to the fresh wal-1.log.
	if err := os.WriteFile(filepath.Join(dir, "wal-0.log"), stale, 0o644); err != nil {
		t.Fatal(err)
	}

	frames, lsns, last := collect(t, dir)
	if len(frames) != 1 || last != 3 {
		t.Fatalf("stale frames: %d frames, lastLSN %d; want only the post-checkpoint frame, lastLSN 3", len(frames), last)
	}
	if len(lsns) == 1 && lsns[0] != 3 {
		t.Fatalf("first replayed lsn = %d, want 3", lsns[0])
	}
}

func TestReplayIdempotent(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, ModeAlways)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		w.Append(testCS(i))
	}
	run := func() int {
		n := 0
		if _, err := w.Replay(func(Frame) error { n++; return nil }); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if a, b := run(), run(); a != 3 || b != 3 {
		t.Fatalf("replay not idempotent: %d then %d, want 3 and 3", a, b)
	}
}

func TestManifestSkipAndFirstFrameLSN(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, ModeAlways)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		w.Append(testCS(i))
	}
	// Manifest lsn=3 without Reset: frames 1-3 are already in the snapshot,
	// replay must apply 4-5 only.
	if err := WriteManifest(dir, Manifest{Snapshot: "graph.db", Segment: 0, LSN: 3}); err != nil {
		t.Fatal(err)
	}
	frames, lsns, last := collect(t, dir)
	if len(frames) != 2 || len(lsns) != 2 || lsns[0] != 4 || lsns[1] != 5 || last != 5 {
		t.Fatalf("manifest skip: frames=%d lsns=%v last=%d; want 2 frames, lsn 4..5", len(frames), lsns, last)
	}

	// Checkpoint + Reset: the manifest is persisted first (real checkpoint
	// order), then the log rolls over; the next frame must land at
	// manifest.lsn+1.
	if err := WriteManifest(dir, Manifest{Snapshot: "graph.db", Segment: 0, LSN: 5}); err != nil {
		t.Fatal(err)
	}
	if err := w.Reset(5); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(testCS(6)); err != nil {
		t.Fatal(err)
	}
	w.Close()
	frames, lsns, last = collect(t, dir)
	if len(frames) != 1 || lsns[0] != 6 || last != 6 {
		t.Fatalf("post-reset: frames=%d lsns=%v last=%d; want first lsn == manifest.lsn+1 == 6", len(frames), lsns, last)
	}
}

func TestResetFreshSegmentState(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, ModeAlways)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(testCS(1))
	w.Append(testCS(2))
	if err := w.Reset(2); err != nil {
		t.Fatal(err)
	}
	if w.Segment() != 1 || w.salt != 1 || w.LastLSN() != 2 {
		t.Fatalf("post-reset state: segment=%d salt=%d lsn=%d; want 1, 1, 2", w.Segment(), w.salt, w.LastLSN())
	}
	lsn, err := w.Append(testCS(3))
	if err != nil || lsn != 3 {
		t.Fatalf("append after reset: lsn=%d err=%v; want 3", lsn, err)
	}
	// Old segment is gone.
	if _, err := os.Stat(filepath.Join(dir, "wal-0.log")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("wal-0.log should be removed after Reset, stat err = %v", err)
	}
	w.Close()
}

func TestSyncModesBothReplayable(t *testing.T) {
	for _, mode := range []Mode{ModeAlways, ModeInterval} {
		dir := t.TempDir()
		w, err := Open(dir, mode)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Append(testCS(1)); err != nil {
			t.Fatal(err)
		}
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
		w.Close()
		frames, _, last := collect(t, dir)
		if len(frames) != 1 || last != 1 {
			t.Fatalf("mode %d: %d frames, last %d; want 1, 1", mode, len(frames), last)
		}
	}
}

func TestClosedWALErrors(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, ModeAlways)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(testCS(1)); !errors.Is(err, ErrClosed) {
		t.Fatalf("Append after Close: %v, want ErrClosed", err)
	}
	if err := w.Sync(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Sync after Close: %v, want ErrClosed", err)
	}
}

func TestHasSegments(t *testing.T) {
	dir := t.TempDir()
	if ok, err := HasSegments(dir); err != nil || ok {
		t.Fatalf("empty dir: ok=%v err=%v; want false, nil", ok, err)
	}
	w, err := Open(dir, ModeAlways)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if ok, err := HasSegments(dir); err != nil || !ok {
		t.Fatalf("after open: ok=%v err=%v; want true, nil", ok, err)
	}
}

func TestIndexDeclRoundtrip(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, ModeAlways)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(testCS(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := w.AppendIndex(IndexDecl{Keys: []string{"team", "region"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(testCS(2)); err != nil {
		t.Fatal(err)
	}
	if got := w.LastLSN(); got != 3 {
		t.Fatalf("LastLSN = %d, want 3 (index frames consume LSNs)", got)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(dir, ModeAlways)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	type seenFrame struct {
		lsn  uint64
		cs   bool
		keys []string
	}
	var order []seenFrame
	last, err := w2.Replay(func(fr Frame) error {
		s := seenFrame{lsn: fr.LSN, keys: fr.IndexKeys}
		s.cs = fr.Changeset != nil
		order = append(order, s)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if last != 3 || len(order) != 3 {
		t.Fatalf("replayed %d frames to lsn %d, want 3 to 3", len(order), last)
	}
	if !order[0].cs || order[1].cs || len(order[1].keys) != 2 || !order[2].cs {
		t.Fatalf("frame order wrong: %+v", order)
	}
	// the recovered write head must park AFTER the index frame — an Append
	// continues the LSN sequence instead of overwriting it
	if got := w2.LastLSN(); got != 3 {
		t.Fatalf("recovered LastLSN = %d, want 3", got)
	}
	if _, err := w2.Append(testCS(9)); err != nil {
		t.Fatal(err)
	}
	if got := w2.LastLSN(); got != 4 {
		t.Fatalf("post-recovery LastLSN = %d, want 4 (index frame overwritten?)", got)
	}
}

// TestVectorIndexDeclRoundtrip: vector declarations ride IndexDecl frames
// and surface on Frame.VectorIndexes (spec §3.3); old frames without the
// field replay with it empty.
func TestVectorIndexDeclRoundtrip(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, ModeAlways)
	if err != nil {
		t.Fatal(err)
	}
	decls := []VectorIndexDecl{
		{Name: "mem", Prop: "embedding", Dim: 768, Metric: "cosine"},
		{Name: "img", Prop: "feat", Dim: 384, Metric: "l2", M: 24, EfConstruction: 300},
	}
	if _, err := w.AppendIndex(IndexDecl{VectorIndexes: decls}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.AppendIndex(IndexDecl{Keys: []string{"team"}}); err != nil { // old-style frame
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w2, err := Open(dir, ModeAlways)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	var gotVec [][]VectorIndexDecl
	var gotKeys int
	if _, err := w2.Replay(func(fr Frame) error {
		gotVec = append(gotVec, fr.VectorIndexes)
		if len(fr.IndexKeys) > 0 {
			gotKeys++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(gotVec) != 2 || len(gotVec[0]) != 2 || len(gotVec[1]) != 0 || gotKeys != 1 {
		t.Fatalf("vector decl replay wrong: %+v keys=%d", gotVec, gotKeys)
	}
	want := decls[1]
	if gotVec[0][1] != want {
		t.Fatalf("decl mismatch: %+v want %+v", gotVec[0][1], want)
	}
}

// TestIndexDeclFullTextKeysRoundtrip: FTS declarations ride IndexDecl frames
// alongside the typed keys (2026-10-26 FTS plan) and surface on
// Frame.FullTextKeys; old frames without the field replay with it empty
// (null ≡ empty — there is no drop-all op for this kind).
func TestIndexDeclFullTextKeysRoundtrip(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, ModeAlways)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.AppendIndex(IndexDecl{
		Keys:         []string{"team"},
		FullTextKeys: []string{"desc", "title"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.AppendIndex(IndexDecl{Keys: []string{"old"}}); err != nil {
		t.Fatal(err) // pre-FTS shape: FullTextKeys absent on the wire
	}
	if _, err := w.Append(testCS(1)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(dir, ModeAlways)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	var declFrames, csFrames int
	var gotKeys, gotFTS [][]string
	if _, err := w2.Replay(func(fr Frame) error {
		if fr.Changeset != nil {
			csFrames++
			return nil
		}
		declFrames++
		gotKeys = append(gotKeys, fr.IndexKeys)
		gotFTS = append(gotFTS, fr.FullTextKeys)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if declFrames != 2 || csFrames != 1 {
		t.Fatalf("replayed decl=%d cs=%d frames, want 2 decl + 1 cs", declFrames, csFrames)
	}
	if !slices.Equal(gotKeys[0], []string{"team"}) || !slices.Equal(gotKeys[1], []string{"old"}) {
		t.Fatalf("IndexKeys replay wrong: %v", gotKeys)
	}
	if !slices.Equal(gotFTS[0], []string{"desc", "title"}) {
		t.Fatalf("FullTextKeys = %v, want [desc title]", gotFTS[0])
	}
	if len(gotFTS[1]) != 0 {
		t.Fatalf("old-shape frame replayed FullTextKeys = %v, want empty", gotFTS[1])
	}
}
