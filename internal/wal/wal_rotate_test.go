package wal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// --- Rotate: the retaining form of a checkpoint rollover ---------------------

// TestRotateRetainsSegments: Rotate keeps every older segment on disk (the
// retained chain IS the increment over the last full snapshot) and replay
// walks the whole salt-chained list in LSN order.
func TestRotateRetainsSegments(t *testing.T) {
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
	preRotate := w.LastLSN()
	if err := w.Rotate(); err != nil {
		t.Fatal(err)
	}
	if w.Segment() != 1 || w.salt != 1 {
		t.Fatalf("post-rotate: segment=%d salt=%d; want 1, 1 (fresh segment, salt+1)", w.Segment(), w.salt)
	}
	if w.LastLSN() != preRotate {
		t.Fatalf("rotate moved LastLSN: %d -> %d; want continuity", preRotate, w.LastLSN())
	}
	// The old segment file must still exist: Rotate retains, unlike Reset.
	if _, err := os.Stat(filepath.Join(dir, "wal-0.log")); err != nil {
		t.Fatalf("wal-0.log missing after Rotate: %v (Rotate must retain older segments)", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "wal-1.log")); err != nil {
		t.Fatalf("wal-1.log missing after Rotate: %v", err)
	}
	lsn, err := w.Append(testCS(4))
	if err != nil || lsn != 4 {
		t.Fatalf("append after rotate: lsn=%d err=%v; want 4 (marker bases at pre-rotate lsn+1)", lsn, err)
	}
	if _, err := w.Append(testCS(5)); err != nil {
		t.Fatal(err)
	}
	w.Close()

	frames, lsns, last := collect(t, dir)
	if len(frames) != 5 || last != 5 {
		t.Fatalf("replay: %d frames, lastLSN %d; want all 5 retained+tail frames, lastLSN 5", len(frames), last)
	}
	for i, l := range lsns {
		if l != uint64(i+1) {
			t.Fatalf("lsns = %v, want 1..5 in order across the rotated chain", lsns)
		}
	}
}

// TestRotateResetsVolumeAccounting: Rotate zeroes the volume counter like
// Reset does — the checkpoint boundary owns everything logged so far, so the
// volume trigger must restart from the new segment.
func TestRotateResetsVolumeAccounting(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, ModeAlways)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.Append(testCS(7)); err != nil {
		t.Fatal(err)
	}
	first := w.Size()
	if first <= 0 {
		t.Fatalf("size after first append = %d, want > 0", first)
	}
	if err := w.Rotate(); err != nil {
		t.Fatal(err)
	}
	if got := w.Size(); got != 0 {
		t.Fatalf("size after rotate = %d, want 0 (checkpoint boundary owns the logged volume)", got)
	}
	if _, err := w.Append(testCS(7)); err != nil { // same frame bytes as the first append
		t.Fatal(err)
	}
	if got := w.Size(); got != first {
		t.Fatalf("size after post-rotate append = %d, want %d (counts new-segment frames only)", got, first)
	}
	if _, err := w.Append(testCS(8)); err != nil {
		t.Fatal(err)
	}
	if got := w.Size(); got <= first {
		t.Fatalf("size = %d after second append, want > %d (grows with new frames)", got, first)
	}
}

// TestManifestV2RoundTrip: RetainFrom+Format persist; a legacy v1 manifest
// (no new keys on disk) decodes with zeros; our own zero-value write stays
// byte-identical to v1 so compacting checkpoints keep v1-shaped manifests.
func TestManifestV2RoundTrip(t *testing.T) {
	dir := t.TempDir()
	v2 := Manifest{Snapshot: "graph.db", Segment: 5, LSN: 42, RetainFrom: 2, Format: 2}
	if err := WriteManifest(dir, v2); err != nil {
		t.Fatal(err)
	}
	got, ok, err := ReadManifest(dir)
	if err != nil || !ok {
		t.Fatalf("read: ok=%v err=%v", ok, err)
	}
	if got != v2 {
		t.Fatalf("v2 round trip: %+v; want %+v", got, v2)
	}

	legacy := []byte(`{"snapshot":"old.db","segment":3,"lsn":10}`)
	if err := os.WriteFile(filepath.Join(dir, manifestName), legacy, 0o644); err != nil {
		t.Fatal(err)
	}
	got, ok, err = ReadManifest(dir)
	if err != nil || !ok {
		t.Fatalf("read legacy: ok=%v err=%v", ok, err)
	}
	if want := (Manifest{Snapshot: "old.db", Segment: 3, LSN: 10}); got != want {
		t.Fatalf("legacy decode: %+v; want %+v (new fields zero = v1 semantics)", got, want)
	}

	b, err := json.Marshal(Manifest{Snapshot: "s.db", Segment: 1, LSN: 2})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"snapshot":"s.db","segment":1,"lsn":2}` {
		t.Fatalf("v1-shaped write gained keys: %s", b)
	}
}

// TestReplayStartsAtRetainFrom: the v2 manifest floors the scan at
// RetainFrom while Segment names the active tail. Chain: wal-0 (lsn 1-2,
// fully checkpointed), wal-1 (lsn 3-4, straddles the watermark), wal-2
// (lsn 5, tail). Frame 3 is skipped by the LSN watermark — not the segment
// floor — and frame 4 in the same retained segment MUST be reached: without
// the widened floor replay would start at wal-2 and die on the 3→5 LSN gap,
// silently losing frame 4.
func TestReplayStartsAtRetainFrom(t *testing.T) {
	dir := t.TempDir()
	writeSegment(t, dir, "wal-0.log", 0, 1, testCS(1), testCS(2))
	writeSegment(t, dir, "wal-1.log", 1, 3, testCS(3), testCS(4))
	writeSegment(t, dir, "wal-2.log", 2, 5, testCS(5))
	if err := WriteManifest(dir, Manifest{Snapshot: "base.db", Segment: 2, LSN: 3, RetainFrom: 1, Format: 2}); err != nil {
		t.Fatal(err)
	}

	w, err := Open(dir, ModeAlways)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if w.LastLSN() != 5 {
		t.Fatalf("recovered LastLSN = %d, want 5 (chain scanned from RetainFrom)", w.LastLSN())
	}
	var lsns []uint64
	last, err := w.replayLSN(func(lsn uint64, fr Frame) error {
		if fr.Changeset == nil {
			t.Errorf("lsn %d: not a changeset frame", lsn)
		}
		lsns = append(lsns, lsn)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(lsns) != 2 || lsns[0] != 4 || lsns[1] != 5 || last != 5 {
		t.Fatalf("replay lsns = %v last = %d; want [4 5], 5 (watermark gates, the floor only widens)", lsns, last)
	}
}

// TestRetainFromChainGapStopsReplay: widening the scan floor does not relax
// LSN continuity — a gap at the retained head still stops replay rather
// than guessing (here wal-1 is missing, so wal-2's lsn 2 cannot follow
// baseLSN 0's expected 1).
func TestRetainFromChainGapStopsReplay(t *testing.T) {
	dir := t.TempDir()
	writeSegment(t, dir, "wal-0.log", 0, 1, testCS(1))
	writeSegment(t, dir, "wal-2.log", 2, 2, testCS(2)) // wal-1 missing: gap at the retained head
	if err := WriteManifest(dir, Manifest{Snapshot: "base.db", Segment: 2, LSN: 0, RetainFrom: 1, Format: 2}); err != nil {
		t.Fatal(err)
	}

	frames, _, last := collect(t, dir)
	if len(frames) != 0 || last != 0 {
		t.Fatalf("gap at retained head: %d frames, last %d; want replay stopped (0, 0)", len(frames), last)
	}
}

// TestRotateChainAcceptedAcrossRotations: recovery's contiguous+salt gating
// must accept a chain several rotations long (each Rotate is one more
// salt+1 link), park the write head at the tail, and continue the LSN
// sequence from there.
func TestRotateChainAcceptedAcrossRotations(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, ModeAlways)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ { // 3 rotations → segments 0..3, one frame each in 0..2
		if _, err := w.Append(testCS(i + 1)); err != nil {
			t.Fatal(err)
		}
		if err := w.Rotate(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Append(testCS(4)); err != nil { // tail frame on segment 3
		t.Fatal(err)
	}
	w.Close()

	w2, err := Open(dir, ModeAlways)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	if w2.Segment() != 3 || w2.salt != 3 || w2.LastLSN() != 4 {
		t.Fatalf("recovered head: segment=%d salt=%d lsn=%d; want 3, 3, 4 (long chain accepted)", w2.Segment(), w2.salt, w2.LastLSN())
	}
	var lsns []uint64
	last, err := w2.replayLSN(func(lsn uint64, fr Frame) error {
		lsns = append(lsns, lsn)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(lsns) != 4 || last != 4 {
		t.Fatalf("replay after 3 rotations: lsns = %v last = %d; want 1..4, 4", lsns, last)
	}
	for i, l := range lsns {
		if l != uint64(i+1) {
			t.Fatalf("lsns = %v, want 1..4 in order", lsns)
		}
	}
	if got, err := w2.Append(testCS(9)); err != nil || got != 5 {
		t.Fatalf("append after recovery: lsn=%d err=%v; want 5 (continuity)", got, err)
	}
	for i := 0; i <= 3; i++ {
		if _, err := os.Stat(filepath.Join(dir, fmt.Sprintf("wal-%d.log", i))); err != nil {
			t.Fatalf("wal-%d.log missing: %v (all rotated segments retained)", i, err)
		}
	}
}
