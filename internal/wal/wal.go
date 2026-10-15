// Package wal implements Vectra's embedded logical write-ahead log.
//
// Layout: segments live at <dir>/wal-<N>.log with N strictly increasing
// (numbers never wrap; a Reset allocates N+1 and deletes everything older).
// Every segment starts with a Marker frame carrying the segment's salt (the
// number of segment creations before it in this lineage — it increments on
// every rollover and Reset) and the LSN its first Changeset frame will carry.
// Segment files are preallocated (zero-extended) to segmentSize to keep
// appends sequential; the zero tail is what a replaying scan stops on.
//
// Frame format (little-endian):
//
//	[8B header: uint64, low 56 bits = len(type+payload), high bits reserved 0]
//	[4B CRC32C (Castagnoli) over type byte || payload]
//	[1B type: 1 = Changeset, 2 = Marker]
//	[JSON payload]
//
// Durability: Append fsyncs per frame in ModeAlways only; ModeInterval leaves
// fsync to the caller's boundaries (Sync / checkpoint / Close). There are no
// goroutines, channels, locks or timers in this package — the single-writer
// contract is the caller's (direct mode: one goroutine; wrapper mode: the
// writer goroutine).
//
// This package is the definition site of the core Changeset vocabulary
// (NodeUpsert/EdgeUpsert/EdgeDelete/Changeset) because the root package both
// re-exports those types publicly and consumes this package — defining them
// here keeps the dependency graph acyclic. Field names and json tags are the
// wire contract: WAL frames are whole-frame JSON, and the server module's
// raft Batch embeds Changeset so gob flattens to the same field names.
package wal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// NodeUpsert / EdgeUpsert / EdgeDelete are the atomic items of a Changeset.
// Semantics: an existing node key updates its props; edges upsert by their
// (srcKey, dstKey, label) key — the most recent edge with the same key is
// replaced in place and extra parallels collapse (last write wins on props).
// AllowParallel=true opts an edge out of upsert: it is appended as a true
// parallel edge (AddEdge semantics).
type NodeUpsert struct {
	Key   string         `json:"key"`
	Label string         `json:"label"`
	Props map[string]any `json:"props"`
}

type EdgeUpsert struct {
	SrcKey        string         `json:"srcKey"`
	DstKey        string         `json:"dstKey"`
	Label         string         `json:"label"`
	Props         map[string]any `json:"props"`
	AllowParallel bool           `json:"allowParallel"`
}

// EdgeDelete addresses edges by their logical key (srcKey, dstKey, label) —
// EdgeID cannot be used because Seal-time compaction renumbers IDs, and logs
// must not depend on it. Node deletes cascade edges.
type EdgeDelete struct {
	SrcKey string `json:"srcKey"`
	DstKey string `json:"dstKey"`
	Label  string `json:"label"`
}

// Changeset is one WAL frame's payload (and the core write vocabulary).
// Fixed apply order (deterministic replay): node upserts → edge upserts →
// edge deletes → node deletes.
type Changeset struct {
	Version     int          `json:"version"`
	Nodes       []NodeUpsert `json:"nodes"`
	Edges       []EdgeUpsert `json:"edges"`
	EdgeDeletes []EdgeDelete `json:"edgeDeletes"`
	NodeDeletes []string     `json:"nodeDeletes"`
}

// Mode selects the WAL's durability behavior. The zero value is ModeOff (no
// WAL at all — v0.6 snapshot-only behavior).
type Mode int

const (
	ModeOff      Mode = iota
	ModeAlways        // fsync every frame
	ModeInterval      // fsync at caller boundaries only (Sync/Close/checkpoint)
)

// segmentSize is the preallocation size per segment. It is a var (not a
// const) so white-box tests can shrink it to force rollovers; production
// code never writes it.
var segmentSize = 64 << 20

const (
	frameChangeset = 1
	frameMarker    = 2
	headerLen      = 12 // 8B length + 4B CRC
	// rolloverHeadroom: a frame may only trigger a rollover once the segment
	// holds more than this many bytes (i.e. past its leading marker).
	rolloverHeadroom = 64
	maxFramePayload  = 1<<56 - 1
)

var (
	crcTable  = crc32.MakeTable(crc32.Castagnoli)
	ErrClosed = errors.New("wal: closed")

	// errStopScan aborts a frame scan at the current offset (torn/corrupt
	// tail or a rejected frame): replay stops there by design.
	errStopScan = errors.New("wal: scan stop")
)

// marker is the leading frame of every segment: salt guards against stale
// frames from a previous epoch, base is the LSN of the first Changeset frame
// that follows in this segment.
type marker struct {
	Salt uint32 `json:"salt"`
	Base uint64 `json:"base"`
}

// Manifest ties a checkpointed snapshot to the WAL prefix it contains.
// Written temp+rename (atomic); LSN is the exact export boundary — recovery
// replays frames with lsn > LSN only, and the reconciliation invariant is
// first replayed lsn == LSN+1.
type Manifest struct {
	Snapshot string `json:"snapshot"` // snapshot file name (basename)
	Segment  int    `json:"segment"`  // current segment at checkpoint time
	LSN      uint64 `json:"lsn"`      // last LSN contained in the snapshot
}

// WAL is an append-only logical log. Single writer, no internal
// synchronization — see the package comment.
type WAL struct {
	dir  string
	mode Mode

	f       *os.File
	segment int    // current segment number (monotonic)
	salt    uint32 // current segment's salt (increments per segment creation)
	off     int64  // next write offset within the current segment
	lsn     uint64 // last assigned LSN

	baseLSN  uint64 // from manifest: frames with lsn <= this are skipped on replay
	startSeg int    // from manifest: segments below this number are ignored
}

// --- manifest --------------------------------------------------------------

const manifestName = "manifest.json"

// ReadManifest loads dir/manifest.json. A missing manifest is (zero, false,
// nil); a corrupt one is an error.
func ReadManifest(dir string) (Manifest, bool, error) {
	b, err := os.ReadFile(filepath.Join(dir, manifestName))
	if errors.Is(err, fs.ErrNotExist) {
		return Manifest{}, false, nil
	}
	if err != nil {
		return Manifest{}, false, err
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return Manifest{}, false, fmt.Errorf("wal: corrupt manifest: %w", err)
	}
	return m, true, nil
}

// WriteManifest atomically writes dir/manifest.json (temp file + fsync +
// rename + directory fsync).
func WriteManifest(dir string, m Manifest) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, manifestName+".tmp")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, manifestName)); err != nil {
		os.Remove(tmp)
		return err
	}
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
	return nil
}

// HasSegments reports whether dir already holds any wal-<N>.log file.
func HasSegments(dir string) (bool, error) {
	segs, err := listSegments(dir)
	if err != nil {
		return false, err
	}
	return len(segs) > 0, nil
}

// --- segment plumbing --------------------------------------------------------

func segmentPath(dir string, n int) string {
	return filepath.Join(dir, fmt.Sprintf("wal-%d.log", n))
}

func listSegments(dir string) ([]int, error) {
	es, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var segs []int
	for _, e := range es {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, "wal-") || !strings.HasSuffix(name, ".log") {
			continue
		}
		n, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(name, "wal-"), ".log"))
		if err != nil || n < 0 {
			continue
		}
		segs = append(segs, n)
	}
	sort.Ints(segs)
	return segs, nil
}

// createSegment switches w to a brand-new segment file: leading marker frame,
// preallocation to segmentSize, write head parked after the marker. The
// previous file handle (if any) is closed.
func (w *WAL) createSegment(n int, salt uint32, base uint64) error {
	f, err := os.OpenFile(segmentPath(w.dir, n), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	mj, err := json.Marshal(marker{Salt: salt, Base: base})
	if err != nil {
		f.Close()
		return err
	}
	frame := appendFrame(nil, frameMarker, mj)
	if _, err := f.WriteAt(frame, 0); err != nil {
		f.Close()
		return err
	}
	if err := f.Truncate(int64(segmentSize)); err != nil { // preallocate (sparse on most FSes)
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil { // the marker must survive a crash
		f.Close()
		return err
	}
	if w.f != nil {
		w.f.Close()
	}
	w.f, w.segment, w.salt, w.off, w.lsn = f, n, salt, int64(len(frame)), base-1
	return nil
}

// segScan is what a scan of one segment file found.
type segScan struct {
	valid   int64 // length of the valid frame prefix (write head resumes here)
	hasMark bool  // leading marker frame present and parseable
	salt    uint32
	base    uint64
	nframes int // Changeset frames within the valid prefix
}

// scanSegmentFile reads and parses one segment file's frame prefix.
func scanSegmentFile(dir string, n int) (segScan, error) {
	var sc segScan
	buf, err := os.ReadFile(segmentPath(dir, n))
	if err != nil {
		return sc, err
	}
	first := true
	end, err := scanFrames(buf, func(typ byte, payload []byte) error {
		if first {
			first = false
			if typ != frameMarker {
				return errStopScan
			}
			var m marker
			if err := json.Unmarshal(payload, &m); err != nil {
				return errStopScan
			}
			sc.hasMark, sc.salt, sc.base = true, m.Salt, m.Base
			return nil
		}
		if typ != frameChangeset {
			return errStopScan
		}
		sc.nframes++
		return nil
	})
	if err != nil && !errors.Is(err, errStopScan) {
		return sc, err
	}
	sc.valid = end
	return sc, nil
}

// Open recovers (or creates) a WAL in dir. It reads manifest.json when
// present (base LSN + start segment for replay skipping), scans the segment
// chain to find the recovery point, and parks the write head after the last
// valid frame — a torn tail is overwritten by the next Append.
func Open(dir string, mode Mode) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	w := &WAL{dir: dir, mode: mode}
	if man, ok, err := ReadManifest(dir); err != nil {
		return nil, err
	} else if ok {
		w.baseLSN, w.startSeg = man.LSN, man.Segment
	}
	segs, err := listSegments(dir)
	if err != nil {
		return nil, err
	}

	// Walk the contiguous, salt-continuous chain of valid segments to find
	// the write head. Anything past the first break is tail damage.
	prev := -1
	var (
		head    *segScan
		headSeg int
	)
	for _, s := range segs {
		if s < w.startSeg {
			continue
		}
		if prev >= 0 && s != prev+1 {
			break
		}
		sc, err := scanSegmentFile(dir, s)
		if err != nil {
			return nil, err
		}
		if !sc.hasMark {
			break
		}
		if prev >= 0 && sc.salt != head.salt+1 {
			break
		}
		prev = s
		head, headSeg = &sc, s
	}

	if head != nil {
		f, err := os.OpenFile(segmentPath(dir, headSeg), os.O_RDWR, 0o644)
		if err != nil {
			return nil, err
		}
		w.f, w.segment, w.salt, w.off = f, headSeg, head.salt, head.valid
		w.lsn = head.base + uint64(head.nframes) - 1
		if w.lsn < w.baseLSN {
			w.lsn = w.baseLSN // manifest ahead of the files: never reuse an LSN
		}
		return w, nil
	}

	// Fresh (or unrecoverable) lineage: allocate a segment number above every
	// file already in the dir so garbage is never overwritten, and seed the
	// salt from the segment number (in a healthy lineage salt == segment).
	n := w.startSeg
	if len(segs) > 0 && segs[len(segs)-1] >= n {
		n = segs[len(segs)-1] + 1
	}
	if err := w.createSegment(n, uint32(n), w.baseLSN+1); err != nil {
		return nil, err
	}
	return w, nil
}

// Append writes cs as one frame and returns its LSN. In ModeAlways the frame
// is fsynced before returning; in ModeInterval durability waits for an
// explicit Sync (or the file system's default writeback).
func (w *WAL) Append(cs Changeset) (uint64, error) {
	if w.f == nil {
		return 0, ErrClosed
	}
	payload, err := json.Marshal(cs)
	if err != nil {
		return 0, err
	}
	frame := appendFrame(nil, frameChangeset, payload)
	if w.off > rolloverHeadroom && w.off+int64(len(frame)) > int64(segmentSize) {
		if err := w.createSegment(w.segment+1, w.salt+1, w.lsn+1); err != nil {
			return 0, err
		}
	}
	if _, err := w.f.WriteAt(frame, w.off); err != nil {
		return 0, err
	}
	w.off += int64(len(frame))
	w.lsn++
	if w.mode == ModeAlways {
		if err := w.f.Sync(); err != nil {
			return 0, err
		}
	}
	return w.lsn, nil
}

// Sync fsyncs the current segment (the ModeInterval caller boundary).
func (w *WAL) Sync() error {
	if w.f == nil {
		return ErrClosed
	}
	return w.f.Sync()
}

// Replay applies every frame above the manifest's base LSN to fn, in LSN
// order, and returns the last applied LSN. A torn or corrupt tail stops the
// replay at the first bad frame (returned as a normal end, not an error —
// crash-torn tails are expected); an fn error aborts and propagates. Replay
// is stateless with respect to the WAL: calling it twice replays the same
// frames twice.
func (w *WAL) Replay(fn func(Changeset) error) (uint64, error) {
	return w.replayLSN(func(_ uint64, cs Changeset) error { return fn(cs) })
}

// replayLSN is Replay with per-frame LSN visibility (used by tests to assert
// the manifest reconciliation invariant).
func (w *WAL) replayLSN(fn func(lsn uint64, cs Changeset) error) (uint64, error) {
	segs, err := listSegments(w.dir)
	if err != nil {
		return 0, err
	}
	last := w.baseLSN
	applied := false
	prevSeg, prevSalt := -1, uint32(0)
	for _, s := range segs {
		if s < w.startSeg {
			continue
		}
		if prevSeg >= 0 && s != prevSeg+1 {
			break // gap in the chain: everything past it is unreachable
		}
		buf, err := os.ReadFile(segmentPath(w.dir, s))
		if err != nil {
			return last, err
		}
		var (
			m       marker
			saltOK  bool
			seen    uint64
			stopped bool
		)
		_, serr := scanFrames(buf, func(typ byte, payload []byte) error {
			if !saltOK { // first frame: the segment marker
				if typ != frameMarker {
					stopped = true
					return errStopScan
				}
				if err := json.Unmarshal(payload, &m); err != nil {
					stopped = true
					return errStopScan
				}
				saltOK = prevSeg < 0 || m.Salt == prevSalt+1
				if !saltOK { // stale epoch or forged gap: refuse the whole segment
					stopped = true
					return errStopScan
				}
				return nil
			}
			if typ != frameChangeset {
				stopped = true
				return errStopScan
			}
			lsn := m.Base + seen
			seen++
			if lsn <= w.baseLSN && !applied {
				return nil // already contained in the checkpointed snapshot
			}
			if lsn != last+1 {
				stopped = true
				return errStopScan // gap or regression: stop rather than guess
			}
			var cs Changeset
			if err := json.Unmarshal(payload, &cs); err != nil {
				stopped = true
				return errStopScan
			}
			if err := fn(lsn, cs); err != nil {
				return err
			}
			last = lsn
			applied = true
			return nil
		})
		if serr != nil {
			return last, serr // fn error: propagate with the recovery point
		}
		if stopped || !saltOK {
			break
		}
		prevSeg, prevSalt = s, m.Salt
	}
	return last, nil
}

// Reset rolls the log over after a checkpoint: it allocates a fresh segment
// (salt+1) whose marker bases LSNs at baseLSN+1, then deletes every older
// segment. Old frames are unreadable afterwards even if a crash interrupts
// the cleanup — the manifest's segment/LSN gating refuses them. The caller
// must have persisted the manifest before Reset.
func (w *WAL) Reset(baseLSN uint64) error {
	if w.f == nil {
		return ErrClosed
	}
	newSeg := w.segment + 1
	if err := w.createSegment(newSeg, w.salt+1, baseLSN+1); err != nil {
		return err
	}
	w.lsn = baseLSN
	segs, err := listSegments(w.dir)
	if err != nil {
		return err
	}
	for _, s := range segs {
		if s < newSeg {
			if err := os.Remove(segmentPath(w.dir, s)); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
		}
	}
	return nil
}

// Close releases the segment file handle (idempotent). It does not fsync —
// callers checkpoint first.
func (w *WAL) Close() error {
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}

// LastLSN reports the last assigned (or recovered) LSN.
func (w *WAL) LastLSN() uint64 { return w.lsn }

// Segment reports the current segment number.
func (w *WAL) Segment() int { return w.segment }

// --- frame codec -------------------------------------------------------------

// appendFrame appends one framed record to dst: [8B length header (low 56
// bits, high bits reserved zero)][4B CRC32C over type+payload][type][payload].
func appendFrame(dst []byte, typ byte, payload []byte) []byte {
	var hdr [12]byte
	binary.LittleEndian.PutUint64(hdr[:8], uint64(1+len(payload)))
	crc := crc32.Update(0, crcTable, []byte{typ})
	crc = crc32.Update(crc, crcTable, payload)
	binary.LittleEndian.PutUint32(hdr[8:], crc)
	dst = append(dst, hdr[:]...)
	dst = append(dst, typ)
	return append(dst, payload...)
}

// scanFrames walks buf's valid frame prefix, invoking fn per frame. It stops
// at the first torn/corrupt frame (zero headers included — that is how the
// preallocated tail ends a segment) and returns the length of the valid
// prefix. A non-errStopScan error from fn aborts the scan and is returned
// as-is; the offset then points at the frame fn rejected.
func scanFrames(buf []byte, fn func(typ byte, payload []byte) error) (int64, error) {
	off := 0
	for {
		if off+headerLen > len(buf) {
			break
		}
		n := binary.LittleEndian.Uint64(buf[off : off+8])
		if n&^uint64(maxFramePayload) != 0 {
			break // reserved bits set: not a frame
		}
		if n == 0 || n > uint64(len(buf)) {
			break // zero header (preallocated tail) or forged/overflowing length
		}
		end := off + headerLen + int(n)
		if end > len(buf) {
			break // torn payload
		}
		want := binary.LittleEndian.Uint32(buf[off+8 : off+12])
		if got := crc32.Update(0, crcTable, buf[off+12:end]); got != want {
			break // corrupt frame
		}
		typ := buf[off+12]
		if typ != frameChangeset && typ != frameMarker {
			break
		}
		if err := fn(typ, buf[off+13:end]); err != nil {
			if errors.Is(err, errStopScan) {
				break
			}
			return int64(off), err
		}
		off = end
	}
	return int64(off), nil
}
