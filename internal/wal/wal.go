// Package wal implements Vectra's embedded logical write-ahead log.
//
// Layout: segments live at <dir>/wal-<N>.log with N strictly increasing
// (numbers never wrap; a Reset allocates N+1 and deletes everything older,
// while a Rotate allocates N+1 and keeps the older segments as the retained
// increment chain a manifest-v2 RetainFrom anchors).
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
//	[1B type: 1 = Changeset, 2 = Marker, 3 = IndexDecl, 4 = ChangesetV2]
//	[payload: JSON for types 1-3; a self-describing binary vector envelope
//	 for type 4 — see AppendHoisted]
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
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/projanvil/vectra/internal/vecbin"
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

// IndexDecl is an index-declaration frame's payload: the property keys
// IndexProperty declared since the previous frame (idempotent, order-
// independent of data frames). The industry norm — PostgreSQL WAL-logs
// CREATE INDEX, RocksDB logs column-family edits to the MANIFEST — is that
// schema/metadata operations must survive crash recovery like data writes.
// Vectra implements this as a separate frame type rather than a Changeset
// field, keeping the Changeset wire shape frozen (spec §4 R9: the server
// raft Batch embeds it with gob-flattened field names).
//
// Forward compatibility: a pre-0.7.1 reader stops replay at the first
// IndexDecl frame (the standing stop-rather-than-guess rule for unknown
// frame types); because the WAL resets at every checkpoint, the exposure
// window is one checkpoint interval of a mixed-version downgrade.
type IndexDecl struct {
	Keys []string `json:"keys"`
	// VectorIndexes carries vector index declarations alongside the typed
	// property keys (vector Phase 1, spec §3.3). JSON decoding ignores the
	// field on old payloads (forward compatible); an old reader skips the
	// whole frame type by the standing stop-rather-than-guess rule.
	// no omitempty: null = this frame carries no vector declarations
	// (pre-Phase-1 frames decode this way), while an explicit empty array
	// means "drop all" - a DropVectorIndex-only flush must survive the
	// wire (Phase 1 audit: the guard-replay pair used to eat it).
	VectorIndexes []VectorIndexDecl `json:"vectorIndexes"`
}

// VectorIndexDecl is the wire form of a vector index declaration
// (spec §3.1). Metric: "cosine" | "l2" | "dot"; zero M/EfConstruction
// mean engine defaults.
type VectorIndexDecl struct {
	Name           string `json:"name"`
	Prop           string `json:"prop"`
	Dim            int    `json:"dim"`
	Metric         string `json:"metric"`
	M              int    `json:"m,omitempty"`
	EfConstruction int    `json:"efConstruction,omitempty"`
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
	frameIndexDecl = 3 // v0.7.1: index declarations (spec errata III) — see IndexDecl
	// frameChangesetV2 is a Changeset whose declared vector props left the
	// JSON channel for a raw float32 section (vector Phase 2, spec §1.2).
	// Old readers stop replay at it — the standing silent-truncation rule
	// for unknown types; the store v5 version tripwire is what actually
	// blocks downgrades (spec §1.3).
	frameChangesetV2 = 4
	headerLen        = 12 // 8B length + 4B CRC
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
	// RetainFrom is the manifest-v2 increment anchor: the first live
	// retained segment of the chain a light checkpoint (Rotate) left behind.
	// A light checkpoint writes no snapshot — the retained chain IS the
	// increment over the still-named Snapshot — so recovery must scan from
	// RetainFrom while the LSN watermark still gates application. 0/absent
	// keeps v1 semantics (Segment is the scan floor); a compacting
	// checkpoint (Reset) clears it, which omitempty renders as a v1-shaped
	// file. Callers must keep the first checkpoint after any lineage start
	// compacting, or the chain's head (segment 0) is inexpressible here.
	RetainFrom int `json:"retainFrom,omitempty"`
	// Format marks the writer generation: 2 = retained-WAL checkpoints
	// (v0.11.2+), 0/absent = v1. Advisory/diagnostic only — the hard
	// downgrade tripwire is the snapshot's store version (v6), not this
	// field: an old binary that ignores both new fields reads the v6 base
	// and fails there instead of silently losing the increment.
	Format int `json:"format,omitempty"`
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
	startSeg int    // first segment of the live chain: recovery floors here, and a light checkpoint copies it into manifest RetainFrom
	// segBase maps each live segment number to its marker's base LSN (the
	// LSN its first frame carries) - the coverage oracle for
	// FirstUncoveredSegment. Writer-thread only, like every WAL field.
	segBase map[int]uint64

	// bytes counts VALID frame bytes across the lineage since the last
	// Reset (preallocated zero tails are not frames). Atomic so Size is
	// safe to call from any goroutine; only the single writer mutates it.
	bytes atomic.Int64

	// retained counts the same frames as bytes but NEVER resets on Rotate —
	// it is the summed valid frame bytes of the whole live chain (retained
	// segments and current segment alike), i.e. the replay volume the next
	// recovery pays. Only Reset (the compacting form, which deletes the
	// chain) zeroes it. Atomic for the same reason as bytes.
	retained atomic.Int64
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
	slices.Sort(segs)
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
	if w.segBase == nil {
		w.segBase = map[int]uint64{}
	}
	w.segBase[n] = base
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
		// ModeInterval defers durability to Sync/checkpoint boundaries; the
		// rollover itself is such a boundary — frames left in the outgoing
		// segment would never be fsynced by a later Sync() on the new one.
		if err := w.f.Sync(); err != nil {
			w.f.Close()
			return err
		}
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
	nframes int // non-marker frames (Changeset + IndexDecl) in the valid prefix — each consumes an LSN
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
		// every non-marker frame type consumes an LSN — nframes must count
		// them all or the write head parks early (next Append would
		// overwrite a valid frame) and w.lsn regresses. Payloads are not
		// decoded here: a v2 frame with a forged ref still parks the head
		// after it and Replay is what reports the damage.
		if typ != frameChangeset && typ != frameIndexDecl && typ != frameChangesetV2 {
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
	w := &WAL{dir: dir, mode: mode, segBase: map[int]uint64{}}
	if man, ok, err := ReadManifest(dir); err != nil {
		return nil, err
	} else if ok {
		w.baseLSN, w.startSeg = man.LSN, man.Segment
		// Manifest v2: a light checkpoint kept every older segment, so the
		// salt-continuous chain to recover starts at RetainFrom, below the
		// active Segment. Widening the floor only ADDS segments to the
		// scanned set — the baseLSN watermark still refuses frames the
		// checkpoint already contains, so application behavior is gated
		// exactly as in v1. The >0 guard (not a bare min) is what keeps a
		// v1 manifest's zero field from flooring the scan at segment 0 and
		// resurrecting deleted pre-checkpoint segments.
		if man.RetainFrom > 0 && man.RetainFrom < w.startSeg {
			w.startSeg = man.RetainFrom
		}
	}
	segs, err := listSegments(dir)
	if err != nil {
		return nil, err
	}

	// Walk the contiguous, salt-continuous chain of valid segments to find
	// the write head. Anything past the first break is tail damage.
	prev := -1
	firstSeg := -1
	var (
		head       *segScan
		headSeg    int
		chainBytes int64
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
		if prev < 0 {
			firstSeg = s // first segment the walk accepted = live-chain head
		}
		prev = s
		head, headSeg = &sc, s
		w.segBase[s] = sc.base
		chainBytes += sc.valid
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
		w.startSeg = firstSeg // tighten the manifest floor to the first LIVE segment
		w.bytes.Store(chainBytes)
		w.retained.Store(chainBytes)
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
	w.startSeg = n // the fresh segment is the whole live chain
	return w, nil
}

// Append writes cs as one frame and returns its LSN. In ModeAlways the frame
// is fsynced before returning; in ModeInterval durability waits for an
// explicit Sync (or the file system's default writeback).
func (w *WAL) Append(cs Changeset) (uint64, error) {
	payload, err := json.Marshal(cs)
	if err != nil {
		return 0, err
	}
	return w.writeFrame(frameChangeset, payload)
}

// AppendIndex writes one IndexDecl frame (same rollover/LSN/durability
// mechanics as Append).
func (w *WAL) AppendIndex(d IndexDecl) (uint64, error) {
	payload, err := json.Marshal(d)
	if err != nil {
		return 0, err
	}
	return w.writeFrame(frameIndexDecl, payload)
}

// AppendHoisted writes cs as a frameChangesetV2 frame when any node's
// declared prop hoists (vecbin.Hoist over Nodes), else a plain v1
// frameChangeset frame. The v2 payload envelope is self-describing:
// [4B "VVB2"][u16 nProps][u16 nameLen+name+u16 dim]*[u32 jsonLen]
// [json skeleton][raw float32 LE section].
//
// The hint section names only the props actually hoisted in THIS frame, so
// Replay restores with zero cross-frame state: the WAL stays stateless and
// declaration (IndexDecl) frames are not required to precede data frames —
// a fresh segment after Reset replays v2 frames on its own. Edge props are
// never indexed (spec §1.2) and stay JSON; undeclared numeric arrays are
// data and stay JSON. cs is never mutated, and the fallback path reuses
// Append verbatim so pure-graph changesets keep byte-identical v1 frames.
func (w *WAL) AppendHoisted(cs Changeset, decls vecbin.PropDecls) (uint64, error) {
	if len(decls) == 0 || len(cs.Nodes) == 0 {
		return w.Append(cs)
	}
	// Detection is declaration-based, never Go-type-based (spec §1.2): a hit
	// is a declared prop holding a numeric array — exactly vecbin.Hoist's
	// predicate, probed here with the allocation-free NumericArray so the v1
	// fallback costs nothing. The recorded dim is what this frame's values
	// actually carry (per-frame truth, not the declared dim); the hint is
	// descriptive only, each "$vec" ref holds the authoritative off/dim.
	hintDims := map[string]int{}
	for _, n := range cs.Nodes {
		for k, v := range n.Props {
			if _, declared := decls[k]; !declared {
				continue
			}
			if dim, ok := vecbin.NumericArray(v); ok {
				hintDims[k] = dim
			}
		}
	}
	if len(hintDims) == 0 {
		return w.Append(cs)
	}
	// Hoist pass. The section's run order within a node follows map
	// iteration; refs address runs absolutely, so the layout is irrelevant.
	// nodes is a clone — the caller's slice elements keep their props.
	var sec []float32
	nodes := make([]NodeUpsert, len(cs.Nodes))
	copy(nodes, cs.Nodes)
	for i, n := range nodes {
		p, ok := vecbin.Hoist(n.Props, decls, &sec)
		if ok {
			nodes[i].Props = p
		}
	}
	// v1 rejects non-finite floats at json.Marshal time; the binary section
	// would carry them silently, so v2 refuses them too (spec §1.2: NaN/Inf
	// behavior matches v1 — NormalizeVec makes this unreachable in practice,
	// the guard covers direct callers).
	for _, f := range sec {
		if math.IsNaN(float64(f)) || math.IsInf(float64(f), 0) {
			return 0, fmt.Errorf("wal: hoisted vector section holds non-finite float %v (JSON channel rejects it; so does v2)", f)
		}
	}
	skeleton, err := json.Marshal(Changeset{Version: cs.Version, Nodes: nodes, Edges: cs.Edges,
		EdgeDeletes: cs.EdgeDeletes, NodeDeletes: cs.NodeDeletes})
	if err != nil {
		return 0, err
	}
	if len(hintDims) > 0xFFFF {
		return 0, fmt.Errorf("wal: %d hoisted vector props overflow the u16 hint count", len(hintDims))
	}
	hints := make([]v2Hint, 0, len(hintDims))
	for name, dim := range hintDims {
		if len(name) > 0xFFFF || dim > 0xFFFF {
			return 0, fmt.Errorf("wal: vector prop hint %q (dim %d) overflows the u16 envelope fields", name, dim)
		}
		hints = append(hints, v2Hint{name: name, dim: dim})
	}
	// Sorted names keep v2 frame bytes reproducible despite map iteration.
	slices.SortFunc(hints, func(a, b v2Hint) int { return strings.Compare(a.name, b.name) })
	return w.writeFrame(frameChangesetV2, appendV2Envelope(nil, hints, skeleton, sec))
}

// writeFrame is the shared append path: rollover check, write, LSN
// assignment, ModeAlways fsync.
func (w *WAL) writeFrame(typ byte, payload []byte) (uint64, error) {
	if w.f == nil {
		return 0, ErrClosed
	}
	frame := appendFrame(nil, typ, payload)
	if w.off > rolloverHeadroom && w.off+int64(len(frame)) > int64(segmentSize) {
		if err := w.createSegment(w.segment+1, w.salt+1, w.lsn+1); err != nil {
			return 0, err
		}
	}
	if _, err := w.f.WriteAt(frame, w.off); err != nil {
		return 0, err
	}
	w.off += int64(len(frame))
	w.bytes.Add(int64(len(frame)))
	w.retained.Add(int64(len(frame)))
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

// Frame is one replayed frame above the checkpoint watermark: exactly one of
// Changeset / IndexKeys is set.
type Frame struct {
	LSN       uint64
	Changeset *Changeset // frameChangeset
	IndexKeys []string   // frameIndexDecl
	// VectorIndexes rides frameIndexDecl payloads (spec §3.3): empty on
	// pre-Phase-1 frames.
	VectorIndexes []VectorIndexDecl
}

// Replay applies every frame above the manifest's base LSN to fn, in LSN
// order, and returns the last applied LSN. A torn or corrupt tail stops the
// replay at the first bad frame (returned as a normal end, not an error —
// crash-torn tails are expected); an fn error aborts and propagates. Replay
// is stateless with respect to the WAL: calling it twice replays the same
// frames twice.
func (w *WAL) Replay(fn func(Frame) error) (uint64, error) {
	return w.replayLSN(func(_ uint64, fr Frame) error { return fn(fr) })
}

// replayLSN is Replay with per-frame LSN visibility on the raw frame (used
// by tests to assert the manifest reconciliation invariant).
func (w *WAL) replayLSN(fn func(lsn uint64, fr Frame) error) (uint64, error) {
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
			if typ != frameChangeset && typ != frameIndexDecl && typ != frameChangesetV2 {
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
			var fr Frame
			fr.LSN = lsn
			if typ == frameIndexDecl {
				var d IndexDecl
				if err := json.Unmarshal(payload, &d); err != nil {
					stopped = true
					return errStopScan
				}
				fr.IndexKeys = d.Keys
				fr.VectorIndexes = d.VectorIndexes
			} else if typ == frameChangesetV2 {
				// A CRC-intact frame whose envelope still fails to decode
				// (bad magic, torn hint section, forged out-of-range $vec
				// ref) is not a crash artifact — real torn tails die in
				// scanFrames and end replay silently by design — so surface
				// it as an error instead of silently dropping every frame
				// behind it.
				cs, derr := decodeV2Changeset(payload)
				if derr != nil {
					return fmt.Errorf("wal: lsn %d: %w", lsn, derr)
				}
				fr.Changeset = cs
			} else {
				var cs Changeset
				if err := json.Unmarshal(payload, &cs); err != nil {
					stopped = true
					return errStopScan
				}
				fr.Changeset = &cs
			}
			if err := fn(lsn, fr); err != nil {
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
	w.bytes.Store(0) // the checkpoint owns everything logged so far
	w.retained.Store(0)
	w.startSeg = newSeg // the chain restarts at the fresh segment; everything older is deleted below
	for s := range w.segBase {
		if s < newSeg {
			delete(w.segBase, s)
		}
	}
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

// Rotate starts a fresh segment (salt+1, marker base lsn+1) and KEEPS every
// older segment on disk: the retained segment chain is the incremental
// checkpoint a manifest-v2 RetainFrom anchor names — replaying it over the
// last full snapshot reproduces state without writing one (the O(1) light
// checkpoint; recovery cost is bounded by the caller's compaction trigger
// over the retained bytes instead). Reset stays the compacting form and
// remains the only deleter; Rotate differs from it solely by not deleting,
// so LSN continuity, the recovery salt chain and torn-tail discipline are
// exactly a Reset's. bytes zeroes because the checkpoint boundary now owns
// the volume logged so far — the volume trigger must restart from the new
// segment — while retained keeps counting (see RetainedBytes: the chain-wide
// replay volume survives the rotation, and the compaction trigger reads it).
// The caller persists the v2 manifest after Rotate returns.
func (w *WAL) Rotate() error {
	if w.f == nil {
		return ErrClosed
	}
	if err := w.createSegment(w.segment+1, w.salt+1, w.lsn+1); err != nil {
		return err
	}
	w.bytes.Store(0)
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

// Size reports the valid frame bytes logged since the last checkpoint
// boundary (Reset, or Rotate's light form): the input for volume-triggered
// checkpoints (spec §4's "WAL ≥4MB" trigger). Preallocated zero tails never
// count. NOTE: after an in-process Rotate this restarts at zero even though
// the retained chain is still on disk — callers that need the whole live
// chain's replay volume (compaction triggers) must read RetainedBytes
// instead, never Size.
func (w *WAL) Size() int64 { return w.bytes.Load() }

// RetainedBytes reports the summed valid frame bytes across the live chain —
// every segment from FirstSegment() to the head, retained and current alike —
// which is exactly the replay volume the next recovery pays. Unlike Size(),
// an in-process Rotate does not reset it (the retained chain keeps counting),
// so a compaction trigger reading RetainedBytes can never be fooled into
// never firing by a sequence of light checkpoints. Only Reset (the compacting
// form, which deletes the chain) zeroes it, and recovery initializes it from
// the same walk that parks the head. Segment marker frames count (recovery's
// walk counts them too; the overshoot is one small marker per segment, which
// errs toward earlier compaction — the safe side).
func (w *WAL) RetainedBytes() int64 { return w.retained.Load() }

// FirstUncoveredSegment returns the first chain segment NOT fully covered
// by a snapshot through coverLSN - the async compact's busy-case
// completion uses it as the re-anchoring manifest's RetainFrom survivor.
// A segment's frames run [base(s), base(s+1)-1], so it is covered exactly
// when the NEXT segment's base is at or below coverLSN+1; the ACTIVE
// segment always counts as uncovered (the write head lives there and its
// frames may exceed any cover). A missing base entry keeps the segment
// (conservative: an unknown range is never deletable).
func (w *WAL) FirstUncoveredSegment(coverLSN uint64) int {
	for s := w.startSeg; s < w.segment; s++ {
		next, ok := w.segBase[s+1]
		if !ok || next > coverLSN+1 {
			return s
		}
	}
	return w.segment
}

// DropSegmentsBelow deletes every chain segment numbered below seg. The
// async compact's busy-case completion calls it AFTER the re-anchoring
// manifest landed, with seg = FirstUncoveredSegment(snapshot LSN) - the
// Reset discipline (manifest first, deletions only for frames the
// snapshot provably covers; the manifest's segment/LSN gating is what
// makes the deleted frames unreadable afterwards). Reset stays the
// compacting form's deleter; this is the second, segment-granular one.
// Writer-thread only. RetainedBytes sheds the dropped segments' valid
// bytes and startSeg advances; the active segment is never in range.
func (w *WAL) DropSegmentsBelow(seg int) error {
	if seg > w.segment {
		return fmt.Errorf("wal: DropSegmentsBelow(%d) would drop the active segment %d", seg, w.segment)
	}
	var dropped int64
	for s := w.startSeg; s < seg; s++ {
		if sc, err := scanSegmentFile(w.dir, s); err == nil {
			dropped += sc.valid
		}
		if err := os.Remove(segmentPath(w.dir, s)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		delete(w.segBase, s)
	}
	if dropped > 0 {
		w.retained.Add(-dropped)
	}
	if w.startSeg < seg {
		w.startSeg = seg
	}
	return nil
}

// FirstSegment reports the first segment of the live chain: the value a light
// checkpoint copies into manifest RetainFrom. It is maintained by recovery
// (the first segment Open's walk accepted — never a manifest floor naming an
// already-deleted segment) and by Reset (the chain restarts at the fresh
// segment); Rotate and in-segment rollovers never change it. 0 means the
// chain starts at segment 0 — inexpressible as RetainFrom (the v1 sentinel).
// The invariant is enforced at the tier decision, not just advised: a 0 here
// forces the compacting checkpoint, so the light tier can never emit a
// RetainFrom-0 manifest that orphans the retained chain.
func (w *WAL) FirstSegment() int { return w.startSeg }

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
		if typ != frameChangeset && typ != frameMarker && typ != frameIndexDecl && typ != frameChangesetV2 {
			break // unknown type: old-reader silent truncation (spec §1.3); 4 is known
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

// --- changeset v2 codec ------------------------------------------------------

// v2Magic prefixes every frameChangesetV2 payload; a type-4 payload without
// it is corruption (old readers cannot produce type 4 at all).
var v2Magic = [4]byte{'V', 'V', 'B', '2'}

// v2Hint is one hint-section entry: a prop name actually hoisted in this
// frame plus the dim its values carried. The authoritative addressing lives
// in each "$vec" ref; the hint exists so Replay can rebuild the declared-prop
// set from the frame alone — the envelope is self-describing and the WAL
// stays stateless (no IndexDecl frame needs to precede data frames).
type v2Hint struct {
	name string
	dim  int
}

// appendV2Envelope lays out a frameChangesetV2 payload:
// [4B "VVB2"][u16 nProps][u16 nameLen+name+u16 dim]*[u32 jsonLen]
// [json skeleton][raw float32 LE section]. Every part but the float section
// is length-prefixed, so the split is unambiguous and the section's size is
// implied by the payload tail. Callers pre-validate the u16 fields (name
// length, dim) — a silent uint16 truncation would forge a corrupt frame.
func appendV2Envelope(dst []byte, hints []v2Hint, skeleton []byte, sec []float32) []byte {
	dst = append(dst, v2Magic[:]...)
	var b [4]byte
	binary.LittleEndian.PutUint16(b[:2], uint16(len(hints)))
	dst = append(dst, b[:2]...)
	for _, h := range hints {
		binary.LittleEndian.PutUint16(b[:2], uint16(len(h.name)))
		dst = append(dst, b[:2]...)
		dst = append(dst, h.name...)
		binary.LittleEndian.PutUint16(b[:2], uint16(h.dim))
		dst = append(dst, b[:2]...)
	}
	binary.LittleEndian.PutUint32(b[:], uint32(len(skeleton)))
	dst = append(dst, b[:]...)
	dst = append(dst, skeleton...)
	for _, f := range sec {
		dst = binary.LittleEndian.AppendUint32(dst, math.Float32bits(f))
	}
	return dst
}

// splitV2Envelope parses a frameChangesetV2 payload back into its restore
// inputs: the declared-prop hints, the JSON skeleton and the raw float32
// section. Every bound is checked (hint counts, name lengths, jsonLen, float
// alignment); a violation is corruption.
func splitV2Envelope(p []byte) (decls vecbin.PropDecls, skeleton []byte, sec []float32, err error) {
	if len(p) < len(v2Magic)+2 || !bytes.Equal(p[:4], v2Magic[:]) {
		return nil, nil, nil, fmt.Errorf("bad v2 magic % x", p[:min(4, len(p))])
	}
	off := len(v2Magic)
	nProps := int(binary.LittleEndian.Uint16(p[off:]))
	off += 2
	decls = make(vecbin.PropDecls, nProps)
	for i := 0; i < nProps; i++ {
		if off+2 > len(p) {
			return nil, nil, nil, fmt.Errorf("v2 hint %d: truncated name length", i)
		}
		nameLen := int(binary.LittleEndian.Uint16(p[off:]))
		off += 2
		if off+nameLen+2 > len(p) {
			return nil, nil, nil, fmt.Errorf("v2 hint %d: name length %d overruns payload", i, nameLen)
		}
		name := string(p[off : off+nameLen])
		off += nameLen
		decls[name] = int(binary.LittleEndian.Uint16(p[off:]))
		off += 2
	}
	if off+4 > len(p) {
		return nil, nil, nil, fmt.Errorf("v2 envelope: truncated json length")
	}
	jsonLen := uint64(binary.LittleEndian.Uint32(p[off:])) // uint64: u32 must not sign-flip a 32-bit int
	off += 4
	if jsonLen > uint64(len(p)-off) {
		return nil, nil, nil, fmt.Errorf("v2 jsonLen %d overruns payload (len %d)", jsonLen, len(p)-off)
	}
	skeleton = p[off : off+int(jsonLen)]
	off += int(jsonLen)
	rest := p[off:]
	if len(rest)%4 != 0 {
		return nil, nil, nil, fmt.Errorf("v2 float section len %d is not a float32 multiple", len(rest))
	}
	sec = make([]float32, len(rest)/4) // copy: the payload buffer is scan scratch
	for i := range sec {
		sec[i] = math.Float32frombits(binary.LittleEndian.Uint32(rest[4*i:]))
	}
	return decls, skeleton, sec, nil
}

// decodeV2Changeset restores a frameChangesetV2 payload to the Changeset the
// caller handed AppendHoisted: split the envelope, JSON-decode the skeleton,
// then resolve every node's "$vec" refs against the section (bounds-checked
// by vecbin.Restore — out of range is corruption). Edge props never hoisted,
// so they never restore. Frame is unchanged: callers see a fully materialized
// Changeset and are unaware of the envelope.
func decodeV2Changeset(payload []byte) (*Changeset, error) {
	decls, skeleton, sec, err := splitV2Envelope(payload)
	if err != nil {
		return nil, err
	}
	var cs Changeset
	if err := json.Unmarshal(skeleton, &cs); err != nil {
		return nil, fmt.Errorf("v2 skeleton: %w", err)
	}
	for i := range cs.Nodes {
		if cs.Nodes[i].Props == nil {
			continue // nil props stay nil, exactly as a v1 frame decodes them
		}
		p, rerr := vecbin.Restore(cs.Nodes[i].Props, decls, sec)
		if rerr != nil {
			return nil, fmt.Errorf("node %q: %w", cs.Nodes[i].Key, rerr)
		}
		cs.Nodes[i].Props = p
	}
	return &cs, nil
}
