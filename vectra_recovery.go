// WAL session machinery: pend buffer recorder + flusher, Open recovery + migration
// gate, tiered checkpoints, and the async compact flight.

package vectra

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/projanvil/vectra/internal/apply"
	"github.com/projanvil/vectra/internal/propval"
	"github.com/projanvil/vectra/internal/store"
	"github.com/projanvil/vectra/internal/wal"
)

// opKind orders pend buffer entries exactly like the Changeset apply order:
// node upserts → edge upserts → edge deletes → node deletes. opIndex is
// outside that order: it never enters a Changeset — flushPending emits each
// run of index declarations as its own IndexDecl frame, preserving op order.
type opKind int

const (
	opNode opKind = iota
	opEdge
	opEdgeDelete
	opNodeDelete
	opIndex
	opVecIndex // declaration-class like opIndex: rides IndexDecl frames
	opFTS      // full-text declaration-class: rides IndexDecl frames too (FTS plan)
	opUnique   // unique-constraint declaration-class: rides IndexDecl frames too
)

// pendEntry is one line of the pend order log: the op's kind plus the
// index of its payload in the matching per-kind slice of pendBuf. idx is
// uint32 — a single unflushed buffer holds at most 4G ops of one kind,
// far beyond any real batch (>160 GB of payloads alone).
type pendEntry struct {
	kind opKind
	idx  uint32
}

// pendBuf is the recorded-write buffer (audit F-11): a compact order log
// plus one payload slice per kind, so an op costs its payload plus a
// 2-word log entry instead of a struct embedding every kind's payload
// (the old 240 B/op pendOp — 5x the 48 B/op a 2:1 node:edge mix needs,
// see docs/benchmarks/2026-11-01-pendop-buffer-ab.md). The order log is
// the single source of original-op-order truth flushPending replays; the
// per-kind indices are dense within their own slice only. Write-side
// invariant: every recorder appends the order entry and its payload as one
// statement pair — order entry first, its idx bound to the payload's dense
// index — so an order entry must never exist without its payload (and vice
// versa); rewind and flushPending rely on that pairing.
type pendBuf struct {
	order    []pendEntry // one entry per recorded op, in op order
	nodes    []NodeUpsert
	edges    []EdgeUpsert
	edgeDels []EdgeDelete
	strs     []string                 // opNodeDelete / opIndex / opFTS: the single string payload
	vecDecls [][]wal.VectorIndexDecl  // opVecIndex: full declaration list, one per op
	ucDecls  [][]wal.UniqueConstraint // opUnique: full declaration list, one per op
}

// pendRetainCap bounds the backing arrays reset() keeps between batches:
// ordinary batches (thousands of ops) reuse their arrays, so batch flushes
// stop paying append-amortized growth allocations, while a one-off giant
// batch releases its arrays instead of pinning them forever.
const pendRetainCap = 1 << 16 // entries per slice

// pendTrunc empties s for reuse: zeroed first so no payload reference
// (key string, Props map) outlives the flush, then re-based to len 0.
func pendTrunc[T any](s []T) []T {
	if cap(s) > pendRetainCap {
		return nil
	}
	clear(s)
	return s[:0]
}

// reset empties the buffer after a successful flush, retaining the backing
// arrays (bounded by pendRetainCap).
func (p *pendBuf) reset() {
	p.order = pendTrunc(p.order)
	p.nodes = pendTrunc(p.nodes)
	p.edges = pendTrunc(p.edges)
	p.edgeDels = pendTrunc(p.edgeDels)
	p.strs = pendTrunc(p.strs)
	p.vecDecls = pendTrunc(p.vecDecls)
	p.ucDecls = pendTrunc(p.ucDecls)
}

// rewind drops the flushed prefix [0:i) after a failed Append and re-bases
// the payload slices onto the surviving order-log entries, so the next
// flush re-attempts exactly the unflushed ops, in the original order.
// Fresh slices (no capacity retention): the error path is rare, and the
// retry buffer must never alias the arrays the caller may still inspect.
func (p *pendBuf) rewind(i int) {
	if i == 0 {
		return
	}
	np := pendBuf{}
	for _, e := range p.order[i:] {
		switch e.kind {
		case opNode:
			np.order = append(np.order, pendEntry{kind: opNode, idx: uint32(len(np.nodes))})
			np.nodes = append(np.nodes, p.nodes[e.idx])
		case opEdge:
			np.order = append(np.order, pendEntry{kind: opEdge, idx: uint32(len(np.edges))})
			np.edges = append(np.edges, p.edges[e.idx])
		case opEdgeDelete:
			np.order = append(np.order, pendEntry{kind: opEdgeDelete, idx: uint32(len(np.edgeDels))})
			np.edgeDels = append(np.edgeDels, p.edgeDels[e.idx])
		case opNodeDelete, opIndex, opFTS:
			np.order = append(np.order, pendEntry{kind: e.kind, idx: uint32(len(np.strs))})
			np.strs = append(np.strs, p.strs[e.idx])
		case opVecIndex:
			np.order = append(np.order, pendEntry{kind: opVecIndex, idx: uint32(len(np.vecDecls))})
			np.vecDecls = append(np.vecDecls, p.vecDecls[e.idx])
		case opUnique:
			np.order = append(np.order, pendEntry{kind: opUnique, idx: uint32(len(np.ucDecls))})
			np.ucDecls = append(np.ucDecls, p.ucDecls[e.idx])
		default:
			panic(fmt.Sprintf("vectra: unhandled pend kind %d in rewind", e.kind))
		}
	}
	*p = np
}

// isDeclKind reports whether k is one of the declaration-class kinds that
// ride IndexDecl frames (typed keys, vector, full-text, unique).
func isDeclKind(k opKind) bool {
	return k == opIndex || k == opVecIndex || k == opFTS || k == opUnique
}

// --- WAL op recorder ---------------------------------------------------------
//
// When a WAL is attached, each successful write method records one pend entry
// BEFORE mutating memory (log-first for single writes). Outside Batch the op
// flushes immediately as its own frame — one fsync per write in ModeAlways,
// the documented cost of unbatched direct-mode writes — and the layer
// materialization follows the frame, so a flush failure leaves memory
// untouched. Inside Batch the ops accumulate and the boundary flushes them
// as frame(s), then materializes one micro-layer for the whole batch.
//
// Frame partitioning: a Changeset applies ops in kind order (nodes → edges →
// edge deletes → node deletes), so a batch whose op sequence is NOT
// kind-ordered (e.g. delete a key, then re-add it) cannot be expressed as one
// frame without reordering semantics. flushPending therefore cuts the op
// sequence at every kind regression and emits one frame per kind-ordered run
// — replaying the frames reproduces the original op order exactly. Normal
// batches stay a single frame (one fsync).
//
// Append/fsync failure discipline: a write method (or Batch/Close flush)
// that returns a WAL error must be retried or the graph abandoned — never
// silently continued. A failed Append may still have put the frame on disk
// (a write/fsync can fail after partial success), and any ops it did not
// persist stay buffered in the pend buffer, so the next flush (Batch boundary, or
// Close) re-attempts exactly those; continuing to write without a
// successful flush risks losing the earlier ops.

func (g *Graph) recordNode(key, label string, props map[string]any) error {
	if g.wal == nil {
		return nil
	}
	p := &g.pendOps
	p.order = append(p.order, pendEntry{kind: opNode, idx: uint32(len(p.nodes))})
	p.nodes = append(p.nodes, NodeUpsert{Key: key, Label: label, Props: propval.Copy(props)})
	return g.maybeFlush()
}

func (g *Graph) recordEdge(srcKey, dstKey, label string, props map[string]any, allowParallel bool) error {
	if g.wal == nil {
		return nil
	}
	p := &g.pendOps
	p.order = append(p.order, pendEntry{kind: opEdge, idx: uint32(len(p.edges))})
	p.edges = append(p.edges, EdgeUpsert{
		SrcKey: srcKey, DstKey: dstKey, Label: label,
		Props: propval.Copy(props), AllowParallel: allowParallel,
	})
	return g.maybeFlush()
}

func (g *Graph) recordEdgeDelete(srcKey, dstKey, label string) error {
	if g.wal == nil {
		return nil
	}
	p := &g.pendOps
	p.order = append(p.order, pendEntry{kind: opEdgeDelete, idx: uint32(len(p.edgeDels))})
	p.edgeDels = append(p.edgeDels, EdgeDelete{SrcKey: srcKey, DstKey: dstKey, Label: label})
	return g.maybeFlush()
}

func (g *Graph) recordNodeDelete(key string) error {
	if g.wal == nil {
		return nil
	}
	p := &g.pendOps
	p.order = append(p.order, pendEntry{kind: opNodeDelete, idx: uint32(len(p.strs))})
	p.strs = append(p.strs, key)
	return g.maybeFlush()
}

func (g *Graph) recordIndexDecl(key string) error {
	if g.wal == nil {
		return nil
	}
	p := &g.pendOps
	p.order = append(p.order, pendEntry{kind: opIndex, idx: uint32(len(p.strs))})
	p.strs = append(p.strs, key)
	return g.maybeFlush()
}

// recordFTSDecl is recordIndexDecl for the full-text kind (FTS plan): an
// opFTS pend entry coalesced into the same IndexDecl frame run at flush.
func (g *Graph) recordFTSDecl(key string) error {
	if g.wal == nil {
		return nil
	}
	p := &g.pendOps
	p.order = append(p.order, pendEntry{kind: opFTS, idx: uint32(len(p.strs))})
	p.strs = append(p.strs, key)
	return g.maybeFlush()
}

// maybeFlush flushes unless a Batch boundary is holding the buffer.
func (g *Graph) maybeFlush() error {
	if g.batching {
		return nil
	}
	return g.flushPending()
}

// flushPending emits the buffered ops as one frame per kind-ordered run;
// consecutive index declarations (typed, vector, full-text and unique-
// constraint alike) coalesce into one IndexDecl frame at their position in
// the op order. On error the unflushed ops stay buffered so a retry (e.g. a
// retried Close) does not silently drop them.
func (g *Graph) flushPending() error {
	if g.wal == nil {
		g.pendOps = pendBuf{}
		return nil
	}
	p := &g.pendOps
	i := 0
	for i < len(p.order) {
		if isDeclKind(p.order[i].kind) {
			j := i
			keys := make([]string, 0, 4)
			var ftsKeys []string
			var vecs []wal.VectorIndexDecl
			var ucs []wal.UniqueConstraint
			for j < len(p.order) && isDeclKind(p.order[j].kind) {
				switch p.order[j].kind {
				case opIndex:
					keys = append(keys, p.strs[p.order[j].idx])
				case opFTS:
					ftsKeys = append(ftsKeys, p.strs[p.order[j].idx])
				case opUnique:
					ucs = p.ucDecls[p.order[j].idx]
				default:
					vecs = p.vecDecls[p.order[j].idx]
				}
				j++
			}
			if _, err := g.wal.AppendIndex(IndexDecl{Keys: keys, VectorIndexes: vecs, FullTextKeys: ftsKeys, UniqueConstraints: ucs}); err != nil {
				p.rewind(i)
				return err
			}
			i = j
			continue
		}
		j := i + 1
		for j < len(p.order) && !isDeclKind(p.order[j].kind) && p.order[j].kind >= p.order[j-1].kind {
			j++
		}
		cs := Changeset{Version: changesetVersion}
		for _, e := range p.order[i:j] {
			switch e.kind {
			case opNode:
				cs.Nodes = append(cs.Nodes, p.nodes[e.idx])
			case opEdge:
				cs.Edges = append(cs.Edges, p.edges[e.idx])
			case opEdgeDelete:
				cs.EdgeDeletes = append(cs.EdgeDeletes, p.edgeDels[e.idx])
			case opNodeDelete:
				cs.NodeDeletes = append(cs.NodeDeletes, p.strs[e.idx])
			default:
				panic(fmt.Sprintf("vectra: unhandled pend kind %d in flushPending", e.kind))
			}
		}
		// Frame selection (spec 2026-10-21 §1.2): declared vector props ride
		// the v2 binary section; anything else falls back inside AppendHoisted
		// to the byte-identical v1 frame. Decls are computed here, at the
		// logging call — declarations can change between batches and a cached
		// map would go stale.
		if _, err := g.wal.AppendHoisted(cs, g.vecPropDecls()); err != nil {
			p.rewind(i)
			return err
		}
		i = j
	}
	p.reset()
	return nil
}

// replayChangeset replays one frame with the write-method semantics (the
// shared apply walk under apply.StopAtFirst — audit F-02). Used by Open's
// recovery path (recording is detached there — replaying must not
// re-record). One Batch boundary per frame: recovery rebuilds one micro-layer
// per replayed frame. Any error means the frame references state the replay
// did not rebuild identically: surfaced raw, not skipped (the graph is
// abandoned by the caller, the partially-applied layer never escapes Open).
func (g *Graph) replayChangeset(cs Changeset) error {
	var firstErr error
	err := g.Batch(func(g *Graph) {
		if fatal := g.ApplyChangeset(cs, apply.StopAtFirst, nil); fatal.Err != nil {
			firstErr = fatal.Err
		}
	})
	if err != nil {
		return err
	}
	return firstErr
}

// Open opens (or creates) a durable graph session at path. Unlike
// LoadFile — which merges the snapshot into a base — an Open session
// recovers into a writable graph: snapshot first (when a checkpoint exists),
// then WAL frames replayed in LSN order above the checkpoint boundary (one
// micro-layer per frame), with no recording during replay. Merging is
// deferred to the checkpoint at Close or an explicit Seal.
//
// v6 migration + fresh-library tripwire (spec 2026-10-21 §1.3, rulings
// #4/#9; target lifted v5 -> v6 by the 2026-10-22 retained-WAL checkpoint
// plan): a WAL-mode Open ENDS with one Seal unless the recovered head is
// already a v6 snapshot — legacy (v1-v5) libraries migrate in place, and
// fresh or WAL-only libraries (no snapshot head) get an empty/full v6
// checkpoint, so the on-disk state an older binary would open always
// hard-fails at the snapshot version byte instead of silently losing data
// (a v0.10 binary truncates unknown type-4 WAL frames; a v0.11.1 binary
// ignores manifest v2's RetainFrom and would read a light-checkpointed
// base alone, missing the retained chain). Reopening a v6 library seals
// nothing (idempotent). ModeOff Opens never migrate (see the branch
// below). The pre-migration watermark is retained as a copy
// <name>-v<ver>.bak before the migration seal (rollback artifact: recovery
// never reads it, later seals never delete it; the original stays in place
// until the seal's own cleanup removes it, so no crash window opens).
//
// Checkpoints never write `path` in place: each one writes a watermark
// snapshot <path minus ext>-<lsn>.db next to path and atomically points the
// manifest at it (old watermarks are deleted only after the new manifest
// lands). A crash between SaveFile and the manifest write therefore recovers
// from the previous, untouched watermark plus a full WAL replay — already
// checkpointed frames can never be applied twice (AllowParallel appends and
// deletes are not idempotent).
//
// Recovery rules:
//   - manifest present: load the snapshot file it names (resolved next to
//     path); frames with lsn > manifest.lsn replay on top. A missing
//     snapshot file is an error.
//   - no manifest, no WAL segments, snapshot present (first WAL-mode Open
//     next to a v0.6 file): the snapshot is adopted.
//   - no manifest, WAL segments present (checkpoint manifest lost, or a
//     session that never Closed): recovery starts from an empty graph and
//     replays every frame — the WAL is the source of truth.
//
// Close checkpoints (tiered, v0.11.2: light rotation under the compaction
// threshold, full merge → SaveFile → manifest → WAL reset otherwise); a
// checkpoint failure returns an error and keeps the WAL for the next
// recovery attempt.
func Open(path string, opts Options) (*Graph, error) {
	if opts.WALMode == ModeOff {
		// v0.6 behavior: snapshot-only. Existing file loads merged; a missing
		// file starts a fresh writable graph (durability via Seal+SaveFile).
		//
		// No v6 migration here (spec §1.3 scopes it to WAL-mode Opens): Seal
		// without a WAL is merge-only — it never writes a snapshot — so an
		// in-place migration would be a SaveFile behind the caller's back,
		// changing ModeOff's read-only Open contract. ModeOff also never
		// replays a WAL, which is where the silent-truncation hazard lives;
		// Decode reads v1-v6 transparently and any caller-side SaveFile
		// writes v6 through the same Encode path.
		if _, err := os.Stat(path); err == nil {
			return LoadFile(path)
		} else if !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
		return New(), nil
	}

	walDir := opts.WALDir
	if walDir == "" {
		walDir = path + ".wal"
	}
	man, hasMan, err := wal.ReadManifest(walDir)
	if err != nil {
		return nil, err
	}
	hasSegs, err := wal.HasSegments(walDir)
	if err != nil {
		return nil, err
	}

	// Forest-cache pre-gate (spec §2.3, stale-tolerant since v0.11.2): CRC
	// must hold and the cache's LSN must be AT OR PAST the manifest's
	// compact boundary (img.lsn >= man.LSN) before the snapshot load; the
	// declaration-set gate runs later, in loadGraph, once the registry
	// exists to compare against. Without a manifest there is no boundary to
	// check (WAL-only recovery, fresh library, v0.6 adoption), so a present
	// cache is unusable. Every failure discards the file — the cache is
	// never a source of truth; the store snapshot and the WAL are, and the
	// rebuild path serves both cases.
	//
	// Why >= and not == (plan review focus #4): the compact tier writes
	// its manifest and its forest at the SAME lsn and the light tier moves
	// neither, so every rested pipeline state has img.lsn == man.LSN — the
	// exact-match fast path over an empty chain, behavior unchanged. The
	// reachable divergence is the compact crash window where the manifest
	// landed but the forest rename did not (img.lsn < man.LSN): discard,
	// as before — that forest is missing frames the base snapshot already
	// absorbed, and replay never re-delivers them (wal.Replay skips every
	// frame at or below the manifest LSN), so mounting it would silently
	// lose the window. img.lsn > man.LSN — a manifest rolled back beneath
	// a newer forest — is a valid PREFIX of the recoverable history: the
	// retained-chain replay re-applies the (man.LSN, forestLSN] window onto
	// the mounted forest and applies everything above forestLSN as fresh
	// increments.
	//
	// Idempotency of that re-application, verified against internal/vector
	// State semantics and the vecIntake paths:
	//   - the forest serializes the whole State — vidOf, nextVID, deadBits,
	//     overlay, tiers — so a re-Add of a key the mounted forest already
	//     knows reuses its vid and appends an overlay entry carrying the
	//     identical normalized vector;
	//   - the read path collapses duplicates by vid (vecView.search merges
	//     into a map[vid]best; overlay entries assign in scan order, so the
	//     newest occurrence wins and identical vectors score identically),
	//     and Pack/packSeg dedup by vid with newest-wins while never
	//     inserting dead vids — re-applied entries never surface as
	//     duplicates in queries or packed segments;
	//   - Delete re-sets an idempotent dead bit and no-ops on unknown keys,
	//     Add clears it (revive), so LSN-ordered replay — the same order
	//     the forest state absorbed — re-derives the identical final dead
	//     set and live overlay;
	//   - replay reaches State only through vecIntakeNode/vecIntakeDelete
	//     on the writer thread (replayChangeset -> AddNode/DeleteNode), the
	//     same funnel that produced the forest, so normalization and
	//     ordering match exactly.
	// Re-application costs O(chain window), bounded by the compaction
	// threshold; skipping vector ops for frames <= forestLSN is
	// deliberately out of scope (correctness over speed).
	var ff *vecForestImage
	if img, err := readVecForest(vecForestPath(path)); img != nil || err != nil {
		if err == nil && hasMan && img.lsn >= man.LSN {
			ff = img
		} else {
			discardVecForest(vecForestPath(path))
		}
	}

	var g *Graph
	// snapVer is the store version of the snapshot recovery loads below
	// (0 = nothing was loaded: fresh library, or WAL-only recovery). Open's
	// v6/tripwire gate at the end reads it.
	var snapVer uint16
	// legacySnap is the manifest-named watermark snapshot when one was loaded:
	// the pre-migration bytes the migration gate at the end must retain as a
	// rollback artifact before its Seal's cleanup deletes them ("" otherwise).
	var legacySnap string
	loadSnapshot := func(p string) error {
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		defer f.Close()
		if snapVer, err = store.ProbeVersion(f); err != nil {
			return err
		}
		if _, err = f.Seek(0, io.SeekStart); err != nil {
			return err
		}
		lg, err := loadGraphForest(f, ff)
		if err != nil {
			return err
		}
		g = lg
		return nil
	}
	switch {
	case hasMan:
		// The manifest names the exact watermark snapshot this session
		// continues from; `path` itself is never the checkpoint target.
		snap := filepath.Join(filepath.Dir(path), man.Snapshot)
		if err := loadSnapshot(snap); err != nil {
			return nil, fmt.Errorf("vectra: manifest at %s references snapshot %s: %w", walDir, snap, err)
		}
		legacySnap = snap
	case hasSegs:
		// WAL-only recovery: start empty, replay everything below.
	default:
		if _, err := os.Stat(path); err == nil {
			if err := loadSnapshot(path); err != nil {
				return nil, err
			}
		} else if !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
	}
	if g == nil {
		g = New()
	}

	g.onVecSkip = opts.OnVecSkip
	w, err := wal.Open(walDir, opts.WALMode)
	if err != nil {
		return nil, err
	}
	// Replay with recording detached (g.wal still nil): replaying must not
	// amplify frames. Torn tails stop at the last complete frame. IndexDecl
	// frames rebuild their declarations through IndexProperty/IndexFullText
	// and the applyVecDecls/applyUniqueDecls REPLACE branches (all no-op
	// records while g.wal is nil).
	if _, err := w.Replay(func(fr wal.Frame) error {
		g.replayLSN = fr.LSN // forest-coverage gate (see vecIntakeNode)
		if fr.Changeset != nil {
			return g.replayChangeset(*fr.Changeset)
		}
		for _, k := range fr.IndexKeys {
			g.IndexProperty(k)
		}
		for _, k := range fr.FullTextKeys {
			g.IndexFullText(k)
		}
		if fr.VectorIndexes != nil {
			// nil = keys-only frame (pre-Phase-1 or IndexProperty); a
			// non-nil slice - including empty - is a vector declaration
			// frame: empty means drop-all.
			g.applyVecDecls(fr.VectorIndexes)
		}
		if fr.UniqueConstraints != nil {
			// Same IndexDecl rule: nil = pre-feature frame, the registry
			// stands; non-nil INCLUDING empty replaces the whole
			// declaration set — merge-style replay would resurrect dropped
			// constraints (§3.4).
			g.applyUniqueDecls(fr.UniqueConstraints)
		}
		return nil
	}); err != nil {
		w.Close()
		return nil, err
	}
	// The coverage gate is replay-only: a stale replayLSN would silently
	// drop every live vector intake of this session when the mounted
	// forest covers the whole chain (final-review C1 - the B1 shape ends
	// replay exactly at the watermark).
	g.replayLSN = 0
	g.wal, g.walDir, g.dbPath = w, walDir, path
	g.vecKick() // recovery: replays left every vector in the overlay; drain bottom-up (spec 7.1)
	// v6 migration + fresh-library tripwire (spec §1.3, rulings #4/#9;
	// target lifted v5 -> v6 by the 2026-10-22 retained-WAL checkpoint
	// plan), at the END of Open: no writer is attached and no reader holds
	// the graph yet, so one Seal runs uncontended. The replayed tail may
	// contain v2 (type-4) frames that a v0.10 binary silently truncates,
	// fresh or WAL-only recoveries leave no snapshot head at all, and — the
	// v6 reason — a v0.11.1 binary ignores manifest v2's RetainFrom and
	// would read a light-checkpointed library's base alone, silently
	// missing the retained chain. Folding the recovered state into a
	// current-version watermark snapshot closes all three: old readers
	// hard-fail on the version byte instead of misreading the layout. The
	// gate is exact (snapVer==6 ⇒ nothing), so reopening a v6 library is
	// write-free; v4 AND v5 bases now migrate (v6 is a version-only bump
	// over v5, so a v5-built library is byte-identical but for the stamp).
	// The migration seal always takes the COMPACT tier (compactTier forces
	// it on a non-v6 base), so the product is a real current-version (v7)
	// snapshot, never a light rotation over a stale base. A failed seal fails Open — the library
	// must not pretend to be fine while its head stays
	// downgrade-truncatable; recovery state survives either way
	// (pre-manifest failures keep the WAL as the source of truth, a
	// manifest that already landed makes the next Open a clean
	// no-migration session).
	if snapVer < store.VersionV6 {
		// Rollback artifact: the Seal's checkpoint deletes the pre-migration
		// watermark via cleanupSnapshots — COPY it aside first (the manifest
		// still names the original until the seal's WriteManifest lands, so a
		// rename would open a crash window; .bak never matches the cleanup
		// pattern, and a failed seal keeps the .bak as the artifact). The
		// stamp is the ACTUAL pre-version (a v5 migration leaves
		// <name>-v5.bak), Copy errors are ignored (best effort, cleanup
		// discipline).
		retainLegacyArtifact(legacySnap, snapVer)
		if err := g.Seal(); err != nil {
			w.Close()
			return nil, fmt.Errorf("vectra: migrating %s to current snapshot version: %w", path, err)
		}
	}
	return g, nil
}

// snapshotFile is the checkpoint target for a given watermark: <dbPath minus
// ext>-<lsn>.db in the same directory. The LSN in the name is what closes
// the checkpoint crash window — a snapshot is only ever referenced by a
// manifest that names it exactly, and files are never overwritten in place
// across checkpoints.
func (g *Graph) snapshotFile(lsn uint64) string {
	return fmt.Sprintf("%s-%d.db", strings.TrimSuffix(g.dbPath, filepath.Ext(g.dbPath)), lsn)
}

// cleanupSnapshots deletes watermark snapshots from older checkpoints (best
// effort: leftovers are ignored by recovery — the manifest only ever names
// one — and the next checkpoint retries the cleanup).
func (g *Graph) cleanupSnapshots(keep string) {
	namePrefix := filepath.Base(strings.TrimSuffix(g.dbPath, filepath.Ext(g.dbPath)) + "-")
	dir := filepath.Dir(g.dbPath)
	es, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range es {
		if e.IsDir() || e.Name() == keep {
			continue
		}
		if !strings.HasPrefix(e.Name(), namePrefix) || !strings.HasSuffix(e.Name(), ".db") {
			continue
		}
		// Only <base>-<lsn>.db files are checkpoint artifacts: the middle
		// segment must be all digits. User files that merely share the
		// prefix (graph-backup.db, graph-export.db, date-stamped exports)
		// are never touched.
		mid := strings.TrimSuffix(strings.TrimPrefix(e.Name(), namePrefix), ".db")
		if _, err := strconv.ParseUint(mid, 10, 64); err != nil {
			continue
		}
		os.Remove(filepath.Join(dir, e.Name()))
	}
}

// Compact is the explicit compaction entry point (spec §6): fold every
// unmerged layer into a fresh immutable base and — when a WAL is attached —
// run the COMPACT checkpoint tier unconditionally (current-version (v7)
// watermark snapshot + manifest + forest + WAL reset deleting the retained
// chain), whatever the
// retained volume: the caller asked for a full checkpoint, not a rotation.
// Spec §3.1's "deep compaction renumbering" is absorbed by the merge itself:
// folding already rebuilds node and edge IDs densely (R1). Seal differs only
// in its checkpoint tiering (light under the compaction threshold); the
// merge halves are the same pipeline. Like Seal it is a write-path method
// guarded by the writer discipline (ErrNotWriter off-writer in debug
// builds): concurrent-safe only via the writer handle or single-goroutine
// discipline — it rebuilds writer-private state.
func (g *Graph) Compact() error {
	if err := g.WaitAsyncCompact(); err != nil {
		return err
	}
	if err := g.checkWriter(); err != nil {
		return err
	}
	if g.wal != nil {
		if err := g.flushPending(); err != nil {
			return err
		}
	}
	if err := g.forceMerge(); err != nil {
		return err
	}
	if g.wal == nil {
		return nil
	}
	return g.compactCheckpoint(g.wal.LastLSN())
}

// checkpoint persists the session durably in one of two tiers (v0.11.2):
//
//   - light: Rotate the WAL and rewrite the manifest (v2, RetainFrom>0) so
//     recovery widens its replay to the retained chain — O(1): no snapshot,
//     no forest, no cleanup. The manifest's Snapshot/LSN stay AT THE COMPACT
//     BOUNDARY: the LSN watermark is the only application gate, and moving it
//     forward without a fresh base would silently drop every frame in
//     between. The retained segment chain IS the increment.
//   - compact: the full v0.11.1 pipeline — current-version (v7) watermark
//     snapshot, manifest
//     (v1-shaped: RetainFrom cleared, Format 2), forest cache, Reset (which
//     deletes the retained chain), old-watermark cleanup.
//
// The compact tier is forced when (a) no manifest-named snapshot exists yet —
// the FIRST checkpoint of a lineage must compact because RetainFrom 0 is the
// v1 sentinel and a retained chain is then inexpressible; a boundary LSN of 0
// means only the byte-one tripwire seal's empty base exists, so the first
// user checkpoint still compacts; (b) the named base is not a store-v6
// snapshot — a pre-v6 base kept alive under a manifest v2 is the silent-loss
// downgrade hazard (a pre-0.11.2 binary ignores RetainFrom and reads the base
// alone), so migration and tripwire seals always compact; (c) the live
// chain's retained bytes have reached the compaction threshold, bounding the
// recovery replay volume; or (d) the live chain's first segment is 0 —
// RetainFrom 0 is the v1 sentinel, so a light manifest (which copies
// FirstSegment verbatim) would floor recovery at Segment and silently skip
// the whole retained chain. Structurally unreachable through the pipeline
// (every LSN>0 manifest descends from a compact whose Reset restarts the
// chain at ≥1), but enforced at the tier decision so no future writer — the
// migration seal next — can ever emit it.
func (g *Graph) checkpoint(lsn uint64) error {
	compact, man, err := g.compactTier()
	if err != nil {
		return err
	}
	if compact {
		return g.compactCheckpoint(lsn)
	}
	return g.lightCheckpoint(man)
}

// DefaultCompactBytes is the retained-WAL volume at which a checkpoint flips
// from the light tier (O(1): WAL rotate + manifest rewrite) to the compacting
// tier (full v6 snapshot + retained-chain deletion + forest). The bound
// exists for RECOVERY, not for the write path: every retained byte is a byte
// the next Open must replay, so an unbounded chain would trade checkpoint
// latency for unbounded restart cost. 256MB follows the RocksDB
// max_total_wal_size precedent of capping total WAL volume to bound recovery;
// shrinking it trades checkpoint pauses for faster restarts, growing it the
// reverse.
const DefaultCompactBytes int64 = 256 << 20

// compactBytes is the runtime copy of DefaultCompactBytes. It is a var (not
// the const itself) purely so the internal test suite can force the
// compacting tier on tiny libraries; production code never writes it.
var compactBytes = DefaultCompactBytes

// compactTier decides the next checkpoint's tier. The CURRENT on-disk
// manifest — the exact bytes recovery would read — is the single source for
// the compact boundary the light tier must carry unchanged (Snapshot/LSN) and
// for the base-snapshot name whose store version gates light eligibility.
func (g *Graph) compactTier() (compact bool, man wal.Manifest, err error) {
	man, ok, err := wal.ReadManifest(g.walDir)
	if err != nil {
		return false, man, err
	}
	if !ok || man.Snapshot == "" || man.LSN == 0 || !g.snapshotAtLeastV6(man) ||
		g.wal.RetainedBytes() >= compactBytes || g.wal.FirstSegment() == 0 {
		return true, man, nil
	}
	return false, man, nil
}

// snapshotAtLeastV6 reports whether the manifest-named base snapshot is a
// store-v6-OR-NEWER file — the hard downgrade tripwire floor. A light
// checkpoint keeps the base, so it is legal only over v6+: an older base
// under a manifest v2 is silently lossy for pre-0.11.2 binaries (they
// ignore RetainFrom and open the base alone), so v1-v5 bases force the
// compacting tier, which rewrites the base as the current version. v7
// bases (and any future bump) carry the same retained-WAL safety, hence
// the >=. Any probe failure also forces compaction — the compact tier
// rewrites the base and self-heals.
func (g *Graph) snapshotAtLeastV6(man wal.Manifest) bool {
	f, err := os.Open(filepath.Join(filepath.Dir(g.dbPath), man.Snapshot))
	if err != nil {
		return false
	}
	defer f.Close()
	v, err := store.ProbeVersion(f)
	return err == nil && v >= store.VersionV6
}

// compactCheckpoint is the full-snapshot tier: SaveFile (v7) → manifest →
// forest → Reset (deletes the retained chain only now, after the manifest
// landed — a crash before the manifest leaves the old base plus the old
// retained chain fully replayable) → old-watermark cleanup. Order and failure
// semantics are exactly the v0.11.1 pipeline, retargeted at manifest v2.
func (g *Graph) compactCheckpoint(lsn uint64) error {
	snap := g.snapshotFile(lsn)
	if err := g.SaveFile(snap); err != nil {
		return err
	}
	if err := wal.WriteManifest(g.walDir, wal.Manifest{
		Snapshot: filepath.Base(snap),
		Segment:  g.wal.Segment(),
		LSN:      lsn,
		Format:   2, // RetainFrom stays 0: omitempty renders a v1-shaped file
	}); err != nil {
		return err
	}
	// The forest cache joins the checkpoint batch here: snapshot and manifest
	// have landed, so a crash before the cache rename below leaves the OLD
	// cache (watermark mismatch → discard at Open) or none at all — both
	// recover through the rebuild path (spec §2.3).
	if err := g.writeVecForest(lsn); err != nil {
		return err
	}
	if err := g.wal.Reset(lsn); err != nil && !errors.Is(err, wal.ErrClosed) {
		return err
	}
	g.cleanupSnapshots(filepath.Base(snap))
	return nil
}

// compactFlight is one in-progress background write of a compact
// checkpoint's BYTES (snapshot + forest). The writer pinned view and lsn at
// start; the goroutine writes both files from that immutable state and
// reports through done/err. Every manifest write and WAL mutation happens
// in the writer-side completion (PollAsyncCompact/WaitAsyncCompact), never
// here - Segment fields need the writer's current head and Reset/Drop must
// serialize with appends.
type compactFlight struct {
	lsn  uint64
	view *genView
	ucs  []store.UniqueConstraint // declaration snapshot pinned with the view (writer-private registry)
	err  error
	done chan struct{}
}

// TryAsyncCompact starts a compact-tier flight if one is due and none is
// active (writer thread). Returns whether a flight started; false also
// covers "suppressed: a flight is already in flight" - the caller defers
// its volume checkpoint to a later batch. The pinned view is the CURRENT
// published generation after a pending flush: export walks it immutably
// while the writer keeps cutting layers on top.
//
// Wrapper-mode coupling surface: internal/writer only, not covered by API
// stability.
func (g *Graph) TryAsyncCompact() bool {
	if g.wal == nil || g.compactFlight != nil {
		return false
	}
	compact, _, err := g.compactTier()
	if err != nil || !compact {
		return false // light tier or unreadable manifest: the synchronous path owns both
	}
	if err := g.flushPending(); err != nil {
		return false // surfaced by the next synchronous checkpoint
	}
	// Publish the pend before pinning: flushPending emits WAL frames but
	// does NOT cut the layer (Save's first step is flushPend). In wrapper
	// mode the batch's g.Batch end already published; in direct mode this
	// is what brings sub-pendMaxOps builder contents into the view.
	g.flushPend()
	fl := &compactFlight{lsn: g.wal.LastLSN(), view: g.view.Load(), ucs: g.uniqueStoreDecls(), done: make(chan struct{})}
	g.compactFlight = fl
	go g.runCompactFlight(fl)
	return true
}

// runCompactFlight is the background stage: write the pinned view as the
// watermark snapshot, then the forest cache at the same LSN (its encoded
// State may be newer than the pin - the cache load gate and the recovery
// intake's coverage gate absorb the overlap). No manifest, no WAL contact.
func (g *Graph) runCompactFlight(fl *compactFlight) {
	defer close(fl.done)
	if g.compactFlightHook != nil {
		if err := g.compactFlightHook(); err != nil {
			fl.err = err
			return
		}
	}
	if err := g.saveViewFile(fl.view, g.snapshotFile(fl.lsn), fl.ucs); err != nil {
		fl.err = err
		return
	}
	if err := g.writeVecForest(fl.lsn); err != nil {
		fl.err = err
		return
	}
}

// AsyncCompactActive reports whether a flight is in progress (writer
// thread).
//
// Wrapper-mode coupling surface: internal/writer only, not covered by API
// stability.
func (g *Graph) AsyncCompactActive() bool { return g.compactFlight != nil }

// PollAsyncCompact completes a finished flight on the writer thread and
// returns its error; an in-flight one is left alone (nil). The completion
// is the whole manifest+WAL side of the compact tier:
//
//   - quiescent (no frames past the pin): exactly today's compact
//     checkpoint - manifest, Reset, cleanup;
//   - busy (the writer appended during the flight - 1M measured ~480MB of
//     frames per 26s flight): Reset at the pin would DELETE fsynced frames
//     the snapshot does not cover. Instead the manifest re-anchors v2-style
//     at the flight's snapshot (LSN=pin, RetainFrom=the first segment not
//     fully covered) and only the covered segments are dropped - the
//     discipline is manifest FIRST (the reconciliation invariant makes the
//     deleted frames unreadable), deletion after.
//
// Wrapper-mode coupling surface: internal/writer only, not covered by API
// stability.
func (g *Graph) PollAsyncCompact() error {
	fl := g.compactFlight
	if fl == nil {
		return nil
	}
	select {
	case <-fl.done:
	default:
		return nil
	}
	return g.completeAsyncCompact(fl)
}

// WaitAsyncCompact blocks for the active flight (if any) and runs its
// completion on the writer thread. Seal/Close call it first: a checkpoint
// landing while a flight's manifest is pending would race the flight's own
// re-anchor.
//
// Wrapper-mode coupling surface: internal/writer only, not covered by API
// stability.
func (g *Graph) WaitAsyncCompact() error {
	fl := g.compactFlight
	if fl == nil {
		return nil
	}
	<-fl.done
	return g.completeAsyncCompact(fl)
}

func (g *Graph) completeAsyncCompact(fl *compactFlight) error {
	g.compactFlight = nil
	if fl.err != nil {
		return fl.err
	}
	snap := g.snapshotFile(fl.lsn)
	if g.wal.LastLSN() == fl.lsn {
		if err := wal.WriteManifest(g.walDir, wal.Manifest{
			Snapshot: filepath.Base(snap),
			LSN:      fl.lsn,
			Segment:  g.wal.Segment(),
			Format:   2,
		}); err != nil {
			return err
		}
		if err := g.wal.Reset(fl.lsn); err != nil && !errors.Is(err, wal.ErrClosed) {
			return err
		}
	} else {
		survivor := g.wal.FirstUncoveredSegment(fl.lsn)
		if err := wal.WriteManifest(g.walDir, wal.Manifest{
			Snapshot:   filepath.Base(snap),
			LSN:        fl.lsn,
			Segment:    g.wal.Segment(),
			RetainFrom: survivor,
			Format:     2,
		}); err != nil {
			return err
		}
		if err := g.wal.DropSegmentsBelow(survivor); err != nil {
			return err
		}
	}
	g.cleanupSnapshots(filepath.Base(snap))
	return nil
}

// lightCheckpoint is the O(1) tier: Rotate FIRST, manifest second. A crash in
// between leaves the old manifest plus one empty extra segment — recovery
// chain-continues into it, parks the head, and loses nothing (the rotated
// segment's marker fsyncs before Rotate returns, so the chain never breaks).
// The manifest keeps the compact boundary's Snapshot/LSN, names the NEW
// current segment, and anchors the retained chain at the first live segment
// the WAL recovered/created. Nothing else is written: no snapshot, no
// forest, no cleanup.
func (g *Graph) lightCheckpoint(man wal.Manifest) error {
	retainFrom := g.wal.FirstSegment()
	// A dead WAL handle (wal.ErrClosed from Rotate) is benign, exactly as it
	// is for Reset in the compact tier: the manifest rewrite below is still
	// correct — Segment names the same head, the chain on disk is unchanged —
	// and it is what gates recovery.
	if err := g.wal.Rotate(); err != nil && !errors.Is(err, wal.ErrClosed) {
		return err
	}
	return wal.WriteManifest(g.walDir, wal.Manifest{
		Snapshot:   man.Snapshot,    // the compact boundary — unchanged
		LSN:        man.LSN,         // the application watermark — unchanged
		Segment:    g.wal.Segment(), // the NEW current (post-rotate) segment
		RetainFrom: retainFrom,
		Format:     2,
	})
}
