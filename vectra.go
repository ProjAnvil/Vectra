// Package vectra is an embedded in-memory graph engine: all-memory
// pointer-free CSR adjacency + write-side SCC compression + level-synchronous
// parallel BFS. It targets enterprise dependency-graph scenarios where
// "the graph always fits in single-machine memory": bulk import, lock-free
// concurrent reads after Seal, disk used only for snapshot recovery.
//
// Write semantics: AddNode is idempotent, UpsertEdge keeps the (src,dst,
// label) triple unique (weight-like props update in place), AddEdge is pure
// append; deletes are recorded as tombstones and compacted at Seal/export
// time — node and edge IDs may be renumbered by compaction.
//
// Layout: this package is the public API facade (writes, options, type
// translation); sealed state lives in internal/engine, query algorithms in
// internal/traverse, encode/decode in internal/store.
package vectra

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/projanvil/vectra/internal/engine"
	"github.com/projanvil/vectra/internal/store"
	"github.com/projanvil/vectra/internal/wal"
)

// NodeID / EdgeID are integer identifiers within the graph. After Seal all
// query structures are addressed by NodeID; string keys are converted once
// at the API boundary.
type NodeID uint32
type EdgeID uint32

var (
	ErrSealed       = errors.New("vectra: graph sealed, mutation rejected")
	ErrNotSealed    = errors.New("vectra: graph not sealed, query rejected")
	ErrNodeNotFound = errors.New("vectra: node key not found")
	// ErrClosed is returned by write methods after Close. Queries remain
	// legal on a closed graph (it reads the last checkpointed state).
	ErrClosed = errors.New("vectra: graph closed, mutation rejected")
)

type Node struct {
	ID    NodeID         `json:"id"`
	Key   string         `json:"key"`
	Label string         `json:"label"`
	Props map[string]any `json:"props,omitempty"`
}

type Edge struct {
	ID    EdgeID         `json:"id"`
	Src   NodeID         `json:"src"`
	Dst   NodeID         `json:"dst"`
	Label string         `json:"label"`
	Props map[string]any `json:"props,omitempty"`
}

// Stats reports graph size and cycle-compression results. CyclicNodes is
// the node count of SCCs with size>1 (self-loops excluded, reported
// separately by Cycles()).
type Stats struct {
	Nodes       int
	Edges       int
	SCCs        int
	CyclicNodes int
}

type edgeRec struct {
	src, dst NodeID
	label    uint32
	props    map[string]any
}

// edgeKey is the logical identity of an edge for UpsertEdge/DeleteEdge:
// the (src, dst, label) triple. EdgeID cannot serve as identity because
// Seal-time compaction renumbers IDs.
type edgeKey struct {
	src, dst NodeID
	label    uint32
}

// Graph is a Vectra graph. Single writer during the write phase; immutable
// after Seal with lock-free concurrent reads. On Seal the write-state memory
// migrates into the snapshot and is released.
//
// WAL sessions (see Open): when a WAL is attached, every successful write
// method is recorded as Changeset entries; writes outside Batch flush one
// frame (one fsync in ModeAlways) per write, Batch flushes one frame per
// boundary. A torn Batch (flush error) may leave memory ahead of the log —
// the wrapper-mode writer (v0.7 task 5) closes that gap.
type Graph struct {
	nodes     []Node
	byKey     map[string]NodeID
	edges     []edgeRec
	labels    []string // labelID -> name
	labIDs    map[string]uint32
	indexKeys []string // property keys to index at Seal time
	sealed    bool
	snap      *engine.Snapshot

	edgeIdx map[edgeKey][]EdgeID // (src,dst,label) -> live edge IDs (append order)
	tombE   []bool               // parallel to edges: true = deleted (compacted at Seal)
	tombN   []bool               // parallel to nodes: true = deleted (revivable via AddNode)
	deadE   int                  // true-count in tombE (visible edges = len(edges)-deadE)
	deadN   int                  // true-count in tombN

	// WAL session state (all zero for New()/Load() graphs — v0.6 behavior).
	wal      *wal.WAL // nil = no recording
	walDir   string   // checkpoint manifest location
	dbPath   string   // session path; checkpoints write <dbPath minus ext>-<lsn>.db next to it
	pendOps  []pendOp // recorded-but-unflushed ops (inside Batch, or single writes mid-record)
	batching bool     // inside Batch: records accumulate instead of flushing
	closed   bool     // after Close: writes fail with ErrClosed, queries legal
}

// opKind orders pendOp entries exactly like the Changeset apply order:
// node upserts → edge upserts → edge deletes → node deletes.
type opKind int

const (
	opNode opKind = iota
	opEdge
	opEdgeDelete
	opNodeDelete
)

// pendOp is one recorded write, kept in original order until flush.
type pendOp struct {
	kind opKind
	node NodeUpsert
	edge EdgeUpsert
	edel EdgeDelete
	ndel string
}

func New() *Graph {
	return &Graph{byKey: map[string]NodeID{}, labIDs: map[string]uint32{}, edgeIdx: map[edgeKey][]EdgeID{}}
}

func (g *Graph) internLabel(name string) uint32 {
	if id, ok := g.labIDs[name]; ok {
		return id
	}
	id := uint32(len(g.labels))
	g.labels = append(g.labels, name)
	g.labIDs[name] = id
	return id
}

func copyProps(props map[string]any) map[string]any {
	if props == nil {
		return nil
	}
	m := make(map[string]any, len(props))
	for k, v := range props {
		m[k] = v
	}
	return m
}

// AddNode idempotently writes a node: if the key already exists it updates
// label/props and returns the original ID. Re-adding a deleted (tombstoned)
// key revives the node, reusing its ID.
//
// Props sharing contract: this method copies the passed-in map; but the maps
// exposed via Node/Edge after Seal are internal references — updates must
// replace them wholesale (AddNode/AddEdge again); in-place mutation is
// forbidden.
func (g *Graph) AddNode(key, label string, props map[string]any) (NodeID, error) {
	if g.closed {
		return 0, ErrClosed
	}
	if g.sealed {
		return 0, ErrSealed
	}
	if err := g.recordNode(key, label, props); err != nil {
		return 0, err
	}
	if id, ok := g.byKey[key]; ok {
		if g.tombN[id] { // deleted then re-added: revive, reusing the ID
			g.tombN[id] = false
			g.deadN--
		}
		g.nodes[id].Label = label
		g.nodes[id].Props = copyProps(props)
		return id, nil
	}
	id := NodeID(len(g.nodes))
	g.nodes = append(g.nodes, Node{ID: id, Key: key, Label: label, Props: copyProps(props)})
	g.tombN = append(g.tombN, false)
	g.byKey[key] = id
	return id, nil
}

// AddEdge appends a directed edge (multi-edges/self-loops both allowed).
func (g *Graph) AddEdge(srcKey, dstKey, label string, props map[string]any) (EdgeID, error) {
	if g.closed {
		return 0, ErrClosed
	}
	if g.sealed {
		return 0, ErrSealed
	}
	src, dst, lid, err := g.resolveEdge(srcKey, dstKey, label)
	if err != nil {
		return 0, err
	}
	if err := g.recordEdge(srcKey, dstKey, label, props, true); err != nil {
		return 0, err
	}
	return g.appendEdge(src, dst, lid, props), nil
}

// UpsertEdge replaces-by-key: if live edges with the same (src, dst, label)
// exist, their Props are replaced on the most recent one (edge ID kept;
// extra parallel edges deleted); otherwise a new edge is appended. Unlike
// AddEdge (pure append, multigraph semantics) this method guarantees the
// triple is unique — it is the entry point for updating weight-like props
// (relation strength, event counts).
func (g *Graph) UpsertEdge(srcKey, dstKey, label string, props map[string]any) (EdgeID, error) {
	if g.closed {
		return 0, ErrClosed
	}
	if g.sealed {
		return 0, ErrSealed
	}
	src, dst, lid, err := g.resolveEdge(srcKey, dstKey, label)
	if err != nil {
		return 0, err
	}
	if err := g.recordEdge(srcKey, dstKey, label, props, false); err != nil {
		return 0, err
	}
	k := edgeKey{src: src, dst: dst, label: lid}
	ids := g.edgeIdx[k]
	if len(ids) == 0 {
		return g.appendEdge(src, dst, lid, props), nil
	}
	for _, id := range ids[:len(ids)-1] { // drop extra parallels, keep the most recent
		g.tombE[id] = true
		g.deadE++
	}
	keep := ids[len(ids)-1]
	g.edges[keep].props = copyProps(props) // wholesale replacement (same contract as AddNode)
	g.edgeIdx[k] = []EdgeID{keep}
	return keep, nil
}

// resolveEdge looks up edge endpoints and label; deleted (tombstoned) nodes
// count as nonexistent.
func (g *Graph) resolveEdge(srcKey, dstKey, label string) (NodeID, NodeID, uint32, error) {
	src, ok := g.byKey[srcKey]
	if !ok || g.tombN[src] {
		return 0, 0, 0, ErrNodeNotFound
	}
	dst, ok := g.byKey[dstKey]
	if !ok || g.tombN[dst] {
		return 0, 0, 0, ErrNodeNotFound
	}
	return src, dst, g.internLabel(label), nil
}

// appendEdge is the shared tail of AddEdge/UpsertEdge: appends the edge
// record and registers it in the key index.
func (g *Graph) appendEdge(src, dst NodeID, lid uint32, props map[string]any) EdgeID {
	g.edges = append(g.edges, edgeRec{src: src, dst: dst, label: lid, props: copyProps(props)})
	g.tombE = append(g.tombE, false)
	id := EdgeID(len(g.edges) - 1)
	k := edgeKey{src: src, dst: dst, label: lid}
	g.edgeIdx[k] = append(g.edgeIdx[k], id)
	return id
}

// DeleteEdge removes every live parallel edge with the given (src, dst,
// label) triple (idempotent: deleting a triple that never existed succeeds
// with 0). Returns the number of edges actually deleted.
func (g *Graph) DeleteEdge(srcKey, dstKey, label string) (int, error) {
	if g.closed {
		return 0, ErrClosed
	}
	if g.sealed {
		return 0, ErrSealed
	}
	src, ok := g.byKey[srcKey]
	if !ok || g.tombN[src] {
		return 0, ErrNodeNotFound
	}
	dst, ok := g.byKey[dstKey]
	if !ok || g.tombN[dst] {
		return 0, ErrNodeNotFound
	}
	lid, ok := g.labIDs[label]
	if !ok {
		return 0, nil // label never interned: no match possible, idempotent success
	}
	if err := g.recordEdgeDelete(srcKey, dstKey, label); err != nil {
		return 0, err
	}
	k := edgeKey{src: src, dst: dst, label: lid}
	for _, id := range g.edgeIdx[k] {
		g.tombE[id] = true
	}
	n := len(g.edgeIdx[k])
	g.deadE += n
	delete(g.edgeIdx, k)
	return n, nil
}

// DeleteNode removes a node and cascade-deletes all its incident edges
// (DETACH DELETE semantics). Cost is O(live edge keys) — a scan of the key
// index. Re-adding the same key via AddNode revives the node (ID reused).
// Returns the number of cascaded edge deletions.
func (g *Graph) DeleteNode(key string) (int, error) {
	if g.closed {
		return 0, ErrClosed
	}
	if g.sealed {
		return 0, ErrSealed
	}
	id, ok := g.byKey[key]
	if !ok || g.tombN[id] {
		return 0, ErrNodeNotFound
	}
	if err := g.recordNodeDelete(key); err != nil {
		return 0, err
	}
	removed := 0
	for k, ids := range g.edgeIdx { // deleting during range is safe in Go
		if k.src != id && k.dst != id {
			continue
		}
		for _, eid := range ids {
			g.tombE[eid] = true
		}
		g.deadE += len(ids)
		removed += len(ids)
		delete(g.edgeIdx, k)
	}
	g.tombN[id] = true
	g.deadN++
	return removed, nil
}

// --- WAL op recorder ---------------------------------------------------------
//
// When a WAL is attached, each successful write method records one pendOp
// BEFORE mutating memory (log-first for single writes). Outside Batch the op
// flushes immediately as its own frame — one fsync per write in ModeAlways,
// the documented cost of unbatched direct-mode writes. Inside Batch the ops
// accumulate and the boundary flushes them as frame(s).
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
// persist stay buffered in pendOps, so the next flush (Batch boundary, or
// Close) re-attempts exactly those; continuing to write without a
// successful flush risks losing the earlier ops.

func (g *Graph) recordNode(key, label string, props map[string]any) error {
	if g.wal == nil {
		return nil
	}
	g.pendOps = append(g.pendOps, pendOp{kind: opNode, node: NodeUpsert{Key: key, Label: label, Props: copyProps(props)}})
	return g.maybeFlush()
}

func (g *Graph) recordEdge(srcKey, dstKey, label string, props map[string]any, allowParallel bool) error {
	if g.wal == nil {
		return nil
	}
	g.pendOps = append(g.pendOps, pendOp{kind: opEdge, edge: EdgeUpsert{
		SrcKey: srcKey, DstKey: dstKey, Label: label,
		Props: copyProps(props), AllowParallel: allowParallel,
	}})
	return g.maybeFlush()
}

func (g *Graph) recordEdgeDelete(srcKey, dstKey, label string) error {
	if g.wal == nil {
		return nil
	}
	g.pendOps = append(g.pendOps, pendOp{kind: opEdgeDelete, edel: EdgeDelete{SrcKey: srcKey, DstKey: dstKey, Label: label}})
	return g.maybeFlush()
}

func (g *Graph) recordNodeDelete(key string) error {
	if g.wal == nil {
		return nil
	}
	g.pendOps = append(g.pendOps, pendOp{kind: opNodeDelete, ndel: key})
	return g.maybeFlush()
}

// maybeFlush flushes unless a Batch boundary is holding the buffer.
func (g *Graph) maybeFlush() error {
	if g.batching {
		return nil
	}
	return g.flushPending()
}

// flushPending emits the buffered ops as one frame per kind-ordered run. On
// error the unflushed ops stay buffered so a retry (e.g. a retried Close)
// does not silently drop them.
func (g *Graph) flushPending() error {
	if g.wal == nil {
		g.pendOps = nil
		return nil
	}
	i := 0
	for i < len(g.pendOps) {
		j := i + 1
		for j < len(g.pendOps) && g.pendOps[j].kind >= g.pendOps[j-1].kind {
			j++
		}
		cs := Changeset{Version: changesetVersion}
		for _, op := range g.pendOps[i:j] {
			switch op.kind {
			case opNode:
				cs.Nodes = append(cs.Nodes, op.node)
			case opEdge:
				cs.Edges = append(cs.Edges, op.edge)
			case opEdgeDelete:
				cs.EdgeDeletes = append(cs.EdgeDeletes, op.edel)
			case opNodeDelete:
				cs.NodeDeletes = append(cs.NodeDeletes, op.ndel)
			}
		}
		if _, err := g.wal.Append(cs); err != nil {
			g.pendOps = append([]pendOp(nil), g.pendOps[i:]...)
			return err
		}
		i = j
	}
	g.pendOps = nil
	return nil
}

// applyChangeset replays one frame with the write-method semantics. Used by
// Open's recovery path (recording is detached there — replaying must not
// re-record). Any error means the frame references state the replay did not
// rebuild identically: surfaced, not skipped.
func (g *Graph) applyChangeset(cs Changeset) error {
	for _, n := range cs.Nodes {
		if _, err := g.AddNode(n.Key, n.Label, n.Props); err != nil {
			return err
		}
	}
	for _, e := range cs.Edges {
		var err error
		if e.AllowParallel {
			_, err = g.AddEdge(e.SrcKey, e.DstKey, e.Label, e.Props)
		} else {
			_, err = g.UpsertEdge(e.SrcKey, e.DstKey, e.Label, e.Props)
		}
		if err != nil {
			return err
		}
	}
	for _, d := range cs.EdgeDeletes {
		if _, err := g.DeleteEdge(d.SrcKey, d.DstKey, d.Label); err != nil {
			return err
		}
	}
	for _, key := range cs.NodeDeletes {
		if _, err := g.DeleteNode(key); err != nil {
			return err
		}
	}
	return nil
}

// Batch groups writes into one WAL boundary: fn's write methods record
// Changeset entries without flushing, and the boundary flushes them as one
// frame (one fsync in ModeAlways — group commit for direct-mode writers).
// A normal batch is exactly one frame; batches whose op order regresses
// across Changeset kinds are split at each regression so replay preserves
// the original op order (see the op-recorder comment).
//
// Without a WAL attached, Batch is a plain grouping with no extra semantics.
// Nested Batch calls are absorbed into the outer boundary. WALOff callers
// pay nothing; Batch outside a WAL session never errors on grouping alone.
//
// Known direct-mode limitation (closed by the v0.7 wrapper-mode writer): a
// flush error at the boundary returns after fn's mutations already hit
// memory — memory may be ahead of the log until the next successful flush.
func (g *Graph) Batch(fn func(*Graph)) error {
	if g.closed {
		return ErrClosed
	}
	if g.sealed {
		return ErrSealed
	}
	if g.wal == nil || g.batching {
		fn(g)
		return nil
	}
	g.batching = true
	defer func() { g.batching = false }() // even if fn panics, never wedge the recorder
	fn(g)
	return g.flushPending()
}

// Open opens (or creates) a durable graph session at path. Unlike
// LoadFile — which auto-Seals a snapshot — an Open session recovers into a
// writable unsealed graph: snapshot first (when a checkpoint exists), then
// WAL frames replayed in LSN order above the checkpoint boundary, with no
// recording during replay. Sealing is deferred to the checkpoint at Close.
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
// Close checkpoints (Seal → SaveFile → manifest → WAL reset); a checkpoint
// failure returns an error and keeps the WAL for the next recovery attempt.
func Open(path string, opts Options) (*Graph, error) {
	if opts.WALMode == ModeOff {
		// v0.6 behavior: snapshot-only. Existing file loads sealed; a missing
		// file starts a fresh writable graph (durability via Seal+SaveFile).
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

	g := New()
	loadSnapshot := func(p string) error {
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		defer f.Close()
		lg, err := loadGraph(f)
		if err != nil {
			return err
		}
		*g = *lg // loadGraph returns a fresh graph; move its state in (no WAL state yet)
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

	w, err := wal.Open(walDir, opts.WALMode)
	if err != nil {
		return nil, err
	}
	// Replay with recording detached (g.wal still nil): replaying must not
	// amplify frames. Torn tails stop at the last complete frame.
	if _, err := w.Replay(g.applyChangeset); err != nil {
		w.Close()
		return nil, err
	}
	g.wal, g.walDir, g.dbPath = w, walDir, path
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

// Close ends a session. With a WAL attached, Close is a mandatory checkpoint:
// flush pending frames + fsync → Seal → SaveFile(<dbPath minus ext>-<lsn>.db)
// → manifest (atomically pointing at that watermark) → WAL reset → delete
// older watermark snapshots.
//
// Failure semantics:
//   - before the manifest lands, any failure returns the error WITHOUT
//     deleting the WAL — the next Open recovers from it, and Close can be
//     retried (Seal and SaveFile are safe on an already-sealed graph);
//   - once the manifest has landed the checkpoint is committed: later
//     failures (segment rollover, fd close) are surfaced but the session is
//     already closed, so a retry is a clean no-op — it can never wedge;
//   - a WAL handle that died mid-checkpoint (wal.ErrClosed from Sync/Reset/
//     Close) is treated as benign: the manifest gates the un-reset segments.
//
// Append/fsync failure discipline (see the op-recorder comment): if a frame
// Append ever returned an error, the caller must retry the flush (Batch/
// Close) or abandon the graph — buffered ops stay in pendOps and are
// re-attempted by the next flush, because a frame whose fsync reported
// failure may still be on disk.
//
// WALOff sessions: Close only flips to closed (writes then return ErrClosed;
// durability was the caller's job, v0.6 style). In both modes queries stay
// legal after Close.
func (g *Graph) Close() error {
	if g.closed {
		return nil
	}
	if g.wal == nil {
		g.closed = true
		return nil
	}
	if err := g.flushPending(); err != nil {
		return err
	}
	if err := g.wal.Sync(); err != nil && !errors.Is(err, wal.ErrClosed) {
		return err
	}
	lsn := g.wal.LastLSN()
	if err := g.Seal(); err != nil {
		return err
	}
	snap := g.snapshotFile(lsn)
	if err := g.SaveFile(snap); err != nil {
		return err
	}
	if err := wal.WriteManifest(g.walDir, wal.Manifest{
		Snapshot: filepath.Base(snap),
		Segment:  g.wal.Segment(),
		LSN:      lsn,
	}); err != nil {
		return err
	}
	// The manifest has landed: this checkpoint is committed. From here on a
	// failure is surfaced but must never leave the session retryable-but-
	// unwritable — mark closed so a retry is a clean no-op.
	g.closed = true
	if err := g.wal.Reset(lsn); err != nil && !errors.Is(err, wal.ErrClosed) {
		g.wal.Close()
		g.wal = nil
		return err
	}
	if err := g.wal.Close(); err != nil && !errors.Is(err, wal.ErrClosed) {
		g.wal = nil
		return err
	}
	g.wal = nil
	g.cleanupSnapshots(filepath.Base(snap))
	return nil
}

func (g *Graph) NodeByKey(key string) (Node, bool) {
	id, ok := g.byKey[key]
	if !ok {
		return Node{}, false
	}
	return g.NodeByID(id)
}

func (g *Graph) NodeByID(id NodeID) (Node, bool) {
	if g.sealed {
		rec, ok := g.snap.NodeInfo(uint32(id))
		if !ok {
			return Node{}, false
		}
		return Node{ID: id, Key: rec.Key, Label: rec.Label, Props: rec.Props}, true
	}
	if int(id) >= len(g.nodes) || g.tombN[id] {
		return Node{}, false
	}
	return g.nodes[id], true
}

func (g *Graph) EdgeByID(id EdgeID) (Edge, bool) {
	if !g.sealed {
		if int(id) >= len(g.edges) || g.tombE[id] {
			return Edge{}, false
		}
		e := g.edges[id]
		return Edge{ID: id, Src: e.src, Dst: e.dst, Label: g.labels[e.label], Props: e.props}, true
	}
	src, dst, label, props, ok := g.snap.EdgeInfo(uint32(id))
	if !ok {
		return Edge{}, false
	}
	return Edge{ID: id, Src: NodeID(src), Dst: NodeID(dst), Label: label, Props: props}, true
}

// NodeCount/EdgeCount report the visible size in both states: before Seal
// tombstoned records are subtracted; after Seal the write state has already
// migrated into the snapshot and been released, reads come from the snapshot.
func (g *Graph) NodeCount() int {
	if g.sealed {
		return g.snap.NodeCount()
	}
	return len(g.nodes) - g.deadN
}

func (g *Graph) EdgeCount() int {
	if g.sealed {
		return g.snap.EdgeCount()
	}
	return len(g.edges) - g.deadE
}

func (g *Graph) Sealed() bool { return g.sealed }

func (g *Graph) Stats() Stats {
	if g.sealed {
		st := Stats{
			Nodes: g.snap.NodeCount(),
			Edges: g.snap.EdgeCount(),
			SCCs:  int(g.snap.SCCCount()),
		}
		for c := uint32(0); c < g.snap.SCCCount(); c++ {
			if size := len(g.snap.SCCMembers(c)); size > 1 {
				st.CyclicNodes += size
			}
		}
		return st
	}
	st := Stats{Nodes: g.NodeCount(), Edges: g.EdgeCount()}
	return st
}

// compact returns the tombstone-free raw records: tombstoned nodes/edges are
// skipped, edges whose endpoints were dropped are discarded too (defensive
// for import paths), and node/edge IDs are renumbered densely. remap maps
// old node ID -> new node ID (^NodeID(0) for dropped nodes). Shared by Seal
// and ExportSnapshot.
func (g *Graph) compact() (nodes []engine.NodeRec, edges []engine.EdgeRec, remap []NodeID) {
	remap = make([]NodeID, len(g.nodes))
	nodes = make([]engine.NodeRec, 0, len(g.nodes)-g.deadN)
	for i, n := range g.nodes {
		if g.tombN[i] {
			remap[i] = ^NodeID(0)
			continue
		}
		remap[i] = NodeID(len(nodes))
		nodes = append(nodes, engine.NodeRec{Key: n.Key, Label: n.Label, Props: n.Props})
	}
	edges = make([]engine.EdgeRec, 0, len(g.edges)-g.deadE)
	for i, e := range g.edges {
		if g.tombE[i] || remap[e.src] == ^NodeID(0) || remap[e.dst] == ^NodeID(0) {
			continue
		}
		edges = append(edges, engine.EdgeRec{
			Src: uint32(remap[e.src]), Dst: uint32(remap[e.dst]),
			LabelID: e.label, Props: e.props,
		})
	}
	return nodes, edges, remap
}

// Seal compacts tombstones first (node and edge IDs may be renumbered), then
// builds every immutable structure queries need (forward/reverse CSR, Tarjan
// SCC, forward/reverse condensation DAG, member tables, self-loop marks,
// property indexes); afterwards the graph is read-only with lock-free
// concurrent reads.
func (g *Graph) Seal() error {
	if g.sealed {
		return nil
	}
	nodes, edges, remap := g.compact()
	snap, err := engine.Seal(nodes, edges, g.labels, g.indexKeys)
	if err != nil {
		return err
	}
	for i, n := range g.nodes { // byKey re-points at compacted IDs
		if remap[i] != ^NodeID(0) {
			g.byKey[n.Key] = remap[i]
		} else {
			delete(g.byKey, n.Key)
		}
	}
	g.snap = snap
	g.sealed = true
	g.nodes, g.edges = nil, nil // write state migrated into snapshot, release
	g.edgeIdx, g.tombE, g.tombN = nil, nil, nil
	return nil
}

// Save writes the whole graph to w. Requires Seal first (guarantees a
// consistent exported state). Property-index declarations are persisted too
// (store v3), so Load restores both the graph and its lookup indexes.
func (g *Graph) Save(w io.Writer) error {
	if !g.sealed {
		return ErrNotSealed
	}
	snapNodes := g.snap.ExportNodes()
	snapEdges := g.snap.ExportEdges()
	nodes := make([]store.Node, len(snapNodes))
	for i, n := range snapNodes {
		nodes[i] = store.Node{Key: n.Key, Label: n.Label, Props: n.Props}
	}
	edges := make([]store.Edge, 0, len(snapEdges))
	for _, e := range snapEdges {
		src, _ := g.snap.NodeInfo(e.Src)
		dst, _ := g.snap.NodeInfo(e.Dst)
		edges = append(edges, store.Edge{
			SrcKey: src.Key,
			DstKey: dst.Key,
			Label:  g.snap.LabelName(e.LabelID),
			Props:  e.Props,
		})
	}
	return store.Encode(w, store.Data{Nodes: nodes, Edges: edges, IndexKeys: g.snap.IndexedKeys()})
}

// SaveFile saves to a path: temp file + fsync + atomic rename, avoiding
// half-written files.
func (g *Graph) SaveFile(path string) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if err := g.Save(f); err != nil {
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
	return os.Rename(tmp, path)
}

// Load restores a graph from a snapshot and Seals it automatically. Index
// declarations persisted by the snapshot (store v3) are re-registered before
// Seal, so LookupByProperty works immediately (v1/v2 files carry none).
func Load(r io.Reader) (*Graph, error) {
	g, err := loadGraph(r)
	if err != nil {
		return nil, err
	}
	if err := g.Seal(); err != nil {
		return nil, err
	}
	return g, nil
}

func loadGraph(r io.Reader) (*Graph, error) {
	data, err := store.Decode(r)
	if err != nil {
		return nil, err
	}
	g := New()
	for _, k := range data.IndexKeys {
		g.IndexProperty(k)
	}
	for _, n := range data.Nodes {
		if _, err := g.AddNode(n.Key, n.Label, n.Props); err != nil {
			return nil, err
		}
	}
	for _, e := range data.Edges {
		if _, err := g.AddEdge(e.SrcKey, e.DstKey, e.Label, e.Props); err != nil {
			return nil, err
		}
	}
	return g, nil
}

// LoadFile restores a graph from a path.
func LoadFile(path string) (*Graph, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Load(f)
}

// IndexProperty registers a string property key to index at Seal time
// (idempotent; a no-op after Seal). Unregistered keys yield ok=false in
// LookupByProperty.
func (g *Graph) IndexProperty(key string) *Graph {
	if g.sealed {
		return g
	}
	for _, k := range g.indexKeys {
		if k == key {
			return g
		}
	}
	g.indexKeys = append(g.indexKeys, key)
	return g
}

// IndexedPropertyKeys returns the property-index declarations: the live list
// before Seal, the sealed snapshot's built index keys afterwards (nil when
// none). Consumers that persist or hand over a graph (snapshot services,
// clone-and-extend flows) use it to preserve index configuration.
func (g *Graph) IndexedPropertyKeys() []string {
	if g.sealed {
		return g.snap.IndexedKeys()
	}
	return slices.Clone(g.indexKeys)
}

// Clone deep-copies into a graph that can keep taking writes (the result is
// always unsealed). Props maps share references per the sharing contract
// (relies on "replace, don't mutate" discipline). The server module's
// hot-swap publisher uses it to clone the live graph.
func (g *Graph) Clone() *Graph {
	c := New()
	if g.sealed {
		for _, n := range g.snap.ExportNodes() {
			_, _ = c.AddNode(n.Key, n.Label, n.Props)
		}
		for _, e := range g.snap.ExportEdges() {
			src, _ := g.snap.NodeInfo(e.Src)
			dst, _ := g.snap.NodeInfo(e.Dst)
			_, _ = c.AddEdge(src.Key, dst.Key, g.snap.LabelName(e.LabelID), e.Props)
		}
		c.indexKeys = slices.Clone(g.indexKeys) // index declarations carry over to the clone
		return c
	}
	for _, n := range g.nodes {
		c.nodes = append(c.nodes, Node{ID: NodeID(len(c.nodes)), Key: n.Key, Label: n.Label, Props: n.Props})
		c.byKey[n.Key] = n.ID
	}
	c.edges = slices.Clone(g.edges)
	c.labels = slices.Clone(g.labels)
	c.labIDs = maps.Clone(g.labIDs)
	c.indexKeys = slices.Clone(g.indexKeys)
	c.tombE = slices.Clone(g.tombE)
	c.tombN = slices.Clone(g.tombN)
	c.deadE, c.deadN = g.deadE, g.deadN
	c.edgeIdx = make(map[edgeKey][]EdgeID, len(g.edgeIdx))
	for k, ids := range g.edgeIdx {
		c.edgeIdx[k] = slices.Clone(ids)
	}
	return c
}

// LookupByProperty finds nodes by string property value on the Seal-time
// index (ascending IDs). An unregistered key returns ok=false; a registered
// one returns the hit set (possibly empty).
func (g *Graph) LookupByProperty(key, value string) ([]Node, bool) {
	if !g.sealed {
		return nil, false
	}
	ids, ok := g.snap.LookupIndex(key, value)
	return g.nodesFromIDs(ids, ok)
}

// LookupByNumber finds nodes whose numeric property equals v. Numbers of any
// Go/JSON width normalize to float64 in the index, so int-written and
// round-tripped values match alike.
func (g *Graph) LookupByNumber(key string, v float64) ([]Node, bool) {
	if !g.sealed {
		return nil, false
	}
	ids, ok := g.snap.LookupNumber(key, v)
	return g.nodesFromIDs(ids, ok)
}

// LookupByNumberRange finds nodes whose numeric property lies in [min, max]
// (inclusive; nil leaves that side unbounded). int64 values beyond 2^53 lose
// precision (inherent to the JSON number model).
func (g *Graph) LookupByNumberRange(key string, min, max *float64) ([]Node, bool) {
	if !g.sealed {
		return nil, false
	}
	ids, ok := g.snap.LookupNumberRange(key, min, max)
	return g.nodesFromIDs(ids, ok)
}

// LookupByBool finds nodes whose boolean property equals v.
func (g *Graph) LookupByBool(key string, v bool) ([]Node, bool) {
	if !g.sealed {
		return nil, false
	}
	ids, ok := g.snap.LookupBool(key, v)
	return g.nodesFromIDs(ids, ok)
}

// nodesFromIDs materializes index hits as nodes; ok passes the
// registered/unregistered distinction through.
func (g *Graph) nodesFromIDs(ids []uint32, ok bool) ([]Node, bool) {
	if !ok {
		return nil, false
	}
	out := make([]Node, 0, len(ids))
	for _, id := range ids {
		n, _ := g.NodeByID(NodeID(id))
		out = append(out, n)
	}
	return out, true
}

// ExportSnapshot serializes the current write state or sealed state (export
// works before Seal too — the server module's FSM snapshot uses it on the
// live graph). The format matches Save (CRC included).
func (g *Graph) ExportSnapshot() ([]byte, error) {
	if g.sealed {
		var buf bytes.Buffer
		if err := g.Save(&buf); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}
	nodesC, edgesC, _ := g.compact()
	nodes := make([]store.Node, len(nodesC))
	for i, n := range nodesC {
		nodes[i] = store.Node{Key: n.Key, Label: n.Label, Props: n.Props}
	}
	edges := make([]store.Edge, 0, len(edgesC))
	for _, e := range edgesC {
		edges = append(edges, store.Edge{
			SrcKey: nodesC[e.Src].Key,
			DstKey: nodesC[e.Dst].Key,
			Label:  g.labels[e.LabelID],
			Props:  e.Props,
		})
	}
	var buf bytes.Buffer
	if err := store.Encode(&buf, store.Data{Nodes: nodes, Edges: edges, IndexKeys: g.indexKeys}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ImportSnapshot builds an unsealed graph from snapshot bytes (writable
// again). The caller decides when to Seal — the server FSM's Restore path
// uses it to rebuild the live graph.
func ImportSnapshot(data []byte) (*Graph, error) {
	g, err := loadGraph(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	return g, nil
}
