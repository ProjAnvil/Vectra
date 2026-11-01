// Package vectra is an embedded in-memory graph engine: all-memory
// pointer-free CSR adjacency + write-side SCC compression + level-synchronous
// parallel BFS. It targets enterprise dependency-graph scenarios where
// "the graph always fits in single-machine memory": bulk import, lock-free
// concurrent reads, disk used only for snapshot recovery / WAL replay.
//
// v0.7 write model (spec §2.3, micro-layer generations): every write batch
// (one Batch(fn) boundary, or the direct-mode absorption window — a run of
// unbatched per-op writes closed by the next read, boundary, or capacity
// cap) is materialized into an immutable micro-layer and published as a new
// generation {base, layers[]} through an atomic.Pointer — readers never
// lock. Queries are always legal and every family is immediately
// consistent:
//
//   - Point-query family (NodeByKey/NodeByID/EdgeByID/NodeCount/EdgeCount/
//     EdgesOf/LookupBy*) reads the three sources base ∪ layers with
//     tombstone shadowing — write-then-read is immediate.
//   - Traversal family (BFS/Reachable/ShortestPath/Cypher/NeighborsOf)
//     iterates rows over the same union through the generation View
//     (internal/engine): with nothing unmerged it IS the base CSR (the
//     slice-zip hot path runs untouched); Reachable's SCC-supernode
//     acceleration exists only at merge boundaries, so unmerged delta falls
//     back to node-level BFS. Cycles is the exception and merges pending
//     layers first (SCC structures only exist on a merged base).
//   - Seal() is an explicit merge (+ WAL checkpoint when a WAL is attached):
//     layers fold into a fresh immutable base. Sealed() reports "no unmerged
//     layers"; the v0.6 sealed/unsealed gates are dissolved (spec §6) —
//     writes after Seal simply open a new generation.
//
// Write semantics: AddNode is idempotent, UpsertEdge keeps the (src,dst,
// label) triple unique (weight-like props update in place, the visible
// edge keeps its EdgeID), AddEdge is pure append; deletes are tombstones
// shadowing everything older than the deleting layer and compact at merge
// time — node and edge IDs may be renumbered by a merge (spec §3.1 R1).
// Within a generation IDs come from a ratchet and are never recycled.
//
// Layout: this package is the public API facade (writes, options, type
// translation); the merged base lives in internal/engine, micro-layers in
// internal/delta, query algorithms in internal/traverse, encode/decode in
// internal/store.
//
// Wrapper-mode coupling surface — internal/writer only, not covered by API
// stability: wrapper mode (internal/writer, the single-writer wrapper that
// runs the background merger, async compaction, the vector folder lane and
// the writer discipline) drives the engine through a fixed set of exported
// symbols. Wrapper-only — direct-mode callers never touch them:
// CaptureMerge, PublishMerge, the MergeCapture/MergeResult pair,
// RepairAfterRebase, SealForClose, TryAsyncCompact, PollAsyncCompact,
// WaitAsyncCompact, AsyncCompactActive, BaseEdgeCount, VecHandle,
// VectorHandles, VectorOverlayPressure, ClaimWriter, YieldWriter. Shared
// with direct mode — wrapper-coupled but also documented direct-mode
// utilities or plain observability: FlushPend, LayerPressure,
// VectorOverlayLen. All of them are engine coupling rather than supported
// API: they are excluded from API stability and may change in any release
// to serve the wrapper.
package vectra

import (
	"cmp"
	"errors"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/projanvil/vectra/internal/delta"
	"github.com/projanvil/vectra/internal/engine"
	"github.com/projanvil/vectra/internal/propval"
	"github.com/projanvil/vectra/internal/wal"
)

// NodeID / EdgeID are integer identifiers within the graph. After a merge all
// query structures are addressed by NodeID; string keys are converted once
// at the API boundary. A merge may renumber IDs (spec §3.1 R1); between
// merges new records draw IDs from a ratchet and deleted records' IDs are
// never reused.
type NodeID uint32
type EdgeID uint32

var (
	// ErrSealed is a v0.6 sentinel: v0.7 dissolved the sealed gates and no
	// code path has returned it since. Kept for source compatibility only.
	//
	// Deprecated: nothing returns ErrSealed; it will be removed in a future
	// minor release.
	ErrSealed = errors.New("vectra: graph sealed, mutation rejected")
	// ErrNotSealed is ErrSealed's query-side twin, equally unreturned since
	// v0.7 dissolved the sealed gates. Kept for source compatibility only.
	//
	// Deprecated: nothing returns ErrNotSealed; it will be removed in a
	// future minor release.
	ErrNotSealed    = errors.New("vectra: graph not sealed, query rejected")
	ErrNodeNotFound = errors.New("vectra: node key not found")
	// ErrClosed is returned by write methods after Close. Queries remain
	// legal on a closed graph (it reads the last checkpointed state).
	ErrClosed = errors.New("vectra: graph closed, mutation rejected")

	// ErrNotWriter enforces wrapper-mode single-writer discipline (spec
	// §2.3 R7): after internal/writer.Attach claims the graph, write
	// methods (AddNode/AddEdge/UpsertEdge/DeleteEdge/DeleteNode/Batch)
	// called from any goroutine other than the attached writer are
	// rejected. Detection is goroutine-token based and only compiled into
	// non-release builds (build tag "release" compiles it away — release
	// builds rely on documentation instead; concurrent misuse there is a
	// data race regardless). IndexProperty is writer-private state too:
	// keep it on the writer goroutine as well (its signature predates this
	// check and cannot carry an error).
	ErrNotWriter = errors.New("vectra: mutation rejected: not the attached writer")

	// ErrUniqueViolation is the errors.Is target for write-time unique
	// constraint rejections: the engine returns *UniqueViolationError
	// values (carrying the label/prop/value and both node keys) that
	// unwrap to it — match with errors.Is for the class, errors.As for
	// the payload (unique-constraints spec §2.3).
	ErrUniqueViolation = errors.New("vectra: unique constraint violation")

	// ErrConstraintExists / ErrConstraintNotFound guard the declaration
	// surface: declaring the same (label, prop) pair twice, or dropping
	// one that was never declared, fails before any side effect
	// (unique-constraints spec §2.1).
	ErrConstraintExists   = errors.New("vectra: unique constraint already declared")
	ErrConstraintNotFound = errors.New("vectra: unique constraint not declared")

	// ErrInBatch is the errors.Is target for operations rejected because a
	// Batch boundary is open: declarations (DeclareUnique/DropUnique,
	// CreateVectorIndex) must run between changesets — a mid-batch
	// declaration would scan a pre-publish view and miss the batch's own
	// writes (replay applies declaration frames BETWEEN changesets, never
	// inside one). Raise sites wrap it with the operation name.
	ErrInBatch = errors.New("vectra: operation is not supported inside Batch")
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
// separately by Cycles()). SCC fields exist only on the merged base — with
// unmerged layers present Stats reports counts only.
type Stats struct {
	Nodes       int
	Edges       int
	SCCs        int
	CyclicNodes int
}

// genView is one immutable generation of the graph: a merged base plus the
// micro-layers stacked on top of it (newest last). Published through
// Graph.view (atomic.Pointer); after Store nothing reachable from a genView
// is ever mutated, so readers may walk it without synchronization.
//
// Composition contract (internal/delta package doc): a layer's tombstones
// shadow everything OLDER — the base and earlier layers; a layer's live
// records win over its own tombstones; a record of layer L is visible iff no
// layer newer than L tombstones its key; a base record is visible iff no
// layer tombstones its key.
type genView struct {
	base     *engine.Snapshot
	baseKeys map[string]NodeID // base node key -> base node id (rebuilt at merge/load)
	layers   []*delta.Layer    // immutable micro-layers, oldest first
	lsn      uint64            // WAL LSN covered by the newest layer (informational)
	nodes    int               // visible node count (three-source)
	edges    int               // visible edge count (three-source)

	// cached is this generation's three-source View slot (see engineViewOf
	// and prebuildView). Per-generation rather than per-graph: the writer
	// fills it BEFORE publishing the generation, so readers of a fresh
	// generation always hit, and a reader's own build can never clobber a
	// newer generation's writer-maintained entry (the single-slot cache had
	// both races — a miss storm of full rebuilds on every publish was the
	// 2026-10-17 writer-throttle finding).
	cached atomic.Pointer[viewCacheEntry]

	// splitF/splitT partition this generation's stack for the fold
	// governor: [0,splitF) = the old folded run F, [splitF,splitT) = the
	// newer folded run T, [splitT,:) = the raw tail. Immutable with the
	// generation (the writer stamps them at publish from the fold result it
	// incorporated), so the background folder reads them race-free.
	splitF, splitT int
}

// nodeWinner resolves key to its visible node (three-source).
func (v *genView) nodeWinner(key string) (Node, bool) {
	for i := len(v.layers) - 1; i >= 0; i-- {
		l := v.layers[i]
		if rec, ok := l.LookupNode(key); ok {
			id, _ := l.LookupNodeID(key)
			return Node{ID: NodeID(id), Key: key, Label: rec.Label, Props: rec.Props}, true
		}
		if l.NodeTombstones()[key] {
			return Node{}, false
		}
	}
	if id, ok := v.baseKeys[key]; ok {
		if rec, ok2 := v.base.NodeInfo(uint32(id)); ok2 {
			return Node{ID: id, Key: key, Label: rec.Label, Props: rec.Props}, true
		}
	}
	return Node{}, false
}

// nodeIDOf resolves key to its winner id for the endpoint fields of layer
// edge records (EdgesOf, edgeByID).
//
// Invariant: a live layer edge never references a dead endpoint — AddEdge/
// UpsertEdge validate endpoints via checkLiveNodes, and DeleteNode cascade-
// tombstones every incident edge — so callers only reach the fallback
// through a broken invariant upstream, never through user input. The
// fallback returns 0 rather than panicking because the signatures cannot
// error and queries must stay total; a 0 here means "consistency bug to
// investigate", not a legitimate miss.
func (v *genView) nodeIDOf(key string) NodeID {
	if n, ok := v.nodeWinner(key); ok {
		return n.ID
	}
	return 0
}

// baseKeyOf maps a base node id to its key ("" when out of range).
func (v *genView) baseKeyOf(id uint32) string {
	rec, ok := v.base.NodeInfo(id)
	if !ok {
		return ""
	}
	return rec.Key
}

// baseEdgeKeyOf derives the logical key of a base edge id.
func (v *genView) baseEdgeKeyOf(eid uint32) delta.EdgeKey {
	src, dst, label, _, _ := v.base.EdgeInfo(eid)
	return delta.EdgeKey{SrcKey: v.baseKeyOf(src), DstKey: v.baseKeyOf(dst), Label: label}
}

// baseEdgeLive reports whether the base edge with this key is still visible
// (no layer tombstones it).
func (v *genView) baseEdgeLive(k delta.EdgeKey) bool {
	for _, l := range v.layers {
		if l.EdgeTombstones()[k] {
			return false
		}
	}
	return true
}

// layerEdgeLive reports whether a record of layer i with this key is still
// visible (no strictly newer layer tombstones it).
func (v *genView) layerEdgeLive(i int, k delta.EdgeKey) bool {
	for j := i + 1; j < len(v.layers); j++ {
		if v.layers[j].EdgeTombstones()[k] {
			return false
		}
	}
	return true
}

// nodeRef is the writer-side mirror of one node key: its stable id (kept
// across deletes for revive-reuse, v0.6 semantics), visibility, and the
// current winner label/props (O(1) endpoint materialization without walking
// the layer stack).
type nodeRef struct {
	id    NodeID
	live  bool
	label string
	props map[string]any
}

// edgeRef is the writer-side mirror of one edge key: the winner EdgeID
// (transferred by folds, v0.6 "keeps the most recent edge's ID") and the
// number of visible parallels.
type edgeRef struct {
	winner EdgeID
	n      int
}

// Graph is a Vectra graph. Single writer (spec D5: the caller's goroutine in
// direct mode); lock-free concurrent reads through the generation pointer.
// Writer-private mirrors (nodes/edges/labels/batch state) are only touched
// by the writer and are never read by query paths.
//
// Direct-mode absorption window: unbatched per-op writes accumulate in one
// open builder and materialize as ONE micro-layer per flush — flushed by
// the next read API entry from the writing goroutine (write-then-read is
// immediate), a Batch/Seal/Compact/Close/Cycles boundary, or the capacity
// caps (4096 ops / 64K edge records). Concurrent reader goroutines that are
// not the writer see the last published generation, exactly as mid-Batch
// readers always have. The wrapper (internal/writer) never opens a window:
// its writer batches, and its readers skip flushing.
//
// WAL sessions (see Open): every successful write method is recorded as
// Changeset entries; writes outside Batch flush one frame (one fsync in
// ModeAlways) per write — the durability contract is per-op regardless of
// layer absorption — while the layer side absorbs (N frames may share one
// materialized layer; the layer's lsn is the greatest frame lsn recorded at
// flush time, so a checkpoint's snapshot always contains every frame ≤ its
// watermark lsn). Batch flushes one frame per boundary (kind-regressing
// batches split) and materializes one layer per boundary.
type Graph struct {
	view atomic.Pointer[genView]

	// vector indexes (Phase 1): copy-on-write registry behind an atomic
	// pointer - lock-free reads, writer-thread mutations. Each entry owns
	// its published snapshot head (spec D1; see vectra_vector.go).
	vecIdx     atomic.Pointer[[]*vecIndex]
	onVecSkip  func(key, index string) // Options.OnVecSkip; nil = silent skip
	vecReaders sync.Pool               // *vector.Reader scratch for QueryVector

	// .vec forest-cache state (Phase 2, spec §2.2-2.3), writer-thread
	// only, touched exclusively inside Open/loadGraph. vecSkipBackfill
	// suppresses backfillVectorIndex while the load gate is deciding between
	// mounting the persisted forest and the rebuild path; vecForestLoaded
	// records which path won (surfaced through export_test.go).
	vecSkipBackfill bool
	vecForestLoaded bool

	// writer-private published-state mirrors (reconciled at publish)
	nodes     map[string]nodeRef        // key -> id + visibility
	edges     map[delta.EdgeKey]edgeRef // key -> winner id + visible parallels
	nodeTotal int
	edgeTotal int
	nextNode  NodeID // ID ratchets: never reused within a generation
	nextEdge  EdgeID
	labels    map[string]bool // label strings known to have been used

	// active batch (a single write is a batch of one). bNodeIDs/bLiveNodes
	// double as the carried endpoint registry: mid-burst window cuts keep
	// them warm so published endpoints stay one lookup (the re-lift was the
	// absorption window's residual per-op cost); full closes (Batch
	// boundary, Seal/forceMerge, Close) reset them.
	builder    *delta.Builder
	bNodeIDs   map[string]uint32     // key -> assigned id (written or lifted)
	bEdgeIDs   []uint32              // one id per edge slot appended (this batch)
	bLiveNodes map[string]bool       // keys live in the builder or published-and-lifted
	bKilled    map[delta.EdgeKey]int // per key: published parallels already tombstoned in this window (>0 also forces fresh IDs on revive)
	bIndex     []string              // pending index declarations (this batch)
	bFTS       []string              // pending full-text index declarations (this batch, 2026-10-26 FTS plan)

	// unique-constraint registry (unique-constraints spec 2026-10-30 §3.1),
	// writer-private like the mirrors and persistent across publishes —
	// folds/Seal/CaptureMerge renumber IDs, never keys/labels/props, so the
	// registry is fold-immune and needs no per-publish reset. nil/empty = no
	// constraints: every write path's fast path is a single len check, zero
	// cost and zero allocations on unconstrained graphs. uniqueStale marks
	// the registry suspect (Batch panic recovery, rebase repair — flagged in
	// a later task) and the next registry touch rebuilds every map from live
	// state (uniqueEnsureFresh).
	uniqueIdx   map[UniqueConstraint]map[string]string // constraint -> value -> owning key
	uniqueStale bool

	// WAL session state (all zero for New()/Load() graphs — v0.6 behavior).
	wal    *wal.WAL // nil = no recording
	walDir string   // checkpoint manifest location
	// replayLSN is the LSN of the WAL frame Open's recovery is applying:
	// the vector intake's forest-coverage gate reads it (0 on every live
	// write path - only Open's replay closure sets it).
	replayLSN uint64

	// Async compact flight (2026-10-24 fix arc, spec item 2). All flight
	// state is writer-thread only; the goroutine communicates through the
	// flight struct's channel and error field. compactFlightHook is a nil-
	// in-production test injection point that runs inside the flight's
	// byte-writing stage.
	compactFlight     *compactFlight
	compactFlightHook func() error
	dbPath            string      // session path; checkpoints write <dbPath minus ext>-<lsn>.db next to it
	pendOps           pendBuf     // recorded-but-unflushed ops: order log + per-kind payloads (audit F-11)
	batching          bool        // inside Batch: records accumulate instead of flushing
	closed            atomic.Bool // after Close: writes fail with ErrClosed, queries legal

	// writerTok is the wrapper-mode writer discipline hook (spec §2.3 R7):
	// 0 = direct mode (all writes legal); non-zero = the goroutine token
	// recorded by ClaimWriter. Checked by checkWriter at write-method entry
	// (debug builds only; release builds compile the check away). Only the
	// hook pair in writer_hook*.go touches it.
	writerTok atomic.Uint64

	// rebaseHappened is the C1 repair flag: set by PublishMerge when a
	// background merge rebase renumbers the base, cleared by forceMerge.
	// While set, the writer-private mirrors may carry pre-rebase IDs that the
	// rebased base's dense renumbering assigned to OTHER keys — the writer
	// must rebuild them (forceMerge) before stamping IDs into new layers, or
	// two live records of one generation can share an ID. Atomic because the
	// merger goroutine sets it (under the caller's publication mutex) while
	// the writer goroutine reads it. Direct mode never sets it.
	rebaseHappened atomic.Bool

	// ---- direct-mode write absorption window (task 7 / A4) -------------------
	//
	// Unbatched per-op writes used to materialize one micro-layer per op —
	// the A4 throughput regression. They now accumulate in the open builder
	// ("the pend") and materialize as ONE layer per flush. Flushes fire at
	// read-API entries (write-then-read stays immediate: in direct mode the
	// reader IS the writer), at Batch/Seal/Close boundaries, and at the
	// capacity cap below. The WAL recorder is NOT deferred — per-op frames
	// (and their fsyncs) stay per-op; only layer materialization is absorbed.
	//
	// Concurrency: pend is an atomic pointer so concurrent readers can check
	// for an open window without a data race; they only read the pointer and
	// the owner token (immutable after the Store), never the builder. Only
	// the goroutine that opened the window (pendWindow.owner) may publish
	// it; wrapper-mode graphs skip flushing entirely (writerAttached — the
	// wrapper's readers are never the writer, and its writer batches).
	writerAttached atomic.Bool
	pend           atomic.Pointer[pendWindow]

	// autoCompactLayers is the direct-mode layer-stack governor (B1 fix):
	// once a publish leaves the stack at/above the cap (and the raw tail
	// carries real volume), the background folder folds the T run plus the
	// tail into one layer (size-tiered: the folded run merges into the
	// older run only at half its size — folding the whole stack every
	// trigger is quadratic under sustained writes and the A3 gate caught
	// it). Bounded stacks keep the per-generation View cheap so readers keep
	// hitting their slots under sustained writes (the 2026-10-16 mixed
	// read collapse). Folds are pure view transforms — mirrors, absorption
	// window and WAL frames untouched. 0 disables. Set via SetAutoCompact
	// before the first write.
	//
	// The fold runs OFF the writer (2026-10-17, the LevelDB/RocksDB/Pebble
	// model): the writer only signals pressure, the folder goroutine
	// computes the folded prefix plus its full view-build state from the
	// published (immutable) generation, and hands the result over in
	// pendingFold; the writer incorporates it at its next publish — a
	// prefix splice plus an O(tail) view append. Publication stays with the
	// single writer (no CAS protocol, no lost updates); the only shared
	// state is atomics and a wake channel. A result whose source prefix no
	// longer matches (Seal/rebase in between) is discarded.
	//
	// autoSealEdges is the volume governor: at Batch boundaries, once the
	// unmerged stack carries at least max(autoSealEdges, base) edge records,
	// a forceMerge folds everything into a fresh base — bounding unmerged
	// VOLUME (and reclaiming tombstoned records) at a base-proportional
	// interval (every fold costs O(base), A6's lesson; the wrapper uses the
	// same rule). It stays inline on the writer: it rebuilds writer-private
	// mirrors. Never fires from read-triggered flushes — an O(base) seal on
	// the query path blows write-then-read p99 (the A3 gate caught that
	// too). 0 disables. WAL checkpointing stays with explicit Seal.
	//
	// merging guards forceMerge's internal publishLayer from re-entering the
	// governor. writerAttached (wrapper mode) skips the governor entirely:
	// the wrapper owns its merge scheduling (pubMu protocol, MaxLayers,
	// volume folds) and its gates measure exactly that.
	autoCompactLayers int
	autoSealEdges     int
	merging           bool

	// Background folder plumbing (direct mode). foldWake carries the
	// writer's pressure signals (cap 1, non-blocking sends); foldInFlight
	// is set while a fold is pending or computing so the writer signals at
	// most once per fold; folderRunning is the lazily started goroutine's
	// liveness flag (it exits after foldIdleExit without signals);
	// pendingFold is the hand-over slot the writer Swaps at publish.
	foldWake      chan struct{}
	foldInFlight  atomic.Bool
	folderRunning atomic.Bool
	pendingFold   atomic.Pointer[foldResult]
}

// foldResult is one background fold hand-over: the source prefix it was
// computed from (pointer-identical layers — the writer only ever appends,
// so a prefix match means the result still applies), the folded replacement
// run(s), the full view-build state of the folded prefix (the writer
// appends the tail onto it), and the split marks for the resulting stack.
type foldResult struct {
	src    []*delta.Layer
	folded []*delta.Layer
	ext    *viewBuild
	splitF int
	splitT int
}

// viewCacheEntry is one generation's memoized three-source View (held in
// genView.cached). view is immutable once stored; ext is the writer's
// incremental build state (nil on reader-built entries) — readers only read
// view, so the writer may steal and extend a superseded generation's ext
// race-free by field separation.
type viewCacheEntry struct {
	view *engine.View
	ext  *viewBuild
}

// engineViewOf returns v's three-source View through the generation's slot.
// Writer-published generations arrive pre-filled (prebuildView); a miss is a
// generation published without prebuild (wrapper rebase, replay) — racing
// first-readers each build (engineView reads immutable state only), one
// CAS wins and the losers' copies become garbage.
func (g *Graph) engineViewOf(v *genView) *engine.View {
	if c := v.cached.Load(); c != nil {
		return c.view
	}
	ev := v.engineView()
	v.cached.CompareAndSwap(nil, &viewCacheEntry{view: ev})
	if c := v.cached.Load(); c != nil {
		return c.view
	}
	return ev
}

// layerPrefixMatch reports whether a and b share their first k layer
// pointers (immutable layers — pointer equality is identity).
func layerPrefixMatch(a, b []*delta.Layer, k int) bool {
	if len(a) < k || len(b) < k {
		return false
	}
	for i := range k {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// prebuildView refreshes the view cache on the writer path so readers of a
// freshly published generation always hit. Without it the first query after
// every publish pays the full O(unmerged-delta) assembly, and under
// sustained writes that cost outlasts the publish interval — the cache
// never hits and read latency collapses (the B1 mixed-read baseline:
// rebuild ~50-90ms vs a 6ms publish interval at 165 batches/s).
//
// prebuildView fills nv's view slot BEFORE nv is published (callers Store
// the generation afterwards), extending the predecessor generation's build
// where it can. Readers of a published generation therefore always hit.
// Filling the slot after the Store re-opened that window on every publish —
// a miss storm of reader full rebuilds that also clobbered the writer's
// build state (the 2026-10-17 writer throttle to 54 batches/s).
//
// The incremental cases: a pure append reuses the predecessor's sources and
// cumulative state (cost O(new layers)); a T-run fold rewinds to the
// F-boundary snapshot and rebuilds T+tail (cost O(T+tail), one O(F) map
// copy per fold interval). Anything else (Seal, F-merge, rebase, replay)
// rebuilds fully. Writer-path only; the predecessor is g.view at call time
// and its ext is stolen and mutated in place (see viewCacheEntry).
func (g *Graph) prebuildView(nv *genView, from *viewBuild) {
	if len(nv.layers) == 0 || g.merging {
		return
	}
	if l := nv.layers[len(nv.layers)-1]; l.EdgeCount() > prebuildMaxLayerEdges {
		return // bulk-import window: no interleaved readers to serve
	}
	var vb *viewBuild
	if from != nil {
		// a background fold was incorporated: its state covers the folded
		// prefix, append whatever the writer stacked past it
		vb = from
	} else {
		prev := g.view.Load()
		if c := prev.cached.Load(); c != nil && c.ext != nil && prev.base == nv.base &&
			c.ext.builtTo <= len(nv.layers) && layerPrefixMatch(prev.layers, nv.layers, c.ext.builtTo) {
			vb = c.ext // pure append: layers[builtTo:] are new
		}
	}
	if vb == nil {
		vb = newViewBuild(nv)
	}
	for i := vb.builtTo; i < len(nv.layers); i++ {
		vb.appendLayer(nv, i)
	}
	// the carried kill sets already cover every appended source — assembly
	// costs O(new layer), not O(stack)
	nv.cached.Store(&viewCacheEntry{
		view: engine.NewViewWithKills(nv.base, vb.sources, vb.nodeN, vb.deadBase, vb.killed),
		ext:  vb,
	})
}

// New returns an empty graph. The initial generation is an empty merged
// base (engine.Seal of nothing), so every query is legal from the start;
// Sealed() is true until the first write opens a layer.
// defaultAutoCompactLayers bounds the direct-mode unmerged layer stack: with
// sustained small batches a deeper stack makes the per-generation View
// rebuild outlast the publish interval and the viewCache never hits (every
// query pays a full O(delta × stack) rebuild — the B1 collapse). 32 keeps
// rebuilds sub-millisecond at the M/L tiers while folding only every ~32
// publishes at O(pending records), amortized well under 1% of write time.
const defaultAutoCompactLayers = 32

// defaultAutoSealEdges is the floor of the volume-proportional auto-seal
// trigger (mirrors internal/writer's MergeEdges): a fold fires when the
// unmerged stack carries max(defaultAutoSealEdges, base.EdgeCount) edges.
const defaultAutoSealEdges = 65536

func New() *Graph {
	g := &Graph{
		nodes:             map[string]nodeRef{},
		edges:             map[delta.EdgeKey]edgeRef{},
		labels:            map[string]bool{},
		autoCompactLayers: defaultAutoCompactLayers,
		autoSealEdges:     defaultAutoSealEdges,
		foldWake:          make(chan struct{}, 1),
	}
	g.view.Store(&genView{base: engine.Seal(nil, nil, nil, nil, nil), baseKeys: map[string]NodeID{}})
	return g
}

// SetAutoCompact tunes the direct-mode layer-stack governor (see
// Graph.autoCompactLayers): the stack folds into one layer once a publish
// leaves it at/above maxLayers. 0 disables. Chainable; call before the
// first write (write-path field, like the rest of the tunables).
func (g *Graph) SetAutoCompact(maxLayers int) *Graph {
	g.autoCompactLayers = maxLayers
	return g
}

// SetAutoSealEdges tunes the volume governor (see Graph.autoSealEdges): a
// fold into a fresh base fires at Batch boundaries once the unmerged stack
// carries at least max(minEdges, base.EdgeCount()) edge records. 0
// disables. Chainable; call before the first write.
func (g *Graph) SetAutoSealEdges(minEdges int) *Graph {
	g.autoSealEdges = minEdges
	return g
}

// minFoldMerge is the floor on the size-tier rule: a folded run merges into
// the older run only at max(minFoldMerge, olderRunEdges/2) records, so tiny
// tails never trigger F-merges (each costing O(F)) and small graphs fold
// into one run early instead of carrying two.
const minFoldMerge = 512

// minTailFoldEdges is the tail volume gate: below it a fold costs more than
// the tail carries (write amplification), so thin tails keep accumulating
// raw layers instead. Bounded by the layer cap above, the extra depth is
// a small constant.
const minTailFoldEdges = 64

// prebuildMaxLayerEdges is the layer size above which the writer skips the
// per-publish view prebuild: bulk-import windows (64K-edge absorption cuts)
// carry no interleaved readers, so prebuilding them is pure writer overhead
// (the import regression: 1.27M -> 0.78M edges/s). Readers of an un-prebuilt
// generation build once through the slot's CAS fallback; the next small
// publish pays one full rebuild to re-establish the incremental chain.
const prebuildMaxLayerEdges = 4096

// foldIdleExit is how long the folder goroutine waits for another pressure
// signal before exiting (it restarts lazily on the next signal), so idle or
// short-lived graphs carry no goroutine.
const foldIdleExit = 500 * time.Millisecond

// foldStallFactor: with a fold in flight and the stack past this multiple
// of the layer cap, the writer sleeps 1ms per publish (LevelDB's slowdown
// stall) so the folder catches up. A long-run safety valve; the mixed
// workload never reaches it.
const foldStallFactor = 4

// afterPublish is the writer-side half of the direct-mode governor, run
// after every publish: signal the background folder when the stack is under
// pressure, pace the writer if the folder is far behind, and (at Batch
// boundaries only) fire the inline volume seal. Writer-path only; callers
// are cutLayer and Batch's end. The wrapper skips it (it owns merging).
func (g *Graph) afterPublish(atBatchEnd bool) {
	if g.autoCompactLayers <= 0 || g.merging || g.writerAttached.Load() {
		return
	}
	v := g.view.Load()
	n := len(v.layers)
	if n < g.autoCompactLayers {
		return
	}
	// Volume seal (Batch boundaries only, stack at the cap): bounds record
	// VOLUME and reclaims tombstoned records at a base-proportional
	// interval; inline because it rebuilds writer-private mirrors.
	if atBatchEnd && g.autoSealEdges > 0 {
		total := 0
		for _, l := range v.layers {
			total += l.EdgeCount()
		}
		if total >= max(g.autoSealEdges, v.base.EdgeCount()) {
			_ = g.forceMerge()
			return
		}
	}
	if g.foldInFlight.Load() {
		if n >= foldStallFactor*g.autoCompactLayers {
			time.Sleep(time.Millisecond)
		}
		return
	}
	tailEdges := 0
	for _, l := range v.layers[v.splitT:] {
		tailEdges += l.EdgeCount()
	}
	if tailEdges < minTailFoldEdges {
		return
	}
	g.foldInFlight.Store(true)
	if g.folderRunning.CompareAndSwap(false, true) {
		go g.foldLoop()
	}
	select {
	case g.foldWake <- struct{}{}:
	default:
	}
}

// foldLoop is the background folder goroutine: it folds on each wake and
// exits after foldIdleExit without one. The exit re-checks the wake channel
// after clearing folderRunning so a signal landing in that gap is never lost
// (the signaler restarts a goroutine only when the flag reads false).
func (g *Graph) foldLoop() {
	idle := time.NewTimer(foldIdleExit)
	defer idle.Stop()
	for {
		select {
		case <-g.foldWake:
			if g.closed.Load() {
				g.folderRunning.Store(false)
				return
			}
			g.runVecDrain()
			g.runFold()
			idle.Reset(foldIdleExit)
		case <-idle.C:
			g.folderRunning.Store(false)
			select {
			case <-g.foldWake:
				if !g.folderRunning.CompareAndSwap(false, true) {
					return // the signaler started a fresh goroutine
				}
				if g.closed.Load() {
					g.folderRunning.Store(false)
					return
				}
				g.runVecDrain()
				g.runFold()
				idle.Reset(foldIdleExit)
			default:
				return
			}
		}
	}
}

// runFold computes one fold from the published generation and hands it
// over. Pure with respect to writer state: it reads immutable layers and the
// generation's own split marks, and the writer only sees the result through
// pendingFold.
func (g *Graph) runFold() {
	defer g.foldInFlight.Store(false)
	v := g.view.Load()
	r := computeFold(v)
	if r != nil {
		g.pendingFold.Store(r)
	}
}

// computeFold is the size-tiered fold of one generation's stack: T plus the
// raw tail fold into one layer — O(T + tail records), never touching F
// (re-folding F every trigger is quadratic under sustained writes) — and
// the folded run merges into F only when it reaches max(minFoldMerge, F/2)
// (amortized <=3 record copies per write). It also builds the folded
// prefix's full view state (the writer appends only the tail onto it). nil
// when there is nothing to fold.
func computeFold(v *genView) *foldResult {
	n := len(v.layers)
	splitF, splitT := v.splitF, v.splitT
	if splitF > n {
		splitF = n
	}
	if splitT > n || splitT < splitF {
		splitT = splitF
	}
	if splitT >= n {
		return nil
	}
	folded := delta.FoldLayers(v.layers[splitF:])
	if folded == nil {
		return nil
	}
	newLayers := make([]*delta.Layer, 0, splitF+1)
	newLayers = append(newLayers, v.layers[:splitF]...)
	newLayers = append(newLayers, folded)
	fEdges := 0
	for _, l := range v.layers[:splitF] {
		fEdges += l.EdgeCount()
	}
	r := &foldResult{src: v.layers}
	if splitF == 0 || folded.EdgeCount() >= max(minFoldMerge, fEdges/2) {
		if merged := delta.FoldLayers(newLayers); merged != nil {
			newLayers = []*delta.Layer{merged}
		}
		r.splitF, r.splitT = 1, 1
	} else {
		r.splitF, r.splitT = splitF, splitF+1
	}
	r.folded = newLayers
	// view state of the folded prefix — resolve/live/memo built over the
	// folded layers only (they stand on the same base)
	fv := &genView{base: v.base, baseKeys: v.baseKeys, layers: newLayers}
	vb := newViewBuild(fv)
	for i := range newLayers {
		vb.appendLayer(fv, i)
	}
	r.ext = vb
	return r
}

// ---- public write path ------------------------------------------------------

// AddNode idempotently writes a node: if the key already exists it updates
// label/props and returns the original ID (a deleted key revives, reusing
// its ID). The write is immediately visible to every point query.
//
// Props sharing contract: this method copies the passed-in map; but the maps
// exposed via Node/Edge after a merge are internal references — updates must
// replace them wholesale (AddNode/AddEdge again); in-place mutation is
// forbidden.
func (g *Graph) AddNode(key, label string, props map[string]any) (NodeID, error) {
	if g.closed.Load() {
		return 0, ErrClosed
	}
	if err := g.checkWriter(); err != nil {
		return 0, err
	}
	// Unique-constraint gate (spec 2026-10-30 §2.3): the check runs at ENTRY,
	// before recordNode and before the ID ratchet advances, so a rejected
	// write leaves WAL frames, the pend buffer, the builder, the registry, the ratchet
	// and the vector intake untouched. The pre-image fetch must precede every
	// mutation of the node's records (the builder overwrites them below).
	pre := g.uniquePreState(key)
	if err := g.uniqueCheckAdd(key, label, props); err != nil {
		return 0, err
	}
	if err := g.recordNode(key, label, props); err != nil {
		return 0, err
	}
	g.ensureBuilder()
	id, ok := g.bNodeIDs[key]
	if !ok {
		if ref, seen := g.nodes[key]; seen {
			id = uint32(ref.id) // override/revive keeps the original ID
		} else {
			id = uint32(g.nextNode)
			g.nextNode++
		}
	}
	g.bNodeIDs[key] = id
	g.bLiveNodes[key] = true
	if err := g.builder.UpsertNode(key, label, props); err != nil {
		return 0, err // unreachable for a fresh builder; surfaced anyway
	}
	g.vecIntakeNode(key, props)
	g.uniqueSwap(pre, key, label, props)
	g.finishOp(0)
	return NodeID(id), nil
}

// AddEdge appends a directed edge (multi-edges/self-loops both allowed).
// Immediately visible; new EdgeIDs come from the generation ratchet.
func (g *Graph) AddEdge(srcKey, dstKey, label string, props map[string]any) (EdgeID, error) {
	if g.closed.Load() {
		return 0, ErrClosed
	}
	if err := g.checkWriter(); err != nil {
		return 0, err
	}
	if err := g.checkLiveNodes(srcKey, dstKey); err != nil {
		return 0, err
	}
	if err := g.recordEdge(srcKey, dstKey, label, props, true); err != nil {
		return 0, err
	}
	g.ensureBuilder()
	g.materializeEndpoint(srcKey)
	g.materializeEndpoint(dstKey)
	g.labels[label] = true
	id := g.newEdgeSlotID()
	if err := g.builder.AddEdgeValidated(srcKey, dstKey, label, props); err != nil {
		return 0, err
	}
	g.finishOp(1)
	return id, nil
}

// UpsertEdge replaces-by-key: if live edges with the same (src, dst, label)
// triple exist anywhere in the visible state (base or layers), the triple collapses
// onto one edge — the visible edge keeps its EdgeID and its Props are
// replaced wholesale; otherwise a new edge is appended. Unlike AddEdge (pure
// append, multigraph semantics) this method guarantees the triple is unique
// — it is the entry point for updating weight-like props (relation
// strength, event counts).
func (g *Graph) UpsertEdge(srcKey, dstKey, label string, props map[string]any) (EdgeID, error) {
	if g.closed.Load() {
		return 0, ErrClosed
	}
	if err := g.checkWriter(); err != nil {
		return 0, err
	}
	if err := g.checkLiveNodes(srcKey, dstKey); err != nil {
		return 0, err
	}
	if err := g.recordEdge(srcKey, dstKey, label, props, false); err != nil {
		return 0, err
	}
	g.ensureBuilder()
	g.materializeEndpoint(srcKey)
	g.materializeEndpoint(dstKey)
	g.labels[label] = true
	k := delta.EdgeKey{SrcKey: srcKey, DstKey: dstKey, Label: label}
	var id EdgeID
	if _, last, live := g.builder.EdgeSlots(k); live {
		id = EdgeID(g.bEdgeIDs[last]) // fold onto this batch's most recent parallel (in place)
	} else {
		// the fold appends a slot in this layer: it carries either the
		// transferred winner ID (visible edge keeps its ID) or a fresh one
		// (window-tombstoned parallels must not lend their IDs — ratchet)
		if ref, ok := g.edges[k]; ok && g.bKilled[k] == 0 {
			id = ref.winner
		} else {
			id = g.nextEdge
			g.nextEdge++
		}
		g.bEdgeIDs = append(g.bEdgeIDs, uint32(id))
	}
	if err := g.builder.UpsertEdgeFoldValidated(srcKey, dstKey, label, props); err != nil {
		return 0, err
	}
	g.finishOp(1)
	return id, nil
}

// DeleteEdge removes every live parallel edge with the given (src, dst,
// label) triple across all three sources (idempotent: deleting a triple that
// never existed succeeds with 0). Returns the number of edges actually
// deleted. Immediately invisible afterwards.
func (g *Graph) DeleteEdge(srcKey, dstKey, label string) (int, error) {
	if g.closed.Load() {
		return 0, ErrClosed
	}
	if err := g.checkWriter(); err != nil {
		return 0, err
	}
	if err := g.checkLiveNodes(srcKey, dstKey); err != nil {
		return 0, err
	}
	if !g.labels[label] {
		return 0, nil // label never used by an edge: no match possible, idempotent success
	}
	k := delta.EdgeKey{SrcKey: srcKey, DstKey: dstKey, Label: label}
	n := 0
	if g.builder != nil {
		if c, _, live := g.builder.EdgeSlots(k); live {
			n = c // this batch's parallels
		}
	}
	// published parallels this window has not tombstoned yet (a delete →
	// re-add → delete sequence must not re-count the first delete's kills:
	// the mirror reconciles only at publishLayer)
	pub := g.edges[k].n - g.bKilled[k]
	if pub > 0 {
		n += pub
	}
	if err := g.recordEdgeDelete(srcKey, dstKey, label); err != nil {
		return 0, err
	}
	g.ensureBuilder()
	if _, err := g.builder.DeleteEdge(k); err != nil {
		return 0, err
	}
	g.bKilled[k] += pub
	g.finishOp(0)
	return n, nil
}

// DeleteNode removes a node and cascade-deletes all its incident edges
// (DETACH DELETE semantics) across all three sources: base incidents are
// discovered by bidirectional CSR row scans (spec §3.1 R2), older-layer
// incidents by record scan; everything older dies by tombstone. Re-adding
// the same key via AddNode revives the node (ID reused) without resurrecting
// the cascaded edges. Returns the number of cascaded edge deletions.
func (g *Graph) DeleteNode(key string) (int, error) {
	if g.closed.Load() {
		return 0, ErrClosed
	}
	if err := g.checkWriter(); err != nil {
		return 0, err
	}
	if !g.nodeLive(key) {
		return 0, ErrNodeNotFound
	}
	// Fetch the node's pre-image BEFORE any mutation: once the builder's
	// tombstone lands, the record is only reachable through the (stale until
	// publish) mirror. The release itself runs only after the write steps
	// succeeded, next to vecIntakeDelete — an error path leaves the registry
	// exactly as it was.
	pre := g.uniquePreState(key)
	if err := g.recordNodeDelete(key); err != nil {
		return 0, err
	}
	g.ensureBuilder()
	var hookKeys []delta.EdgeKey
	n, err := g.builder.DeleteNode(key, func(k string) []delta.EdgeKey {
		hookKeys = g.olderIncidentEdges(k)
		return hookKeys
	})
	if err != nil {
		return 0, err
	}
	g.bLiveNodes[key] = false
	g.vecIntakeDelete(key)
	if pre.live {
		g.uniqueRelease(pre.label, pre.props, key)
	}
	for _, k := range hookKeys {
		// mark the hook's published parallels killed (one per emitted copy) so
		// later same-batch upserts draw fresh IDs (ratchet discipline) and
		// later deletes/hooks skip re-counting them; the builder's cascade
		// already tombstoned the in-window parallels.
		g.bKilled[k]++
	}
	g.finishOp(0)
	return n, nil
}

// Batch groups writes into one WAL boundary AND one micro-layer: fn's write
// methods record Changeset entries without flushing, and the boundary flushes
// them as one frame (one fsync in ModeAlways — group commit for direct-mode
// writers) and materializes the whole batch as a single layer (published
// atomically at the boundary — writes inside fn are not yet visible to
// queries). A normal batch is exactly one frame; batches whose op order
// regresses across Changeset kinds are split at each regression so replay
// preserves the original op order (see the op-recorder comment).
//
// Nested Batch calls are absorbed into the outer boundary. WALOff callers
// pay nothing for frames but still get the per-batch layer grouping.
//
// Known direct-mode limitation (closed by the v0.7 wrapper-mode writer): a
// flush error at the boundary returns after fn's mutations already hit
// memory — memory may be ahead of the log until the next successful flush.
func (g *Graph) Batch(fn func(*Graph)) error {
	if g.closed.Load() {
		return ErrClosed
	}
	if err := g.checkWriter(); err != nil {
		return err
	}
	if g.batching {
		fn(g) // nested: absorbed into the outer boundary
		return nil
	}
	g.batching = true
	g.ensureBuilder()
	defer func() { // even if fn panics, never wedge the recorder
		g.batching = false
		if g.builder != nil {
			// Panic path: drop the un-published layer (the buffered ops stay
			// in the pend buffer for the next flush, T2 discipline).
			g.builder = nil
			g.bNodeIDs, g.bEdgeIDs = nil, nil
			g.bLiveNodes, g.bKilled, g.bIndex, g.bFTS = nil, nil, nil, nil
			// The unique-constraint registry saw the dropped window's
			// writes: mark it stale (spec §3.3) — strictly AFTER
			// g.batching cleared above, so the rebuild's flush can never
			// cut an open batch (the ordering the Task 2 review pinned).
			g.uniqueStale = true
		}
	}()
	fn(g)
	var err error
	if g.wal != nil {
		err = g.flushPending()
	}
	g.publishLayer()
	g.afterPublish(true) // Batch boundary: the volume seal may fire here
	return err
}

// Close ends a session. With a WAL attached, Close is a mandatory checkpoint
// in whichever tier checkpoint() picks (v0.11.2): the first checkpoint of a
// lineage, any pre-v6 base, and every library whose retained chain reached
// DefaultCompactBytes take the COMPACT tier — merge → SaveFile(<dbPath minus
// ext>-<lsn>.db) → manifest → forest → WAL reset (deletes the retained chain)
// → old-watermark cleanup — while every later Close under the threshold takes
// the LIGHT tier: WAL rotate + manifest-v2 rewrite, no snapshot write, no
// merge (the in-memory layer stack already answers post-Close queries; the
// retained chain carries the increment on disk).
//
// Failure semantics:
//   - before the manifest lands, any failure returns the error WITHOUT
//     deleting the WAL — the next Open recovers from it, and Close can be
//     retried (merge and SaveFile are safe on an already-merged graph; a
//     retried light Close at worst leaves one extra empty segment, which
//     recovery chain-continues into);
//   - once the manifest has landed the checkpoint is committed and the
//     session is marked closed: later failures (segment deletion, fd close)
//     are surfaced but a retry is a clean no-op — it can never wedge;
//   - a WAL handle that died mid-checkpoint (wal.ErrClosed from Sync/Rotate/
//     Reset/Close) is treated as benign: the manifest gates the un-reset
//     segments.
//
// Append/fsync failure discipline (see the op-recorder comment): if a frame
// Append ever returned an error, the caller must retry the flush (Batch/
// Close) or abandon the graph — buffered ops stay in the pend buffer and are
// re-attempted by the next flush, because a frame whose fsync reported
// failure may still be on disk.
//
// WALOff sessions: materialize any open absorption window (queries must
// keep seeing every write after Close) and flip to closed (writes then
// return ErrClosed; durability was the caller's job, v0.6 style). In both
// modes queries stay legal after Close.
func (g *Graph) Close() error {
	if g.closed.Load() {
		return nil
	}
	// Close rewrites writer-private state (flushPending/forceMerge/
	// publishLayer) — heavier than any write method, all of which are
	// guarded; wrapper mode must go through Handle.Close (which yields the
	// claim first), not race the writer from the outside.
	if err := g.checkWriter(); err != nil {
		return err
	}
	if g.wal == nil {
		g.publishLayer()
		g.closed.Store(true)
		return nil
	}
	if err := g.WaitAsyncCompact(); err != nil {
		return err
	}
	if err := g.flushPending(); err != nil {
		return err
	}
	if err := g.wal.Sync(); err != nil && !errors.Is(err, wal.ErrClosed) {
		return err
	}
	compact, man, err := g.compactTier()
	if err != nil {
		return err
	}
	if compact {
		// The compact tier exports the merged view: fold first (SaveFile can
		// merge on demand, but the merge also re-densifies IDs and drops the
		// endpoint registry — the v0.11.1 Close contract).
		if err := g.forceMerge(); err != nil {
			return err
		}
		if err := g.compactCheckpoint(g.wal.LastLSN()); err != nil {
			return err
		}
	} else if err := g.lightCheckpoint(man); err != nil {
		return err
	}
	// Close-shaped: a light tier must still persist converged pack work,
	// or the reopen restores the stale forest's frozen overlay (1M B1).
	// The compact tier's own forest write already advanced the markers.
	if !compact {
		if err := g.closeForestRefresh(); err != nil {
			return err
		}
	}
	// The manifest has landed: this checkpoint is committed. From here on a
	// failure is surfaced but must never leave the session retryable-but-
	// unwritable — mark closed so a retry is a clean no-op.
	g.closed.Store(true)
	if err := g.wal.Close(); err != nil && !errors.Is(err, wal.ErrClosed) {
		g.wal = nil
		return err
	}
	g.wal = nil
	return nil
}

// ---- point queries (three-source: layers new→old + base, tombstones) --------

// NodeByKey finds the winning (newest live) record for key across base and
// layers; tombstoned keys are invisible. The entry materializes the caller's
// absorption window first (direct mode: write-then-read is immediate).
func (g *Graph) NodeByKey(key string) (Node, bool) {
	g.flushPend()
	return g.view.Load().nodeWinner(key)
}

// NodeByID resolves an ID to its visible record. Overridden/deleted records
// (whose IDs may still be listed by older layers) do not resolve.
func (g *Graph) NodeByID(id NodeID) (Node, bool) {
	g.flushPend()
	return g.view.Load().nodeByID(id)
}

// nodeByID is NodeByID pinned to this generation (the three-source surfaces
// in view.go run against exactly one published genView).
func (v *genView) nodeByID(id NodeID) (Node, bool) {
	for i := len(v.layers) - 1; i >= 0; i-- {
		l := v.layers[i]
		idx, ok := l.FindNodeID(uint32(id))
		if !ok {
			continue
		}
		rec := l.Nodes()[idx]
		// A newer layer's live record for the same key would carry the same
		// (transferred) id and have matched above; only tombstones can kill.
		for j := i + 1; j < len(v.layers); j++ {
			if v.layers[j].NodeTombstones()[rec.Key] {
				return Node{}, false
			}
		}
		return Node{ID: id, Key: rec.Key, Label: rec.Label, Props: rec.Props}, true
	}
	if uint32(id) < uint32(v.base.NodeCount()) {
		rec, ok := v.base.NodeInfo(uint32(id))
		if !ok {
			return Node{}, false
		}
		for _, l := range v.layers {
			if l.NodeTombstones()[rec.Key] {
				return Node{}, false
			}
		}
		return Node{ID: id, Key: rec.Key, Label: rec.Label, Props: rec.Props}, true
	}
	return Node{}, false
}

// EdgeByID resolves an EdgeID to its visible record. Layer records are
// resolved first (a fold transfers the winner ID onto the newer record);
// base records resolve unless a layer tombstones their key.
func (g *Graph) EdgeByID(id EdgeID) (Edge, bool) {
	g.flushPend()
	return g.view.Load().edgeByID(id)
}

// edgeByID is EdgeByID pinned to this generation.
func (v *genView) edgeByID(id EdgeID) (Edge, bool) {
	for i := len(v.layers) - 1; i >= 0; i-- {
		l := v.layers[i]
		idx, ok := l.FindEdgeID(uint32(id))
		if !ok {
			continue
		}
		rec := l.Edges()[idx]
		k := delta.EdgeKey{SrcKey: rec.SrcKey, DstKey: rec.DstKey, Label: rec.Label}
		if !v.layerEdgeLive(i, k) {
			return Edge{}, false
		}
		return Edge{
			ID: id, Src: v.nodeIDOf(rec.SrcKey), Dst: v.nodeIDOf(rec.DstKey),
			Label: rec.Label, Props: rec.Props,
		}, true
	}
	if uint32(id) < uint32(v.base.EdgeCount()) {
		src, dst, label, props, ok := v.base.EdgeInfo(uint32(id))
		if !ok {
			return Edge{}, false
		}
		k := delta.EdgeKey{SrcKey: v.baseKeyOf(src), DstKey: v.baseKeyOf(dst), Label: label}
		if !v.baseEdgeLive(k) {
			return Edge{}, false
		}
		return Edge{ID: id, Src: NodeID(src), Dst: NodeID(dst), Label: label, Props: props}, true
	}
	return Edge{}, false
}

// NodeCount/EdgeCount report the visible size of the whole generation
// (base minus tombstoned plus visible layer records). Both materialize the
// caller's absorption window first.
func (g *Graph) NodeCount() int { g.flushPend(); return g.view.Load().nodes }

func (g *Graph) EdgeCount() int { g.flushPend(); return g.view.Load().edges }

// Sealed reports whether the graph has no unmerged micro-layers. It flips to
// false with the first write after a merge and back on Seal/forceMerge.
func (g *Graph) Sealed() bool { g.flushPend(); return len(g.view.Load().layers) == 0 }

// WALSize reports the valid WAL frame bytes logged since the last checkpoint
// (0 when no WAL is attached) — the volume input for checkpoint triggers.
func (g *Graph) WALSize() int64 {
	if g.wal == nil {
		return 0
	}
	return g.wal.Size()
}

// Stats reports graph size and, when the generation is fully merged, the
// SCC compression results of the base. With unmerged layers present only the
// counts are reported (SCC structures exist on the merged base only).
func (g *Graph) Stats() Stats {
	g.flushPend()
	v := g.view.Load()
	if len(v.layers) > 0 {
		return Stats{Nodes: v.nodes, Edges: v.edges}
	}
	st := Stats{
		Nodes: v.base.NodeCount(),
		Edges: v.base.EdgeCount(),
		SCCs:  int(v.base.SCCCount()),
	}
	for c := uint32(0); c < v.base.SCCCount(); c++ {
		if size := len(v.base.SCCMembers(c)); size > 1 {
			st.CyclicNodes += size
		}
	}
	return st
}

// ---- merge pipeline ----------------------------------------------------------

// unionRecords computes the logical view of the CURRENT generation (see the
// genView method for the folding rules). Used by forceMerge, Save,
// ExportSnapshot and Clone (merge-on-demand without publishing).
func (g *Graph) unionRecords() ([]engine.NodeRec, []engine.EdgeRec, []string, []string, []string) {
	return g.view.Load().unionRecords()
}

// unionRecords computes the logical view of this generation as raw engine
// records: the base with all layer tombstones applied, overlaid by layer
// records oldest→newest (node override by key, edge append), labels merged
// (base dictionary first — base LabelIDs stay valid), and pending index
// declarations harvested — typed and full-text kinds alike (the FTS plan:
// both ride base ∪ layers into the merge). Deterministic order: base
// records first, then layer records in (layer, index) order.
//
// Purity: the function reads only the pinned generation (base + layers),
// never writer-private state — that is what lets the background merger
// (internal/writer) run it against a captured prefix while the writer
// keeps publishing newer layers (spec §2.3 two-writer publish protocol).
func (v *genView) unionRecords() ([]engine.NodeRec, []engine.EdgeRec, []string, []string, []string) {
	labels := make([]string, 0, v.base.LabelCount())
	labID := make(map[string]uint32, v.base.LabelCount())
	intern := func(name string) uint32 {
		if id, ok := labID[name]; ok {
			return id
		}
		id := uint32(len(labels))
		labID[name] = id
		labels = append(labels, name)
		return id
	}
	for i := range v.base.LabelCount() {
		intern(v.base.LabelName(uint32(i)))
	}
	for _, l := range v.layers {
		for _, lb := range l.Labels() {
			intern(lb)
		}
	}

	// nodes: base order, overrides in place, layer-first-seen appended
	type nodeSlot struct {
		rec  engine.NodeRec
		dead bool
	}
	ns := make([]nodeSlot, 0, v.base.NodeCount())
	idx := make(map[string]int, v.base.NodeCount())
	for _, rec := range v.base.ExportNodes() {
		idx[rec.Key] = len(ns)
		ns = append(ns, nodeSlot{rec: rec})
	}
	for _, l := range v.layers {
		for k := range l.NodeTombstones() {
			if i, ok := idx[k]; ok {
				ns[i].dead = true
				delete(idx, k)
			}
		}
		for _, rec := range l.Nodes() {
			if i, ok := idx[rec.Key]; ok {
				ns[i].rec = engine.NodeRec{Key: rec.Key, Label: rec.Label, Props: rec.Props}
			} else {
				idx[rec.Key] = len(ns)
				ns = append(ns, nodeSlot{rec: engine.NodeRec{Key: rec.Key, Label: rec.Label, Props: rec.Props}})
			}
		}
	}
	nodes := make([]engine.NodeRec, 0, len(ns))
	for _, s := range ns {
		if s.dead {
			continue
		}
		idx[s.rec.Key] = len(nodes)
		nodes = append(nodes, s.rec)
	}

	// edges: a record survives iff no strictly newer layer tombstones its key
	suff := make([]map[delta.EdgeKey]bool, len(v.layers)+1)
	suff[len(v.layers)] = map[delta.EdgeKey]bool{}
	for i := len(v.layers) - 1; i >= 0; i-- {
		m := make(map[delta.EdgeKey]bool, len(suff[i+1])+len(v.layers[i].EdgeTombstones()))
		maps.Copy(m, suff[i+1])
		for k := range v.layers[i].EdgeTombstones() {
			m[k] = true
		}
		suff[i] = m
	}
	edges := make([]engine.EdgeRec, 0, v.base.EdgeCount())
	appendEdge := func(srcKey, dstKey, label string, lid uint32, props map[string]any) {
		s, okS := idx[srcKey]
		d, okD := idx[dstKey]
		if !okS || !okD {
			return // defensive: edges whose endpoints died (they are tombstoned anyway)
		}
		edges = append(edges, engine.EdgeRec{Src: uint32(s), Dst: uint32(d), LabelID: lid, Props: props})
	}
	for _, er := range v.base.ExportEdges() {
		k := v.baseEdgeKeyFrom(er)
		if suff[0][k] {
			continue
		}
		appendEdge(k.SrcKey, k.DstKey, k.Label, er.LabelID, er.Props)
	}
	for i, l := range v.layers {
		next := suff[i+1]
		for _, rec := range l.Edges() {
			k := delta.EdgeKey{SrcKey: rec.SrcKey, DstKey: rec.DstKey, Label: rec.Label}
			if next[k] {
				continue
			}
			appendEdge(rec.SrcKey, rec.DstKey, rec.Label, intern(rec.Label), rec.Props)
		}
	}

	// pending index declarations harvested into the merge — both kinds
	keySet := map[string]bool{}
	for _, k := range v.base.IndexedKeys() {
		keySet[k] = true
	}
	for _, l := range v.layers {
		for _, k := range l.IndexKeys() {
			keySet[k] = true
		}
	}
	indexKeys := slices.Sorted(maps.Keys(keySet))
	ftsSet := map[string]bool{}
	for _, k := range v.base.FTSKeys() {
		ftsSet[k] = true
	}
	for _, l := range v.layers {
		for _, k := range l.FTSKeys() {
			ftsSet[k] = true
		}
	}
	ftsKeys := slices.Sorted(maps.Keys(ftsSet))
	return nodes, edges, labels, indexKeys, ftsKeys
}

// baseEdgeKeyFrom derives the logical key of a raw base edge record.
func (v *genView) baseEdgeKeyFrom(er engine.EdgeRec) delta.EdgeKey {
	return delta.EdgeKey{
		SrcKey: v.baseKeyOf(er.Src),
		DstKey: v.baseKeyOf(er.Dst),
		Label:  v.base.LabelName(er.LabelID),
	}
}

// forceMerge folds the whole generation (base ∪ layers) into a fresh
// immutable base and publishes {newBase, layers=nil}: tombstones applied to
// the base first, layer records overlaid oldest→newest, labels/index
// declarations merged, node and edge IDs renumbered densely (R1) with the
// ratchets reset to the new base size. No-op when nothing is unmerged —
// except after a background rebase (rebaseHappened): the mirrors then hold
// pre-rebase IDs even with an empty layer stack, so the rebuild still runs.
// The old generation stays readable for concurrent holders; memory is
// reclaimed by GC. Cycles() uses this before answering (spec §3.2).
func (g *Graph) forceMerge() error {
	// merging guards afterPublish against re-entry: forceMerge's own
	// publishLayer must not fire the governor (this IS the full fold).
	g.merging = true
	defer func() { g.merging = false }()
	// §3.3 stale-mark gate: this fold re-derives the writer-private mirrors
	// because a rebase renumbered under them (RepairAfterRebase and every
	// other post-rebase forceMerge caller — the mirror-rebuild sites). The
	// unique-constraint registry is key-stable and stays correct across
	// folds, but the self-healing ruling still rebuilds it after a mirror
	// rebuild — never mid-Batch (the rebuild's flush must not cut an open
	// window) and never for ordinary publish/fold/seal paths.
	rebaseRepair := g.rebaseHappened.Load()
	// unconditional: a mid-batch Seal flushes the pending batch, and even
	// with the builder already closed (window cut by an earlier read) the
	// endpoint registry must be dropped — the merge below renumbers IDs, and
	// a surviving registry entry would stamp a pre-merge ID onto a post-
	// merge record (two live nodes sharing one ID).
	g.publishLayer()
	v := g.view.Load()
	if len(v.layers) == 0 && !g.rebaseHappened.Load() {
		return nil
	}
	nodes, edges, labels, indexKeys, ftsKeys := g.unionRecords()
	snap := engine.Seal(nodes, edges, labels, indexKeys, ftsKeys)
	baseKeys := make(map[string]NodeID, len(nodes))
	g.nodes = make(map[string]nodeRef, len(nodes))
	for i, n := range nodes {
		baseKeys[n.Key] = NodeID(i)
		g.nodes[n.Key] = nodeRef{id: NodeID(i), live: true, label: n.Label, props: n.Props}
	}
	g.nodeTotal = len(nodes)
	g.edges = make(map[delta.EdgeKey]edgeRef, len(edges))
	g.edgeTotal = 0
	for i, er := range edges {
		k := delta.EdgeKey{SrcKey: nodes[er.Src].Key, DstKey: nodes[er.Dst].Key, Label: labels[er.LabelID]}
		ref := g.edges[k]
		ref.winner = EdgeID(i)
		ref.n++
		g.edges[k] = ref
		g.edgeTotal++
	}
	g.nextNode, g.nextEdge = NodeID(len(nodes)), EdgeID(len(edges))
	// The label mirror carries EDGE labels only (v0.6 labIDs parity:
	// internLabel ran on resolveEdge alone). The merged label dictionary is
	// a superset — delta Layers also intern node labels (D6) — so filtering
	// by edge usage keeps DeleteEdge's "label never used" no-op exact.
	g.labels = make(map[string]bool, len(labels))
	for _, er := range edges {
		g.labels[labels[er.LabelID]] = true
	}
	g.rebaseHappened.Store(false) // mirrors now match the published view
	if rebaseRepair && !g.batching {
		g.uniqueStale = true // the §3.3 mirror-rebuild mark (see the gate above)
	}
	g.pendingFold.Store(nil) // any fold in hand-over targeted the old stack
	g.view.Store(&genView{
		base: snap, baseKeys: baseKeys, layers: nil,
		lsn: v.lsn, nodes: g.nodeTotal, edges: g.edgeTotal,
	})
	return nil
}

// Seal is an explicit merge: unmerged layers fold into a fresh immutable
// base (IDs may be renumbered, R1) and — when a WAL is attached — a tiered
// checkpoint follows (v0.11.2: light rotation under the compaction
// threshold, full watermark snapshot + WAL reset otherwise — see
// checkpoint). Idempotent: on an already-merged graph it is at most a
// checkpoint. Writes after Seal are legal (they open a new generation); this
// method no longer makes the graph read-only. Seal is a write-path method
// guarded by the writer discipline: in wrapper mode it must run on the
// writer goroutine (debug builds return ErrNotWriter otherwise), and
// concurrent-safe only via the writer handle or single-goroutine
// discipline — it rebuilds writer-private state.
func (g *Graph) Seal() error { return g.seal(false) }

// SealForClose is Seal with the close-shaped checkpoint (2026-10-24 fix
// arc, spec item 1): the light tier additionally refreshes the vector
// forest cache when packs drifted since its last write. Without it, a
// light Close after lane convergence leaves the on-disk forest at the
// last compact's state and the reopen restores its frozen overlay
// wholesale - the 1M B1 finding (970,751 entries, 2h57m re-pay). Exported
// for internal/writer's session-end Seal (the RepairAfterRebase pattern);
// the compact tier needs nothing extra, its own forest write advances the
// drift markers.
//
// Wrapper-mode coupling surface: internal/writer only, not covered by API
// stability.
func (g *Graph) SealForClose() error { return g.seal(true) }

// seal is Seal's body; closeShape routes the light tier through the
// forest refresh (see SealForClose).
func (g *Graph) seal(closeShape bool) error {
	if err := g.checkWriter(); err != nil {
		return err
	}
	if err := g.WaitAsyncCompact(); err != nil {
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
	if !closeShape {
		return g.checkpoint(g.wal.LastLSN())
	}
	compact, man, err := g.compactTier()
	if err != nil {
		return err
	}
	if compact {
		return g.compactCheckpoint(g.wal.LastLSN())
	}
	if err := g.lightCheckpoint(man); err != nil {
		return err
	}
	return g.closeForestRefresh()
}

// closeForestRefresh persists pack work the on-disk forest does not carry
// (vecForestDrift). The written image carries the current LSN watermark -
// the v0.11.2 load gate accepts forests at or past the base LSN, and the
// chain replay's re-added entries dedup by vid (State.Add reuses vids,
// Pack keeps newest-wins).
func (g *Graph) closeForestRefresh() error {
	if !vecForestDrift(g.vecReg()) {
		return nil
	}
	return g.writeVecForest(g.wal.LastLSN())
}

// IndexProperty registers a string property key to index. Declarations made
// while layers are unmerged (or on a merged graph) are pending until the
// next merge (spec §3.3 R14): until then LookupBy* answers along the old
// base index plus a line scan of the delta (ok=true), and Seal folds the
// declaration into the base index. Idempotent; unregistered keys yield
// ok=false in LookupByProperty.
//
// Durability: with a WAL attached the declaration is recorded log-first as
// an IndexDecl frame (one frame per run of consecutive declarations inside
// a Batch), durable per the session's WAL mode — matching the industry norm
// that schema operations survive crash recovery like data writes
// (PostgreSQL WAL-logs CREATE INDEX; RocksDB logs column-family edits to
// its MANIFEST). Checkpoints persist declarations in the snapshot either
// way; WAL replay rebuilds them. A flush failure cannot surface through
// this chainable API: the op stays buffered (never dropped) and the error
// surfaces at the next error-capable boundary (a write op, Batch/Seal/
// Compact/Close).
//
// Write-path method (it publishes a layer): after Close the declaration is
// dropped and the graph returned unchanged (writes are closed; the chain
// stays usable), and in wrapper mode it must run on the writer goroutine —
// debug builds panic with ErrNotWriter otherwise (release builds rely on
// the documented single-writer discipline, like Cycles).
func (g *Graph) IndexProperty(key string) *Graph {
	if g.closed.Load() {
		return g
	}
	g.assertWriter()
	if g.keyDeclared(key,
		func(s *engine.Snapshot, key string) bool { _, ok := s.LookupIndex(key, ""); return ok },
		func(l *delta.Layer, key string) bool { return slices.Contains(l.IndexKeys(), key) },
		g.bIndex) {
		return g
	}
	_ = g.recordIndexDecl(key)
	g.ensureBuilder()
	g.builder.IndexProperty(key)
	g.bIndex = append(g.bIndex, key)
	g.finishOp(0) // absorbed like any other unbatched write (cap-checked)
	return g
}

// IndexFullText registers a string property key for full-text (substring)
// lookup (2026-10-26 FTS plan). Same declaration contract as IndexProperty:
// pending until the next merge — Seal builds the base's FTS gram buckets —
// idempotent over the base FTS keys ∪ unmerged layers ∪ the open batch, WAL
// log-first as IndexDecl frames, and dropped after Close.
//
// Coexistence: a key may be BOTH typed-indexed and FTS-indexed. The kinds
// never reject each other (the MCP layer auto-declares typed indexes over
// every property key it saw during injection, so cross-kind rejection would
// silently drop the FTS half on agent graphs).
//
// An empty key is a lenient no-op (house style): never recorded, never
// declared. The query side (LookupByContains) ships with the FTS read path.
func (g *Graph) IndexFullText(key string) *Graph {
	if g.closed.Load() {
		return g
	}
	g.assertWriter()
	if key == "" {
		return g
	}
	if g.keyDeclared(key,
		func(s *engine.Snapshot, key string) bool { return slices.Contains(s.FTSKeys(), key) },
		func(l *delta.Layer, key string) bool { return slices.Contains(l.FTSKeys(), key) },
		g.bFTS) {
		return g
	}
	_ = g.recordFTSDecl(key)
	g.ensureBuilder()
	g.builder.IndexFullText(key)
	g.bFTS = append(g.bFTS, key)
	g.finishOp(0) // absorbed like any other unbatched write (cap-checked)
	return g
}

// keyDeclared reports whether key already has a declaration of one kind —
// effective (base) or pending (unmerged layer, open batch) — probing only
// that kind's accessors, so a typed declaration of the same key never
// suppresses an FTS declare (and vice versa). indexDeclared/ftsDeclared's
// twin bodies collapsed here (audit F-17): the kinds differ only in which
// accessor they probe, never in the view/batch discipline.
func (g *Graph) keyDeclared(key string, inBase func(*engine.Snapshot, string) bool, inLayer func(*delta.Layer, string) bool, batch []string) bool {
	v := g.view.Load()
	if inBase(v.base, key) {
		return true
	}
	for _, l := range v.layers {
		if inLayer(l, key) {
			return true
		}
	}
	return slices.Contains(batch, key)
}

// declaredKeys enumerates one kind's index declarations: the merged base's
// keys plus pending declarations from unmerged layers, deduplicated and
// sorted (nil when none). IndexedPropertyKeys/FullTextIndexedKeys's twin
// bodies collapsed here (audit F-17): the kinds differ only in which key
// sets they probe. It answers from the published generation only — a
// declaration made inside a running Batch becomes visible at its boundary
// like every other batch write — which keeps it lock-free. The entry
// materializes the caller's absorption window first (a declaration
// absorbed mid-window is included).
func (g *Graph) declaredKeys(baseKeys func(*engine.Snapshot) []string, layerKeys func(*delta.Layer) []string) []string {
	g.flushPend()
	v := g.view.Load()
	seen := make(map[string]bool)
	var out []string
	add := func(k string) {
		if !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	for _, k := range baseKeys(v.base) {
		add(k)
	}
	for _, l := range v.layers {
		for _, k := range layerKeys(l) {
			add(k)
		}
	}
	slices.Sort(out)
	return out
}

// IndexedPropertyKeys returns the typed property-index declarations: the
// merged base's effective keys plus pending declarations from unmerged layers
// (sorted; nil when none). It answers from the published generation only —
// a declaration made inside a running Batch becomes visible at its boundary
// like every other batch write — which keeps it lock-free. The entry
// materializes the caller's absorption window first (a declaration absorbed
// mid-window is included). Consumers that persist or hand over a graph
// (snapshot services, clone-and-extend flows) use it to preserve index
// configuration. Full-text declarations are a separate kind with a separate
// accessor (FullTextIndexedKeys): enumerating them here would make snapshot
// consumers auto-declare typed indexes over FTS keys (unrequested memory).
func (g *Graph) IndexedPropertyKeys() []string {
	return g.declaredKeys((*engine.Snapshot).IndexedKeys, (*delta.Layer).IndexKeys)
}

// FullTextIndexedKeys is IndexedPropertyKeys's full-text sibling: the FTS
// declarations — the merged base's FTS keys plus pending declarations from
// unmerged layers (sorted; nil when none) — answering from the published
// generation only, lock-free, with the caller's absorption window
// materialized first, exactly like IndexedPropertyKeys. A snapshot consumer
// that wants to preserve full-text configuration re-declares these keys
// through IndexFullText.
func (g *Graph) FullTextIndexedKeys() []string {
	return g.declaredKeys((*engine.Snapshot).FTSKeys, (*delta.Layer).FTSKeys)
}

// ---- property lookups (three-source with pending semantics) -------------------

// LookupByProperty finds nodes whose string property equals value. Keys with
// an effective base index answer from it (delta overrides filtered through
// the winner resolution); pending keys answer via line scan (R14); unknown
// keys return ok=false. Hits are sorted by NodeID ascending.
func (g *Graph) LookupByProperty(key, value string) ([]Node, bool) {
	return g.lookupNodes(key,
		func(v *genView) ([]uint32, bool) { return v.base.LookupIndex(key, value) },
		typedPendingIn(key),
		func(props map[string]any) bool {
			s, ok := props[key].(string)
			return ok && s == value
		})
}

// LookupByNumber finds nodes whose numeric property equals v. Numbers of any
// Go/JSON width normalize to float64 in the index, so int-written and
// round-tripped values match alike.
func (g *Graph) LookupByNumber(key string, v float64) ([]Node, bool) {
	return g.lookupNodes(key,
		func(gv *genView) ([]uint32, bool) { return gv.base.LookupNumber(key, v) },
		typedPendingIn(key),
		func(props map[string]any) bool {
			f, ok := propval.ToFloat(props[key])
			return ok && f == v
		})
}

// LookupByNumberRange finds nodes whose numeric property lies in [min, max]
// (inclusive; nil leaves that side unbounded). int64 values beyond 2^53 lose
// precision (inherent to the JSON number model).
func (g *Graph) LookupByNumberRange(key string, min, max *float64) ([]Node, bool) {
	return g.lookupNodes(key,
		func(gv *genView) ([]uint32, bool) { return gv.base.LookupNumberRange(key, min, max) },
		typedPendingIn(key),
		func(props map[string]any) bool {
			f, ok := propval.ToFloat(props[key])
			if !ok {
				return false
			}
			if min != nil && f < *min {
				return false
			}
			if max != nil && f > *max {
				return false
			}
			return true
		})
}

// LookupByBool finds nodes whose boolean property equals v.
func (g *Graph) LookupByBool(key string, v bool) ([]Node, bool) {
	return g.lookupNodes(key,
		func(gv *genView) ([]uint32, bool) { return gv.base.LookupBool(key, v) },
		typedPendingIn(key),
		func(props map[string]any) bool {
			b, ok := props[key].(bool)
			return ok && b == v
		})
}

// LookupByContains finds nodes whose string property key CONTAINS substr,
// case-insensitively (needle and value both fold to lower case). It is the
// read side of the full-text plan (IndexFullText): declared keys answer
// from the sealed gram buckets, pending declarations from unmerged layers
// answer via full scan (R14), and every candidate still passes a mandatory
// verify Contains — the bigram prefilter is deliberately a superset filter
// (gram collisions like value "xxabyy bc zz" vs needle "abc" are dropped by
// the verify pass, never returned as hits). Declaration is probed FIRST —
// base FTS keys ∪ unmerged layers' FTS keys — so an undeclared key answers
// (nil, false) regardless of needle length: the 1-rune scan path carries no
// index verdict of its own. A needle shorter than two runes yields no grams
// and takes the full-scan path (correct, documented slower). substr == ""
// is not a search: (nil, false). Declared keys return ok=true even with
// zero hits. Hits are sorted by NodeID ascending.
func (g *Graph) LookupByContains(key, substr string) ([]Node, bool) {
	if substr == "" {
		return nil, false
	}
	g.flushPend()
	return lookupContainsOf(g.view.Load(), key, substr)
}

// lookupContainsOf is LookupByContains over a pinned generation — the
// shared body behind the public API and the Cypher anchor prefilter
// (cypherSource.LookupContains), so both answer identically: the bigram
// bucket narrows the base scan, unmerged layers are unioned, and every
// candidate is re-verified with a case-insensitive Contains. substr must be
// non-empty (the public wrapper rejects "" before reaching here).
func lookupContainsOf(v *genView, key, substr string) ([]Node, bool) {
	needle := strings.ToLower(substr)
	grams := engine.Bigrams(substr)
	// The declaration probe runs before any path choice: with grams the
	// base LookupFTS ok carries the verdict, but a 1-rune needle scans and
	// would otherwise answer an undeclared key — so the probe is explicit
	// for every needle length (LookupByProperty's effective/pending split,
	// mirrored over the FTS kind).
	pendingIn := ftsPendingIn(key)
	if !slices.Contains(v.base.FTSKeys(), key) {
		pending := false
		for _, l := range v.layers {
			if pendingIn(l) {
				pending = true
				break
			}
		}
		if !pending {
			return nil, false
		}
	}
	// No grams (1-rune needle) ⇒ no bucket to intersect: nil forces the
	// full-scan branch inside lookupNodesOf.
	var baseIndex func(*genView) ([]uint32, bool)
	if len(grams) > 0 {
		baseIndex = func(v *genView) ([]uint32, bool) { return v.base.LookupFTS(key, grams) }
	}
	return lookupNodesOf(v, key, baseIndex, pendingIn, func(props map[string]any) bool {
		s, ok := props[key].(string)
		return ok && strings.Contains(strings.ToLower(s), needle)
	})
}

// typedPendingIn is the pending-declaration probe of the typed index kind:
// whether key was IndexProperty-declared in an unmerged layer (the R14
// line-scan answer window before the merge makes it effective).
func typedPendingIn(key string) func(*delta.Layer) bool {
	return func(l *delta.Layer) bool { return slices.Contains(l.IndexKeys(), key) }
}

// ftsPendingIn is typedPendingIn's full-text sibling: an IndexFullText
// declaration in an unmerged layer keeps the FTS lookup answerable via scan
// until the merge seals the gram buckets. The kinds probe independently —
// a typed declaration never answers an FTS lookup, and vice versa.
func ftsPendingIn(key string) func(*delta.Layer) bool {
	return func(l *delta.Layer) bool { return slices.Contains(l.FTSKeys(), key) }
}

// lookupNodes is the shared three-source lookup: base hits (from the index
// when effective, or a full base scan for pending keys) overridden by layer
// records and carved by layer tombstones, then filtered by match over the
// winner's props. pendingIn carries the caller's declaration kind so the
// pending scan branch answers exactly that kind's declarations. The entry
// materializes the caller's absorption window (every LookupBy* passes
// through here).
func (g *Graph) lookupNodes(key string, baseIndex func(*genView) ([]uint32, bool), pendingIn func(*delta.Layer) bool, match func(map[string]any) bool) ([]Node, bool) {
	g.flushPend()
	return lookupNodesOf(g.view.Load(), key, baseIndex, pendingIn, match)
}

// lookupNodesOf is lookupNodes pinned to one generation (the Cypher
// execution source in view.go answers anchors against exactly the
// generation its rows came from). baseIndex nil means "no index to ask"
// (the FTS 1-rune scan path): the whole base becomes the candidate set —
// the pending scan branch, generalized — and the caller's own pre-probe
// (LookupByContains probes the declaration before choosing paths) owns the
// declared/undeclared verdict, so the kind gate below must not reject it.
func lookupNodesOf(v *genView, key string, baseIndex func(*genView) ([]uint32, bool), pendingIn func(*delta.Layer) bool, match func(map[string]any) bool) ([]Node, bool) {
	var ids []uint32
	effective, scan := false, false
	if baseIndex == nil {
		scan = true
	} else {
		ids, effective = baseIndex(v)
	}
	// Pending detection is kind-aware: each lookup family probes its own
	// declaration kind in the unmerged layers, so a key declared either way
	// stays answerable via scan (R14) — but only for the family that owns
	// the kind.
	pending := false
	for _, l := range v.layers {
		if pendingIn(l) {
			pending = true
			break
		}
	}
	if !effective && !pending && !scan {
		return nil, false
	}
	type hit struct {
		id NodeID
		n  Node
	}
	m := make(map[string]hit)
	if effective {
		for _, id := range ids {
			rec, ok := v.base.NodeInfo(id)
			if !ok {
				continue
			}
			m[rec.Key] = hit{NodeID(id), Node{ID: NodeID(id), Key: rec.Key, Label: rec.Label, Props: rec.Props}}
		}
	} else {
		for id := range v.base.NodeCount() {
			rec, _ := v.base.NodeInfo(uint32(id))
			m[rec.Key] = hit{NodeID(id), Node{ID: NodeID(id), Key: rec.Key, Label: rec.Label, Props: rec.Props}}
		}
	}
	for _, l := range v.layers { // oldest → newest: overrides replace, tombstones carve
		for k := range l.NodeTombstones() {
			delete(m, k)
		}
		for i := range l.Nodes() {
			rec := l.Nodes()[i]
			id := NodeID(l.NodeIDAt(i))
			m[rec.Key] = hit{id, Node{ID: id, Key: rec.Key, Label: rec.Label, Props: rec.Props}}
		}
	}
	out := make([]Node, 0, len(m))
	for _, h := range m {
		if match(h.n.Props) {
			out = append(out, h.n)
		}
	}
	slices.SortFunc(out, func(a, b Node) int { return cmp.Compare(a.ID, b.ID) })
	return out, true
}
