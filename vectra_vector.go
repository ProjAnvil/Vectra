// Vector index declarations and the per-Graph registry (vector Phase 1,
// spec §3.1/§9.1). The registry is copy-on-write behind an atomic pointer:
// reads (VectorIndexes, QueryVector's index lookup) never lock; mutations
// run on the writer thread only.

package vectra

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/projanvil/vectra/internal/store"
	"github.com/projanvil/vectra/internal/vecbin"
	"github.com/projanvil/vectra/internal/vector"
	"github.com/projanvil/vectra/internal/wal"
)

// VectorIndexDecl declares one vector index (spec §3.1). Metric is one of
// "cosine" | "l2" | "dot" and locks at creation; Dim 1..4096; zero M /
// EfConstruction take the engine defaults (16 / 200).
type VectorIndexDecl struct {
	Name           string
	Prop           string
	Dim            int
	Metric         string
	M              int
	EfConstruction int
}

func (d VectorIndexDecl) toWal() wal.VectorIndexDecl {
	return wal.VectorIndexDecl{
		Name: d.Name, Prop: d.Prop, Dim: d.Dim, Metric: d.Metric,
		M: d.M, EfConstruction: d.EfConstruction,
	}
}

func vecDeclFromWal(w wal.VectorIndexDecl) VectorIndexDecl {
	return VectorIndexDecl{
		Name: w.Name, Prop: w.Prop, Dim: w.Dim, Metric: w.Metric,
		M: w.M, EfConstruction: w.EfConstruction,
	}
}

func vecDeclToStore(d VectorIndexDecl) store.VectorIndexDecl {
	return store.VectorIndexDecl{
		Name: d.Name, Prop: d.Prop, Dim: d.Dim, Metric: d.Metric,
		M: d.M, EfConstruction: d.EfConstruction,
	}
}

func vecMetric(s string) (vector.Metric, bool) {
	switch s {
	case "cosine", "": // empty = cosine (the agent-memory default)
		return vector.Cosine, true
	case "l2":
		return vector.L2, true
	case "dot":
		return vector.Dot, true
	}
	return 0, false
}

// vecDefaults applies the engine's zero-means-default build parameters
// (M 16, EfConstruction 200 — the same values NewHNSW substitutes per
// build) at State creation. NewState keeps m/efc verbatim, and the forest
// codec rejects m/efc of 0 on decode, so the State must carry the effective
// config from birth; every consumer of State.m/State.efc feeds NewHNSW,
// which normalizes identically, so this is behavior-neutral.
func vecDefaults(m, efc int) (int, int) {
	if m <= 0 {
		m = 16
	}
	if efc <= 0 {
		efc = 200
	}
	return m, efc
}

// vecIndex is one registered index: the declaration, the writer-side
// State (single-writer), and the published snapshot head concurrent
// readers load (spec D1). head updates at publish points and after
// folder packs - never mid-Add.
//
// Read-only mounts (Graph.Clone, ruling #5): a mounted index carries
// decl+seed+head with st nil from birth — no writer State exists, so no
// write path can touch the source through it. st is assigned only at
// construction (or whole-swapped by mountVecForest, never nil on either
// side of the swap), so an unlocked nil check races nothing: nil today
// means nil forever.
type vecIndex struct {
	decl VectorIndexDecl
	seed int64 // State's seed, for the folder's lock-free builds
	// mu serializes State access between the writer goroutine (Add/Delete
	// intake) and the folder lane (drain/publish). Uncontended on the
	// writer hot path; readers never take it (they load head).
	mu   sync.Mutex
	st   *vector.State
	head atomic.Pointer[vector.Snapshot]
}

func (vi *vecIndex) publish() {
	vi.mu.Lock()
	defer vi.mu.Unlock()
	if vi.st == nil { // read-only mount: the installed head IS the state
		return
	}
	vi.head.Store(vi.st.Snapshot()) // store under the lock: a delayed
	// unlock-then-store could swap an older snapshot over a newer one
}

// drain packs up to maxChunks overlay chunks and swaps a fresh snapshot
// head per chunk batch; returns true when more backlog remains. Runs on
// a folder goroutine; the writer's intake contends only on mu.
func (vi *vecIndex) drain(maxChunks int) bool {
	if vi.st == nil { // read-only mount: no backlog, nothing to publish
		return false
	}
	for c := 0; c < maxChunks; c++ {
		vi.mu.Lock()
		plan := vi.st.BeginPack()
		vi.mu.Unlock()
		if plan == nil {
			break
		}
		// Lock-free build on the folder goroutine: the writer's intake
		// only contends on the short capture/commit windows (spec D5 -
		// an 8-chunk hold here once stalled writes for whole HNSW builds).
		metric, _ := vecMetric(vi.decl.Metric)
		plan.Build(metric, vi.decl.M, vi.decl.EfConstruction, vi.seed)
		vi.mu.Lock()
		ok := vi.st.CommitPack(plan)
		vi.mu.Unlock()
		if !ok {
			continue // head moved (cannot happen today; the writer never
			// removes chunks) - retry with fresh inputs
		}
	}
	vi.mu.Lock()
	defer vi.mu.Unlock()
	vi.head.Store(vi.st.Snapshot())
	return vi.st.FullChunkPending()
}

// overlayFull reports the pack trigger (folder lane wake signal).
func (vi *vecIndex) overlayFull() bool {
	if vi.st == nil { // read-only mount: never pressurizes the folder lane
		return false
	}
	vi.mu.Lock()
	defer vi.mu.Unlock()
	return vi.st.FullChunkPending()
}

// size is the adaptive-ef input, taken under the lock (an unlocked
// State read races the folder drain and the writer intake - verified
// by -race, audit 2026-10-20).
func (vi *vecIndex) size() int {
	vi.mu.Lock()
	defer vi.mu.Unlock()
	if vi.st == nil { // read-only mount: size from the published head
		if s := vi.head.Load(); s != nil {
			return s.Len()
		}
		return 0
	}
	return vi.st.Len()
}

var (
	ErrVectorIndexExists   = errors.New("vectra: vector index name already declared")
	ErrVectorIndexNotFound = errors.New("vectra: vector index not found")
)

// CreateVectorIndex declares and builds a vector index (spec §9.1).
// Existing nodes' Prop values are ingested on the spot (one union scan);
// payloads that fail the intake rules (wrong dim, non-numeric, NaN/Inf)
// are skipped and reported through Options.OnVecSkip - never an error
// (spec §13-2). The declaration is WAL-logged log-first as a full-list
// IndexDecl frame: replay re-declares the list, drops are absences.
func (g *Graph) CreateVectorIndex(d VectorIndexDecl) error {
	if g.closed.Load() {
		return ErrClosed
	}
	if err := g.checkWriter(); err != nil {
		return err
	}
	if g.batching {
		// Mid-batch backfill would scan a pre-publish view and miss this
		// batch's own nodes (replay applies decl frames BETWEEN changesets,
		// never inside one) - reject instead of diverging (audit 2026-10-20).
		return errors.New("vectra: CreateVectorIndex inside Batch is not supported")
	}
	if d.Name == "" || d.Prop == "" {
		return errors.New("vectra: vector index name and prop are required")
	}
	if d.Dim < 1 || d.Dim > 4096 {
		return errors.New("vectra: vector index dim must be 1..4096")
	}
	metric, ok := vecMetric(d.Metric)
	if !ok {
		return errors.New("vectra: vector index metric must be cosine, l2, or dot")
	}
	g.flushPend()
	for _, vi := range g.vecReg() {
		if vi.decl.Name == d.Name {
			return ErrVectorIndexExists
		}
	}
	seed := vecSeed(d.Name)
	m, efc := vecDefaults(d.M, d.EfConstruction)
	vi := &vecIndex{
		decl: d,
		seed: seed,
		st:   vector.NewState(metric, d.Dim, m, efc, seed),
	}
	g.setVecRegistry(append(g.vecRegistryCopy(), vi))
	g.recordVecDecls()
	g.backfillVectorIndex(vi)
	vi.publish()
	return nil
}

// DropVectorIndex removes the declaration and tears the index down. The
// WAL frame re-declares the surviving set; replay drops by absence.
func (g *Graph) DropVectorIndex(name string) error {
	if g.closed.Load() {
		return ErrClosed
	}
	if err := g.checkWriter(); err != nil {
		return err
	}
	if g.batching {
		return errors.New("vectra: DropVectorIndex inside Batch is not supported")
	}
	g.flushPend()
	reg := g.vecRegistryCopy()
	for i, vi := range reg {
		if vi.decl.Name == name {
			reg = append(reg[:i:i], reg[i+1:]...)
			g.setVecRegistry(reg)
			g.recordVecDecls()
			return nil
		}
	}
	return ErrVectorIndexNotFound
}

// VectorIndexes returns the declarations (flushed at entry, like every
// other read API; lock-free off the atomic registry).
func (g *Graph) VectorIndexes() []VectorIndexDecl {
	g.flushPend()
	reg := g.vecReg()
	out := make([]VectorIndexDecl, 0, len(reg))
	for _, vi := range reg {
		out = append(out, vi.decl)
	}
	return out
}

// vecReg returns the current registry, lazily installing the empty
// registry on first touch (every Graph constructor path included).
func (g *Graph) vecReg() []*vecIndex {
	if p := g.vecIdx.Load(); p != nil {
		return *p
	}
	empty := []*vecIndex{}
	if g.vecIdx.CompareAndSwap(nil, &empty) {
		return empty
	}
	return *g.vecIdx.Load()
}

// vecRegistryCopy snapshots the registry for writer-side mutation.
func (g *Graph) vecRegistryCopy() []*vecIndex {
	return append([]*vecIndex(nil), g.vecReg()...)
}

func (g *Graph) setVecRegistry(reg []*vecIndex) { g.vecIdx.Store(&reg) }

// vecPropDecls snapshots the registry as a vecbin.PropDecls (prop name →
// declared dim) for the WAL's v2 frame selection. Built fresh at every
// logging call — never cached: declarations can change between batches.
// The dim is nominal (Hoist records what each value actually carries);
// nil for an empty registry so plain graphs pay no allocation and
// AppendHoisted falls straight through to the v1 frame.
func (g *Graph) vecPropDecls() vecbin.PropDecls {
	reg := g.vecReg()
	if len(reg) == 0 {
		return nil
	}
	decls := make(vecbin.PropDecls, len(reg))
	for _, vi := range reg {
		decls[vi.decl.Prop] = vi.decl.Dim
	}
	return decls
}

func (g *Graph) vecIndexByName(name string) *vecIndex {
	for _, vi := range g.vecReg() {
		if vi.decl.Name == name {
			return vi
		}
	}
	return nil
}

// recordVecDecls logs the full current declaration list as one IndexDecl
// frame at the current op position (log-first; flushPending coalesces it
// with adjacent declaration ops).
func (g *Graph) recordVecDecls() {
	if g.wal == nil {
		return
	}
	reg := g.vecReg()
	decls := make([]wal.VectorIndexDecl, 0, len(reg))
	for _, vi := range reg {
		decls = append(decls, vi.decl.toWal())
	}
	g.pendOps = append(g.pendOps, pendOp{kind: opVecIndex, vecDecls: decls})
	_ = g.maybeFlush()
}

// backfillVectorIndex ingests every existing node's declared prop through
// the intake rules. One union scan per Create; recovery (Load/Open) reuses
// it for the store-loaded base. Suppressed while Open's forest gate is
// deciding (vecSkipBackfill): a mounted cache replaces these states whole,
// and a failed gate re-runs this with the flag back down.
func (g *Graph) backfillVectorIndex(vi *vecIndex) {
	if g.vecSkipBackfill {
		return
	}
	nodes, _, _, _ := g.unionRecords()
	for _, n := range nodes {
		if v, ok := n.Props[vi.decl.Prop]; ok {
			if fv, ok := vector.NormalizeVec(v, vi.decl.Dim); ok {
				vi.mu.Lock()
				vi.st.Add(n.Key, fv)
				vi.mu.Unlock()
			} else if g.onVecSkip != nil {
				g.onVecSkip(n.Key, vi.decl.Name)
			}
		}
	}
}

// vecSeed derives a stable per-index HNSW seed from the name: same index
// name across loads rebuilds the same graph shape for a same op stream.
func vecSeed(name string) int64 {
	var h int64 = 1469598103934665603
	for i := 0; i < len(name); i++ {
		h ^= int64(name[i])
		h *= 1099511628211
	}
	return h
}

// vecIntakeNode feeds one node's effective props to every index (spec
// 5.1): valid vector -> Add; prop absent or invalid -> Delete (overwrite
// to a non-vector kills the old embedding, spec 4.4; Delete of a
// never-indexed key is a no-op). Runs on the writer thread inside
// AddNode/applyChangeset - replay parity holds because applyChangeset
// replays through AddNode itself.
func (g *Graph) vecIntakeNode(key string, props map[string]any) {
	for _, vi := range g.vecReg() {
		if vi.st == nil { // read-only mount (Clone): writes never touch it
			continue
		}
		if v, ok := props[vi.decl.Prop]; ok {
			if fv, ok := vector.NormalizeVec(v, vi.decl.Dim); ok {
				vi.mu.Lock()
				vi.st.Add(key, fv)
				vi.mu.Unlock()
				continue
			}
			if g.onVecSkip != nil {
				g.onVecSkip(key, vi.decl.Name)
			}
		}
		vi.mu.Lock()
		vi.st.Delete(key)
		vi.mu.Unlock()
	}
}

// vecIntakeDelete tombstones the key in every index (node delete).
func (g *Graph) vecIntakeDelete(key string) {
	for _, vi := range g.vecReg() {
		if vi.st == nil { // read-only mount (Clone): writes never touch it
			continue
		}
		vi.mu.Lock()
		vi.st.Delete(key)
		vi.mu.Unlock()
	}
}

// vecPublish swaps fresh snapshots into every index head. Called at
// generation publish points (publishLayer) - per batch boundary, never
// per Add (spec 5.3: the writer path only appends).
func (g *Graph) vecPublish() {
	for _, vi := range g.vecReg() {
		vi.publish()
	}
}

// applyVecDecls reconciles the registry to a full declaration list
// (IndexDecl replay and load). Names absent from the list drop; a kept
// name whose declaration CHANGED (drop+recreate in one frame window)
// rebuilds from the recorded parameters - a name-only match would keep
// the stale State (audit 2026-10-20). Duplicate names in one list take
// the last parameters, first position. Replay runs with recording
// detached (g.wal nil), so recordVecDecls no-ops there.
func (g *Graph) applyVecDecls(list []wal.VectorIndexDecl) {
	uniq := make([]wal.VectorIndexDecl, 0, len(list))
	pos := make(map[string]int, len(list))
	for _, w := range list {
		if i, ok := pos[w.Name]; ok {
			uniq[i] = w
			continue
		}
		pos[w.Name] = len(uniq)
		uniq = append(uniq, w)
	}
	want := make(map[string]wal.VectorIndexDecl, len(uniq))
	for _, w := range uniq {
		want[w.Name] = w
	}
	kept := make([]*vecIndex, 0, len(uniq))
	for _, vi := range g.vecReg() {
		w, ok := want[vi.decl.Name]
		if !ok || vi.decl != vecDeclFromWal(w) {
			continue // dropped, or recreated with new parameters
		}
		kept = append(kept, vi)
	}
	have := make(map[string]bool, len(kept))
	for _, vi := range kept {
		have[vi.decl.Name] = true
	}
	var fresh []*vecIndex
	for _, w := range uniq {
		if have[w.Name] {
			continue
		}
		d := vecDeclFromWal(w)
		metric, ok := vecMetric(d.Metric)
		if !ok {
			continue // malformed replay entry: skip, never crash recovery
		}
		seed := vecSeed(d.Name)
		m, efc := vecDefaults(d.M, d.EfConstruction)
		nv := &vecIndex{decl: d, seed: seed, st: vector.NewState(metric, d.Dim, m, efc, seed)}
		kept = append(kept, nv)
		fresh = append(fresh, nv)
	}
	g.setVecRegistry(kept)
	g.recordVecDecls()
	// Replay parity: a decl frame at op position P must see every node
	// written before P (live CreateVectorIndex backfills the same way).
	for _, vi := range fresh {
		g.backfillVectorIndex(vi)
		vi.publish()
	}
}

// QueryVectorOptions tunes one top-k query (spec §9.1). K defaults to 10
// (hard upper bound callers enforce themselves); Ef 0 selects the
// adaptive default max(64, c·log2 n) (spec §4.3); Label applies the
// post-filter with 3x oversampling (spec §13-1: pre-filter is a
// non-goal - callers widen K instead).
type QueryVectorOptions struct {
	K     int
	Ef    int
	Label string
}

// VectorHit is one approximate-nearest-neighbor result (spec §3.2).
// Results are approximate and their order is not stable across calls
// (spec D8/§4.5).
type VectorHit struct {
	Key   string
	Score float64
}

// QueryVector returns up to K approximate nearest keys for vec in the
// named index (spec §9.1). Direct mode flushes at entry first
// (read-your-write, spec D7); wrapper mode reads the last published
// generation, matching every other query API. Zero-lock on the read
// path: the snapshot head plus a pooled per-goroutine Reader.
func (g *Graph) QueryVector(index string, vec []float32, o QueryVectorOptions) ([]VectorHit, error) {
	vi := g.vecIndexByName(index)
	if vi == nil {
		return nil, ErrVectorIndexNotFound
	}
	if len(vec) != vi.decl.Dim {
		return nil, errors.New("vectra: query vector dim does not match the index declaration")
	}
	g.flushPend() // read-your-write (direct mode); no-op cost in wrapper mode
	k := o.K
	if k <= 0 {
		k = 10
	}
	ef := o.Ef
	if ef < k {
		ef = vector.AdaptiveEf(k, vi.size(), vector.DefaultC)
	}
	snap := vi.head.Load()
	if snap == nil {
		vi.publish()
		snap = vi.head.Load()
	}
	r := g.vecReader()
	defer g.putVecReader(r)
	fetch := k
	if o.Label != "" {
		fetch = 3 * k // oversample for the post-filter (spec 13-1)
		if ef < fetch {
			ef = fetch
		}
	}
	hits := snap.Search(vec, fetch, ef, r)
	out := make([]VectorHit, 0, k)
	view := g.view.Load()
	for _, h := range hits {
		if o.Label != "" {
			n, ok := view.nodeWinner(h.Key)
			if !ok || n.Label != o.Label {
				continue
			}
		}
		out = append(out, VectorHit(h))
		if len(out) == k {
			break
		}
	}
	return out, nil
}

// Pooled per-goroutine search readers: buffers are index-agnostic
// scratch (they grow to the largest segment seen), so one pool serves
// every index on the graph.
func (g *Graph) vecReader() *vector.Reader {
	if r, ok := g.vecReaders.Get().(*vector.Reader); ok && r != nil {
		return r
	}
	return &vector.Reader{}
}

func (g *Graph) putVecReader(r *vector.Reader) { g.vecReaders.Put(r) }

// runVecDrain is the direct-mode folder lane's vector stage: drain every
// index's overlay in bounded chunks (A6-style time share - the graph
// fold below still gets its turn), re-arming the wake while backlog
// remains. Pure State work under each index's mutex; heads swap per
// batch so partial progress is queryable (spec 7.1).
func (g *Graph) runVecDrain() {
	const chunksPerWake = 8
	rearm := false
	for _, vi := range g.vecReg() {
		if vi.drain(chunksPerWake) { // ONE bounded pass per wake: the
			// re-signal drives continuation, and the graph fold keeps
			// its turn (audit 2026-10-20 - the looped version held the
			// lane for the whole backlog)
			rearm = true
		}
	}
	if rearm && !g.closed.Load() {
		g.vecKick()
	}
}

// vecKick signals the folder lane (non-blocking): overlay pressure or a
// recovery rebuild. Follows the folder's arming protocol - start the
// goroutine if none is running, else just wake it; the loop's idle-exit
// re-check makes a signal in the gap never lost. Writer-path and
// Load/Open only.
func (g *Graph) vecKick() {
	if g.folderRunning.CompareAndSwap(false, true) {
		go g.foldLoop()
	}
	select {
	case g.foldWake <- struct{}{}:
	default:
	}
}

// vecOverlayPressure reports whether any index needs packing.
func (g *Graph) vecOverlayPressure() bool {
	for _, vi := range g.vecReg() {
		if vi.overlayFull() {
			return true
		}
	}
	return false
}

// VecHandle is the folder-lane surface of one index for the wrapper-mode
// writer (spec D10): drain packs overlay chunks and swaps the snapshot
// head; the return reports remaining backlog.
type VecHandle struct {
	vi *vecIndex
}

func (v *VecHandle) Drain(maxChunks int) bool { return v.vi.drain(maxChunks) }

// VectorHandles exposes the folder-lane handles (wrapper merger use).
func (g *Graph) VectorHandles() []*VecHandle {
	reg := g.vecReg()
	out := make([]*VecHandle, 0, len(reg))
	for _, vi := range reg {
		out = append(out, &VecHandle{vi: vi})
	}
	return out
}

// VectorOverlayPressure reports whether any index's overlay reached the
// pack size (the wrapper's afterBatch trigger).
func (g *Graph) VectorOverlayPressure() bool { return g.vecOverlayPressure() }

// VectorOverlayLen reports one index's pending entry count (folder-lane
// observability for tests and operators).
func (g *Graph) VectorOverlayLen(name string) int {
	if vi := g.vecIndexByName(name); vi != nil {
		vi.mu.Lock()
		defer vi.mu.Unlock()
		if vi.st == nil {
			return 0 // read-only mount: no overlay, no backlog
		}
		return vi.st.OverlayLen()
	}
	return -1
}

// VectorForNode returns node key's normalized vector under index's
// declared property (nil when the node lacks a valid vector). Serves
// similar_to-style queries without exposing the intake layer.
func (g *Graph) VectorForNode(index, key string) ([]float32, bool) {
	g.flushPend() // read-your-write, matching NodeByKey/QueryVector
	vi := g.vecIndexByName(index)
	if vi == nil {
		return nil, false
	}
	n, ok := g.view.Load().nodeWinner(key)
	if !ok {
		return nil, false
	}
	return vector.NormalizeVec(n.Props[vi.decl.Prop], vi.decl.Dim)
}

// ---- vectors.vec forest cache (Phase 2, spec §2.2-2.3) ------------------------
//
// One cache file per library directory, written inside the checkpoint batch
// (after the snapshot and the manifest) and mounted by Open behind the
// watermark gate. The file is a pure cache: declarations live in the store
// snapshot and the WAL, so any gate failure discards it and Open takes the
// ordinary rebuild path. Format ("VCVF1"):
//
//	magic "VCVF1" | u64 checkpointLSN | u32 declCount
//	  per decl:  u32 jsonLen | wal.VectorIndexDecl JSON | u64 seed
//	  per index: u32 blobLen | internal/vector "VCS1" State blob
//	u32 CRC32 (IEEE) over everything before it
//
// The per-State CRCs stay nested inside their blobs — harmless double
// integrity, and the wrapper stays free of State internals.

const (
	vecForestName  = "vectors.vec"
	vecForestMagic = "VCVF1"
)

// vecForestPath is the cache location: one file per database directory,
// next to the checkpoint snapshots (spec §2.2).
func vecForestPath(dbPath string) string {
	return filepath.Join(filepath.Dir(dbPath), vecForestName)
}

func vecForestPutU32(w *bytes.Buffer, v uint32) {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	w.Write(b[:])
}

func vecForestPutU64(w *bytes.Buffer, v uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	w.Write(b[:])
}

// encodeVecForest renders the registry as one cache image. State capture
// runs under each index's mutex: a checkpoint must never tear a State, and
// interleaving with the folder lane is safe — BeginPack leaves the chunk in
// the overlay until CommitPack swaps the built segment in, so every
// interleaving is a complete ≤LSN image (overlay or segment, never both
// missing).
func (g *Graph) encodeVecForest(lsn uint64) ([]byte, error) {
	reg := g.vecReg()
	blobs := make([][]byte, len(reg))
	for i, vi := range reg {
		var b bytes.Buffer
		vi.mu.Lock()
		err := vi.st.Encode(&b)
		vi.mu.Unlock()
		if err != nil {
			return nil, err
		}
		blobs[i] = b.Bytes()
	}
	var out bytes.Buffer
	out.WriteString(vecForestMagic)
	vecForestPutU64(&out, lsn)
	vecForestPutU32(&out, uint32(len(reg)))
	for _, vi := range reg {
		decl, err := json.Marshal(vi.decl.toWal())
		if err != nil {
			return nil, err
		}
		vecForestPutU32(&out, uint32(len(decl)))
		out.Write(decl)
		vecForestPutU64(&out, uint64(vi.seed))
	}
	for _, b := range blobs {
		vecForestPutU32(&out, uint32(len(b)))
		out.Write(b)
	}
	vecForestPutU32(&out, crc32.ChecksumIEEE(out.Bytes()))
	return out.Bytes(), nil
}

// writeVecForest persists the cache as part of the checkpoint batch: temp +
// fsync + rename + directory fsync, SaveFile's atomicity discipline. An
// index-less graph removes a stale cache instead — the load gate would
// discard it anyway (empty declaration set vs the file's), but removing
// keeps reopen cheap.
func (g *Graph) writeVecForest(lsn uint64) error {
	path := vecForestPath(g.dbPath)
	reg := g.vecReg()
	if len(reg) == 0 {
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		return nil
	}
	for _, vi := range reg {
		if vi.st == nil {
			// A read-only mount has no writer State to encode: a Snapshot
			// image would decode with an empty vid space (vidOf/nextVID are
			// State-private), and every later Add would resurrect duplicate
			// live vids. Skip the cache. Unreachable today — only
			// WAL-attached graphs checkpoint, and WALs attach at Open, never
			// on a clone — kept as a hard guard over the mount's lifetime.
			return nil
		}
	}
	b, err := g.encodeVecForest(lsn)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
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
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	// Best-effort directory fsync, as in SaveFile: the rename is not durable
	// everywhere until the parent directory is.
	if d, err := os.Open(filepath.Dir(path)); err == nil {
		d.Sync()
		d.Close()
	}
	return nil
}

var errVecForestCorrupt = errors.New("vectra: corrupt vectors.vec")

// vecForestReader is a bounds-checked cursor over the cache body; a sticky
// error turns every later read into a no-op, keeping the parse flat.
type vecForestReader struct {
	b   []byte
	i   int
	err error
}

func (r *vecForestReader) take(n int) []byte {
	if r.err != nil || n < 0 || len(r.b)-r.i < n {
		if r.err == nil {
			r.err = errVecForestCorrupt
		}
		return nil
	}
	p := r.b[r.i : r.i+n]
	r.i += n
	return p
}

func (r *vecForestReader) u32() uint32 {
	if p := r.take(4); p != nil {
		return binary.LittleEndian.Uint32(p)
	}
	return 0
}

func (r *vecForestReader) u64() uint64 {
	if p := r.take(8); p != nil {
		return binary.LittleEndian.Uint64(p)
	}
	return 0
}

func (r *vecForestReader) remain() int { return len(r.b) - r.i }

// vecForestEntry is one decoded declaration section: the wire declaration
// plus the HNSW seed the index ran with (name-derived; stored and compared
// anyway, so a renamed-and-recreated index never mounts stale sections).
type vecForestEntry struct {
	decl wal.VectorIndexDecl
	seed int64
}

// vecForestImage is one parsed cache file; blobs[i] belongs to entries[i].
type vecForestImage struct {
	path    string
	lsn     uint64
	entries []vecForestEntry
	blobs   [][]byte
}

// readVecForest loads and CRC-verifies the cache at path. A missing file is
// (nil, nil) — no cache, the ordinary rebuild path. Every other failure is
// reported as an error the caller treats as "discard and rebuild".
func readVecForest(path string) (*vecForestImage, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	// magic + lsn + declCount + at least one zero-length section + trailer
	if len(raw) < len(vecForestMagic)+8+4+4+4 {
		return nil, errVecForestCorrupt
	}
	if want := binary.LittleEndian.Uint32(raw[len(raw)-4:]); crc32.ChecksumIEEE(raw[:len(raw)-4]) != want {
		return nil, errVecForestCorrupt
	}
	rd := &vecForestReader{b: raw[:len(raw)-4]}
	if string(rd.take(len(vecForestMagic))) != vecForestMagic {
		return nil, errVecForestCorrupt
	}
	img := &vecForestImage{path: path, lsn: rd.u64()}
	n := int(rd.u32())
	// Forged-count guard: a declaration section carries at least a 4-byte
	// JSON length + 8 bytes of seed, and its index a 4-byte blob length.
	if int64(n)*16 > int64(rd.remain()) {
		return nil, errVecForestCorrupt
	}
	img.entries = make([]vecForestEntry, n)
	for i := range img.entries {
		dl := int(rd.u32())
		db := rd.take(dl)
		if err := json.Unmarshal(db, &img.entries[i].decl); err != nil {
			return nil, errVecForestCorrupt
		}
		img.entries[i].seed = int64(rd.u64())
	}
	img.blobs = make([][]byte, n)
	for i := range img.blobs {
		bl := int(rd.u32())
		img.blobs[i] = rd.take(bl)
	}
	if rd.err != nil || rd.remain() != 0 {
		return nil, errVecForestCorrupt
	}
	return img, nil
}

// discardVecForest removes an unusable cache file. Best effort: a removal
// failure only costs the next Open one wasted gate check — the gates
// discard the file again.
func discardVecForest(path string) { _ = os.Remove(path) }

// mountVecForest swaps the cache's decoded States into the registry,
// replacing the fresh empty ones applyVecDecls built, and publishes their
// snapshot heads. Every gate must hold (spec §2.3): the file's declaration
// set must equal the registry's EXACTLY — name, prop, dim, metric, m,
// efConstruction, and the stored seed — with one section per index and
// every section decodable. Declarations are authoritative in the store
// snapshot and the WAL; the file is a pure cache, so any mismatch removes
// it and returns false — the caller backfills from the snapshot instead
// (the rebuild path). The WAL tail replays after loadGraph returns, onto
// whichever states won, through the normal intake.
func (g *Graph) mountVecForest(img *vecForestImage) bool {
	reg := g.vecReg()
	if len(reg) == 0 || len(reg) != len(img.entries) {
		discardVecForest(img.path)
		return false
	}
	byName := make(map[string]int, len(img.entries))
	for i, e := range img.entries {
		byName[e.decl.Name] = i
	}
	// Decode everything before swapping anything in: a bad section must
	// leave the registry untouched for the caller's rebuild.
	states := make([]*vector.State, len(reg))
	for i, vi := range reg {
		j, ok := byName[vi.decl.Name]
		if !ok || vecDeclFromWal(img.entries[j].decl) != vi.decl || img.entries[j].seed != vi.seed {
			discardVecForest(img.path)
			return false
		}
		st, err := vector.DecodeState(bytes.NewReader(img.blobs[j]))
		if err != nil {
			discardVecForest(img.path)
			return false
		}
		states[i] = st
	}
	for i, vi := range reg {
		vi.mu.Lock()
		vi.st = states[i]
		vi.head.Store(states[i].Snapshot())
		vi.mu.Unlock()
	}
	return true
}
