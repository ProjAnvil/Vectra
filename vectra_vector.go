// Vector index declarations and the per-Graph registry (vector Phase 1,
// spec §3.1/§9.1). The registry is copy-on-write behind an atomic pointer:
// reads (VectorIndexes, QueryVector's index lookup) never lock; mutations
// run on the writer thread only.

package vectra

import (
	"errors"
	"sync/atomic"

	"github.com/projanvil/vectra/internal/store"
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

// vecIndex is one registered index: the declaration, the writer-side
// State (single-writer), and the published snapshot head concurrent
// readers load (spec D1). head updates at publish points and after
// folder packs - never mid-Add.
type vecIndex struct {
	decl VectorIndexDecl
	st   *vector.State
	head atomic.Pointer[vector.Snapshot]
}

func (vi *vecIndex) publish() { vi.head.Store(vi.st.Snapshot()) }

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
	vi := &vecIndex{
		decl: d,
		st:   vector.NewState(metric, d.Dim, d.M, d.EfConstruction, vecSeed(d.Name)),
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
// the intake rules. One union scan per Create; recovery (Load/Open)
// reuses it for the store-loaded base.
func (g *Graph) backfillVectorIndex(vi *vecIndex) {
	nodes, _, _, _ := g.unionRecords()
	for _, n := range nodes {
		if v, ok := n.Props[vi.decl.Prop]; ok {
			if fv, ok := vector.NormalizeVec(v, vi.decl.Dim); ok {
				vi.st.Add(n.Key, fv)
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
		if v, ok := props[vi.decl.Prop]; ok {
			if fv, ok := vector.NormalizeVec(v, vi.decl.Dim); ok {
				vi.st.Add(key, fv)
				continue
			}
			if g.onVecSkip != nil {
				g.onVecSkip(key, vi.decl.Name)
			}
		}
		vi.st.Delete(key)
	}
}

// vecIntakeDelete tombstones the key in every index (node delete).
func (g *Graph) vecIntakeDelete(key string) {
	for _, vi := range g.vecReg() {
		vi.st.Delete(key)
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
		nv := &vecIndex{decl: d, st: vector.NewState(metric, d.Dim, d.M, d.EfConstruction, vecSeed(d.Name))}
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
		ef = vector.AdaptiveEf(k, vi.st.Len()+vi.st.OverlayLen(), vector.DefaultC)
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
	for _, h := range hits {
		if o.Label != "" {
			v := g.view.Load()
			n, ok := v.nodeWinner(h.Key)
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
