// Snapshot I/O: Save/SaveFile/Load/LoadFile, byte-level export/import,
// engine-record graph construction, and deep Clone.

package vectra

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/projanvil/vectra/internal/atomicfile"
	"github.com/projanvil/vectra/internal/delta"
	"github.com/projanvil/vectra/internal/engine"
	"github.com/projanvil/vectra/internal/store"
	"github.com/projanvil/vectra/internal/wal"
)

// retainLegacyArtifact copies the pre-migration watermark snapshot aside as a
// rollback artifact — path -> path-v<ver>.bak in the same directory, via
// temp+fsync+rename (SaveFile discipline) — before the migration Seal. COPY,
// never rename: until the seal's WriteManifest lands the on-disk manifest
// still names `path`, so the original must survive every crash in that window
// (the checkpoint invariant above holds — old watermarks are deleted only
// after the new manifest lands; the seal's own cleanupSnapshots then removes
// the original, on success only). The .bak suffix fails cleanup's
// HasSuffix(".db") gate, so later seals spare it too, and recovery never sees
// it (Open loads only what the manifest names). A failed seal leaves the .bak
// as a harmless kept artifact — the next migration attempt overwrites it.
// Best effort, cleanupSnapshots discipline: copy errors are ignored (a
// missing artifact degrades rollback, never migration) and an empty path
// (nothing was loaded from a manifest) is a no-op.
func retainLegacyArtifact(path string, ver uint16) {
	if path == "" {
		return
	}
	bak := fmt.Sprintf("%s-v%d.bak", path, ver)
	in, err := os.Open(path)
	if err != nil {
		return
	}
	defer in.Close()
	_ = atomicfile.Write(bak, func(w io.Writer) error {
		_, err := io.Copy(w, in)
		return err
	})
}

// ---- snapshot I/O -------------------------------------------------------------

// Save writes the whole graph to w: unconditionally exports the current
// logical view (base ∪ layers, tombstones applied — merge-on-demand into a
// temporary record set, nothing published). Property-index declarations —
// effective and pending, typed and full-text alike — are persisted too
// (store v3/v7), so Load restores both the graph and its lookup indexes.
// The entry materializes the caller's absorption window first.
//
// Goroutine safety: with unique constraints declared, the declaration
// snapshot (uniqueStoreDecls) iterates the writer-private registry on the
// caller's goroutine, so the encode must run on the writer goroutine or
// with writes quiesced — Saving a live constrained graph from, say, a
// monitoring goroutine data-races. Unconstrained graphs keep the
// any-goroutine guarantee the pre-branch saveView doc stated (the view and
// the vector registry ride atomics).
func (g *Graph) Save(w io.Writer) error {
	g.flushPend()
	return g.saveView(w, g.view.Load(), g.uniqueStoreDecls())
}

// saveView serializes one (pinned) generation - Save's body without the
// pend flush. ucs is the unique-constraint declaration snapshot; callers on
// the writer goroutine pass g.uniqueStoreDecls(), the async-compact flight
// passes the copy pinned at launch. Safe from any goroutine only when ucs
// was pinned on the writer goroutine (the flight's launch discipline): the
// view and the declaration list are immutable and the vector-registry load
// is atomic; with a live registry read (Save/SaveFile's path) the encode
// inherits their writer-goroutine-or-quiesced requirement.
func (g *Graph) saveView(w io.Writer, v *genView, ucs []store.UniqueConstraint) error {
	nodes, edges, labels, indexKeys, ftsKeys := v.unionRecords()
	sn := make([]store.Node, len(nodes))
	for i, n := range nodes {
		sn[i] = store.Node{Key: n.Key, Label: n.Label, Props: n.Props}
	}
	se := make([]store.Edge, 0, len(edges))
	for _, e := range edges {
		se = append(se, store.Edge{
			SrcKey: nodes[e.Src].Key,
			DstKey: nodes[e.Dst].Key,
			Label:  labels[e.LabelID],
			Props:  e.Props,
		})
	}
	reg := g.vecReg()
	decls := make([]store.VectorIndexDecl, 0, len(reg))
	for _, vi := range reg {
		decls = append(decls, vecDeclToStore(vi.decl))
	}
	return store.Encode(w, store.Data{Nodes: sn, Edges: se, IndexKeys: indexKeys, VectorDecls: decls, FTSKeys: ftsKeys, UniqueConstraints: ucs})
}

// SaveFile saves to a path: temp file + fsync + atomic rename, avoiding
// half-written files. Goroutine safety as Save: with unique constraints
// declared the declaration snapshot reads the writer-private registry, so
// run on the writer goroutine or with writes quiesced; the async-compact
// flight uses saveViewFile with a launch-pinned snapshot instead.
func (g *Graph) SaveFile(path string) error {
	return g.saveViewFile(g.view.Load(), path, g.uniqueStoreDecls())
}

// saveViewFile is SaveFile over a pinned view (the async compact flight's
// writer-side export: no pend flush, no CURRENT-view load). ucs is the
// declaration snapshot pinned alongside the view at flight launch — the
// flight goroutine must not read the writer-private registry, which is
// exactly what makes this safe from any goroutine (see saveView).
func (g *Graph) saveViewFile(v *genView, path string, ucs []store.UniqueConstraint) error {
	// atomicfile.Write owns the full ritual: temp file in the same dir,
	// write, fsync, close, rename, best-effort dir fsync, temp cleanup on
	// every failure branch. Checkpoint manifests point at this file, so the
	// rename's durability discipline is the helper's, not the caller's.
	return atomicfile.Write(path, func(w io.Writer) error { return g.saveView(w, v, ucs) })
}

// Load restores a graph from a snapshot and merges it into the initial base
// (equivalent to a merge: the graph is immediately queryable AND writable).
// Index declarations persisted by the snapshot (store v3) are rebuilt, so
// LookupByProperty works immediately (v1/v2 files carry none).
func Load(r io.Reader) (*Graph, error) {
	return loadGraph(r)
}

func loadGraph(r io.Reader) (*Graph, error) {
	return loadGraphForest(r, nil)
}

// loadGraphForest is loadGraph with the forest-cache decision (Open's
// watermark path only; img nil everywhere else). img non-nil means the CRC
// gate and the stale-tolerant LSN gate (file LSN >= the manifest's compact
// boundary, v0.11.2) already passed in Open: the declaration backfill is
// suppressed while applyVecDecls builds the registry, then mountVecForest
// compares declaration sets and swaps the decoded States in. Any gate
// failure falls through to the ordinary backfill — the pre-existing rebuild
// path, unchanged. The WAL tail replays afterwards, on top of whichever
// states won, through the normal intake (vecIntakeNode); frames at or below
// the forest's own LSN re-apply idempotently (the verification note lives
// at Open's gate above).
func loadGraphForest(r io.Reader, img *vecForestImage) (*Graph, error) {
	data, err := store.Decode(r)
	if err != nil {
		return nil, err
	}
	nodes := make([]engine.NodeRec, 0, len(data.Nodes))
	idx := make(map[string]uint32, len(data.Nodes))
	for _, n := range data.Nodes {
		rec := engine.NodeRec{Key: n.Key, Label: n.Label, Props: n.Props}
		if i, ok := idx[n.Key]; ok { // duplicate keys: last write wins (AddNode semantics)
			nodes[i] = rec
			continue
		}
		idx[n.Key] = uint32(len(nodes))
		nodes = append(nodes, rec)
	}
	labels := make([]string, 0, 8)
	labID := make(map[string]uint32, 8)
	intern := func(name string) uint32 {
		if id, ok := labID[name]; ok {
			return id
		}
		id := uint32(len(labels))
		labID[name] = id
		labels = append(labels, name)
		return id
	}
	edges := make([]engine.EdgeRec, 0, len(data.Edges))
	for _, e := range data.Edges {
		s, okS := idx[e.SrcKey]
		d, okD := idx[e.DstKey]
		if !okS || !okD {
			return nil, fmt.Errorf("vectra: snapshot edge %s->%s references unknown node", e.SrcKey, e.DstKey)
		}
		edges = append(edges, engine.EdgeRec{
			Src: s, Dst: d, LabelID: intern(e.Label), Props: e.Props,
		})
	}
	g := graphFromEngine(nodes, edges, labels, data.IndexKeys, data.FTSKeys)
	if len(data.VectorDecls) > 0 {
		list := make([]wal.VectorIndexDecl, 0, len(data.VectorDecls))
		for _, d := range data.VectorDecls {
			list = append(list, wal.VectorIndexDecl{
				Name: d.Name, Prop: d.Prop, Dim: d.Dim, Metric: d.Metric,
				M: d.M, EfConstruction: d.EfConstruction,
			})
		}
		if img != nil {
			g.vecSkipBackfill = true // forest gate pending: empty states only
		}
		g.applyVecDecls(list)
		g.vecSkipBackfill = false
	}
	if len(data.UniqueConstraints) > 0 {
		// Post-construction registration, the applyVecDecls discipline: the
		// declarations ride the decoded store section (v8), not
		// graphFromEngine. Trust path (§3.4): full build, validation
		// skipped — the data was valid when persisted. loadGraphForest is
		// the decode funnel for Load/LoadFile/ImportSnapshot and Open's
		// snapshot path alike, so raft Restore carries constraints too.
		ucs := make([]wal.UniqueConstraint, 0, len(data.UniqueConstraints))
		for _, uc := range data.UniqueConstraints {
			ucs = append(ucs, wal.UniqueConstraint{Label: uc.Label, Prop: uc.Prop})
		}
		g.applyUniqueDecls(ucs)
	}
	if img != nil {
		if g.mountVecForest(img) {
			g.vecForestLoaded = true
		} else {
			// Rebuild: the registry states are fresh and empty (the backfill
			// was suppressed); populate them exactly as applyVecDecls would
			// have — the path a forest-less Open takes.
			for _, vi := range g.vecReg() {
				g.backfillVectorIndex(vi)
				vi.publish()
			}
		}
	}
	g.vecKick() // backfill or forest load left every vector in the overlay; drain (spec 7.1)
	return g, nil
}

// graphFromEngine builds a Graph whose initial generation is the merged base
// of the given records (writer mirrors and ratchets seeded from the base).
// ftsKeys seeds the base's FTS gram buckets (the FTS plan's load path).
func graphFromEngine(nodes []engine.NodeRec, edges []engine.EdgeRec, labels []string, indexKeys []string, ftsKeys []string) *Graph {
	snap := engine.Seal(nodes, edges, labels, indexKeys, ftsKeys)
	g := New()
	baseKeys := make(map[string]NodeID, len(nodes))
	g.nodes = make(map[string]nodeRef, len(nodes))
	for i, n := range nodes {
		baseKeys[n.Key] = NodeID(i)
		g.nodes[n.Key] = nodeRef{id: NodeID(i), live: true, label: n.Label, props: n.Props}
	}
	g.edges = make(map[delta.EdgeKey]edgeRef, len(edges))
	for i, er := range edges {
		k := delta.EdgeKey{SrcKey: nodes[er.Src].Key, DstKey: nodes[er.Dst].Key, Label: labels[er.LabelID]}
		ref := g.edges[k]
		ref.winner = EdgeID(i)
		ref.n++
		g.edges[k] = ref
	}
	g.nodeTotal, g.edgeTotal = len(nodes), len(edges)
	g.nextNode, g.nextEdge = NodeID(len(nodes)), EdgeID(len(edges))
	// Edge labels only — the label mirror feeds DeleteEdge's no-op early
	// exit and callers may pass a node-label superset (Clone's union).
	g.labels = make(map[string]bool, len(labels))
	for _, er := range edges {
		g.labels[labels[er.LabelID]] = true
	}
	g.view.Store(&genView{
		base: snap, baseKeys: baseKeys,
		nodes: g.nodeTotal, edges: g.edgeTotal,
	})
	return g
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

// Clone deep-copies into a graph that can keep taking writes: the union
// records (base ∪ layers) build the clone's fresh base, so the clone starts
// fully merged and writable. Props maps share references per the sharing
// contract (relies on "replace, don't mutate" discipline). Vector indexes
// mount read-only (ruling #5): each live index's declaration plus its
// currently published snapshot head ride along — never the writer State —
// so the clone answers QueryVector identically and stays frozen at the
// cloned generation no matter what the source writes next. The server
// module's hot-swap publisher uses it to clone the live graph (reads are
// served from published clones). The entry materializes the caller's
// absorption window first.
func (g *Graph) Clone() *Graph {
	g.flushPend()
	nodes, edges, labels, indexKeys, ftsKeys := g.unionRecords()
	c := graphFromEngine(nodes, edges, labels, indexKeys, ftsKeys)
	if reg := g.vecReg(); len(reg) > 0 {
		mounted := make([]*vecIndex, len(reg))
		for i, vi := range reg {
			h := vi.head.Load()
			if h == nil {
				// Index declared but not yet published (the CreateVectorIndex
				// window): publish on the spot — the same fallback a racing
				// QueryVector reader uses.
				vi.publish()
				h = vi.head.Load()
			}
			nv := &vecIndex{decl: vi.decl, seed: vi.seed} // st nil: read-only
			nv.head.Store(h)
			mounted[i] = nv
		}
		// Pointer-cheap by State.Snapshot's design: segments and frozen
		// chunks are shared, only the tail and dead bits were copied at the
		// source's publish. Isolation is the head pointer itself: later
		// source publishes swap the source's head, never the clone's.
		c.setVecRegistry(mounted)
	}
	if len(g.uniqueIdx) > 0 {
		// The unique-constraint registry rides along deep-copied (spec §3.5,
		// the setVecRegistry bug class): the clone's base IS the source's
		// live union, so an exact registry transfers exactly — and stays
		// isolated, the clone's registrations never leak back. A
		// stale-marked source rebuilds first, so the copy carries ground
		// truth, never the dropped window's phantoms.
		g.uniqueEnsureFresh()
		idx := make(map[UniqueConstraint]map[string]string, len(g.uniqueIdx))
		for uc, vals := range g.uniqueIdx {
			m := make(map[string]string, len(vals))
			for v, k := range vals {
				m[v] = k
			}
			idx[uc] = m
		}
		c.uniqueIdx = idx
	}
	return c
}

// ExportSnapshot serializes the current logical view (base ∪ layers union —
// export works with unmerged layers too; the server module's FSM snapshot
// uses it on the live graph). The format matches Save (CRC included).
func (g *Graph) ExportSnapshot() ([]byte, error) {
	var buf bytes.Buffer
	if err := g.Save(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ImportSnapshot builds a graph from snapshot bytes: the records merge into
// the initial base (immediately queryable and writable again). The server
// FSM's Restore path uses it to rebuild the live graph.
func ImportSnapshot(data []byte) (*Graph, error) {
	return loadGraph(bytes.NewReader(data))
}
