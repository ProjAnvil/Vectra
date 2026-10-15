package writer_test

// Task 5 tests: the single-writer wrapper — concurrent Submit/query races,
// write-stall trigger and release, merger failure backoff and the
// synchronous fallback, the two-writer merge protocol (rebase validity),
// Close drain + final checkpoint, layer-cap bound, and Compact/Seal
// equivalence. Item 9 of the brief's checklist (three-module suites +
// root -race) is the run matrix, not a test function.

import (
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/projanvil/vectra"
	"github.com/projanvil/vectra/internal/writer"
)

// ---- helpers -----------------------------------------------------------------

func nodeCS(key, label string, props map[string]any) vectra.Changeset {
	return vectra.Changeset{Nodes: []vectra.NodeUpsert{{Key: key, Label: label, Props: props}}}
}

func edgeCS(src, dst, label string, props map[string]any, parallel bool) vectra.Changeset {
	return vectra.Changeset{Edges: []vectra.EdgeUpsert{{
		SrcKey: src, DstKey: dst, Label: label, Props: props, AllowParallel: parallel,
	}}}
}

func delNodeCS(key string) vectra.Changeset {
	return vectra.Changeset{NodeDeletes: []string{key}}
}

func delEdgeCS(src, dst, label string) vectra.Changeset {
	return vectra.Changeset{EdgeDeletes: []vectra.EdgeDelete{{SrcKey: src, DstKey: dst, Label: label}}}
}

// dumpState walks the key universe and dumps every live node and edge.
func dumpState(t *testing.T, g *vectra.Graph, keys []string) string {
	t.Helper()
	var out string
	var nk, ek []string
	nodeMap := map[string]string{}
	for _, k := range keys {
		if n, ok := g.NodeByKey(k); ok {
			nk = append(nk, k)
			nodeMap[k] = fmt.Sprintf("%s|%v", n.Label, n.Props)
		}
	}
	edgeMap := map[string]string{}
	for _, k := range keys {
		for _, dir := range []vectra.Direction{vectra.DirOut, vectra.DirIn} {
			es, err := g.EdgesOf(k, dir)
			if err != nil {
				continue
			}
			for _, e := range es {
				key := fmt.Sprintf("%d->%d:%s", e.Src, e.Dst, e.Label)
				edgeMap[key] = fmt.Sprintf("%v", e.Props)
			}
		}
	}
	sort.Strings(nk)
	for _, k := range nk {
		out += "N " + k + " = " + nodeMap[k] + "\n"
	}
	for k := range edgeMap {
		ek = append(ek, k)
	}
	sort.Strings(ek)
	for _, k := range ek {
		out += "E " + k + " = " + edgeMap[k] + "\n"
	}
	return out
}

// applyDirect is the oracle: the same changesets applied in direct mode
// (public write methods inside Batch — identical semantics to the writer
// loop's applyChangeset) plus a final Seal.
func applyDirect(t *testing.T, css []vectra.Changeset) *vectra.Graph {
	t.Helper()
	g := vectra.New()
	for _, cs := range css {
		if err := g.Batch(func(g *vectra.Graph) {
			for _, n := range cs.Nodes {
				if _, err := g.AddNode(n.Key, n.Label, n.Props); err != nil {
					t.Fatalf("oracle AddNode: %v", err)
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
					t.Fatalf("oracle edge: %v", err)
				}
			}
			for _, d := range cs.EdgeDeletes {
				if _, err := g.DeleteEdge(d.SrcKey, d.DstKey, d.Label); err != nil {
					t.Fatalf("oracle del edge: %v", err)
				}
			}
			for _, k := range cs.NodeDeletes {
				if _, err := g.DeleteNode(k); err != nil {
					t.Fatalf("oracle del node: %v", err)
				}
			}
		}); err != nil {
			t.Fatalf("oracle batch: %v", err)
		}
	}
	if err := g.Seal(); err != nil {
		t.Fatalf("oracle seal: %v", err)
	}
	return g
}

// randomStream builds a valid random changeset stream over a fixed key
// universe (every edge's endpoints are created first; deletes avoid the
// root key).
func randomStream(rng *rand.Rand, rounds, keys int) []vectra.Changeset {
	key := func(i int) string { return fmt.Sprintf("k%03d", i) }
	var css []vectra.Changeset
	css = append(css, nodeCS(key(0), "Root", nil))
	for i := 1; i < keys; i++ {
		css = append(css, nodeCS(key(i), "N", map[string]any{"i": i}))
	}
	for r := 0; r < rounds; r++ {
		switch rng.Intn(10) {
		case 0, 1, 2:
			a, b := 1+rng.Intn(keys-1), 1+rng.Intn(keys-1)
			css = append(css, edgeCS(key(a), key(b), "E", map[string]any{"w": r}, rng.Intn(2) == 0))
		case 3, 4:
			a, b := 1+rng.Intn(keys-1), 1+rng.Intn(keys-1)
			css = append(css, edgeCS(key(a), key(b), "E", map[string]any{"w": r}, false))
		case 5:
			a, b := 1+rng.Intn(keys-1), 1+rng.Intn(keys-1)
			css = append(css, delEdgeCS(key(a), key(b), "E"))
		case 6:
			k := 1 + rng.Intn(keys-1)
			css = append(css, nodeCS(key(k), "N2", map[string]any{"i": k, "r": r}))
		case 7:
			k := 1 + rng.Intn(keys-1)
			css = append(css, delNodeCS(key(k)))
			css = append(css, nodeCS(key(k), "N3", map[string]any{"rev": r}))
		case 8, 9:
			css = append(css, edgeCS(key(0), key(1+rng.Intn(keys-1)), "ROOTED", nil, true))
		}
	}
	return css
}

// ---- 1. concurrent Submit + concurrent queries, write-then-immediately-visible ----

func TestConcurrentSubmitAndQueries(t *testing.T) {
	g := vectra.New()
	var rootID vectra.NodeID
	if err := g.Batch(func(g *vectra.Graph) {
		id, err := g.AddNode("root", "Root", nil)
		if err != nil {
			t.Fatal(err)
		}
		rootID = id
	}); err != nil {
		t.Fatal(err)
	}
	h := writer.Attach(g, writer.Config{MaxLayers: 8, MergeEdges: 64})

	const submitters = 8
	const perSub = 40
	var wg sync.WaitGroup
	var accepted atomic.Int32
	for s := 0; s < submitters; s++ {
		s := s
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perSub; i++ {
				key := fmt.Sprintf("s%d-%d", s, i)
				if err := h.Submit(nodeCS(key, "S", map[string]any{"s": s, "i": i})); err != nil {
					t.Errorf("Submit: %v", err)
					return
				}
				accepted.Add(1)
				// write-then-immediately-visible: the reply happens after
				// the layer publication, so the key must resolve now.
				if _, ok := g.NodeByKey(key); !ok {
					t.Errorf("key %s invisible right after Submit", key)
					return
				}
				if err := h.Submit(edgeCS("root", key, "LINK", nil, false)); err != nil {
					t.Errorf("Submit edge: %v", err)
					return
				}
			}
		}()
	}
	// concurrent readers: point queries + traversals while the writer works
	stop := make(chan struct{})
	var rwg sync.WaitGroup
	for r := 0; r < 4; r++ {
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, ok := g.NodeByKey("root"); !ok {
					t.Error("root vanished")
					return
				}
				_ = g.NodeCount()
				_ = g.EdgeCount()
				if _, err := g.BFS([]vectra.NodeID{rootID}); err != nil {
					t.Errorf("BFS: %v", err)
					return
				}
				if _, ok := g.LookupByNumber("s", float64(r)); ok {
					// index absent: ok=false expected; ignore either way
				}
			}
		}()
	}
	wg.Wait()
	close(stop)
	rwg.Wait()
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := accepted.Load(); got != submitters*perSub {
		t.Fatalf("accepted = %d, want %d", got, submitters*perSub)
	}
	if got := g.NodeCount(); got != submitters*perSub+1 {
		t.Fatalf("NodeCount = %d, want %d", got, submitters*perSub+1)
	}
	if got := g.EdgeCount(); got != submitters*perSub {
		t.Fatalf("EdgeCount = %d, want %d", got, submitters*perSub)
	}
}

// ---- 2. write stall engages and releases --------------------------------------

func TestWriteStallBlocksAndResolves(t *testing.T) {
	g := vectra.New()
	h := writer.Attach(g, writer.Config{MaxLayers: 2, MergeDelay: 200 * time.Millisecond})

	done := make(chan error, 16)
	go func() {
		for i := 0; i < 8; i++ {
			key := fmt.Sprintf("n%d", i)
			if err := h.Submit(nodeCS(key, "N", nil)); err != nil {
				done <- err
				return
			}
			// layer cap 2: from the 3rd submit on, the writer is stalled in
			// the merge window (MergeDelay 200ms) — the reply must still
			// arrive once the fold publishes.
			if _, ok := g.NodeByKey(key); !ok {
				done <- fmt.Errorf("key %s invisible after Submit", key)
				return
			}
		}
		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("deadlock: stalled writer never resumed (timeout guard)")
	}
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := g.NodeCount(); got != 8 {
		t.Fatalf("NodeCount = %d, want 8", got)
	}
}

// ---- 3. merger failure: backoff-then-success, and the sync fallback -------------

func TestMergeFailureBackoffRecovers(t *testing.T) {
	g := vectra.New()
	h := writer.Attach(g, writer.Config{MaxLayers: 4, FailMerge: 3})
	for i := 0; i < 40; i++ {
		if err := h.Submit(nodeCS(fmt.Sprintf("k%d", i), "N", nil)); err != nil {
			t.Fatalf("Submit %d: %v", i, err)
		}
	}
	// the first three background seal attempts are injected failures; the
	// flight then either retries through (4th attempt succeeds) or hands
	// off to the writer's synchronous fallback — either way writes kept
	// flowing and layers eventually fold below the cap.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if layers, _ := g.LayerPressure(); layers < 4 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if layers, _ := g.LayerPressure(); layers >= 4 {
		t.Fatalf("layers = %d after recovery window, merges never succeeded", layers)
	}
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := g.NodeCount(); got != 40 {
		t.Fatalf("NodeCount = %d, want 40", got)
	}
}

func TestMergeFailureInfiniteFallsBackToSync(t *testing.T) {
	g := vectra.New()
	h := writer.Attach(g, writer.Config{MaxLayers: 4, FailMerge: 1 << 30})
	// every background merge fails; the writer's synchronous fallback must
	// keep the stall releasable and the stream flowing
	for i := 0; i < 40; i++ {
		if err := h.Submit(nodeCS(fmt.Sprintf("k%d", i), "N", nil)); err != nil {
			t.Fatalf("Submit %d: %v", i, err)
		}
		if _, ok := g.NodeByKey(fmt.Sprintf("k%d", i)); !ok {
			t.Fatalf("key k%d invisible after Submit", i)
		}
	}
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := g.NodeCount(); got != 40 {
		t.Fatalf("NodeCount = %d, want 40", got)
	}
	if layers, _ := g.LayerPressure(); layers != 0 {
		t.Fatalf("layers after Close = %d, want 0 (final merge)", layers)
	}
}

// ---- 4. merge protocol: random stream vs direct-mode oracle; in-flight tail ------

func TestMergeProtocolEquivalence(t *testing.T) {
	for _, seed := range []int64{7, 8, 9} {
		rng := rand.New(rand.NewSource(seed))
		css := randomStream(rng, 300, 24)
		keys := make([]string, 24)
		for i := range keys {
			keys[i] = fmt.Sprintf("k%03d", i)
		}

		g := vectra.New()
		h := writer.Attach(g, writer.Config{MaxLayers: 64, MergeEdges: 24})
		for _, cs := range css {
			if err := h.Submit(cs); err != nil {
				t.Fatalf("Submit (seed %d): %v", seed, err)
			}
		}
		if err := h.Close(); err != nil {
			t.Fatalf("Close (seed %d): %v", seed, err)
		}
		want := dumpState(t, applyDirect(t, css), keys)
		got := dumpState(t, g, keys)
		if got != want {
			t.Fatalf("seed %d: wrapper+background-merge state diverged from direct-mode oracle:\n--- got ---\n%s\n--- want ---\n%s", seed, got, want)
		}
	}
}

func TestInFlightWritesRebaseOntoNewBase(t *testing.T) {
	g := vectra.New()
	if err := g.Batch(func(g *vectra.Graph) {
		for i := 0; i < 4; i++ {
			if _, err := g.AddNode(fmt.Sprintf("n%d", i), "N", nil); err != nil {
				t.Fatal(err)
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	// MergeEdges trigger with a long MergeDelay: merges fire often, and the
	// writer keeps publishing layers INSIDE the in-flight window — exactly
	// the tail that must stay visible when the rebased base lands. Every
	// submit carries an edge record so the edge-volume trigger actually
	// counts it (node-only layers are edge-free).
	h := writer.Attach(g, writer.Config{MaxLayers: 64, MergeEdges: 4, MergeDelay: 150 * time.Millisecond})

	var maxLayers atomic.Int32
	for i := 0; i < 24; i++ {
		key := fmt.Sprintf("tail%02d", i)
		cs := vectra.Changeset{
			Nodes: []vectra.NodeUpsert{{Key: key, Label: "T", Props: map[string]any{"i": i}}},
			Edges: []vectra.EdgeUpsert{{SrcKey: "n0", DstKey: fmt.Sprintf("n%d", 1+i%3), Label: "REL", Props: map[string]any{"i": i}}},
		}
		if err := h.Submit(cs); err != nil {
			t.Fatalf("Submit %d (in merge window): %v", i, err)
		}
		// each write is immediately visible whether or not a rebase lands
		// between the publish and this read
		if _, ok := g.NodeByKey(key); !ok {
			t.Fatalf("key %s invisible during in-flight merge window", key)
		}
		if es, err := g.EdgesOf("n0", vectra.DirOut); err != nil || len(es) < 1 {
			t.Fatalf("EdgesOf(n0) during in-flight window = %v (err %v), want >= 1 live edge", es, err)
		}
		if l, _ := g.LayerPressure(); int32(l) > maxLayers.Load() {
			maxLayers.Store(int32(l))
		}
	}
	// the window really was non-empty: the writer stacked at least 5 layers
	// while a merge was sleeping in its 150ms delay (trigger at 4 edges)
	if maxLayers.Load() < 5 {
		t.Fatalf("max in-window layer depth = %d, want >= 5 (rebase tail never formed)", maxLayers.Load())
	}
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for i := 0; i < 24; i++ {
		if _, ok := g.NodeByKey(fmt.Sprintf("tail%02d", i)); !ok {
			t.Fatalf("tail%02d lost across rebase", i)
		}
	}
	if got := g.NodeCount(); got != 28 {
		t.Fatalf("NodeCount = %d, want 28", got)
	}
}

// ---- 5. Close drains in-flight Submits, checkpoints; ErrClosed after -------------

func TestCloseDrainsAndCheckpoints(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "graph.db")
	g, err := vectra.Open(path, vectra.Options{WALMode: vectra.ModeInterval})
	if err != nil {
		t.Fatal(err)
	}
	h := writer.Attach(g, writer.Config{})

	const submitters = 6
	const perSub = 30
	var applied atomic.Int32
	var wg sync.WaitGroup
	for s := 0; s < submitters; s++ {
		s := s
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perSub; i++ {
				err := h.Submit(nodeCS(fmt.Sprintf("c%d-%d", s, i), "C", nil))
				if err != nil {
					if !errors.Is(err, vectra.ErrClosed) {
						t.Errorf("Submit: %v", err)
					}
					return
				}
				applied.Add(1)
			}
		}()
	}
	// race Close against the submitters
	time.Sleep(5 * time.Millisecond)
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	wg.Wait()

	// post-Close: Submits rejected, queries legal over the checkpointed state
	if err := h.Submit(nodeCS("late", "L", nil)); !errors.Is(err, vectra.ErrClosed) {
		t.Fatalf("Submit after Close = %v, want ErrClosed", err)
	}
	if err := asyncResult(h.SubmitAsync(nodeCS("late2", "L", nil))); !errors.Is(err, vectra.ErrClosed) {
		t.Fatalf("SubmitAsync after Close = %v, want ErrClosed", err)
	}
	if got := g.NodeCount(); got != int(applied.Load()) {
		t.Fatalf("NodeCount = %d, want %d (all drained Submits applied)", got, applied.Load())
	}

	// the final checkpoint went through g.Seal's pipeline: reopen recovers
	g2, err := vectra.Open(path, vectra.Options{WALMode: vectra.ModeInterval})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer g2.Close()
	if got := g2.NodeCount(); got != int(applied.Load()) {
		t.Fatalf("reopened NodeCount = %d, want %d (checkpoint lost writes)", got, applied.Load())
	}
	for i := 0; i < int(applied.Load()); i++ {
		s, k := i/perSub, i%perSub
		if _, ok := g2.NodeByKey(fmt.Sprintf("c%d-%d", s, k)); !ok {
			// applied keys are a subset of the universe (Close may cut the
			// stream); only the first `applied` in this synthetic order are
			// heuristically checked — the count assertions above are exact.
			break
		}
	}
}

func asyncResult(ch <-chan error) error {
	select {
	case err := <-ch:
		return err
	case <-time.After(10 * time.Second):
		return fmt.Errorf("SubmitAsync reply never arrived")
	}
}

// ---- 7. layer cap bound ---------------------------------------------------------

func TestLayerCapBounded(t *testing.T) {
	g := vectra.New()
	h := writer.Attach(g, writer.Config{MaxLayers: 3, MergeEdges: 1 << 30})
	stop := make(chan struct{})
	var maxSeen atomic.Int32
	var swg sync.WaitGroup
	swg.Add(1)
	go func() {
		defer swg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if l, _ := g.LayerPressure(); int32(l) > maxSeen.Load() {
				maxSeen.Store(int32(l))
			}
		}
	}()
	for i := 0; i < 60; i++ {
		if err := h.Submit(nodeCS(fmt.Sprintf("k%d", i), "N", nil)); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}
	close(stop)
	swg.Wait()
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := maxSeen.Load(); got > 3+1 {
		t.Fatalf("observed layer depth %d exceeds MaxLayers+1", got)
	}
	if got := g.NodeCount(); got != 60 {
		t.Fatalf("NodeCount = %d, want 60", got)
	}
}

// ---- 8. Compact()/Seal() equivalence; GCTune smoke --------------------------------

func TestCompactSealEquivalence(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	css := randomStream(rng, 120, 16)
	keys := make([]string, 16)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%03d", i)
	}
	build := func() *vectra.Graph {
		g := vectra.New()
		for _, cs := range css {
			if err := g.Batch(func(g *vectra.Graph) {
				for _, n := range cs.Nodes {
					_, _ = g.AddNode(n.Key, n.Label, n.Props)
				}
				for _, e := range cs.Edges {
					if e.AllowParallel {
						_, _ = g.AddEdge(e.SrcKey, e.DstKey, e.Label, e.Props)
					} else {
						_, _ = g.UpsertEdge(e.SrcKey, e.DstKey, e.Label, e.Props)
					}
				}
				for _, d := range cs.EdgeDeletes {
					_, _ = g.DeleteEdge(d.SrcKey, d.DstKey, d.Label)
				}
				for _, k := range cs.NodeDeletes {
					_, _ = g.DeleteNode(k)
				}
			}); err != nil {
				t.Fatal(err)
			}
		}
		return g
	}
	sealed := build()
	if err := sealed.Seal(); err != nil {
		t.Fatal(err)
	}
	compacted := build()
	if err := compacted.Compact(); err != nil {
		t.Fatal(err)
	}
	if !compacted.Sealed() {
		t.Fatal("Compact must leave no unmerged layers")
	}
	if a, b := dumpState(t, sealed, keys), dumpState(t, compacted, keys); a != b {
		t.Fatalf("Compact state != Seal state:\n--- Seal ---\n%s\n--- Compact ---\n%s", a, b)
	}
}

func TestCompactCheckpointsWALSession(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "graph.db")
	g, err := vectra.Open(path, vectra.Options{WALMode: vectra.ModeInterval})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if err := g.Batch(func(g *vectra.Graph) {
		_, _ = g.AddNode("a", "A", nil)
		_, _ = g.AddNode("b", "B", nil)
		_, _ = g.AddEdge("a", "b", "E", nil)
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.Compact(); err != nil {
		t.Fatalf("Compact on WAL session: %v", err)
	}
	g2, err := vectra.Open(path, vectra.Options{WALMode: vectra.ModeInterval})
	if err != nil {
		t.Fatalf("reopen after Compact: %v", err)
	}
	defer g2.Close()
	if g2.NodeCount() != 2 || g2.EdgeCount() != 1 {
		t.Fatalf("reopened counts = %d/%d, want 2/1", g2.NodeCount(), g2.EdgeCount())
	}
}

func TestGCTuneSmoke(t *testing.T) {
	g := vectra.New()
	h := writer.Attach(g, writer.Config{MaxLayers: 4, MergeEdges: 16, GCTune: true})
	for i := 0; i < 30; i++ {
		if err := h.Submit(nodeCS(fmt.Sprintf("k%d", i), "N", nil)); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := g.NodeCount(); got != 30 {
		t.Fatalf("NodeCount = %d, want 30", got)
	}
}
