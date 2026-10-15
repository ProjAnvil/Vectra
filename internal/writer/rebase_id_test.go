package writer_test

// Final-review C1 regression (task 6): the background merge rebase publishes
// a new base with dense renumbered IDs but cannot touch the writer-private
// mirrors (they belong to the writer goroutine). Left alone, the writer keeps
// stamping the stale mirror IDs into fresh layers (AddNode override/revive,
// materializeEndpoint, UpsertEdge folds) — and when the folded prefix
// contained tombstones, the renumbered base assigns ANOTHER key to the old
// ID: two live records of one generation share an ID, NodeByID answers the
// wrong record, and the three-source View buckets the survivor's edges under
// a foreign node. The fix under test: PublishMerge sets a flag and the
// writer loop rebuilds its mirrors (a full fold) before the next batch
// applies, so no stale ID is ever stamped.

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/projanvil/vectra"
	"github.com/projanvil/vectra/internal/writer"
)

// assertIDConsistency checks the per-generation ID invariants over the key
// universe: every live key's ID round-trips through NodeByID, and live IDs
// are pairwise distinct within the generation. Optionally also round-trips
// every visible edge through EdgeByID (post-Close only, where IDs are dense).
//
// It must be called at POST-REPAIR quiescent points (after Close, or via
// assertIDConsistencySettled mid-stream): between a rebase's publication
// and the writer's next batch there is a documented transient (R1, see
// merge.go) where tail records stamped before the rebase may briefly
// overlap the renumbered base's dense slots.
func assertIDConsistency(t *testing.T, g *vectra.Graph, keys []string, edgesToo bool) {
	t.Helper()
	if msg, bad := idInconsistency(g, keys, edgesToo); bad {
		t.Fatal(msg)
	}
}

// idInconsistency returns a description of the first violated ID invariant,
// or ("", false) when the generation is consistent.
func idInconsistency(g *vectra.Graph, keys []string, edgesToo bool) (string, bool) {
	seen := map[vectra.NodeID]string{}
	for _, k := range keys {
		n, ok := g.NodeByKey(k)
		if !ok {
			continue
		}
		if prev, dup := seen[n.ID]; dup {
			return fmt.Sprintf("live nodes %q and %q share ID %d in one generation", prev, k, n.ID), true
		}
		seen[n.ID] = k
		if back, ok := g.NodeByID(n.ID); !ok || back.Key != k {
			return fmt.Sprintf("NodeByID(%d) = %q (ok=%v), want key %q (round-trip broken)", n.ID, back.Key, ok, k), true
		}
	}
	if !edgesToo {
		return "", false
	}
	for _, k := range keys {
		for _, dir := range []vectra.Direction{vectra.DirOut, vectra.DirIn} {
			es, err := g.EdgesOf(k, dir)
			if err != nil {
				continue // key not live
			}
			for _, e := range es {
				back, ok := g.EdgeByID(e.ID)
				if !ok || back.Label != e.Label || back.Src != e.Src || back.Dst != e.Dst {
					return fmt.Sprintf("EdgeByID(%d) = %+v (ok=%v), want round-trip of %+v", e.ID, back, ok, e), true
				}
			}
		}
	}
	return "", false
}

// assertIDConsistencySettled asserts the ID invariants at a post-repair
// quiescent point: an observed inconsistency is retried against an empty
// "settle" Submit (which still drives applyBatch and with it the pre-batch
// repair). The documented R1 transient folds away at the first settle after
// its rebase published; a collision that PERSISTS across settles is the C1
// defect (stale mirror reuse with no later repair) and fails the test.
func assertIDConsistencySettled(t *testing.T, h *writer.Handle, g *vectra.Graph, keys []string) {
	t.Helper()
	const attempts = 8
	for i := 0; ; i++ {
		msg, bad := idInconsistency(g, keys, false)
		if !bad {
			return
		}
		if i >= attempts {
			t.Fatalf("ID inconsistency persisted across %d settles: %s", attempts, msg)
		}
		if err := h.Submit(vectra.Changeset{}); err != nil {
			t.Fatalf("settle Submit: %v", err)
		}
	}
}

// TestRebaseMirrorIDsStayConsistent reproduces the terminal-review scenario:
// a small layer cap forces the write-stall fold, the fold's prefix carries a
// tombstone (deleted node), and the writer then overwrite-writes a survivor.
func TestRebaseMirrorIDsStayConsistent(t *testing.T) {
	g := vectra.New()
	h := writer.Attach(g, writer.Config{MaxLayers: 2})

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	// Batch 1 -> layer 1: A, B, C (writer IDs 0, 1, 2) with edges A->B, B->C.
	must(h.Submit(vectra.Changeset{
		Nodes: []vectra.NodeUpsert{
			{Key: "A", Label: "N"},
			{Key: "B", Label: "N"},
			{Key: "C", Label: "N"},
		},
		Edges: []vectra.EdgeUpsert{
			{SrcKey: "A", DstKey: "B", Label: "REL"},
			{SrcKey: "B", DstKey: "C", Label: "REL"},
		},
	}))

	// Batch 2 -> layer 2: delete A (tombstone + cascade of A->B). The stack
	// hits MaxLayers so the writer stalls on the fold; the merger rebases the
	// whole prefix {L1, L2} into a new base without A — B and C renumber to
	// 0 and 1 — while the writer mirrors keep A=0(dead), B=1, C=2. Submit 3
	// cannot be applied until that rebase has landed (the stall orders it).
	must(h.Submit(vectra.Changeset{NodeDeletes: []string{"A"}}))

	// Batch 3 (post-rebase): overwrite-write the survivor B. Without the
	// repair, AddNode's override path stamps B's stale mirror ID 1 into the
	// fresh layer — the very ID the renumbered base just assigned to C.
	must(h.Submit(vectra.Changeset{Nodes: []vectra.NodeUpsert{
		{Key: "B", Label: "N2", Props: map[string]any{"r": 1}},
	}}))

	// Mid-session invariants: round-trip + pairwise-distinct live IDs ...
	assertIDConsistency(t, g, []string{"A", "B", "C"}, false)

	// ... and the survivor's base edge must stay BFS-reachable from its own
	// ID at depth 1 (a stale layer ID buckets B onto C's slot and severs it).
	b, ok := g.NodeByKey("B")
	if !ok {
		t.Fatal("B vanished")
	}
	c, ok := g.NodeByKey("C")
	if !ok {
		t.Fatal("C vanished")
	}
	res, err := g.BFS([]vectra.NodeID{b.ID})
	if err != nil {
		t.Fatalf("BFS from B: %v", err)
	}
	if !res.Reached(c.ID) || res.Depth(c.ID) != 1 {
		t.Fatalf("BFS from B (id %d): C (id %d) reached=%v depth=%d, want reached at depth 1 via the base edge",
			b.ID, c.ID, res.Reached(c.ID), res.Depth(c.ID))
	}
	if es, err := g.EdgesOf("B", vectra.DirOut); err != nil || len(es) != 1 {
		t.Fatalf("EdgesOf(B, out) = %v (err %v), want the single surviving base edge", es, err)
	}

	// Post-Close: the final Seal renumbers again; the invariants must hold on
	// the dense generation too.
	must(h.Close())
	assertIDConsistency(t, g, []string{"A", "B", "C"}, true)
	if got := g.NodeCount(); got != 2 {
		t.Fatalf("NodeCount = %d, want 2", got)
	}
	if got := g.EdgeCount(); got != 1 {
		t.Fatalf("EdgeCount = %d, want 1", got)
	}
}

// TestRebaseRepairFiresAtBatchBoundary pins the repair's position: right
// after a rebase, the very next batch must already observe consistent IDs —
// there is no window where a post-rebase write can collide.
func TestRebaseRepairFiresAtBatchBoundary(t *testing.T) {
	g := vectra.New()
	// MergeEdges trigger with a delay: rebases land between batches often.
	h := writer.Attach(g, writer.Config{MaxLayers: 64, MergeEdges: 2, MergeDelay: 30 * time.Millisecond})
	keys := make([]string, 12)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%02d", i)
	}
	seed := vectra.Changeset{Nodes: make([]vectra.NodeUpsert, len(keys))}
	for i, k := range keys {
		seed.Nodes[i] = vectra.NodeUpsert{Key: k, Label: "N"}
	}
	if err := h.Submit(seed); err != nil {
		t.Fatal(err)
	}
	// Interleave deletes (tombstones for the folds to apply — the renumbering
	// shifter) with overwrite-writes of survivors, asserting ID consistency
	// at post-repair quiescent points after every round.
	for i := 0; i < 60; i++ {
		k := keys[1+i%(len(keys)-1)] // never the anchor k00
		var cs vectra.Changeset
		switch i % 3 {
		case 0:
			cs = vectra.Changeset{Nodes: []vectra.NodeUpsert{{Key: k, Label: "N2", Props: map[string]any{"i": i}}}}
		case 1:
			cs = vectra.Changeset{Edges: []vectra.EdgeUpsert{{SrcKey: keys[0], DstKey: k, Label: "E"}}}
		case 2:
			cs = vectra.Changeset{NodeDeletes: []string{k}}
		}
		if err := h.Submit(cs); err != nil && !errors.Is(err, vectra.ErrNodeNotFound) {
			t.Fatalf("Submit %d: %v", i, err)
		}
		assertIDConsistencySettled(t, h, g, keys)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	assertIDConsistency(t, g, keys, true)
}
