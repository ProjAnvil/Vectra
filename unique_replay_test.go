package vectra

// Task 3 of the unique-constraints arc (spec 2026-10-30): persistence parity
// and self-healing — restart round-trips, WAL replay REPLACE semantics, the
// decode funnel (loadGraphForest), Clone's registry carry, store-v8 end to
// end via saveView, and the stale-marked rebuilds (Batch panic recovery,
// RepairAfterRebase) pinned by verifyUniqueInvariant.

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/projanvil/vectra/internal/wal"
)

// --- restart round-trip (snapshot seeding + replay both restore) -----------------

func TestUniqueRestartRoundTrip(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "g.db")
	g, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.DeclareUnique("Svc", "email"); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("n1", "Svc", map[string]any{"email": "a@x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("n2", "Other", nil); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}

	// Session 2: the close-time checkpoint carries the declaration; the
	// constraint must survive with exact ownership, not just as a key.
	g2, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	want := []UniqueConstraint{{Label: "Svc", Prop: "email"}}
	if got := g2.UniqueConstraints(); !reflect.DeepEqual(got, want) {
		t.Fatalf("reopened constraints = %v, want %v (declarations vanished on restart)", got, want)
	}
	if _, err := g2.AddNode("n3", "Svc", map[string]any{"email": "a@x"}); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("reopened enforcement: %v, want ErrUniqueViolation", err)
	}
	if _, err := g2.AddNode("n3", "Svc", map[string]any{"email": "b@x"}); err != nil {
		t.Fatalf("reopened legal write: %v", err)
	}
	if err := g2.VerifyUniqueInvariantForTest(UniqueConstraint{Label: "Svc", Prop: "email"}); err != nil {
		t.Fatalf("reopened registry drifted: %v", err)
	}

	// Session 3 after a drop: the drop must survive the restart too —
	// replaying merge-style would resurrect the constraint.
	if err := g2.DropUnique("Svc", "email"); err != nil {
		t.Fatal(err)
	}
	if err := g2.Close(); err != nil {
		t.Fatal(err)
	}
	g3, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer g3.Close()
	if got := g3.UniqueConstraints(); len(got) != 0 {
		t.Fatalf("dropped constraint resurrected on restart: %v", got)
	}
	if _, err := g3.AddNode("n4", "Svc", map[string]any{"email": "a@x"}); err != nil {
		t.Fatalf("enforcement must stop across restarts after the drop: %v", err)
	}
}

// --- WAL replay: REPLACE semantics (§3.4) -----------------------------------------

// craftWAL builds a WAL directory by hand so the replayed frame ORDER is
// exact — the assertion surface for REPLACE/nil-skip semantics.
func craftWAL(t *testing.T, db string, frames ...any) {
	t.Helper()
	w, err := wal.Open(db+".wal", ModeAlways)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range frames {
		var err error
		switch fr := f.(type) {
		case wal.IndexDecl:
			_, err = w.AppendIndex(fr)
		case wal.Changeset:
			_, err = w.Append(fr)
		default:
			t.Fatalf("craftWAL: unknown frame type %T", f)
		}
		if err != nil {
			t.Fatalf("craftWAL append %T: %v", f, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestUniqueReplayReplaceSemantics(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "g.db")
	uc := func(label, prop string) wal.UniqueConstraint { return wal.UniqueConstraint{Label: label, Prop: prop} }
	node := func(key, email string, props map[string]any) wal.NodeUpsert {
		if props == nil {
			props = map[string]any{}
		}
		props["email"] = email
		return wal.NodeUpsert{Key: key, Label: "Svc", Props: props}
	}
	craftWAL(t, db,
		// declare two constraints
		wal.IndexDecl{UniqueConstraints: []wal.UniqueConstraint{uc("Svc", "email"), uc("Svc", "ref")}},
		wal.Changeset{Nodes: []wal.NodeUpsert{node("k1", "a@x", map[string]any{"ref": "r1"})}},
		// replace with ONE: the "ref" constraint is dropped, "email" stays
		wal.IndexDecl{UniqueConstraints: []wal.UniqueConstraint{uc("Svc", "email")}},
		// legal only because "ref" is gone (r1 would otherwise be taken)
		wal.Changeset{Nodes: []wal.NodeUpsert{node("k2", "b@x", map[string]any{"ref": "r1"})}},
		// replace with NONE: explicit empty list drops the rest
		wal.IndexDecl{UniqueConstraints: []wal.UniqueConstraint{}},
		// legal only because "email" is gone too (a@x would otherwise be taken)
		wal.Changeset{Nodes: []wal.NodeUpsert{node("k3", "a@x", nil)}},
	)

	g, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatalf("open over replay: %v (a merge-style replay would resurrect the dropped constraint and reject a frame)", err)
	}
	defer g.Close()
	if got := g.UniqueConstraints(); len(got) != 0 {
		t.Fatalf("replayed declarations = %v, want none (REPLACE, not merge)", got)
	}
	for _, key := range []string{"k1", "k2", "k3"} {
		if _, ok := g.NodeByKey(key); !ok {
			t.Fatalf("node %s missing after replay", key)
		}
	}
	// The replayed state is real: k1 and k3 now share a@x, so a fresh
	// declaration over the same pair hits the Neo4j rule.
	if err := g.DeclareUnique("Svc", "email"); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("declare over replay-made duplicates: %v, want ErrUniqueViolation", err)
	}
	// A mid-chain snapshot of the registry is pinned by the invariant helper
	// (REPLACE-with-none dropped every constraint by this point).
	if err := g.VerifyUniqueInvariantForTest(); err != nil {
		t.Fatal(err)
	}
}

func TestUniqueReplayNilFrameSkips(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "g.db")
	craftWAL(t, db,
		wal.IndexDecl{UniqueConstraints: []wal.UniqueConstraint{{Label: "Svc", Prop: "email"}}},
		wal.Changeset{Nodes: []wal.NodeUpsert{{Key: "k1", Label: "Svc", Props: map[string]any{"email": "a@x"}}}},
		// pre-feature frame shape: an IndexDecl carrying NO uniqueConstraints
		// field (nil) — replay must skip it, not treat it as drop-all.
		wal.IndexDecl{Keys: []string{"zz"}},
		wal.Changeset{Nodes: []wal.NodeUpsert{{Key: "k2", Label: "Svc", Props: map[string]any{"email": "b@x"}}}},
	)

	g, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	want := []UniqueConstraint{{Label: "Svc", Prop: "email"}}
	if got := g.UniqueConstraints(); !reflect.DeepEqual(got, want) {
		t.Fatalf("nil uniqueConstraints frame wiped the registry: got %v, want %v", got, want)
	}
	// The declaration survives WITH its values: a@x is still owned by k1.
	if _, err := g.AddNode("k3", "Svc", map[string]any{"email": "a@x"}); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("post-nil-frame enforcement: %v, want ErrUniqueViolation", err)
	}
	if err := g.VerifyUniqueInvariantForTest(UniqueConstraint{Label: "Svc", Prop: "email"}); err != nil {
		t.Fatal(err)
	}
}

// TestWALReplayErrorPolicy pins applyChangeset's replay error discipline
// (audit F-02, root lane): every changeset lane is first-error-stops, the
// first failing OP's error surfaces from Open RAW (the engine's own text —
// no replay/frame wrapping), and the failure is deterministic across
// reopenings of the same bytes. Lane order is observable through the error
// class: a frame carrying both a unique-violating node write (nodes run
// first) and a ghost-endpoint edge (edges run second) must report the node
// violation — the edge op behind it never ran.
func TestWALReplayErrorPolicy(t *testing.T) {
	db := filepath.Join(t.TempDir(), "g.db")
	craftWAL(t, db,
		wal.IndexDecl{UniqueConstraints: []wal.UniqueConstraint{{Label: "Svc", Prop: "email"}}},
		wal.Changeset{Nodes: []wal.NodeUpsert{{Key: "k1", Label: "Svc", Props: map[string]any{"email": "a@x"}}}},
		wal.Changeset{
			Nodes: []wal.NodeUpsert{{Key: "k2", Label: "Svc", Props: map[string]any{"email": "a@x"}}}, // violates k1's a@x
			Edges: []wal.EdgeUpsert{{SrcKey: "k1", DstKey: "ghost", Label: "E"}},                      // would fail too — never reached
		},
	)
	g, err := Open(db, Options{WALMode: ModeAlways})
	if err == nil {
		g.Close()
		t.Fatal("open over a replay-violating frame: want error, got nil")
	}
	var uv *UniqueViolationError
	if !errors.As(err, &uv) || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("replay error = %v, want the raw unique-violation class", err)
	}
	if uv.Value != "a@x" || uv.ExistingKey != "k1" || uv.AttemptedKey != "k2" {
		t.Fatalf("violation payload = %+v, want value a@x owner k1 attempted k2", uv)
	}
	// Raw surface: the engine's own text rides out unchanged (nodes-lane
	// stop-at-first; the ghost edge behind it is not reported).
	if want := `unique constraint violation: (Svc.email) value "a@x" already owned by key "k1"`; err.Error() != want {
		t.Fatalf("replay error text = %q, want the engine's raw text %q", err.Error(), want)
	}

	// Edge-lane failure surfaces the bare sentinel the same way (raw, no
	// wrapper), and the abort is deterministic: the same bytes fail the same
	// way on an independent library.
	db2 := filepath.Join(t.TempDir(), "g2.db")
	craftWAL(t, db2,
		wal.Changeset{
			Nodes:       []wal.NodeUpsert{{Key: "a", Label: "Svc"}},
			Edges:       []wal.EdgeUpsert{{SrcKey: "a", DstKey: "ghost", Label: "E"}},
			NodeDeletes: []string{"a"}, // after the failing edge: never executed
		},
	)
	_, err2a := Open(db2, Options{WALMode: ModeAlways})
	_, err2b := Open(db2, Options{WALMode: ModeAlways})
	for _, e := range []error{err2a, err2b} {
		if !errors.Is(e, ErrNodeNotFound) {
			t.Fatalf("edge-lane replay error = %v, want ErrNodeNotFound", e)
		}
		if e.Error() != "vectra: node key not found" {
			t.Fatalf("edge-lane replay text = %q, want the raw sentinel text", e.Error())
		}
	}
	if err2a.Error() != err2b.Error() {
		t.Fatalf("replay failure not deterministic: %q vs %q", err2a.Error(), err2b.Error())
	}
}

// --- Clone carry (§3.5) -----------------------------------------------------------

func TestUniqueCloneCarriesConstraints(t *testing.T) {
	g := New()
	if err := g.DeclareUnique("Svc", "email"); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("n1", "Svc", map[string]any{"email": "a@x"}); err != nil {
		t.Fatal(err)
	}
	c := g.Clone()
	want := []UniqueConstraint{{Label: "Svc", Prop: "email"}}
	if got := c.UniqueConstraints(); !reflect.DeepEqual(got, want) {
		t.Fatalf("clone constraints = %v, want %v (Clone dropped the registry)", got, want)
	}
	if _, err := c.AddNode("n2", "Svc", map[string]any{"email": "a@x"}); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("clone enforcement: %v, want ErrUniqueViolation", err)
	}
	if err := c.VerifyUniqueInvariantForTest(UniqueConstraint{Label: "Svc", Prop: "email"}); err != nil {
		t.Fatalf("clone registry drifted: %v", err)
	}
	if _, err := c.AddNode("n2", "Svc", map[string]any{"email": "b@x"}); err != nil {
		t.Fatalf("clone legal write: %v", err)
	}
	// Registry isolation, both directions: the clone's registration of b@x
	// never leaks into the source (deep copy, not shared maps) ...
	if _, err := g.AddNode("n3", "Svc", map[string]any{"email": "b@x"}); err != nil {
		t.Fatalf("clone write leaked into the source registry: %v", err)
	}
	// ... and the source's ownership of a@x never leaks into the clone.
	if _, err := c.AddNode("n4", "Svc", map[string]any{"email": "a@x"}); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("source ownership leaked into the clone registry: %v", err)
	}

	// A dropped constraint stays dropped on clones.
	if err := g.DropUnique("Svc", "email"); err != nil {
		t.Fatal(err)
	}
	if got := g.Clone().UniqueConstraints(); len(got) != 0 {
		t.Fatalf("clone of a dropped registry = %v, want none", got)
	}
}

// --- store v8 end-to-end via saveView (§3.5 encode side) ---------------------------

func TestUniqueStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "g.db")
	g := New()
	// Deliberate insertion-order shuffle: the persisted list must be sorted.
	for _, c := range [][2]string{{"Zeta", "z"}, {"Alpha", "a"}, {"Mid", "m"}} {
		if err := g.DeclareUnique(c[0], c[1]); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := g.AddNode("n1", "Alpha", map[string]any{"a": "a@x"}); err != nil {
		t.Fatal(err)
	}
	if got := g.NodeCount(); got != 1 { // publish the window: SaveFile pins the view as-is (no pend flush)
		t.Fatalf("setup: %d nodes, want 1", got)
	}

	if err := g.SaveFile(db); err != nil {
		t.Fatal(err)
	}
	g2, err := LoadFile(db)
	if err != nil {
		t.Fatal(err)
	}
	want := []UniqueConstraint{
		{Label: "Alpha", Prop: "a"},
		{Label: "Mid", Prop: "m"},
		{Label: "Zeta", Prop: "z"},
	}
	if got := g2.UniqueConstraints(); !reflect.DeepEqual(got, want) {
		t.Fatalf("LoadFile constraints = %v, want %v (sorted)", got, want)
	}
	if _, err := g2.AddNode("n2", "Alpha", map[string]any{"a": "a@x"}); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("LoadFile enforcement: %v, want ErrUniqueViolation", err)
	}
	if err := g2.VerifyUniqueInvariantForTest(want...); err != nil {
		t.Fatal(err)
	}

	// The io.Writer pair (Save/Load — server snapshots ride this shape).
	var buf bytes.Buffer
	if err := g.Save(&buf); err != nil {
		t.Fatal(err)
	}
	g3, err := Load(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got := g3.UniqueConstraints(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Load constraints = %v, want %v", got, want)
	}
	if _, err := g3.AddNode("n2", "Alpha", map[string]any{"a": "a@x"}); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("Load enforcement: %v, want ErrUniqueViolation", err)
	}

	// Unconstrained graphs round-trip to unconstrained graphs.
	g4 := New()
	if _, err := g4.AddNode("x", "L", map[string]any{"p": "v"}); err != nil {
		t.Fatal(err)
	}
	var buf2 bytes.Buffer
	if err := g4.Save(&buf2); err != nil {
		t.Fatal(err)
	}
	g5, err := Load(&buf2)
	if err != nil {
		t.Fatal(err)
	}
	if got := g5.UniqueConstraints(); len(got) != 0 {
		t.Fatalf("unconstrained round-trip = %v, want none", got)
	}
	if _, err := g5.AddNode("y", "L", map[string]any{"p": "v"}); err != nil {
		t.Fatalf("unconstrained round-trip must not enforce: %v", err)
	}
}

// --- stale-marked rebuild: Batch panic recovery (§3.3) ------------------------------

func TestUniqueBatchPanicStaleRebuild(t *testing.T) {
	g := New()
	if err := g.DeclareUnique("Svc", "email"); err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() { _ = recover() }()
		g.Batch(func(g *Graph) {
			if _, err := g.AddNode("ph", "Svc", map[string]any{"email": "phantom@x"}); err != nil {
				t.Fatal(err)
			}
			panic("boom")
		})
	}()
	// The recovered state: batching cleared and the registry marked stale —
	// the panic defer marks only AFTER g.batching clears, so the rebuild's
	// flush can never cut an open batch (carry-over ruling from Task 2).
	if g.batching {
		t.Fatal("batching flag wedged after panic recovery")
	}
	if !g.uniqueStale {
		t.Fatal("Batch panic recovery must mark the registry stale")
	}
	if _, ok := g.NodeByKey("ph"); ok {
		t.Fatal("panicked write must not be published")
	}
	// The next write rebuilds from live state: the phantom ownership the
	// dropped window registered must be gone, so the same value is free.
	if _, err := g.AddNode("fresh", "Svc", map[string]any{"email": "phantom@x"}); err != nil {
		t.Fatalf("phantom ownership survived the rebuild: %v", err)
	}
	if g.uniqueStale {
		t.Fatal("the rebuild must clear the stale flag")
	}
	if err := g.VerifyUniqueInvariantForTest(UniqueConstraint{Label: "Svc", Prop: "email"}); err != nil {
		t.Fatal(err)
	}
	// Enforcement is exact afterwards: the rebuilt registry tracks "fresh".
	if _, err := g.AddNode("dup", "Svc", map[string]any{"email": "phantom@x"}); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("post-rebuild enforcement: %v, want ErrUniqueViolation", err)
	}
}

// --- stale-marked rebuild: RepairAfterRebase (§3.3) ---------------------------------

func TestUniqueRepairAfterRebaseStaleRebuild(t *testing.T) {
	g := New()
	if err := g.DeclareUnique("Svc", "email"); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("n1", "Svc", map[string]any{"email": "a@x"}); err != nil {
		t.Fatal(err)
	}
	// The production trigger (a background PublishMerge) belongs to
	// internal/writer's tests; here the rebase flag is set directly and the
	// repair path is what this row pins.
	g.rebaseHappened.Store(true)
	if err := g.RepairAfterRebase(); err != nil {
		t.Fatal(err)
	}
	if !g.uniqueStale {
		t.Fatal("the mirror rebuild must mark the registry stale")
	}
	// Prove the next write really rebuilds: poison a value map after the
	// mark — the rebuild must restore ground-truth ownership.
	c := UniqueConstraint{Label: "Svc", Prop: "email"}
	g.uniqueIdx[c]["a@x"] = "ghost"
	if _, err := g.AddNode("n2", "Svc", map[string]any{"email": "b@x"}); err != nil {
		t.Fatal(err)
	}
	if got := g.uniqueIdx[c]["a@x"]; got != "n1" {
		t.Fatalf("rebuild must restore ground-truth ownership: a@x -> %q, want n1", got)
	}
	if _, err := g.AddNode("n3", "Svc", map[string]any{"email": "a@x"}); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("post-repair enforcement: %v, want ErrUniqueViolation", err)
	}
	if g.uniqueStale {
		t.Fatal("the rebuild must clear the stale flag")
	}
	if err := g.VerifyUniqueInvariantForTest(UniqueConstraint{Label: "Svc", Prop: "email"}); err != nil {
		t.Fatal(err)
	}
	// Without a rebase the repair is a no-op — and never marks.
	if err := g.RepairAfterRebase(); err != nil {
		t.Fatal(err)
	}
	if g.uniqueStale {
		t.Fatal("a no-rebase repair must not mark the registry stale")
	}
}

// --- folds during writes keep the registry exact (§3.3/§6) ---------------------------

func TestUniqueFoldDuringWritesInvariant(t *testing.T) {
	g := New().SetAutoCompact(2) // folds fire during the write stream
	if err := g.DeclareUnique("Person", "email"); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("anchor", "Person", map[string]any{"email": "anchor@x"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 60; i++ {
		key := fmt.Sprintf("k%02d", i)
		if _, err := g.AddNode(key, "Person", map[string]any{"email": key + "@x"}); err != nil {
			t.Fatalf("iter %d add %s: %v", i, key, err)
		}
		switch {
		case i%7 == 3: // tombstones: the renumbering shifter for folds
			if _, err := g.DeleteNode(key); err != nil {
				t.Fatalf("iter %d delete %s: %v", i, key, err)
			}
		case i%5 == 0: // override: releases the old value, registers the new
			if _, err := g.AddNode(key, "Person", map[string]any{"email": key + "@y"}); err != nil {
				t.Fatalf("iter %d override %s: %v", i, key, err)
			}
		}
		if i%11 == 5 { // a rejected write mid-stream: zero side effects
			if _, err := g.AddNode("t", "Person", map[string]any{"email": "anchor@x"}); !errors.Is(err, ErrUniqueViolation) {
				t.Fatalf("iter %d dup attempt: %v, want ErrUniqueViolation", i, err)
			}
		}
		if err := g.VerifyUniqueInvariantForTest(UniqueConstraint{Label: "Person", Prop: "email"}); err != nil {
			t.Fatalf("iter %d: %v", i, err)
		}
	}
	// A plain Seal is a fold WITHOUT a rebase: it must not mark stale (§3.3
	// — normal publish/fold/seal paths rely on incremental correctness).
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	if g.uniqueStale {
		t.Fatal("a normal seal must not mark the registry stale")
	}
	if err := g.VerifyUniqueInvariantForTest(UniqueConstraint{Label: "Person", Prop: "email"}); err != nil {
		t.Fatal(err)
	}
	// Post-fold enforcement is exact.
	if _, err := g.AddNode("t2", "Person", map[string]any{"email": "anchor@x"}); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("post-fold enforcement: %v, want ErrUniqueViolation", err)
	}
}

// --- the invariant helper pins the KEY SET, not just the values ---------------------

// TestUniqueVerifyInvariantPinsKeySet pins the closed blind spot of
// VerifyUniqueInvariantForTest: the expected constraint set is now an
// argument (zero args = no constraints), so a registry that wrongly DROPPED
// or wrongly KEPT a constraint key fails by name instead of the helper
// verifying whatever set it happens to hold.
func TestUniqueVerifyInvariantPinsKeySet(t *testing.T) {
	g := New()
	for _, c := range [][2]string{{"Svc", "email"}, {"Svc", "ref"}} {
		if err := g.DeclareUnique(c[0], c[1]); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := g.AddNode("n1", "Svc", map[string]any{"email": "a@x", "ref": "r1"}); err != nil {
		t.Fatal(err)
	}
	if err := g.DropUnique("Svc", "ref"); err != nil {
		t.Fatal(err)
	}
	email := UniqueConstraint{Label: "Svc", Prop: "email"}
	ref := UniqueConstraint{Label: "Svc", Prop: "ref"}

	// The remaining set verifies clean.
	if err := g.VerifyUniqueInvariantForTest(email); err != nil {
		t.Fatalf("remaining set: %v", err)
	}
	// Asking for the dropped one reports it MISSING by name — the old
	// zero-arg helper derived its expected set from the registry itself and
	// could never see a dropped key.
	err := g.VerifyUniqueInvariantForTest(email, ref)
	if err == nil || !strings.Contains(err.Error(), "missing (Svc.ref)") {
		t.Fatalf("dropped constraint: %v, want a missing-constraint error naming (Svc.ref)", err)
	}
	// A zero-arg call on a constrained graph reports the EXTRA by name.
	err = g.VerifyUniqueInvariantForTest()
	if err == nil || !strings.Contains(err.Error(), "extra (Svc.email)") {
		t.Fatalf("zero-arg call on a constrained graph: %v, want an extra-constraint error naming (Svc.email)", err)
	}
}

// --- carry-over: deterministic declaration-list emission ------------------------------

func TestUniqueDeclFrameEmissionSorted(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "g.db")
	g, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	// Insertion order deliberately unsorted; the second frame carries the
	// full list and must be byte-stable regardless of map order.
	for _, c := range [][2]string{{"Zeta", "z"}, {"Alpha", "a"}, {"Mid", "m"}} {
		if err := g.DeclareUnique(c[0], c[1]); err != nil {
			t.Fatal(err)
		}
	}
	frames := walFrames(t, db+".wal")
	var list []wal.UniqueConstraint
	for _, fr := range frames {
		if fr.UniqueConstraints != nil {
			list = fr.UniqueConstraints
		}
	}
	want := []wal.UniqueConstraint{
		{Label: "Alpha", Prop: "a"},
		{Label: "Mid", Prop: "m"},
		{Label: "Zeta", Prop: "z"},
	}
	if !reflect.DeepEqual(list, want) {
		t.Fatalf("last declaration frame = %v, want %v (sorted emission)", list, want)
	}
}
