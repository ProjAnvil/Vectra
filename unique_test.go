package vectra

import (
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/projanvil/vectra/internal/wal"
)

// snapshotUnique flattens the registry for deep comparison: "Label.Prop|value"
// -> owning key.
func snapshotUnique(g *Graph) map[string]string {
	out := map[string]string{}
	for c, vals := range g.uniqueIdx {
		for v, k := range vals {
			out[c.Label+"."+c.Prop+"|"+v] = k
		}
	}
	return out
}

// walFrames reopens the WAL directory read-only and returns every frame in
// LSN order (the replayStats pattern from wal_integration_test.go).
func walFrames(t *testing.T, walDir string) []wal.Frame {
	t.Helper()
	w, err := wal.Open(walDir, ModeAlways)
	if err != nil {
		t.Fatalf("wal open: %v", err)
	}
	defer w.Close()
	var out []wal.Frame
	if _, err := w.Replay(func(fr wal.Frame) error {
		out = append(out, fr)
		return nil
	}); err != nil {
		t.Fatalf("wal replay: %v", err)
	}
	return out
}

// --- Task 1 carry-over: the pinned error face ---------------------------------

func TestUniqueViolationErrorFace(t *testing.T) {
	e := &UniqueViolationError{
		Label: "Person", Prop: "email", Value: "a@x",
		ExistingKey: "n1", AttemptedKey: "n2",
	}
	want := `unique constraint violation: (Person.email) value "a@x" already owned by key "n1"`
	if got := e.Error(); got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
	if !errors.Is(e, ErrUniqueViolation) {
		t.Fatal("errors.Is(violation, ErrUniqueViolation) must hold")
	}
	var uv *UniqueViolationError
	if !errors.As(error(e), &uv) || uv.AttemptedKey != "n2" || uv.ExistingKey != "n1" {
		t.Fatalf("errors.As payload: %+v", uv)
	}
	if errors.Is(e, ErrConstraintExists) || errors.Is(e, ErrConstraintNotFound) {
		t.Fatal("the violation must not cross-match the declaration sentinels")
	}
}

// --- declaration lifecycle -----------------------------------------------------

func TestDeclareUniqueLifecycle(t *testing.T) {
	g := New()
	if got := g.UniqueConstraints(); len(got) != 0 {
		t.Fatalf("fresh graph constraints = %v, want none", got)
	}
	if err := g.DeclareUnique("Person", "email"); err != nil {
		t.Fatalf("declare: %v", err)
	}
	if err := g.DeclareUnique("Person", "email"); !errors.Is(err, ErrConstraintExists) {
		t.Fatalf("duplicate declare: %v, want ErrConstraintExists", err)
	}
	if err := g.DeclareUnique("Alpha", "x"); err != nil {
		t.Fatal(err)
	}
	if err := g.DeclareUnique("Person", "name"); err != nil {
		t.Fatal(err)
	}
	got := g.UniqueConstraints()
	want := []UniqueConstraint{
		{Label: "Alpha", Prop: "x"},
		{Label: "Person", Prop: "email"},
		{Label: "Person", Prop: "name"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("UniqueConstraints() = %v, want %v (sorted by label then prop)", got, want)
	}
	// The listing is a copy: mutating it never touches the registry.
	got[0].Prop = "mutated"
	if again := g.UniqueConstraints(); !reflect.DeepEqual(again, want) {
		t.Fatalf("registry mutated through the listing: %v", again)
	}

	if err := g.DropUnique("Person", "name"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if err := g.DropUnique("Person", "name"); !errors.Is(err, ErrConstraintNotFound) {
		t.Fatalf("re-drop: %v, want ErrConstraintNotFound", err)
	}
	if err := g.DropUnique("Nope", "x"); !errors.Is(err, ErrConstraintNotFound) {
		t.Fatalf("drop unknown: %v, want ErrConstraintNotFound", err)
	}
	want2 := []UniqueConstraint{
		{Label: "Alpha", Prop: "x"},
		{Label: "Person", Prop: "email"},
	}
	if got := g.UniqueConstraints(); !reflect.DeepEqual(got, want2) {
		t.Fatalf("after drop: %v, want %v", got, want2)
	}
}

// --- declaration-time validation (Neo4j rule) ----------------------------------

func TestDeclareUniqueRejectsDuplicateData(t *testing.T) {
	g := New()
	g.AddNode("n1", "Svc", map[string]any{"email": "a@x"})
	g.AddNode("n2", "Svc", map[string]any{"email": "a@x"})
	g.AddNode("n3", "Svc", map[string]any{"email": "b@x"})
	g.AddNode("n4", "Svc", map[string]any{"email": "b@x"})
	g.AddNode("n5", "Svc", map[string]any{"email": "c@x"}) // unique value: no pair

	err := g.DeclareUnique("Svc", "email")
	if !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("declare over duplicates: %v, want ErrUniqueViolation", err)
	}
	// Deterministic listing: pairs sorted by value then key.
	want := `vectra: DeclareUnique(Svc.email): duplicate values: "a@x" owned by "n1", "a@x" owned by "n2", "b@x" owned by "n3", "b@x" owned by "n4"`
	if err.Error() != want {
		t.Fatalf("listing:\n got %q\nwant %q", err.Error(), want)
	}
	// The constraint is NOT registered and enforcement stays off.
	if got := g.UniqueConstraints(); len(got) != 0 {
		t.Fatalf("failed declaration registered: %v", got)
	}
	if _, err := g.AddNode("n6", "Svc", map[string]any{"email": "a@x"}); err != nil {
		t.Fatalf("no enforcement after failed declaration: %v", err)
	}
}

func TestDeclareUniqueListingCapAndDeterminism(t *testing.T) {
	build := func(order []string) error {
		g := New()
		// Six duplicated values (two keys each) = 12 pairs; the listing caps
		// at 10. Insertion order varies across the two builds.
		for _, key := range order {
			g.AddNode(key, "Svc", map[string]any{"email": "dup+" + key})
		}
		for _, key := range order {
			g.AddNode("b_"+key, "Svc", map[string]any{"email": "dup+" + key})
		}
		return g.DeclareUnique("Svc", "email")
	}
	forward := []string{"v01", "v02", "v03", "v04", "v05", "v06"}
	reverse := []string{"v06", "v05", "v04", "v03", "v02", "v01"}
	e1, e2 := build(forward), build(reverse)
	if e1 == nil || e2 == nil {
		t.Fatal("declaration over duplicates must fail")
	}
	if !errors.Is(e1, ErrUniqueViolation) || !errors.Is(e2, ErrUniqueViolation) {
		t.Fatalf("declaration errors must match ErrUniqueViolation: %v / %v", e1, e2)
	}
	if e1.Error() != e2.Error() {
		t.Fatalf("listing must be deterministic across insertion orders:\n%q\n%q", e1.Error(), e2.Error())
	}
	// The cap: exactly 10 pairs, the first 10 in (value, key) order.
	const marker = ` owned by "`
	if got := strings.Count(e1.Error(), marker); got != 10 {
		t.Fatalf("listing carries %d pairs, want the 10-pair cap: %q", got, e1.Error())
	}
	if !strings.HasPrefix(e1.Error(), `vectra: DeclareUnique(Svc.email): duplicate values: "dup+v01" owned by "b_v01"`) {
		t.Fatalf("listing must start at the smallest (value, key): %q", e1.Error())
	}
}

func TestDeclareUniqueScanSeesOpenWindow(t *testing.T) {
	g := New()
	// Both writes absorb into the open absorption window (no read in
	// between): the validation scan must still see them — DeclareUnique
	// publishes the caller's window before scanning.
	g.AddNode("n1", "Svc", map[string]any{"email": "dup@x"})
	g.AddNode("n2", "Svc", map[string]any{"email": "dup@x"})
	if err := g.DeclareUnique("Svc", "email"); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("declare over window-resident duplicates: %v", err)
	}
}

// --- write-time enforcement at AddNode -----------------------------------------

func TestUniqueAddNodeViolationZeroSideEffects(t *testing.T) {
	db := filepath.Join(t.TempDir(), "g.db")
	g, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	if err := g.CreateVectorIndex(VectorIndexDecl{Name: "v", Prop: "emb", Dim: 2, Metric: "cosine"}); err != nil {
		t.Fatal(err)
	}
	if err := g.DeclareUnique("Svc", "email"); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("a", "Svc", map[string]any{"email": "a@x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("b", "Svc", map[string]any{"emb": []float32{1, 0}}); err != nil {
		t.Fatal(err)
	}
	// Settle: publish the window and the vector overlay.
	if got := g.NodeCount(); got != 2 {
		t.Fatalf("setup nodes = %d, want 2", got)
	}
	hits, err := g.QueryVector("v", []float32{1, 0}, QueryVectorOptions{K: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Key != "b" {
		t.Fatalf("setup vector hits = %+v", hits)
	}

	lsn := g.wal.LastLSN()
	pend := len(g.pendOps)
	ratchet := g.nextNode
	reg := snapshotUnique(g)

	_, err = g.AddNode("c", "Svc", map[string]any{"email": "a@x", "emb": []float32{0, 1}})
	var uv *UniqueViolationError
	if !errors.As(err, &uv) || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("violation error: %v", err)
	}
	if uv.Label != "Svc" || uv.Prop != "email" || uv.Value != "a@x" ||
		uv.ExistingKey != "a" || uv.AttemptedKey != "c" {
		t.Fatalf("violation payload: %+v", uv)
	}
	if g.wal.LastLSN() != lsn {
		t.Fatalf("rejected write advanced the WAL: %d -> %d", lsn, g.wal.LastLSN())
	}
	if len(g.pendOps) != pend {
		t.Fatalf("rejected write left pendOps: %d -> %d", pend, len(g.pendOps))
	}
	if g.nextNode != ratchet {
		t.Fatalf("rejected write advanced the ID ratchet: %d -> %d", ratchet, g.nextNode)
	}
	if got := snapshotUnique(g); !reflect.DeepEqual(got, reg) {
		t.Fatalf("rejected write touched the registry: %v -> %v", reg, got)
	}
	if g.builder != nil && g.builder.HasNode("c") {
		t.Fatal("rejected write reached the builder")
	}
	if got := g.NodeCount(); got != 2 {
		t.Fatalf("rejected write visible: %d nodes", got)
	}
	hits2, err := g.QueryVector("v", []float32{1, 0}, QueryVectorOptions{K: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits2) != 1 || hits2[0].Key != "b" {
		t.Fatalf("rejected write reached the vector intake: %+v", hits2)
	}
}

func TestUniqueAddNodeViolationWalOff(t *testing.T) {
	g := New()
	if err := g.DeclareUnique("Svc", "email"); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("a", "Svc", map[string]any{"email": "a@x"}); err != nil {
		t.Fatal(err)
	}
	_, err := g.AddNode("b", "Svc", map[string]any{"email": "a@x"})
	var uv *UniqueViolationError
	if !errors.As(err, &uv) {
		t.Fatalf("violation error: %v", err)
	}
	if uv.ExistingKey != "a" || uv.AttemptedKey != "b" || uv.Value != "a@x" {
		t.Fatalf("violation payload: %+v", uv)
	}
	if _, ok := g.NodeByKey("b"); ok {
		t.Fatal("rejected write must be invisible")
	}
	// Self-reassignment of the same value stays legal.
	if _, err := g.AddNode("a", "Svc", map[string]any{"email": "a@x"}); err != nil {
		t.Fatalf("same-key same-value override: %v", err)
	}
	// A different label never conflicts with the constraint.
	if _, err := g.AddNode("x", "Other", map[string]any{"email": "a@x"}); err != nil {
		t.Fatalf("label mismatch must be invisible: %v", err)
	}
}

// TestUniqueMultiConstraintViolationOrder pins the deterministic
// first-violation payload when one write violates several constraints at
// once: the sorted-first constraint (label, then prop — the listing/save
// emission order) is reported, not whichever one map iteration happens to
// visit first. Accept/reject is deterministic regardless; only the error
// payload follows iteration order, and raft replicas compare it
// (server/constraint_test.go). The rejected write has zero side effects,
// so the same attempt is replayed to pin the order against map randomness.
func TestUniqueMultiConstraintViolationOrder(t *testing.T) {
	g := New()
	for _, c := range [][2]string{{"Svc", "email"}, {"Svc", "name"}, {"Svc", "ssn"}} {
		if err := g.DeclareUnique(c[0], c[1]); err != nil {
			t.Fatal(err)
		}
	}
	// Seeds unique per constraint; "dup" below collides with all three.
	seeds := []struct{ key, email, name, ssn string }{
		{"ownEmail", "dup@x", "u1", "u1"},
		{"ownName", "u1@x", "dup", "u2"},
		{"ownSsn", "u2@x", "u2", "111"},
	}
	for _, s := range seeds {
		if _, err := g.AddNode(s.key, "Svc", map[string]any{
			"email": s.email, "name": s.name, "ssn": s.ssn,
		}); err != nil {
			t.Fatal(err)
		}
	}
	// email < name < ssn sorted: every attempt must report the email
	// constraint, never name or ssn.
	want := UniqueViolationError{
		Label: "Svc", Prop: "email", Value: "dup@x",
		ExistingKey: "ownEmail", AttemptedKey: "dup",
	}
	for i := 0; i < 25; i++ {
		_, err := g.AddNode("dup", "Svc", map[string]any{
			"email": "dup@x", "name": "dup", "ssn": "111",
		})
		var uv *UniqueViolationError
		if !errors.As(err, &uv) {
			t.Fatalf("attempt %d: violation error: %v", i, err)
		}
		if *uv != want {
			t.Fatalf("attempt %d: first violation = %+v, want the sorted-first constraint: %+v", i, uv, want)
		}
	}
}

// --- SetNodeProps facade (covered transitively through AddNode) -----------------

func TestUniqueSetNodePropsFacade(t *testing.T) {
	g := New()
	w := newWriteGraph(g)
	g.AddNode("n1", "Person", map[string]any{"email": "a@x"})
	g.AddNode("n2", "Person", nil)
	if err := g.DeclareUnique("Person", "email"); err != nil {
		t.Fatal(err)
	}

	_, err := w.SetNodeProps("n2", map[string]any{"email": "a@x"})
	var uv *UniqueViolationError
	if !errors.As(err, &uv) || uv.AttemptedKey != "n2" || uv.ExistingKey != "n1" {
		t.Fatalf("facade violation: %v (%+v)", err, uv)
	}
	if _, err := w.SetNodeProps("n2", map[string]any{"email": "b@x"}); err != nil {
		t.Fatalf("facade distinct value: %v", err)
	}
	// nil deletes the prop: the value a@x is freed with it.
	if _, err := w.SetNodeProps("n1", map[string]any{"email": nil}); err != nil {
		t.Fatalf("facade nil-delete: %v", err)
	}
	if _, err := w.SetNodeProps("n2", map[string]any{"email": "a@x"}); err != nil {
		t.Fatalf("value freed by nil-delete must be reclaimable: %v", err)
	}
}

// --- value lifecycle ------------------------------------------------------------

func TestUniqueOverrideReleasesOldValue(t *testing.T) {
	g := New()
	g.DeclareUnique("Svc", "email")
	g.AddNode("n1", "Svc", map[string]any{"email": "a@x"})
	// Override replaces props wholesale: the old value must be released.
	if _, err := g.AddNode("n1", "Svc", map[string]any{"email": "b@x"}); err != nil {
		t.Fatalf("override to a fresh value: %v", err)
	}
	if _, err := g.AddNode("n2", "Svc", map[string]any{"email": "a@x"}); err != nil {
		t.Fatalf("released value must be reclaimable: %v", err)
	}
	// ...and the label can change away from the constraint entirely: the
	// old registration is released with it.
	if _, err := g.AddNode("n1", "Other", map[string]any{"ref": "z"}); err != nil {
		t.Fatalf("label change off the constraint: %v", err)
	}
	if _, err := g.AddNode("n3", "Svc", map[string]any{"email": "b@x"}); err != nil {
		t.Fatalf("the label change must release the old registration: %v", err)
	}
}

func TestUniqueDeleteNodeFreesValueSameBatch(t *testing.T) {
	g := New()
	g.DeclareUnique("Svc", "email")
	err := g.Batch(func(g *Graph) {
		if _, err := g.AddNode("n1", "Svc", map[string]any{"email": "a@x"}); err != nil {
			t.Fatal(err)
		}
		if _, err := g.DeleteNode("n1"); err != nil {
			t.Fatal(err)
		}
		// No tombstone hold: the value is reclaimable within the same batch.
		if _, err := g.AddNode("n2", "Svc", map[string]any{"email": "a@x"}); err != nil {
			t.Fatalf("same-batch reclaim: %v", err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if n, ok := g.NodeByKey("n2"); !ok || n.Props["email"] != "a@x" {
		t.Fatalf("reclaiming node: %+v ok=%v", n, ok)
	}
}

func TestUniqueReviveRechecks(t *testing.T) {
	g := New()
	g.DeclareUnique("Svc", "email")
	g.AddNode("n1", "Svc", map[string]any{"email": "a@x"})
	if _, err := g.DeleteNode("n1"); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("n2", "Svc", map[string]any{"email": "a@x"}); err != nil {
		t.Fatalf("freed by delete: %v", err)
	}
	// Reviving n1 with the same value re-checks through its AddNode entry:
	// n2 owns it now.
	_, err := g.AddNode("n1", "Svc", map[string]any{"email": "a@x"})
	var uv *UniqueViolationError
	if !errors.As(err, &uv) || uv.ExistingKey != "n2" || uv.AttemptedKey != "n1" {
		t.Fatalf("revive re-check: %v (%+v)", err, uv)
	}
	if _, err := g.AddNode("n1", "Svc", map[string]any{"email": "c@x"}); err != nil {
		t.Fatalf("revive with a free value: %v", err)
	}
}

func TestUniqueNonStringInvisible(t *testing.T) {
	g := New()
	g.DeclareUnique("Svc", "ref")
	if _, err := g.AddNode("n1", "Svc", map[string]any{"ref": 123}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("n2", "Svc", map[string]any{"ref": 123}); err != nil {
		t.Fatalf("non-string duplicate must be invisible: %v", err)
	}
	if _, err := g.AddNode("n3", "Svc", map[string]any{"ref": true}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("n4", "Svc", map[string]any{"ref": true}); err != nil {
		t.Fatalf("bool duplicate must be invisible: %v", err)
	}
	if _, err := g.AddNode("n5", "Svc", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("n6", "Svc", nil); err != nil {
		t.Fatalf("missing prop must be invisible: %v", err)
	}
	// Strings still conflict.
	g.AddNode("n7", "Svc", map[string]any{"ref": "s"})
	if _, err := g.AddNode("n8", "Svc", map[string]any{"ref": "s"}); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("string duplicate must conflict: %v", err)
	}
}

// TestUniqueConstrainedAddAllocParity pins the zero-allocation violation
// scan: a happy-path AddNode on a graph WITH declared constraints must
// allocate exactly as much as the same write on a structurally identical
// unconstrained graph. The pre-fix scan built and sorted a candidate slice
// sized by the WHOLE registry on every constrained write — one heap
// allocation per write once two or more constraints exist (a single-entry
// slice stays on the stack), while the check gate itself must be free like
// the zero-constraint fast path. The registry therefore carries 8
// constraints (past the stack-allocation bound) with one matching the
// written label. Both graphs pre-add the same node population (every shared
// structure — builder maps, node tables, the constraint's value map — sits
// past the same growth thresholds) and each measured run writes a fresh key
// with a fresh constrained value. The measured window lands the value map at
// 357 entries, below its next growth threshold with margin, so the registry
// inserts uniqueSwap performs allocate nothing and the two counts must land
// on the SAME number.
func TestUniqueConstrainedAddAllocParity(t *testing.T) {
	if raceEnabled {
		t.Skip("alloc counts differ under -race")
	}
	const (
		pre         = 256
		constraints = 8
	)
	build := func(constrained bool) *Graph {
		g := New()
		if constrained {
			// One live constraint on the written label; the rest sit on
			// labels no write ever touches, so the registry's SIZE (what the
			// candidate slice was sized by) is pinned without changing the
			// write's own registration work.
			if err := g.DeclareUnique("Svc", "email"); err != nil {
				t.Fatal(err)
			}
			for i := 1; i < constraints; i++ {
				if err := g.DeclareUnique("Aux"+itoa(i), "email"); err != nil {
					t.Fatal(err)
				}
			}
		}
		for i := 0; i < pre; i++ {
			key := "seed" + itoa(i)
			if _, err := g.AddNode(key, "Svc", map[string]any{"email": key + "@x"}); err != nil {
				t.Fatal(err)
			}
		}
		return g
	}
	measure := func(g *Graph) float64 {
		n := 0
		return testing.AllocsPerRun(100, func() {
			key := "run" + itoa(n)
			n++
			if _, err := g.AddNode(key, "Svc", map[string]any{"email": key + "@x"}); err != nil {
				t.Fatal(err)
			}
		})
	}
	plain, cons := build(false), build(true)
	nPlain, nCons := measure(plain), measure(cons)
	t.Logf("happy-path AddNode allocs: unconstrained %.2f, constrained (%d constraints) %.2f", nPlain, constraints, nCons)
	// Absolute equality: the invariant under test is that the constrained
	// scan adds ZERO allocations, not any particular absolute count. If a
	// future toolchain's map-growth tuning ever flakes this (an amortized
	// registry-insert bucket), relax to a ±1 delta — the pre-fix regression
	// this pin catches was exactly +1.00.
	if nCons != nPlain {
		t.Fatalf("constrained AddNode allocs = %.2f vs unconstrained %.2f: the violation scan must be allocation-free", nCons, nPlain)
	}
}

func TestUniqueDropStopsEnforcement(t *testing.T) {
	g := New()
	g.DeclareUnique("Svc", "email")
	g.AddNode("n1", "Svc", map[string]any{"email": "a@x"})
	if err := g.DropUnique("Svc", "email"); err != nil {
		t.Fatal(err)
	}
	if got := g.UniqueConstraints(); len(got) != 0 {
		t.Fatalf("constraints after drop: %v", got)
	}
	if _, err := g.AddNode("n2", "Svc", map[string]any{"email": "a@x"}); err != nil {
		t.Fatalf("enforcement must stop with the drop: %v", err)
	}
	// Re-declaring now hits the Neo4j rule: the live data is duplicated.
	if err := g.DeclareUnique("Svc", "email"); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("re-declare over duplicated live data: %v", err)
	}
}

// --- mid-Batch rejection --------------------------------------------------------

func TestUniqueDeclareDropMidBatchRejected(t *testing.T) {
	g := New()
	var declErr, dropErr error
	err := g.Batch(func(g *Graph) {
		declErr = g.DeclareUnique("Svc", "email")
		dropErr = g.DropUnique("Svc", "email")
		if _, err := g.AddNode("n1", "Svc", nil); err != nil {
			t.Fatal(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if declErr == nil || !strings.Contains(declErr.Error(), "inside Batch") {
		t.Fatalf("mid-Batch declare: %v", declErr)
	}
	if dropErr == nil || !strings.Contains(dropErr.Error(), "inside Batch") {
		t.Fatalf("mid-Batch drop: %v", dropErr)
	}
	// The sentinel identity is the programmatic face (F-15): both raise
	// sites wrap vectra.ErrInBatch with the operation name, so server/CLI
	// classification does not string-match. One site pinned suffices —
	// DeclareUnique and DropUnique share the raise shape.
	if !errors.Is(declErr, ErrInBatch) {
		t.Fatalf("mid-Batch declare does not unwrap to ErrInBatch: %v", declErr)
	}
	if !errors.Is(dropErr, ErrInBatch) {
		t.Fatalf("mid-Batch drop does not unwrap to ErrInBatch: %v", dropErr)
	}
	if got := g.UniqueConstraints(); len(got) != 0 {
		t.Fatalf("mid-Batch declaration leaked into the registry: %v", got)
	}
	// The batch itself stayed healthy and the declaration lands outside.
	if err := g.DeclareUnique("Svc", "email"); err != nil {
		t.Fatalf("declare after the batch: %v", err)
	}
	if _, err := g.AddNode("n2", "Svc", map[string]any{"email": "x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("n3", "Svc", map[string]any{"email": "x"}); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("post-batch enforcement: %v", err)
	}
}

// --- partial application (Go Batch: first-error-stops) ---------------------------

func TestUniqueBatchPartialApplication(t *testing.T) {
	db := filepath.Join(t.TempDir(), "g.db")
	g, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if err := g.DeclareUnique("Svc", "email"); err != nil {
		t.Fatal(err)
	}

	var viol error
	err = g.Batch(func(g *Graph) {
		if _, err := g.AddNode("k1", "Svc", map[string]any{"email": "a"}); err != nil {
			viol = err
			return
		}
		if _, err := g.AddNode("k2", "Svc", map[string]any{"email": "a"}); err != nil {
			viol = err // the violating op: zero side effects
			return
		}
		t.Fatal("post-violation op must never execute")
	})
	if err != nil {
		t.Fatalf("batch boundary: %v", err)
	}
	var uv *UniqueViolationError
	if !errors.As(viol, &uv) || uv.ExistingKey != "k1" || uv.AttemptedKey != "k2" {
		t.Fatalf("violating op: %v (%+v)", viol, uv)
	}
	if _, ok := g.NodeByKey("k1"); !ok {
		t.Fatal("pre-violation op must be durable (published)")
	}
	if _, ok := g.NodeByKey("k2"); ok {
		t.Fatal("violating op must leave no node")
	}
	if got := g.NodeCount(); got != 1 {
		t.Fatalf("live nodes = %d, want 1", got)
	}
	// The WAL carries the declaration frame and exactly the pre-violation op:
	// [IndexDecl(Svc.email), cs{k1}] — the violating op never records.
	frames := walFrames(t, db+".wal")
	if len(frames) != 2 {
		t.Fatalf("wal frames after partial batch = %d, want 2 (decl, cs): %+v", len(frames), frames)
	}
	if len(frames[0].UniqueConstraints) != 1 || frames[0].Changeset != nil {
		t.Fatalf("frame 0 = %+v, want the declaration frame", frames[0])
	}
	if frames[1].Changeset == nil || len(frames[1].Changeset.Nodes) != 1 || frames[1].Changeset.Nodes[0].Key != "k1" {
		t.Fatalf("frame 1 = %+v, want exactly k1's node op", frames[1])
	}
	// The registry is not poisoned by the rejected attempt.
	if _, err := g.AddNode("k2", "Svc", map[string]any{"email": "a"}); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("k1's value must still be owned: %v", err)
	}
	if _, err := g.AddNode("k2", "Svc", map[string]any{"email": "b"}); err != nil {
		t.Fatalf("fresh value after rejection: %v", err)
	}
}

// --- the pre-state asymmetry (spec §2.3/§8): A -> B -> A across two keys ---------

func TestUniquePreStateAsymmetryABA(t *testing.T) {
	t.Run("builder-only window", func(t *testing.T) {
		// k1's A record exists ONLY in the open window's builder: the
		// release step must fetch pre-state builder-first, or A leaks as
		// owned forever and k2's write is wrongly rejected.
		g := New()
		g.DeclareUnique("Person", "email")
		g.AddNode("k1", "Person", map[string]any{"email": "A"})
		if _, err := g.AddNode("k1", "Person", map[string]any{"email": "B"}); err != nil {
			t.Fatalf("in-window override: %v", err)
		}
		if _, err := g.AddNode("k2", "Person", map[string]any{"email": "A"}); err != nil {
			t.Fatalf("A must be free after the in-window override: %v", err)
		}
		// The registry kept the new ownership: B belongs to k1.
		_, err := g.AddNode("k3", "Person", map[string]any{"email": "B"})
		var uv *UniqueViolationError
		if !errors.As(err, &uv) || uv.ExistingKey != "k1" || uv.Value != "B" {
			t.Fatalf("B ownership after override: %v (%+v)", err, uv)
		}
	})

	t.Run("inside one batch", func(t *testing.T) {
		g := New()
		g.DeclareUnique("Person", "email")
		err := g.Batch(func(g *Graph) {
			if _, err := g.AddNode("k1", "Person", map[string]any{"email": "A"}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.AddNode("k1", "Person", map[string]any{"email": "B"}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.AddNode("k2", "Person", map[string]any{"email": "A"}); err != nil {
				t.Fatalf("A must be free after the override: %v", err)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("published mirror via the facade", func(t *testing.T) {
		// The other half of the asymmetry: the facade reads the published
		// generation, so k1's record is published-resident when the override
		// lands — the release must come from the mirror.
		g := New()
		w := newWriteGraph(g)
		g.DeclareUnique("Person", "email")
		if _, _, err := w.CreateNode("k1", "Person", map[string]any{"email": "A"}); err != nil {
			t.Fatal(err)
		}
		if _, _, err := w.CreateNode("k2", "Person", nil); err != nil {
			t.Fatal(err)
		}
		if got := g.NodeCount(); got != 2 { // force publication
			t.Fatalf("setup nodes = %d", got)
		}
		if _, err := w.SetNodeProps("k1", map[string]any{"email": "B"}); err != nil {
			t.Fatalf("published-side override: %v", err)
		}
		if _, err := w.SetNodeProps("k2", map[string]any{"email": "A"}); err != nil {
			t.Fatalf("A must be free after the published override: %v", err)
		}
		if _, err := w.SetNodeProps("k2", map[string]any{"email": "B"}); !errors.Is(err, ErrUniqueViolation) {
			t.Fatalf("B must be owned by k1 now: %v", err)
		}
	})
}

// --- WAL recording of declarations ----------------------------------------------

func TestUniqueDeclWALFrames(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "g.db")
	g, err := Open(db, Options{WALMode: ModeAlways})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	g.AddNode("n0", "Svc", nil)
	if err := g.DeclareUnique("Svc", "email"); err != nil {
		t.Fatal(err)
	}
	if err := g.DropUnique("Svc", "email"); err != nil {
		t.Fatal(err)
	}
	if err := g.DeclareUnique("Svc", "email"); err != nil {
		t.Fatal(err)
	}
	if _, err := g.AddNode("n1", "Svc", map[string]any{"email": "a"}); err != nil {
		t.Fatal(err)
	}

	frames := walFrames(t, db+".wal")
	if len(frames) != 5 {
		t.Fatalf("frames = %d, want 5 (cs, decl, drop-all, decl, cs): %+v", len(frames), frames)
	}
	type shape struct {
		cs     bool
		ucsNil bool
		ucs    int
	}
	want := []shape{
		{cs: true, ucsNil: true},
		{cs: false, ucsNil: false, ucs: 1},
		{cs: false, ucsNil: false, ucs: 0}, // drop-all: non-nil empty survives the wire
		{cs: false, ucsNil: false, ucs: 1},
		{cs: true, ucsNil: true},
	}
	for i, w := range want {
		fr := frames[i]
		got := shape{cs: fr.Changeset != nil, ucsNil: fr.UniqueConstraints == nil, ucs: len(fr.UniqueConstraints)}
		if got != w {
			t.Fatalf("frame %d = %+v, want %+v", i, got, w)
		}
	}
	if got := frames[1].UniqueConstraints[0]; got != (wal.UniqueConstraint{Label: "Svc", Prop: "email"}) {
		t.Fatalf("declaration frame payload: %+v", got)
	}
}

// --- zero-constraint fast path ---------------------------------------------------

func TestUniqueFastPathLeavesRegistryNil(t *testing.T) {
	g := New()
	g.AddNode("a", "L", map[string]any{"p": "x"})
	g.AddNode("b", "L", map[string]any{"p": "x"}) // duplicates, but unconstrained
	if err := g.Batch(func(g *Graph) {
		g.AddNode("c", "L", nil)
		g.DeleteNode("c")
	}); err != nil {
		t.Fatal(err)
	}
	if g.uniqueIdx != nil {
		t.Fatal("an unconstrained graph must never allocate the registry")
	}
	if g.uniqueStale {
		t.Fatal("the stale flag must start clear")
	}
	if got := len(g.UniqueConstraints()); got != 0 {
		t.Fatalf("constraints = %d, want 0", got)
	}
}
