// Unique-constraint registry (spec 2026-10-30): declare/drop/list,
// write-time check/swap/release, rebuild, and WAL/store declaration I/O.

package vectra

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/projanvil/vectra/internal/store"
	"github.com/projanvil/vectra/internal/wal"
)

// UniqueConstraint declares property-value uniqueness over one (label,
// prop) pair (unique-constraints spec §2.1): once declared, a write that
// would leave two live nodes of that label sharing the same string value
// for the prop is rejected with *UniqueViolationError. The (label, prop)
// pair is the constraint's identity — there are no constraint names.
type UniqueConstraint struct {
	Label string
	Prop  string
}

// UniqueViolationError is the write-time rejection payload: the (Label,
// Prop) constraint already owns Value for ExistingKey, and the write that
// produced this error carried AttemptedKey, which would have created a
// second owner.
type UniqueViolationError struct {
	Label        string
	Prop         string
	Value        string
	ExistingKey  string
	AttemptedKey string
}

// Error pins the violation face shared by every surface (Go API, Cypher,
// server wire): the constraint identity, the contended value and its
// current owner.
func (e *UniqueViolationError) Error() string {
	return fmt.Sprintf("unique constraint violation: (%s.%s) value %q already owned by key %q", e.Label, e.Prop, e.Value, e.ExistingKey)
}

// Unwrap makes errors.Is(err, ErrUniqueViolation) true for every violation
// while errors.As keeps the payload reachable.
func (e *UniqueViolationError) Unwrap() error { return ErrUniqueViolation }

// --- unique constraints (spec 2026-10-30) -------------------------------------

// uniqueDup is one duplicate ownership pair surfaced by a DeclareUnique
// validation scan: value is already owned by key (among others).
type uniqueDup struct{ value, key string }

// uniqueDeclError is the declaration-time validation failure (spec §2.2):
// declaring over live data where two nodes already share a (label, prop)
// string value. It unwraps to ErrUniqueViolation — the same class as
// write-time rejections — and lists up to ten duplicate (value, key) pairs
// sorted by value then key, so every raft replica reports byte-identical
// text from any map iteration order.
type uniqueDeclError struct {
	c     UniqueConstraint
	pairs []uniqueDup
}

func (e *uniqueDeclError) Error() string {
	parts := make([]string, len(e.pairs))
	for i, p := range e.pairs {
		parts[i] = strconv.Quote(p.value) + " owned by " + strconv.Quote(p.key)
	}
	return "vectra: DeclareUnique(" + e.c.Label + "." + e.c.Prop + "): duplicate values: " +
		strings.Join(parts, ", ")
}

func (e *uniqueDeclError) Unwrap() error { return ErrUniqueViolation }

// DeclareUnique registers a property-value uniqueness constraint over one
// (label, prop) pair: afterwards a write that would leave two live nodes of
// the label sharing the same string value for the prop is rejected at the
// write-method entry with zero side effects for that operation (spec §2.1).
// The (label, prop) pair is the constraint's identity — there are no
// constraint names, and the two nodes-mutating root entries (AddNode,
// DeleteNode) are the only enforcement points: every other surface —
// SetNodeProps, Cypher writes, WAL replay — funnels into AddNode.
//
// Declaration validates the FULL live state first (the Neo4j rule, §2.2):
// any string value the (label, prop) pair already duplicates fails the
// declaration with an error matching ErrUniqueViolation listing up to ten
// (value, key) pairs sorted by value then key; the constraint is then NOT
// registered. A duplicate declaration fails with ErrConstraintExists; both
// rejections happen before any side effect.
//
// Rejected inside Batch (the CreateVectorIndex precedent, §2.1): the
// validation scan needs the full live view and replay applies declaration
// frames between changesets, never inside one. Non-string prop values never
// participate (documented v1 divergence from Neo4j, §2.5). The declaration
// is WAL-logged log-first as a full-list IndexDecl frame — drops are
// absences, replay replaces the whole set.
//
// Single-writer discipline: like every write method, call from the graph's
// writer goroutine. The scan is O(live nodes) once per declaration (the
// same class as a full-text build at Seal, §8).
func (g *Graph) DeclareUnique(label, prop string) error {
	if g.closed.Load() {
		return ErrClosed
	}
	if err := g.checkWriter(); err != nil {
		return err
	}
	if g.batching {
		// The validation scan must see this batch's own writes, which are
		// pre-publication, and replay applies declaration frames BETWEEN
		// changesets, never inside one — reject instead of diverging (the
		// CreateVectorIndex precedent, vectra_vector.go).
		return fmt.Errorf("%w: DeclareUnique", ErrInBatch)
	}
	g.flushPend() // publish the open window so the scan sees all live state
	c := UniqueConstraint{Label: label, Prop: prop}
	if _, ok := g.uniqueIdx[c]; ok {
		return ErrConstraintExists
	}
	g.uniqueEnsureFresh()
	// Clear the stale flag unconditionally: the rebuild above is gated on
	// constraints existing, so a mark set while the graph was unconstrained
	// (a Batch panic, §3.3) would linger past an empty registry — declaring
	// here has nothing to rebuild, the scan below registers this
	// constraint from live state.
	g.uniqueStale = false
	vals, dups := g.uniqueScanConstraint(c)
	if len(dups) > 0 {
		return &uniqueDeclError{c: c, pairs: dups}
	}
	if g.uniqueIdx == nil {
		g.uniqueIdx = map[UniqueConstraint]map[string]string{}
	}
	g.uniqueIdx[c] = vals
	g.recordUniqueDecls()
	return nil
}

// DropUnique removes the constraint: enforcement stops immediately and its
// value map is discarded (the value lifetimes it tracked end with it —
// nodes that held duplicate values before the declaration remain untouched).
// The WAL frame re-declares the surviving list; replay drops by absence.
// Dropping an unknown constraint fails with ErrConstraintNotFound before
// any side effect; declarations never run inside Batch (see DeclareUnique).
func (g *Graph) DropUnique(label, prop string) error {
	if g.closed.Load() {
		return ErrClosed
	}
	if err := g.checkWriter(); err != nil {
		return err
	}
	if g.batching {
		return fmt.Errorf("%w: DropUnique", ErrInBatch)
	}
	g.flushPend()
	c := UniqueConstraint{Label: label, Prop: prop}
	if _, ok := g.uniqueIdx[c]; !ok {
		return ErrConstraintNotFound
	}
	delete(g.uniqueIdx, c)
	g.recordUniqueDecls()
	return nil
}

// UniqueConstraints returns the declared constraints as a sorted copy
// (label, then prop); mutating the result never touches the registry.
// Flushes the caller's absorption window first like every read entry. No
// freshness rebuild runs here: the §3.3 rebuilds rebuild each constraint's
// VALUE map only and never add or remove a constraint, so the key set this
// listing serves is always exact. The registry is writer-private state:
// call from the writer goroutine or with writes quiesced.
func (g *Graph) UniqueConstraints() []UniqueConstraint {
	g.flushPend()
	out := make([]UniqueConstraint, 0, len(g.uniqueIdx))
	for c := range g.uniqueIdx {
		out = append(out, c)
	}
	slices.SortFunc(out, func(a, b UniqueConstraint) int {
		if c := cmp.Compare(a.Label, b.Label); c != 0 {
			return c
		}
		return cmp.Compare(a.Prop, b.Prop)
	})
	return out
}

// uniqueScanConstraint builds one constraint's value map from the writer's
// live state — the published mirror, which callers make complete by
// flushing the open window first (= published ∪ builder-visible, §2.2) —
// and collects the duplicate (value, key) pairs it finds, sorted by value
// then key and capped at ten (deterministic across raft replicas).
func (g *Graph) uniqueScanConstraint(c UniqueConstraint) (vals map[string]string, dups []uniqueDup) {
	vals = make(map[string]string)
	owners := map[string][]string{} // value -> owning keys (len > 1 = duplicate)
	for key, ref := range g.nodes {
		if !ref.live || ref.label != c.Label {
			continue
		}
		v, ok := ref.props[c.Prop].(string)
		if !ok {
			continue // non-string values are invisible (§2.5)
		}
		vals[v] = key
		owners[v] = append(owners[v], key)
	}
	for v, keys := range owners {
		if len(keys) > 1 {
			slices.Sort(keys)
			for _, k := range keys {
				dups = append(dups, uniqueDup{value: v, key: k})
			}
		}
	}
	slices.SortFunc(dups, func(a, b uniqueDup) int {
		if c := cmp.Compare(a.value, b.value); c != 0 {
			return c
		}
		return cmp.Compare(a.key, b.key)
	})
	if len(dups) > 10 {
		dups = dups[:10]
	}
	return vals, dups
}

// uniquePre is the pre-image of one node for the registry's release step.
type uniquePre struct {
	label string
	props map[string]any
	live  bool
}

// uniquePreState fetches a node's current (label, props) as the writer sees
// it — BUILDER FIRST, then the published mirror. The mirror (g.nodes)
// reconciles only at publishLayer, so a key written in the open window
// lives only in the builder; reading the mirror alone would leak the key's
// registered values forever (the §2.3 asymmetry: the Cypher facade reads
// the published generation, the release reads builder-first). A key deleted
// this window has no live pre-image (its values were released at delete
// time). The zero-constraint fast path makes this a no-op — one len check,
// no allocation — on unconstrained graphs.
func (g *Graph) uniquePreState(key string) uniquePre {
	if len(g.uniqueIdx) == 0 {
		return uniquePre{}
	}
	g.uniqueEnsureFresh()
	if g.builder != nil {
		if rec, ok := g.builder.LookupNode(key); ok {
			return uniquePre{label: rec.Label, props: rec.Props, live: true}
		}
		if g.builder.NodeTombstoned(key) {
			return uniquePre{} // deleted this window: nothing live to release
		}
	}
	if ref, ok := g.nodes[key]; ok && ref.live {
		return uniquePre{label: ref.label, props: ref.props, live: true}
	}
	return uniquePre{}
}

// uniqueCheckAdd validates one AddNode against the registry (§2.3): the
// write is rejected when any constraint matching the node's NEW label has
// its prospective string value owned by a DIFFERENT live key — the key's
// own current ownership counts as released (override/revive re-assign their
// own values freely). Non-string values and non-matching labels are
// invisible. Callers run this before recordNode, the ID ratchet and the
// builder, making rejection zero-side-effect by construction. The scan is
// single-pass and allocation-free (happy path): every constraint is visited
// — no early return, since a partial map walk would make the reported
// violation depend on iteration order — and the ORDER-MINIMAL violated
// constraint under the label-then-prop order (the listing/save emission
// order) is kept, so a write violating several constraints at once reports
// the same FIRST violation everywhere despite map randomness. Violations
// are the cold path, so the full walk is free where it matters; only the
// returned error allocates.
func (g *Graph) uniqueCheckAdd(key, label string, props map[string]any) error {
	if len(g.uniqueIdx) == 0 {
		return nil
	}
	var (
		found     bool
		best      UniqueConstraint
		bestVal   string
		bestOwner string
	)
	for c := range g.uniqueIdx {
		if c.Label != label {
			continue
		}
		v, ok := props[c.Prop].(string)
		if !ok {
			continue
		}
		owner, taken := g.uniqueIdx[c][v]
		if !taken || owner == key {
			continue
		}
		// Every candidate shares the write's label, so the prop alone
		// breaks ties under the label-then-prop order; registry keys are
		// unique (label, prop) pairs, so the minimum is exact.
		if !found || c.Prop < best.Prop {
			found, best, bestVal, bestOwner = true, c, v, owner
		}
	}
	if !found {
		return nil
	}
	return &UniqueViolationError{
		Label: best.Label, Prop: best.Prop, Value: bestVal,
		ExistingKey: bestOwner, AttemptedKey: key,
	}
}

// uniqueSwap applies one ACCEPTED AddNode to the registry (§3.2): release
// the key's previously-owned values (the pre-image fetched at entry,
// builder-first), then register the new label/props' string values. Runs
// after the write's WAL and builder steps succeeded, keeping the registry
// in lockstep with live state; the single writer makes check→swap atomic.
func (g *Graph) uniqueSwap(pre uniquePre, key, label string, props map[string]any) {
	if len(g.uniqueIdx) == 0 {
		return
	}
	if pre.live {
		g.uniqueRelease(pre.label, pre.props, key)
	}
	for c := range g.uniqueIdx {
		if c.Label != label {
			continue
		}
		if v, ok := props[c.Prop].(string); ok {
			g.uniqueIdx[c][v] = key
		}
	}
}

// uniqueRelease drops every value the (label, props) pre-image owns in the
// registry, guarded to entries the key itself owns: idempotent, and a stale
// pre-image can never steal another key's registration.
func (g *Graph) uniqueRelease(label string, props map[string]any, key string) {
	for c := range g.uniqueIdx {
		if c.Label != label {
			continue
		}
		v, ok := props[c.Prop].(string)
		if !ok {
			continue
		}
		if owner, taken := g.uniqueIdx[c][v]; taken && owner == key {
			delete(g.uniqueIdx[c], v)
		}
	}
}

// uniqueEnsureFresh rebuilds the registry when the stale flag is set (§3.3).
// The flag is marked only at the self-healing boundaries — Batch panic
// recovery and forceMerge's post-rebase mirror rebuild; normal
// publish/fold/seal paths deliberately never mark it (incremental
// maintenance over stable keys keeps the registry correct across layer
// changes). No-op unless stale.
func (g *Graph) uniqueEnsureFresh() {
	if g.uniqueStale && len(g.uniqueIdx) > 0 {
		g.uniqueRebuild()
	}
}

// uniqueRebuild is the self-healing rebuild entrypoint (§3.3, the stale
// flag's recovery wiring): every constraint's value
// map is rebuilt from the writer's current live state — the open absorption
// window publishes first, so the mirror scan sees all of it (published ∪
// builder-visible). Mid-Batch the stale flag can never be set (Batch panic
// recovery drops the builder only after g.batching clears; rebase repair
// runs at merge boundaries outside Batch), so the flush never cuts an open
// batch. Live state that genuinely duplicates a value keeps an arbitrary
// single owner — the registry's at-most-one-owner invariant holds; the
// duplicate data itself is a caller problem that DeclareUnique surfaces.
func (g *Graph) uniqueRebuild() {
	g.uniqueStale = false
	if len(g.uniqueIdx) == 0 {
		return
	}
	g.flushPend()
	for c := range g.uniqueIdx {
		g.uniqueIdx[c] = make(map[string]string, g.nodeTotal)
	}
	for key, ref := range g.nodes {
		if !ref.live {
			continue
		}
		for c := range g.uniqueIdx {
			if c.Label != ref.label {
				continue
			}
			if v, ok := ref.props[c.Prop].(string); ok {
				g.uniqueIdx[c][v] = key
			}
		}
	}
}

// applyUniqueDecls reconciles the registry to a FULL declaration list with
// REPLACE semantics (spec §3.4, the applyVecDecls rule): constraints absent
// from the list drop — a non-nil list including the empty one replaces the
// whole set, so replayed drops are never resurrected. This is the trust-path
// counterpart of DeclareUnique: no duplicate validation (the data was valid
// when persisted; a genuinely duplicated live value keeps an arbitrary
// single owner exactly like a rebuild), and replay applies frames in their
// original op order, so a constraint the registry already tracks keeps its
// incrementally-maintained value map — only new constraints scan live
// state. WAL replay runs with recording detached (g.wal nil) and
// loadGraphForest runs pre-attachment, so recordUniqueDecls no-ops on both
// call paths.
func (g *Graph) applyUniqueDecls(list []wal.UniqueConstraint) {
	want := make(map[UniqueConstraint]struct{}, len(list))
	for _, uc := range list {
		want[UniqueConstraint{Label: uc.Label, Prop: uc.Prop}] = struct{}{}
	}
	for c := range g.uniqueIdx {
		if _, ok := want[c]; !ok {
			delete(g.uniqueIdx, c)
		}
	}
	g.flushPend() // the scan below must see the whole live state (§2.2's union)
	for c := range want {
		if _, ok := g.uniqueIdx[c]; ok {
			continue
		}
		vals, _ := g.uniqueScanConstraint(c)
		if g.uniqueIdx == nil {
			g.uniqueIdx = map[UniqueConstraint]map[string]string{}
		}
		g.uniqueIdx[c] = vals
	}
	g.recordUniqueDecls()
}

// recordUniqueDecls logs the full current unique-constraint declaration list
// as one opUnique pendOp at the current op position (log-first; flushPending
// coalesces it with adjacent declaration ops into the IndexDecl run,
// last-in-run wins). The slice is ALWAYS built non-nil — including the empty
// list after the last constraint drops — so a drop-all declaration survives
// the wire as an explicit empty rather than a null frame (the recordVecDecls
// rule, spec 2026-10-30 §4.1).
func (g *Graph) recordUniqueDecls() {
	if g.wal == nil {
		return
	}
	decls := make([]wal.UniqueConstraint, 0, len(g.uniqueIdx))
	for c := range g.uniqueIdx {
		decls = append(decls, wal.UniqueConstraint{Label: c.Label, Prop: c.Prop})
	}
	// Sorted emission: map iteration order is not byte-stable, and the
	// frame bytes must be deterministic across sessions and raft replicas
	// (Task 2 review carry-over; UniqueConstraints sorts the same way).
	slices.SortFunc(decls, func(a, b wal.UniqueConstraint) int {
		if c := cmp.Compare(a.Label, b.Label); c != 0 {
			return c
		}
		return cmp.Compare(a.Prop, b.Prop)
	})
	g.pendOps = append(g.pendOps, pendOp{kind: opUnique, uniqueDecls: decls})
	_ = g.maybeFlush()
}

// uniqueStoreDecls snapshots the declaration list for persistence (the
// saveView encode side, spec §3.5): sorted label-then-prop for
// byte-stable snapshots, nil when unconstrained so the zero-constraint
// store path allocates nothing. Writer-side only — the registry is
// writer-private state (the async-compact flight pins this BEFORE its
// goroutine starts).
func (g *Graph) uniqueStoreDecls() []store.UniqueConstraint {
	if len(g.uniqueIdx) == 0 {
		return nil
	}
	out := make([]store.UniqueConstraint, 0, len(g.uniqueIdx))
	for c := range g.uniqueIdx {
		out = append(out, store.UniqueConstraint{Label: c.Label, Prop: c.Prop})
	}
	slices.SortFunc(out, func(a, b store.UniqueConstraint) int {
		if c := cmp.Compare(a.Label, b.Label); c != 0 {
			return c
		}
		return cmp.Compare(a.Prop, b.Prop)
	})
	return out
}
