package vectra

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
)

// Test-only surface for the .vec forest cache (Phase 2, spec §2.3):
// lets external-package tests assert which Open path a session took.

// VecForestLoadedForTest reports whether the last Open mounted the
// .vec forest cache (true) or discarded it and rebuilt the indexes
// from the snapshot (false).
func (g *Graph) VecForestLoadedForTest() bool { return g.vecForestLoaded }

// RetainLegacyArtifactForTest exposes the migration rollback-artifact copy
// helper so external tests can pin its copy semantics directly (original
// stays in place; .bak holds the bytes) — Open has no cheap Seal-failure
// injection seam.
var RetainLegacyArtifactForTest = retainLegacyArtifact

// RaceEnabledForTest exposes the race-detector build tag so external-package
// tests can scale timing-sensitive wait windows under -race (the pack lane
// slows many-fold; see root's race_enabled.go).
var RaceEnabledForTest = raceEnabled

// VerifyUniqueInvariantForTest compares the writer-private unique-constraint
// registry against ground truth derived from the PUBLISHED generation (the
// same union a rebuild, a Save or a Clone would see — deliberately not the
// writer mirror the registry is incrementally maintained against). The
// caller PINS the expected constraint set (want; zero args means no
// constraints): the registry's key set must equal it exactly, with wrongly
// kept (extra) and wrongly dropped (missing) constraints reported by name —
// the helper never derives the expected set from the registry it is
// verifying. For each wanted constraint it pins the spec §3.3 claim that
// folds, seals and background rebases preserve the registry with no rebuild:
// any drift between incremental maintenance and live state — a leaked value,
// a phantom owner, a lost registration — fails with a description.
// Duplicate string values in LIVE state under a declared constraint also
// fail (enforcement itself broke). Test-only helper (unique-constraints
// spec §6); used by the persistence suite and the stress arc. Writer
// discipline applies (it flushes the absorption window).
func (g *Graph) VerifyUniqueInvariantForTest(want ...UniqueConstraint) error {
	g.flushPend()
	// Key set first: the registry must hold exactly the wanted constraints
	// (g.uniqueIdx is itself keyed by constraint, so membership is direct).
	wantSet := make(map[UniqueConstraint]struct{}, len(want))
	for _, c := range want {
		wantSet[c] = struct{}{}
	}
	var extra, missing []UniqueConstraint
	for c := range g.uniqueIdx {
		if _, ok := wantSet[c]; !ok {
			extra = append(extra, c)
		}
	}
	for c := range wantSet {
		if _, ok := g.uniqueIdx[c]; !ok {
			missing = append(missing, c)
		}
	}
	if len(extra) > 0 || len(missing) > 0 {
		return fmt.Errorf("unique invariant: registry key set mismatch: missing %s, extra %s",
			formatUniqueConstraints(missing), formatUniqueConstraints(extra))
	}
	// Value truth for the wanted set only, from the published generation.
	v := g.view.Load()
	nodes, _, _, _, _ := v.unionRecords()
	truth := make(map[UniqueConstraint]map[string]string, len(wantSet))
	for c := range wantSet {
		truth[c] = make(map[string]string)
	}
	for _, n := range nodes {
		for c := range truth {
			if n.Label != c.Label {
				continue
			}
			s, ok := n.Props[c.Prop].(string)
			if !ok {
				continue // non-string values are invisible (§2.5)
			}
			if prev, dup := truth[c][s]; dup {
				return fmt.Errorf("unique invariant: live state duplicates (%s.%s)=%q: keys %q and %q",
					c.Label, c.Prop, s, prev, n.Key)
			}
			truth[c][s] = n.Key
		}
	}
	for c, wantVals := range truth {
		got := g.uniqueIdx[c]
		if len(got) != len(wantVals) {
			return fmt.Errorf("unique invariant: (%s.%s) registry holds %d values, live state holds %d",
				c.Label, c.Prop, len(got), len(wantVals))
		}
		for val, key := range wantVals {
			if owner, ok := got[val]; !ok || owner != key {
				return fmt.Errorf("unique invariant: (%s.%s) value %q owned by %q in registry, %q in live state",
					c.Label, c.Prop, val, owner, key)
			}
		}
	}
	return nil
}

// formatUniqueConstraints renders a constraint list in the deterministic
// label-then-prop order (the listing/save emission order): "(A.p), (B.q)";
// an empty list renders as "none". Callers pass locally built slices, so
// the in-place sort never touches the caller's variadic backing array.
func formatUniqueConstraints(cs []UniqueConstraint) string {
	if len(cs) == 0 {
		return "none"
	}
	slices.SortFunc(cs, func(a, b UniqueConstraint) int {
		if c := cmp.Compare(a.Label, b.Label); c != 0 {
			return c
		}
		return cmp.Compare(a.Prop, b.Prop)
	})
	parts := make([]string, len(cs))
	for i, c := range cs {
		parts[i] = fmt.Sprintf("(%s.%s)", c.Label, c.Prop)
	}
	return strings.Join(parts, ", ")
}
