package vectra

import "fmt"

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
// writer mirror the registry is incrementally maintained against). It pins
// the spec §3.3 claim that folds, seals and background rebases preserve the
// registry with no rebuild: any drift between incremental maintenance and
// live state — a leaked value, a phantom owner, a lost registration — fails
// with a description. Duplicate string values in LIVE state under a declared
// constraint also fail (enforcement itself broke). Test-only helper
// (unique-constraints spec §6); used by the persistence suite and the stress
// arc. Writer discipline applies (it flushes the absorption window).
func (g *Graph) VerifyUniqueInvariantForTest() error {
	g.flushPend()
	v := g.view.Load()
	nodes, _, _, _, _ := v.unionRecords()
	truth := make(map[UniqueConstraint]map[string]string, len(g.uniqueIdx))
	for c := range g.uniqueIdx {
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
	for c, want := range truth {
		got := g.uniqueIdx[c]
		if len(got) != len(want) {
			return fmt.Errorf("unique invariant: (%s.%s) registry holds %d values, live state holds %d",
				c.Label, c.Prop, len(got), len(want))
		}
		for val, key := range want {
			if owner, ok := got[val]; !ok || owner != key {
				return fmt.Errorf("unique invariant: (%s.%s) value %q owned by %q in registry, %q in live state",
					c.Label, c.Prop, val, owner, key)
			}
		}
	}
	return nil
}
