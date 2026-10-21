package vectra

// Test-only surface for the .vec forest cache (Phase 2, spec §2.3):
// lets external-package tests assert which Open path a session took.

// VecForestLoadedForTest reports whether the last Open mounted the
// .vec forest cache (true) or discarded it and rebuilt the indexes
// from the snapshot (false).
func (g *Graph) VecForestLoadedForTest() bool { return g.vecForestLoaded }

// RetainLegacyArtifactForTest exposes the migration rollback-artifact helper
// (rename-aside + restore func) so external tests can pin its restore
// semantics directly — Open has no cheap Seal-failure injection seam.
var RetainLegacyArtifactForTest = retainLegacyArtifact
