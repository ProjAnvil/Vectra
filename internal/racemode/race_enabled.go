//go:build race

// Package racemode single-sources the race-detector build flag (audit
// F-07): the one build-tag pair in the module exporting whether the race
// detector is on. Packages with allocation-count or runtime-budget test
// assertions alias it as a local const instead of carrying their own copy
// of the pair — the pattern had already spread to five packages.
package racemode

// Enabled reports whether the race detector build tag is set. Exact alloc
// bounds are meaningless under -race (the detector adds its own
// bookkeeping allocations) and runtime budgets blow up many-fold, so test
// gates skip or scale under it.
const Enabled = true
